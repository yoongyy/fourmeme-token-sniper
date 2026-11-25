use anyhow::{Context, Result, anyhow};
use ethers::contract::abigen;
use ethers::middleware::SignerMiddleware;
use ethers::providers::{JsonRpcClient, Middleware, Provider, Ws};
use ethers::signers::{LocalWallet, Signer};
use ethers::types::transaction::eip2718::TypedTransaction;
use ethers::types::{Address, BlockNumber, H256, TransactionRequest, U256, U64};
use ethers::utils::hex;
use mongodb::bson::oid::ObjectId;
use redis::streams::StreamReadReply;
use sniper_fourmeme::ave::{AveClient, ChainWalletSwapPayload, ChainWalletTx};
use sniper_fourmeme::config::{Config, TradeMethod};
use sniper_fourmeme::redis_utils::{build_redis_client, live_trade_stream_key};
use sniper_fourmeme::rpc_fallback::{RpcProvider, build_http_provider, rpc_urls_from_env};
use sniper_fourmeme::storage::MongoLogger;
use sniper_fourmeme::token_manager;
use sniper_fourmeme::trader::Trader;
use sniper_fourmeme::types::{TokenCreated, TokenEvent};
use serde::{Deserialize, Serialize};
use serde_json;
use std::env;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

const ORDER_STREAM_KEY: &str = "sniper:orders";
const NATIVE_TOKEN_ADDRESS: &str = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
const SMART_HOLD_PENDING_KEY: &str = "smart_hold:pending";
const SMART_HOLD_LAST_PRICE_KEY: &str = "smart_hold:last_price";
const SMART_HOLD_QUEUE_PREFIX: &str = "smart_hold:queue:";

abigen!(
    ERC20,
    r#"[
        function allowance(address owner, address spender) view returns (uint256)
        function approve(address spender, uint256 value) returns (bool)
    ]"#
);

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SmartHoldEntry {
    wallet: String,
    token: String,
    token_name: String,
    token_symbol: String,
    buy_amount: f64,
    smart_trade_bnb: f64,
    liquidity_usd: Option<f64>,
    source: Option<String>,
    live_mode: bool,
    #[serde(default)]
    trigger_price: Option<f64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let cfg = Config::from_env();
    let mongo_logger = MongoLogger::connect(cfg.mongo()).await;
    let cfg_snapshot = cfg.clone();
    let redis = build_redis_client(cfg.redis()).context("redis is required for live_trade")?;
    let stream_key = live_trade_stream_key();

    let wss_url = std::env::var("LIVE_TRADE_WSS_URL").unwrap_or_else(|_| cfg.wss_url.clone());
    let ws = Ws::connect(wss_url).await.context("failed to connect ws")?;
    let ws_provider = Arc::new(Provider::new(ws).interval(Duration::from_millis(100)));
    let ws_provider_for_trader = ws_provider.clone();

    let rpc_urls = rpc_urls_from_env(cfg.rpc_url.clone(), &["RPC_URL2", "RPC_URL3"]);
    let http_provider = Arc::new(
        build_http_provider(rpc_urls.clone(), Duration::from_millis(1500))
            .context("RPC_URL is invalid")?,
    );
    let ave_client = if cfg_snapshot.ave().is_enabled() {
        Some(AveClient::new(cfg_snapshot.ave().clone()))
    } else {
        None
    };
    let ave_wallet = cfg_snapshot
        .private_key
        .clone()
        .and_then(|key| key.parse::<LocalWallet>().ok())
        .map(|wallet| wallet.with_chain_id(56u64));
    let ave_wallet_address = ave_wallet.as_ref().map(|wallet| wallet.address());
    let (_tx, rx) = mpsc::channel(1);
    let trader = Trader::new(
        rx,
        cfg,
        rpc_urls,
        None,
        None,
        ws_provider_for_trader,
        None,
        "live-trade",
        false,
        false,
    )
    .await?;

    let mut conn = redis.get_multiplexed_async_connection().await?;
    let _ = reset_live_trade_stream(&redis, &stream_key).await;
    let mut last_id = "0".to_string();
    let mut token_label_cache: HashMap<Address, String> = HashMap::new();
    println!(
        "{} [live-trade] listening stream={}",
        log_timestamp(),
        stream_key
    );
    loop {
        let reply: StreamReadReply = redis::cmd("XREAD")
            .arg("BLOCK")
            .arg(1000)
            .arg("COUNT")
            .arg(10)
            .arg("STREAMS")
            .arg(&stream_key)
            .arg(&last_id)
            .query_async(&mut conn)
            .await?;

        for key in reply.keys {
            for entry in key.ids {
                let stream_entry_id = entry.id.clone();
                last_id = stream_entry_id.clone();
                let map = entry.map;
                let Some(kind) = map_get_string(&map, "type") else { continue };
                let Some(token_raw) = map_get_string(&map, "token") else { continue };
                let token = match Address::from_str(&token_raw) {
                    Ok(addr) => addr,
                    Err(err) => {
                        eprintln!(
                            "{} [live-trade] invalid token {}: {err:?}",
                            log_timestamp(),
                            token_raw
                        );
                        continue;
                    }
                };
                let source = map_get_string(&map, "source").unwrap_or_else(|| "unknown".into());
                let token_label =
                    resolve_token_label(&mut token_label_cache, ws_provider.clone(), token).await;

                if kind == "buy" {
                    let amount_bnb = map_get_string(&map, "amount_bnb")
                        .and_then(|raw| raw.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    if amount_bnb <= 0.0 {
                        eprintln!(
                            "{} [live-trade] skip buy token={} : amount_bnb invalid",
                            log_timestamp(),
                            token_label
                        );
                        continue;
                    }
                    let alert_type = map_get_string(&map, "alert_type");
                    let smart_wallet = map_get_string(&map, "smart_wallet")
                        .and_then(|raw| Address::from_str(&raw).ok());
                    let smart_trade_bnb = map_get_string(&map, "smart_trade_bnb")
                        .and_then(|raw| raw.parse::<f64>().ok())
                        .filter(|value| *value > 0.0);
                    let wallet_name = map_get_string(&map, "wallet_name");
                    println!(
                        "{} [live-trade] buy token={} amount_bnb={:.6} source={}",
                        log_timestamp(),
                        token_label,
                        amount_bnb,
                        source
                    );
                    match trader.live_buy_with_details(token, amount_bnb).await {
                        Ok(result) => {
                            println!(
                                "{} [live-trade] buy submitted token={} tx={:#x}",
                                log_timestamp(),
                                token_label,
                                result.tx_hash
                            );
                            if cfg_snapshot.trade_method == TradeMethod::Wallet
                                && result.amount_raw.is_zero()
                            {
                                eprintln!(
                                    "{} [live-trade] buy resulted in zero tokens token={}",
                                    log_timestamp(),
                                    token_label
                                );
                                let _ = delete_stream_entry(
                                    &redis,
                                    &stream_key,
                                    &stream_entry_id,
                                )
                                .await;
                                continue;
                            }
                            if let Some(alert_type) = alert_type.as_deref() {
                                if alert_type == "SmartMoney Live" && smart_wallet.is_none() {
                                    eprintln!(
                                        "{} [live-trade] smart_wallet missing for smart sell monitor token={}",
                                        log_timestamp(),
                                        token_label
                                    );
                                    continue;
                                }
                                if let Some(mongo) = mongo_logger.as_ref() {
                                    let (name, symbol) = token_manager::fetch_token_name_symbol(
                                        ws_provider.clone(),
                                        token,
                                    )
                                    .await
                                    .unwrap_or((None, None));
                                    let name = name.unwrap_or_else(|| format!("{:#x}", token));
                                    let symbol = symbol.unwrap_or_else(|| "unknown".into());
                                    let token_created = TokenCreated {
                                        creator: Address::zero(),
                                        token,
                                        name: name.clone(),
                                        symbol: symbol.clone(),
                                    };
                                    let token_event = TokenEvent {
                                        contract: token,
                                        name: Some(name.clone()),
                                        symbol: Some(symbol.clone()),
                                        ticker: symbol.clone(),
                                        creator: Some(Address::zero()),
                                        tx_hash: Some(result.tx_hash),
                                        buy_amount: amount_bnb,
                                    };
                                    let price_source = trade_method_label(cfg_snapshot.trade_method);
                                    let wallet_name = wallet_name
                                        .as_deref()
                                        .or(cfg_snapshot.wallet_name.as_deref());
                                    let order_id = mongo
                                        .create_real_order(
                                            &token_created,
                                            &token_event,
                                            result.tx_hash,
                                            result.entry_price,
                                            0.0,
                                            0.0,
                                            alert_type,
                                            price_source,
                                            amount_bnb,
                                            result.entry_value_usd,
                                            result.amount_tokens,
                                            wallet_name,
                                            smart_wallet,
                                            smart_trade_bnb,
                                        )
                                        .await;
                                    match order_id {
                                        Ok(order_id) => {
                                            if let Err(err) = publish_real_order_event(
                                                &redis,
                                                &order_id,
                                                token,
                                                alert_type,
                                                cfg_snapshot.trade_method,
                                            )
                                            .await
                                            {
                                                eprintln!(
                                                    "{} [live-trade] failed to publish order event: {err:?}",
                                                    log_timestamp()
                                                );
                                            }
                                        }
                                        Err(err) => {
                                            eprintln!(
                                                "{} [mongo] failed to log real order: {err:?}",
                                                log_timestamp()
                                            );
                                        }
                                    }
                                } else {
                                    eprintln!(
                                        "{} [live-trade] mongo disabled; skipping order creation",
                                        log_timestamp()
                                    );
                                }
                                if alert_type == "SmartMoney Live" {
                                    if let Some(wallet) = smart_wallet {
                                        increment_live_follow_counts(&redis, wallet, token).await;
                                    }
                                }
                            }
                            let _ = delete_stream_entry(
                                &redis,
                                &stream_key,
                                &stream_entry_id,
                            )
                            .await;
                        }
                        Err(err) => {
                            eprintln!(
                                "{} [live-trade] buy failed token={}: {err:?}",
                                log_timestamp(),
                                token_label
                            );
                            if let Some(alert_type) = alert_type.as_deref() {
                                if alert_type == "SmartMoney Live" {
                                    if smart_hold_enabled() {
                                        if let Some(wallet) = smart_wallet {
                                            if smart_wallet_zero_balance(
                                                http_provider.clone(),
                                                token,
                                                wallet,
                                                &token_label,
                                            )
                                            .await
                                            {
                                                println!(
                                                    "{} [smart-hold] buy failed; skip hold (smart wallet balance zero) token={} wallet={:#x}",
                                                    log_timestamp(),
                                                    token_label,
                                                    wallet
                                                );
                                            } else {
                                                let (name, symbol) =
                                                    token_manager::fetch_token_name_symbol(
                                                        ws_provider.clone(),
                                                        token,
                                                    )
                                                    .await
                                                    .unwrap_or((None, None));
                                                let token_name = name
                                                    .unwrap_or_else(|| format!("{:#x}", token));
                                                let token_symbol =
                                                    symbol.unwrap_or_else(|| "unknown".into());
                                                let hold_entry = SmartHoldEntry {
                                                    wallet: format!("{wallet:#x}"),
                                                    token: format!("{token:#x}"),
                                                    token_name,
                                                    token_symbol,
                                                    buy_amount: amount_bnb,
                                                    smart_trade_bnb: smart_trade_bnb
                                                        .unwrap_or(0.0),
                                                    liquidity_usd: None,
                                                    source: Some("smart_money_live".into()),
                                                    live_mode: true,
                                                    trigger_price: None,
                                                };
                                                let price = token_manager::fetch_last_price(
                                                    ws_provider.clone(),
                                                    cfg_snapshot.token_manager_address,
                                                    token,
                                                )
                                                .await
                                                .unwrap_or(None)
                                                .unwrap_or(0.0);
                                                let _ = set_hold_last_price(
                                                    &redis,
                                                    wallet,
                                                    token,
                                                    price,
                                                )
                                                .await;
                                                if enqueue_or_merge_smart_hold(
                                                    &redis,
                                                    wallet,
                                                    token,
                                                    &hold_entry,
                                                )
                                                .await
                                                {
                                                    println!(
                                                        "{} [smart-hold] buy failed; added to hold token={} wallet={:#x} amount_bnb={:.6}",
                                                        log_timestamp(),
                                                        token_label,
                                                        wallet,
                                                        amount_bnb
                                                    );
                                                    let _ = delete_stream_entry(
                                                        &redis,
                                                        &stream_key,
                                                        &stream_entry_id,
                                                    )
                                                    .await;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else if kind == "sell" {
                    let force_ave = map_get_string(&map, "force_ave")
                        .map(|value| parse_bool_value(&value))
                        .unwrap_or(false);
                    let allow_ave_fallback = force_ave;
                    let amount_raw = map_get_string(&map, "amount_raw")
                        .and_then(|raw| U256::from_dec_str(&raw).ok())
                        .unwrap_or_else(U256::zero);
                    if source == "smart_sell_guard" {
                        println!(
                            "{} [live-trade] guard sell event token={} amount_raw={} force_ave={}",
                            log_timestamp(),
                            token_label,
                            amount_raw,
                            force_ave
                        );
                    }
                    if force_ave {
                        if let (Some(client), Some(wallet), Some(wallet_address)) =
                            (&ave_client, ave_wallet.as_ref(), ave_wallet_address)
                        {
                            let amount_raw = if amount_raw.is_zero() {
                                token_manager::fetch_token_balance(
                                    http_provider.clone(),
                                    token,
                                    wallet_address,
                                )
                                .await
                                .unwrap_or(U256::zero())
                            } else {
                                amount_raw
                            };
                            if amount_raw.is_zero() {
                                eprintln!(
                                    "{} [live-trade] AVE force sell skipped (balance zero) token={}",
                                    log_timestamp(),
                                    token_label
                                );
                                let _ = delete_stream_entry(
                                    &redis,
                                    &stream_key,
                                    &stream_entry_id,
                                )
                                .await;
                                continue;
                            }
                            let slippage_pct =
                                cfg_snapshot.liquidity_trade().sell_sl_per_block_pct();
                            match ave_sell_fallback(
                                client,
                                http_provider.clone(),
                                wallet,
                                wallet_address,
                                token,
                                amount_raw,
                                slippage_pct,
                            )
                            .await
                            {
                                Ok(hash) => {
                                    println!(
                                        "{} [live-trade] ave force sell submitted token={} tx={:#x}",
                                        log_timestamp(),
                                        token_label,
                                        hash
                                    );
                                    let _ = delete_stream_entry(
                                        &redis,
                                        &stream_key,
                                        &stream_entry_id,
                                    )
                                    .await;
                                }
                                Err(err) => {
                                    eprintln!(
                                        "{} [live-trade] ave force sell failed token={}: {err:?}",
                                        log_timestamp(),
                                        token_label
                                    );
                                }
                            }
                            continue;
                        } else {
                            eprintln!(
                                "{} [live-trade] ave force sell requested but AVE not configured; falling back to router token={}",
                                log_timestamp(),
                                token_label
                            );
                        }
                    }
                    if amount_raw.is_zero() {
                        println!(
                            "{} [live-trade] sell token={} amount_raw=all source={}",
                            log_timestamp(),
                            token_label,
                            source
                        );
                        match trader.live_sell_all_for_test(token).await {
                            Ok(hash) => {
                                println!(
                                    "{} [live-trade] sell submitted token={} tx={:#x}",
                                    log_timestamp(),
                                    token_label,
                                    hash
                                );
                                let _ = delete_stream_entry(
                                    &redis,
                                    &stream_key,
                                    &stream_entry_id,
                                )
                                .await;
                            }
                            Err(err) => {
                                eprintln!(
                                    "{} [live-trade] sell failed token={}: {err:?}",
                                    log_timestamp(),
                                    token_label
                                );
                                if allow_ave_fallback {
                                    if let (Some(client), Some(wallet), Some(wallet_address)) =
                                        (&ave_client, ave_wallet.as_ref(), ave_wallet_address)
                                    {
                                        let amount_raw = token_manager::fetch_token_balance(
                                            http_provider.clone(),
                                            token,
                                            wallet_address,
                                        )
                                        .await
                                        .unwrap_or(U256::zero());
                                        if amount_raw.is_zero() {
                                            eprintln!(
                                                "{} [live-trade] AVE fallback skipped (balance zero) token={}",
                                                log_timestamp(),
                                                token_label
                                            );
                                            continue;
                                        }
                                        let slippage_pct =
                                            cfg_snapshot.liquidity_trade().sell_sl_per_block_pct();
                                        match ave_sell_fallback(
                                            client,
                                            http_provider.clone(),
                                            wallet,
                                            wallet_address,
                                            token,
                                            amount_raw,
                                            slippage_pct,
                                        )
                                        .await
                                        {
                                            Ok(hash) => {
                                                println!(
                                                    "{} [live-trade] ave fallback sell submitted token={} tx={:#x}",
                                                    log_timestamp(),
                                                    token_label,
                                                    hash
                                                );
                                                let _ = delete_stream_entry(
                                                    &redis,
                                                    &stream_key,
                                                    &stream_entry_id,
                                                )
                                                .await;
                                            }
                                            Err(fallback_err) => {
                                                eprintln!(
                                                    "{} [live-trade] ave fallback sell failed token={}: {fallback_err:?}",
                                                    log_timestamp(),
                                                    token_label
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        println!(
                            "{} [live-trade] sell token={} amount_raw={} source={}",
                            log_timestamp(),
                            token_label,
                            amount_raw,
                            source
                        );
                        match trader.live_sell_amount_for_test(token, amount_raw).await {
                            Ok(hash) => {
                                println!(
                                    "{} [live-trade] sell submitted token={} tx={:#x}",
                                    log_timestamp(),
                                    token_label,
                                    hash
                                );
                                let _ = delete_stream_entry(
                                    &redis,
                                    &stream_key,
                                    &stream_entry_id,
                                )
                                .await;
                            }
                            Err(err) => {
                                eprintln!(
                                    "{} [live-trade] sell failed token={}: {err:?}",
                                    log_timestamp(),
                                    token_label
                                );
                                if allow_ave_fallback {
                                    if let (Some(client), Some(wallet), Some(wallet_address)) =
                                        (&ave_client, ave_wallet.as_ref(), ave_wallet_address)
                                    {
                                        let slippage_pct =
                                            cfg_snapshot.liquidity_trade().sell_sl_per_block_pct();
                                        match ave_sell_fallback(
                                            client,
                                            http_provider.clone(),
                                            wallet,
                                            wallet_address,
                                            token,
                                            amount_raw,
                                            slippage_pct,
                                        )
                                        .await
                                        {
                                            Ok(hash) => {
                                                println!(
                                                    "{} [live-trade] ave fallback sell submitted token={} tx={:#x}",
                                                    log_timestamp(),
                                                    token_label,
                                                    hash
                                                );
                                                let _ = delete_stream_entry(
                                                    &redis,
                                                    &stream_key,
                                                    &stream_entry_id,
                                                )
                                                .await;
                                            }
                                            Err(fallback_err) => {
                                                eprintln!(
                                                    "{} [live-trade] ave fallback sell failed token={}: {fallback_err:?}",
                                                    log_timestamp(),
                                                    token_label
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else {
                    eprintln!(
                        "{} [live-trade] unknown type={} token={}",
                        log_timestamp(),
                        kind,
                        token_label
                    );
                }
            }
        }
    }
}

fn map_get_string(map: &HashMap<String, redis::Value>, key: &str) -> Option<String> {
    map.get(key).and_then(|value| match value {
        redis::Value::Data(bytes) => String::from_utf8(bytes.clone()).ok(),
        redis::Value::Okay => Some("OK".to_string()),
        redis::Value::Int(value) => Some(value.to_string()),
        _ => None,
    })
}

fn parse_bool_value(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes"
    )
}

fn log_timestamp() -> String {
    chrono::Local::now().format("%d/%m/%y %H:%M:%S").to_string()
}

fn trade_method_label(method: TradeMethod) -> &'static str {
    match method {
        TradeMethod::Telegram => "telegram",
        TradeMethod::Wallet => "wallet",
    }
}

async fn resolve_token_label<C>(
    cache: &mut HashMap<Address, String>,
    provider: Arc<Provider<C>>,
    token: Address,
) -> String
where
    C: JsonRpcClient + 'static,
{
    if let Some(label) = cache.get(&token) {
        return label.clone();
    }
    let (name, symbol) = token_manager::fetch_token_name_symbol(provider, token)
        .await
        .unwrap_or((None, None));
    let label = match (name, symbol) {
        (_, Some(symbol)) if !symbol.is_empty() => symbol,
        (Some(name), _) if !name.is_empty() => name,
        _ => "unknown".to_string(),
    };
    cache.insert(token, label.clone());
    label
}

async fn publish_real_order_event(
    redis: &redis::Client,
    order_id: &ObjectId,
    token: Address,
    alert_type: &str,
    trade_method: TradeMethod,
) -> Result<()> {
    let mut conn = redis.get_multiplexed_async_connection().await?;
    let trade_method = trade_method_label(trade_method);
    let _: String = redis::cmd("XADD")
        .arg(ORDER_STREAM_KEY)
        .arg("*")
        .arg("type")
        .arg("real_order")
        .arg("order_id")
        .arg(order_id.to_hex())
        .arg("token")
        .arg(format!("{:#x}", token))
        .arg("alert_type")
        .arg(alert_type)
        .arg("trade_method")
        .arg(trade_method)
        .query_async(&mut conn)
        .await?;
    Ok(())
}

async fn increment_live_follow_counts(
    redis: &redis::Client,
    wallet: Address,
    token: Address,
) {
    let mut conn = match redis.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return,
    };
    let wallet_key = format!("{wallet:#x}");
    let token_key = format!("{token:#x}");
    let _: i64 = redis::cmd("HINCRBY")
        .arg("smart_follow:wallets:live")
        .arg(&wallet_key)
        .arg(1)
        .query_async(&mut conn)
        .await
        .unwrap_or(0);
    let _: i64 = redis::cmd("HINCRBY")
        .arg("smart_follow:tokens:live")
        .arg(&token_key)
        .arg(1)
        .query_async(&mut conn)
        .await
        .unwrap_or(0);
}

async fn delete_stream_entry(
    redis: &redis::Client,
    stream_key: &str,
    entry_id: &str,
) -> Result<()> {
    let mut conn = redis.get_multiplexed_async_connection().await?;
    let _: i64 = redis::cmd("XDEL")
        .arg(stream_key)
        .arg(entry_id)
        .query_async(&mut conn)
        .await?;
    Ok(())
}

async fn reset_live_trade_stream(redis: &redis::Client, stream_key: &str) -> Result<()> {
    let mut conn = redis.get_multiplexed_async_connection().await?;
    let _: i64 = redis::cmd("DEL").arg(stream_key).query_async(&mut conn).await?;
    println!(
        "{} [live-trade] reset stream={}",
        log_timestamp(),
        stream_key
    );
    Ok(())
}

fn smart_hold_enabled() -> bool {
    env_bool_value("SMART_HOLD_ENABLED", false)
}

fn env_bool_value(key: &str, default: bool) -> bool {
    env::var(key)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes"
            )
        })
        .unwrap_or(default)
}

async fn smart_wallet_zero_balance<C>(
    provider: Arc<Provider<C>>,
    token: Address,
    wallet: Address,
    token_label: &str,
) -> bool
where
    C: JsonRpcClient + 'static,
{
    let balance = match token_manager::fetch_token_balance(provider.clone(), token, wallet).await {
        Ok(value) => value,
        Err(err) => {
            eprintln!(
                "{} [smart-hold] failed to read smart wallet balance token={} wallet={:#x}: {err:?}",
                log_timestamp(),
                token_label,
                wallet
            );
            return false;
        }
    };
    let mut zero_balance = balance.is_zero();
    if !zero_balance {
        if let Ok(decimals) = token_manager::fetch_token_decimals(provider.clone(), token).await {
            if let Ok(value) = token_manager::u256_to_f64(balance, decimals) {
                if value <= 1e-6 {
                    zero_balance = true;
                }
            }
        }
    }
    zero_balance
}

fn wallet_token_field(wallet: Address, token: Address) -> String {
    format!("{wallet:#x}:{token:#x}")
}

async fn set_hold_last_price(
    redis: &redis::Client,
    wallet: Address,
    token: Address,
    price: f64,
) -> Result<()> {
    if price <= 0.0 {
        return Ok(());
    }
    let mut conn = redis.get_multiplexed_async_connection().await?;
    let field = wallet_token_field(wallet, token);
    let _: () = redis::cmd("HSET")
        .arg(SMART_HOLD_LAST_PRICE_KEY)
        .arg(&field)
        .arg(format!("{:.10}", price))
        .query_async(&mut conn)
        .await
        .unwrap_or(());
    Ok(())
}

async fn enqueue_or_merge_smart_hold(
    redis: &redis::Client,
    wallet: Address,
    token: Address,
    entry: &SmartHoldEntry,
) -> bool {
    let payload = match serde_json::to_string(entry) {
        Ok(value) => value,
        Err(_) => return false,
    };
    let mut conn = match redis.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return false,
    };
    let field = wallet_token_field(wallet, token);
    let queue_key = format!("{}{}", SMART_HOLD_QUEUE_PREFIX, field);
    let pending: i64 = redis::cmd("HGET")
        .arg(SMART_HOLD_PENDING_KEY)
        .arg(&field)
        .query_async(&mut conn)
        .await
        .unwrap_or(0);
    if pending > 0 {
        let last_raw: Option<String> = redis::cmd("LINDEX")
            .arg(&queue_key)
            .arg(-1)
            .query_async(&mut conn)
            .await
            .ok()
            .flatten();
        if let Some(raw) = last_raw {
            if let Ok(mut existing) = serde_json::from_str::<SmartHoldEntry>(&raw) {
                existing.buy_amount += entry.buy_amount;
                existing.smart_trade_bnb += entry.smart_trade_bnb;
                if existing.token_name.is_empty() {
                    existing.token_name = entry.token_name.clone();
                }
                if existing.token_symbol.is_empty() {
                    existing.token_symbol = entry.token_symbol.clone();
                }
                let merged = match serde_json::to_string(&existing) {
                    Ok(value) => value,
                    Err(_) => payload,
                };
                let _: () = redis::cmd("LSET")
                    .arg(&queue_key)
                    .arg(-1)
                    .arg(merged)
                    .query_async(&mut conn)
                    .await
                    .unwrap_or(());
                return true;
            }
        }
    }
    let _: i64 = redis::cmd("RPUSH")
        .arg(&queue_key)
        .arg(payload)
        .query_async(&mut conn)
        .await
        .unwrap_or(0);
    let _: i64 = redis::cmd("HINCRBY")
        .arg(SMART_HOLD_PENDING_KEY)
        .arg(&field)
        .arg(1)
        .query_async(&mut conn)
        .await
        .unwrap_or(0);
    true
}

async fn ave_sell_fallback(
    client: &AveClient,
    provider: Arc<RpcProvider>,
    wallet: &LocalWallet,
    wallet_address: Address,
    token: Address,
    amount: U256,
    slippage_pct: f64,
) -> Result<H256> {
    if amount.is_zero() {
        return Err(anyhow!("sell amount is zero"));
    }
    let settings = client.settings();
    let slippage_bps = pct_to_bps_string(slippage_pct);
    let payload = ChainWalletSwapPayload {
        chain: "bsc".into(),
        creator_address: format!("{:#x}", wallet_address),
        in_token_address: format!("{:#x}", token),
        out_token_address: NATIVE_TOKEN_ADDRESS.into(),
        in_amount: amount.to_string(),
        swap_type: "sell".into(),
        slippage: slippage_bps,
        use_mev: settings.use_mev,
        extra_gas: Some(settings.extra_gas.to_string()),
        auto_gas: settings.auto_gas.clone(),
        auto_slippage: settings.auto_slippage.then(|| true),
    };
    let spender_raw = settings
        .chain_wallet_address
        .as_deref()
        .context("AVE sell requires AVE_AI_CHAIN_WALLET_ADDRESS")?;
    let spender =
        Address::from_str(spender_raw).context("invalid AVE_AI_CHAIN_WALLET_ADDRESS")?;
    let signer = Arc::new(SignerMiddleware::new(provider.clone(), wallet.clone()));
    let erc20 = ERC20::new(token, signer.clone());
    let allowance = erc20
        .allowance(wallet_address, spender)
        .call()
        .await
        .context("failed to read allowance for AVE sell")?;
    if allowance < amount {
        println!(
            "{} [ave-fallback] allowance {} < amount {}. Approving spender {:#x}...",
            log_timestamp(),
            allowance,
            amount,
            spender
        );
        let approve_call = erc20.approve(spender, U256::MAX);
        let approve_pending = approve_call
            .send()
            .await
            .context("approve submission failed")?;
        let approve_hash = approve_pending.tx_hash();
        println!(
            "{} [ave-fallback] approve tx submitted: {approve_hash:?}, waiting...",
            log_timestamp()
        );
        let approve_receipt = approve_pending
            .await
            .context("approve tx dropped before confirmation")?
            .context("approve tx receipt missing")?;
        if approve_receipt.status != Some(U64::from(1)) {
            return Err(anyhow!("approve transaction reverted on-chain"));
        }
    }
    let tx_data = client
        .create_chain_wallet_tx(&payload)
        .await
        .context("AVE chain wallet request failed")?;
    sign_and_send_chain_wallet_tx(
        client,
        wallet,
        provider,
        settings.chain_wallet_address.as_deref(),
        wallet_address,
        wallet.chain_id(),
        "sell",
        slippage_pct,
        tx_data,
    )
    .await
}

async fn sign_and_send_chain_wallet_tx(
    client: &AveClient,
    wallet: &LocalWallet,
    provider: Arc<RpcProvider>,
    chain_wallet_address: Option<&str>,
    wallet_address: Address,
    chain_id: u64,
    swap_type: &str,
    slippage_pct: f64,
    tx_data: ChainWalletTx,
) -> Result<H256> {
    let slippage_bps = pct_to_bps_string(slippage_pct);
    let to_address_raw = tx_data
        .to_address
        .as_deref()
        .or(chain_wallet_address)
        .context("AVE missing toAddress: set AVE_AI_CHAIN_WALLET_ADDRESS")?;
    let to_address = Address::from_str(to_address_raw)
        .context("invalid AVE to address returned from createEvmTx")?;
    let tx_content = tx_data.tx_content.trim_start_matches("0x");
    let data = hex::decode(tx_content).context("failed to decode AVE txContent for signing")?;
    let gas_limit =
        U256::from_dec_str(&tx_data.gas_limit).context("failed to parse AVE gasLimit")?;
    let in_amount =
        U256::from_dec_str(&tx_data.in_amount).context("failed to parse AVE inAmount")?;
    let value = if swap_type.eq_ignore_ascii_case("buy") {
        in_amount
    } else {
        U256::zero()
    };
    let gas_price = provider
        .get_gas_price()
        .await
        .context("failed to read gas price for AVE transaction")?;
    let request = TransactionRequest::new()
        .to(to_address)
        .data(data)
        .gas(gas_limit)
        .value(value)
        .gas_price(gas_price);
    let mut typed: TypedTransaction = request.into();
    typed.set_chain_id(chain_id);
    let nonce = provider
        .get_transaction_count(wallet_address, Some(BlockNumber::Pending.into()))
        .await
        .context("failed to read nonce for AVE transaction")?;
    typed.set_nonce(nonce);
    let signature = wallet
        .sign_transaction(&typed)
        .await
        .context("failed to sign AVE chain wallet transaction")?;
    let tx_hash_hex = client
        .send_signed_chain_wallet_tx_bytes(
            "bsc",
            &tx_data.request_tx_id,
            typed.rlp_signed(&signature).as_ref(),
            client.settings().use_mev,
        )
        .await
        .context("failed to submit signed AVE transaction")?;
    let hash = H256::from_str(tx_hash_hex.trim_start_matches("0x"))
        .context("invalid AVE transaction hash returned")?;
    println!(
        "{} [ave-fallback] submitted {} tx hash={:#x} request={} slippage={}bps",
        log_timestamp(),
        swap_type,
        hash,
        tx_data.request_tx_id,
        slippage_bps
    );
    Ok(hash)
}

fn pct_to_bps_string(value: f64) -> String {
    let capped = value.clamp(0.0, 100.0);
    ((capped * 100.0).round() as u64).to_string()
}
