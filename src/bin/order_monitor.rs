use anyhow::{Context, Result, anyhow};
use chrono::{FixedOffset, Utc};
use ethers::providers::Middleware;
use ethers::{
    abi::{Abi, AbiParser},
    contract::Contract,
    middleware::SignerMiddleware,
    providers::{Provider, Ws},
    signers::{LocalWallet, Signer},
    types::{Address, Filter, H256, U256},
};
use futures_util::StreamExt;
use mongodb::bson::{Document, oid::ObjectId};
use redis::Client as RedisClient;
use redis::streams::StreamReadReply;
use serde_json::from_slice;
use sniper_fourmeme::{
    ave::AveClient,
    config::{Config, GmgnSettings, SimulationSettings, TradeMethod},
    gmgn::{GmgnClient, GmgnTokenInfo},
    listener::Listener,
    price::{build_price_context, read_pair_price},
    redis_utils::{build_redis_client, enqueue_live_trade},
    rpc_fallback::{RpcProvider, build_http_provider, rpc_urls_from_env},
    storage::MongoLogger,
    telegram_trade::TelegramTradeClient,
    token_manager,
    trader::{LiquidityPosition, Trader},
    types::TokenCreated,
};
use std::{collections::{HashMap, HashSet}, env, str::FromStr, sync::Arc, time::Duration};
use tokio::time::sleep;

type Client = SignerMiddleware<Arc<RpcProvider>, LocalWallet>;

const ORDER_STREAM_KEY: &str = "sniper:orders";
const LIVE_SELL_MAX_RETRIES: usize = 0;
const LIVE_SELL_RETRY_BACKOFF_SECS: u64 = 3;
const SMART_HOLD_PENDING_KEY: &str = "smart_hold:pending";
const SMART_HOLD_LAST_PRICE_KEY: &str = "smart_hold:last_price";
const SMART_HOLD_QUEUE_PREFIX: &str = "smart_hold:queue:";
const SMART_HOLD_READY_KEY: &str = "smart_hold:ready";

fn is_unexpected_close(err: &impl std::fmt::Debug) -> bool {
    format!("{err:?}").contains("UnexpectedClose")
}

fn live_wss_fallback() -> Option<String> {
    env::var("VOLUME_LIVE_WSS_URL2")
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn live_rpc_fallback() -> Option<String> {
    env::var("VOLUME_LIVE_RPC_URL2")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            env::var("VOLUME_LIVE_RPC_URL3")
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
}

async fn enqueue_live_sell(
    ctx: &MonitorContext,
    token: Address,
    amount: U256,
    source: &str,
    wallet: Address,
    force_ave: bool,
) -> Result<()> {
    let redis = ctx
        .redis
        .as_ref()
        .context("redis not configured for live_trade enqueue")?;
    let mut fields = vec![
        ("type", "sell".to_string()),
        ("token", format!("{:#x}", token)),
        ("amount_raw", amount.to_string()),
        ("source", source.to_string()),
        ("wallet", format!("{:#x}", wallet)),
    ];
    if force_ave {
        fields.push(("force_ave", "1".to_string()));
    }
    enqueue_live_trade(
        redis, fields,
    )
    .await?;
    Ok(())
}
const SMART_FOLLOW_WALLETS_LIVE_KEY: &str = "smart_follow:wallets:live";
const SMART_FOLLOW_TOKENS_LIVE_KEY: &str = "smart_follow:tokens:live";
const SMART_FOLLOW_WALLET_TOKEN_TOTAL_SIM_KEY: &str = "smart_follow:wallet_tokens_total:sim";

#[derive(Clone, Debug)]
struct RealOrder {
    id: ObjectId,
    token: Address,
    name: String,
    symbol: String,
    creator: Address,
    entry_price: f64,
    take_profit_price: f64,
    stop_loss_price: f64,
    price_source: String,
    alert_type: String,
    smart_wallet: Option<Address>,
    entry_value_usd: Option<f64>,
    token_amount: Option<f64>,
}

#[derive(Debug, Default, Clone, Copy)]
struct LiveSmartSellProgress {
    updated_orders: usize,
    closed_orders: usize,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
struct SimOrder {
    id: ObjectId,
    token: Address,
    name: String,
    symbol: String,
    entry_price: f64,
    take_profit_price: f64,
    stop_loss_price: f64,
    price_source: String,
    pair_address: Option<Address>,
    entry_value_usd: Option<f64>,
    smart_wallet: Option<Address>,
}

#[derive(Clone, Debug)]
struct SimRequest {
    token: Address,
    name: String,
    symbol: String,
    creator: Address,
    alert_type: String,
    min_liquidity: f64,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone)]
struct WalletContext {
    contract: Contract<Client>,
    wallet_address: Address,
}

#[derive(Clone)]
struct MonitorContext {
    ws_provider: Arc<Provider<Ws>>,
    reader_provider: Arc<RpcProvider>,
    token_manager_address: Address,
    helper_address: Address,
    pancake_v2_router: Address,
    pancake_v3_router: Address,
    wallet_main: Option<WalletContext>,
    wallet_tweet: Option<WalletContext>,
    telegram_main: Option<Arc<TelegramTradeClient>>,
    telegram_tweet: Option<Arc<TelegramTradeClient>>,
    redis: Option<RedisClient>,
    ave_client: Option<Arc<AveClient>>,
    mongo: Arc<MongoLogger>,
    gmgn_client: Option<Arc<GmgnClient>>,
    gmgn_settings: GmgnSettings,
    simulation: SimulationSettings,
    simulation_tweet: SimulationSettings,
    sell_all_liquidity: f64,
    sell_all_liquidity_tweet: f64,
    sell_all_migrated: bool,
    sell_all_migrated_tweet: bool,
    sell_sl_per_block_pct: f64,
    sell_sl_per_block_pct_tweet: f64,
    live_mode: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let mut cfg = Config::from_env();
    let live_mode = env::args().any(|arg| arg == "--live");
    let rpc_env_key = if live_mode {
        "VOLUME_LIVE_RPC_URL"
    } else {
        "VOLUME_RPC_URL"
    };
    if let Ok(rpc_url) = env::var(rpc_env_key) {
        if !rpc_url.trim().is_empty() {
            cfg.rpc_url = rpc_url;
        }
    }
    let mongo = MongoLogger::connect(cfg.mongo())
        .await
        .ok_or_else(|| anyhow!("mongo is required for monitor"))?;
    let redis = build_redis_client(cfg.redis()).ok();
    if let Some(client) = redis.as_ref() {
        cleanup_active_keys(Some(client), mongo.clone()).await;
        if live_mode {
            cleanup_live_watch_keys(Some(client)).await;
        }
    }

    let wss_env_key = if live_mode {
        "VOLUME_LIVE_WSS_URL"
    } else {
        "VOLUME_WSS_URL"
    };
    let wss_url = env::var(wss_env_key).unwrap_or_else(|_| cfg.wss_url.clone());
    println!("[startup] order_monitor wss={}", wss_url);
    println!("[startup] order_monitor rpc={}", cfg.rpc_url);
    let ws = Ws::connect(wss_url).await?;
    let ws_provider = Arc::new(Provider::new(ws));
    let rpc_fallback_keys = if live_mode {
        ["VOLUME_LIVE_RPC_URL2", "VOLUME_LIVE_RPC_URL3"].as_slice()
    } else {
        ["VOLUME_RPC_URL2", "VOLUME_RPC_URL3"].as_slice()
    };
    let rpc_urls = rpc_urls_from_env(cfg.rpc_url.clone(), rpc_fallback_keys);
    let reader_provider = Arc::new(
        build_http_provider(rpc_urls, Duration::from_millis(1500)).context("RPC_URL is invalid")?,
    );

    let (wallet_main, wallet_tweet) = build_wallet_contexts(&cfg, reader_provider.clone())?;
    let telegram_main = if cfg.trade_method() == TradeMethod::Telegram {
        TelegramTradeClient::connect(cfg.telegram_trade())
            .await
            .ok()
            .map(Arc::new)
    } else {
        None
    };
    let telegram_tweet = if cfg.trade_method_tweet() == TradeMethod::Telegram {
        TelegramTradeClient::connect(cfg.telegram_trade_tweet())
            .await
            .ok()
            .map(Arc::new)
    } else {
        None
    };

    let gmgn_client = if cfg.gmgn.enabled {
        GmgnClient::new(cfg.gmgn.clone()).ok()
    } else {
        None
    };
    let ave_client = if cfg.ave().is_enabled() {
        Some(Arc::new(AveClient::new(cfg.ave().clone())))
    } else {
        None
    };
    let ctx = MonitorContext {
        ws_provider,
        reader_provider,
        token_manager_address: cfg.token_manager_address,
        helper_address: cfg.helper3_address,
        pancake_v2_router: cfg.pancake_v2_router,
        pancake_v3_router: cfg.pancake_v3_router,
        wallet_main,
        wallet_tweet,
        telegram_main,
        telegram_tweet,
        redis,
        ave_client: ave_client.clone(),
        mongo,
        gmgn_client,
        gmgn_settings: cfg.gmgn.clone(),
        simulation: cfg.simulation().clone(),
        simulation_tweet: cfg.simulation_tweet().clone(),
        sell_all_liquidity: cfg.sell_all_liquidity(),
        sell_all_liquidity_tweet: cfg.sell_all_liquidity_tweet(),
        sell_all_migrated: cfg.sell_all_migrated(),
        sell_all_migrated_tweet: cfg.sell_all_migrated_tweet(),
        sell_sl_per_block_pct: cfg.liquidity_trade().sell_sl_per_block_pct(),
        sell_sl_per_block_pct_tweet: cfg.tweet_trade().sell_sl_per_block_pct(),
        live_mode,
    };

    let ctx = Arc::new(ctx);
    resume_open_orders(ctx.clone()).await?;
    spawn_smart_hold_monitor(ctx.clone());
    spawn_smart_sell_guard(ctx.clone());
    consume_stream(ctx.clone()).await?;
    Ok(())
}

async fn fetch_price_usd_prefer_helper(
    provider: Arc<Provider<Ws>>,
    helper_address: Address,
    token_manager_address: Address,
    token: Address,
) -> Option<f64> {
    if let Ok(info_opt) =
        token_manager::fetch_helper_info(provider.clone(), helper_address, token).await
    {
        if let Some(info) = info_opt {
            let mut candidate = token_manager::derive_pancake_pair(
                provider.clone(),
                info.pancake_factory,
                token,
                info.quote,
            )
            .await
            .ok()
            .flatten();
            if candidate.is_none() {
                candidate = info.pancake_pair;
            }
            if candidate.is_none() {
                candidate = token_manager::derive_pancake_pair(
                    provider.clone(),
                    info.pancake_v3_factory,
                    token,
                    info.quote,
                )
                .await
                .ok()
                .flatten();
            }
            if let Some(pair_addr) = candidate {
                if let Ok(context) = build_price_context(provider.clone(), pair_addr, token).await {
                    if let Ok(raw) = read_pair_price(&provider, &context).await {
                        if let Ok(usd) = token_manager::convert_price_to_usd(
                            &provider,
                            raw,
                            context.quote_address(),
                        )
                        .await
                        {
                            return Some(usd);
                        }
                    }
                }
            }
            if let Some(price) = info.last_price_usd {
                return Some(price);
            }
        }
    }

    match token_manager::fetch_last_price(provider, token_manager_address, token).await {
        Ok(Some(value)) => Some(value),
        _ => None,
    }
}

fn build_wallet_contexts(
    cfg: &Config,
    provider: Arc<RpcProvider>,
) -> Result<(Option<WalletContext>, Option<WalletContext>)> {
    let wallet_main: Option<WalletContext> = cfg
        .private_key
        .as_ref()
        .map(|key| {
            let wallet = key
                .parse::<LocalWallet>()
                .context("PRIVATE_KEY is invalid")?
                .with_chain_id(56u64);
            let client = Arc::new(SignerMiddleware::new(provider.clone(), wallet.clone()));
            let abi: Abi = from_slice(include_bytes!("../abi/TokenManager2.lite.abi"))
                .context("failed to parse TokenManager ABI")?;
            let contract = Contract::new(cfg.token_manager_address, abi, client);
            Ok::<WalletContext, anyhow::Error>(WalletContext {
                contract,
                wallet_address: wallet.address(),
            })
        })
        .transpose()?;

    let wallet_tweet: Option<WalletContext> = cfg
        .private_key_tweet
        .as_ref()
        .map(|key| {
            let wallet = key
                .parse::<LocalWallet>()
                .context("PRIVATE_KEY_TWEET is invalid")?
                .with_chain_id(56u64);
            let client = Arc::new(SignerMiddleware::new(provider.clone(), wallet.clone()));
            let abi: Abi = from_slice(include_bytes!("../abi/TokenManager2.lite.abi"))
                .context("failed to parse TokenManager ABI")?;
            let contract = Contract::new(cfg.token_manager_address, abi, client);
            Ok::<WalletContext, anyhow::Error>(WalletContext {
                contract,
                wallet_address: wallet.address(),
            })
        })
        .transpose()?;

    Ok((wallet_main, wallet_tweet))
}

async fn resume_open_orders(ctx: Arc<MonitorContext>) -> Result<()> {
    if ctx.live_mode {
        let open_real = ctx.mongo.find_open_real_orders().await?;
        for doc in open_real {
            if let Ok(order) = parse_real_order(&doc) {
                let ctx_clone = ctx.clone();
                tokio::spawn(async move {
                    if let Err(err) = monitor_real_order(ctx_clone, order).await {
                        eprintln!("[monitor] real order failed: {err:?}");
                    }
                });
            }
        }
    } else {
        let open_sim = ctx.mongo.find_open_sim_orders().await?;
        for doc in open_sim {
            if let Ok(order) = parse_sim_order(&doc) {
                let ctx_clone = ctx.clone();
                tokio::spawn(async move {
                    if let Err(err) = monitor_sim_order(ctx_clone, order).await {
                        eprintln!("[monitor] sim order failed: {err:?}");
                    }
                });
            }
        }
    }
    Ok(())
}

async fn consume_stream(ctx: Arc<MonitorContext>) -> Result<()> {
    let redis = match ctx.redis.as_ref() {
        Some(client) => client,
        None => return Ok(()),
    };
    let mut conn = redis.get_multiplexed_async_connection().await?;
    let mut last_id = "$".to_string();
    loop {
        let reply: StreamReadReply = redis::cmd("XREAD")
            .arg("BLOCK")
            .arg(1000)
            .arg("COUNT")
            .arg(100)
            .arg("STREAMS")
            .arg(ORDER_STREAM_KEY)
            .arg(&last_id)
            .query_async(&mut conn)
            .await?;

        for key in reply.keys {
            for entry in key.ids {
                last_id = entry.id.clone();
                let map = entry.map;
                if let Some(event_type) = map_get_string(&map, "type") {
                    match event_type.as_str() {
                        "real_order" if ctx.live_mode => {
                            if let Some(order_id) = map_get_string(&map, "order_id") {
                                if let Ok(order_id) = ObjectId::parse_str(order_id) {
                                    let ctx_clone = ctx.clone();
                                    tokio::spawn(async move {
                                        if let Err(err) =
                                            fetch_and_monitor_real(ctx_clone, order_id).await
                                        {
                                            eprintln!("[monitor] real order fetch failed: {err:?}");
                                        }
                                    });
                                }
                            }
                        }
                        "sim_request" if !ctx.live_mode => {
                            if let Some(req) = parse_sim_request(&map) {
                                let ctx_clone = ctx.clone();
                                tokio::spawn(async move {
                                    if let Err(err) = handle_sim_request(ctx_clone, req).await {
                                        eprintln!("[monitor] sim request failed: {err:?}");
                                    }
                                });
                            }
                        }
                        "sim_order" if !ctx.live_mode => {
                            if let Some(order_id) = map_get_string(&map, "order_id") {
                                if let Ok(order_id) = ObjectId::parse_str(order_id) {
                                    let ctx_clone = ctx.clone();
                                    tokio::spawn(async move {
                                        if let Err(err) =
                                            fetch_and_monitor_sim(ctx_clone, order_id).await
                                        {
                                            eprintln!("[monitor] sim order fetch failed: {err:?}");
                                        }
                                    });
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

async fn fetch_and_monitor_real(ctx: Arc<MonitorContext>, order_id: ObjectId) -> Result<()> {
    let doc = match ctx.mongo.find_real_order(order_id).await? {
        Some(doc) => doc,
        None => return Ok(()),
    };
    let order = parse_real_order(&doc)?;
    monitor_real_order(ctx, order).await
}

async fn fetch_and_monitor_sim(ctx: Arc<MonitorContext>, order_id: ObjectId) -> Result<()> {
    let doc = match ctx.mongo.find_sim_order(order_id).await? {
        Some(doc) => doc,
        None => return Ok(()),
    };
    let order = parse_sim_order(&doc)?;
    monitor_sim_order(ctx, order).await
}

async fn monitor_real_order(ctx: Arc<MonitorContext>, order: RealOrder) -> Result<()> {
    let redis = ctx.redis.clone();
    let active_key = format!("orders:active:{}", order.id.to_hex());
    update_active(redis.as_ref(), &active_key, "monitoring").await?;

    if order.alert_type == "SmartMoney Live" {
        if ctx.live_mode && !smart_money_live_buy_confirmed(ctx.as_ref(), &order).await? {
            eprintln!(
                "[{}] [order-monitor] smart buy not confirmed; continuing to monitor token={} order={}",
                now_timestamp_myt(),
                format_token_label(&order.name, &order.symbol, order.token),
                order.id
            );
        }
        return monitor_real_smart_order(ctx, order).await;
    }

    if order.alert_type == "Tweet Alert" && debug_enabled() {
        println!("----------------------");
    }
    if order.alert_type == "Tweet Alert" && order.price_source == "telegram" {
        update_active(redis.as_ref(), &active_key, "gmgn").await?;
        update_from_mongo(
            redis.as_ref(),
            &active_key,
            ctx.mongo.clone(),
            order.id,
            false,
        )
        .await?;
        return Ok(());
    }

    let (
        trade_method,
        telegram_trade,
        wallet_ctx,
        sell_sl_per_block_pct,
        sell_all_liquidity,
        sell_all_migrated,
    ) = resolve_trade_context(&ctx, &order);

    let position = LiquidityPosition {
        token: TokenCreated {
            creator: order.creator,
            token: order.token,
            name: order.name.clone(),
            symbol: order.symbol.clone(),
        },
        amount_raw: U256::zero(),
        amount_tokens: None,
        entry_price: order.entry_price,
        take_profit_price: order.take_profit_price,
        stop_loss_price: order.stop_loss_price,
        order_id: Some(order.id),
    };

    let positions = Arc::new(dashmap::DashMap::new());
    let wallet_ctx = wallet_ctx.ok_or_else(|| anyhow!("wallet context missing"))?;
    let telegram_delay = if order.alert_type == "Tweet Alert" {
        Duration::from_millis(100)
    } else {
        Duration::from_millis(0)
    };

    Trader::monitor_liquidity_position(
        ctx.ws_provider.clone(),
        wallet_ctx.contract.clone(),
        ctx.token_manager_address,
        ctx.helper_address,
        ctx.pancake_v2_router,
        ctx.pancake_v3_router,
        ctx.reader_provider.clone(),
        wallet_ctx.wallet_address,
        ctx.redis.clone(),
        ctx.live_mode,
        ctx.ave_client.clone(),
        Some(ctx.mongo.clone()),
        sell_sl_per_block_pct,
        trade_method,
        telegram_trade,
        56,
        position,
        positions,
        None,
        ctx.gmgn_client.clone(),
        sell_all_migrated,
        if sell_all_liquidity > 0.0 {
            Some(sell_all_liquidity)
        } else {
            None
        },
        telegram_delay,
    )
    .await?;

    update_from_mongo(
        redis.as_ref(),
        &active_key,
        ctx.mongo.clone(),
        order.id,
        false,
    )
    .await?;
    Ok(())
}

fn spawn_smart_hold_monitor(ctx: Arc<MonitorContext>) {
    let enabled = env_bool_value("SMART_HOLD_ENABLED", false);
    if !enabled {
        return;
    }
    if ctx.redis.is_none() {
        eprintln!(
            "[{}] [smart-hold] redis unavailable; hold monitor disabled",
            now_timestamp_myt()
        );
        return;
    }
    let drop_pct = env_f64_value("SMART_HOLD_DROP_PCT", 20.0).max(0.0);
    let check_secs = env_u64_value("SMART_HOLD_CHECK_SECS", 5).max(1);
    tokio::spawn(async move {
        let mut token_labels: HashMap<Address, String> = HashMap::new();
        let mut interval = tokio::time::interval(Duration::from_secs(check_secs));
        loop {
            interval.tick().await;
            let client = match ctx.redis.clone() {
                Some(client) => client,
                None => continue,
            };
            let mut conn = match client.get_multiplexed_async_connection().await {
                Ok(c) => c,
                Err(_) => continue,
            };
            let pending: HashMap<String, i64> = redis::cmd("HGETALL")
                .arg(SMART_HOLD_PENDING_KEY)
                .query_async(&mut conn)
                .await
                .unwrap_or_default();
            if pending.is_empty() {
                continue;
            }
            for (field, count) in pending {
                if count <= 0 {
                    continue;
                }
                let (wallet, token) = match parse_wallet_token_field(&field) {
                    Some(value) => value,
                    None => continue,
                };
                let balance =
                    token_manager::fetch_token_balance(ctx.reader_provider.clone(), token, wallet)
                        .await
                        .ok();
                if let Some(balance_value) = balance {
                    let mut zero_balance = balance_value.is_zero();
                    if !zero_balance {
                        if let Ok(decimals) =
                            token_manager::fetch_token_decimals(ctx.reader_provider.clone(), token)
                                .await
                        {
                            if let Ok(value) = token_manager::u256_to_f64(balance_value, decimals) {
                                if value <= 1e-6 {
                                    zero_balance = true;
                                }
                            }
                        }
                    }
                    if zero_balance {
                        clear_smart_hold(ctx.redis.clone(), wallet, token).await;
                        continue;
                    }
                }
                let last_raw: Option<String> = redis::cmd("HGET")
                    .arg(SMART_HOLD_LAST_PRICE_KEY)
                    .arg(&field)
                    .query_async(&mut conn)
                    .await
                    .ok()
                    .flatten();
                let last_price = match last_raw.and_then(|value| value.parse::<f64>().ok()) {
                    Some(price) if price > 0.0 => price,
                    _ => continue,
                };
                let current_price = fetch_price_usd_prefer_helper(
                    ctx.ws_provider.clone(),
                    ctx.helper_address,
                    ctx.token_manager_address,
                    token,
                )
                .await
                .unwrap_or(0.0);
                if current_price <= 0.0 {
                    continue;
                }
                let target_price = last_price * (1.0 - drop_pct / 100.0);
                if debug_enabled() {
                    let token_label = resolve_token_label_from_chain(
                        ctx.reader_provider.clone(),
                        token,
                        &mut token_labels,
                    )
                    .await;
                    println!(
                        "[{}] [smart-hold] monitoring token={} wallet={:#x} last={:.10} current={:.10} target={:.10} drop_pct={:.2}",
                        now_timestamp_myt(),
                        token_label,
                        wallet,
                        last_price,
                        current_price,
                        target_price,
                        drop_pct
                    );
                }
                if current_price > target_price {
                    continue;
                }
                let balance =
                    token_manager::fetch_token_balance(ctx.reader_provider.clone(), token, wallet)
                        .await
                        .ok();
                if let Some(balance_value) = balance {
                    let mut zero_balance = balance_value.is_zero();
                    if !zero_balance {
                        if let Ok(decimals) =
                            token_manager::fetch_token_decimals(ctx.reader_provider.clone(), token)
                                .await
                        {
                            if let Ok(value) = token_manager::u256_to_f64(balance_value, decimals) {
                                if value <= 1e-6 {
                                    zero_balance = true;
                                }
                            }
                        }
                    }
                    if zero_balance {
                        clear_smart_hold(ctx.redis.clone(), wallet, token).await;
                        let token_label = resolve_token_label_from_chain(
                            ctx.reader_provider.clone(),
                            token,
                            &mut token_labels,
                        )
                        .await;
                        println!(
                            "[{}] [smart-hold] release canceled: smart wallet sold all token={} wallet={:#x}",
                            now_timestamp_myt(),
                            token_label,
                            wallet
                        );
                        continue;
                    }
                }
                let queue_key = format!("{}{}", SMART_HOLD_QUEUE_PREFIX, field);
                let payload: Option<String> = redis::cmd("LPOP")
                    .arg(&queue_key)
                    .query_async(&mut conn)
                    .await
                    .ok()
                    .flatten();
                let payload = match payload {
                    Some(value) => value,
                    None => {
                        let _: () = redis::cmd("HDEL")
                            .arg(SMART_HOLD_PENDING_KEY)
                            .arg(&field)
                            .query_async(&mut conn)
                            .await
                            .unwrap_or(());
                        continue;
                    }
                };
                let mut entry: SmartHoldEntry = match serde_json::from_str(&payload) {
                    Ok(value) => value,
                    Err(_) => {
                        let _: i64 = redis::cmd("HINCRBY")
                            .arg(SMART_HOLD_PENDING_KEY)
                            .arg(&field)
                            .arg(-1)
                            .query_async(&mut conn)
                            .await
                            .unwrap_or(0);
                        continue;
                    }
                };
                entry.trigger_price = Some(current_price);
                let payload = match serde_json::to_string(&entry) {
                    Ok(value) => value,
                    Err(_) => continue,
                };
                let _: i64 = redis::cmd("LPUSH")
                    .arg(SMART_HOLD_READY_KEY)
                    .arg(payload)
                    .query_async(&mut conn)
                    .await
                    .unwrap_or(0);
                let _: () = redis::cmd("HSET")
                    .arg(SMART_HOLD_LAST_PRICE_KEY)
                    .arg(&field)
                    .arg(format!("{:.10}", current_price))
                    .query_async(&mut conn)
                    .await
                    .unwrap_or(());
                let remaining: i64 = redis::cmd("HINCRBY")
                    .arg(SMART_HOLD_PENDING_KEY)
                    .arg(&field)
                    .arg(-1)
                    .query_async(&mut conn)
                    .await
                    .unwrap_or(0);
                if remaining <= 0 {
                    let _: () = redis::cmd("HDEL")
                        .arg(SMART_HOLD_PENDING_KEY)
                        .arg(&field)
                        .query_async(&mut conn)
                        .await
                        .unwrap_or(());
                }
                let token_label = resolve_token_label_from_chain(
                    ctx.reader_provider.clone(),
                    token,
                    &mut token_labels,
                )
                .await;
                println!(
                    "[{}] [smart-hold] release queued token={} wallet={:#x} price={:.10} target={:.10}",
                    now_timestamp_myt(),
                    token_label,
                    wallet,
                    current_price,
                    target_price
                );
            }
        }
    });
}

fn spawn_smart_sell_guard(ctx: Arc<MonitorContext>) {
    if !ctx.live_mode {
        return;
    }
    let guard_secs = env_u64_value("SMART_SELL_GUARD_SECS", 60).max(1);
    let max_tries = env_u64_value("SMART_SELL_GUARD_MAX_TRIES", 3).max(1);
    if guard_secs == 0 {
        return;
    }
    tokio::spawn(async move {
        let mut attempts: HashMap<ObjectId, u64> = HashMap::new();
        let mut interval = tokio::time::interval(Duration::from_secs(guard_secs));
        loop {
            interval.tick().await;
            let open_orders = match ctx.mongo.find_open_real_orders().await {
                Ok(value) => value,
                Err(err) => {
                    eprintln!(
                        "[{}] [smart-sell-guard] failed to fetch open orders: {err:?}",
                        now_timestamp_myt()
                    );
                    continue;
                }
            };
            let mut open_ids: HashSet<ObjectId> = HashSet::new();
            for doc in open_orders {
                let order = match parse_real_order(&doc) {
                    Ok(value) => value,
                    Err(_) => continue,
                };
                open_ids.insert(order.id);
                if order.alert_type != "SmartMoney Live" {
                    continue;
                }
                let token_label = format_token_label(&order.name, &order.symbol, order.token);
                let smart_wallet = match order.smart_wallet {
                    Some(value) => value,
                    None => continue,
                };
                let tries = attempts.entry(order.id).or_insert(0);
                let balance = token_manager::fetch_token_balance(
                    ctx.reader_provider.clone(),
                    order.token,
                    smart_wallet,
                )
                .await
                .ok();
                let mut smart_zero = balance.map(|value| value.is_zero()).unwrap_or(false);
                let decimals = if smart_zero {
                    None
                } else {
                    token_manager::fetch_token_decimals(ctx.reader_provider.clone(), order.token)
                        .await
                        .ok()
                };
                if !smart_zero {
                    if let (Some(decimals), Some(value)) = (decimals, balance) {
                        if let Ok(amount) = token_manager::u256_to_f64(value, decimals) {
                            if amount <= 1e-6 {
                                smart_zero = true;
                            }
                        }
                    }
                }
                let balance_display = match (balance, decimals) {
                    (Some(value), Some(decimals)) => token_manager::u256_to_f64(value, decimals)
                        .ok()
                        .map(|amount| format!("{:.6}", amount))
                        .unwrap_or_else(|| value.to_string()),
                    (Some(value), None) => value.to_string(),
                    (None, _) => "n/a".into(),
                };
                println!(
                    "[{}] [smart-sell-guard] tick token={} smart_wallet={:#x} balance={} balance_raw={} zero={} tries={}/{}",
                    now_timestamp_myt(),
                    token_label,
                    smart_wallet,
                    balance_display,
                    balance
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "n/a".into()),
                    smart_zero,
                    *tries,
                    max_tries
                );
                if *tries >= max_tries {
                    continue;
                }
                if !smart_zero {
                    continue;
                }
                let wallet_ctx = match ctx.wallet_main.as_ref() {
                    Some(value) => value,
                    None => {
                        eprintln!(
                            "[{}] [smart-sell-guard] wallet context missing; cannot sell token={:#x}",
                            now_timestamp_myt(),
                            order.token
                        );
                        continue;
                    }
                };
                let migrated = is_token_migrated(&ctx, order.token).await;
                let force_ave = migrated;
                if let Err(err) = enqueue_live_sell(
                    ctx.as_ref(),
                    order.token,
                    U256::zero(),
                    "smart_sell_guard",
                    wallet_ctx.wallet_address,
                    force_ave,
                )
                .await
                {
                    eprintln!(
                        "[{}] [smart-sell-guard] failed to enqueue sell token={:#x}: {err:?}",
                        now_timestamp_myt(),
                        order.token
                    );
                    continue;
                }
                *tries += 1;
                println!(
                    "[{}] [smart-sell-guard] queued sell-all token={:#x} smart_wallet={:#x} migrated={} attempt={}/{}",
                    now_timestamp_myt(),
                    order.token,
                    smart_wallet,
                    migrated,
                    *tries,
                    max_tries
                );
            }
            attempts.retain(|id, _| open_ids.contains(id));
        }
    });
}

async fn monitor_real_smart_order(ctx: Arc<MonitorContext>, order: RealOrder) -> Result<()> {
    let token_label = format_token_label(&order.name, &order.symbol, order.token);
    let redis = ctx.redis.clone();
    let wallet = match order.smart_wallet {
        Some(value) => value,
        None => return Ok(()),
    };
    let watch_key = format!("orders:watching:live:{:#x}:{:#x}", order.token, wallet);
    if let Some(client) = redis.as_ref() {
        let mut conn = match client.get_multiplexed_async_connection().await {
            Ok(c) => c,
            Err(_) => return Ok(()),
        };
        let locked: bool = redis::cmd("SET")
            .arg(&watch_key)
            .arg(order.id.to_hex())
            .arg("NX")
            .arg("EX")
            .arg(3600)
            .query_async(&mut conn)
            .await
            .unwrap_or(false);
        if !locked {
            println!(
                "[{}] [order-monitor] smart sell (live) already watching token={} wallet={:#x}, skipping duplicate order={}",
                now_timestamp_myt(),
                token_label,
                wallet,
                order.id
            );
            return Ok(());
        }
    }
    let mut ws_provider = ctx.ws_provider.clone();
    let mut reader_provider = ctx.reader_provider.clone();
    let transfer_topic = H256::from(sniper_fourmeme::utils::keccak256_hex(
        "Transfer(address,address,uint256)",
    ));
    let filter = Filter::new()
        .address(order.token)
        .topic0(transfer_topic)
        .topic1(H256::from(wallet));
    let ws_provider_for_subs = ws_provider.clone();
    let mut stream = match ws_provider_for_subs.subscribe_logs(&filter).await {
        Ok(stream) => stream,
        Err(err) => {
            if ctx.live_mode && is_unexpected_close(&err) {
                if let Some(alt_wss) = live_wss_fallback() {
                    eprintln!(
                        "[{}] [order-monitor] subscribe failed with UnexpectedClose; swapping WSS to {}",
                        now_timestamp_myt(),
                        alt_wss
                    );
                    let ws = Ws::connect(alt_wss).await?;
                    ws_provider = Arc::new(Provider::new(ws));
                    if let Some(alt_rpc) = live_rpc_fallback() {
                        if let Ok(provider) =
                            build_http_provider(vec![alt_rpc.clone()], Duration::from_millis(1500))
                        {
                            reader_provider = Arc::new(provider);
                            eprintln!(
                                "[{}] [order-monitor] swapped RPC to {}",
                                now_timestamp_myt(),
                                alt_rpc
                            );
                        }
                    }
                    ws_provider.subscribe_logs(&filter).await?
                } else {
                    return Err(err.into());
                }
            } else {
                return Err(err.into());
            }
        }
    };
    println!(
        "[{}] [order-monitor] watching smart sell (live) for token={} wallet={:#x} order={}",
        now_timestamp_myt(),
        token_label,
        wallet,
        order.id
    );
    let token_decimals = token_manager::fetch_token_decimals(reader_provider.clone(), order.token)
        .await
        .ok();
    let mut initial_balance =
        token_manager::fetch_token_balance(reader_provider.clone(), order.token, wallet)
            .await
            .ok();
    if initial_balance.is_none() {
        for _ in 0..2 {
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            initial_balance =
                token_manager::fetch_token_balance(reader_provider.clone(), order.token, wallet)
                    .await
                    .ok();
            if initial_balance.is_some() {
                break;
            }
        }
    }
    let wallet_ctx = match ctx.wallet_main.clone() {
        Some(value) => value,
        None => {
            eprintln!(
                "[order-monitor] wallet context missing; cannot execute live sells for {}",
                token_label
            );
            return Ok(());
        }
    };
    if let Some(balance) = initial_balance {
        let mut zero_balance = balance.is_zero();
        if !zero_balance {
            if let Some(decimals) = token_decimals {
                if let Ok(value) = token_manager::u256_to_f64(balance, decimals) {
                    if value <= 1e-6 {
                        zero_balance = true;
                    }
                }
            }
        }
        if zero_balance {
            let wallet_balance = token_manager::fetch_token_balance(
                reader_provider.clone(),
                order.token,
                wallet_ctx.wallet_address,
            )
            .await
            .unwrap_or(U256::zero());
            let mut wallet_zero = wallet_balance.is_zero();
            if !wallet_zero {
                if let Some(decimals) = token_decimals {
                    if let Ok(value) = token_manager::u256_to_f64(wallet_balance, decimals) {
                        if value <= 1e-6 {
                            wallet_zero = true;
                        }
                    }
                }
            }
            if wallet_zero {
                let progress = apply_live_smart_sell_progress(
                    ctx.as_ref(),
                    order.token,
                    wallet,
                    100.0,
                    true,
                    None,
                    None,
                    None,
                    None,
                )
                .await?;
                println!(
                    "[{}] [order-monitor] smart sell live order closed (balance zero) token={} wallet={:#x} closed_orders={} updated_orders={}",
                    now_timestamp_myt(),
                    token_label,
                    wallet,
                    progress.closed_orders,
                    progress.updated_orders
                );
                clear_live_watch(redis.as_ref(), &watch_key).await;
                return Ok(());
            }
            clear_smart_hold(ctx.redis.clone(), wallet, order.token).await;
            println!(
                "[{}] [order-monitor] smart balance zero at startup; selling full wallet balance token={} wallet={:#x} amount={}",
                now_timestamp_myt(),
                token_label,
                wallet_ctx.wallet_address,
                wallet_balance
            );
            let mut attempt = 0usize;
            let mut submitted_hash: Option<H256> = None;
            loop {
                match enqueue_live_sell(
                    ctx.as_ref(),
                    order.token,
                    wallet_balance,
                    "order_monitor_startup",
                    wallet_ctx.wallet_address,
                    false,
                )
                .await
                {
                    Ok(()) => {
                        println!(
                            "[{}] [order-monitor] live sell queued (startup) token={} amount={} attempt={}/{}",
                            now_timestamp_myt(),
                            token_label,
                            wallet_balance,
                            attempt + 1,
                            LIVE_SELL_MAX_RETRIES + 1
                        );
                        submitted_hash = Some(H256::zero());
                        break;
                    }
                    Err(err) => {
                        if attempt >= LIVE_SELL_MAX_RETRIES {
                            eprintln!(
                                "[{}] [order-monitor] live sell failed (startup) token={}: {err:?}",
                                now_timestamp_myt(),
                                token_label
                            );
                            break;
                        }
                        eprintln!(
                            "[{}] [order-monitor] live sell failed (startup) token={}: {err:?} (retry {}/{}, backoff {}s)",
                            now_timestamp_myt(),
                            token_label,
                            attempt + 1,
                            LIVE_SELL_MAX_RETRIES + 1,
                            LIVE_SELL_RETRY_BACKOFF_SECS
                        );
                        sleep(Duration::from_secs(LIVE_SELL_RETRY_BACKOFF_SECS)).await;
                        attempt += 1;
                    }
                }
            }
            if let Some(hash) = submitted_hash {
                let exit_price = fetch_price_usd_prefer_helper(
                    ctx.ws_provider.clone(),
                    ctx.helper_address,
                    ctx.token_manager_address,
                    order.token,
                )
                .await;
                let exit_price_value = exit_price.unwrap_or(0.0);
                let progress = apply_live_smart_sell_progress(
                    ctx.as_ref(),
                    order.token,
                    wallet,
                    100.0,
                    true,
                    Some(&format!("{hash:#x}")),
                    Some(exit_price_value),
                    None,
                    None,
                )
                .await?;
                println!(
                    "[{}] [order-monitor] smart sell live order closed (startup smart balance zero) token={} wallet={:#x} tx={:#x} exit_price={:.10} closed_orders={} updated_orders={}",
                    now_timestamp_myt(),
                    token_label,
                    wallet,
                    hash,
                    exit_price_value,
                    progress.closed_orders,
                    progress.updated_orders
                );
                clear_live_watch(redis.as_ref(), &watch_key).await;
                return Ok(());
            }
            println!(
                "[{}] [order-monitor] startup sell failed; live order left open token={} wallet={:#x}",
                now_timestamp_myt(),
                token_label,
                wallet
            );
            clear_live_watch(redis.as_ref(), &watch_key).await;
            return Ok(());
        }
    }
    let mut sell_count = 0usize;
    let mut tx_sold_total: HashMap<String, U256> = HashMap::new();
    let mut tx_processed_pct: HashMap<String, f64> = HashMap::new();
    let mut hold_cleared = false;
    let mut sold_pct_for_us = 0.0f64;
    while let Some(log) = stream.next().await {
        let tx_hash = log
            .transaction_hash
            .map(|h| format!("{h:#x}"))
            .unwrap_or_else(|| "unknown".into());
        let block_number = log.block_number.map(|value| value.as_u64() as i64);
        let log_index = log.log_index.map(|value| value.as_u64() as i64);
        let sold_amount = if log.data.0.len() == 32 {
            U256::from_big_endian(&log.data.0)
        } else {
            U256::zero()
        };
        let sold_total = tx_sold_total
            .entry(tx_hash.clone())
            .and_modify(|total| *total = total.saturating_add(sold_amount))
            .or_insert(sold_amount);
        sell_count = sell_count.saturating_add(1);
        let mut balance_after =
            token_manager::fetch_token_balance(reader_provider.clone(), order.token, wallet)
                .await
                .ok();
        if balance_after.is_none() {
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            balance_after =
                token_manager::fetch_token_balance(reader_provider.clone(), order.token, wallet)
                    .await
                    .ok();
        }
        let sold_display = token_decimals
            .and_then(|decimals| token_manager::u256_to_f64(*sold_total, decimals).ok())
            .map(|value| format!("{:.6}", value))
            .unwrap_or_else(|| sold_total.to_string());
        let balance_display = token_decimals
            .and_then(|decimals| {
                balance_after.and_then(|value| token_manager::u256_to_f64(value, decimals).ok())
            })
            .map(|value| format!("{:.6}", value))
            .unwrap_or_else(|| {
                balance_after
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "n/a".into())
            });
        let balance_left_value = token_decimals
            .and_then(|decimals| {
                balance_after.and_then(|value| token_manager::u256_to_f64(value, decimals).ok())
            });
        let smart_wallet_zero = balance_left_value
            .map(|value| value <= 1e-6)
            .unwrap_or(false)
            || balance_after.map(|value| value.is_zero()).unwrap_or(false);
        let sold_pct = initial_balance
            .filter(|value| !value.is_zero())
            .and_then(|value| {
                let decimals = token_decimals.unwrap_or(0);
                let init = token_manager::u256_to_f64(value, decimals).ok()?;
                let sold = token_manager::u256_to_f64(*sold_total, decimals).ok()?;
                if init > 0.0 {
                    Some((sold / init) * 100.0)
                } else {
                    None
                }
            });
        if !hold_cleared {
            clear_smart_hold(ctx.redis.clone(), wallet, order.token).await;
            hold_cleared = true;
        }
        println!(
            "[{}] [order-monitor] smart sell observed (live) token={} wallet={:#x} tx={} sell_count={} sold={} sold_pct={} balance_left={}",
            now_timestamp_myt(),
            token_label,
            wallet,
            tx_hash,
            sell_count,
            sold_display,
            sold_pct
                .map(|v| format!("{:.2}%", v))
                .unwrap_or_else(|| "n/a".into()),
            balance_display
        );

        let prev_pct = tx_processed_pct.get(&tx_hash).copied().unwrap_or(0.0);
        let delta_pct = sold_pct
            .map(|value| (value - prev_pct).max(0.0))
            .unwrap_or(0.0);
        let mut effective_delta = 0.0;
        if smart_wallet_zero {
            if sold_pct_for_us < 100.0 - f64::EPSILON {
                effective_delta = 100.0 - sold_pct_for_us;
                sold_pct_for_us = 100.0;
            }
        } else if delta_pct > 0.0 {
            let new_total = (sold_pct_for_us + delta_pct).min(100.0);
            let delta = (new_total - sold_pct_for_us).max(0.0);
            sold_pct_for_us = new_total;
            effective_delta = delta;
        }
        let sell_pct_bps = if effective_delta > 0.0 {
            (effective_delta * 100.0).round() as u64
        } else {
            0
        };
        if sell_pct_bps > 0 {
            let wallet_balance = token_manager::fetch_token_balance(
                reader_provider.clone(),
                order.token,
                wallet_ctx.wallet_address,
            )
            .await
            .unwrap_or(U256::zero());
            if !wallet_balance.is_zero() {
                let sell_amount =
                    wallet_balance.saturating_mul(U256::from(sell_pct_bps)) / U256::from(10_000u64);
                if !sell_amount.is_zero() {
                    let mut attempt = 0usize;
                    loop {
                        match enqueue_live_sell(
                            ctx.as_ref(),
                            order.token,
                            sell_amount,
                            "order_monitor_live",
                            wallet_ctx.wallet_address,
                            false,
                        )
                        .await
                        {
                            Ok(()) => {
                                println!(
                                    "[{}] [order-monitor] live sell queued token={} amount={} attempt={}/{}",
                                    now_timestamp_myt(),
                                    token_label,
                                    sell_amount,
                                    attempt + 1,
                                    LIVE_SELL_MAX_RETRIES + 1
                                );
                                break;
                            }
                            Err(err) => {
                                if attempt >= LIVE_SELL_MAX_RETRIES {
                                    eprintln!(
                                        "[{}] [order-monitor] live sell failed token={}: {err:?}",
                                        now_timestamp_myt(),
                                        token_label
                                    );
                                    break;
                                }
                                eprintln!(
                                    "[{}] [order-monitor] live sell failed token={}: {err:?} (retry {}/{}, backoff {}s)",
                                    now_timestamp_myt(),
                                    token_label,
                                    attempt + 1,
                                    LIVE_SELL_MAX_RETRIES + 1,
                                    LIVE_SELL_RETRY_BACKOFF_SECS
                                );
                                sleep(Duration::from_secs(LIVE_SELL_RETRY_BACKOFF_SECS)).await;
                                attempt += 1;
                            }
                        }
                    }
                }
            }
        }
        if let Some(current_pct) = sold_pct {
            tx_processed_pct.insert(tx_hash.clone(), current_pct);
        }

        let mut zero_balance = balance_after.map(|v| v.is_zero()).unwrap_or(false);
        if !zero_balance {
            if let (Some(decimals), Some(value)) = (token_decimals, balance_after) {
                if let Ok(amount) = token_manager::u256_to_f64(value, decimals) {
                    if amount <= 1e-6 {
                        zero_balance = true;
                    }
                }
            }
        }
        let close_on_sell = smart_wallet_zero
            || sold_pct
                .map(|value| value >= 100.0 - f64::EPSILON)
                .unwrap_or(false);
        let exit_price = if close_on_sell || effective_delta > 0.0 {
            fetch_price_usd_prefer_helper(
                ctx.ws_provider.clone(),
                ctx.helper_address,
                ctx.token_manager_address,
                order.token,
            )
            .await
        } else {
            None
        };
        let exit_price_value = exit_price.unwrap_or(0.0);
        let progress = if close_on_sell {
            apply_live_smart_sell_progress(
                ctx.as_ref(),
                order.token,
                wallet,
                100.0,
                true,
                Some(&tx_hash),
                Some(exit_price_value),
                block_number,
                log_index,
            )
            .await?
        } else if effective_delta > 0.0 {
            apply_live_smart_sell_progress(
                ctx.as_ref(),
                order.token,
                wallet,
                effective_delta,
                false,
                Some(&tx_hash),
                Some(exit_price_value),
                block_number,
                log_index,
            )
            .await?
        } else {
            LiveSmartSellProgress::default()
        };
        if progress.updated_orders > 0 {
            println!(
                "[{}] [order-monitor] smart sell progress (live) token={} wallet={:#x} tx={} sell_delta={:.4}% updated_orders={} closed_orders={}",
                now_timestamp_myt(),
                token_label,
                wallet,
                tx_hash,
                if close_on_sell { 100.0 } else { effective_delta },
                progress.updated_orders,
                progress.closed_orders
            );
        }
        if zero_balance || close_on_sell {
            println!(
                "[{}] [order-monitor] smart sell live order closed token={} wallet={:#x} tx={} exit_price={:.10} closed_orders={}",
                now_timestamp_myt(),
                token_label,
                wallet,
                tx_hash,
                exit_price_value,
                progress.closed_orders
            );
            clear_live_watch(redis.as_ref(), &watch_key).await;
            break;
        }
    }
    clear_live_watch(redis.as_ref(), &watch_key).await;
    Ok(())
}

async fn clear_live_watch(redis: Option<&RedisClient>, key: &str) {
    let client = match redis {
        Some(client) => client,
        None => return,
    };
    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return,
    };
    let _: () = redis::cmd("DEL")
        .arg(key)
        .query_async(&mut conn)
        .await
        .unwrap_or(());
}

async fn cleanup_live_watch_keys(redis: Option<&RedisClient>) {
    let client = match redis {
        Some(client) => client,
        None => return,
    };
    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(err) => {
        eprintln!(
            "[{}] [order-monitor] failed to connect redis for cleanup: {err:?}",
            now_timestamp_myt()
        );
            return;
        }
    };
    let mut cursor: u64 = 0;
    let mut total = 0usize;
    loop {
        let res: Result<(u64, Vec<String>), _> = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg("orders:watching:live:*")
            .arg("COUNT")
            .arg(500)
            .query_async(&mut conn)
            .await;
        let (next, keys) = match res {
            Ok(value) => value,
            Err(err) => {
            eprintln!(
                "[{}] [order-monitor] redis scan failed: {err:?}",
                now_timestamp_myt()
            );
                break;
            }
        };
        if !keys.is_empty() {
            let _: Result<(), _> = redis::cmd("DEL").arg(&keys).query_async(&mut conn).await;
            total += keys.len();
        }
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    if total > 0 {
    println!(
        "[{}] [order-monitor] cleaned {} live watch keys",
        now_timestamp_myt(),
        total
    );
    }
}

async fn cleanup_active_keys(redis: Option<&RedisClient>, mongo: Arc<MongoLogger>) {
    let client = match redis {
        Some(client) => client,
        None => return,
    };
    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(err) => {
        eprintln!(
            "[{}] [order-monitor] failed to connect redis for cleanup: {err:?}",
            now_timestamp_myt()
        );
            return;
        }
    };
    let patterns = [("simulations:active:*", true), ("orders:active:*", false)];
    for (pattern, is_sim) in patterns {
        let mut cursor: u64 = 0;
        loop {
            let res: Result<(u64, Vec<String>), _> = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(200)
                .query_async(&mut conn)
                .await;
            let (next, keys) = match res {
                Ok(value) => value,
                Err(err) => {
            eprintln!(
                "[{}] [order-monitor] redis scan failed: {err:?}",
                now_timestamp_myt()
            );
                    break;
                }
            };
            cursor = next;
            for key in keys {
                let id_str = key.rsplit(':').next().unwrap_or("");
                let object_id = match ObjectId::parse_str(id_str) {
                    Ok(value) => value,
                    Err(_) => {
                        let _: Result<(), _> =
                            redis::cmd("DEL").arg(&key).query_async(&mut conn).await;
                        continue;
                    }
                };
                let doc = if is_sim {
                    mongo.find_sim_order(object_id).await
                } else {
                    mongo.find_real_order(object_id).await
                };
                match doc {
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        let _: Result<(), _> =
                            redis::cmd("DEL").arg(&key).query_async(&mut conn).await;
                    }
                    Err(err) => {
                eprintln!(
                    "[{}] [order-monitor] mongo lookup failed for {}: {err:?}",
                    now_timestamp_myt(),
                    key
                );
                    }
                }
            }
            if cursor == 0 {
                break;
            }
        }
    }
}

async fn apply_live_smart_sell_progress(
    ctx: &MonitorContext,
    token: Address,
    wallet: Address,
    sell_ratio_pct: f64,
    force_close: bool,
    tx_hash: Option<&str>,
    exit_price: Option<f64>,
    block_number: Option<i64>,
    log_index: Option<i64>,
) -> Result<LiveSmartSellProgress> {
    let mut progress = LiveSmartSellProgress::default();
    let open_orders = ctx.mongo.find_open_real_orders().await?;
    for doc in open_orders {
        let order = match parse_real_order(&doc) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if order.alert_type != "SmartMoney Live" {
            continue;
        }
        if order.token != token || order.smart_wallet != Some(wallet) {
            continue;
        }
        let total_tokens = order.token_amount.or_else(|| {
            if order.entry_price > 0.0 {
                order.entry_value_usd.map(|value| value / order.entry_price)
            } else {
                None
            }
        });
        let Some(total_tokens) = total_tokens.filter(|value| *value > f64::EPSILON) else {
            continue;
        };
        let mut remaining_tokens = doc_f64(&doc, "remaining_token_amount").unwrap_or(total_tokens);
        if remaining_tokens <= f64::EPSILON {
            continue;
        }
        let sell_tokens = if force_close {
            remaining_tokens
        } else {
            (remaining_tokens * (sell_ratio_pct / 100.0)).min(remaining_tokens)
        };
        if sell_tokens <= f64::EPSILON {
            continue;
        }
        remaining_tokens = (remaining_tokens - sell_tokens).max(0.0);
        let sold_total = (total_tokens - remaining_tokens).max(0.0);
        let sold_pct = if total_tokens > 0.0 {
            (sold_total / total_tokens * 100.0).min(100.0)
        } else {
            0.0
        };
        let close_order = force_close || remaining_tokens <= 1e-9;
        let amount_usd = exit_price
            .filter(|price| *price > 0.0)
            .map(|price| price * sell_tokens)
            .unwrap_or(0.0);
        let price_usd = exit_price.unwrap_or(0.0);
        if let Err(err) = ctx
            .mongo
            .record_order_tx(
                order.id,
                token,
                Some(wallet),
                "sell",
                price_usd,
                sell_tokens,
                amount_usd,
                tx_hash,
                block_number,
                log_index,
            )
            .await
        {
            eprintln!(
                "[{}] [order-monitor] failed to record order_tx order={} token={:#x}: {err:?}",
                now_timestamp_myt(),
                order.id,
                token
            );
        }
        let profit = if close_order {
            if let (Some(exit_price), Some(entry_value)) = (exit_price, order.entry_value_usd) {
                if exit_price > 0.0 && order.entry_price > 0.0 && entry_value > 0.0 {
                    Some((exit_price / order.entry_price - 1.0) * entry_value)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };
        ctx.mongo
            .update_real_order_smart_sell_progress(
                order.id,
                sell_tokens,
                remaining_tokens,
                sold_pct,
                if close_order { Some("smart_sell") } else { None },
                exit_price,
                profit,
                tx_hash,
            )
            .await?;
        let active_key = format!("orders:active:{}", order.id.to_hex());
        update_from_mongo(
            ctx.redis.as_ref(),
            &active_key,
            ctx.mongo.clone(),
            order.id,
            false,
        )
        .await?;
        progress.updated_orders = progress.updated_orders.saturating_add(1);
        if close_order {
            decrement_live_follow_counts(ctx.redis.clone(), wallet, token).await;
            decrement_smart_token_total(ctx.redis.clone(), wallet, token, 1).await;
            progress.closed_orders = progress.closed_orders.saturating_add(1);
        }
    }
    if force_close || progress.updated_orders > 0 {
        clear_smart_hold(ctx.redis.clone(), wallet, token).await;
    }
    Ok(progress)
}

async fn monitor_sim_order(ctx: Arc<MonitorContext>, order: SimOrder) -> Result<()> {
    let token_label = format_token_label(&order.name, &order.symbol, order.token);
    let redis = ctx.redis.clone();
    let active_key = format!("simulations:active:{}", order.id.to_hex());
    update_active(redis.as_ref(), &active_key, "monitoring").await?;

    // Smart-money follow sims: close when smart wallet sells (transfer out).
    if let Some(wallet) = order.smart_wallet {
        let token_decimals =
            token_manager::fetch_token_decimals(ctx.reader_provider.clone(), order.token)
                .await
                .ok();
        // On startup, if wallet balance is already zero (or dust), close immediately to avoid missing sells while monitor was down.
        let mut initial_balance =
            token_manager::fetch_token_balance(ctx.reader_provider.clone(), order.token, wallet)
                .await
                .ok();
        if initial_balance.is_none() {
            for _ in 0..2 {
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                initial_balance = token_manager::fetch_token_balance(
                    ctx.reader_provider.clone(),
                    order.token,
                    wallet,
                )
                .await
                .ok();
                if initial_balance.is_some() {
                    break;
                }
            }
        }
        if let Some(balance) = initial_balance {
            let mut zero_balance = balance.is_zero();
            if !zero_balance {
                if let Some(decimals) = token_decimals {
                    if let Ok(value) = token_manager::u256_to_f64(balance, decimals) {
                        if value <= 1e-6 {
                            zero_balance = true;
                        }
                    }
                }
            }
            if zero_balance {
                let exit_price = fetch_price_usd_prefer_helper(
                    ctx.ws_provider.clone(),
                    ctx.helper_address,
                    ctx.token_manager_address,
                    order.token,
                )
                .await;
                ctx.mongo
                    .complete_sim_order(
                        order.id,
                        "smart_sell",
                        exit_price,
                        None,
                        None,
                        Some(wallet),
                    )
                    .await?;
                clear_smart_hold(ctx.redis.clone(), wallet, order.token).await;
                decrement_smart_token_total(ctx.redis.clone(), wallet, order.token, 1).await;
                let wallet_label = wallet_name(ctx.mongo.clone(), wallet).await;
                println!(
                    "[{}] [order-monitor] smart sell sim closed (balance zero) token={} wallet={} exit_price={}",
                    now_timestamp_myt(),
                    token_label,
                    wallet_label,
                    exit_price
                        .map(|p| format!("{:.10}", p))
                        .unwrap_or_else(|| "n/a".into()),
                );
                update_from_mongo(
                    ctx.redis.as_ref(),
                    &active_key,
                    ctx.mongo.clone(),
                    order.id,
                    true,
                )
                .await?;
                return Ok(());
            }
        } else {
            println!(
                "[{}] [order-monitor] smart sell balance unavailable at startup token={} wallet={:#x} sim={}",
                now_timestamp_myt(),
                token_label,
                wallet,
                order.id
            );
        }
        let transfer_topic = H256::from(sniper_fourmeme::utils::keccak256_hex(
            "Transfer(address,address,uint256)",
        ));
        let filter = Filter::new()
            .address(order.token)
            .topic0(transfer_topic)
            .topic1(H256::from(wallet));
        let mut stream = ctx.ws_provider.subscribe_logs(&filter).await?;
        println!(
            "[{}] [order-monitor] watching smart sell for token={} wallet={:#x} sim={}",
            now_timestamp_myt(),
            token_label,
            wallet,
            order.id
        );
        let mut got_event = false;
        let mut sell_count = 0usize;
        let mut tx_sold_total: HashMap<String, U256> = HashMap::new();
        let mut hold_cleared = false;
        let mut sold_pct_for_us = 0.0f64;
        let mut sim_entry_value_usd = order.entry_value_usd;
        let mut sim_entry_price = if order.entry_price > 0.0 {
            Some(order.entry_price)
        } else {
            None
        };
        let mut last_profit_usd: Option<f64> = None;
        while !got_event {
            tokio::select! {
                Some(log) = stream.next() => {
                    let tx_hash = log
                        .transaction_hash
                        .map(|h| format!("{h:#x}"))
                        .unwrap_or_else(|| "unknown".into());
                    let sold_amount = if log.data.0.len() == 32 {
                        U256::from_big_endian(&log.data.0)
                    } else {
                        U256::zero()
                    };
                    let sold_total = tx_sold_total
                        .entry(tx_hash.clone())
                        .and_modify(|total| *total = total.saturating_add(sold_amount))
                        .or_insert(sold_amount);
                    sell_count = sell_count.saturating_add(1);
                    let mut balance_after = token_manager::fetch_token_balance(
                        ctx.reader_provider.clone(),
                        order.token,
                        wallet,
                    )
                    .await
                    .ok();
                    if balance_after.is_none() {
                        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                        balance_after = token_manager::fetch_token_balance(
                            ctx.reader_provider.clone(),
                            order.token,
                            wallet,
                        )
                        .await
                        .ok();
                    }
                    let sold_display = token_decimals
                        .and_then(|decimals| token_manager::u256_to_f64(*sold_total, decimals).ok())
                        .map(|value| format!("{:.6}", value))
                        .unwrap_or_else(|| sold_total.to_string());
                    let balance_display = token_decimals
                        .and_then(|decimals| {
                            balance_after
                                .and_then(|value| token_manager::u256_to_f64(value, decimals).ok())
                        })
                        .map(|value| format!("{:.6}", value))
                        .unwrap_or_else(|| balance_after.map(|v| v.to_string()).unwrap_or_else(|| "n/a".into()));
                    let sold_pct = initial_balance
                        .filter(|value| !value.is_zero())
                        .and_then(|value| {
                            let decimals = token_decimals.unwrap_or(0);
                            let init = token_manager::u256_to_f64(value, decimals).ok()?;
                            let sold = token_manager::u256_to_f64(*sold_total, decimals).ok()?;
                            if init > 0.0 {
                                Some((sold / init) * 100.0)
                            } else {
                                None
                            }
                        });
                    if !hold_cleared {
                        clear_smart_hold(ctx.redis.clone(), wallet, order.token).await;
                        hold_cleared = true;
                    }
                    println!(
                        "[{}] [order-monitor] smart sell observed token={} wallet={:#x} tx={} sell_count={} sold={} sold_pct={} balance_left={}",
                        now_timestamp_myt(),
                        token_label,
                        wallet,
                        tx_hash,
                        sell_count,
                        sold_display,
                        sold_pct.map(|v| format!("{:.2}%", v)).unwrap_or_else(|| "n/a".into()),
                        balance_display
                    );
                    let exit_price = fetch_price_usd_prefer_helper(
                        ctx.ws_provider.clone(),
                        ctx.helper_address,
                        ctx.token_manager_address,
                        order.token,
                    )
                    .await;
                    let exit_price_value = exit_price.unwrap_or(0.0);
                    if let Ok(Some(doc)) = ctx.mongo.find_sim_order(order.id).await {
                        sim_entry_value_usd = doc_f64(&doc, "entry_value_usd");
                        sim_entry_price = doc_f64(&doc, "entry_price").filter(|v| *v > 0.0);
                    }
                    let buy_pct = match (sim_entry_value_usd, sim_entry_price, token_decimals, initial_balance) {
                        (Some(entry_value_usd), Some(entry_price), Some(decimals), Some(balance)) => {
                            if entry_value_usd > 0.0 && entry_price > 0.0 {
                                let smart_tokens = token_manager::u256_to_f64(balance, decimals).ok();
                                smart_tokens
                                    .filter(|value| *value > 0.0)
                                    .map(|value| {
                                        let pct = (entry_value_usd / entry_price) / value * 100.0;
                                        pct.min(100.0)
                                    })
                            } else {
                                None
                            }
                        }
                        _ => None,
                    };
                    if exit_price_value > 0.0 {
                        let sell_pct_delta = initial_balance
                            .filter(|value| !value.is_zero())
                            .and_then(|value| {
                                let decimals = token_decimals.unwrap_or(0);
                                let init = token_manager::u256_to_f64(value, decimals).ok()?;
                                let sold = token_manager::u256_to_f64(sold_amount, decimals).ok()?;
                                if init > 0.0 {
                                    Some((sold / init) * 100.0)
                                } else {
                                    None
                                }
                            })
                            .unwrap_or(0.0);
                        let effective_delta = if let Some(target_pct) = buy_pct {
                            let new_total = (sold_pct_for_us + sell_pct_delta).min(target_pct);
                            let delta = (new_total - sold_pct_for_us).max(0.0);
                            sold_pct_for_us = new_total;
                            delta
                        } else if sold_pct_for_us <= f64::EPSILON {
                            sold_pct_for_us = 100.0;
                            100.0
                        } else {
                            0.0
                        };
                        if effective_delta > 0.0 {
                            if let (Some(entry_value_usd), Some(entry_price)) =
                                (sim_entry_value_usd, sim_entry_price)
                            {
                                if entry_value_usd > 0.0 && entry_price > 0.0 {
                                    let sim_total_tokens = entry_value_usd / entry_price;
                                    let sell_tokens = sim_total_tokens * (effective_delta / 100.0);
                                    let amount_usd = sell_tokens * exit_price_value;
                                    if sell_tokens > 0.0 && amount_usd > 0.0 {
                                        if let Ok(agg) = ctx
                                            .mongo
                                            .record_simulation_tx(
                                                order.id,
                                                order.token,
                                                Some(wallet),
                                                "sell",
                                                exit_price_value,
                                                sell_tokens,
                                                amount_usd,
                                                Some(&tx_hash),
                                                log.block_number.map(|v| v.as_u64() as i64),
                                                log.log_index.map(|v| v.as_u64() as i64),
                                            )
                                            .await
                                        {
                                            last_profit_usd = Some(agg.profit_usd);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    let mut zero_balance = balance_after.map(|v| v.is_zero()).unwrap_or(false);
                    if !zero_balance {
                        if let (Some(decimals), Some(value)) = (token_decimals, balance_after) {
                            if let Ok(amount) = token_manager::u256_to_f64(value, decimals) {
                                if amount <= 1e-6 {
                                    zero_balance = true;
                                }
                            }
                        }
                    }
                    let close_on_sell = match (sold_pct, buy_pct) {
                        (Some(value), Some(target)) => value >= target,
                        (Some(value), None) => value > 0.0,
                        (None, None) => true,
                        (None, Some(_)) => false,
                    };
                    if zero_balance || close_on_sell {
                        let profit = last_profit_usd;
                        ctx.mongo
                            .complete_sim_order(
                                order.id,
                                "smart_sell",
                                Some(exit_price_value),
                                profit,
                                Some(&tx_hash),
                                Some(wallet),
                            )
                            .await?;
                        clear_smart_hold(ctx.redis.clone(), wallet, order.token).await;
                        decrement_smart_token_total(ctx.redis.clone(), wallet, order.token, 1).await;
                        let profit_pct = if let (Some(value), Some(entry_value_usd)) =
                            (last_profit_usd, sim_entry_value_usd)
                        {
                            if entry_value_usd > 0.0 {
                                Some(value / entry_value_usd * 100.0)
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                        let wallet_label = wallet_name(ctx.mongo.clone(), wallet).await;
                        println!(
                            "[{}] [order-monitor] smart sell sim closed token={} wallet={} tx={} exit_price={:.10} profit_usd={} profit_pct={} sold_pct={} buy_pct={}",
                            now_timestamp_myt(),
                            token_label,
                            wallet_label,
                            tx_hash,
                            exit_price_value,
                            profit.map(|p| format!("{:.4}", p)).unwrap_or_else(|| "n/a".into()),
                            profit_pct.map(|p| format!("{:.2}%", p)).unwrap_or_else(|| "n/a".into()),
                            sold_pct.map(|v| format!("{:.2}%", v)).unwrap_or_else(|| "n/a".into()),
                            buy_pct.map(|v| format!("{:.2}%", v)).unwrap_or_else(|| "n/a".into())
                        );
                        got_event = true;
                    }
                }
                else => break,
            }
        }
        if !got_event {
            println!(
                "[{}] [order-monitor] smart sell stream ended without event token={} wallet={:#x} sim={}",
                now_timestamp_myt(),
                token_label,
                wallet,
                order.id
            );
        }
    } else {
        // No smart wallet associated; close immediately to avoid hanging sims.
        ctx.mongo
            .complete_sim_order(order.id, "skipped", None, None, None, None)
            .await?;
    }

    update_from_mongo(
        redis.as_ref(),
        &active_key,
        ctx.mongo.clone(),
        order.id,
        true,
    )
    .await?;
    Ok(())
}

async fn handle_sim_request(ctx: Arc<MonitorContext>, req: SimRequest) -> Result<()> {
    let settings = if req.alert_type == "Tweet Alert" {
        ctx.simulation_tweet.clone()
    } else {
        ctx.simulation.clone()
    };
    if !settings.enabled {
        return Ok(());
    }
    if ctx.mongo.has_sim_order(req.token).await? {
        return Ok(());
    }
    let client = match ctx.gmgn_client.as_ref() {
        Some(client) => client.clone(),
        None => return Ok(()),
    };
    let gmgn_info = client
        .wait_for_token(req.token, ctx.gmgn_settings.lookup_timeout())
        .await
        .ok_or_else(|| anyhow!("gmgn info unavailable"))?;
    if !passes_min_liquidity(&gmgn_info, req.min_liquidity) {
        return Ok(());
    }
    let entry_price = gmgn_info.price.unwrap_or(0.0);
    if entry_price <= 0.0 {
        return Ok(());
    }
    let helper_info =
        token_manager::fetch_helper_info(ctx.ws_provider.clone(), ctx.helper_address, req.token)
            .await
            .ok()
            .flatten();
    let price_feed = Listener::resolve_price_feed(
        ctx.ws_provider.clone(),
        req.token,
        gmgn_info.pool_address,
        helper_info,
        ctx.token_manager_address,
    )
    .await;
    let take_profit_price = if settings.take_profit_pct > 0.0 {
        Some(entry_price * (1.0 + settings.take_profit_pct / 100.0))
    } else {
        None
    };
    let stop_loss_price = if settings.stop_loss_pct > 0.0 {
        Some((entry_price * (1.0 - settings.stop_loss_pct / 100.0)).max(0.0))
    } else {
        None
    };
    let trade_amount_bnb = settings.trade_amount_bnb().max(0.0);
    let entry_value_usd = if trade_amount_bnb > 0.0 {
        token_manager::fetch_bnb_usd_price(&ctx.ws_provider)
            .await
            .ok()
            .map(|price| price * trade_amount_bnb)
    } else {
        None
    };
    let stored_tp = take_profit_price.unwrap_or(entry_price);
    let stored_sl = stop_loss_price.unwrap_or(entry_price);
    let price_source = price_feed.label();
    let order_id = ctx
        .mongo
        .create_sim_order(
            &TokenCreated {
                creator: req.creator,
                token: req.token,
                name: req.name.clone(),
                symbol: req.symbol.clone(),
            },
            price_feed.pair_address(),
            entry_price,
            stored_tp,
            stored_sl,
            price_source,
            trade_amount_bnb,
            entry_value_usd,
            None,
            None,
            None,
        )
        .await?;
    if let Some(value_usd) = entry_value_usd {
        if entry_price > 0.0 {
            let amount_tokens = value_usd / entry_price;
            if amount_tokens > 0.0 {
                let _ = ctx
                    .mongo
                    .record_simulation_tx(
                        order_id,
                        req.token,
                        None,
                        "buy",
                        entry_price,
                        amount_tokens,
                        value_usd,
                        None,
                        None,
                        None,
                    )
                    .await;
            }
        }
    }

    let order = SimOrder {
        id: order_id,
        token: req.token,
        name: req.name,
        symbol: req.symbol,
        entry_price,
        take_profit_price: stored_tp,
        stop_loss_price: stored_sl,
        price_source: price_source.to_string(),
        pair_address: price_feed.pair_address(),
        entry_value_usd,
        smart_wallet: None,
    };
    monitor_sim_order(ctx, order).await
}

fn passes_min_liquidity(info: &GmgnTokenInfo, min_liquidity: f64) -> bool {
    matches!(info.liquidity, Some(value) if value >= min_liquidity)
}

fn resolve_trade_context(
    ctx: &MonitorContext,
    order: &RealOrder,
) -> (
    TradeMethod,
    Option<Arc<TelegramTradeClient>>,
    Option<WalletContext>,
    f64,
    f64,
    bool,
) {
    let trade_method = match order.price_source.as_str() {
        "telegram" => TradeMethod::Telegram,
        "wallet" => TradeMethod::Wallet,
        _ => TradeMethod::Wallet,
    };
    let use_tweet = order.alert_type == "Tweet Alert";
    let telegram_trade = if use_tweet {
        ctx.telegram_tweet
            .clone()
            .or_else(|| ctx.telegram_main.clone())
    } else {
        ctx.telegram_main.clone()
    };
    let wallet_ctx = if use_tweet {
        ctx.wallet_tweet.clone().or_else(|| ctx.wallet_main.clone())
    } else {
        ctx.wallet_main.clone()
    };
    let sell_sl_per_block_pct = if use_tweet {
        ctx.sell_sl_per_block_pct_tweet
    } else {
        ctx.sell_sl_per_block_pct
    };
    let sell_all_liquidity = if use_tweet {
        ctx.sell_all_liquidity_tweet
    } else {
        ctx.sell_all_liquidity
    };
    let sell_all_migrated = if use_tweet {
        ctx.sell_all_migrated_tweet
    } else {
        ctx.sell_all_migrated
    };
    (
        trade_method,
        telegram_trade,
        wallet_ctx,
        sell_sl_per_block_pct,
        sell_all_liquidity,
        sell_all_migrated,
    )
}

fn parse_real_order(doc: &Document) -> Result<RealOrder> {
    let id = doc
        .get_object_id("_id")
        .context("missing order _id")?
        .to_owned();
    let token = parse_address(doc, "token")?;
    let name = doc.get_str("name").context("missing name")?.to_string();
    let symbol = doc.get_str("symbol").context("missing symbol")?.to_string();
    let creator = doc
        .get_str("creator")
        .ok()
        .and_then(|value| Address::from_str(value).ok())
        .unwrap_or_else(Address::zero);
    let entry_price = doc_f64(doc, "entry_price").unwrap_or(0.0);
    let take_profit_price = doc_f64(doc, "take_profit_price").unwrap_or(entry_price);
    let stop_loss_price = doc_f64(doc, "stop_loss_price").unwrap_or(entry_price);
    let price_source = doc.get_str("price_source").unwrap_or("wallet").to_string();
    let alert_type = doc
        .get_str("alert_type")
        .unwrap_or("Liquidity Alert")
        .to_string();
    let smart_wallet = doc
        .get_str("smart_wallet")
        .ok()
        .and_then(|value| Address::from_str(value).ok());
    let entry_value_usd = doc_f64(doc, "entry_value_usd");
    let token_amount = doc_f64(doc, "token_amount");
    Ok(RealOrder {
        id,
        token,
        name,
        symbol,
        creator,
        entry_price,
        take_profit_price,
        stop_loss_price,
        price_source,
        alert_type,
        smart_wallet,
        entry_value_usd,
        token_amount,
    })
}

async fn smart_money_live_buy_confirmed(ctx: &MonitorContext, order: &RealOrder) -> Result<bool> {
    if let Some(amount) = order.token_amount {
        if amount > f64::EPSILON {
            return Ok(true);
        }
    }
    let wallet_ctx = match ctx.wallet_main.as_ref() {
        Some(value) => value,
        None => return Ok(false),
    };
    let balance = token_manager::fetch_token_balance(
        ctx.reader_provider.clone(),
        order.token,
        wallet_ctx.wallet_address,
    )
    .await
    .unwrap_or(U256::zero());
    Ok(!balance.is_zero())
}

fn parse_sim_order(doc: &Document) -> Result<SimOrder> {
    let id = doc
        .get_object_id("_id")
        .context("missing sim _id")?
        .to_owned();
    let token = parse_address(doc, "token")?;
    let name = doc.get_str("name").context("missing name")?.to_string();
    let symbol = doc.get_str("symbol").context("missing symbol")?.to_string();
    let entry_price = doc_f64(doc, "entry_price").unwrap_or(0.0);
    let take_profit_price = doc_f64(doc, "take_profit_price").unwrap_or(entry_price);
    let stop_loss_price = doc_f64(doc, "stop_loss_price").unwrap_or(entry_price);
    let price_source = doc
        .get_str("price_source")
        .unwrap_or("token_manager")
        .to_string();
    let pair_address = doc
        .get_str("pair_address")
        .ok()
        .and_then(|value| Address::from_str(value).ok());
    let entry_value_usd = doc_f64(doc, "entry_value_usd");
    let smart_wallet = doc
        .get_str("smart_wallet")
        .ok()
        .and_then(|value| Address::from_str(value).ok());
    Ok(SimOrder {
        id,
        token,
        name,
        symbol,
        entry_price,
        take_profit_price,
        stop_loss_price,
        price_source,
        pair_address,
        entry_value_usd,
        smart_wallet,
    })
}

fn parse_sim_request(map: &HashMap<String, redis::Value>) -> Option<SimRequest> {
    let token = map_get_string(map, "token").and_then(|value| Address::from_str(&value).ok())?;
    let name = map_get_string(map, "name")?;
    let symbol = map_get_string(map, "symbol")?;
    let creator = map_get_string(map, "creator")
        .and_then(|value| Address::from_str(&value).ok())
        .unwrap_or_else(Address::zero);
    let alert_type = map_get_string(map, "alert_type").unwrap_or_else(|| "Liquidity Alert".into());
    let min_liquidity = map_get_string(map, "min_liquidity")
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(0.0);
    Some(SimRequest {
        token,
        name,
        symbol,
        creator,
        alert_type,
        min_liquidity,
    })
}

fn map_get_string(map: &HashMap<String, redis::Value>, key: &str) -> Option<String> {
    map.get(key)
        .and_then(|value| redis::from_redis_value::<String>(value).ok())
}

fn parse_address(doc: &Document, key: &str) -> Result<Address> {
    let value = doc.get_str(key).context(format!("missing {key}"))?;
    Address::from_str(value).context(format!("invalid address in {key}"))
}

fn doc_f64(doc: &Document, key: &str) -> Option<f64> {
    doc.get_f64(key)
        .ok()
        .or_else(|| doc.get_i64(key).ok().map(|value| value as f64))
}

async fn wallet_name(mongo: Arc<MongoLogger>, wallet: Address) -> String {
    if let Ok(Some(name)) = mongo.smart_money_name(wallet).await {
        return name;
    }
    format!("{:#x}", wallet)
}

fn format_token_label(name: &str, symbol: &str, token: Address) -> String {
    let token_addr = format!("{:#x}", token);
    if !is_placeholder_token_value(&token_addr, symbol) {
        return symbol.trim().to_string();
    }
    if !is_placeholder_token_value(&token_addr, name) {
        return name.trim().to_string();
    }
    token_addr
}

fn is_placeholder_token_value(token_addr: &str, value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return true;
    }
    let lower = trimmed.to_ascii_lowercase();
    lower == "unknown" || lower == token_addr.to_ascii_lowercase()
}

async fn resolve_token_label_from_chain(
    provider: Arc<RpcProvider>,
    token: Address,
    cache: &mut HashMap<Address, String>,
) -> String {
    if let Some(value) = cache.get(&token) {
        return value.clone();
    }
    let token_addr = format!("{:#x}", token);
    let label =
        if let Ok((name, symbol)) = token_manager::fetch_token_name_symbol(provider, token).await {
            if let Some(value) = symbol {
                if !is_placeholder_token_value(&token_addr, &value) {
                    value
                } else if let Some(value) = name {
                    if !is_placeholder_token_value(&token_addr, &value) {
                        value
                    } else {
                        token_addr
                    }
                } else {
                    token_addr
                }
            } else if let Some(value) = name {
                if !is_placeholder_token_value(&token_addr, &value) {
                    value
                } else {
                    token_addr
                }
            } else {
                token_addr
            }
        } else {
            token_addr
        };
    cache.insert(token, label.clone());
    label
}

async fn update_active(redis: Option<&RedisClient>, key: &str, status: &str) -> Result<()> {
    let redis = match redis {
        Some(client) => client,
        None => return Ok(()),
    };
    let mut conn = redis.get_multiplexed_async_connection().await?;
    let _: () = redis::cmd("HSET")
        .arg(key)
        .arg("status")
        .arg(status)
        .query_async(&mut conn)
        .await?;
    let _: () = redis::cmd("EXPIRE")
        .arg(key)
        .arg(120)
        .query_async(&mut conn)
        .await?;
    Ok(())
}

async fn update_from_mongo(
    redis: Option<&RedisClient>,
    key: &str,
    mongo: Arc<MongoLogger>,
    order_id: ObjectId,
    simulation: bool,
) -> Result<()> {
    let redis = match redis {
        Some(client) => client,
        None => return Ok(()),
    };
    let mut conn = redis.get_multiplexed_async_connection().await?;
    let doc = if simulation {
        mongo.find_sim_order(order_id).await?
    } else {
        mongo.find_real_order(order_id).await?
    };
    if let Some(doc) = doc {
        if let Ok(status) = doc.get_str("status") {
            if status == "new" {
                let _: () = redis::cmd("HSET")
                    .arg(key)
                    .arg("status")
                    .arg(status)
                    .query_async(&mut conn)
                    .await?;
                let _: () = redis::cmd("EXPIRE")
                    .arg(key)
                    .arg(120)
                    .query_async(&mut conn)
                    .await?;
            } else {
                let _: () = redis::cmd("DEL").arg(key).query_async(&mut conn).await?;
            }
        } else {
            let _: () = redis::cmd("DEL").arg(key).query_async(&mut conn).await?;
        }
    } else {
        let _: () = redis::cmd("DEL").arg(key).query_async(&mut conn).await?;
    }
    Ok(())
}

fn now_timestamp_myt() -> String {
    const MALAYSIA_OFFSET_SECS: i32 = 8 * 3600;
    const MALAYSIA_TIME_LABEL: &str = "MYT";
    let offset = FixedOffset::east_opt(MALAYSIA_OFFSET_SECS)
        .unwrap_or_else(|| FixedOffset::east_opt(MALAYSIA_OFFSET_SECS).expect("valid MYT offset"));
    let now = Utc::now().with_timezone(&offset);
    format!(
        "{} {}",
        now.format("%Y-%m-%d %H:%M:%S"),
        MALAYSIA_TIME_LABEL
    )
}

fn debug_enabled() -> bool {
    let raw = env::var("ORDER_MONITOR_DEBUG")
        .or_else(|_| env::var("DEBUG"))
        .unwrap_or_default();
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes"
    )
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

fn env_f64_value(key: &str, default: f64) -> f64 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(default)
}

fn env_u64_value(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

async fn is_token_migrated(ctx: &MonitorContext, token: Address) -> bool {
    let info = match token_manager::fetch_helper_info(
        ctx.ws_provider.clone(),
        ctx.helper_address,
        token,
    )
    .await
    {
        Ok(value) => value,
        Err(_) => None,
    };
    let Some(info) = info else {
        return false;
    };
    if info.liquidity_added || info.pancake_pair.is_some() {
        return true;
    }
    if let Ok(Some(pair)) = token_manager::derive_pancake_pair(
        ctx.ws_provider.clone(),
        info.pancake_factory,
        token,
        info.quote,
    )
    .await
    {
        if !pair.is_zero() {
            return true;
        }
    }
    if has_v3_pool(
        ctx.ws_provider.clone(),
        info.pancake_v3_factory,
        token,
        info.quote,
    )
    .await
    {
        return true;
    }
    false
}

async fn has_v3_pool(
    provider: Arc<Provider<Ws>>,
    factory: Address,
    token: Address,
    quote: Address,
) -> bool {
    if factory.is_zero() {
        return false;
    }
    let wbnb = if quote.is_zero() {
        Address::from_str("0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c")
            .unwrap_or_else(|_| Address::zero())
    } else {
        quote
    };
    if wbnb.is_zero() {
        return false;
    }
    let (token0, token1) = if token < wbnb {
        (token, wbnb)
    } else {
        (wbnb, token)
    };
    let abi = match AbiParser::default()
        .parse(&["function getPool(address,address,uint24) view returns (address)"])
    {
        Ok(value) => value,
        Err(_) => return false,
    };
    let contract = Contract::new(factory, abi, provider);
    for fee in [100u32, 500u32, 2500u32, 10000u32] {
        match contract.method::<_, Address>("getPool", (token0, token1, fee)) {
            Ok(call) => {
                if let Ok(pool) = call.call().await {
                    if !pool.is_zero() {
                        return true;
                    }
                }
            }
            Err(_) => continue,
        }
    }
    false
}

async fn decrement_live_follow_counts(redis: Option<RedisClient>, wallet: Address, token: Address) {
    let client = match redis {
        Some(client) => client,
        None => return,
    };
    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return,
    };
    let wallet_key = format!("{:#x}", wallet);
    let token_key = format!("{:#x}", token);
    let wallet_count: i64 = redis::cmd("HINCRBY")
        .arg(SMART_FOLLOW_WALLETS_LIVE_KEY)
        .arg(&wallet_key)
        .arg(-1)
        .query_async(&mut conn)
        .await
        .unwrap_or(0);
    if wallet_count <= 0 {
        let _: () = redis::cmd("HDEL")
            .arg(SMART_FOLLOW_WALLETS_LIVE_KEY)
            .arg(&wallet_key)
            .query_async(&mut conn)
            .await
            .unwrap_or(());
    }
    let token_count: i64 = redis::cmd("HINCRBY")
        .arg(SMART_FOLLOW_TOKENS_LIVE_KEY)
        .arg(&token_key)
        .arg(-1)
        .query_async(&mut conn)
        .await
        .unwrap_or(0);
    if token_count <= 0 {
        let _: () = redis::cmd("HDEL")
            .arg(SMART_FOLLOW_TOKENS_LIVE_KEY)
            .arg(&token_key)
            .query_async(&mut conn)
            .await
            .unwrap_or(());
    }
}

async fn decrement_smart_token_total(
    redis: Option<RedisClient>,
    wallet: Address,
    token: Address,
    amount: i64,
) {
    if amount <= 0 {
        return;
    }
    let client = match redis {
        Some(client) => client,
        None => return,
    };
    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return,
    };
    let field = wallet_token_field(wallet, token);
    let count: i64 = redis::cmd("HINCRBY")
        .arg(SMART_FOLLOW_WALLET_TOKEN_TOTAL_SIM_KEY)
        .arg(&field)
        .arg(-amount)
        .query_async(&mut conn)
        .await
        .unwrap_or(0);
    if count <= 0 {
        let _: () = redis::cmd("HDEL")
            .arg(SMART_FOLLOW_WALLET_TOKEN_TOTAL_SIM_KEY)
            .arg(&field)
            .query_async(&mut conn)
            .await
            .unwrap_or(());
    }
}

async fn clear_smart_hold(redis: Option<RedisClient>, wallet: Address, token: Address) {
    let client = match redis {
        Some(client) => client,
        None => return,
    };
    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return,
    };
    let field = wallet_token_field(wallet, token);
    let pending: i64 = redis::cmd("HGET")
        .arg(SMART_HOLD_PENDING_KEY)
        .arg(&field)
        .query_async(&mut conn)
        .await
        .unwrap_or(0);
    let queue_key = format!("{}{}", SMART_HOLD_QUEUE_PREFIX, field);
    let _: () = redis::cmd("DEL")
        .arg(&queue_key)
        .query_async(&mut conn)
        .await
        .unwrap_or(());
    let _: () = redis::cmd("HDEL")
        .arg(SMART_HOLD_PENDING_KEY)
        .arg(&field)
        .query_async(&mut conn)
        .await
        .unwrap_or(());
    let _: () = redis::cmd("HDEL")
        .arg(SMART_HOLD_LAST_PRICE_KEY)
        .arg(&field)
        .query_async(&mut conn)
        .await
        .unwrap_or(());
    if pending > 0 {
        let _ = decrement_smart_token_total(Some(client), wallet, token, pending).await;
    }
}

fn wallet_token_field(wallet: Address, token: Address) -> String {
    format!("{wallet:#x}:{token:#x}")
}

fn parse_wallet_token_field(field: &str) -> Option<(Address, Address)> {
    let mut parts = field.splitn(2, ':');
    let wallet = parts.next()?;
    let token = parts.next()?;
    let wallet_addr = Address::from_str(wallet).ok()?;
    let token_addr = Address::from_str(token).ok()?;
    Some((wallet_addr, token_addr))
}
