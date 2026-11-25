use crate::{
    config::{Config, GmgnSettings, LiquidityTradeSettings, SimulationSettings, TelegramSettings},
    gmgn::{GmgnClient, GmgnTokenInfo},
    price::{PairPriceContext, build_price_context, pair_contains_token, read_pair_price},
    redis_utils::{build_redis_client, clear_smart_buy_pending, set_smart_buy_pending},
    storage::MongoLogger,
    token_manager::{self, HelperPriceInfo},
    tweets::TweetService,
    types::{BotEvent, LiquidityTradeEvent, SmartMoneyTradeEvent, TokenCreated, TokenEvent},
    utils,
};
use anyhow::{Context, Result, anyhow};
use chrono::{FixedOffset, Local, TimeZone, Utc};
use dashmap::DashMap;
use ethers::{
    abi::AbiParser,
    contract::Contract,
    providers::{Middleware, Provider, Ws},
    types::{Address, Filter, H256, Log, U256},
};
use futures_util::{StreamExt, future};
use mongodb::bson::oid::ObjectId;
use once_cell::sync::Lazy;
use redis::Client as RedisClient;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{
    env, fs,
    path::Path,
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::mpsc::Sender;

fn is_unexpected_close(err: &impl std::fmt::Debug) -> bool {
    format!("{err:?}").contains("UnexpectedClose")
}

const TOKEN_CREATE_SIG: &str =
    "TokenCreate(address,address,uint256,string,string,uint256,uint256,uint256)";
const TOKEN_PURCHASE_SIG: &str =
    "TokenPurchase(address,address,uint256,uint256,uint256,uint256,uint256,uint256)";
const LIQUIDITY_ADDED_SIG: &str = "LiquidityAdded(address,uint256,address,uint256)";
const TRANSFER_SIG: &str = "Transfer(address,address,uint256)";
const DEFAULT_BUY_AMOUNT: f64 = 0.001;
const MALAYSIA_OFFSET_SECS: i32 = 8 * 3600;
const MALAYSIA_TIME_LABEL: &str = "MYT";
const ORDER_STREAM_KEY: &str = "sniper:orders";
const SMART_HOLD_PENDING_KEY: &str = "smart_hold:pending";
const SMART_HOLD_LAST_PRICE_KEY: &str = "smart_hold:last_price";
const SMART_HOLD_QUEUE_PREFIX: &str = "smart_hold:queue:";
const SMART_HOLD_READY_KEY: &str = "smart_hold:ready";
const SMART_FOLLOW_WALLETS_LIVE_KEY: &str = "smart_follow:wallets:live";
const SMART_FOLLOW_TOKENS_LIVE_KEY: &str = "smart_follow:tokens:live";
const SMART_FOLLOW_WALLETS_SIM_KEY: &str = "smart_follow:wallets:sim";
const SMART_FOLLOW_TOKENS_SIM_KEY: &str = "smart_follow:tokens:sim";
const SMART_FOLLOW_WALLET_TOKEN_TOTAL_SIM_KEY: &str = "smart_follow:wallet_tokens_total:sim";
static WBNB_ADDRESS: Lazy<Address> = Lazy::new(|| {
    Address::from_str("0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c").expect("wbnb address")
});
static SMART_TX_SEEN: Lazy<Mutex<HashSet<String>>> = Lazy::new(|| Mutex::new(HashSet::new()));

#[derive(Debug, Default, Deserialize, Clone)]
struct RawFilterConfig {
    #[serde(default)]
    ticker: Option<String>,
    #[serde(default)]
    creator_filter: Option<String>,
    #[serde(default)]
    name_filter: Option<String>,
    #[serde(default)]
    buy_amount: Option<String>,
}

#[derive(Debug, Clone)]
struct FilterConfig {
    inner: RawFilterConfig,
    buy_amount: f64,
    creator_addr: Option<Address>,
}

impl FilterConfig {
    fn load() -> Self {
        let inner: RawFilterConfig = fs::read_to_string(Path::new("ticker.json"))
            .ok()
            .and_then(|contents| serde_json::from_str(&contents).ok())
            .unwrap_or_default();

        let buy_amount = inner
            .buy_amount
            .as_deref()
            .and_then(|value| value.parse::<f64>().ok())
            .or_else(|| {
                env::var("TRADE_AMT")
                    .ok()
                    .and_then(|v| v.parse::<f64>().ok())
            })
            .unwrap_or(DEFAULT_BUY_AMOUNT);

        let creator_addr = inner
            .creator_filter
            .as_deref()
            .and_then(|value| Address::from_str(value).ok());

        Self {
            inner,
            buy_amount,
            creator_addr,
        }
    }

    fn buy_amount(&self) -> f64 {
        self.buy_amount
    }

    fn matches(&self, token: &TokenCreated) -> bool {
        if let Some(target) = self.inner.ticker.as_ref().filter(|s| !s.is_empty()) {
            if !equals_case_insensitive(&token.symbol, target) {
                return false;
            }
        }

        if let Some(expected_creator) = self.creator_addr {
            if token.creator != expected_creator {
                return false;
            }
        }

        if let Some(target) = self.inner.name_filter.as_ref().filter(|s| !s.is_empty()) {
            if !token.name.to_lowercase().contains(&target.to_lowercase()) {
                return false;
            }
        }

        true
    }
}

fn equals_case_insensitive(left: &str, right: &str) -> bool {
    if left.eq_ignore_ascii_case(right) {
        return true;
    }
    left.to_lowercase() == right.to_lowercase()
}

#[derive(Clone)]
pub enum SimulationPriceFeed {
    Pair {
        context: PairPriceContext,
        manager_address: Address,
    },
    TokenManager {
        manager_address: Address,
    },
}

impl SimulationPriceFeed {
    pub fn label(&self) -> &'static str {
        match self {
            SimulationPriceFeed::Pair { .. } => "pair",
            SimulationPriceFeed::TokenManager { .. } => "token_manager",
        }
    }

    pub fn pair_address(&self) -> Option<Address> {
        match self {
            SimulationPriceFeed::Pair { context, .. } => Some(context.pair_address()),
            SimulationPriceFeed::TokenManager { .. } => None,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct TxLiquidity {
    bnb: f64,
    usd: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    trigger_price: Option<f64>,
}

#[derive(Clone)]
pub struct Listener {
    provider: Arc<Provider<Ws>>,
    tx: Sender<BotEvent>,
    tweet_tx: Option<Sender<BotEvent>>,
    filter: FilterConfig,
    enable_liquidity_monitor: bool,
    enable_liquidity_alerts: bool,
    factory_address: Address,
    token_manager_address: Address,
    helper_address: Address,
    flap_router: Address,
    dex_router: Address,
    token_create_topic: H256,
    gmgn_client: Option<Arc<GmgnClient>>,
    gmgn_settings: GmgnSettings,
    min_candidate_liquidity: f64,
    min_volume_usd: f64,
    min_tweet_liquidity: f64,
    telegram: TelegramSettings,
    tweet_service: Option<Arc<TweetService>>,
    mongo: Option<Arc<MongoLogger>>,
    redis: Option<RedisClient>,
    simulation: SimulationSettings,
    simulation_tweet: SimulationSettings,
    liquidity_trade: LiquidityTradeSettings,
    tweet_trade: LiquidityTradeSettings,
    enable_token_buy: bool,
    enable_buy_tokenamap: bool,
    enable_add_liquidity: bool,
    enable_smart_wallet: bool,
    live_mode: bool,
    skip_tokens: Arc<HashSet<Address>>,
    max_smart_follow: usize,
    max_token_follow: usize,
    max_smart_token_follow: usize,
    smart_hold_enabled: bool,
    smart_hold_drop_pct: f64,
    active_trades: Option<Arc<AtomicUsize>>,
    token_cache: Arc<DashMap<Address, TokenCreated>>,
    liquidity_trade_seen: Arc<DashMap<Address, ()>>,
}

impl Listener {
    fn now_timestamp_myt() -> String {
        let offset = FixedOffset::east_opt(MALAYSIA_OFFSET_SECS).unwrap_or_else(|| {
            FixedOffset::east_opt(MALAYSIA_OFFSET_SECS).expect("valid MYT offset") // fallback for older chrono
        });
        let now = Utc::now().with_timezone(&offset);
        format!(
            "{} {}",
            now.format("%Y-%m-%d %H:%M:%S"),
            MALAYSIA_TIME_LABEL
        )
    }

    pub fn new(
        provider: Arc<Provider<Ws>>,
        tx: Sender<BotEvent>,
        tweet_tx: Option<Sender<BotEvent>>,
        enable_liquidity_monitor: bool,
        enable_liquidity_alerts: bool,
        enable_smart_wallet: bool,
        live_mode: bool,
        cfg: Config,
        gmgn_client: Option<Arc<GmgnClient>>,
        mongo: Option<Arc<MongoLogger>>,
        active_trades: Option<Arc<AtomicUsize>>,
    ) -> Self {
        let filter = FilterConfig::load();
        let token_create_topic = H256::from(utils::keccak256_hex(TOKEN_CREATE_SIG));
        let max_smart_follow = std::env::var("MAX_SMART_FOLLOW")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let max_token_follow = std::env::var("MAX_TOKEN_FOLLOW")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let max_smart_token_follow = env_usize("MAX_SMART_TOKEN_FOLLOW", 0);
        let smart_hold_enabled = env_bool_value("SMART_HOLD_ENABLED", false);
        let smart_hold_drop_pct = env_f64_value("SMART_HOLD_DROP_PCT", 20.0).max(0.0);
        Self {
            provider,
            tx,
            tweet_tx,
            filter,
            enable_liquidity_monitor,
            enable_liquidity_alerts,
            factory_address: cfg.factory_address,
            token_manager_address: cfg.token_manager_address,
            helper_address: cfg.helper3_address,
            flap_router: cfg.flap_router,
            dex_router: cfg.dex_router,
            token_create_topic,
            gmgn_client,
            gmgn_settings: cfg.gmgn.clone(),
            min_candidate_liquidity: cfg.min_candidate_liquidity(),
            min_volume_usd: cfg.min_volume_usd(),
            min_tweet_liquidity: cfg.min_tweet_liquidity(),
            telegram: cfg.telegram().clone(),
            tweet_service: TweetService::new(cfg.redis(), cfg.gemini().clone(), mongo.clone())
                .map(Arc::new),
            mongo,
            redis: build_redis_client(cfg.redis()).ok(),
            simulation: cfg.simulation().clone(),
            simulation_tweet: cfg.simulation_tweet().clone(),
            liquidity_trade: cfg.liquidity_trade().clone(),
            tweet_trade: cfg.tweet_trade().clone(),
            enable_token_buy: cfg.enable_token_buy(),
            enable_buy_tokenamap: cfg.enable_buy_tokenamap(),
            enable_add_liquidity: cfg.enable_add_liquidity(),
            enable_smart_wallet,
            live_mode,
            skip_tokens: Arc::new(cfg.skip_tokens.clone()),
            max_smart_follow,
            max_token_follow,
            max_smart_token_follow,
            smart_hold_enabled,
            smart_hold_drop_pct,
            active_trades,
            token_cache: Arc::new(DashMap::new()),
            liquidity_trade_seen: Arc::new(DashMap::new()),
        }
    }

    pub async fn run(mut self) -> Result<()> {
        if self.enable_smart_wallet {
            self.spawn_smart_wallet_transfer_listener();
        }
        if self.enable_smart_wallet && self.smart_hold_enabled {
            self.spawn_smart_hold_executor();
        }
        if self.enable_liquidity_monitor {
            println!("👂 Listener subscribed to TokenCreate events");
            let filter = Filter::new()
                .address(self.factory_address)
                .topic0(self.token_create_topic);
            let provider = self.provider.clone();
            let mut stream = provider.subscribe_logs(&filter).await?;
            self.spawn_token_manager_monitors();

            while let Some(log) = stream.next().await {
                if let Err(err) = self.process_log(log).await {
                    eprintln!("[listener] failed to process log: {err:?}");
                }
            }
        } else {
            println!("👂 Listener skipping TokenCreate/liquidity monitors (volume mode)");
            future::pending::<()>().await;
        }

        Ok(())
    }

    fn spawn_token_manager_monitors(&self) {
        let provider = self.provider.clone();
        let token_manager_address = self.token_manager_address;
        let helper_address = self.helper_address;
        let token_create_topic = self.token_create_topic;
        let token_purchase_topic = H256::from(utils::keccak256_hex(TOKEN_PURCHASE_SIG));
        let liquidity_added_topic = H256::from(utils::keccak256_hex(LIQUIDITY_ADDED_SIG));
        let min_liquidity = self.min_candidate_liquidity;
        let telegram = self.telegram.clone();
        let mongo = self.mongo.clone();
        let trade_tx = self.tx.clone();
        let liquidity_trade = self.liquidity_trade.clone();
        let active_trades = self.active_trades.clone();
        let token_cache = self.token_cache.clone();
        let liquidity_trade_seen = self.liquidity_trade_seen.clone();
        let liquidity_trade_seen_create = liquidity_trade_seen.clone();
        let enable_liquidity_alerts = self.enable_liquidity_alerts;

        tokio::spawn(async move {
            let filter = Filter::new()
                .address(token_manager_address)
                .topic0(token_create_topic);
            let mut stream = match provider.subscribe_logs(&filter).await {
                Ok(value) => value,
                Err(err) => {
                    eprintln!("[listener] failed to subscribe token manager TokenCreate: {err:?}");
                    return;
                }
            };

            while let Some(log) = stream.next().await {
                let token_created = match utils::parse_token_created_log(&log) {
                    Ok(token) => token,
                    Err(err) => {
                        eprintln!("[listener] failed to parse token manager TokenCreate: {err:?}");
                        continue;
                    }
                };
                token_cache.insert(token_created.token, token_created.clone());
                Self::handle_token_manager_liquidity_event(
                    token_created,
                    log,
                    provider.clone(),
                    telegram.clone(),
                    mongo.clone(),
                    min_liquidity,
                    liquidity_trade.clone(),
                    trade_tx.clone(),
                    active_trades.clone(),
                    liquidity_trade_seen_create.clone(),
                    None,
                    "token_manager:createToken:tx_value".to_string(),
                    enable_liquidity_alerts,
                )
                .await;
            }
        });

        if self.enable_buy_tokenamap {
            let provider = self.provider.clone();
            let token_manager_address = self.token_manager_address;
            let min_liquidity = self.min_candidate_liquidity;
            let telegram = self.telegram.clone();
            let mongo = self.mongo.clone();
            let trade_tx = self.tx.clone();
            let liquidity_trade = self.liquidity_trade.clone();
            let active_trades = self.active_trades.clone();
            let token_cache = self.token_cache.clone();
            let liquidity_trade_seen_purchase = self.liquidity_trade_seen.clone();

            tokio::spawn(async move {
                let filter = Filter::new()
                    .address(token_manager_address)
                    .topic0(token_purchase_topic);
                let mut stream = match provider.subscribe_logs(&filter).await {
                    Ok(value) => value,
                    Err(err) => {
                        eprintln!(
                            "[listener] failed to subscribe token manager TokenPurchase: {err:?}"
                        );
                        return;
                    }
                };

                while let Some(log) = stream.next().await {
                    let token = match utils::parse_token_purchase_log(&log) {
                        Ok(value) => value,
                        Err(err) => {
                            eprintln!(
                                "[listener] failed to parse token manager TokenPurchase: {err:?}"
                            );
                            continue;
                        }
                    };
                    let token_created = Self::lookup_cached_token(&token_cache, token);
                    Self::handle_token_manager_liquidity_event(
                        token_created,
                        log,
                        provider.clone(),
                        telegram.clone(),
                        mongo.clone(),
                        min_liquidity,
                        liquidity_trade.clone(),
                        trade_tx.clone(),
                        active_trades.clone(),
                        liquidity_trade_seen_purchase.clone(),
                        None,
                        "token_manager:buyTokenAMAP:tx_value".to_string(),
                        enable_liquidity_alerts,
                    )
                    .await;
                }
            });
        }

        if self.enable_add_liquidity {
            let provider = self.provider.clone();
            let token_manager_address = self.token_manager_address;
            let min_liquidity = self.min_candidate_liquidity;
            let telegram = self.telegram.clone();
            let mongo = self.mongo.clone();
            let trade_tx = self.tx.clone();
            let liquidity_trade = self.liquidity_trade.clone();
            let active_trades = self.active_trades.clone();
            let token_cache = self.token_cache.clone();
            let liquidity_trade_seen_liquidity = self.liquidity_trade_seen.clone();

            tokio::spawn(async move {
                let filter = Filter::new()
                    .address(token_manager_address)
                    .topic0(liquidity_added_topic);
                let mut stream = match provider.subscribe_logs(&filter).await {
                    Ok(value) => value,
                    Err(err) => {
                        eprintln!(
                            "[listener] failed to subscribe token manager LiquidityAdded: {err:?}"
                        );
                        return;
                    }
                };

                while let Some(log) = stream.next().await {
                    let liquidity_log = match utils::parse_liquidity_added_log(&log) {
                        Ok(value) => value,
                        Err(err) => {
                            eprintln!(
                                "[listener] failed to parse token manager LiquidityAdded: {err:?}"
                            );
                            continue;
                        }
                    };
                    let token_created = Self::lookup_cached_token(&token_cache, liquidity_log.base);
                    let liquidity_override = Self::build_liquidity_from_liquidity_added(
                        provider.clone(),
                        helper_address,
                        liquidity_log.base,
                        liquidity_log.quote,
                        liquidity_log.funds,
                    )
                    .await;
                    Self::handle_token_manager_liquidity_event(
                        token_created,
                        log,
                        provider.clone(),
                        telegram.clone(),
                        mongo.clone(),
                        min_liquidity,
                        liquidity_trade.clone(),
                        trade_tx.clone(),
                        active_trades.clone(),
                        liquidity_trade_seen_liquidity.clone(),
                        liquidity_override,
                        "token_manager:liquidityAdded:tx_value".to_string(),
                        enable_liquidity_alerts,
                    )
                    .await;
                }
            });
        }
    }

    fn lookup_cached_token(cache: &DashMap<Address, TokenCreated>, token: Address) -> TokenCreated {
        cache
            .get(&token)
            .map(|entry| entry.clone())
            .unwrap_or(TokenCreated {
                creator: Address::zero(),
                token,
                name: "unknown".to_string(),
                symbol: "unknown".to_string(),
            })
    }

    async fn build_liquidity_from_liquidity_added(
        provider: Arc<Provider<Ws>>,
        helper_address: Address,
        base: Address,
        quote: Address,
        funds: U256,
    ) -> Option<(TxLiquidity, String)> {
        let quote_decimals = if quote.is_zero() || quote == *WBNB_ADDRESS {
            18u8
        } else {
            token_manager::fetch_token_decimals(provider.clone(), quote)
                .await
                .unwrap_or(18u8)
        };
        let quote_amount = match token_manager::u256_to_f64(funds, quote_decimals) {
            Ok(value) if value > f64::EPSILON => value,
            Ok(_) => 0.0,
            Err(err) => {
                eprintln!(
                    "[listener] failed to parse LiquidityAdded funds for {:#x}: {err:?}",
                    base
                );
                0.0
            }
        };
        let funds_usd = if quote_amount > f64::EPSILON {
            match token_manager::convert_price_to_usd(&provider, quote_amount, quote).await {
                Ok(value) => Some(value),
                Err(err) => {
                    eprintln!(
                        "[listener] failed to convert LiquidityAdded funds to USD for {:#x}: {err:?}",
                        base
                    );
                    None
                }
            }
        } else {
            None
        };
        let mut funds_bnb = if quote.is_zero() || quote == *WBNB_ADDRESS {
            quote_amount
        } else {
            0.0
        };

        let helper_info = fetch_helper_info(provider.clone(), helper_address, base).await;
        let mut pair_liquidity_usd = None;
        if let Some(helper) = helper_info.as_ref() {
            let quote_address = if helper.quote.is_zero() {
                quote
            } else {
                helper.quote
            };
            let mut pair_candidate = helper.pancake_pair;
            if pair_candidate.is_none() {
                if let Ok(Some(factory_pair)) = token_manager::derive_pancake_pair(
                    provider.clone(),
                    helper.pancake_factory,
                    base,
                    quote_address,
                )
                .await
                {
                    pair_candidate = Some(factory_pair);
                }
            }
            if let Some(pair) = pair_candidate {
                match token_manager::fetch_pancake_pair_liquidity_usd(
                    provider.clone(),
                    pair,
                    base,
                    quote_address,
                )
                .await
                {
                    Ok(Some(value)) if value > f64::EPSILON => {
                        pair_liquidity_usd = Some(value / 2.0);
                    }
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!(
                            "[listener] failed to read pair liquidity for {:#x}: {err:?}",
                            base
                        );
                    }
                }
            }
        }

        let use_pair = match (pair_liquidity_usd, funds_usd) {
            (Some(pair_value), Some(fund_value)) => pair_value > fund_value,
            (Some(_), None) => true,
            _ => false,
        };

        if use_pair {
            if let Some(pair_value) = pair_liquidity_usd {
                if quote.is_zero() || quote == *WBNB_ADDRESS {
                    if let Ok(bnb_price) = token_manager::fetch_bnb_usd_price(&provider).await {
                        if bnb_price > f64::EPSILON {
                            funds_bnb = pair_value / bnb_price;
                        }
                    }
                }
                return Some((
                    TxLiquidity {
                        bnb: funds_bnb,
                        usd: Some(pair_value),
                    },
                    "token_manager:liquidityAdded:pair".to_string(),
                ));
            }
        }

        if let Some(value) = funds_usd {
            return Some((
                TxLiquidity {
                    bnb: funds_bnb,
                    usd: Some(value),
                },
                "token_manager:liquidityAdded:funds".to_string(),
            ));
        }

        None
    }

    async fn handle_token_manager_liquidity_event(
        token: TokenCreated,
        log: Log,
        provider: Arc<Provider<Ws>>,
        _telegram: TelegramSettings,
        mongo: Option<Arc<MongoLogger>>,
        min_liquidity: f64,
        liquidity_trade: LiquidityTradeSettings,
        trade_tx: Sender<BotEvent>,
        active_trades: Option<Arc<AtomicUsize>>,
        liquidity_trade_seen: Arc<DashMap<Address, ()>>,
        liquidity_override: Option<(TxLiquidity, String)>,
        source: String,
        enable_liquidity_alerts: bool,
    ) {
        let (tx_liquidity, source) = match liquidity_override {
            Some((liquidity, label)) => (Some(liquidity), label),
            None => (
                fetch_tx_liquidity(provider.clone(), log.transaction_hash).await,
                source,
            ),
        };
        let mut previous_alert_sent = None;

        if let Some(liquidity) = tx_liquidity {
            let should_store = liquidity
                .usd
                .filter(|value| *value >= min_liquidity)
                .is_some();
            if should_store {
                if let Some(storage) = mongo.as_ref() {
                    if let Some(tx_hash) = log.transaction_hash {
                        if let Err(err) = storage
                            .update_token_tx_value(&token, tx_hash, liquidity.bnb, liquidity.usd)
                            .await
                        {
                            eprintln!(
                                "[mongo] failed to update tx value for {} ({:#x}): {err:?}",
                                token.symbol, token.token
                            );
                        }
                    }
                    match storage
                        .upsert_pair_tx_liquidity(
                            &token,
                            liquidity.bnb,
                            liquidity.usd,
                            min_liquidity,
                        )
                        .await
                    {
                        Ok((alert_sent, _tweet_alert_sent)) => {
                            previous_alert_sent = alert_sent;
                        }
                        Err(err) => {
                            eprintln!(
                                "[mongo] failed to log tx liquidity for {} ({:#x}): {err:?}",
                                token.symbol, token.token
                            );
                        }
                    }
                }
            }

            if let Some(liquidity_usd) = liquidity.usd.filter(|value| *value > f64::EPSILON) {
                if liquidity_usd >= min_liquidity && enable_liquidity_alerts {
                    let latency_secs = if let Some(block_number) = log.block_number {
                        if let Ok(Some(block)) = provider.get_block(block_number).await {
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs();
                            Some(now.saturating_sub(block.timestamp.as_u64()))
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    let latency_display = latency_secs
                        .map(|v| format!("{v}s"))
                        .unwrap_or_else(|| "n/a".to_string());
                    println!(
                        "[liquidity-alert] [{}] source={} token={} ({:#x}) liquidity_usd={:.4} min_liquidity={:.2} latency={}",
                        Self::now_timestamp_myt(),
                        source,
                        token.symbol,
                        token.token,
                        liquidity_usd,
                        min_liquidity,
                        latency_display
                    );
                    let max_positions_reached = active_trades
                        .as_ref()
                        .map(|counter| counter.load(Ordering::SeqCst))
                        .map(|active| active >= liquidity_trade.max_positions())
                        .unwrap_or(false);
                    if max_positions_reached {
                        eprintln!(
                            "[liquidity-trade] max positions reached ({}), skip tx liquidity for {} ({:#x})",
                            liquidity_trade.max_positions(),
                            token.symbol,
                            token.token
                        );
                        return;
                    }
                    if !previous_alert_sent.unwrap_or(false) {
                        if liquidity_trade_seen.insert(token.token, ()).is_some() {
                            eprintln!(
                                "[liquidity-trade] skip duplicate token alert for {} ({:#x})",
                                token.symbol, token.token
                            );
                            return;
                        }
                        Self::maybe_trigger_liquidity_trade(
                            &token,
                            liquidity_trade.clone(),
                            trade_tx.clone(),
                            Some(liquidity_usd),
                            min_liquidity,
                            Some(source.clone()),
                        )
                        .await;
                        if let Some(storage) = mongo.as_ref() {
                            if let Err(err) = storage.mark_pair_alert_sent(token.token).await {
                                eprintln!("[mongo] failed to mark alert sent: {err:?}");
                            }
                        }
                    }
                }
            }
        }
    }

    async fn process_log(&mut self, log: Log) -> Result<()> {
        let token_created = utils::parse_token_created_log(&log)?;
        self.log_basic_detection(&token_created, &log).await?;
        self.spawn_enrichment(token_created.clone(), log.clone());

        if !self.enable_token_buy || !self.filter.matches(&token_created) {
            return Ok(());
        }

        let token_event = TokenEvent {
            contract: token_created.token,
            name: Some(token_created.name.clone()),
            symbol: Some(token_created.symbol.clone()),
            ticker: token_created.symbol.clone(),
            creator: Some(token_created.creator),
            tx_hash: log.transaction_hash,
            buy_amount: self.filter.buy_amount(),
        };

        match self
            .tx
            .send(BotEvent::TokenCreated(token_event.clone()))
            .await
        {
            Ok(_) => {
                if self.gmgn_settings.log_requests {
                    println!(
                        "🚨 Forwarded candidate token {} ({})",
                        token_created.name, token_created.symbol
                    );
                }
            }
            Err(err) => eprintln!("[listener] failed to forward event: {err:?}"),
        }

        Ok(())
    }

    async fn log_basic_detection(&self, token: &TokenCreated, log: &Log) -> Result<()> {
        if let Some(storage) = self.mongo.clone() {
            if let Err(err) = storage
                .log_token_creation(token, log, self.gmgn_client.is_some())
                .await
            {
                eprintln!("[mongo] failed to log token detection: {err:?}");
            }
        }
        if self.gmgn_settings.log_requests {
            println!(
                "[Detect] token={:?} name={} symbol={} (fetching security data...)",
                token.token, token.name, token.symbol
            );
        }
        Ok(())
    }

    fn spawn_enrichment(&self, token: TokenCreated, log: Log) {
        let gmgn_client = self.gmgn_client.clone();
        let gmgn_settings = self.gmgn_settings.clone();
        let log_requests = gmgn_settings.log_requests;
        let min_liquidity = self.min_candidate_liquidity;
        let min_tweet_liquidity = self.min_tweet_liquidity;
        let telegram = self.telegram.clone();
        let tweet_service = self.tweet_service.clone();
        let mongo = self.mongo.clone();
        let simulation = self.simulation.clone();
        let simulation_tweet = self.simulation_tweet.clone();
        let provider = self.provider.clone();
        let token_manager_address = self.token_manager_address;
        let helper_address = self.helper_address;
        let trade_tx = self.tx.clone();
        let tweet_trade_tx = self.tweet_tx.clone();
        let liquidity_trade = self.liquidity_trade.clone();
        let tweet_trade = self.tweet_trade.clone();
        let redis = self.redis.clone();
        let active_trades = self.active_trades.clone();
        let liquidity_trade_seen = self.liquidity_trade_seen.clone();

        tokio::spawn(async move {
            let tx_liquidity = fetch_tx_liquidity(provider.clone(), log.transaction_hash).await;
            let mut previous_alert_sent = None;
            let mut tx_liquidity_usd = None;

            if let Some(liquidity) = tx_liquidity {
                tx_liquidity_usd = liquidity.usd.filter(|value| *value > f64::EPSILON);
                let min_store_liquidity = min_liquidity.min(min_tweet_liquidity);
                let should_store = liquidity
                    .usd
                    .filter(|value| *value >= min_store_liquidity)
                    .is_some();
                if should_store {
                    if let Some(storage) = mongo.as_ref() {
                        if let Some(tx_hash) = log.transaction_hash {
                            if let Err(err) = storage
                                .update_token_tx_value(
                                    &token,
                                    tx_hash,
                                    liquidity.bnb,
                                    liquidity.usd,
                                )
                                .await
                            {
                                eprintln!(
                                    "[mongo] failed to update tx value for {} ({:#x}): {err:?}",
                                    token.symbol, token.token
                                );
                            }
                        }
                        match storage
                            .upsert_pair_tx_liquidity(
                                &token,
                                liquidity.bnb,
                                liquidity.usd,
                                min_liquidity,
                            )
                            .await
                        {
                            Ok((alert_sent, _tweet_alert_sent)) => {
                                previous_alert_sent = alert_sent;
                            }
                            Err(err) => {
                                eprintln!(
                                    "[mongo] failed to log tx liquidity for {} ({:#x}): {err:?}",
                                    token.symbol, token.token
                                );
                            }
                        }
                    }
                }

                if let Some(liquidity_usd) = liquidity.usd.filter(|value| *value > f64::EPSILON) {
                    let mut skip_gmgn = false;

                    if liquidity_usd >= min_liquidity {
                        let max_positions_reached = active_trades
                            .as_ref()
                            .map(|counter| counter.load(Ordering::SeqCst))
                            .map(|active| active >= liquidity_trade.max_positions())
                            .unwrap_or(false);
                        if max_positions_reached {
                            eprintln!(
                                "[liquidity-trade] max positions reached ({}), skip tx liquidity for {} ({:#x})",
                                liquidity_trade.max_positions(),
                                token.symbol,
                                token.token
                            );
                        } else if !previous_alert_sent.unwrap_or(false) {
                            if liquidity_trade_seen.insert(token.token, ()).is_some() {
                                eprintln!(
                                    "[liquidity-trade] skip duplicate token alert for {} ({:#x})",
                                    token.symbol, token.token
                                );
                                return;
                            }
                            Self::maybe_trigger_liquidity_trade(
                                &token,
                                liquidity_trade.clone(),
                                trade_tx.clone(),
                                Some(liquidity_usd),
                                min_liquidity,
                                Some("tx_value".to_string()),
                            )
                            .await;
                            if let Some(storage) = mongo.as_ref() {
                                if let Err(err) = storage.mark_pair_alert_sent(token.token).await {
                                    eprintln!("[mongo] failed to mark alert sent: {err:?}");
                                }
                            }
                        }
                        skip_gmgn = true;
                    }

                    if skip_gmgn {
                        return;
                    }
                }
            }

            let mut gmgn_info =
                fetch_gmgn_async(gmgn_client.clone(), gmgn_settings, token.token).await;
            let helper_info =
                fetch_helper_info(provider.clone(), helper_address, token.token).await;

            if let Some(info) = gmgn_info.as_mut() {
                if info.price.is_none() {
                    if let Some(pair_addr) = info.pool_address {
                        if pair_addr != token.token
                            && is_valid_pair(provider.clone(), pair_addr).await
                        {
                            if let Some(price) =
                                fetch_pair_price(provider.clone(), pair_addr, token.token).await
                            {
                                info.price = Some(price);
                            }
                        }
                    }
                }
                if let Some(helper) = helper_info.as_ref() {
                    let mut pair_candidate = helper.pancake_pair;
                    if pair_candidate.is_none() {
                        if let Ok(Some(factory_pair)) = token_manager::derive_pancake_pair(
                            provider.clone(),
                            helper.pancake_factory,
                            token.token,
                            helper.quote,
                        )
                        .await
                        {
                            pair_candidate = Some(factory_pair);
                        }
                    }
                    if info.price.is_none() {
                        if let Some(pair_addr) = pair_candidate {
                            if is_valid_pair(provider.clone(), pair_addr).await {
                                if let Some(price) =
                                    fetch_pair_price(provider.clone(), pair_addr, token.token).await
                                {
                                    info.price = Some(price);
                                }
                            }
                        }
                    }
                }
                if info.price.is_none() {
                    if let Some(helper_price) =
                        helper_info.as_ref().and_then(|data| data.last_price_usd)
                    {
                        info.price = Some(helper_price);
                    }
                }
                if info.price.is_none() {
                    if let Some(price) = fetch_token_manager_price(
                        provider.clone(),
                        token_manager_address,
                        token.token,
                    )
                    .await
                    {
                        info.price = Some(price);
                    }
                }
            }

            if gmgn_info.is_none() {
                return;
            }

            if let Err(err) = Self::log_enriched_details(
                &token,
                &log,
                gmgn_info.as_ref(),
                min_liquidity,
                telegram.clone(),
                mongo.clone(),
                simulation.clone(),
                provider.clone(),
                helper_info.clone(),
                token_manager_address,
                trade_tx.clone(),
                liquidity_trade.clone(),
                log_requests,
                redis.clone(),
                active_trades.clone(),
            )
            .await
            {
                eprintln!("[listener] failed to log enrichment: {err:?}");
            }

            if let (Some(service), Some(info)) = (tweet_service, gmgn_info) {
                if let Err(err) = Self::handle_tweet_alert(
                    service,
                    telegram.clone(),
                    token.clone(),
                    info,
                    helper_info.clone(),
                    mongo.clone(),
                    simulation_tweet.clone(),
                    provider.clone(),
                    min_tweet_liquidity,
                    tx_liquidity_usd,
                    token_manager_address,
                    tweet_trade.clone(),
                    tweet_trade_tx.clone(),
                    redis.clone(),
                )
                .await
                {
                    eprintln!("[tweet] failed to evaluate tweet alert: {err:?}");
                }
            }
        });
    }

    async fn handle_tweet_alert(
        service: Arc<TweetService>,
        _telegram: TelegramSettings,
        token: TokenCreated,
        _gmgn_info: GmgnTokenInfo,
        _helper_info: Option<HelperPriceInfo>,
        _mongo: Option<Arc<MongoLogger>>,
        simulation: SimulationSettings,
        _provider: Arc<Provider<Ws>>,
        min_liquidity: f64,
        tx_liquidity_usd: Option<f64>,
        _token_manager_address: Address,
        tweet_trade: LiquidityTradeSettings,
        tweet_trade_tx: Option<Sender<BotEvent>>,
        redis: Option<RedisClient>,
    ) -> Result<()> {
        let tx_liquidity_usd = match tx_liquidity_usd {
            Some(value) if value >= min_liquidity => value,
            _ => return Ok(()),
        };
        let related = match service
            .related_tweet(&token.name, &token.symbol, token.token)
            .await
        {
            Ok(value) => value,
            Err(err) => {
                eprintln!("[tweet] failed to evaluate tweets: {err:?}");
                None
            }
        };
        if let Some(_record) = related {
            if simulation.enabled {
                if let Err(err) =
                    Self::publish_sim_request(redis.as_ref(), &token, "Tweet Alert", min_liquidity)
                        .await
                {
                    eprintln!("[sim-request] failed to publish tweet request: {err:?}");
                }
            }

            if let Some(tx) = tweet_trade_tx {
                Self::maybe_trigger_tweet_trade(
                    token,
                    tweet_trade,
                    tx,
                    Some(tx_liquidity_usd),
                    min_liquidity,
                    Some("token_manager:createToken:tx_value".to_string()),
                )
                .await;
            }
        }
        Ok(())
    }

    async fn log_enriched_details(
        token: &TokenCreated,
        _log: &Log,
        gmgn_info: Option<&GmgnTokenInfo>,
        min_liquidity: f64,
        _telegram: TelegramSettings,
        mongo: Option<Arc<MongoLogger>>,
        _simulation: SimulationSettings,
        _provider: Arc<Provider<Ws>>,
        _helper_info: Option<HelperPriceInfo>,
        _token_manager_address: Address,
        _trade_tx: Sender<BotEvent>,
        _liquidity_trade: LiquidityTradeSettings,
        log_requests: bool,
        _redis: Option<RedisClient>,
        _active_trades: Option<Arc<AtomicUsize>>,
    ) -> Result<()> {
        if let Some(info) = gmgn_info {
            if let Some(storage) = mongo.as_ref() {
                if let Err(err) = storage
                    .upsert_pair_details(token, info, min_liquidity)
                    .await
                {
                    eprintln!("[mongo] failed to log pair info: {err:?}");
                }
            }
            if log_requests {
                println!(
                    "[GMGN] token={:?} pool={} liq={} holders={}",
                    token.token,
                    fmt_address(info.pool_address),
                    fmt_f64(info.liquidity),
                    fmt_u64(info.holder_count)
                );
            }
        }

        Ok(())
    }

    async fn publish_sim_request(
        redis: Option<&RedisClient>,
        token: &TokenCreated,
        alert_type: &str,
        min_liquidity: f64,
    ) -> Result<()> {
        let redis = match redis {
            Some(client) => client,
            None => return Ok(()),
        };
        let mut conn = redis.get_multiplexed_async_connection().await?;
        let _: String = redis::cmd("XADD")
            .arg(ORDER_STREAM_KEY)
            .arg("*")
            .arg("type")
            .arg("sim_request")
            .arg("token")
            .arg(format!("{:#x}", token.token))
            .arg("name")
            .arg(&token.name)
            .arg("symbol")
            .arg(&token.symbol)
            .arg("creator")
            .arg(format!("{:#x}", token.creator))
            .arg("alert_type")
            .arg(alert_type)
            .arg("min_liquidity")
            .arg(format!("{:.6}", min_liquidity))
            .query_async(&mut conn)
            .await?;
        Ok(())
    }

    async fn publish_sim_order(
        redis: Option<RedisClient>,
        order_id: ObjectId,
        alert_type: &str,
    ) -> Result<()> {
        let client = match redis {
            Some(c) => c,
            None => return Ok(()),
        };
        let mut conn = client.get_multiplexed_async_connection().await?;
        let _: String = redis::cmd("XADD")
            .arg(ORDER_STREAM_KEY)
            .arg("*")
            .arg("type")
            .arg("sim_order")
            .arg("order_id")
            .arg(order_id.to_hex())
            .arg("alert_type")
            .arg(alert_type)
            .query_async(&mut conn)
            .await?;
        Ok(())
    }

    async fn maybe_trigger_liquidity_trade(
        token: &TokenCreated,
        settings: LiquidityTradeSettings,
        trade_tx: Sender<BotEvent>,
        liquidity_usd: Option<f64>,
        min_liquidity: f64,
        source: Option<String>,
    ) {
        if !settings.is_enabled() {
            return;
        }
        let buy_amount = settings.trade_amount_bnb();
        if buy_amount <= 0.0 {
            return;
        }
        println!("----------------------");
        println!(
            "[liquidity-trade] queued {} ({:#x}) buy_amount={:.6}",
            token.symbol, token.token, buy_amount
        );
        let payload = LiquidityTradeEvent {
            token: token.clone(),
            buy_amount,
            liquidity_usd,
            min_liquidity,
            source,
        };
        if let Err(err) = trade_tx.send(BotEvent::LiquidityTrade(payload)).await {
            eprintln!("[listener] failed to queue liquidity trade: {err:?}");
        }
    }

    async fn maybe_trigger_tweet_trade(
        token: TokenCreated,
        settings: LiquidityTradeSettings,
        trade_tx: Sender<BotEvent>,
        liquidity_usd: Option<f64>,
        min_liquidity: f64,
        source: Option<String>,
    ) {
        if !settings.is_enabled() {
            eprintln!(
                "[tweet-trade] skipped {} ({:#x}) because ENABLE_TRADE_TWEET=false",
                token.symbol, token.token
            );
            return;
        }
        let buy_amount = settings.trade_amount_bnb();
        if buy_amount <= 0.0 {
            eprintln!(
                "[tweet-trade] skipped {} ({:#x}) because TRADE_AMT_TWEET <= 0",
                token.symbol, token.token
            );
            return;
        }
        println!("----------------------");
        println!(
            "[tweet-trade] queued {} ({:#x}) buy_amount={:.6}",
            token.symbol, token.token, buy_amount
        );
        let payload = LiquidityTradeEvent {
            token,
            buy_amount,
            liquidity_usd,
            min_liquidity,
            source,
        };
        if let Err(err) = trade_tx.send(BotEvent::TweetTrade(payload)).await {
            eprintln!("[listener] failed to queue tweet trade: {err:?}");
        }
    }

    #[allow(dead_code)]
    async fn send_liquidity_alert(
        telegram: &TelegramSettings,
        chat_id: Option<&str>,
        token: &TokenCreated,
        liquidity: f64,
        price: Option<f64>,
        min_liquidity: f64,
        creation_line: Option<String>,
        title: &str,
        source: Option<&str>,
    ) -> Result<bool> {
        let address_hex = format!("{:#x}", token.token);
        let gmgn_link = format!("https://gmgn.ai/bsc/token/{}", address_hex);
        let escaped_name = escape_html(&token.name);
        let escaped_symbol = escape_html(&token.symbol);
        let price_display = price
            .map(|value| format!("{:.10}", value))
            .unwrap_or_else(|| "unknown".into());
        let creation_display =
            creation_line.unwrap_or_else(|| format!("Created: unknown ({})", MALAYSIA_TIME_LABEL));
        let source_line = source
            .map(|value| format!("\nSource: {}", escape_html(value)))
            .unwrap_or_default();
        let message = format!(
            "<b>{title}</b>\nName: {name}\nSymbol: {symbol}\nAddress: <code>{address}</code>\nLiquidity: {liq:.4}\nPrice: {price}\n{creation}\nThreshold: {min:.2}{source}\n<a href=\"{url}\">View on GMGN</a>",
            title = escape_html(title),
            name = escaped_name,
            symbol = escaped_symbol,
            address = address_hex,
            liq = liquidity,
            price = price_display,
            creation = creation_display,
            min = min_liquidity,
            source = source_line,
            url = gmgn_link
        );

        Self::send_telegram_message(telegram, chat_id, &message).await
    }

    #[allow(dead_code)]
    async fn maybe_start_simulation(
        mongo: Option<Arc<MongoLogger>>,
        redis: Option<RedisClient>,
        simulation: SimulationSettings,
        provider: Arc<Provider<Ws>>,
        token: TokenCreated,
        gmgn_info: GmgnTokenInfo,
        helper_info: Option<HelperPriceInfo>,
        min_liquidity: f64,
        token_manager_address: Address,
        helper_address: Address,
        max_simulations: usize,
        smart_volume_usd: Option<f64>,
        smart_trade_bnb: Option<f64>,
        smart_wallet: Option<Address>,
    ) -> Result<Option<ObjectId>> {
        if !simulation.enabled {
            return Ok(None);
        }
        let mongo = match mongo {
            Some(storage) => storage,
            None => return Ok(None),
        };
        let liquidity = match gmgn_info.liquidity {
            Some(value) => value,
            None => return Ok(None),
        };
        if liquidity < min_liquidity {
            return Ok(None);
        }
        if max_simulations > 0 {
            match mongo.count_open_sim_orders().await {
                Ok(count) if count as usize >= max_simulations => {
                    eprintln!(
                        "[simulation] max simulations ({}) reached, skipping {:#x}",
                        max_simulations, token.token
                    );
                    return Ok(None);
                }
                Ok(_) => {}
                Err(err) => {
                    eprintln!("[simulation] failed to count open simulations: {err:?}, proceeding");
                }
            }
        }
        let entry_price = match gmgn_info.price {
            Some(price) if price > 0.0 => price,
            _ => return Ok(None),
        };
        let price_feed = Self::resolve_price_feed(
            provider.clone(),
            token.token,
            gmgn_info.pool_address,
            helper_info,
            token_manager_address,
        )
        .await;
        let take_profit_price = if simulation.take_profit_pct > 0.0 {
            Some(entry_price * (1.0 + simulation.take_profit_pct / 100.0))
        } else {
            None
        };
        let stop_loss_price = if simulation.stop_loss_pct > 0.0 {
            Some((entry_price * (1.0 - simulation.stop_loss_pct / 100.0)).max(0.0))
        } else {
            None
        };
        let trade_amount_bnb = smart_trade_bnb
            .unwrap_or_else(|| simulation.trade_amount_bnb())
            .max(0.0);
        let entry_value_usd = if trade_amount_bnb > 0.0 {
            match token_manager::fetch_bnb_usd_price(&provider).await {
                Ok(price) => Some(price * trade_amount_bnb),
                Err(err) => {
                    eprintln!(
                        "[simulation] failed to fetch BNB price for token {:#x}: {err:?}",
                        token.token
                    );
                    None
                }
            }
        } else {
            None
        };

        let stored_tp = take_profit_price.unwrap_or(entry_price);
        let stored_sl = stop_loss_price.unwrap_or(entry_price);
        let price_source = price_feed.label();
        let mut token = token;
        let token_addr = format!("{:#x}", token.token);
        if is_placeholder_token_value(&token_addr, &token.name)
            || is_placeholder_token_value(&token_addr, &token.symbol)
        {
            if let Ok((name, symbol)) =
                token_manager::fetch_token_name_symbol(provider.clone(), token.token).await
            {
                if let Some(value) = name {
                    if !is_placeholder_token_value(&token_addr, &value) {
                        token.name = value;
                    }
                }
                if let Some(value) = symbol {
                    if !is_placeholder_token_value(&token_addr, &value) {
                        token.symbol = value;
                    }
                }
            }
        }
        let order_id = mongo
            .create_sim_order(
                &token,
                price_feed.pair_address(),
                entry_price,
                stored_tp,
                stored_sl,
                price_source,
                trade_amount_bnb,
                entry_value_usd,
                smart_volume_usd,
                smart_trade_bnb,
                smart_wallet,
            )
            .await
            .context("failed to log simulated order")?;
        if smart_wallet.is_none() {
            if let Some(value_usd) = entry_value_usd {
                if entry_price > 0.0 {
                    let amount_tokens = value_usd / entry_price;
                    if amount_tokens > 0.0 {
                        let _ = mongo
                            .record_simulation_tx(
                                order_id,
                                token.token,
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
        }
        if let Some(client) = redis.clone() {
            let _ = Self::publish_sim_order(Some(client), order_id, "smart_money").await;
        }
        let pair_display = price_feed
            .pair_address()
            .map(|addr| format!("{addr:#x}"))
            .unwrap_or_else(|| "token-manager".into());
        let gmgn_link = format!("https://gmgn.ai/bsc/token/{:#x}", token.token);
        println!(
            "[Simulation] {{\"time\":\"{}\",\"token\":\"{:#x}\",\"name\":\"{}\",\"entry\":{:.10},\"tp\":{:.10},\"sl\":{:.10},\"source\":\"{}\",\"pair\":\"{}\",\"gmgn\":\"{}\"}}",
            Self::now_timestamp_myt(),
            token.token,
            token.name,
            entry_price,
            stored_tp,
            stored_sl,
            price_source,
            pair_display,
            gmgn_link
        );
        tokio::spawn(async move {
            let db = mongo.clone();
            if let Err(err) = Self::monitor_simulated_order(
                provider,
                token,
                order_id,
                entry_price,
                take_profit_price,
                stop_loss_price,
                db.clone(),
                price_feed,
                helper_address,
                entry_value_usd,
            )
            .await
            {
                eprintln!("[Simulation] monitor failed for order {order_id:?}: {err:?}",);
                if let Err(update_err) = db
                    .complete_sim_order(order_id, "skipped", None, None, None, None)
                    .await
                {
                    eprintln!("[mongo] failed to mark simulation failure: {update_err:?}");
                }
            }
        });
        Ok(Some(order_id))
    }

    pub async fn monitor_simulated_order(
        provider: Arc<Provider<Ws>>,
        token: TokenCreated,
        order_id: ObjectId,
        entry_price: f64,
        take_profit_price: Option<f64>,
        stop_loss_price: Option<f64>,
        mongo: Arc<MongoLogger>,
        price_feed: SimulationPriceFeed,
        helper_address: Address,
        entry_value_usd: Option<f64>,
    ) -> Result<()> {
        let mut blocks = provider.subscribe_blocks().await?;
        let mut price_feed = price_feed;
        let mut outcome: Option<(&'static str, f64)> = None;
        let tp_display = take_profit_price
            .map(|p| format!("{:.10}", p))
            .unwrap_or_else(|| "disabled".into());
        let sl_display = stop_loss_price
            .map(|p| format!("{:.10}", p))
            .unwrap_or_else(|| "disabled".into());
        let pair_display = price_feed
            .pair_address()
            .map(|addr| format!("{addr:#x}"))
            .unwrap_or_else(|| "token-manager".into());
        let source_label = price_feed.label();
        println!(
            "[Simulation] [{}] Monitoring token {:#x} (entry={:.10}, tp={}, sl={}, source={}, pair={})",
            Self::now_timestamp_myt(),
            token.token,
            entry_price,
            tp_display,
            sl_display,
            source_label,
            pair_display
        );
        while outcome.is_none() {
            let Some(block) = blocks.next().await else {
                break;
            };
            let (price, feed_update) = match price_feed.clone() {
                SimulationPriceFeed::Pair {
                    context,
                    manager_address,
                } => match read_pair_price(&provider, &context).await {
                    Ok(raw) => {
                        match token_manager::convert_price_to_usd(
                            &provider,
                            raw,
                            context.quote_address(),
                        )
                        .await
                        {
                            Ok(value) => (value, None),
                            Err(err) => {
                                eprintln!(
                                    "[Simulation] failed to convert price for token {:#x}: {err:?}",
                                    token.token
                                );
                                continue;
                            }
                        }
                    }
                    Err(_err) => {
                        if let Some(new_ctx) = rebuild_pair_context_with_helper(
                            provider.clone(),
                            token.token,
                            helper_address,
                        )
                        .await
                        {
                            price_feed = SimulationPriceFeed::Pair {
                                context: new_ctx,
                                manager_address: manager_address,
                            };
                            continue;
                        } else {
                            eprintln!(
                                "[Simulation] pair price fetch failed for token {:#x}; helper re-derive failed, keeping pair feed",
                                token.token
                            );
                            continue;
                        }
                    }
                },
                SimulationPriceFeed::TokenManager { manager_address } => {
                    match token_manager::fetch_last_price(
                        provider.clone(),
                        manager_address,
                        token.token,
                    )
                    .await
                    {
                        Ok(Some(value)) => (value, None),
                        Ok(None) => {
                            eprintln!(
                                "[Simulation] TokenManager price unavailable for token {:#x}",
                                token.token
                            );
                            continue;
                        }
                        Err(err) => {
                            eprintln!(
                                "[Simulation] TokenManager price fetch failed for token {:#x}: {err:?}",
                                token.token
                            );
                            continue;
                        }
                    }
                }
            };
            if let Some(new_feed) = feed_update {
                price_feed = new_feed;
            }
            let _block_number = block.number.map(|n| n.as_u64()).unwrap_or(0);
            if let Some(target) = take_profit_price {
                if price >= target {
                    outcome = Some(("tp_hit", price));
                    break;
                }
            }

            if let Some(target) = stop_loss_price {
                if price <= target {
                    outcome = Some(("sl_hit", price));
                    break;
                }
            }
        }

        if let Some((status, exit_price)) = outcome {
            let mut profit_usd = entry_value_usd.and_then(|notional| {
                if entry_price > 0.0 {
                    Some(notional * (exit_price / entry_price - 1.0))
                } else {
                    None
                }
            });
            if let Some(entry_value_usd) = entry_value_usd {
                if entry_price > 0.0 {
                    let amount_tokens = entry_value_usd / entry_price;
                    if amount_tokens > 0.0 {
                        let amount_usd = amount_tokens * exit_price;
                        if let Ok(agg) = mongo
                            .record_simulation_tx(
                                order_id,
                                token.token,
                                None,
                                "sell",
                                exit_price,
                                amount_tokens,
                                amount_usd,
                                None,
                                None,
                                None,
                            )
                            .await
                        {
                            profit_usd = Some(agg.profit_usd);
                        }
                    }
                }
            }
            mongo
                .complete_sim_order(order_id, status, Some(exit_price), profit_usd, None, None)
                .await?;
            let label = if status == "tp_hit" {
                "Take-profit"
            } else {
                "Stop-loss"
            };
            println!(
                "[Simulation] {} hit for token {:#x} at price {:.10}",
                label, token.token, exit_price
            );
            Ok(())
        } else {
            Err(anyhow!(
                "block subscription ended while monitoring simulation for token {:#x}",
                token.token
            ))
        }
    }

    pub async fn resolve_price_feed(
        provider: Arc<Provider<Ws>>,
        token: Address,
        gmgn_pair: Option<Address>,
        helper_info: Option<HelperPriceInfo>,
        token_manager_address: Address,
    ) -> SimulationPriceFeed {
        if let Some(pair_address) = gmgn_pair {
            if let Some(context) =
                Self::build_pair_context_from_address(provider.clone(), token, pair_address).await
            {
                return SimulationPriceFeed::Pair {
                    context,
                    manager_address: token_manager_address,
                };
            }
        }
        if let Some(helper) = helper_info {
            if let Some(context) =
                Self::build_helper_pair_context(provider.clone(), token, helper).await
            {
                return SimulationPriceFeed::Pair {
                    context,
                    manager_address: token_manager_address,
                };
            }
        }
        SimulationPriceFeed::TokenManager {
            manager_address: token_manager_address,
        }
    }

    async fn build_pair_context_from_address(
        provider: Arc<Provider<Ws>>,
        token: Address,
        pair_address: Address,
    ) -> Option<PairPriceContext> {
        if pair_address.is_zero() || pair_address == token {
            return None;
        }
        if !is_valid_pair(provider.clone(), pair_address).await {
            return None;
        }
        match pair_contains_token(provider.clone(), pair_address, token).await {
            Ok(true) => {}
            Ok(false) => {
                eprintln!(
                    "[Simulation] pair {:#x} does not contain token {:#x}",
                    pair_address, token
                );
                return None;
            }
            Err(err) => {
                eprintln!(
                    "[Simulation] pair {:#x} verification failed: {err:?}",
                    pair_address
                );
                return None;
            }
        }
        match build_price_context(provider, pair_address, token).await {
            Ok(context) => Some(context),
            Err(err) => {
                eprintln!(
                    "[Simulation] failed to build price context for pair {:#x}: {err:?}",
                    pair_address
                );
                None
            }
        }
    }

    async fn build_helper_pair_context(
        provider: Arc<Provider<Ws>>,
        token: Address,
        helper: HelperPriceInfo,
    ) -> Option<PairPriceContext> {
        let mut candidate = helper.pancake_pair;
        if candidate.is_none() {
            if let Ok(Some(factory_pair)) = token_manager::derive_pancake_pair(
                provider.clone(),
                helper.pancake_factory,
                token,
                helper.quote,
            )
            .await
            {
                candidate = Some(factory_pair);
            }
            // If V2 factory misses (migrated), try V3 factory hint as well.
            if candidate.is_none() {
                if let Ok(Some(factory_pair)) = token_manager::derive_pancake_pair(
                    provider.clone(),
                    helper.pancake_v3_factory,
                    token,
                    helper.quote,
                )
                .await
                {
                    candidate = Some(factory_pair);
                }
            }
        }
        let pair_address = candidate?;
        Self::build_pair_context_from_address(provider, token, pair_address).await
    }

    async fn send_telegram_message(
        telegram: &TelegramSettings,
        chat_id: Option<&str>,
        message: &str,
    ) -> Result<bool> {
        let token_value = match telegram.token() {
            Some(value) => value,
            None => return Ok(false),
        };
        let chat_value = match chat_id {
            Some(chat) => chat,
            None => return Ok(false),
        };
        let url = format!(
            "https://api.telegram.org/bot{token}/sendMessage",
            token = token_value
        );
        let payload = json!({
            "chat_id": chat_value,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": true,
        });
        reqwest::Client::new()
            .post(&url)
            .json(&payload)
            .send()
            .await?;
        Ok(true)
    }

    pub async fn send_tweet_alert_debug(
        telegram: &TelegramSettings,
        token: TokenCreated,
        gmgn_info: GmgnTokenInfo,
        tweet_text: &str,
        translation: Option<&str>,
    ) -> Result<bool> {
        let tweet_text = escape_html(tweet_text);
        let translation_line = translation
            .filter(|text| !text.trim().is_empty())
            .map(|text| format!("\nTranslation: {}", escape_html(text)))
            .unwrap_or_default();
        let address_hex = format!("{:#x}", token.token);
        let gmgn_link = format!("https://gmgn.ai/bsc/token/{}", address_hex);
        let liquidity_line = format!("Liquidity: {}", fmt_f64(gmgn_info.liquidity));
        let price_line = format!("Price: {}", fmt_price(gmgn_info.price));
        let creation_line = format_creation_summary(Some(&gmgn_info))
            .unwrap_or_else(|| format!("Created: unknown ({})", MALAYSIA_TIME_LABEL));
        let message = format!(
            "<b>Tweet Mention Alert</b>\nName: {name}\nSymbol: {symbol}\nAddress: <code>{address}</code>\n{liquidity}\n{price}\n{creation}\nTweet: {tweet}{translation}\n<a href=\"{url}\">View on GMGN</a>",
            name = escape_html(&token.name),
            symbol = escape_html(&token.symbol),
            address = address_hex,
            liquidity = liquidity_line,
            price = price_line,
            creation = creation_line,
            tweet = tweet_text,
            translation = translation_line,
            url = gmgn_link
        );
        Self::send_telegram_message(telegram, telegram.tweet_chat(), &message).await
    }

    fn spawn_smart_wallet_transfer_listener(&self) {
        let provider = self.provider.clone();
        let redis_client = self.redis.clone();
        let mongo = self.mongo.clone();
        let gmgn_client = self.gmgn_client.clone();
        let gmgn_settings = self.gmgn_settings.clone();
        let helper_address = self.helper_address;
        let token_manager_address = self.token_manager_address;
        let flap_router = self.flap_router;
        let dex_router = self.dex_router;
        let simulation = self.simulation.clone();
        let trade_tx = self.tx.clone();
        let trade_enabled = self.liquidity_trade.enabled;
        let min_volume_usd = self.min_volume_usd;
        let min_liquidity = self.min_candidate_liquidity;
        let max_simulations = self.liquidity_trade.max_positions();
        let skip_tokens = self.skip_tokens.clone();
        let live_mode = self.live_mode;
        let max_smart_follow = self.max_smart_follow;
        let max_token_follow = self.max_token_follow;
        let max_smart_token_follow = self.max_smart_token_follow;
        let smart_hold_enabled = self.smart_hold_enabled;
        let smart_hold_drop_pct = self.smart_hold_drop_pct;
        let refresh_hours = std::env::var("SMART_WALLET_REFRESH_HOURS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(4);
        let refresh_interval = if refresh_hours == 0 {
            None
        } else {
            Some(Duration::from_secs(refresh_hours.saturating_mul(3600)))
        };
        tokio::spawn(async move {
            let alt_wss_url = if live_mode {
                env::var("VOLUME_LIVE_WSS_URL2")
                    .ok()
                    .filter(|value| !value.trim().is_empty())
            } else {
                None
            };
            let mut provider = provider.clone();
            loop {
                let wallets = load_smart_wallets(mongo.clone(), live_mode).await;
                if wallets.is_empty() {
                    eprintln!("[smart-wallet] no smart wallets found in mongo");
                    if let Some(interval) = refresh_interval {
                        tokio::time::sleep(interval).await;
                        continue;
                    }
                    return;
                }
                println!(
                    "[smart-wallet] loaded {} wallets from mongo (live_mode={})",
                    wallets.len(),
                    live_mode
                );
                let topic_addrs: Vec<H256> = wallets.iter().map(|a| H256::from(*a)).collect();
                let transfer_topic = H256::from(utils::keccak256_hex(TRANSFER_SIG));

                let provider_for_subs = provider.clone();
                let (from_stream_opt, to_stream_opt, next_provider) = {
                    let from_result = provider_for_subs
                        .subscribe_logs(
                            &Filter::new()
                                .topic0(transfer_topic)
                                .topic1(topic_addrs.clone()),
                        )
                        .await;
                    match from_result {
                        Ok(from_stream) => {
                            let to_result = provider_for_subs
                                .subscribe_logs(
                                    &Filter::new()
                                        .topic0(transfer_topic)
                                        .topic2(topic_addrs.clone()),
                                )
                                .await;
                            match to_result {
                                Ok(to_stream) => (Some(from_stream), Some(to_stream), None),
                                Err(err) => {
                                    if is_unexpected_close(&err) {
                                        if let Some(ref alt_wss) = alt_wss_url {
                                            eprintln!(
                                                "[smart-wallet] subscribe (to) failed with UnexpectedClose; swapping WSS to {}",
                                                alt_wss
                                            );
                                            match Ws::connect(alt_wss).await {
                                                Ok(ws) => {
                                                    let next = Arc::new(
                                                        Provider::new(ws)
                                                            .interval(Duration::from_millis(100)),
                                                    );
                                                    (None, None, Some(next))
                                                }
                                                Err(connect_err) => {
                                                    eprintln!(
                                                        "[smart-wallet] failed to connect fallback WSS {}: {connect_err:?}",
                                                        alt_wss
                                                    );
                                                    (None, None, None)
                                                }
                                            }
                                        } else {
                                            (None, None, None)
                                        }
                                    } else {
                                        (None, None, None)
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            if is_unexpected_close(&err) {
                                if let Some(ref alt_wss) = alt_wss_url {
                                    eprintln!(
                                        "[smart-wallet] subscribe (from) failed with UnexpectedClose; swapping WSS to {}",
                                        alt_wss
                                    );
                                    match Ws::connect(alt_wss).await {
                                        Ok(ws) => {
                                            let next = Arc::new(
                                                Provider::new(ws)
                                                    .interval(Duration::from_millis(100)),
                                            );
                                            (None, None, Some(next))
                                        }
                                        Err(connect_err) => {
                                            eprintln!(
                                                "[smart-wallet] failed to connect fallback WSS {}: {connect_err:?}",
                                                alt_wss
                                            );
                                            (None, None, None)
                                        }
                                    }
                                } else {
                                    (None, None, None)
                                }
                            } else {
                                (None, None, None)
                            }
                        }
                    }
                };
                if let Some(next) = next_provider {
                    provider = next;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                let mut from_stream = match from_stream_opt {
                    Some(stream) => stream,
                    None => {
                        eprintln!("[smart-wallet] failed to subscribe transfers (from)");
                        return;
                    }
                };
                let mut to_stream = match to_stream_opt {
                    Some(stream) => stream,
                    None => {
                        eprintln!("[smart-wallet] failed to subscribe transfers (to)");
                        return;
                    }
                };

                let mut refresh_sleep =
                    refresh_interval.map(|interval| Box::pin(tokio::time::sleep(interval)));
                loop {
                    tokio::select! {
                        _ = async {
                            if let Some(ref mut sleep) = refresh_sleep {
                                sleep.as_mut().await;
                            }
                        }, if refresh_sleep.is_some() => {
                            println!(
                                "[smart-wallet] refreshing smart wallet list (every {}h)",
                                refresh_hours
                            );
                            break;
                        }
                        maybe_log = from_stream.next() => {
                            if let Some(log) = maybe_log {
                                Self::log_smart_wallet_transfer(
                                    provider.clone(),
                                    mongo.clone(),
                                    gmgn_client.clone(),
                                    gmgn_settings.clone(),
                                    redis_client.clone(),
                                    helper_address,
                                    token_manager_address,
                                    flap_router,
                                    dex_router,
                                    simulation.clone(),
                                    trade_tx.clone(),
                                    trade_enabled,
                                    min_volume_usd,
                                    min_liquidity,
                                    max_simulations,
                                    skip_tokens.clone(),
                                    live_mode,
                                    max_smart_follow,
                                    max_token_follow,
                                    max_smart_token_follow,
                                    smart_hold_enabled,
                                    smart_hold_drop_pct,
                                    log,
                                    true
                                ).await;
                            } else {
                                break;
                            }
                        }
                        maybe_log = to_stream.next() => {
                            if let Some(log) = maybe_log {
                                Self::log_smart_wallet_transfer(
                                    provider.clone(),
                                    mongo.clone(),
                                    gmgn_client.clone(),
                                    gmgn_settings.clone(),
                                    redis_client.clone(),
                                    helper_address,
                                    token_manager_address,
                                    flap_router,
                                    dex_router,
                                    simulation.clone(),
                                    trade_tx.clone(),
                                    trade_enabled,
                                    min_volume_usd,
                                    min_liquidity,
                                    max_simulations,
                                    skip_tokens.clone(),
                                    live_mode,
                                    max_smart_follow,
                                    max_token_follow,
                                    max_smart_token_follow,
                                    smart_hold_enabled,
                                    smart_hold_drop_pct,
                                    log,
                                    false
                                ).await;
                            } else {
                                break;
                            }
                        }
                    }
                }
                if refresh_interval.is_none() {
                    break;
                }
            }
        });
    }

    fn spawn_smart_hold_executor(&self) {
        let redis_client = self.redis.clone();
        if redis_client.is_none() {
            eprintln!(
                "{} [smart-hold] redis unavailable; hold executor disabled",
                log_timestamp()
            );
            return;
        }
        let provider = self.provider.clone();
        let helper_address = self.helper_address;
        let token_manager_address = self.token_manager_address;
        let trade_tx = self.tx.clone();
        let simulation = self.simulation.clone();
        let mongo = self.mongo.clone();
        let min_liquidity = self.min_candidate_liquidity;
        let max_simulations = self.liquidity_trade.max_positions();

        tokio::spawn(async move {
            loop {
                let client = match redis_client.clone() {
                    Some(client) => client,
                    None => continue,
                };
                let mut conn = match client.get_multiplexed_async_connection().await {
                    Ok(c) => c,
                    Err(_) => continue,
                };
                let payload: Option<(String, String)> = redis::cmd("BRPOP")
                    .arg(SMART_HOLD_READY_KEY)
                    .arg(5)
                    .query_async(&mut conn)
                    .await
                    .ok()
                    .flatten();
                let payload = match payload {
                    Some((_, value)) => value,
                    None => continue,
                };
                let entry: SmartHoldEntry = match serde_json::from_str(&payload) {
                    Ok(value) => value,
                    Err(_) => continue,
                };
                let token_addr = match Address::from_str(&entry.token) {
                    Ok(value) => value,
                    Err(_) => continue,
                };
                let wallet_addr = match Address::from_str(&entry.wallet) {
                    Ok(value) => value,
                    Err(_) => continue,
                };
                let token_decimals =
                    token_manager::fetch_token_decimals(provider.clone(), token_addr)
                        .await
                        .ok();
                let balance =
                    token_manager::fetch_token_balance(provider.clone(), token_addr, wallet_addr)
                        .await
                        .ok();
                if let Some(balance_value) = balance {
                    let mut zero_balance = balance_value.is_zero();
                    if !zero_balance {
                        if let Some(decimals) = token_decimals {
                            if let Ok(value) = token_manager::u256_to_f64(balance_value, decimals) {
                                if value <= 1e-6 {
                                    zero_balance = true;
                                }
                            }
                        }
                    }
                    if zero_balance {
                        continue;
                    }
                }
                if entry.live_mode {
                    let token_created = TokenCreated {
                        creator: Address::zero(),
                        token: token_addr,
                        name: entry.token_name.clone(),
                        symbol: entry.token_symbol.clone(),
                    };
                    let event = SmartMoneyTradeEvent {
                        token: token_created,
                        buy_amount: entry.buy_amount,
                        smart_wallet: wallet_addr,
                        smart_trade_bnb: entry.smart_trade_bnb,
                        liquidity_usd: entry.liquidity_usd,
                        source: entry.source.clone(),
                    };
                    if trade_tx
                        .send(BotEvent::SmartMoneyTrade(event))
                        .await
                        .is_err()
                    {
                        eprintln!(
                            "{} [smart-hold] live buy dropped: trade channel closed for {:#x}",
                            log_timestamp(),
                            token_addr
                        );
                        continue;
                    }
                } else {
                    let current_price = entry.trigger_price.filter(|value| *value > 0.0);
                    let current_price = match current_price {
                        Some(value) => value,
                        None => {
                            let price_info = fetch_price_usd_prefer_helper(
                                provider.clone(),
                                helper_address,
                                token_manager_address,
                                token_addr,
                            )
                            .await;
                            match price_info {
                                Some(info) if info.price > 0.0 => info.price,
                                _ => 0.0,
                            }
                        }
                    };
                    let gmgn_info = GmgnTokenInfo {
                        price: if current_price > 0.0 {
                            Some(current_price)
                        } else {
                            None
                        },
                        liquidity: entry.liquidity_usd,
                        initial_liquidity: entry.liquidity_usd,
                        ..Default::default()
                    };
                    let helper_info = match token_manager::fetch_helper_info(
                        provider.clone(),
                        helper_address,
                        token_addr,
                    )
                    .await
                    {
                        Ok(info) => info,
                        Err(_) => None,
                    };
                    let token_created = TokenCreated {
                        creator: Address::zero(),
                        token: token_addr,
                        name: entry.token_name.clone(),
                        symbol: entry.token_symbol.clone(),
                    };
                    let _ = Listener::maybe_start_simulation(
                        mongo.clone(),
                        Some(client.clone()),
                        simulation.clone(),
                        provider.clone(),
                        token_created,
                        gmgn_info,
                        helper_info,
                        min_liquidity,
                        token_manager_address,
                        helper_address,
                        max_simulations,
                        entry.liquidity_usd,
                        Some(entry.smart_trade_bnb),
                        Some(wallet_addr),
                    )
                    .await;
                    if let Some(db) = mongo.clone() {
                        if let Ok(Some(order_id)) = db
                            .find_open_sim_order_for_wallet(token_addr, wallet_addr)
                            .await
                        {
                            let entry_value_usd = if entry.smart_trade_bnb > 0.0 {
                                token_manager::fetch_bnb_usd_price(&provider)
                                    .await
                                    .ok()
                                    .map(|price| price * entry.smart_trade_bnb)
                            } else {
                                None
                            };
                            if let Some(value_usd) = entry_value_usd {
                                if current_price > 0.0 {
                                    let price_usd = current_price;
                                    let amount_tokens = if price_usd > 0.0 {
                                        value_usd / price_usd
                                    } else {
                                        0.0
                                    };
                                    if amount_tokens > 0.0 {
                                        let _ = db
                                            .record_simulation_tx(
                                                order_id,
                                                token_addr,
                                                Some(wallet_addr),
                                                "buy",
                                                price_usd,
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
                        }
                    }
                }
            }
        });
    }

    async fn log_smart_wallet_transfer(
        provider: Arc<Provider<Ws>>,
        mongo: Option<Arc<MongoLogger>>,
        _gmgn_client: Option<Arc<GmgnClient>>,
        _gmgn_settings: GmgnSettings,
        redis: Option<RedisClient>,
        helper_address: Address,
        token_manager_address: Address,
        flap_router: Address,
        dex_router: Address,
        simulation: SimulationSettings,
        trade_tx: Sender<BotEvent>,
        trade_enabled: bool,
        min_volume_usd: f64,
        min_liquidity: f64,
        max_simulations: usize,
        skip_tokens: Arc<HashSet<Address>>,
        live_mode: bool,
        max_smart_follow: usize,
        max_token_follow: usize,
        max_smart_token_follow: usize,
        smart_hold_enabled: bool,
        smart_hold_drop_pct: f64,
        log: Log,
        is_sell: bool,
    ) {
        if log.topics.len() < 3 {
            return;
        }
        if skip_tokens.contains(&log.address) {
            return;
        }
        let from = Address::from_slice(&log.topics[1].as_bytes()[12..]);
        let to = Address::from_slice(&log.topics[2].as_bytes()[12..]);
        let amount = if log.data.0.len() == 32 {
            U256::from_big_endian(&log.data.0)
        } else {
            U256::zero()
        };
        let latency = if let Some(block_number) = log.block_number {
            if let Ok(Some(block)) = provider.get_block(block_number).await {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                Some(now.saturating_sub(block.timestamp.as_u64()))
            } else {
                None
            }
        } else {
            None
        };
        let latency_display = latency
            .map(|v| format!("{v}s"))
            .unwrap_or_else(|| "n/a".to_string());
        let tx_hash = log
            .transaction_hash
            .map(|h| format!("{h:#x}"))
            .unwrap_or_else(|| "unknown".into());
        let direction = if is_sell { "sell" } else { "buy" };

        if is_sell {
            // Volume bot only logs buys; sells handled by order_monitor.
            return;
        } else {
            let helper_info =
                token_manager::fetch_helper_info(provider.clone(), helper_address, log.address)
                    .await
                    .ok()
                    .flatten();
            let pair_addresses =
                resolve_pancake_pairs_from_helper(provider.clone(), log.address, &helper_info)
                    .await;
            let from_valid = from == token_manager_address
                || (!flap_router.is_zero() && from == flap_router)
                || (!dex_router.is_zero() && from == dex_router)
                || pair_addresses.iter().any(|pair| *pair == from);
            if !from_valid {
                if debug_enabled() {
                    let token_label =
                        resolve_token_label_for_log(provider.clone(), log.address, "", "").await;
                    let pairs_display = if pair_addresses.is_empty() {
                        "n/a".to_string()
                    } else {
                        pair_addresses
                            .iter()
                            .map(|value| format!("{value:#x}"))
                            .collect::<Vec<_>>()
                            .join(",")
                    };
                    eprintln!(
                        "{} [smart-trade] invalid buy transfer skipped token={} token_addr={:#x} wallet={:#x} from={:#x} pairs={} tx={} token_manager={:#x} flap_router={:#x} dex_router={:#x}",
                        log_timestamp(),
                        token_label,
                        log.address,
                        to,
                        from,
                        pairs_display,
                        tx_hash,
                        token_manager_address,
                        flap_router,
                        dex_router
                    );
                }
                return;
            }
            if let Some(hash) = log.transaction_hash {
                let key = format!("{hash:#x}:{to:#x}:{:#x}", log.address);
                let mut seen = SMART_TX_SEEN
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner());
                if seen.contains(&key) {
                    return;
                }
                seen.insert(key);
            }
            let provider_clone = provider.clone();
            let redis_clone = redis.clone();
            tokio::spawn(async move {
                let price_info = match fetch_price_usd_prefer_helper(
                    provider_clone.clone(),
                    helper_address,
                    token_manager_address,
                    log.address,
                )
                .await
                {
                    Some(value) if value.price > 0.0 => value,
                    _ => return,
                };
                let price_usd = price_info.price;
                let bnb_usd_price = token_manager::fetch_bnb_usd_price(&provider_clone)
                    .await
                    .ok();
                let decimals =
                    match token_manager::fetch_token_decimals(provider_clone.clone(), log.address)
                        .await
                    {
                        Ok(value) => value,
                        Err(_) => return,
                    };
                let amount_tokens = match token_manager::u256_to_f64(amount, decimals) {
                    Ok(v) => v,
                    Err(_) => return,
                };
                let is_first_follow = get_hold_last_price(redis_clone.clone(), to, log.address)
                    .await
                    .filter(|price| *price > 0.0)
                    .is_none();
                if is_first_follow {
                    let balance_tokens = match token_manager::fetch_token_balance(
                        provider_clone.clone(),
                        log.address,
                        to,
                    )
                    .await
                    {
                        Ok(value) => token_manager::u256_to_f64(value, decimals).ok(),
                        Err(_) => None,
                    };
                    if let Some(balance) = balance_tokens {
                        if balance > amount_tokens + 1e-6 {
                            let token_label = resolve_token_label_for_log(
                                provider_clone.clone(),
                                log.address,
                                "",
                                "",
                            )
                            .await;
                            println!(
                                "{} [smart-trade] skip non-fresh buy token={} wallet={:#x} balance={:.6} buy_amount={:.6}",
                                log_timestamp(),
                                token_label,
                                to,
                                balance,
                                amount_tokens
                            );
                            return;
                        }
                    } else {
                        let token_label = resolve_token_label_for_log(
                            provider_clone.clone(),
                            log.address,
                            "",
                            "",
                        )
                        .await;
                        eprintln!(
                            "{} [smart-trade] skip buy: failed to verify fresh balance token={} wallet={:#x}",
                            log_timestamp(),
                            token_label,
                            to
                        );
                        return;
                    }
                }
                // Prefer tx.value (BNB) when available; fall back to amount * price.
                let volume_usd = if let Some(tx_hash) = log.transaction_hash {
                    match provider_clone.get_transaction(tx_hash).await {
                        Ok(Some(tx)) if !tx.value.is_zero() => {
                            let bnb = match token_manager::u256_to_f64(tx.value, 18) {
                                Ok(v) => v,
                                Err(_) => 0.0,
                            };
                            bnb_usd_price
                                .map(|price| price * bnb)
                                .unwrap_or_else(|| price_usd * amount_tokens)
                        }
                        _ => price_usd * amount_tokens,
                    }
                } else {
                    price_usd * amount_tokens
                };
                let smart_trade_bnb = if let Some(tx_hash) = log.transaction_hash {
                    match provider_clone.get_transaction(tx_hash).await {
                        Ok(Some(tx)) if !tx.value.is_zero() => {
                            token_manager::u256_to_f64(tx.value, 18).ok()
                        }
                        _ => bnb_usd_price.and_then(|price| {
                            if price > 0.0 {
                                Some(volume_usd / price)
                            } else {
                                None
                            }
                        }),
                    }
                } else {
                    bnb_usd_price.and_then(|price| {
                        if price > 0.0 {
                            Some(volume_usd / price)
                        } else {
                            None
                        }
                    })
                };
                let smart_trade_bnb = smart_trade_bnb.filter(|value| *value > 0.0);
                if smart_trade_bnb.is_none() {
                    return;
                }
                if volume_usd < min_volume_usd {
                    return;
                }
                if volume_usd.is_finite()
                    && volume_usd > 0.0
                    && !price_info.pair_based
                    && log.transaction_hash.is_some()
                {
                    if let Ok(Some(tx)) = provider_clone
                        .get_transaction(log.transaction_hash.unwrap())
                        .await
                    {
                        if tx.value.is_zero() {
                            return;
                        }
                    }
                }

                if live_mode {
                    let token_label =
                        resolve_token_label_for_log(provider_clone.clone(), log.address, "", "")
                            .await;
                    if !trade_enabled {
                        eprintln!(
                            "{} [smart-trade] live buy skipped for {}: trade disabled",
                            log_timestamp(),
                            token_label
                        );
                        return;
                    }
                    if !should_follow_live(
                        redis.clone(),
                        max_smart_follow,
                        max_token_follow,
                        to,
                        log.address,
                        &token_label,
                    )
                    .await
                    {
                        return;
                    }
                    if !should_follow_smart_token_limit(
                        redis.clone(),
                        max_smart_token_follow,
                        to,
                        log.address,
                        &token_label,
                    )
                    .await
                    {
                        return;
                    }
                    let smart_balance = provider_clone
                        .get_balance(to, None)
                        .await
                        .ok()
                        .and_then(|value| token_manager::u256_to_f64(value, 18).ok())
                        .unwrap_or(0.0);
                    let trade_amt = simulation.trade_amount_bnb().max(0.0);
                    let balance_floor = if smart_balance > 0.0 {
                        smart_balance.max(trade_amt)
                    } else {
                        trade_amt
                    };
                    let ratio = if balance_floor > 0.0 {
                        trade_amt / balance_floor
                    } else {
                        0.0
                    };
                    let follow_amount = smart_trade_bnb.unwrap_or(0.0) * ratio;
                    if follow_amount <= f64::EPSILON {
                        eprintln!(
                            "{} [smart-trade] live buy skipped for {}: follow_amount <= 0",
                            log_timestamp(),
                            token_label
                        );
                        return;
                    }
                    if let Some(helper_info) = helper_info.clone() {
                        if let Ok(min_fee_bnb) =
                            token_manager::u256_to_f64(helper_info.details.min_trading_fee, 18)
                        {
                            if min_fee_bnb > 0.0 && follow_amount < min_fee_bnb {
                                eprintln!(
                                    "{} [smart-trade] live buy skipped for {}: follow_amount {:.8} < min_trading_fee {:.8}",
                                    log_timestamp(),
                                    token_label,
                                    follow_amount,
                                    min_fee_bnb
                                );
                                return;
                            }
                        }
                    }
                    let token = TokenCreated {
                        creator: Address::zero(),
                        token: log.address,
                        name: format!("{:#x}", log.address),
                        symbol: "unknown".into(),
                    };
                    if smart_hold_enabled {
                        if let Some(price) =
                            get_hold_last_price(redis_clone.clone(), to, log.address).await
                        {
                            if price > 0.0 {
                                let hold_entry = SmartHoldEntry {
                                    wallet: format!("{to:#x}"),
                                    token: format!("{:#x}", log.address),
                                    token_name: token.name.clone(),
                                    token_symbol: token.symbol.clone(),
                                    buy_amount: follow_amount,
                                    smart_trade_bnb: smart_trade_bnb.unwrap_or(0.0),
                                    liquidity_usd: Some(volume_usd),
                                    source: Some("smart_money_live".into()),
                                    live_mode: true,
                                    trigger_price: None,
                                };
                                if enqueue_smart_hold(
                                    redis_clone.clone(),
                                    to,
                                    log.address,
                                    &hold_entry,
                                )
                                .await
                                {
                                    let _ = increment_smart_token_total(
                                        redis_clone.clone(),
                                        to,
                                        log.address,
                                    )
                                    .await;
                                    println!(
                                        "{} [smart-trade] live buy on hold token={} wallet={:#x} follow_bnb={:.6} drop_pct={:.2}",
                                        log_timestamp(),
                                        token_label,
                                        to,
                                        follow_amount,
                                        smart_hold_drop_pct
                                    );
                                    return;
                                }
                            }
                        }
                    }
                    let mut buy_pending_set = false;
                    if let Some(client) = redis_clone.as_ref() {
                        match set_smart_buy_pending(client, to, log.address).await {
                            Ok(true) => buy_pending_set = true,
                            Ok(false) => {
                                println!(
                                    "{} [smart-trade] buy already pending for token={} wallet={:#x}, skipping duplicate",
                                    log_timestamp(),
                                    token_label,
                                    to
                                );
                                return;
                            }
                            Err(err) => {
                                eprintln!(
                                    "[redis] failed to mark buy pending for token={:#x} wallet={:#x}: {err:?}",
                                    log.address, to
                                );
                            }
                        }
                    }
                    let event = SmartMoneyTradeEvent {
                        token,
                        buy_amount: follow_amount,
                        smart_wallet: to,
                        smart_trade_bnb: smart_trade_bnb.unwrap_or(0.0),
                        liquidity_usd: Some(volume_usd),
                        source: Some("smart_money_live".into()),
                    };
                    println!(
                        "{} [smart-trade] live buy queued token={} wallet={:#x} follow_bnb={:.6} smart_bnb={:.6} volume_usd={:.2} tx={}",
                        log_timestamp(),
                        token_label,
                        to,
                        follow_amount,
                        smart_trade_bnb.unwrap_or(0.0),
                        volume_usd,
                        tx_hash
                    );
                    if trade_tx
                        .send(BotEvent::SmartMoneyTrade(event))
                        .await
                        .is_err()
                    {
                        eprintln!(
                            "{} [smart-trade] live buy dropped: trade channel closed for {:#x}",
                            log_timestamp(),
                            log.address
                        );
                        if buy_pending_set {
                            if let Some(client) = redis_clone.as_ref() {
                                let _ = clear_smart_buy_pending(client, to, log.address).await;
                            }
                        }
                        return;
                    }
                    if smart_hold_enabled {
                        let _ =
                            set_hold_last_price(redis_clone.clone(), to, log.address, price_usd)
                                .await;
                    }
                    let _ = increment_smart_token_total(redis_clone.clone(), to, log.address).await;
                    return;
                }

                let existing_sim_id = if let Some(db) = mongo.clone() {
                    db.find_open_sim_order_for_wallet(log.address, to)
                        .await
                        .ok()
                        .flatten()
                } else {
                    None
                };
                if let Some(order_id) = existing_sim_id {
                    if let Some(db) = mongo.clone() {
                        let amount_usd = price_usd * amount_tokens;
                        let _ = db
                            .record_simulation_tx(
                                order_id,
                                log.address,
                                Some(to),
                                "buy",
                                price_usd,
                                amount_tokens,
                                amount_usd,
                                Some(&tx_hash),
                                log.block_number.map(|v| v.as_u64() as i64),
                                log.log_index.map(|v| v.as_u64() as i64),
                            )
                            .await;
                    }
                }

                let wallet_label = smart_wallet_label(mongo.clone(), to).await;

                println!(
                    "--------------------------------------------------------------------------------"
                );
                println!("[smart-wallet]");
                println!("Time: {}", Self::now_timestamp_myt());
                println!("Action: {}", direction);
                let token_label =
                    resolve_token_label_for_log(provider_clone.clone(), log.address, "", "").await;
                println!("Token: {}", token_label);
                println!("Wallet: {}", wallet_label);
                println!("From: {:#x}", from);
                println!("To: {:#x}", to);
                println!("Amount: {}", amount);
                println!("Volume USD: {:.4}", volume_usd);
                println!("Latency: {}", latency_display);
                println!("Tx: {}", tx_hash);
                println!(
                    "GMGN: https://gmgn.ai/bsc/token/{:#x}?tag=smart&maker={:#x}",
                    log.address, to
                );
                if existing_sim_id.is_some() {
                    return;
                }
                let gmgn_info = GmgnTokenInfo {
                    price: Some(price_usd),
                    liquidity: Some(volume_usd),
                    initial_liquidity: Some(volume_usd),
                    ..Default::default()
                };
                let helper_info = helper_info.clone();
                let token = TokenCreated {
                    creator: Address::zero(),
                    token: log.address,
                    name: format!("{:#x}", log.address),
                    symbol: "unknown".into(),
                };
                let token_label =
                    resolve_token_label_for_log(provider_clone.clone(), log.address, "", "").await;
                let _ = max_smart_token_follow;
                if smart_hold_enabled {
                    if let Some(price) =
                        get_hold_last_price(redis_clone.clone(), to, log.address).await
                    {
                        if price > 0.0 {
                            let hold_entry = SmartHoldEntry {
                                wallet: format!("{to:#x}"),
                                token: format!("{:#x}", log.address),
                                token_name: token.name.clone(),
                                token_symbol: token.symbol.clone(),
                                buy_amount: 0.0,
                                smart_trade_bnb: smart_trade_bnb.unwrap_or(0.0),
                                liquidity_usd: Some(volume_usd),
                                source: Some("smart_money".into()),
                                live_mode: false,
                                trigger_price: None,
                            };
                            if enqueue_smart_hold(redis_clone.clone(), to, log.address, &hold_entry)
                                .await
                            {
                                let _ = increment_smart_token_total(
                                    redis_clone.clone(),
                                    to,
                                    log.address,
                                )
                                .await;
                                let _ =
                                    increment_follow_counts(redis_clone.clone(), to, log.address)
                                        .await;
                                println!(
                                    "[smart-wallet] sim buy on hold token={} wallet={:#x} drop_pct={:.2}",
                                    token_label, to, smart_hold_drop_pct
                                );
                                return;
                            }
                        }
                    }
                }
                if let Err(err) = Self::maybe_start_simulation(
                    mongo.clone(),
                    redis.clone(),
                    SimulationSettings {
                        enabled: simulation.enabled,
                        take_profit_pct: 0.0,
                        stop_loss_pct: 0.0,
                        trade_amount_bnb: simulation.trade_amount_bnb,
                    },
                    provider_clone.clone(),
                    token,
                    gmgn_info,
                    helper_info,
                    min_liquidity,
                    token_manager_address,
                    helper_address,
                    max_simulations,
                    Some(volume_usd),
                    smart_trade_bnb,
                    Some(to),
                )
                .await
                {
                    eprintln!(
                        "[smart-wallet] sim start failed for {:#x}: {err:?}",
                        log.address
                    );
                } else {
                    if let Some(db) = mongo.clone() {
                        if let Ok(Some(order_id)) =
                            db.find_open_sim_order_for_wallet(log.address, to).await
                        {
                            let amount_usd = price_usd * amount_tokens;
                            let _ = db
                                .record_simulation_tx(
                                    order_id,
                                    log.address,
                                    Some(to),
                                    "buy",
                                    price_usd,
                                    amount_tokens,
                                    amount_usd,
                                    Some(&tx_hash),
                                    log.block_number.map(|v| v.as_u64() as i64),
                                    log.log_index.map(|v| v.as_u64() as i64),
                                )
                                .await;
                        }
                    }
                    if smart_hold_enabled {
                        let _ =
                            set_hold_last_price(redis_clone.clone(), to, log.address, price_usd)
                                .await;
                    }
                    let _ = increment_smart_token_total(redis_clone.clone(), to, log.address).await;
                    let _ = increment_follow_counts(redis_clone.clone(), to, log.address).await;
                }
            });
        }
    }
}

fn fmt_f64(value: Option<f64>) -> String {
    value
        .map(|v| format!("{:.4}", v))
        .unwrap_or_else(|| "unknown".into())
}

fn is_placeholder_token_value(token_addr: &str, value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return true;
    }
    let lower = trimmed.to_ascii_lowercase();
    lower == "unknown" || lower == token_addr.to_ascii_lowercase()
}

async fn resolve_token_label_for_log(
    provider: Arc<Provider<Ws>>,
    token: Address,
    name: &str,
    symbol: &str,
) -> String {
    let token_addr = format!("{:#x}", token);
    if !is_placeholder_token_value(&token_addr, symbol) {
        return symbol.trim().to_string();
    }
    if !is_placeholder_token_value(&token_addr, name) {
        return name.trim().to_string();
    }
    if let Ok((chain_name, chain_symbol)) =
        token_manager::fetch_token_name_symbol(provider, token).await
    {
        if let Some(value) = chain_symbol {
            if !is_placeholder_token_value(&token_addr, &value) {
                return value;
            }
        }
        if let Some(value) = chain_name {
            if !is_placeholder_token_value(&token_addr, &value) {
                return value;
            }
        }
    }
    token_addr
}

async fn load_smart_wallets(mongo: Option<Arc<MongoLogger>>, live_only: bool) -> Vec<Address> {
    let Some(mongo) = mongo else {
        return Vec::new();
    };
    match mongo.list_smart_money_wallets(live_only).await {
        Ok(wallets) => wallets,
        Err(err) => {
            eprintln!("[smart-wallet] failed to read mongo smart_money: {err:?}");
            Vec::new()
        }
    }
}

async fn smart_wallet_label(mongo: Option<Arc<MongoLogger>>, wallet: Address) -> String {
    if let Some(mongo) = mongo {
        if let Ok(Some(name)) = mongo.smart_money_name(wallet).await {
            return name;
        }
    }
    format!("{:#x}", wallet)
}

async fn should_follow_live(
    redis: Option<RedisClient>,
    max_wallets: usize,
    max_tokens: usize,
    wallet: Address,
    token: Address,
    token_label: &str,
) -> bool {
    if max_wallets == 0 && max_tokens == 0 {
        return true;
    }
    let client = match redis {
        Some(c) => c,
        None => return true,
    };
    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return true,
    };
    let wallet_key = format!("{:#x}", wallet);
    let token_key = format!("{:#x}", token);

    if max_wallets > 0 {
        let exists: bool = redis::cmd("HEXISTS")
            .arg(SMART_FOLLOW_WALLETS_LIVE_KEY)
            .arg(&wallet_key)
            .query_async(&mut conn)
            .await
            .unwrap_or(false);
        if !exists {
            let count: i64 = redis::cmd("HLEN")
                .arg(SMART_FOLLOW_WALLETS_LIVE_KEY)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
            if count as usize >= max_wallets {
                eprintln!(
                    "{} [smart-trade] live buy skipped: MAX_SMART_FOLLOW hit (count={} max={} wallet={})",
                    log_timestamp(),
                    count,
                    max_wallets,
                    wallet_key
                );
                return false;
            }
        }
    }

    if max_tokens > 0 {
        let exists: bool = redis::cmd("HEXISTS")
            .arg(SMART_FOLLOW_TOKENS_LIVE_KEY)
            .arg(&token_key)
            .query_async(&mut conn)
            .await
            .unwrap_or(false);
        if !exists {
            let count: i64 = redis::cmd("HLEN")
                .arg(SMART_FOLLOW_TOKENS_LIVE_KEY)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
            if count as usize >= max_tokens {
                eprintln!(
                    "{} [smart-trade] live buy skipped: MAX_TOKEN_FOLLOW hit (count={} max={} token={})",
                    log_timestamp(),
                    count,
                    max_tokens,
                    token_label
                );
                return false;
            }
        }
    }
    true
}

async fn should_follow_smart_token_limit(
    redis: Option<RedisClient>,
    max_follow: usize,
    wallet: Address,
    token: Address,
    token_label: &str,
) -> bool {
    if max_follow == 0 {
        return true;
    }
    let client = match redis {
        Some(client) => client,
        None => return true,
    };
    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return true,
    };
    let field = wallet_token_field(wallet, token);
    let count: i64 = redis::cmd("HGET")
        .arg(SMART_FOLLOW_WALLET_TOKEN_TOTAL_SIM_KEY)
        .arg(&field)
        .query_async(&mut conn)
        .await
        .unwrap_or(0);
    if count as usize >= max_follow {
        eprintln!(
            "{} [smart-trade] buy skipped: MAX_SMART_TOKEN_FOLLOW hit (count={} max={} wallet={} token={})",
            log_timestamp(),
            count,
            max_follow,
            format!("{wallet:#x}"),
            token_label
        );
        return false;
    }
    true
}

async fn increment_smart_token_total(
    redis: Option<RedisClient>,
    wallet: Address,
    token: Address,
) -> Result<i64> {
    let client = match redis {
        Some(client) => client,
        None => return Ok(0),
    };
    let mut conn = client.get_multiplexed_async_connection().await?;
    let field = wallet_token_field(wallet, token);
    let count: i64 = redis::cmd("HINCRBY")
        .arg(SMART_FOLLOW_WALLET_TOKEN_TOTAL_SIM_KEY)
        .arg(&field)
        .arg(1)
        .query_async(&mut conn)
        .await
        .unwrap_or(0);
    Ok(count)
}

async fn increment_follow_counts(
    redis: Option<RedisClient>,
    wallet: Address,
    token: Address,
) -> Result<()> {
    let client = match redis {
        Some(client) => client,
        None => return Ok(()),
    };
    let mut conn = client.get_multiplexed_async_connection().await?;
    let wallet_key = format!("{:#x}", wallet);
    let token_key = format!("{:#x}", token);
    let _: i64 = redis::cmd("HINCRBY")
        .arg(SMART_FOLLOW_WALLETS_SIM_KEY)
        .arg(&wallet_key)
        .arg(1)
        .query_async(&mut conn)
        .await
        .unwrap_or(0);
    let _: i64 = redis::cmd("HINCRBY")
        .arg(SMART_FOLLOW_TOKENS_SIM_KEY)
        .arg(&token_key)
        .arg(1)
        .query_async(&mut conn)
        .await
        .unwrap_or(0);
    Ok(())
}

async fn get_hold_last_price(
    redis: Option<RedisClient>,
    wallet: Address,
    token: Address,
) -> Option<f64> {
    let client = redis?;
    let mut conn = client.get_multiplexed_async_connection().await.ok()?;
    let field = wallet_token_field(wallet, token);
    let raw: Option<String> = redis::cmd("HGET")
        .arg(SMART_HOLD_LAST_PRICE_KEY)
        .arg(&field)
        .query_async(&mut conn)
        .await
        .ok()
        .flatten();
    raw.and_then(|value| value.parse::<f64>().ok())
}

async fn set_hold_last_price(
    redis: Option<RedisClient>,
    wallet: Address,
    token: Address,
    price: f64,
) -> Result<()> {
    if price <= 0.0 {
        return Ok(());
    }
    let client = match redis {
        Some(client) => client,
        None => return Ok(()),
    };
    let mut conn = client.get_multiplexed_async_connection().await?;
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

async fn enqueue_smart_hold(
    redis: Option<RedisClient>,
    wallet: Address,
    token: Address,
    entry: &SmartHoldEntry,
) -> bool {
    let client = match redis {
        Some(client) => client,
        None => return false,
    };
    let payload = match serde_json::to_string(entry) {
        Ok(value) => value,
        Err(_) => return false,
    };
    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return false,
    };
    let field = wallet_token_field(wallet, token);
    let queue_key = format!("{}{}", SMART_HOLD_QUEUE_PREFIX, field);
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

fn wallet_token_field(wallet: Address, token: Address) -> String {
    format!("{wallet:#x}:{token:#x}")
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

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn log_timestamp() -> String {
    Local::now().format("%d/%m/%y %H:%M:%S").to_string()
}

fn debug_enabled() -> bool {
    env_bool_value("ORDER_MONITOR_DEBUG", false) || env_bool_value("DEBUG", false)
}

#[derive(Clone, Copy)]
struct PriceResult {
    price: f64,
    pair_based: bool,
}

async fn resolve_pancake_pairs_from_helper(
    provider: Arc<Provider<Ws>>,
    token: Address,
    helper_info: &Option<token_manager::HelperPriceInfo>,
) -> Vec<Address> {
    let mut pairs = Vec::new();
    let Some(helper_info) = helper_info.as_ref() else {
        return pairs;
    };
    let mut quotes = Vec::new();
    if !helper_info.quote.is_zero() {
        push_unique_address(&mut quotes, helper_info.quote);
    }
    push_unique_address(&mut quotes, *WBNB_ADDRESS);
    if let Some(pair) = helper_info.pancake_pair {
        push_unique_address(&mut pairs, pair);
    }
    for quote in quotes {
        if let Ok(Some(pair)) = token_manager::derive_pancake_pair(
            provider.clone(),
            helper_info.pancake_factory,
            token,
            quote,
        )
        .await
        {
            push_unique_address(&mut pairs, pair);
        }
        let v3_pools = derive_pancake_v3_pools(
            provider.clone(),
            helper_info.pancake_v3_factory,
            token,
            quote,
        )
        .await;
        for pool in v3_pools {
            push_unique_address(&mut pairs, pool);
        }
    }
    pairs
}

fn push_unique_address(target: &mut Vec<Address>, value: Address) {
    if value.is_zero() || target.iter().any(|existing| *existing == value) {
        return;
    }
    target.push(value);
}

async fn derive_pancake_v3_pools(
    provider: Arc<Provider<Ws>>,
    factory: Address,
    token: Address,
    quote: Address,
) -> Vec<Address> {
    if factory.is_zero() {
        return Vec::new();
    }
    let wbnb = if quote.is_zero() {
        *WBNB_ADDRESS
    } else {
        quote
    };
    let (token0, token1) = if token < wbnb {
        (token, wbnb)
    } else {
        (wbnb, token)
    };
    let abi = match AbiParser::default()
        .parse(&["function getPool(address,address,uint24) view returns (address)"])
    {
        Ok(value) => value,
        Err(_) => return Vec::new(),
    };
    let contract = Contract::new(factory, abi, provider);
    let mut pools = Vec::new();
    for fee in [100u32, 500u32, 2500u32, 10000u32] {
        match contract.method::<_, Address>("getPool", (token0, token1, fee)) {
            Ok(call) => {
                if let Ok(pool) = call.call().await {
                    push_unique_address(&mut pools, pool);
                }
            }
            Err(_) => continue,
        }
    }
    pools
}

async fn fetch_price_usd_prefer_helper(
    provider: Arc<Provider<Ws>>,
    helper_address: Address,
    token_manager_address: Address,
    token: Address,
) -> Option<PriceResult> {
    if let Ok(info_opt) =
        token_manager::fetch_helper_info(provider.clone(), helper_address, token).await
    {
        if let Some(info) = info_opt {
            // Prefer V2 factory, then helper pair, then V3 factory.
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
                if let Some(ctx) =
                    Listener::build_pair_context_from_address(provider.clone(), token, pair_addr)
                        .await
                {
                    if let Ok(raw) = read_pair_price(&provider, &ctx).await {
                        if let Ok(usd) =
                            token_manager::convert_price_to_usd(&provider, raw, ctx.quote_address())
                                .await
                        {
                            return Some(PriceResult {
                                price: usd,
                                pair_based: true,
                            });
                        }
                    }
                }
            }
            if let Some(price) = info.last_price_usd {
                return Some(PriceResult {
                    price,
                    pair_based: false,
                });
            }
        }
    }

    match token_manager::fetch_last_price(provider, token_manager_address, token).await {
        Ok(Some(value)) => Some(PriceResult {
            price: value,
            pair_based: false,
        }),
        _ => None,
    }
}

async fn rebuild_pair_context_with_helper(
    provider: Arc<Provider<Ws>>,
    token: Address,
    helper_address: Address,
) -> Option<PairPriceContext> {
    let helper_info =
        match token_manager::fetch_helper_info(provider.clone(), helper_address, token).await {
            Ok(Some(info)) => info,
            _ => return None,
        };
    // Prefer V2 factory, then helper pair, then V3 factory.
    let mut candidate = token_manager::derive_pancake_pair(
        provider.clone(),
        helper_info.pancake_factory,
        token,
        helper_info.quote,
    )
    .await
    .ok()
    .flatten();
    if candidate.is_none() {
        candidate = helper_info.pancake_pair;
    }
    if candidate.is_none() {
        candidate = token_manager::derive_pancake_pair(
            provider.clone(),
            helper_info.pancake_v3_factory,
            token,
            helper_info.quote,
        )
        .await
        .ok()
        .flatten();
    }
    let pair_addr = candidate?;
    Listener::build_pair_context_from_address(provider, token, pair_addr).await
}

fn fmt_u64(value: Option<u64>) -> String {
    value
        .map(|v| v.to_string())
        .unwrap_or_else(|| "unknown".into())
}

fn fmt_address(value: Option<Address>) -> String {
    value
        .map(|addr| format!("{addr:?}"))
        .unwrap_or_else(|| "unknown".into())
}

fn fmt_price(value: Option<f64>) -> String {
    match value {
        Some(v) if v != 0.0 => {
            if v.abs() >= 1e-10 {
                format!("{:.12}", v)
            } else {
                format!("{:.3e}", v)
            }
        }
        Some(_) => "0".into(),
        None => "unknown".into(),
    }
}

fn format_creation_summary(info: Option<&GmgnTokenInfo>) -> Option<String> {
    let timestamp = info?.creation_timestamp?;
    let created_at = Utc.timestamp_opt(timestamp as i64, 0).single()?;
    let age_secs = Utc::now()
        .signed_duration_since(created_at)
        .num_seconds()
        .max(0);
    let offset = FixedOffset::east_opt(MALAYSIA_OFFSET_SECS)?;
    let local_time = created_at.with_timezone(&offset);
    Some(format!(
        "Created {age}s ago ({time} {label})",
        age = age_secs,
        time = local_time.format("%Y-%m-%d %H:%M:%S"),
        label = MALAYSIA_TIME_LABEL
    ))
}

fn escape_html(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

async fn fetch_tx_liquidity(
    provider: Arc<Provider<Ws>>,
    tx_hash: Option<H256>,
) -> Option<TxLiquidity> {
    let hash = tx_hash?;
    let tx = match provider.get_transaction(hash).await {
        Ok(Some(value)) => value,
        Ok(None) => return None,
        Err(err) => {
            eprintln!("[listener] failed to fetch tx {hash:?}: {err:?}");
            return None;
        }
    };
    if tx.value.is_zero() {
        return None;
    }
    let bnb = match token_manager::u256_to_f64(tx.value, 18) {
        Ok(value) if value > f64::EPSILON => value,
        Ok(_) => return None,
        Err(err) => {
            eprintln!("[listener] failed to parse tx value for {hash:?}: {err:?}");
            return None;
        }
    };
    let usd = match token_manager::fetch_bnb_usd_price(&provider).await {
        Ok(price) => Some(price * bnb),
        Err(err) => {
            eprintln!("[listener] failed to convert tx value to USD for {hash:?}: {err:?}");
            None
        }
    };
    Some(TxLiquidity { bnb, usd })
}

async fn fetch_gmgn_async(
    client: Option<Arc<GmgnClient>>,
    settings: GmgnSettings,
    token: Address,
) -> Option<GmgnTokenInfo> {
    let client = client?;
    client
        .wait_for_token(token, settings.lookup_timeout())
        .await
}

async fn fetch_token_manager_price(
    provider: Arc<Provider<Ws>>,
    manager_address: Address,
    token: Address,
) -> Option<f64> {
    match token_manager::fetch_last_price(provider, manager_address, token).await {
        Ok(price) => price,
        Err(err) => {
            eprintln!(
                "[token-manager] failed to fetch price for {:#x}: {err:?}",
                token
            );
            None
        }
    }
}

async fn fetch_helper_info(
    provider: Arc<Provider<Ws>>,
    helper_address: Address,
    token: Address,
) -> Option<HelperPriceInfo> {
    match token_manager::fetch_helper_info(provider, helper_address, token).await {
        Ok(info) => info,
        Err(err) => {
            eprintln!(
                "[token-helper] failed to fetch metadata for {:#x}: {err:?}",
                token
            );
            None
        }
    }
}

async fn is_valid_pair(provider: Arc<Provider<Ws>>, pair: Address) -> bool {
    if pair.is_zero() {
        return false;
    }
    match provider.get_code(pair, None).await {
        Ok(code) => !code.0.is_empty(),
        Err(err) => {
            eprintln!(
                "[pair-check] failed to read code for pair {:#x}: {err:?}",
                pair
            );
            false
        }
    }
}

async fn fetch_pair_price(
    provider: Arc<Provider<Ws>>,
    pair_address: Address,
    token: Address,
) -> Option<f64> {
    if !is_valid_pair(provider.clone(), pair_address).await {
        return None;
    }
    match pair_contains_token(provider.clone(), pair_address, token).await {
        Ok(true) => {}
        Ok(false) => {
            eprintln!(
                "[pair-price] skipped pair {:#x}: token {:#x} not found",
                pair_address, token
            );
            return None;
        }
        Err(err) => {
            eprintln!(
                "[pair-price] skipped pair {:#x}: failed to verify membership: {err:?}",
                pair_address
            );
            return None;
        }
    }
    match build_price_context(provider.clone(), pair_address, token).await {
        Ok(context) => match read_pair_price(&provider, &context).await {
            Ok(raw_price) => match token_manager::convert_price_to_usd(
                &provider,
                raw_price,
                context.quote_address(),
            )
            .await
            {
                Ok(usd_price) => Some(usd_price),
                Err(err) => {
                    eprintln!(
                        "[pair-price] failed to convert price for pair {:#x}: {err:?}",
                        pair_address
                    );
                    None
                }
            },
            Err(err) => {
                eprintln!(
                    "[pair-price] failed to read reserves for pair {:#x}: {err:?}",
                    pair_address
                );
                None
            }
        },
        Err(err) => {
            eprintln!(
                "[pair-price] failed to build context for pair {:#x}: {err:?}",
                pair_address
            );
            None
        }
    }
}
