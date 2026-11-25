use crate::telegram_trade::TelegramTradeSettings;
use ethers::types::Address;
use std::{
    collections::HashSet,
    env,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

#[derive(Debug, Clone)]
pub struct Config {
    pub wss_url: String,
    pub rpc_url: String,
    pub factory_address: Address,
    pub token_manager_address: Address,
    pub helper3_address: Address,
    pub flap_router: Address,
    pub dex_router: Address,
    pub pancake_v2_router: Address,
    pub pancake_v3_router: Address,
    pub private_key: Option<String>,
    pub private_key_tweet: Option<String>,
    pub wallet_name: Option<String>,
    pub wallet_name_tweet: Option<String>,
    pub trade_method: TradeMethod,
    pub trade_method_tweet: TradeMethod,
    pub telegram_trade: TelegramTradeSettings,
    pub telegram_trade_tweet: TelegramTradeSettings,
    pub gmgn: GmgnSettings,
    pub telegram: TelegramSettings,
    pub redis: RedisSettings,
    pub gemini: GeminiSettings,
    pub mongo: MongoSettings,
    pub simulation: SimulationSettings,
    pub simulation_tweet: SimulationSettings,
    pub liquidity_trade: LiquidityTradeSettings,
    pub tweet_trade: LiquidityTradeSettings,
    pub min_candidate_liquidity: f64,
    pub min_tweet_liquidity: f64,
    pub sell_all_liquidity: f64,
    pub sell_all_liquidity_tweet: f64,
    pub sell_all_migrated: bool,
    pub sell_all_migrated_tweet: bool,
    pub enable_token_buy: bool,
    pub enable_buy_tokenamap: bool,
    pub enable_add_liquidity: bool,
    pub skip_migrated: bool,
    pub skip_migrated_tweet: bool,
    pub min_volume_usd: f64,
    pub skip_tokens: HashSet<Address>,
    pub ave: AveSettings,
}

impl Config {
    pub fn from_env() -> Self {
        dotenv::dotenv().ok();
        let wss_url = env::var("WSS_URL").unwrap_or_else(|_| "wss://bsc-rpc.publicnode.com".into());
        let rpc_url =
            env::var("RPC_URL").unwrap_or_else(|_| "https://bsc-dataseed.binance.org".into());

        let token_manager_address = env::var("TOKEN_MANAGER_ADDRESS")
            .ok()
            .and_then(|addr| Address::from_str(&addr).ok())
            .unwrap_or_else(|| {
                Address::from_str("0x5c952063c7fc8610ffdb798152d69f0b9550762b")
                    .expect("Invalid default token manager address")
            });

        let factory_address = env::var("FACTORY_ADDRESS")
            .ok()
            .and_then(|addr| Address::from_str(&addr).ok())
            .unwrap_or(token_manager_address);

        let helper3_address = env::var("HELPER3_ADDRESS")
            .ok()
            .and_then(|addr| Address::from_str(&addr).ok())
            .unwrap_or_else(|| {
                Address::from_str("0xF251F83e40a78868FcfA3FA4599Dad6494E46034")
                    .expect("Invalid default helper address")
            });
        let pancake_v2_router = env::var("PANCAKE_V2_ROUTER")
            .ok()
            .and_then(|addr| Address::from_str(&addr).ok())
            .unwrap_or_else(|| {
                Address::from_str("0x10ED43C718714eb63d5aA57B78B54704E256024E")
                    .expect("Invalid default Pancake V2 router address")
            });
        let pancake_v3_router = env::var("PANCAKE_V3_ROUTER")
            .ok()
            .and_then(|addr| Address::from_str(&addr).ok())
            .unwrap_or_else(|| {
                Address::from_str("0x1b81d678ffb9c0263b24a97847620c99d213eb14")
                    .expect("Invalid default Pancake V3 router address")
            });
        let flap_router = env::var("FLAP_ROUTER")
            .ok()
            .and_then(|addr| Address::from_str(&addr).ok())
            .unwrap_or_else(Address::zero);
        let dex_router = env::var("DEX_ROUTER")
            .ok()
            .and_then(|addr| Address::from_str(&addr).ok())
            .unwrap_or_else(Address::zero);

        Self {
            wss_url,
            rpc_url,
            factory_address,
            token_manager_address,
            helper3_address,
            flap_router,
            dex_router,
            pancake_v2_router,
            pancake_v3_router,
            private_key: env::var("PRIVATE_KEY").ok(),
            private_key_tweet: env::var("PRIVATE_KEY_TWEET").ok(),
            wallet_name: env::var("WALLET_NAME").ok(),
            wallet_name_tweet: env::var("WALLET_NAME_TWEET").ok(),
            trade_method: TradeMethod::from_env("TRADE_METHOD", TradeMethod::Wallet),
            trade_method_tweet: TradeMethod::from_env(
                "TRADE_METHOD_TWEET",
                TradeMethod::from_env("TRADE_METHOD", TradeMethod::Wallet),
            ),
            telegram_trade: TelegramTradeSettings::from_env(),
            telegram_trade_tweet: TelegramTradeSettings::from_env_tweet(),
            gmgn: GmgnSettings::from_env(),
            telegram: TelegramSettings::from_env(),
            redis: RedisSettings::from_env(),
            gemini: GeminiSettings::from_env(),
            mongo: MongoSettings::from_env(),
            simulation: SimulationSettings::from_env(),
            simulation_tweet: SimulationSettings::from_env_tweet(),
            liquidity_trade: LiquidityTradeSettings::from_env(),
            tweet_trade: LiquidityTradeSettings::from_env_tweet(),
            min_candidate_liquidity: env_f64("MIN_LIQUIDITY", 1000.0),
            min_tweet_liquidity: env_f64(
                "MIN_LIQUIDITY_TWEET",
                env_f64(
                    "min_liquidity_tweet",
                    env_f64("MIN_TWEET_LIQUIDITY", env_f64("MIN_LIQUIDITY", 1000.0)),
                ),
            ),
            sell_all_liquidity: env_f64("SELL_ALL_LIQUIDITY", 0.0).max(0.0),
            sell_all_liquidity_tweet: env_f64("SELL_ALL_LIQUIDITY_TWEET", 0.0).max(0.0),
            sell_all_migrated: env_bool("SELL_ALL_MIGRATED", env_bool("sell_all_migrated", false)),
            sell_all_migrated_tweet: env_bool(
                "SELL_ALL_MIGRATED_TWEET",
                env_bool("sell_all_migrated_tweet", false),
            ),
            enable_token_buy: env_bool("ENABLE_TOKEN_BUY", true),
            enable_buy_tokenamap: env_bool("ENABLE_BUY_TOKENAMAP", true),
            enable_add_liquidity: env_bool("ENABLE_ADD_LIQUIDITY", true),
            skip_migrated: env_bool("SKIP_MIGRATED", env_bool("skip_migrated", false)),
            skip_migrated_tweet: env_bool(
                "SKIP_MIGRATED_TWEET",
                env_bool("skip_migrated_tweet", false),
            ),
            min_volume_usd: env_f64("MIN_VOLUME_USD", 0.0),
            skip_tokens: parse_skip_tokens("SKIP_TOKEN"),
            ave: AveSettings::from_env(),
        }
    }

    pub fn min_candidate_liquidity(&self) -> f64 {
        self.min_candidate_liquidity
    }

    pub fn telegram(&self) -> &TelegramSettings {
        &self.telegram
    }

    pub fn redis(&self) -> &RedisSettings {
        &self.redis
    }

    pub fn gemini(&self) -> &GeminiSettings {
        &self.gemini
    }

    pub fn mongo(&self) -> &MongoSettings {
        &self.mongo
    }

    pub fn simulation(&self) -> &SimulationSettings {
        &self.simulation
    }

    pub fn simulation_tweet(&self) -> &SimulationSettings {
        &self.simulation_tweet
    }

    pub fn liquidity_trade(&self) -> &LiquidityTradeSettings {
        &self.liquidity_trade
    }

    pub fn tweet_trade(&self) -> &LiquidityTradeSettings {
        &self.tweet_trade
    }

    pub fn trade_method(&self) -> TradeMethod {
        self.trade_method
    }

    pub fn trade_method_tweet(&self) -> TradeMethod {
        self.trade_method_tweet
    }

    pub fn ave(&self) -> &AveSettings {
        &self.ave
    }

    pub fn telegram_trade(&self) -> &TelegramTradeSettings {
        &self.telegram_trade
    }

    pub fn telegram_trade_tweet(&self) -> &TelegramTradeSettings {
        &self.telegram_trade_tweet
    }

    pub fn skip_migrated(&self) -> bool {
        self.skip_migrated
    }

    pub fn skip_migrated_tweet(&self) -> bool {
        self.skip_migrated_tweet
    }

    pub fn min_volume_usd(&self) -> f64 {
        self.min_volume_usd
    }

    pub fn min_tweet_liquidity(&self) -> f64 {
        self.min_tweet_liquidity
    }

    pub fn sell_all_liquidity(&self) -> f64 {
        self.sell_all_liquidity
    }

    pub fn sell_all_liquidity_tweet(&self) -> f64 {
        self.sell_all_liquidity_tweet
    }

    pub fn sell_all_migrated(&self) -> bool {
        self.sell_all_migrated
    }

    pub fn sell_all_migrated_tweet(&self) -> bool {
        self.sell_all_migrated_tweet
    }

    pub fn enable_token_buy(&self) -> bool {
        self.enable_token_buy
    }

    pub fn enable_buy_tokenamap(&self) -> bool {
        self.enable_buy_tokenamap
    }

    pub fn enable_add_liquidity(&self) -> bool {
        self.enable_add_liquidity
    }

    pub fn tweet_variant(&self) -> Self {
        let mut cfg = self.clone();
        if cfg.private_key_tweet.is_some() {
            cfg.private_key = cfg.private_key_tweet.clone();
        }
        if cfg.wallet_name_tweet.is_some() {
            cfg.wallet_name = cfg.wallet_name_tweet.clone();
        }
        cfg.liquidity_trade = cfg.tweet_trade.clone();
        cfg.sell_all_migrated = cfg.sell_all_migrated_tweet;
        cfg.skip_migrated = cfg.skip_migrated_tweet;
        cfg.trade_method = cfg.trade_method_tweet;
        cfg.telegram_trade = cfg.telegram_trade_tweet.clone();
        cfg
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeMethod {
    Wallet,
    Telegram,
}

impl TradeMethod {
    pub fn from_env(key: &str, default: TradeMethod) -> TradeMethod {
        match env::var(key).ok().as_deref() {
            Some(value) => match value.trim().to_ascii_lowercase().as_str() {
                "telegram" => TradeMethod::Telegram,
                "wallet" => TradeMethod::Wallet,
                _ => default,
            },
            None => default,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GmgnSettings {
    pub enabled: bool,
    pub log_requests: bool,
    pub chain: String,
    pub http_url: String,
    pub user_agent: String,
    pub access_token: Option<String>,
    pub trade_api_base: String,
    pub route_key: Option<String>,
    pub trade_slippage_pct: f64,
    pub trade_src: String,
    pub cache_ttl_secs: u64,
    pub lookup_timeout_secs: u64,
    pub helper_path: Option<PathBuf>,
    pub python_bin: String,
    pub helper_retry_count: u32,
}

#[derive(Debug, Clone, Default)]
pub struct TelegramSettings {
    token: Option<String>,
    watchlist_chat: Option<String>,
    tweet_chat: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RedisSettings {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub password: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MongoSettings {
    pub enabled: bool,
    uri: Option<String>,
    pub db_name: String,
    pub tokens_collection: String,
    pub pairs_collection: String,
    pub orders_collection: String,
    pub simulations_collection: String,
    pub simulation_tx_collection: String,
    pub order_tx_collection: String,
    pub smart_money_collection: String,
    pub ai_logs_collection: String,
    pub cleanup_enabled: bool,
    pub cleanup_interval_secs: u64,
    pub tokens_retention_days: u64,
    pub pairs_retention_days: u64,
}

#[derive(Debug, Clone)]
pub struct GeminiSettings {
    api_key: Option<String>,
    pub model: String,
    always_query: bool,
}

#[derive(Debug, Clone)]
pub struct SimulationSettings {
    pub enabled: bool,
    pub take_profit_pct: f64,
    pub stop_loss_pct: f64,
    pub trade_amount_bnb: f64,
}

#[derive(Debug, Clone)]
pub struct LiquidityTradeSettings {
    pub enabled: bool,
    pub max_positions: usize,
    pub trade_amount_bnb: f64,
    pub buy_slip_page_pct: f64,
    pub take_profit_pct: f64,
    pub stop_loss_pct: f64,
    pub sell_sl_per_block_pct: f64,
}

impl GmgnSettings {
    pub fn from_env() -> Self {
        let helper_default = PathBuf::from("scripts/gmgn_fetch.py");
        let helper_path = env::var("GMGN_PYTHON_HELPER")
            .ok()
            .map(PathBuf::from)
            .or_else(|| {
                if helper_default.exists() {
                    Some(helper_default.clone())
                } else {
                    None
                }
            });

        let primary_timeout = env_u64("GMGN_LOOKUP_TIMEOUT_SECS", 0);
        let fallback_timeout = env_u64("GMGN_FULL_SNAPSHOT_TIMEOUT_SECS", 0);
        let lookup_timeout_secs = if primary_timeout > 0 {
            primary_timeout
        } else if fallback_timeout > 0 {
            fallback_timeout
        } else {
            30
        };

        Self {
            enabled: env_bool("GMGN_ENABLE", true),
            log_requests: env_bool("GMGN_LOG_REQUESTS", false),
            chain: env::var("GMGN_CHAIN").unwrap_or_else(|_| "bsc".into()),
            http_url: env::var("GMGN_HTTP_URL")
                .unwrap_or_else(|_| "https://gmgn.ai/api/v1/mutil_window_token_info".into()),
            user_agent: env::var("GMGN_USER_AGENT")
                .unwrap_or_else(|_| "gmgn-rust-client/0.1".into()),
            access_token: env::var("GMGN_ACCESS_TOKEN").ok(),
            trade_api_base: env::var("GMGN_TRADE_API_BASE")
                .unwrap_or_else(|_| "https://gmgn.ai".into()),
            route_key: env::var("GMGN_ROUTE_KEY").ok(),
            trade_slippage_pct: env_f64("GMGN_TRADE_SLIPPAGE_PCT", 10.0),
            trade_src: env::var("GMGN_TRADE_SRC").unwrap_or_else(|_| "gmgn".into()),
            cache_ttl_secs: env_u64("GMGN_CACHE_TTL_SECS", 90),
            lookup_timeout_secs,
            helper_path,
            python_bin: env::var("GMGN_PYTHON_BIN").unwrap_or_else(|_| "python3".into()),
            helper_retry_count: env_u32("GMGN_HELPER_RETRY_COUNT", 3),
        }
    }

    pub fn cache_ttl(&self) -> Duration {
        Duration::from_secs(self.cache_ttl_secs.max(1))
    }

    pub fn lookup_timeout(&self) -> Duration {
        Duration::from_secs(self.lookup_timeout_secs.max(1))
    }

    pub fn python_helper(&self) -> Option<&Path> {
        self.helper_path.as_deref()
    }

    pub fn python_bin(&self) -> &str {
        &self.python_bin
    }

    pub fn helper_retry_count(&self) -> u32 {
        self.helper_retry_count.max(1)
    }
}

impl TelegramSettings {
    pub fn from_env() -> Self {
        let token = env_string("SNIPE_TELEGRAM_TOKEN").or_else(|| env_string("apiToken"));
        let watchlist_chat = env_string("SNIPE_TELEGRAM_CHAT")
            .or_else(|| env_string("chatID_crypto"))
            .or_else(|| env_string("chatID"));
        let tweet_chat = env_string("SNIPE_TWEET_CHAT").or_else(|| env_string("chatID"));
        Self {
            token,
            watchlist_chat,
            tweet_chat,
        }
    }

    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
    }

    pub fn watchlist_chat(&self) -> Option<&str> {
        self.watchlist_chat.as_deref()
    }

    pub fn tweet_chat(&self) -> Option<&str> {
        self.tweet_chat
            .as_deref()
            .or_else(|| self.watchlist_chat.as_deref())
    }
}

impl RedisSettings {
    pub fn from_env() -> Self {
        let enabled = env_bool("REDIS_ENABLE", true);
        let host = env_string("REDIS_HOST").unwrap_or_else(|| "127.0.0.1".into());
        let port = env::var("REDIS_PORT")
            .ok()
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(6379);
        let password = env_string("REDIS_PASSWORD");
        Self {
            enabled,
            host,
            port,
            password,
        }
    }
}

impl MongoSettings {
    pub fn from_env() -> Self {
        let password = env_string("MONGO_PASS").or_else(|| env_string("mongo_pass"));
        let uri = env_string("MONGO_URI").or_else(|| {
            password.as_ref().map(|pw| {
                format!("mongodb+srv://yoongyy:{}@cluster0.9oux3nk.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0", pw)
            })
        });
        let enabled_default = uri.is_some();
        let enabled = env_bool("MONGO_ENABLE", enabled_default);
        let cleanup_enabled = env_bool("MONGO_CLEANUP_ENABLE", false);
        Self {
            enabled,
            uri,
            db_name: env_string("MONGO_DB").unwrap_or_else(|| "gmgn".into()),
            tokens_collection: env_string("MONGO_TOKENS_COLLECTION")
                .unwrap_or_else(|| "tokens".into()),
            pairs_collection: env_string("MONGO_PAIRS_COLLECTION")
                .unwrap_or_else(|| "pairs".into()),
            orders_collection: env_string("MONGO_ORDERS_COLLECTION")
                .unwrap_or_else(|| "orders".into()),
            simulations_collection: env_string("MONGO_SIMULATIONS_COLLECTION")
                .unwrap_or_else(|| "simulations".into()),
            simulation_tx_collection: env_string("MONGO_SIMULATION_TX_COLLECTION")
                .unwrap_or_else(|| "simulation_tx".into()),
            order_tx_collection: env_string("MONGO_ORDER_TX_COLLECTION")
                .unwrap_or_else(|| "order_tx".into()),
            smart_money_collection: env_string("MONGO_SMART_MONEY_COLLECTION")
                .unwrap_or_else(|| "smart_money".into()),
            ai_logs_collection: env_string("MONGO_AI_LOGS_COLLECTION")
                .unwrap_or_else(|| "ai_logs".into()),
            cleanup_enabled,
            cleanup_interval_secs: env_u64("MONGO_CLEANUP_INTERVAL_SECS", 3600),
            tokens_retention_days: env_u64("MONGO_TOKENS_RETENTION_DAYS", 7),
            pairs_retention_days: env_u64("MONGO_PAIRS_RETENTION_DAYS", 7),
        }
    }

    pub fn uri(&self) -> Option<&str> {
        self.uri.as_deref()
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled && self.uri.is_some()
    }

    pub fn cleanup_enabled(&self) -> bool {
        self.cleanup_enabled && self.is_enabled()
    }

    pub fn cleanup_interval_secs(&self) -> u64 {
        self.cleanup_interval_secs.max(60)
    }

    pub fn tokens_retention_days(&self) -> u64 {
        self.tokens_retention_days
    }

    pub fn pairs_retention_days(&self) -> u64 {
        self.pairs_retention_days
    }
}

impl GeminiSettings {
    pub fn from_env() -> Self {
        let model = env_string("GEMINI_MODEL").unwrap_or_else(|| "gemini-1.5-flash".into());
        Self {
            api_key: env_string("GEMINI_API_KEY"),
            model,
            always_query: env_bool("GEMINI_ALWAYS_QUERY", true),
        }
    }

    pub fn api_key(&self) -> Option<&str> {
        self.api_key.as_deref()
    }

    pub fn is_enabled(&self) -> bool {
        self.api_key.is_some()
    }

    pub fn always_query(&self) -> bool {
        self.always_query
    }
}

impl SimulationSettings {
    pub fn from_env() -> Self {
        let enabled = env_bool("SIMULATE_ORDER", env_bool("simulate_order", false));
        let take_profit_pct = env_f64("SELL_ALL_TP", env_f64("sell_all_tp", 5.0)).max(0.0);
        let stop_loss_pct = env_f64("SELL_ALL_SL", env_f64("sell_all_sl", 5.0)).max(0.0);
        let trade_amount_bnb = env_f64("TRADE_AMT", env_f64("trade_amt", 0.001)).max(0.0);
        Self {
            enabled,
            take_profit_pct,
            stop_loss_pct,
            trade_amount_bnb,
        }
    }

    pub fn from_env_tweet() -> Self {
        let enabled = env_bool(
            "SIMULATE_ORDER_TWEET",
            env_bool("simulate_order_tweet", false),
        );
        let take_profit_pct =
            env_f64("SELL_ALL_TP_TWEET", env_f64("sell_all_tp_tweet", 5.0)).max(0.0);
        let stop_loss_pct =
            env_f64("SELL_ALL_SL_TWEET", env_f64("sell_all_sl_tweet", 5.0)).max(0.0);
        let trade_amount_bnb =
            env_f64("TRADE_AMT_TWEET", env_f64("trade_amt_tweet", 0.001)).max(0.0);
        Self {
            enabled,
            take_profit_pct,
            stop_loss_pct,
            trade_amount_bnb,
        }
    }

    pub fn trade_amount_bnb(&self) -> f64 {
        self.trade_amount_bnb
    }
}

impl LiquidityTradeSettings {
    pub fn from_env() -> Self {
        let enabled = env_bool(
            "ENABLE_TRADE",
            env_bool(
                "enable_trade",
                env_bool("enable_liquidity_trade", env_bool("enabley_trade", false)),
            ),
        );
        let max_positions = env_usize(
            "MAX_TRADE",
            env_usize("max_trade", env_usize("max_liquidity_trade", 1)),
        )
        .max(1);
        let trade_amount_bnb = env_f64("TRADE_AMT", env_f64("trade_amt", 0.001)).max(0.0);
        let buy_slip_page_pct = env_f64("BUY_SLIP_PAGE", env_f64("buy_slip_page", 3.0)).max(0.0);
        let take_profit_pct = env_f64("SELL_ALL_TP", env_f64("sell_all_tp", 5.0)).max(0.0);
        let stop_loss_pct = env_f64("SELL_ALL_SL", env_f64("sell_all_sl", 5.0)).max(0.0);
        let sell_sl_per_block_pct =
            env_f64("SELL_SL_PER_BLOCK", env_f64("sell_sl_per_block", 0.0)).max(0.0);
        Self {
            enabled,
            max_positions,
            trade_amount_bnb,
            buy_slip_page_pct,
            take_profit_pct,
            stop_loss_pct,
            sell_sl_per_block_pct,
        }
    }

    pub fn from_env_tweet() -> Self {
        let enabled = env_bool("ENABLE_TRADE_TWEET", env_bool("enable_trade_tweet", false));
        let max_positions = env_usize("MAX_TRADE_TWEET", env_usize("max_trade_tweet", 1)).max(1);
        let trade_amount_bnb =
            env_f64("TRADE_AMT_TWEET", env_f64("trade_amt_tweet", 0.001)).max(0.0);
        let buy_slip_page_pct =
            env_f64("BUY_SLIP_PAGE_TWEET", env_f64("buy_slip_page_tweet", 3.0)).max(0.0);
        let take_profit_pct =
            env_f64("SELL_ALL_TP_TWEET", env_f64("sell_all_tp_tweet", 5.0)).max(0.0);
        let stop_loss_pct =
            env_f64("SELL_ALL_SL_TWEET", env_f64("sell_all_sl_tweet", 5.0)).max(0.0);
        let sell_sl_per_block_pct = env_f64(
            "SELL_SL_PER_BLOCK_TWEET",
            env_f64("sell_sl_per_block_tweet", 0.0),
        )
        .max(0.0);
        Self {
            enabled,
            max_positions,
            trade_amount_bnb,
            buy_slip_page_pct,
            take_profit_pct,
            stop_loss_pct,
            sell_sl_per_block_pct,
        }
    }

    pub fn trade_amount_bnb(&self) -> f64 {
        self.trade_amount_bnb
    }

    pub fn buy_slip_page_pct(&self) -> f64 {
        self.buy_slip_page_pct
    }

    pub fn take_profit_pct(&self) -> f64 {
        self.take_profit_pct
    }

    pub fn stop_loss_pct(&self) -> f64 {
        self.stop_loss_pct
    }

    pub fn sell_sl_per_block_pct(&self) -> f64 {
        self.sell_sl_per_block_pct
    }

    pub fn max_positions(&self) -> usize {
        self.max_positions
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

#[derive(Debug, Clone)]
pub struct AveSettings {
    pub enabled: bool,
    pub api_key: String,
    pub api_secret: Option<String>,
    pub chain_wallet_address: Option<String>,
    pub signed_tx_format: String,
    pub base_url: String,
    pub use_mev: bool,
    pub auto_slippage: bool,
    pub auto_gas: Option<String>,
    pub extra_gas: u64,
}

impl AveSettings {
    pub fn from_env() -> Self {
        let api_key = env_string("AVE_AI_API_KEY").unwrap_or_default();
        let api_secret = env_string("AVE_AI_API_SECRET");
        let chain_wallet_address = env_string("AVE_AI_CHAIN_WALLET_ADDRESS");
        let signed_tx_format = env::var("AVE_AI_SIGNED_TX_FORMAT")
            .unwrap_or_else(|_| "base64".into())
            .trim()
            .to_ascii_lowercase();
        let signed_tx_format = match signed_tx_format.as_str() {
            "hex" => "hex",
            "base64url" => "base64url",
            "base64hex" | "base64_hex" => "base64hex",
            "base64hex0x" | "base64_hex0x" | "base64hex_0x" => "base64hex0x",
            _ => "base64",
        }
        .to_string();
        let enabled_default = !api_key.is_empty();
        let enabled = env_bool("AVE_AI_ENABLED", enabled_default);
        let base_url =
            env::var("AVE_AI_API_BASE_URL").unwrap_or_else(|_| "https://api-bot.ave.ai".into());
        let use_mev = env_bool("AVE_AI_USE_MEV", true);
        let auto_slippage = env_bool("AVE_AI_AUTO_SLIPPAGE", false);
        let auto_gas = env::var("AVE_AI_AUTO_GAS").ok();
        let extra_gas = env_u64("AVE_AI_EXTRA_GAS", 300000);
        Self {
            enabled,
            api_key,
            api_secret,
            chain_wallet_address,
            signed_tx_format,
            base_url,
            use_mev,
            auto_slippage,
            auto_gas,
            extra_gas,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled && !self.api_key.is_empty()
    }
}

fn env_bool(key: &str, default: bool) -> bool {
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

fn env_f64(key: &str, default: f64) -> f64 {
    env::var(key)
        .ok()
        .and_then(|value| value.trim().parse::<f64>().ok())
        .unwrap_or(default)
}

fn env_string(key: &str) -> Option<String> {
    env::var(key).ok().and_then(|value| {
        let cleaned = clean_env_value(value);
        if cleaned.is_empty() {
            None
        } else {
            Some(cleaned)
        }
    })
}

fn clean_env_value(value: String) -> String {
    let trimmed = value.trim();
    if trimmed.len() >= 2 {
        let bytes = trimmed.as_bytes();
        let first = bytes[0];
        let last = bytes[trimmed.len() - 1];
        if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
            return trimmed[1..trimmed.len() - 1].trim().to_string();
        }
    }
    let mut cleaned = trimmed;
    if let Some(idx) = trimmed.find('#') {
        let before = trimmed[..idx].chars().last();
        if idx == 0 || before.map(|ch| ch.is_whitespace()).unwrap_or(false) {
            cleaned = trimmed[..idx].trim_end();
        }
    }
    cleaned.to_string()
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_u32(key: &str, default: u32) -> u32 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn parse_skip_tokens(key: &str) -> HashSet<Address> {
    let mut tokens = HashSet::new();
    let raw = match env::var(key) {
        Ok(value) => value,
        Err(_) => return tokens,
    };
    for part in raw
        .split(|ch: char| ch == ',' || ch.is_whitespace())
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        let normalized = part.to_ascii_lowercase();
        if let Ok(addr) = Address::from_str(&normalized) {
            tokens.insert(addr);
        }
    }
    tokens
}
