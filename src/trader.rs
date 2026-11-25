use crate::{
    ave::AveClient,
    config::{Config, GmgnSettings, LiquidityTradeSettings, TelegramSettings, TradeMethod},
    gmgn::{GmgnClient, GmgnTokenInfo},
    rpc_fallback::{RpcProvider, build_http_provider},
    redis_utils::{build_redis_client, enqueue_live_trade},
    storage::MongoLogger,
    telegram_trade::TelegramTradeClient,
    token_manager,
    types::{BotEvent, LiquidityTradeEvent, SmartMoneyTradeEvent, TokenCreated, TokenEvent},
};
use anyhow::{Context, Result, anyhow};
use dashmap::DashMap;
use ethers::{
    abi::{Abi, AbiParser, ParamType, Token, decode},
    contract::{Contract, ContractError},
    middleware::SignerMiddleware,
    prelude::*,
    providers::{JsonRpcClient, Middleware, Provider, Ws},
    signers::{LocalWallet, Signer},
    types::{Address, H256, U256, U64},
    utils::{hex, keccak256, parse_units},
};
use ethers::types::transaction::eip2718::TypedTransaction;
use ethers::types::NameOrAddress;
use chrono::Local;
use futures_util::StreamExt;
use mongodb::bson::oid::ObjectId;
use once_cell::sync::Lazy;
use serde_json::json;
use serde_json::from_slice;
use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::{sync::mpsc::Receiver, time::sleep};
use redis::{AsyncCommands, Client as RedisClient};

type Client = SignerMiddleware<Arc<RpcProvider>, LocalWallet>;
static WBNB_ADDRESS: Lazy<Address> = Lazy::new(|| {
    Address::from_str("0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c")
        .expect("Invalid WBNB address")
});
const ORDER_STREAM_KEY: &str = "sniper:orders";

abigen!(
    ERC20,
    r#"[
        function balanceOf(address owner) view returns (uint256)
        function allowance(address owner, address spender) view returns (uint256)
        function approve(address spender, uint256 value) returns (bool)
    ]"#
);

#[derive(Clone)]
pub struct LiquidityPosition {
    pub token: TokenCreated,
    pub amount_raw: U256,
    pub amount_tokens: Option<f64>,
    pub entry_price: f64,
    pub take_profit_price: f64,
    pub stop_loss_price: f64,
    pub order_id: Option<ObjectId>,
}

pub struct LiveBuyResult {
    pub tx_hash: H256,
    pub amount_raw: U256,
    pub amount_tokens: Option<f64>,
    pub entry_price: f64,
    pub entry_value_usd: Option<f64>,
}

pub struct Trader {
    rx: Receiver<BotEvent>,
    contract: Contract<Client>,
    helper_address: Address,
    #[allow(dead_code)]
    pancake_v2_router: Address,
    #[allow(dead_code)]
    pancake_v3_router: Address,
    token_manager_address: Address,
    wallet_address: Address,
    ws_provider: Arc<Provider<Ws>>,
    gmgn_client: Option<Arc<GmgnClient>>,
    gmgn_settings: GmgnSettings,
    mongo: Option<Arc<MongoLogger>>,
    skip_migrated: bool,
    sell_all_migrated: bool,
    reader_provider: Arc<RpcProvider>,
    liquidity_trade: LiquidityTradeSettings,
    tweet_trade: LiquidityTradeSettings,
    liquidity_positions: Arc<DashMap<Address, LiquidityPosition>>,
    redis: Option<RedisClient>,
    trade_method: TradeMethod,
    telegram_trade: Option<Arc<TelegramTradeClient>>,
    telegram: TelegramSettings,
    min_candidate_liquidity: f64,
    #[allow(dead_code)]
    approve_on_buy: bool,
    enqueue_live_trades: bool,
    #[allow(dead_code)]
    active_trades: Option<Arc<AtomicUsize>>,
    wallet_name: Option<String>,
    alert_source: &'static str,
}

impl Trader {
    pub async fn new(
        rx: Receiver<BotEvent>,
        cfg: Config,
        rpc_urls: Vec<String>,
        gmgn_client: Option<Arc<GmgnClient>>,
        mongo: Option<Arc<MongoLogger>>,
        ws_provider: Arc<Provider<Ws>>,
        active_trades: Option<Arc<AtomicUsize>>,
        alert_source: &'static str,
        approve_on_buy: bool,
        enqueue_live_trades: bool,
    ) -> Result<Self> {
        let private_key = cfg
            .private_key
            .clone()
            .context("PRIVATE_KEY missing from environment")?;
        let wallet = private_key
            .parse::<LocalWallet>()
            .context("PRIVATE_KEY is invalid")?;
        let wallet_address = wallet.address();
        let wallet = wallet.with_chain_id(56u64);

        let provider = build_http_provider(rpc_urls, Duration::from_millis(1500))
            .context("RPC_URL is invalid")?;
        let provider = Arc::new(provider);
        let client = Arc::new(SignerMiddleware::new(provider.clone(), wallet));

        let abi: Abi = from_slice(include_bytes!("abi/TokenManager2.lite.abi"))
            .context("failed to parse TokenManager ABI")?;
        let contract = Contract::new(cfg.token_manager_address, abi, client.clone());
        let trade_method = cfg.trade_method();
        let telegram_trade = if trade_method == TradeMethod::Telegram {
            Some(Arc::new(
                TelegramTradeClient::connect(cfg.telegram_trade()).await?,
            ))
        } else {
            None
        };
        Ok(Self {
            rx,
            contract,
            helper_address: cfg.helper3_address,
            pancake_v2_router: cfg.pancake_v2_router,
            pancake_v3_router: cfg.pancake_v3_router,
            token_manager_address: cfg.token_manager_address,
            wallet_address,
            ws_provider,
            gmgn_client,
            gmgn_settings: cfg.gmgn.clone(),
            mongo,
            skip_migrated: cfg.skip_migrated(),
            sell_all_migrated: cfg.sell_all_migrated(),
            reader_provider: provider,
            liquidity_trade: cfg.liquidity_trade().clone(),
            tweet_trade: cfg.tweet_trade().clone(),
            liquidity_positions: Arc::new(DashMap::new()),
            redis: build_redis_client(cfg.redis()).ok(),
            trade_method,
            telegram_trade,
            telegram: cfg.telegram().clone(),
            min_candidate_liquidity: cfg.min_candidate_liquidity(),
            approve_on_buy,
            enqueue_live_trades,
            active_trades,
            wallet_name: cfg.wallet_name.clone(),
            alert_source,
        })
    }

    async fn fetch_gmgn_info(&self, address: Address) -> Option<GmgnTokenInfo> {
        let client = self.gmgn_client.as_ref()?;
        client
            .wait_for_token(address, self.gmgn_settings.lookup_timeout())
            .await
    }

    async fn resolve_token_label(&self, token: &TokenEvent) -> String {
        if let Some(symbol) = token.symbol.as_deref().filter(|value| !value.is_empty()) {
            return symbol.to_string();
        }
        if let Some(name) = token.name.as_deref().filter(|value| !value.is_empty()) {
            return name.to_string();
        }
        resolve_token_label_from_chain(self.ws_provider.clone(), token.contract).await
    }

    pub async fn run(&mut self) -> Result<()> {
        println!(
            "🤖 Trader initialized: wallet_name={} trade_mode={:?} min_liquidity={} alert_source={}",
            self.wallet_name.as_deref().unwrap_or("unknown"),
            self.trade_method,
            self.min_candidate_liquidity,
            self.alert_source
        );
        while let Some(event) = self.rx.recv().await {
            match event {
                BotEvent::TokenCreated(token) => {
                    if !self.should_trade(&token).await? {
                        continue;
                    }
                    let name = token.name.clone().unwrap_or_else(|| token.ticker.clone());
                    let symbol = token
                        .symbol
                        .clone()
                        .unwrap_or_else(|| token.ticker.clone());
                    let token_created = TokenCreated {
                        creator: token.creator.unwrap_or_default(),
                        token: token.contract,
                        name,
                        symbol,
                    };
                    let event = LiquidityTradeEvent {
                        token: token_created,
                        buy_amount: token.buy_amount,
                        liquidity_usd: None,
                        min_liquidity: self.min_candidate_liquidity,
                        source: None,
                    };
                    if let Err(err) = self.handle_liquidity_trade(event, "TokenCreated").await {
                        eprintln!("{} [trader] buy failed: {err:?}", log_timestamp());
                    }
                    continue;
                }
                BotEvent::LiquidityTrade(command) => {
                    if let Err(err) = self
                        .handle_liquidity_trade(command, "Liquidity Alert")
                        .await
                    {
                        eprintln!("{} [liquidity-trade] failed: {err:?}", log_timestamp());
                    }
                }
                BotEvent::TweetTrade(command) => {
                    if let Err(err) = self
                        .handle_liquidity_trade(command, "Tweet Alert")
                        .await
                    {
                        eprintln!("{} [tweet-trade] failed: {err:?}", log_timestamp());
                    }
                }
                BotEvent::SmartMoneyTrade(command) => {
                    if let Err(err) = self.handle_smart_money_trade(command).await {
                        eprintln!("{} [smart-trade] failed: {err:?}", log_timestamp());
                    }
                }
                BotEvent::Shutdown => {
                    println!("⚠️ Trader received shutdown signal");
                    break;
                }
            }
        }
        Ok(())
    }

    async fn buy_token(&self, token: &TokenEvent) -> Result<H256> {
        if !(token.buy_amount > 0.0) {
            return Err(anyhow!("buy amount must be greater than zero"));
        }
        let token_label = self.resolve_token_label(token).await;

        if self.enqueue_live_trades {
            let redis = self
                .redis
                .as_ref()
                .context("live_trade enqueue enabled but redis not configured")?;
            enqueue_live_trade(
                redis,
                vec![
                    ("type", "buy".to_string()),
                    ("token", format!("{:#x}", token.contract)),
                    ("amount_bnb", format!("{:.18}", token.buy_amount)),
                    ("source", self.alert_source.to_string()),
                    ("wallet", format!("{:#x}", self.wallet_address)),
                ],
            )
            .await?;
            println!(
                "{} [buy] queued live trade token={} amount_bnb={:.6} source={}",
                log_timestamp(),
                token_label,
                token.buy_amount,
                self.alert_source
            );
            return Ok(H256::zero());
        }

        if self.trade_method == TradeMethod::Telegram {
            let client = self
                .telegram_trade
                .as_ref()
                .context("telegram trade client not configured")?;
            client
                .send_buy(&format!("{:#x}", token.contract), token.buy_amount)
                .await?;
            println!(
                "[{}] [buy] Sent telegram /buy for {} amount {:.6} BNB",
                log_timestamp(),
                token_label,
                token.buy_amount
            );
            return Ok(H256::zero());
        }

        let buy_amount_str = format!("{:.18}", token.buy_amount);
        let funds: U256 = parse_units(buy_amount_str, 18)
            .context("failed to convert buy amount to wei")?
            .into();

        if !self.helper_address.is_zero()
            && (!self.pancake_v2_router.is_zero() || !self.pancake_v3_router.is_zero())
        {
            let helper_abi: Abi = from_slice(include_bytes!("abi/TokenManagerHelper3.abi"))
                .context("failed to parse TokenManagerHelper ABI")?;
            let helper3 = Contract::new(self.helper_address, helper_abi, self.contract.client().clone());
            let wbnb: Address = helper3
                .method("WETH", ())
                .context("build helper WETH call")?
                .call()
                .await
                .unwrap_or_else(|err| {
                    eprintln!("{} [helper3-buy] failed to read WETH: {err:?}", log_timestamp());
                    Address::zero()
                });
            let wbnb = if wbnb.is_zero() { *WBNB_ADDRESS } else { wbnb };

            if !self.pancake_v2_router.is_zero() {
                let mut pair = helper3
                    .method::<_, Address>("getPancakePair", token.contract)
                    .context("build helper getPancakePair call")?
                    .call()
                        .await
                        .unwrap_or_else(|err| {
                            eprintln!(
                                "{} [helper3-buy] failed to read helper pancake pair for {}: {err:?}",
                                log_timestamp(),
                                token_label
                            );
                            Address::zero()
                        });
                if pair.is_zero() {
                    let factory: Address = helper3
                        .method("PANCAKE_FACTORY", ())
                        .context("build helper PANCAKE_FACTORY call")?
                        .call()
                        .await
                        .unwrap_or_else(|err| {
                            eprintln!("{} [helper3-buy] failed to read PANCAKE_FACTORY: {err:?}", log_timestamp());
                            Address::zero()
                        });
                    if !factory.is_zero() {
                        let factory_abi = AbiParser::default()
                            .parse(&["function getPair(address,address) view returns (address)"])
                            .context("failed to parse Pancake factory ABI")?;
                        let factory_contract =
                            Contract::new(factory, factory_abi, helper3.client().clone());
                        pair = factory_contract
                            .method("getPair", (token.contract, wbnb))
                            .context("build getPair call")?
                            .call()
                    .await
                    .unwrap_or_else(|err| {
                        eprintln!(
                            "{} [helper3-buy] failed to read Pancake pair for {}: {err:?}",
                            log_timestamp(),
                            token_label
                        );
                        Address::zero()
                    });
                    }
                }
                if !pair.is_zero() && self.pair_exists(pair).await {
                    return Self::pancake_v2_buy_for_eth(
                        self.pancake_v2_router,
                        self.contract.client().clone(),
                        self.wallet_address,
                        token.contract,
                        funds,
                        U256::zero(),
                        wbnb,
                    )
                    .await;
                }
            }

            if !self.pancake_v3_router.is_zero() {
                let v3_factory: Address = helper3
                    .method("PANCAKE_V3_FACTORY", ())
                    .context("build helper PANCAKE_V3_FACTORY call")?
                    .call()
                    .await
                    .unwrap_or_else(|err| {
                        eprintln!("{} [helper3-buy] failed to read PANCAKE_V3_FACTORY: {err:?}", log_timestamp());
                        Address::zero()
                    });
                if !v3_factory.is_zero() {
                    if let Some(fee) =
                        Self::find_v3_fee_tier(v3_factory, self.contract.client().clone(), token.contract, wbnb)
                            .await
                    {
                        return Self::pancake_v3_buy_for_eth(
                            self.pancake_v3_router,
                            self.contract.client().clone(),
                            self.wallet_address,
                            token.contract,
                            funds,
                            U256::zero(),
                            wbnb,
                            fee,
                        )
                        .await;
                    }
                }
            }
        }

        let decimals =
            token_manager::fetch_token_decimals(self.reader_provider.clone(), token.contract)
                .await
                .unwrap_or(18);

        let slippage_pct = self.liquidity_trade.buy_slip_page_pct().clamp(0.0, 100.0);
        let slippage_bps = (slippage_pct * 100.0).round() as u64;
        let min_amount = match token_manager::try_buy_estimate(
            self.reader_provider.clone(),
            self.helper_address,
            token.contract,
            funds,
        )
        .await
        {
            Ok(Some(estimate_raw)) => {
                if let Some(estimate) =
                    token_manager::format_estimated_tokens(estimate_raw, decimals)
                {
                    println!(
                        "{} [buy] Estimated {:.6} tokens for {:.6} BNB",
                        log_timestamp(),
                        estimate, token.buy_amount
                    );
                }
                let keep_bps = U256::from(10_000u64.saturating_sub(slippage_bps));
                estimate_raw
                    .checked_mul(keep_bps)
                    .unwrap_or(U256::zero())
                    / U256::from(10_000u64)
            }
            Ok(None) => {
                eprintln!(
                    "{} [buy] estimate failed: empty result for {}",
                    log_timestamp(),
                    token_label
                );
                return Err(anyhow!("buy estimate returned empty"));
            }
            Err(err) => {
                eprintln!(
                    "{} [buy] estimate failed for {}: {err:?}",
                    log_timestamp(),
                    token_label
                );
                return Err(anyhow!("buy estimate failed"));
            }
        };

        let hash = keccak256("buyTokenAMAP(address,uint256,uint256)".as_bytes());
        let selector: [u8; 4] = hash[0..4].try_into().unwrap();

        println!(
            "{} [buy] Using min_amount {} (slippage {} bps)",
            log_timestamp(),
            min_amount, slippage_bps
        );

        let call = self
            .contract
            .method_hash::<_, ()>(selector, (token.contract, funds, min_amount))
            .context("failed to build buyTokenAMAP(address,uint256,uint256) call")?
            .value(funds);

        let tx_debug = call.tx.clone();
        let pending_tx = match call.send().await {
            Ok(pending) => pending,
            Err(err) => {
                if let Some(reason) = decode_revert_reason(&err) {
                    eprintln!("{} [trader] buyTokenAMAP revert reason: {}", log_timestamp(), reason);
                }
                log_tx_debug(
                    "token-manager buy",
                    "buyTokenAMAP(address,uint256,uint256)",
                    &tx_debug,
                );
                eprintln!("{} [trader] buyTokenAMAP reverted: {err:?}", log_timestamp());
                return Err(anyhow!(err).context("buyTokenAMAP call reverted"));
            }
        };
        let tx_hash = pending_tx.tx_hash();
        println!(
            "[{}] [buy] tx submitted for {}, waiting for confirmation... tx={tx_hash:?}",
            log_timestamp(),
            token_label
        );
        let receipt = pending_tx
            .await
            .context("transaction dropped before inclusion")?;
        let receipt = receipt.context("transaction receipt not available")?;
        println!(
            "[{}] [buy] tx confirmed for {} status={:?} tx={tx_hash:?}",
            log_timestamp(),
            token_label,
            receipt.status
        );
        match receipt.status {
            Some(status) if status == U64::from(1) => Ok(tx_hash),
            _ => {
                log_tx_debug(
                    "token-manager buy",
                    "buyTokenAMAP(address,uint256,uint256)",
                    &tx_debug,
                );
                Err(anyhow!(
                    "buy transaction failed on-chain (tx={:#x}, status={:?}, block={:?}, gas_used={})",
                    tx_hash,
                    receipt.status,
                    receipt.block_number,
                    receipt.gas_used.unwrap_or_default()
                ))
            }
        }
    }

    pub async fn live_buy_for_test(&self, token: Address, buy_amount: f64) -> Result<H256> {
        let token_event = TokenEvent {
            contract: token,
            name: None,
            symbol: None,
            ticker: format!("{:#x}", token),
            creator: None,
            tx_hash: None,
            buy_amount,
        };
        self.buy_token(&token_event).await
    }

    pub async fn live_buy_with_details(
        &self,
        token: Address,
        buy_amount: f64,
    ) -> Result<LiveBuyResult> {
        let token_event = TokenEvent {
            contract: token,
            name: None,
            symbol: None,
            ticker: format!("{:#x}", token),
            creator: None,
            tx_hash: None,
            buy_amount,
        };
        let track_balance = self.trade_method == TradeMethod::Wallet;
        let before_balance = if track_balance {
            token_manager::fetch_token_balance(
                self.reader_provider.clone(),
                token,
                self.wallet_address,
            )
            .await
            .unwrap_or(U256::zero())
        } else {
            U256::zero()
        };
        let tx_hash = self.buy_token(&token_event).await?;
        let mut amount_raw = U256::zero();
        let mut amount_tokens = None;
        if track_balance {
            let mut after_balance = token_manager::fetch_token_balance(
                self.reader_provider.clone(),
                token,
                self.wallet_address,
            )
            .await
            .unwrap_or(before_balance);
            for _ in 0..5 {
                if after_balance > before_balance {
                    break;
                }
                sleep(Duration::from_millis(750)).await;
                after_balance = token_manager::fetch_token_balance(
                    self.reader_provider.clone(),
                    token,
                    self.wallet_address,
                )
                .await
                .unwrap_or(after_balance);
            }
            amount_raw = after_balance.saturating_sub(before_balance);
            if !amount_raw.is_zero() {
                let decimals = token_manager::fetch_token_decimals(
                    self.reader_provider.clone(),
                    token,
                )
                .await
                .unwrap_or(18);
                amount_tokens = token_manager::u256_to_f64(amount_raw, decimals).ok();
            }
        }
        let entry_value_usd = token_manager::fetch_bnb_usd_price(&self.ws_provider)
            .await
            .ok()
            .map(|price| price * buy_amount);
        let mut entry_price = match (entry_value_usd, amount_tokens) {
            (Some(value), Some(tokens)) if tokens > f64::EPSILON => value / tokens,
            _ => 0.0,
        };
        if entry_price <= 0.0 {
            entry_price = token_manager::fetch_last_price(
                self.ws_provider.clone(),
                self.token_manager_address,
                token,
            )
            .await
            .unwrap_or(None)
            .unwrap_or(0.0);
        }
        if entry_price <= 0.0 {
            if let Some(info) = self.fetch_gmgn_info(token).await {
                entry_price = info.price.unwrap_or(0.0);
            }
        }
        Ok(LiveBuyResult {
            tx_hash,
            amount_raw,
            amount_tokens,
            entry_price,
            entry_value_usd,
        })
    }

    pub async fn live_sell_all_for_test(&self, token: Address) -> Result<H256> {
        let balance = token_manager::fetch_token_balance(
            self.reader_provider.clone(),
            token,
            self.wallet_address,
        )
        .await
        .context("failed to read token balance")?;
        if balance.is_zero() {
            return Err(anyhow!("token balance is zero"));
        }
        Self::sell_position(
            self.contract.clone(),
            self.helper_address,
            self.pancake_v2_router,
            self.pancake_v3_router,
            self.reader_provider.clone(),
            self.wallet_address,
            TradeMethod::Wallet,
            None,
            None,
            None,
            56,
            token,
            balance,
            Duration::from_millis(0),
        )
        .await
    }

    pub async fn live_sell_amount_for_test(
        &self,
        token: Address,
        amount: U256,
    ) -> Result<H256> {
        if amount.is_zero() {
            return Err(anyhow!("sell amount is zero"));
        }
        Self::sell_position(
            self.contract.clone(),
            self.helper_address,
            self.pancake_v2_router,
            self.pancake_v3_router,
            self.reader_provider.clone(),
            self.wallet_address,
            TradeMethod::Wallet,
            None,
            None,
            None,
            56,
            token,
            amount,
            Duration::from_millis(0),
        )
        .await
    }

    async fn handle_liquidity_trade(
        &self,
        event: LiquidityTradeEvent,
        alert_type: &str,
    ) -> Result<()> {
        let is_tweet_alert = alert_type == "Tweet Alert";
        let (trade_settings, log_prefix) = if is_tweet_alert {
            (&self.tweet_trade, "[tweet-trade]")
        } else {
            (&self.liquidity_trade, "[liquidity-trade]")
        };

        if self.trade_method == TradeMethod::Wallet && alert_type == "Liquidity Alert" {
            if let Some(source) = event.source.as_deref() {
                if source.starts_with("token_manager:createToken") {
                    println!(
                        "{} skipped createToken liquidity alert for {} ({:#x}) in wallet mode",
                        log_prefix, event.token.symbol, event.token.token
                    );
                    return Ok(());
                }
            }
        }

        if !trade_settings.is_enabled() {
            eprintln!(
                "{} skipped {} ({:#x}) because trade is disabled",
                log_prefix, event.token.symbol, event.token.token
            );
            return Ok(());
        }
        let max_positions = trade_settings.max_positions();
        let open_positions = if let Some(storage) = self.mongo.as_ref() {
            let alert_filter = if is_tweet_alert { Some(alert_type) } else { None };
            match storage.count_open_real_orders(alert_filter).await {
                Ok(count) => count as usize,
                Err(err) => {
                    eprintln!("{} failed to count open orders: {err:?}", log_prefix);
                    self.liquidity_positions.len()
                }
            }
        } else {
            self.liquidity_positions.len()
        };
        if open_positions >= max_positions {
            println!(
                "{} max active trades ({}) reached, skipping {}",
                log_prefix, max_positions, event.token.symbol
            );
            return Ok(());
        }
        if self.liquidity_positions.contains_key(&event.token.token) {
            println!(
                "{} already tracking {} ({:#x})",
                log_prefix, event.token.symbol, event.token.token
            );
            return Ok(());
        }
        let buy_amount = event.buy_amount.max(0.0);
        if buy_amount <= f64::EPSILON {
            eprintln!(
                "{} buy amount is zero for {}",
                log_prefix, event.token.symbol
            );
            return Ok(());
        }
        if self.skip_migrated && self.has_migrated_liquidity(event.token.token).await? {
            println!(
                "[Skip] {} ({:#x}) already has Pancake liquidity (skip_migrated=true)",
                event.token.symbol, event.token.token
            );
            return Ok(());
        }
        println!("----------------------");
        if alert_type == "TokenCreated" {
            println!(
                "🎯 Attempting to buy {} ({:?})",
                event.token.symbol, event.token.token
            );
        }
        println!(
            "[{}] [{}] attempting buy {} ({:#x}) amount {:.6} (method={:?})",
            log_timestamp(),
            alert_type,
            event.token.symbol,
            event.token.token,
            buy_amount,
            self.trade_method
        );
        let mut token_event = TokenEvent {
            contract: event.token.token,
            name: Some(event.token.name.clone()),
            symbol: Some(event.token.symbol.clone()),
            ticker: event.token.symbol.clone(),
            creator: Some(event.token.creator),
            tx_hash: None,
            buy_amount,
        };
        if self.enqueue_live_trades {
            self.buy_token(&token_event).await.map_err(|err| {
                self.log_reverted_order(
                    &event,
                    &token_event,
                    alert_type.to_string(),
                    format!("{:#}", err),
                );
                err
            })?;
            return Ok(());
        }

        let (_amount_raw, amount_tokens, entry_price, tx_hash, entry_value_usd) = if self.trade_method == TradeMethod::Telegram {
                if alert_type == "Tweet Alert" {
                    sleep(Duration::from_millis(100)).await;
                }
                let tx_hash = self.buy_token(&token_event).await.map_err(|err| {
                    self.log_reverted_order(
                        &event,
                        &token_event,
                        alert_type.to_string(),
                        format!("{:#}", err),
                    );
                    err
                })?;
                let mut entry_price = token_manager::fetch_last_price(
                    self.ws_provider.clone(),
                    self.token_manager_address,
                    event.token.token,
                )
                .await
                .unwrap_or(None)
                .unwrap_or(0.0);
                if entry_price <= 0.0 {
                    if let Some(info) = self.fetch_gmgn_info(event.token.token).await {
                        entry_price = info.price.unwrap_or(0.0);
                    }
                }
                (U256::zero(), None, entry_price, tx_hash, None)
            } else {
                let before_balance = token_manager::fetch_token_balance(
                    self.reader_provider.clone(),
                    event.token.token,
                    self.wallet_address,
                )
                .await
                .unwrap_or_else(|err| {
                    eprintln!(
                        "{} [liquidity-trade] failed to read balance for {:#x}: {err:?}",
                        log_timestamp(),
                        event.token.token
                    );
                    U256::zero()
                });
                let tx_hash = self.buy_token(&token_event).await.map_err(|err| {
                    self.log_reverted_order(
                        &event,
                        &token_event,
                        alert_type.to_string(),
                        format!("{:#}", err),
                    );
                    err
                })?;
                println!(
                    "{} [liquidity-trade] reading balance after buy for {:#x}",
                    log_timestamp(),
                    event.token.token
                );
                let mut after_balance = token_manager::fetch_token_balance(
                    self.reader_provider.clone(),
                    event.token.token,
                    self.wallet_address,
                )
                .await
                .unwrap_or_else(|err| {
                    eprintln!(
                        "{} [liquidity-trade] failed to read balance after buy for {:#x}: {err:?}",
                        log_timestamp(),
                        event.token.token
                    );
                    before_balance
                });
                for _ in 0..5 {
                    if after_balance > before_balance {
                        break;
                    }
                    sleep(Duration::from_millis(750)).await;
                    after_balance = token_manager::fetch_token_balance(
                        self.reader_provider.clone(),
                        event.token.token,
                        self.wallet_address,
                    )
                    .await
                    .unwrap_or(after_balance);
                }
                println!(
                    "{} [liquidity-trade] balance after buy for {:#x}: before={} after={}",
                    log_timestamp(),
                    event.token.token, before_balance, after_balance
                );
                let amount_raw = after_balance.saturating_sub(before_balance);
                if amount_raw.is_zero() {
                    eprintln!(
                        "{} [liquidity-trade] zero tokens received for {} ({:#x})",
                        log_timestamp(),
                        event.token.symbol, event.token.token
                    );
                    return Ok(());
                }
                let decimals = match token_manager::fetch_token_decimals(
                    self.reader_provider.clone(),
                    event.token.token,
                )
                .await
                {
                    Ok(value) => value,
                    Err(err) => {
                        eprintln!(
                            "{} [liquidity-trade] failed to fetch decimals for {} ({:#x}): {err:?}",
                            log_timestamp(),
                            event.token.symbol, event.token.token
                        );
                        18
                    }
                };
                let amount_tokens = match token_manager::u256_to_f64(amount_raw, decimals) {
                    Ok(value) if value > f64::EPSILON => value,
                    Ok(_) => {
                        eprintln!(
                            "{} [liquidity-trade] token amount too small for {} ({:#x})",
                            log_timestamp(),
                            event.token.symbol, event.token.token
                        );
                        return Ok(());
                    }
                    Err(err) => {
                        eprintln!(
                            "{} [liquidity-trade] failed to parse token amount for {} ({:#x}): {err:?}",
                            log_timestamp(),
                            event.token.symbol, event.token.token
                        );
                        return Ok(());
                    }
                };
                let bnb_price = token_manager::fetch_bnb_usd_price(&self.ws_provider).await?;
                let entry_value_usd = buy_amount * bnb_price;
                let entry_price = entry_value_usd / amount_tokens;
                (
                    amount_raw,
                    Some(amount_tokens),
                    entry_price,
                    tx_hash,
                    Some(entry_value_usd),
                )
            };
        token_event.tx_hash = Some(tx_hash);
        let (take_profit_price, stop_loss_price) = if entry_price > 0.0 {
            let take_profit_price = if trade_settings.take_profit_pct() > 0.0 {
                entry_price * (1.0 + trade_settings.take_profit_pct() / 100.0)
            } else {
                f64::MAX
            };
            let stop_loss_price = if trade_settings.stop_loss_pct() > 0.0 {
                (entry_price * (1.0 - trade_settings.stop_loss_pct() / 100.0)).max(0.0)
            } else {
                0.0
            };
            (take_profit_price, stop_loss_price)
        } else {
            (f64::MAX, 0.0)
        };
        if is_tweet_alert && self.trade_method == TradeMethod::Telegram {
            if let Some(client) = self.telegram_trade.as_ref() {
                if entry_price > 0.0 {
                    let token_address = format!("{:#x}", event.token.token);
                    let has_tp = trade_settings.take_profit_pct() > 0.0;
                    let has_sl = trade_settings.stop_loss_pct() > 0.0;
                    if has_tp {
                        if let Err(err) = client
                            .send_limit_sell_full(&token_address, take_profit_price)
                            .await
                        {
                            eprintln!(
                                "{} [tweet-trade] failed to send TP for {:#x}: {err:?}",
                                log_timestamp(),
                                event.token.token
                            );
                        }
                    }
                    if has_tp && has_sl {
                        sleep(Duration::from_millis(500)).await;
                    }
                    if has_sl {
                        if let Err(err) = client
                            .send_limit_sell_full(&token_address, stop_loss_price)
                            .await
                        {
                            eprintln!(
                                "{} [tweet-trade] failed to send SL for {:#x}: {err:?}",
                                log_timestamp(),
                                event.token.token
                            );
                        }
                    }
                } else {
                    eprintln!(
                        "{} [tweet-trade] skipped TP/SL for {:#x} because entry price unavailable",
                        log_timestamp(),
                        event.token.token
                    );
                }
            }
        }
        token_event.buy_amount = buy_amount;
        let order_id = if let Some(storage) = self.mongo.clone() {
            let price_source = match self.trade_method {
                TradeMethod::Telegram => "telegram",
                TradeMethod::Wallet => "wallet",
            };
            match storage
                .create_real_order(
                    &event.token,
                    &token_event,
                    tx_hash,
                    entry_price,
                    take_profit_price,
                    stop_loss_price,
                    alert_type,
                    price_source,
                    buy_amount,
                    entry_value_usd,
                    amount_tokens,
                    self.wallet_name.as_deref(),
                    None,
                    None,
                )
                .await
            {
                Ok(order_id) => Some(order_id),
                Err(err) => {
                    eprintln!("[mongo] failed to log real order: {err:?}");
                    None
                }
            }
        } else {
            None
        };
        if is_tweet_alert && self.trade_method == TradeMethod::Telegram {
            if let (Some(storage), Some(order_id)) = (self.mongo.as_ref(), order_id.as_ref()) {
                if let Err(err) = storage
                    .complete_real_order(order_id.clone(), "completed", None, None)
                    .await
                {
                    eprintln!(
                        "[mongo] failed to mark tweet order completed: {err:?}"
                    );
                }
            }
        }
        if let Some(order_id) = order_id.as_ref() {
            if let Err(err) = Self::publish_real_order_event(
                self.redis.as_ref(),
                order_id,
                &event.token,
                self.trade_method,
                alert_type,
            )
            .await
            {
                eprintln!("{} [redis] failed to publish order event: {err:?}", log_timestamp());
            }
        }
        let timestamp = log_timestamp();
        if let Some(amount_tokens) = amount_tokens {
            println!(
                "[{}] [liquidity-trade] Bought {} ({:#x}) amount {:.4} tokens at {:.6} USD",
                timestamp,
                event.token.symbol,
                event.token.token,
                amount_tokens,
                entry_price
            );
        } else {
            println!(
                "[{}] [liquidity-trade] Telegram buy sent for {} ({:#x}) at {:.6} USD",
                timestamp,
                event.token.symbol,
                event.token.token,
                entry_price
            );
        }
        let should_alert = matches!(alert_type, "Liquidity Alert" | "Tweet Alert")
            || (self.trade_method == TradeMethod::Wallet && alert_type == "TokenCreated");
        if should_alert {
            let alert_label = if self.trade_method == TradeMethod::Wallet && alert_type == "TokenCreated" {
                "TokenCreated"
            } else if self.trade_method == TradeMethod::Wallet && alert_type != "Tweet Alert" {
                "Liquidity Alert"
            } else {
                alert_type
            };
            if let Err(err) = self
                .send_trade_alert(&event, alert_label, entry_price)
                .await
            {
                eprintln!("[telegram] failed to send trade alert: {err:?}");
            }
        }
        Ok(())
    }

    async fn handle_smart_money_trade(&self, event: SmartMoneyTradeEvent) -> Result<()> {
        let trade_settings = &self.liquidity_trade;
        if !trade_settings.is_enabled() {
            eprintln!(
                "{} [smart-trade] skipped {} ({:#x}) because trade is disabled",
                log_timestamp(),
                event.token.symbol, event.token.token
            );
            return Ok(());
        }
        println!(
            "[{}] [smart-trade] received {} ({:#x}) follow_bnb={:.6} smart_bnb={:.6}",
            log_timestamp(),
            event.token.symbol,
            event.token.token,
            event.buy_amount,
            event.smart_trade_bnb
        );
        let buy_amount = event.buy_amount.max(0.0);
        if buy_amount <= f64::EPSILON {
            eprintln!(
                "{} [smart-trade] buy amount is zero for {} ({:#x})",
                log_timestamp(),
                event.token.symbol, event.token.token
            );
            return Ok(());
        }
        let trade_event = LiquidityTradeEvent {
            token: event.token.clone(),
            buy_amount,
            liquidity_usd: event.liquidity_usd,
            min_liquidity: self.min_candidate_liquidity,
            source: event.source.clone(),
        };
        println!("----------------------");
        println!(
            "[{}] [smart-trade] attempting buy {} amount {:.6} (method={:?})",
            log_timestamp(),
            event.token.symbol,
            buy_amount,
            self.trade_method
        );
        let mut token_event = TokenEvent {
            contract: event.token.token,
            name: Some(event.token.name.clone()),
            symbol: Some(event.token.symbol.clone()),
            ticker: event.token.symbol.clone(),
            creator: Some(event.token.creator),
            tx_hash: None,
            buy_amount,
        };
        if self.enqueue_live_trades {
            let redis = self
                .redis
                .as_ref()
                .context("live_trade enqueue enabled but redis not configured")?;
            let mut fields = vec![
                ("type", "buy".to_string()),
                ("token", format!("{:#x}", token_event.contract)),
                ("amount_bnb", format!("{:.18}", token_event.buy_amount)),
                ("source", self.alert_source.to_string()),
                ("wallet", format!("{:#x}", self.wallet_address)),
                ("alert_type", "SmartMoney Live".to_string()),
                ("smart_wallet", format!("{:#x}", event.smart_wallet)),
            ];
            if event.smart_trade_bnb > 0.0 {
                fields.push((
                    "smart_trade_bnb",
                    format!("{:.18}", event.smart_trade_bnb),
                ));
            }
            if let Some(name) = self.wallet_name.as_deref() {
                fields.push(("wallet_name", name.to_string()));
            }
            enqueue_live_trade(redis, fields).await.map_err(|err| {
                eprintln!(
                    "{} [smart-trade] buy failed for {}: {err:?}",
                    log_timestamp(),
                    event.token.symbol
                );
                self.log_reverted_order(
                    &trade_event,
                    &token_event,
                    "SmartMoney Live".to_string(),
                    format!("{:#}", err),
                );
                err
            })?;
            println!(
                "{} [smart-trade] queued live trade token={} amount_bnb={:.6} smart_wallet={:#x}",
                log_timestamp(),
                event.token.symbol,
                token_event.buy_amount,
                event.smart_wallet
            );
            return Ok(());
        }

        let (_amount_raw, amount_tokens, entry_price, tx_hash, entry_value_usd) = if self.trade_method == TradeMethod::Telegram {
                let tx_hash = self.buy_token(&token_event).await.map_err(|err| {
                    eprintln!(
                        "{} [smart-trade] buy failed for {}: {err:?}",
                        log_timestamp(),
                        event.token.symbol
                    );
                    self.log_reverted_order(
                        &trade_event,
                        &token_event,
                        "SmartMoney Live".to_string(),
                        format!("{:#}", err),
                    );
                    err
                })?;
                let mut entry_price = token_manager::fetch_last_price(
                    self.ws_provider.clone(),
                    self.token_manager_address,
                    event.token.token,
                )
                .await
                .unwrap_or(None)
                .unwrap_or(0.0);
                if entry_price <= 0.0 {
                    if let Some(info) = self.fetch_gmgn_info(event.token.token).await {
                        entry_price = info.price.unwrap_or(0.0);
                    }
                }
                println!(
                    "[{}] [smart-trade] buy submitted {} tx={:#x}",
                    log_timestamp(),
                    event.token.symbol,
                    tx_hash
                );
                (U256::zero(), None, entry_price, tx_hash, None)
            } else {
                let before_balance = token_manager::fetch_token_balance(
                    self.reader_provider.clone(),
                    event.token.token,
                    self.wallet_address,
                )
                .await
                .unwrap_or(U256::zero());
                let tx_hash = self.buy_token(&token_event).await.map_err(|err| {
                    eprintln!(
                        "{} [smart-trade] buy failed for {}: {err:?}",
                        log_timestamp(),
                        event.token.symbol
                    );
                    self.log_reverted_order(
                        &trade_event,
                        &token_event,
                        "SmartMoney Live".to_string(),
                        format!("{:#}", err),
                    );
                    err
                })?;
                println!(
                    "[{}] [smart-trade] buy submitted {} tx={:#x}",
                    log_timestamp(),
                    event.token.symbol,
                    tx_hash
                );
                let mut after_balance = token_manager::fetch_token_balance(
                    self.reader_provider.clone(),
                    event.token.token,
                    self.wallet_address,
                )
                .await
                .unwrap_or(before_balance);
                for _ in 0..5 {
                    if after_balance > before_balance {
                        break;
                    }
                    sleep(Duration::from_millis(750)).await;
                    after_balance = token_manager::fetch_token_balance(
                        self.reader_provider.clone(),
                        event.token.token,
                        self.wallet_address,
                    )
                    .await
                    .unwrap_or(after_balance);
                }
                let amount_raw = after_balance.saturating_sub(before_balance);
                if amount_raw.is_zero() {
                    eprintln!(
                        "{} [smart-trade] zero tokens received for {}",
                        log_timestamp(),
                        event.token.symbol
                    );
                    return Ok(());
                }
                let decimals = token_manager::fetch_token_decimals(
                    self.reader_provider.clone(),
                    event.token.token,
                )
                .await
                .unwrap_or(18);
                let amount_tokens = token_manager::u256_to_f64(amount_raw, decimals).ok();
                let bnb_price = token_manager::fetch_bnb_usd_price(&self.ws_provider).await?;
                let entry_value_usd = buy_amount * bnb_price;
                let entry_price = amount_tokens
                    .filter(|v| *v > f64::EPSILON)
                    .map(|v| entry_value_usd / v)
                    .unwrap_or(0.0);
                (
                    amount_raw,
                    amount_tokens,
                    entry_price,
                    tx_hash,
                    Some(entry_value_usd),
                )
            };
        token_event.tx_hash = Some(tx_hash);
        let order_id = if let Some(storage) = self.mongo.clone() {
            let price_source = match self.trade_method {
                TradeMethod::Telegram => "telegram",
                TradeMethod::Wallet => "wallet",
            };
            match storage
                .create_real_order(
                    &event.token,
                    &token_event,
                    tx_hash,
                    entry_price,
                    0.0,
                    0.0,
                    "SmartMoney Live",
                    price_source,
                    buy_amount,
                    entry_value_usd,
                    amount_tokens,
                    self.wallet_name.as_deref(),
                    Some(event.smart_wallet),
                    Some(event.smart_trade_bnb),
                )
                .await
            {
                Ok(order_id) => Some(order_id),
                Err(err) => {
                    eprintln!("[mongo] failed to log real order: {err:?}");
                    None
                }
            }
        } else {
            None
        };
        if let Some(order_id) = order_id.as_ref() {
            if let Err(err) = Self::publish_real_order_event(
                self.redis.as_ref(),
                order_id,
                &event.token,
                self.trade_method,
                "SmartMoney Live",
            )
            .await
            {
                eprintln!("{} [redis] failed to publish order event: {err:?}", log_timestamp());
            }
        }
        increment_live_follow_counts(self.redis.as_ref(), event.smart_wallet, event.token.token)
            .await;
        Ok(())
    }

    async fn send_trade_alert(
        &self,
        event: &LiquidityTradeEvent,
        alert_type: &str,
        entry_price: f64,
    ) -> Result<()> {
        let address_hex = format!("{:#x}", event.token.token);
        let gmgn_link = format!("https://gmgn.ai/bsc/token/{}", address_hex);
        let escaped_name = escape_html(&event.token.name);
        let escaped_symbol = escape_html(&event.token.symbol);
        if alert_type == "Tweet Alert" {
            let message = format!(
                "<b>Tweet Mention Alert</b>\nName: {name}\nSymbol: {symbol}\nAddress: <code>{address}</code>\n<a href=\"{url}\">View on GMGN</a>",
                name = escaped_name,
                symbol = escaped_symbol,
                address = address_hex,
                url = gmgn_link
            );
            Self::send_telegram_message(&self.telegram, self.telegram.tweet_chat(), &message).await?;
            return Ok(());
        }

        let liquidity_display = match event.liquidity_usd {
            Some(value) => format!("{value:.4}"),
            None if self.trade_method == TradeMethod::Wallet => "unknown".to_string(),
            None => return Ok(()),
        };
        let price_display = if entry_price > 0.0 {
            fmt_price(entry_price)
        } else {
            "unknown".to_string()
        };
        let source_line = event
            .source
            .as_deref()
            .map(|value| format!("\nSource: {}", escape_html(value)))
            .unwrap_or_default();
        let message = format!(
            "<b>{title}</b>\nName: {name}\nSymbol: {symbol}\nAddress: <code>{address}</code>\nLiquidity: {liq}\nPrice: {price}\nCreated: unknown (MYT)\nThreshold: {min:.2}{source}\n<a href=\"{url}\">View on GMGN</a>",
            title = alert_type,
            name = escaped_name,
            symbol = escaped_symbol,
            address = address_hex,
            liq = liquidity_display,
            price = price_display,
            min = event.min_liquidity,
            source = source_line,
            url = gmgn_link
        );
        Self::send_telegram_message(
            &self.telegram,
            self.telegram.watchlist_chat(),
            &message,
        )
        .await?;
        Ok(())
    }

    async fn send_telegram_message(
        telegram: &TelegramSettings,
        chat_id: Option<&str>,
        message: &str,
    ) -> Result<()> {
        let token_value = match telegram.token() {
            Some(value) => value,
            None => return Ok(()),
        };
        let chat_value = match chat_id {
            Some(chat) => chat,
            None => return Ok(()),
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
        Ok(())
    }

    async fn publish_real_order_event(
        redis: Option<&RedisClient>,
        order_id: &ObjectId,
        token: &TokenCreated,
        trade_method: TradeMethod,
        alert_type: &str,
    ) -> Result<()> {
        let redis = match redis {
            Some(client) => client,
            None => return Ok(()),
        };
        let mut conn = redis.get_multiplexed_async_connection().await?;
        let trade_method = match trade_method {
            TradeMethod::Telegram => "telegram",
            TradeMethod::Wallet => "wallet",
        };
        let _: String = redis::cmd("XADD")
            .arg(ORDER_STREAM_KEY)
            .arg("*")
            .arg("type")
            .arg("real_order")
            .arg("order_id")
            .arg(order_id.to_hex())
            .arg("token")
            .arg(format!("{:#x}", token.token))
            .arg("alert_type")
            .arg(alert_type)
            .arg("trade_method")
            .arg(trade_method)
            .query_async(&mut conn)
            .await?;
        Ok(())
    }

    #[allow(dead_code)]
    fn spawn_liquidity_monitor(&self, position: LiquidityPosition) {
        let token_address = position.token.token;
        self.liquidity_positions
            .insert(token_address, position.clone());
        if let Some(counter) = self.active_trades.as_ref() {
            counter.fetch_add(1, Ordering::SeqCst);
        }
        let ws_provider = self.ws_provider.clone();
        let contract = self.contract.clone();
        let positions = self.liquidity_positions.clone();
        let manager_address = self.token_manager_address;
        let helper_address = self.helper_address;
        let pancake_v2_router = self.pancake_v2_router;
        let pancake_v3_router = self.pancake_v3_router;
        let wallet_address = self.wallet_address;
        let reader_provider = self.reader_provider.clone();
        let redis = self.redis.clone();
        let mongo = self.mongo.clone();
        let sell_sl_per_block_pct = self.liquidity_trade.sell_sl_per_block_pct();
        let trade_method = self.trade_method;
        let telegram_trade = self.telegram_trade.clone();
        let active_trades = self.active_trades.clone();
        let sell_all_migrated = self.sell_all_migrated;
        let enqueue_live_trades = self.enqueue_live_trades;
        tokio::spawn(async move {
            if let Err(err) = Self::monitor_liquidity_position(
                ws_provider,
                contract,
                manager_address,
                helper_address,
                pancake_v2_router,
                pancake_v3_router,
                reader_provider,
                wallet_address,
                redis,
                enqueue_live_trades,
                None,
                mongo,
                sell_sl_per_block_pct,
                trade_method,
                telegram_trade,
                56,
                position.clone(),
                positions.clone(),
                active_trades.clone(),
                None,
                sell_all_migrated,
                None,
                Duration::from_millis(0),
            )
            .await
            {
                eprintln!(
                    "{} [liquidity-monitor] failed for {} ({:#x}): {err:?}",
                    log_timestamp(),
                    position.token.symbol, position.token.token
                );
            }
        });
    }

    pub async fn monitor_liquidity_position(
        ws_provider: Arc<Provider<Ws>>,
        contract: Contract<Client>,
        manager_address: Address,
        helper_address: Address,
        pancake_v2_router: Address,
        pancake_v3_router: Address,
        reader_provider: Arc<RpcProvider>,
        wallet_address: Address,
        redis: Option<RedisClient>,
        enqueue_live_trades: bool,
        ave_client: Option<Arc<AveClient>>,
        mongo: Option<Arc<MongoLogger>>,
        sell_sl_per_block_pct: f64,
        trade_method: TradeMethod,
        telegram_trade: Option<Arc<TelegramTradeClient>>,
        chain_id: u64,
        position: LiquidityPosition,
        positions: Arc<DashMap<Address, LiquidityPosition>>,
        active_trades: Option<Arc<AtomicUsize>>,
        gmgn_client: Option<Arc<GmgnClient>>,
        sell_all_migrated: bool,
        sell_all_liquidity: Option<f64>,
        telegram_delay: Duration,
    ) -> Result<()> {
        let mut blocks = ws_provider.subscribe_blocks().await?;
        let mut last_heartbeat = Instant::now();
        let mut last_liquidity_check = Instant::now() - Duration::from_secs(60);
        let mut sell_failures: u8 = 0;
        while let Some(block) = blocks.next().await {
            let helper_info = match token_manager::fetch_helper_info(
                ws_provider.clone(),
                helper_address,
                position.token.token,
            )
            .await
            {
                Ok(info) => info,
                Err(err) => {
                    eprintln!(
                        "{} [liquidity-monitor] helper info fetch failed for {}: {err:?}",
                        log_timestamp(),
                        position.token.symbol
                    );
                    None
                }
            };

            let mut migrated = false;
            let mut price: Option<f64> = None;
            let mut pair: Option<Address> = None;
            let mut quote: Option<Address> = None;
            if let Some(info) = helper_info.as_ref() {
                quote = Some(info.quote);
                pair = info.pancake_pair;
                if pair.is_none() {
                    if let Ok(derived) = token_manager::derive_pancake_pair(
                        ws_provider.clone(),
                        info.pancake_factory,
                        position.token.token,
                        info.quote,
                    )
                    .await
                    {
                        pair = derived;
                    }
                }
                if info.liquidity_added || pair.is_some() {
                    migrated = true;
                }
                if let Some(pair_addr) = pair {
                    if let Ok(Some(pair_price)) = token_manager::fetch_pancake_pair_price_usd(
                        ws_provider.clone(),
                        pair_addr,
                        position.token.token,
                    )
                    .await
                    {
                        price = Some(pair_price);
                    }
                }
            }

            if price.is_none() {
                price = match token_manager::fetch_last_price(
                    ws_provider.clone(),
                    manager_address,
                    position.token.token,
                )
                .await
                {
                    Ok(Some(value)) => Some(value),
                    Ok(None) => {
                        if last_heartbeat.elapsed() >= Duration::from_secs(5) {
                            println!(
                                "{} [liquidity-monitor] token={} block={:?} price=unavailable",
                                log_timestamp(),
                                position.token.symbol,
                                block.number
                            );
                            last_heartbeat = Instant::now();
                        }
                        None
                    }
                    Err(err) => {
                        eprintln!(
                            "{} [liquidity-monitor] price fetch failed for {} at block {:?}: {err:?}",
                            log_timestamp(),
                            position.token.symbol, block.number
                        );
                        None
                    }
                };
                if price.is_none() {
                    continue;
                }
            }
            let price = price.unwrap();
            let mut should_sell = false;
            let mut sell_reason = "tp/sl";
            let mut hit_reason: Option<&'static str> = None;

            if last_heartbeat.elapsed() >= Duration::from_secs(5) {
                println!(
                    "{} [liquidity-monitor] token={} block={:?} price={:.10} entry={:.10} tp={:.10} sl={:.10}",
                    log_timestamp(),
                    position.token.symbol,
                    block.number,
                    price,
                    position.entry_price,
                    position.take_profit_price,
                    position.stop_loss_price
                );
                last_heartbeat = Instant::now();
            }

            if trade_method == TradeMethod::Wallet && sell_all_migrated && migrated {
                should_sell = true;
                sell_reason = "migrated";
                hit_reason = Some("MIGRATED");
            }

            if !should_sell {
                if position.take_profit_price.is_finite() && price >= position.take_profit_price {
                    should_sell = true;
                    sell_reason = "tp";
                    hit_reason = Some("TP");
                } else if position.stop_loss_price > 0.0 && price <= position.stop_loss_price {
                    should_sell = true;
                    sell_reason = "sl";
                    hit_reason = Some("SL");
                }
            }

            if !should_sell {
                if let Some(threshold) = sell_all_liquidity {
                    if last_liquidity_check.elapsed() >= Duration::from_secs(2) {
                        let mut liquidity = None;
                        if let (Some(pair_addr), Some(quote)) = (pair, quote) {
                            if let Ok(value) = token_manager::fetch_pancake_pair_liquidity_usd(
                                ws_provider.clone(),
                                pair_addr,
                                position.token.token,
                                quote,
                            )
                            .await
                            {
                                liquidity = value;
                            }
                        }
                        if liquidity.is_none() {
                            if let Some(client) = gmgn_client.as_ref() {
                                if let Some(info) =
                                    client.fetch_token_info_fresh(position.token.token).await
                                {
                                    liquidity = info.liquidity;
                                }
                            }
                        }
                        if let Some(liq) = liquidity {
                            if liq <= threshold {
                                should_sell = true;
                                sell_reason = "liquidity";
                            }
                        }
                        last_liquidity_check = Instant::now();
                    }
                }
            }

            if !should_sell && sell_sl_per_block_pct > 0.0 {
                if let Some(redis_client) = redis.clone() {
                    match Self::load_cached_price(
                        &redis_client,
                        position.token.token,
                        position.order_id.as_ref(),
                    )
                    .await
                    {
                        Ok(Some(prev_price)) if prev_price > 0.0 => {
                            let threshold = prev_price * (1.0 - sell_sl_per_block_pct / 100.0);
                            if price < threshold {
                                should_sell = true;
                                sell_reason = "per-block sl";
                            }
                        }
                        Ok(None) => {
                            if let Err(err) = Self::store_cached_price(
                                &redis_client,
                                position.token.token,
                                price,
                                position.order_id.as_ref(),
                            )
                            .await
                            {
                                eprintln!(
                                    "{} [liquidity-monitor] failed to store price for {:#x}: {err:?}",
                                    log_timestamp(),
                                    position.token.token
                                );
                            }
                        }
                        Err(err) => {
                            eprintln!(
                                "{} [liquidity-monitor] failed to read cached price for {:#x}: {err:?}",
                                log_timestamp(),
                                position.token.token
                            );
                        }
                        _ => {}
                    }
                }
            }

            if should_sell {
                if let Some(hit) = hit_reason {
                    println!("----------------------");
                    println!(
                        "{} [liquidity-monitor] {} hit token={} block={:?} price={:.10} entry={:.10} tp={:.10} sl={:.10}",
                        log_timestamp(),
                        hit,
                        position.token.symbol,
                        block.number,
                        price,
                        position.entry_price,
                        position.take_profit_price,
                        position.stop_loss_price
                    );
                }
                let tx_hash = if enqueue_live_trades {
                    let redis = redis
                        .as_ref()
                        .context("live_trade enqueue enabled but redis not configured")?;
                    enqueue_live_trade(
                        redis,
                        vec![
                            ("type", "sell".to_string()),
                            ("token", format!("{:#x}", position.token.token)),
                            ("amount_raw", position.amount_raw.to_string()),
                            ("source", "liquidity-monitor".to_string()),
                            ("wallet", format!("{:#x}", wallet_address)),
                        ],
                    )
                    .await
                    .map_err(|err| anyhow!("enqueue live sell failed: {err:#}"))?;
                    println!(
                        "{} [liquidity-monitor] queued live sell token={} amount_raw={} reason={}",
                        log_timestamp(),
                        position.token.symbol,
                        position.amount_raw,
                        sell_reason
                    );
                    H256::zero()
                } else {
                    match Self::sell_position(
                        contract.clone(),
                        helper_address,
                        pancake_v2_router,
                        pancake_v3_router,
                        reader_provider.clone(),
                        wallet_address,
                        trade_method,
                        telegram_trade.clone(),
                        redis.as_ref(),
                        ave_client.clone(),
                        chain_id,
                        position.token.token,
                        position.amount_raw,
                        telegram_delay,
                    )
                    .await
                    {
                        Ok(hash) => hash,
                        Err(err) => {
                            sell_failures = sell_failures.saturating_add(1);
                            eprintln!(
                                "{} [liquidity-monitor] sell attempt {}/3 failed for {}: {err:?}",
                                log_timestamp(),
                                sell_failures,
                                position.token.symbol,
                            );
                            if sell_failures >= 3 {
                                eprintln!(
                                    "{} [liquidity-monitor] max sell attempts reached for {}, giving up",
                                    log_timestamp(),
                                    position.token.symbol
                                );
                                break;
                            }
                            continue;
                        }
                    }
                };
                if let (Some(storage), Some(order_id)) =
                    (mongo.as_ref(), position.order_id.clone())
                {
                let status = if sell_reason == "per-block sl" {
                    "sl"
                } else if sell_reason == "liquidity" {
                    "sl"
                } else if position.take_profit_price.is_finite()
                    && price >= position.take_profit_price
                {
                    "tp"
                } else if position.stop_loss_price > 0.0 && price <= position.stop_loss_price
                {
                    "sl"
                } else {
                    "sold"
                };
                    let profit_usd = position.amount_tokens.and_then(|amount_tokens| {
                        if position.entry_price > 0.0 {
                            Some((price - position.entry_price) * amount_tokens)
                        } else {
                            None
                        }
                    });
                    if let Err(err) = storage
                        .complete_real_order(order_id, status, Some(price), profit_usd)
                        .await
                    {
                        eprintln!("[mongo] failed to update real order: {err:?}");
                    }
                }
                println!(
                    "[{}] [liquidity-trade] Sold {} ({:#x}) at {:.6} USD (entry {:.6}) reason={} tx={tx_hash:?}",
                    log_timestamp(),
                    position.token.symbol,
                    position.token.token,
                    price,
                    position.entry_price,
                    sell_reason
                );
                if let Some(redis_client) = redis.clone() {
                    if let Err(err) = Self::delete_cached_price(
                        &redis_client,
                        position.token.token,
                        position.order_id.as_ref(),
                    )
                    .await
                    {
                        eprintln!(
                            "{} [liquidity-monitor] failed to delete cached price for {:#x}: {err:?}",
                            log_timestamp(),
                            position.token.token
                        );
                    }
                }
                if positions.remove(&position.token.token).is_some() {
                    if let Some(counter) = active_trades.as_ref() {
                        counter.fetch_sub(1, Ordering::SeqCst);
                    }
                }
                break;
            }

            if let Some(redis_client) = redis.clone() {
                if let Err(err) = Self::store_cached_price(
                    &redis_client,
                    position.token.token,
                    price,
                    position.order_id.as_ref(),
                )
                .await
                {
                    eprintln!(
                        "{} [liquidity-monitor] failed to store price for {:#x}: {err:?}",
                        log_timestamp(),
                        position.token.token
                    );
                }
            }
        }
        if positions.remove(&position.token.token).is_some() {
            if let Some(counter) = active_trades.as_ref() {
                counter.fetch_sub(1, Ordering::SeqCst);
            }
        }
        Ok(())
    }

    pub async fn sell_position(
        contract: Contract<Client>,
        helper_address: Address,
        pancake_v2_router: Address,
        pancake_v3_router: Address,
        provider: Arc<RpcProvider>,
        wallet: Address,
        trade_method: TradeMethod,
        telegram_trade: Option<Arc<TelegramTradeClient>>,
        _redis: Option<&RedisClient>,
        _ave_client: Option<Arc<AveClient>>,
        _chain_id: u64,
        token: Address,
        amount: U256,
        telegram_delay: Duration,
    ) -> Result<H256> {
        let token_label = resolve_token_label_from_chain(provider.clone(), token).await;
        if trade_method == TradeMethod::Telegram {
            let client = telegram_trade
                .as_ref()
                .context("telegram trade client not configured")?;
            if !telegram_delay.is_zero() {
                sleep(telegram_delay).await;
            }
            client
                .send_sell_percent(&format!("{:#x}", token), 100)
                .await?;
            println!(
                "[{}] [liquidity-trade] Sent telegram /sell for {} (100%)",
                log_timestamp(),
                token_label
            );
            return Ok(H256::zero());
        }

        let client = contract.client();
        let erc20 = ERC20::new(token, client.clone());

        let balance = erc20
            .balance_of(wallet)
            .call()
            .await
            .context("failed to read token balance")?;
        if balance.is_zero() {
            return Err(anyhow!("token balance is zero"));
        }
        let sell_amount = if amount > U256::zero() {
            if amount > balance { balance } else { amount }
        } else {
            balance
        };

        if !helper_address.is_zero()
            && (!pancake_v2_router.is_zero() || !pancake_v3_router.is_zero())
        {
            let helper_abi: Abi = from_slice(include_bytes!("abi/TokenManagerHelper3.abi"))
                .context("failed to parse TokenManagerHelper ABI")?;
            let helper3 = Contract::new(helper_address, helper_abi, contract.client().clone());
            if let Ok(builder) =
                helper3.method::<_, token_manager::HelperTokenInfo>("getTokenInfo", token)
            {
                if let Err(err) = builder.call().await {
                            eprintln!(
                                "{} [helper3-sell] helper getTokenInfo failed for {}: {err:?}",
                                log_timestamp(),
                                token_label
                            );
                }
            } else {
                    eprintln!(
                        "{} [helper3-sell] helper getTokenInfo build failed for {}",
                        log_timestamp(),
                        token_label
                    );
            }
            let wbnb: Address = helper3
                .method("WETH", ())
                .context("build helper WETH call")?
                .call()
                .await
                .unwrap_or_else(|err| {
                    eprintln!("{} [helper3-sell] failed to read WETH: {err:?}", log_timestamp());
                    Address::zero()
                });
            let wbnb = if wbnb.is_zero() { *WBNB_ADDRESS } else { wbnb };
            if !pancake_v2_router.is_zero() {
                let mut pair = helper3
                    .method::<_, Address>("getPancakePair", token)
                    .context("build helper getPancakePair call")?
                    .call()
                    .await
                    .unwrap_or_else(|err| {
                                eprintln!(
                                    "{} [helper3-sell] failed to read helper pancake pair for {}: {err:?}",
                                    log_timestamp(),
                                    token_label
                                );
                        Address::zero()
                    });
                if pair.is_zero() {
                    let factory: Address = helper3
                        .method("PANCAKE_FACTORY", ())
                        .context("build helper PANCAKE_FACTORY call")?
                        .call()
                        .await
                        .unwrap_or_else(|err| {
                            eprintln!("{} [helper3-sell] failed to read PANCAKE_FACTORY: {err:?}", log_timestamp());
                            Address::zero()
                        });
                    if !factory.is_zero() {
                        let factory_abi = AbiParser::default()
                            .parse(&["function getPair(address,address) view returns (address)"])
                            .context("failed to parse Pancake factory ABI")?;
                        let factory_contract =
                            Contract::new(factory, factory_abi, helper3.client().clone());
                        pair = factory_contract
                            .method("getPair", (token, wbnb))
                            .context("build getPair call")?
                            .call()
                            .await
                            .unwrap_or_else(|err| {
                                    eprintln!(
                                        "{} [helper3-sell] failed to read Pancake pair for {}: {err:?}",
                                        log_timestamp(),
                                        token_label
                                    );
                                Address::zero()
                            });
                    }
                }
                if !pair.is_zero() {
                    return Self::pancake_v2_sell_for_eth(
                        pancake_v2_router,
                        client.clone(),
                        wallet,
                        token,
                        sell_amount,
                        U256::zero(),
                        wbnb,
                    )
                    .await;
                }
            }
            if !pancake_v3_router.is_zero() {
                let v3_factory: Address = helper3
                    .method("PANCAKE_V3_FACTORY", ())
                    .context("build helper PANCAKE_V3_FACTORY call")?
                    .call()
                    .await
                    .unwrap_or_else(|err| {
                        eprintln!("{} [helper3-sell] failed to read PANCAKE_V3_FACTORY: {err:?}", log_timestamp());
                        Address::zero()
                    });
                if !v3_factory.is_zero() {
                    if let Some(fee) =
                        Self::find_v3_fee_tier(v3_factory, client.clone(), token, wbnb).await
                    {
                        return Self::pancake_v3_sell_for_eth(
                            pancake_v3_router,
                            client.clone(),
                            wallet,
                            token,
                            sell_amount,
                            U256::zero(),
                            wbnb,
                            fee,
                        )
                        .await;
                    }
                }
            }
        }

        let min_out: U256 = U256::zero();
        let spender: Address = contract.address();
        let allowance = erc20
            .allowance(wallet, spender)
            .call()
            .await
            .context("failed to read allowance")?;
        if allowance < sell_amount {
            println!(
                "{} [liquidity-trade] Allowance {} < amount {}. Approving spender {:#x} for {}...",
                log_timestamp(),
                allowance, sell_amount, spender, token_label
            );
            let approve_call = erc20.approve(spender, U256::MAX);
            let approve_pending = approve_call
                .send()
                .await
                .context("approve submission failed")?;
            let approve_hash = approve_pending.tx_hash();
            println!(
                "{} [liquidity-trade] approve tx submitted: {approve_hash:?}, waiting...",
                log_timestamp(),
            );
            let approve_receipt = approve_pending
                .await
                .context("approve tx dropped before confirmation")?
                .context("approve tx receipt missing")?;
            if approve_receipt.status != Some(U64::from(1)) {
                return Err(anyhow!("approve transaction reverted on-chain"));
            }
        }
        let hash = keccak256("sellToken(address,uint256,uint256)".as_bytes());
        let selector: [u8; 4] = hash[0..4].try_into().unwrap();
        let call = contract
            .method_hash::<_, ()>(selector, (token, sell_amount, min_out))
            .context("failed to build sellToken(address,uint256,uint256) call")?;
        let tx_debug = call.tx.clone();
        let pending_tx = match call.send().await {
            Ok(value) => value,
            Err(err) => {
                log_tx_debug(
                    "token-manager sell",
                    "sellToken(address,uint256,uint256)",
                    &tx_debug,
                );
                return Err(anyhow!("sell transaction failed to submit: {err:?}"));
            }
        };
        let tx_hash = pending_tx.tx_hash();
        let receipt = pending_tx
            .await
            .context("sell transaction dropped before inclusion")?;
        let receipt = receipt.context("sell transaction missing receipt")?;
        match receipt.status {
            Some(status) if status == U64::from(1) => Ok(tx_hash),
            _ => {
                log_tx_debug(
                    "token-manager sell",
                    "sellToken(address,uint256,uint256)",
                    &tx_debug,
                );
                Err(anyhow!(
                    "sell transaction reverted (tx={:#x}, status={:?}, block={:?}, gas_used={})",
                    tx_hash,
                    receipt.status,
                    receipt.block_number,
                    receipt.gas_used.unwrap_or_default()
                ))
            }
        }
    }

    async fn pancake_v2_sell_for_eth(
        router: Address,
        client: Arc<Client>,
        wallet: Address,
        token: Address,
        amount: U256,
        min_out: U256,
        wbnb: Address,
    ) -> Result<H256> {
        if amount.is_zero() {
            return Err(anyhow!("sell amount must be greater than zero"));
        }

        let erc20 = ERC20::new(token, client.clone());
        let allowance = erc20
            .allowance(wallet, router)
            .call()
            .await
            .context("failed to read allowance")?;
        if allowance < amount {
            println!(
                "{} [pancake-v2] Allowance {} < amount {}. Approving router {:#x}...",
                log_timestamp(),
                allowance, amount, router
            );
            let approve_call = erc20.approve(router, U256::MAX);
            let approve_pending = approve_call
                .send()
                .await
                .context("approve submission failed")?;
            let approve_hash = approve_pending.tx_hash();
            println!("{} [pancake-v2] approve tx submitted: {approve_hash:?}, waiting...", log_timestamp());
            let approve_receipt = approve_pending
                .await
                .context("approve tx dropped before confirmation")?
                .context("approve tx receipt missing")?;
            if approve_receipt.status != Some(U64::from(1)) {
                return Err(anyhow!("approve transaction reverted on-chain"));
            }
        }

        let router_abi = AbiParser::default()
            .parse(&[
                "function swapExactTokensForETHSupportingFeeOnTransferTokens(uint256,uint256,address[],address,uint256)",
            ])
            .context("failed to parse Pancake router ABI")?;
        let router_contract = Contract::new(router, router_abi, client.clone());
        let deadline = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock error")?
            .as_secs()
            .saturating_add(120);
        let path = vec![token, wbnb];
        let call = router_contract
            .method::<_, ()>(
                "swapExactTokensForETHSupportingFeeOnTransferTokens",
                (amount, min_out, path, wallet, U256::from(deadline)),
            )
            .context("failed to build router sell call")?;
        let tx_debug = call.tx.clone();
        let pending_tx = match call.send().await {
            Ok(value) => value,
            Err(err) => {
                log_tx_debug(
                    "pancake-v2 sell",
                    "swapExactTokensForETHSupportingFeeOnTransferTokens",
                    &tx_debug,
                );
                return Err(anyhow!(
                    "router sell transaction failed to submit: {err:?}"
                ));
            }
        };
        let tx_hash = pending_tx.tx_hash();
        let receipt = pending_tx
            .await
            .context("router sell transaction dropped before inclusion")?;
        let receipt = receipt.context("router sell transaction missing receipt")?;
        match receipt.status {
            Some(status) if status == U64::from(1) => Ok(tx_hash),
            _ => {
                log_tx_debug(
                    "pancake-v2 sell",
                    "swapExactTokensForETHSupportingFeeOnTransferTokens",
                    &tx_debug,
                );
                Err(anyhow!(
                    "router sell transaction reverted (tx={:#x}, status={:?}, block={:?}, gas_used={})",
                    tx_hash,
                    receipt.status,
                    receipt.block_number,
                    receipt.gas_used.unwrap_or_default()
                ))
            }
        }
    }

    async fn pancake_v2_buy_for_eth(
        router: Address,
        client: Arc<Client>,
        wallet: Address,
        token: Address,
        amount_in: U256,
        min_out: U256,
        wbnb: Address,
    ) -> Result<H256> {
        if amount_in.is_zero() {
            return Err(anyhow!("buy amount must be greater than zero"));
        }

        let router_abi = AbiParser::default()
            .parse(&[
                "function swapExactETHForTokensSupportingFeeOnTransferTokens(uint256,address[],address,uint256)",
            ])
            .context("failed to parse Pancake router ABI")?;
        let router_contract = Contract::new(router, router_abi, client);
        let deadline = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock error")?
            .as_secs()
            .saturating_add(120);
        let path = vec![wbnb, token];
        let call = router_contract
            .method::<_, ()>(
                "swapExactETHForTokensSupportingFeeOnTransferTokens",
                (min_out, path, wallet, U256::from(deadline)),
            )
            .context("failed to build router buy call")?
            .value(amount_in);
        let tx_debug = call.tx.clone();
        let pending_tx = match call.send().await {
            Ok(value) => value,
            Err(err) => {
                log_tx_debug(
                    "pancake-v2 buy",
                    "swapExactETHForTokensSupportingFeeOnTransferTokens",
                    &tx_debug,
                );
                return Err(anyhow!(
                    "router buy transaction failed to submit (router={:#x}, amount_in={}, min_out={}, deadline={}, path={:#x}->{:#x}): {err:?}",
                    router,
                    amount_in,
                    min_out,
                    deadline,
                    wbnb,
                    token
                ));
            }
        };
        let tx_hash = pending_tx.tx_hash();
        let receipt = pending_tx
            .await
            .context("router buy transaction dropped before inclusion")?;
        let receipt = receipt.context("router buy transaction missing receipt")?;
        match receipt.status {
            Some(status) if status == U64::from(1) => Ok(tx_hash),
            _ => {
                log_tx_debug(
                    "pancake-v2 buy",
                    "swapExactETHForTokensSupportingFeeOnTransferTokens",
                    &tx_debug,
                );
                Err(anyhow!(
                    "router buy transaction reverted (tx={:#x}, status={:?}, block={:?}, gas_used={})",
                    tx_hash,
                    receipt.status,
                    receipt.block_number,
                    receipt.gas_used.unwrap_or_default()
                ))
            }
        }
    }

    async fn find_v3_fee_tier(
        factory: Address,
        client: Arc<Client>,
        token: Address,
        wbnb: Address,
    ) -> Option<u32> {
        let factory_abi = AbiParser::default()
            .parse(&["function getPool(address,address,uint24) view returns (address)"])
            .ok()?;
        let factory_contract = Contract::new(factory, factory_abi, client);
        for fee in [100u32, 500u32, 2500u32, 10000u32] {
            match factory_contract.method::<_, Address>("getPool", (token, wbnb, fee)) {
                Ok(call) => match call.call().await {
                    Ok(pool) if !pool.is_zero() => return Some(fee),
                    _ => {}
                },
                Err(_) => {}
            }
        }
        None
    }

    async fn pancake_v3_sell_for_eth(
        router: Address,
        client: Arc<Client>,
        wallet: Address,
        token: Address,
        amount: U256,
        min_out: U256,
        wbnb: Address,
        fee: u32,
    ) -> Result<H256> {
        if amount.is_zero() {
            return Err(anyhow!("sell amount must be greater than zero"));
        }

        let erc20 = ERC20::new(token, client.clone());
        let allowance = erc20
            .allowance(wallet, router)
            .call()
            .await
            .context("failed to read allowance")?;
        if allowance < amount {
            println!(
                "{} [pancake-v3] Allowance {} < amount {}. Approving router {:#x}...",
                log_timestamp(),
                allowance, amount, router
            );
            let approve_call = erc20.approve(router, U256::MAX);
            let approve_pending = approve_call
                .send()
                .await
                .context("approve submission failed")?;
            let approve_hash = approve_pending.tx_hash();
            println!("{} [pancake-v3] approve tx submitted: {approve_hash:?}, waiting...", log_timestamp());
            let approve_receipt = approve_pending
                .await
                .context("approve tx dropped before confirmation")?
                .context("approve tx receipt missing")?;
            if approve_receipt.status != Some(U64::from(1)) {
                return Err(anyhow!("approve transaction reverted on-chain"));
            }
        }

        let router_abi = AbiParser::default()
            .parse(&["function exactInputSingle(tuple(address,address,uint24,address,uint256,uint256,uint256,uint160)) payable returns (uint256)"])
            .context("failed to parse Pancake V3 router ABI")?;
        let router_contract = Contract::new(router, router_abi, client.clone());
        let deadline = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock error")?
            .as_secs()
            .saturating_add(120);
        let params = (
            token,
            wbnb,
            fee,
            wallet,
            U256::from(deadline),
            amount,
            min_out,
            U256::zero(),
        );
        let call = router_contract
            .method::<_, U256>("exactInputSingle", params)
            .context("failed to build router V3 sell call")?;
        let tx_debug = call.tx.clone();
        let pending_tx = match call.send().await {
            Ok(value) => value,
            Err(err) => {
                log_tx_debug("pancake-v3 sell", "exactInputSingle", &tx_debug);
                return Err(anyhow!(
                    "router V3 sell transaction failed to submit: {err:?}"
                ));
            }
        };
        let tx_hash = pending_tx.tx_hash();
        let receipt = pending_tx
            .await
            .context("router V3 sell transaction dropped before inclusion")?;
        let receipt = receipt.context("router V3 sell transaction missing receipt")?;
        match receipt.status {
            Some(status) if status == U64::from(1) => Ok(tx_hash),
            _ => {
                log_tx_debug("pancake-v3 sell", "exactInputSingle", &tx_debug);
                Err(anyhow!(
                    "router V3 sell transaction reverted (tx={:#x}, status={:?}, block={:?}, gas_used={})",
                    tx_hash,
                    receipt.status,
                    receipt.block_number,
                    receipt.gas_used.unwrap_or_default()
                ))
            }
        }
    }

    async fn pancake_v3_buy_for_eth(
        router: Address,
        client: Arc<Client>,
        wallet: Address,
        token: Address,
        amount_in: U256,
        min_out: U256,
        wbnb: Address,
        fee: u32,
    ) -> Result<H256> {
        if amount_in.is_zero() {
            return Err(anyhow!("buy amount must be greater than zero"));
        }

        let router_abi = AbiParser::default()
            .parse(&["function exactInputSingle(tuple(address,address,uint24,address,uint256,uint256,uint256,uint160)) payable returns (uint256)"])
            .context("failed to parse Pancake V3 router ABI")?;
        let router_contract = Contract::new(router, router_abi, client);
        let deadline = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock error")?
            .as_secs()
            .saturating_add(120);
        let params = (
            wbnb,
            token,
            fee,
            wallet,
            U256::from(deadline),
            amount_in,
            min_out,
            U256::zero(),
        );
        let call = router_contract
            .method::<_, U256>("exactInputSingle", params)
            .context("failed to build router V3 buy call")?
            .value(amount_in);
        let tx_debug = call.tx.clone();
        let pending_tx = match call.send().await {
            Ok(value) => value,
            Err(err) => {
                log_tx_debug("pancake-v3 buy", "exactInputSingle", &tx_debug);
                return Err(anyhow!(
                    "router V3 buy transaction failed to submit (router={:#x}, amount_in={}, min_out={}, fee={}, deadline={}, token={:#x}): {err:?}",
                    router,
                    amount_in,
                    min_out,
                    fee,
                    deadline,
                    token
                ));
            }
        };
        let tx_hash = pending_tx.tx_hash();
        let receipt = pending_tx
            .await
            .context("router V3 buy transaction dropped before inclusion")?;
        let receipt = receipt.context("router V3 buy transaction missing receipt")?;
        match receipt.status {
            Some(status) if status == U64::from(1) => Ok(tx_hash),
            _ => {
                log_tx_debug("pancake-v3 buy", "exactInputSingle", &tx_debug);
                Err(anyhow!(
                    "router V3 buy transaction reverted (tx={:#x}, status={:?}, block={:?}, gas_used={})",
                    tx_hash,
                    receipt.status,
                    receipt.block_number,
                    receipt.gas_used.unwrap_or_default()
                ))
            }
        }
    }

    pub async fn helper3_sell_for_eth(
        helper3: &Contract<Client>,
        provider: Arc<RpcProvider>,
        wallet: Address,
        token: Address,
        amount: U256,
        origin: U256,
        min_funds: U256,
        fee_rate: U256,
        fee_recipient: Address,
    ) -> Result<H256> {
        if amount.is_zero() {
            return Err(anyhow!("sell amount must be greater than zero"));
        }

        let balance = token_manager::fetch_token_balance(provider.clone(), token, wallet)
            .await
            .context("failed to read token balance")?;
        if balance < amount {
            return Err(anyhow!(
                "insufficient token balance: have {}, need {}",
                balance,
                amount
            ));
        }

        let spender = helper3.address();
        let client = helper3.client();
        let erc20 = ERC20::new(token, client.clone());
        let allowance = erc20
            .allowance(wallet, spender)
            .call()
            .await
            .context("failed to read allowance")?;
        if allowance < amount {
            println!(
                "{} [helper3-sell] Allowance {} < amount {}. Approving helper {:#x}...",
                log_timestamp(),
                allowance, amount, spender
            );
            let approve_call = erc20.approve(spender, U256::MAX);
            let approve_pending = approve_call
                .send()
                .await
                .context("approve submission failed")?;
            let approve_hash = approve_pending.tx_hash();
            println!(
                "{} [helper3-sell] approve tx submitted: {approve_hash:?}, waiting...",
                log_timestamp(),
            );
            let approve_receipt = approve_pending
                .await
                .context("approve tx dropped before confirmation")?
                .context("approve tx receipt missing")?;
            if approve_receipt.status != Some(U64::from(1)) {
                return Err(anyhow!("approve transaction reverted on-chain"));
            }
        }

        async fn try_send(
            _helper3: &Contract<Client>,
            call_label: &str,
            call: ethers::contract::builders::ContractCall<Client, ()>,
        ) -> Result<H256> {
            let tx_debug = call.tx.clone();
            let pending_tx = match call.send().await {
                Ok(pending) => pending,
                Err(err) => {
                    log_tx_debug("helper3 sell", call_label, &tx_debug);
                    return Err(anyhow!(
                        "helper3 {call_label} transaction failed to submit: {err:?}"
                    ));
                }
            };
            let tx_hash = pending_tx.tx_hash();
            let receipt = pending_tx
                .await
                .with_context(|| format!("helper3 {call_label} transaction dropped"))?;
            let receipt =
                receipt.with_context(|| format!("helper3 {call_label} transaction missing receipt"))?;
            match receipt.status {
                Some(status) if status == U64::from(1) => Ok(tx_hash),
                _ => {
                    log_tx_debug("helper3 sell", call_label, &tx_debug);
                    Err(anyhow!(
                        "helper3 {call_label} transaction reverted (tx={:#x}, status={:?}, block={:?}, gas_used={})",
                        tx_hash,
                        receipt.status,
                        receipt.block_number,
                        receipt.gas_used.unwrap_or_default()
                    ))
                }
            }
        }

        let attempts = [
            (
                "sellForEth(uint256,address,uint256,uint256,uint256,address)",
                helper3.method_hash::<_, ()>(
                    keccak256(
                        "sellForEth(uint256,address,uint256,uint256,uint256,address)".as_bytes(),
                    )[0..4]
                        .try_into()
                        .unwrap(),
                    (origin, token, amount, min_funds, fee_rate, fee_recipient),
                ),
            ),
            (
                "sellForEth(uint256,address,address,uint256,uint256,uint256,address)",
                helper3.method_hash::<_, ()>(
                    keccak256(
                        "sellForEth(uint256,address,address,uint256,uint256,uint256,address)"
                            .as_bytes(),
                    )[0..4]
                        .try_into()
                        .unwrap(),
                    (
                        origin,
                        token,
                        wallet,
                        amount,
                        min_funds,
                        fee_rate,
                        fee_recipient,
                    ),
                ),
            ),
            (
                "sellForEth(uint256,address,uint256,uint256)",
                helper3.method_hash::<_, ()>(
                    keccak256("sellForEth(uint256,address,uint256,uint256)".as_bytes())[0..4]
                        .try_into()
                        .unwrap(),
                    (origin, token, amount, min_funds),
                ),
            ),
            (
                "sellForEth(uint256,address,address,address,uint256,uint256)",
                helper3.method_hash::<_, ()>(
                    keccak256("sellForEth(uint256,address,address,address,uint256,uint256)".as_bytes())
                        [0..4]
                        .try_into()
                        .unwrap(),
                    (origin, token, wallet, wallet, amount, min_funds),
                ),
            ),
        ];

        for (label, call) in attempts {
            let call = match call {
                Ok(call) => call,
                Err(err) => {
                    eprintln!("{} [helper3-sell] failed to build {label}: {err:?}", log_timestamp());
                    continue;
                }
            };
            match try_send(helper3, label, call).await {
                Ok(tx_hash) => return Ok(tx_hash),
                Err(err) => {
                    eprintln!("{} [helper3-sell] {label} failed: {err:?}", log_timestamp());
                }
            }
        }

        Err(anyhow!("helper3 sellForEth failed with all overloads"))
    }

    async fn should_trade(&self, token: &TokenEvent) -> Result<bool> {
        let gmgn_info;
        if self.trade_method == TradeMethod::Wallet {
            let tx_hash = match token.tx_hash {
                Some(hash) => hash,
                None => {
                    eprintln!(
                        "{} [trader] missing tx hash for {} ({:#x}), skip TokenCreated trade",
                        log_timestamp(),
                        token.ticker, token.contract
                    );
                    return Ok(false);
                }
            };
            let tx_value_usd = match self.fetch_tx_value_usd(tx_hash).await {
                Some(value) => value,
                None => return Ok(false),
            };
            if tx_value_usd < self.min_candidate_liquidity {
                return Ok(false);
            }
            gmgn_info = self.fetch_gmgn_info(token.contract).await;
        } else {
            gmgn_info = self.fetch_gmgn_info(token.contract).await;
            let liquidity = gmgn_info.as_ref().and_then(|info| info.liquidity);
            match liquidity {
                Some(value) if value >= self.min_candidate_liquidity => {}
                Some(_) | None => {
                    return Ok(false);
                }
            }
        }
        if self.skip_migrated && self.has_migrated_liquidity(token.contract).await? {
            println!(
                "[Skip] {} ({:#x}) already has Pancake liquidity (skip_migrated=true)",
                token.ticker, token.contract
            );
            return Ok(false);
        }
        self.print_token_summary(token, gmgn_info.as_ref());
        Ok(true)
    }

    async fn fetch_tx_value_usd(&self, tx_hash: H256) -> Option<f64> {
        let tx = match self.reader_provider.get_transaction(tx_hash).await {
            Ok(Some(value)) => value,
            Ok(None) => return None,
            Err(err) => {
                eprintln!("{} [trader] failed to fetch tx {tx_hash:?}: {err:?}", log_timestamp());
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
                eprintln!(
                    "{} [trader] failed to parse tx value for {tx_hash:?}: {err:?}",
                    log_timestamp(),
                );
                return None;
            }
        };
        let usd = match token_manager::fetch_bnb_usd_price(&self.reader_provider).await {
            Ok(price) => price * bnb,
            Err(err) => {
                eprintln!(
                    "{} [trader] failed to convert tx value to USD for {tx_hash:?}: {err:?}",
                    log_timestamp(),
                );
                return None;
            }
        };
        Some(usd)
    }

    fn log_reverted_order(
        &self,
        event: &LiquidityTradeEvent,
        token_event: &TokenEvent,
        alert_type: String,
        error: String,
    ) {
        let Some(storage) = self.mongo.clone() else {
            return;
        };
        let price_source = match self.trade_method {
            TradeMethod::Telegram => "telegram",
            TradeMethod::Wallet => "wallet",
        };
        let trade_amount_bnb = token_event.buy_amount;
        let token = event.token.clone();
        let token_event = token_event.clone();
        let wallet_name = self.wallet_name.clone();
        tokio::spawn(async move {
            if let Err(db_err) = storage
                .create_reverted_order(
                    &token,
                    &token_event,
                    &alert_type,
                    price_source,
                    trade_amount_bnb,
                    &error,
                    wallet_name.as_deref(),
                )
                .await
            {
                eprintln!("[mongo] failed to log reverted order: {db_err:?}");
            }
        });
    }

    async fn has_migrated_liquidity(&self, token: Address) -> Result<bool> {
        if let Some(helper) = token_manager::fetch_helper_info(
            self.reader_provider.clone(),
            self.helper_address,
            token,
        )
        .await?
        {
            if helper.liquidity_added {
                return Ok(true);
            }

            let mut pair = helper.pancake_pair;
            if pair.is_none() {
                pair = token_manager::derive_pancake_pair(
                    self.reader_provider.clone(),
                    helper.pancake_factory,
                    token,
                    helper.quote,
                )
                .await
                .ok()
                .flatten();
            }

            if let Some(addr) = pair {
                if self.pair_exists(addr).await {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    async fn pair_exists(&self, pair: Address) -> bool {
        if pair.is_zero() {
            return false;
        }
        match self.reader_provider.get_code(pair, None).await {
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

    fn print_token_summary(&self, token: &TokenEvent, gmgn: Option<&GmgnTokenInfo>) {
        let liq = gmgn
            .and_then(|info| info.liquidity)
            .map(|v| format!("{:.4}", v))
            .unwrap_or_else(|| "unknown".into());
        let holders = gmgn
            .and_then(|info| info.holder_count)
            .map(|v| v.to_string())
            .unwrap_or_else(|| "unknown".into());

        println!(
            "[Summary] {symbol} | liq={liq} | holders={holders} | honeypot={hp} | verified={verified} | renounced={renounced} | locked={locked}",
            symbol = token.ticker,
            hp = Self::fmt_bool(gmgn.and_then(|info| info.is_honeypot)),
            verified = Self::fmt_bool(gmgn.and_then(|info| info.is_open_source)),
            renounced = Self::fmt_bool(gmgn.and_then(|info| info.is_renounced)),
            locked = Self::fmt_bool(gmgn.and_then(|info| info.is_locked)),
        );
    }

    fn fmt_bool(value: Option<bool>) -> &'static str {
        match value {
            Some(true) => "yes",
            Some(false) => "no",
            None => "unknown",
        }
    }

    fn redis_key(token: Address, order_id: Option<&ObjectId>) -> String {
        match order_id {
            Some(id) => format!("token_latest_price:{:#x}:{}", token, id.to_hex()),
            None => format!("token_latest_price:{:#x}", token),
        }
    }

    async fn load_cached_price(
        redis: &RedisClient,
        token: Address,
        order_id: Option<&ObjectId>,
    ) -> Result<Option<f64>> {
        let key = Self::redis_key(token, order_id);
        let mut conn = redis
            .get_multiplexed_async_connection()
            .await
            .context("redis connection failed")?;
        let value: Option<String> = conn.get(key).await.context("redis get failed")?;
        let parsed = value.and_then(|raw| raw.parse::<f64>().ok());
        Ok(parsed)
    }

    async fn store_cached_price(
        redis: &RedisClient,
        token: Address,
        price: f64,
        order_id: Option<&ObjectId>,
    ) -> Result<()> {
        let key = Self::redis_key(token, order_id);
        let mut conn = redis
            .get_multiplexed_async_connection()
            .await
            .context("redis connection failed")?;
        let payload = format!("{price:.12}");
        let _: () = conn
            .set(key, payload)
            .await
            .context("redis set failed")?;
        Ok(())
    }

    async fn delete_cached_price(
        redis: &RedisClient,
        token: Address,
        order_id: Option<&ObjectId>,
    ) -> Result<()> {
        let key = Self::redis_key(token, order_id);
        let mut conn = redis
            .get_multiplexed_async_connection()
            .await
            .context("redis connection failed")?;
        let _: u64 = conn.del(key).await.context("redis del failed")?;
        Ok(())
    }
}

fn fmt_price(value: f64) -> String {
    if value == 0.0 {
        return "0".into();
    }
    if value.abs() >= 1e-10 {
        format!("{:.12}", value)
    } else {
        format!("{:.3e}", value)
    }
}

fn escape_html(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('\"', "&quot;")
        .replace('\'', "&#39;")
}

fn log_timestamp() -> String {
    Local::now().format("%d/%m/%y %H:%M:%S").to_string()
}

async fn resolve_token_label_from_chain<C>(
    provider: Arc<Provider<C>>,
    token: Address,
) -> String
where
    C: JsonRpcClient + 'static,
{
    let (name, symbol) = token_manager::fetch_token_name_symbol(provider, token)
        .await
        .unwrap_or((None, None));
    match (name, symbol) {
        (_, Some(symbol)) if !symbol.is_empty() => symbol,
        (Some(name), _) if !name.is_empty() => name,
        _ => "unknown".to_string(),
    }
}

fn log_tx_debug(label: &str, function: &str, tx: &TypedTransaction) {
    let from = match tx.from() {
        Some(value) => format!("{value:#x}"),
        None => "n/a".into(),
    };
    let to = match tx.to() {
        Some(NameOrAddress::Address(value)) => format!("{value:#x}"),
        Some(NameOrAddress::Name(value)) => value.clone(),
        None => "n/a".into(),
    };
    let gas = match tx.gas() {
        Some(value) => value.to_string(),
        None => "n/a".into(),
    };
    let gas_price = match tx.gas_price() {
        Some(value) => value.to_string(),
        None => "n/a".into(),
    };
    let (max_fee, max_priority) = match tx {
        TypedTransaction::Eip1559(inner) => (
            inner
                .max_fee_per_gas
                .map(|value| value.to_string())
                .unwrap_or_else(|| "n/a".into()),
            inner
                .max_priority_fee_per_gas
                .map(|value| value.to_string())
                .unwrap_or_else(|| "n/a".into()),
        ),
        _ => ("n/a".into(), "n/a".into()),
    };
    let value = match tx.value() {
        Some(value) => value.to_string(),
        None => "n/a".into(),
    };
    let nonce = match tx.nonce() {
        Some(value) => value.to_string(),
        None => "n/a".into(),
    };
    let chain_id = match tx.chain_id() {
        Some(value) => value.to_string(),
        None => "n/a".into(),
    };
    let data = match tx.data() {
        Some(value) => format!("0x{}", hex::encode(value.as_ref())),
        None => "n/a".into(),
    };
    eprintln!(
        "{} [tx-debug] label={} function={} from={} to={} value={} gas={} gas_price={} max_fee={} max_priority_fee={} nonce={} chain_id={} data={}",
        log_timestamp(),
        label,
        function,
        from,
        to,
        value,
        gas,
        gas_price,
        max_fee,
        max_priority,
        nonce,
        chain_id,
        data
    );
}

fn decode_revert_reason(err: &ContractError<Client>) -> Option<String> {
    let ContractError::Revert(data) = err else {
        return None;
    };
    decode_revert_string(data)
}

fn decode_revert_string(data: &Bytes) -> Option<String> {
    if data.0.len() < 4 || data.0[0..4] != [0x08, 0xc3, 0x79, 0xa0] {
        return None;
    }
    let tokens = decode(&[ParamType::String], &data.0[4..]).ok()?;
    match tokens.into_iter().next() {
        Some(Token::String(reason)) => Some(reason),
        _ => None,
    }
}

async fn increment_live_follow_counts(
    redis: Option<&RedisClient>,
    wallet: Address,
    token: Address,
) {
    let redis = match redis {
        Some(client) => client,
        None => return,
    };
    let mut conn = match redis.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return,
    };
    let wallet_key = format!("{:#x}", wallet);
    let token_key = format!("{:#x}", token);
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
