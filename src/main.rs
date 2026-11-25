use ethers::{
    providers::{Provider, Ws},
    types::Address,
};
use sniper_fourmeme::{
    config::{Config, TradeMethod},
    gmgn::{GmgnClient, GmgnTokenInfo},
    listener::Listener,
    price::{build_price_context, read_pair_price},
    redis_utils::build_redis_client,
    rpc_fallback::rpc_urls_from_env,
    storage::MongoLogger,
    telegram_trade::TelegramTradeClient,
    trader::Trader,
    types::TokenCreated,
};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::{
    str::FromStr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{self, AsyncBufReadExt, BufReader},
    sync::mpsc,
    task,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    std::panic::set_hook(Box::new(|info| {
        eprintln!("[panic] {info}");
    }));

    // Create channel to communicate user commands
    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<String>();

    let cfg = Config::from_env();
    if cfg.trade_method() == TradeMethod::Wallet && !cfg.tweet_trade().is_enabled() {
        eprintln!(
            "[startup] TRADE_METHOD=wallet is reserved for volume_bot; main sniper-fourmeme will not run trades. Exiting."
        );
        return Ok(());
    }
    let enable_liquidity_monitor =
        cfg.trade_method() != TradeMethod::Wallet || cfg.tweet_trade().is_enabled();
    let enable_liquidity_trader = cfg.trade_method() != TradeMethod::Wallet;
    let enable_liquidity_alerts = enable_liquidity_trader;
    let (tx, rx) = mpsc::channel(100);
    let active_trades = Arc::new(AtomicUsize::new(0));
    let (tweet_tx, tweet_rx) = if cfg.tweet_trade().is_enabled() {
        let (tx, rx) = mpsc::channel(100);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };
    let mongo_logger = MongoLogger::connect(cfg.mongo()).await;
    preload_active_trades(&cfg, mongo_logger.as_ref(), &active_trades).await;

    let gmgn_client = if cfg.gmgn.enabled {
        match GmgnClient::new(cfg.gmgn.clone()) {
            Ok(client) => Some(client),
            Err(err) => {
                eprintln!("[GMGN] Failed to start client: {err:?}");
                None
            }
        }
    } else {
        None
    };

    let ws = Ws::connect(cfg.wss_url.clone()).await?;
    println!("✅ WS connected!");
    let provider = Arc::new(Provider::new(ws));
    let rpc_urls = rpc_urls_from_env(cfg.rpc_url.clone(), &["RPC_URL2", "RPC_URL3"]);
    // Create listener
    let listener = Listener::new(
        provider.clone(),
        tx,
        tweet_tx.clone(),
        enable_liquidity_monitor, // keep token manager monitors for tweet alerts
        enable_liquidity_alerts,
        false,
        false,
        cfg.clone(),
        gmgn_client.clone(),
        mongo_logger.clone(),
        Some(active_trades.clone()),
    );

    // Await trader creation
    let trader: Option<Trader> = if enable_liquidity_trader {
        Some(
            Trader::new(
                rx,
                cfg.clone(),
                rpc_urls.clone(),
                gmgn_client.clone(),
                mongo_logger.clone(),
                provider.clone(),
                Some(active_trades.clone()),
                "liquidity",
                false,
                false,
            )
            .await?,
        )
    } else {
        None
    };

    let tweet_trader = if let Some(tweet_rx) = tweet_rx {
        Some(
            Trader::new(
                tweet_rx,
                cfg.tweet_variant(),
                rpc_urls.clone(),
                gmgn_client.clone(),
                mongo_logger.clone(),
                provider.clone(),
                None,
                "tweet",
                false,
                false,
            )
            .await?,
        )
    } else {
        None
    };

    // Spawn listener task
    let listener_task = task::spawn(async move {
        if let Err(e) = listener.run().await {
            eprintln!("[Listener task error] {:?}", e);
        }
    });

    // Spawn trader task
    let trader_task = if let Some(mut trader) = trader {
        Some(task::spawn(async move {
            if let Err(e) = trader.run().await {
                eprintln!("Trader task error: {:?}", e);
            }
        }))
    } else {
        None
    };

    let tweet_trader_task = if let Some(mut tweet_trader) = tweet_trader {
        Some(task::spawn(async move {
            if let Err(e) = tweet_trader.run().await {
                eprintln!("Tweet trader task error: {:?}", e);
            }
        }))
    } else {
        None
    };

    // Task 2: Command listener (reads from stdin)
    let cmd_tx_clone = cmd_tx.clone();
    let stdin_task: task::JoinHandle<()> = task::spawn(async move {
        let mut reader = BufReader::new(io::stdin());
        let mut line = String::new();
        loop {
            line.clear();
            if reader.read_line(&mut line).await.unwrap() == 0 {
                break; // EOF
            }
            let input = line.trim().to_string();
            if !input.is_empty() {
                if cmd_tx_clone.send(input).is_err() {
                    println!("Command channel closed");
                    break;
                }
            }
        }
    });

    // Task 3: Handle user commands
    let provider_for_cmds = provider.clone();
    let telegram_for_cmds = cfg.telegram().clone();
    let telegram_trade_settings = cfg.telegram_trade_tweet().clone();
    let command_handler = task::spawn(async move {
        while let Some(cmd) = cmd_rx.recv().await {
            let mut parts = cmd.split_whitespace();
            let keyword = match parts.next() {
                Some(part) => part,
                None => continue,
            };
            match keyword {
                "status" => println!("✅ Program is running fine."),
                "pause" => println!("⏸️ Pausing operations..."),
                "resume" => println!("▶️ Resuming operations..."),
                "exit" => {
                    println!("👋 Exiting program...");
                    std::process::exit(0);
                }
                "price" => {
                    let token_arg = match parts.next() {
                        Some(value) => value,
                        None => {
                            println!("Usage: price <token_address> <pair_address>");
                            continue;
                        }
                    };
                    let pair_arg = match parts.next() {
                        Some(value) => value,
                        None => {
                            println!("Usage: price <token_address> <pair_address>");
                            continue;
                        }
                    };
                    let token_address = match Address::from_str(token_arg) {
                        Ok(addr) => addr,
                        Err(_) => {
                            eprintln!("Invalid token address: {}", token_arg);
                            continue;
                        }
                    };
                    let pair_address = match Address::from_str(pair_arg) {
                        Ok(addr) => addr,
                        Err(_) => {
                            eprintln!("Invalid pair address: {}", pair_arg);
                            continue;
                        }
                    };
                    let provider = provider_for_cmds.clone();
                    match build_price_context(provider.clone(), pair_address, token_address).await {
                        Ok(ctx) => match read_pair_price(&provider, &ctx).await {
                            Ok(price) => println!(
                                "[Price] token={:#x} pair={:#x} price={:.10}",
                                token_address, pair_address, price
                            ),
                            Err(err) => eprintln!(
                                "[Price] failed to read price for token {:#x}: {err:?}",
                                token_address
                            ),
                        },
                        Err(err) => eprintln!(
                            "[Price] failed to build context for {:#x}: {err:?}",
                            token_address
                        ),
                    }
                }
                "tweet-test" => {
                    let token_arg = match parts.next() {
                        Some(value) => value,
                        None => {
                            println!(
                                "Usage: tweet-test <token_address> <symbol> <liquidity> <price> <tweet_text>"
                            );
                            continue;
                        }
                    };
                    let symbol = match parts.next() {
                        Some(value) => value,
                        None => {
                            println!(
                                "Usage: tweet-test <token_address> <symbol> <liquidity> <price> <tweet_text>"
                            );
                            continue;
                        }
                    };
                    let liquidity = match parts.next().and_then(|value| value.parse::<f64>().ok()) {
                        Some(value) => value,
                        None => {
                            println!(
                                "Usage: tweet-test <token_address> <symbol> <liquidity> <price> <tweet_text>"
                            );
                            continue;
                        }
                    };
                    let price = match parts.next().and_then(|value| value.parse::<f64>().ok()) {
                        Some(value) => value,
                        None => {
                            println!(
                                "Usage: tweet-test <token_address> <symbol> <liquidity> <price> <tweet_text>"
                            );
                            continue;
                        }
                    };
                    let tweet_text = parts.collect::<Vec<_>>().join(" ");
                    if tweet_text.is_empty() {
                        println!(
                            "Usage: tweet-test <token_address> <symbol> <liquidity> <price> <tweet_text>"
                        );
                        continue;
                    }
                    let token_address = match Address::from_str(token_arg) {
                        Ok(addr) => addr,
                        Err(_) => {
                            eprintln!("Invalid token address: {}", token_arg);
                            continue;
                        }
                    };
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    let token = TokenCreated {
                        creator: Address::zero(),
                        token: token_address,
                        name: symbol.to_string(),
                        symbol: symbol.to_string(),
                    };
                    let info = GmgnTokenInfo {
                        liquidity: Some(liquidity),
                        price: Some(price),
                        creation_timestamp: Some(now),
                        ..Default::default()
                    };
                    match Listener::send_tweet_alert_debug(
                        &telegram_for_cmds,
                        token,
                        info,
                        &tweet_text,
                        None,
                    )
                    .await
                    {
                        Ok(true) => println!("✅ Tweet alert sent"),
                        Ok(false) => println!("⚠️ Tweet alert skipped (missing telegram config)"),
                        Err(err) => eprintln!("[tweet-test] failed to send alert: {err:?}"),
                    }
                }
                "tweet-test-buy" => {
                    let token_arg = match parts.next() {
                        Some(value) => value,
                        None => {
                            println!(
                                "Usage: tweet-test-buy <token_address> <symbol> <liquidity> <price> <buy_amount> <tweet_text>"
                            );
                            continue;
                        }
                    };
                    let symbol = match parts.next() {
                        Some(value) => value,
                        None => {
                            println!(
                                "Usage: tweet-test-buy <token_address> <symbol> <liquidity> <price> <buy_amount> <tweet_text>"
                            );
                            continue;
                        }
                    };
                    let liquidity = match parts.next().and_then(|value| value.parse::<f64>().ok()) {
                        Some(value) => value,
                        None => {
                            println!(
                                "Usage: tweet-test-buy <token_address> <symbol> <liquidity> <price> <buy_amount> <tweet_text>"
                            );
                            continue;
                        }
                    };
                    let price = match parts.next().and_then(|value| value.parse::<f64>().ok()) {
                        Some(value) => value,
                        None => {
                            println!(
                                "Usage: tweet-test-buy <token_address> <symbol> <liquidity> <price> <buy_amount> <tweet_text>"
                            );
                            continue;
                        }
                    };
                    let buy_amount = match parts.next().and_then(|value| value.parse::<f64>().ok())
                    {
                        Some(value) => value,
                        None => {
                            println!(
                                "Usage: tweet-test-buy <token_address> <symbol> <liquidity> <price> <buy_amount> <tweet_text>"
                            );
                            continue;
                        }
                    };
                    let tweet_text = parts.collect::<Vec<_>>().join(" ");
                    if tweet_text.is_empty() {
                        println!(
                            "Usage: tweet-test-buy <token_address> <symbol> <liquidity> <price> <buy_amount> <tweet_text>"
                        );
                        continue;
                    }
                    let token_address = match Address::from_str(token_arg) {
                        Ok(addr) => addr,
                        Err(_) => {
                            eprintln!("Invalid token address: {}", token_arg);
                            continue;
                        }
                    };
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    let token = TokenCreated {
                        creator: Address::zero(),
                        token: token_address,
                        name: symbol.to_string(),
                        symbol: symbol.to_string(),
                    };
                    let info = GmgnTokenInfo {
                        liquidity: Some(liquidity),
                        price: Some(price),
                        creation_timestamp: Some(now),
                        ..Default::default()
                    };
                    let alert_sent = match Listener::send_tweet_alert_debug(
                        &telegram_for_cmds,
                        token,
                        info,
                        &tweet_text,
                        None,
                    )
                    .await
                    {
                        Ok(true) => {
                            println!("✅ Tweet alert sent");
                            true
                        }
                        Ok(false) => {
                            println!("⚠️ Tweet alert skipped (missing telegram config)");
                            false
                        }
                        Err(err) => {
                            eprintln!("[tweet-test-buy] failed to send alert: {err:?}");
                            false
                        }
                    };
                    if alert_sent {
                        match TelegramTradeClient::connect(&telegram_trade_settings).await {
                            Ok(client) => {
                                if let Err(err) = client.send_buy(token_arg, buy_amount).await {
                                    eprintln!("[tweet-test-buy] failed to send /buy: {err:?}");
                                } else {
                                    println!("✅ /buy sent to GMGN bot");
                                }
                            }
                            Err(err) => {
                                eprintln!(
                                    "[tweet-test-buy] failed to connect telegram client: {err:?}"
                                );
                            }
                        }
                    }
                }
                _ => println!("❓ Unknown command: {}", cmd),
            }
        }
    });
    let listener_future = async { listener_task.await.map_err(anyhow::Error::from) };
    let trader_future = async {
        if let Some(t) = trader_task {
            t.await.map_err(anyhow::Error::from)?;
        }
        Ok(())
    };
    let tweet_future = async {
        if let Some(t) = tweet_trader_task {
            t.await.map_err(anyhow::Error::from)?;
        }
        Ok(())
    };
    let stdin_future = async { stdin_task.await.map_err(anyhow::Error::from) };
    let command_future = async { command_handler.await.map_err(anyhow::Error::from) };
    tokio::try_join!(
        listener_future,
        trader_future,
        tweet_future,
        stdin_future,
        command_future
    )?;
    Ok(())
}

async fn preload_active_trades(
    cfg: &Config,
    mongo: Option<&Arc<MongoLogger>>,
    active_trades: &Arc<AtomicUsize>,
) {
    if let Some(storage) = mongo {
        match storage.count_open_real_orders(None).await {
            Ok(count) => {
                active_trades.store(count as usize, Ordering::SeqCst);
                println!("[orders] loaded {count} open liquidity orders from Mongo");
            }
            Err(err) => {
                eprintln!("[orders] failed to count open orders from Mongo: {err:?}");
            }
        }
        return;
    }

    if let Ok(redis) = build_redis_client(cfg.redis()) {
        match count_active_orders_in_redis(&redis).await {
            Ok(count) => {
                active_trades.store(count as usize, Ordering::SeqCst);
                println!("[orders] loaded {count} active order keys from Redis");
            }
            Err(err) => {
                eprintln!("[orders] failed to count active orders in Redis: {err:?}");
            }
        }
    }
}

async fn count_active_orders_in_redis(redis: &redis::Client) -> anyhow::Result<usize> {
    let mut conn = redis.get_multiplexed_async_connection().await?;
    let mut cursor: u64 = 0;
    let mut total = 0usize;
    loop {
        let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg("orders:active:*")
            .arg("COUNT")
            .arg(200)
            .query_async(&mut conn)
            .await?;
        total = total.saturating_add(keys.len());
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    Ok(total)
}
