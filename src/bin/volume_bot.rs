use chrono::Local;

fn log_timestamp() -> String {
    Local::now().format("%d/%m/%y %H:%M:%S").to_string()
}

use ethers::providers::{Middleware, Provider, Ws};
use ethers::signers::{LocalWallet, Signer};
use ethers::types::{Address, BlockNumber};
use sniper_fourmeme::{
    config::Config, gmgn::GmgnClient, listener::Listener, redis_utils::build_redis_client,
    rpc_fallback::rpc_urls_from_env, storage::MongoLogger, trader::Trader, types::BotEvent,
};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{self, AsyncBufReadExt, BufReader},
    sync::mpsc,
    task,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    std::panic::set_hook(Box::new(|info| {
        eprintln!("{} [panic] {info}", log_timestamp());
    }));

    // Command channel
    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<String>();

    let live_mode = std::env::args().any(|arg| arg == "--live");
    // Use volume-focused settings: remove liquidity gate by forcing min_liquidity to 0.
    let mut cfg = Config::from_env();
    let rpc_env_key = if live_mode {
        "VOLUME_LIVE_RPC_URL"
    } else {
        "VOLUME_RPC_URL"
    };
    if let Ok(rpc_url) = std::env::var(rpc_env_key) {
        if !rpc_url.trim().is_empty() {
            cfg.rpc_url = rpc_url;
        }
    }
    cfg.min_candidate_liquidity = 0.0;

    let (tx, rx) = mpsc::channel::<BotEvent>(100);

    let mongo_logger = MongoLogger::connect(cfg.mongo()).await;
    let redis = build_redis_client(cfg.redis()).ok();
    if let Some(client) = redis.as_ref() {
        cleanup_cached_price_keys(Some(client), mongo_logger.clone()).await;
    }
    let gmgn_client = if cfg.gmgn.enabled {
        match GmgnClient::new(cfg.gmgn.clone()) {
            Ok(client) => Some(client),
            Err(err) => {
                eprintln!("{} [GMGN] Failed to start client: {err:?}", log_timestamp());
                None
            }
        }
    } else {
        None
    };

    let wss_env_key = if live_mode {
        "VOLUME_LIVE_WSS_URL"
    } else {
        "VOLUME_WSS_URL"
    };
    let wss_url = std::env::var(wss_env_key).unwrap_or_else(|_| cfg.wss_url.clone());
    println!("{} [startup] volume_bot wss={}", log_timestamp(), wss_url);
    println!("{} [startup] volume_bot rpc={}", log_timestamp(), cfg.rpc_url);
    let ws = Ws::connect(wss_url).await?;
    println!("✅ WS connected!");
    let provider = Arc::new(Provider::new(ws).interval(Duration::from_millis(100)));
    if live_mode {
        match cfg
            .private_key
            .as_ref()
            .and_then(|key| key.parse::<LocalWallet>().ok())
        {
            Some(wallet) => {
                let wallet_address = wallet.address();
                match provider
                    .get_transaction_count(wallet_address, Some(BlockNumber::Latest.into()))
                    .await
                {
                    Ok(nonce) => {
                        println!(
                            "{} [startup] volume_bot wallet={:#x} nonce={}",
                            log_timestamp(),
                            wallet_address,
                            nonce
                        );
                    }
                    Err(err) => {
                        eprintln!(
                            "{} [startup] volume_bot wallet={:#x} nonce=error err={err:?}",
                            log_timestamp(),
                            wallet_address
                        );
                    }
                }
            }
            None => {
                eprintln!("{} [startup] volume_bot wallet=missing (PRIVATE_KEY unset/invalid)", log_timestamp());
            }
        }
    }

    // Listener/trader without tweet flow; min_liquidity already zeroed to avoid gating.
    let listener = Listener::new(
        provider.clone(),
        tx,
        None,
        false,
        false,
        true,
        live_mode,
        cfg.clone(),
        gmgn_client.clone(),
        mongo_logger.clone(),
        None,
    );

    let rpc_fallback_keys = if live_mode {
        ["VOLUME_LIVE_RPC_URL2", "VOLUME_LIVE_RPC_URL3"].as_slice()
    } else {
        ["VOLUME_RPC_URL2", "VOLUME_RPC_URL3"].as_slice()
    };
    let rpc_urls = rpc_urls_from_env(cfg.rpc_url.clone(), rpc_fallback_keys);

    if live_mode && !cfg.redis().enabled {
        return Err(anyhow::anyhow!(
            "LIVE mode requires redis for live_trade enqueue"
        ));
    }

    let mut trader: Trader = Trader::new(
        rx,
        cfg.clone(),
        rpc_urls,
        gmgn_client.clone(),
        mongo_logger.clone(),
        provider.clone(),
        None,
        "volume",
        false,
        live_mode,
    )
    .await?;

    // Spawn listener
    let listener_task = task::spawn(async move {
        if let Err(e) = listener.run().await {
            eprintln!(
                "{} [Listener task error] {:?}",
                log_timestamp(),
                e
            );
        }
    });

    // Spawn trader
    let trader_task = task::spawn(async move {
        if let Err(e) = trader.run().await {
            eprintln!("Trader task error: {:?}", e);
        }
    });

    // Command listener
    let cmd_tx_clone = cmd_tx.clone();
    let stdin_task: task::JoinHandle<()> = task::spawn(async move {
        let mut reader = BufReader::new(io::stdin());
        let mut line = String::new();
        loop {
            line.clear();
            if reader.read_line(&mut line).await.unwrap() == 0 {
                break;
            }
            let input = line.trim().to_string();
            if !input.is_empty() && cmd_tx_clone.send(input).is_err() {
                println!("Command channel closed");
                break;
            }
        }
    });

    // Handle commands
    let command_handler = task::spawn(async move {
        while let Some(cmd) = cmd_rx.recv().await {
            match cmd.as_str() {
                "status" => println!("✅ Program is running fine."),
                "exit" => {
                    println!("👋 Exiting program...");
                    std::process::exit(0);
                }
                other => println!("❓ Unknown command: {}", other),
            }
        }
    });

    tokio::try_join!(listener_task, trader_task, stdin_task, command_handler)?;
    Ok(())
}

async fn cleanup_cached_price_keys(redis: Option<&redis::Client>, mongo: Option<Arc<MongoLogger>>) {
    let client = match redis {
        Some(client) => client,
        None => return,
    };
    let mongo = match mongo {
        Some(mongo) => mongo,
        None => return,
    };
    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(err) => {
            eprintln!("{} [volume-bot] failed to connect redis for cleanup: {err:?}", log_timestamp());
            return;
        }
    };
    let mut cursor: u64 = 0;
    loop {
        let res: Result<(u64, Vec<String>), _> = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg("token_latest_price:*")
            .arg("COUNT")
            .arg(200)
            .query_async(&mut conn)
            .await;
        let (next, keys) = match res {
            Ok(value) => value,
            Err(err) => {
                eprintln!("{} [volume-bot] redis scan failed: {err:?}", log_timestamp());
                break;
            }
        };
        cursor = next;
        for key in keys {
            let mut parts = key.split(':');
            let _prefix = parts.next();
            let token_str = parts.next().unwrap_or("");
            let token = match token_str.parse::<Address>() {
                Ok(value) => value,
                Err(_) => {
                    let _: Result<(), _> = redis::cmd("DEL").arg(&key).query_async(&mut conn).await;
                    continue;
                }
            };
            match mongo.has_any_open_order_or_sim_for_token(token).await {
                Ok(true) => {}
                Ok(false) => {
                    let _: Result<(), _> = redis::cmd("DEL").arg(&key).query_async(&mut conn).await;
                }
                Err(err) => {
                    eprintln!(
                        "{} [volume-bot] mongo lookup failed for {}: {err:?}",
                        log_timestamp(),
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
