use anyhow::{Context, Result};
use ethers::providers::{Provider, Ws};
use ethers::types::Address;
use sniper_fourmeme::config::{Config, TradeMethod};
use sniper_fourmeme::rpc_fallback::rpc_urls_from_env;
use sniper_fourmeme::trader::Trader;
use std::{env, str::FromStr, sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let mut args = env::args().skip(1);
    let token_arg = match args.next() {
        Some(value) => value,
        None => {
            eprintln!("Usage: live_trade_test <token_address> <buy_bnb> [sell_delay_secs]");
            return Ok(());
        }
    };
    let buy_arg = match args.next() {
        Some(value) => value,
        None => {
            eprintln!("Usage: live_trade_test <token_address> <buy_bnb> [sell_delay_secs]");
            return Ok(());
        }
    };
    let delay_secs = args
        .next()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(10);

    let token = Address::from_str(token_arg.trim()).context("invalid token address")?;
    let buy_amount = buy_arg.parse::<f64>().context("invalid buy amount")?;

    let mut cfg = Config::from_env();
    cfg.trade_method = TradeMethod::Wallet;
    if let Ok(rpc_url) = env::var("VOLUME_RPC_URL") {
        if !rpc_url.trim().is_empty() {
            cfg.rpc_url = rpc_url;
        }
    }

    let wss_url = env::var("VOLUME_WSS_URL").unwrap_or_else(|_| cfg.wss_url.clone());
    let ws = Ws::connect(wss_url).await.context("failed to connect ws")?;
    let ws_provider = Arc::new(Provider::new(ws).interval(Duration::from_millis(100)));
    let (_tx, rx) = mpsc::channel(1);
    let rpc_urls = rpc_urls_from_env(cfg.rpc_url.clone(), &["VOLUME_RPC_URL2", "VOLUME_RPC_URL3"]);
    let trader = Trader::new(
        rx,
        cfg,
        rpc_urls,
        None,
        None,
        ws_provider,
        None,
        "live-test",
        false,
        false,
    )
    .await?;

    println!(
        "[live-test] buying token={:#x} amount_bnb={:.6}",
        token, buy_amount
    );
    let buy_hash = trader
        .live_buy_for_test(token, buy_amount)
        .await
        .context("buy failed")?;
    println!("[live-test] buy submitted tx={:#x}", buy_hash);

    if delay_secs > 0 {
        println!("[live-test] waiting {}s before sell...", delay_secs);
        sleep(Duration::from_secs(delay_secs)).await;
    }

    println!("[live-test] selling token={:#x}", token);
    let sell_hash = trader
        .live_sell_all_for_test(token)
        .await
        .context("sell failed")?;
    println!("[live-test] sell submitted tx={:#x}", sell_hash);

    Ok(())
}
