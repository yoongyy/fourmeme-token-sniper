use ethers::providers::Middleware;
use ethers::{types::Address, utils::hex};
use sniper_fourmeme::{
    config::Config,
    rpc_fallback::{build_http_provider, rpc_urls_from_env},
    token_manager,
};
use std::{str::FromStr, sync::Arc, time::Instant};

fn read_arg_value(args: &[String], key: &str) -> Option<String> {
    args.iter()
        .position(|value| value == key)
        .and_then(|idx| args.get(idx + 1).cloned())
}

fn read_arg_usize(args: &[String], key: &str) -> Option<usize> {
    read_arg_value(args, key).and_then(|value| value.parse::<usize>().ok())
}

fn read_arg_u64(args: &[String], key: &str) -> Option<u64> {
    read_arg_value(args, key).and_then(|value| value.parse::<u64>().ok())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let cfg = Config::from_env();
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 2 {
        eprintln!(
            "Usage: cargo run --bin balance_check -- <token> <wallet> [--attempts N] [--sleep-ms N] [--rpc URL]"
        );
        std::process::exit(1);
    }
    let token_arg = &args[0];
    let wallet_arg = &args[1];
    let token = Address::from_str(token_arg)
        .map_err(|_| anyhow::anyhow!("Invalid token address: {}", token_arg))?;
    let wallet = Address::from_str(wallet_arg)
        .map_err(|_| anyhow::anyhow!("Invalid wallet address: {}", wallet_arg))?;
    let attempts = read_arg_usize(&args, "--attempts").unwrap_or(5);
    let sleep_ms = read_arg_u64(&args, "--sleep-ms").unwrap_or(300);
    let rpc_url = read_arg_value(&args, "--rpc")
        .or_else(|| std::env::var("VOLUME_RPC_URL").ok())
        .unwrap_or_else(|| cfg.rpc_url.clone());
    let mut rpc_urls = rpc_urls_from_env(rpc_url.clone(), &["RPC_URL2", "RPC_URL3"]);
    if let Ok(value) = std::env::var("VOLUME_RPC_URL2") {
        let value = value.trim();
        if !value.is_empty() && !rpc_urls.iter().any(|url| url == value) {
            rpc_urls.push(value.to_string());
        }
    }
    if let Ok(value) = std::env::var("VOLUME_RPC_URL3") {
        let value = value.trim();
        if !value.is_empty() && !rpc_urls.iter().any(|url| url == value) {
            rpc_urls.push(value.to_string());
        }
    }
    let provider = Arc::new(build_http_provider(
        rpc_urls,
        std::time::Duration::from_millis(0),
    )?);

    println!(
        "[BalanceCheck] rpc={} token={:#x} wallet={:#x} attempts={} sleep_ms={}",
        rpc_url, token, wallet, attempts, sleep_ms
    );
    let code = provider.get_code(token, None).await?;
    println!(
        "[BalanceCheck] code_len={} code_prefix=0x{}",
        code.0.len(),
        hex::encode(code.0.iter().take(8).cloned().collect::<Vec<u8>>())
    );
    let decimals = match token_manager::fetch_token_decimals(provider.clone(), token).await {
        Ok(value) => {
            println!("[BalanceCheck] token_decimals={}", value);
            Some(value)
        }
        Err(err) => {
            println!("[BalanceCheck] token_decimals_error={err:?}");
            None
        }
    };

    for idx in 1..=attempts {
        let started = Instant::now();
        let balance = token_manager::fetch_token_balance(provider.clone(), token, wallet).await;
        let elapsed_ms = started.elapsed().as_millis();
        match balance {
            Ok(value) => {
                let display = match decimals {
                    Some(decimals) => match token_manager::u256_to_f64(value, decimals) {
                        Ok(amount) => format!("{:.12}", amount),
                        Err(_) => value.to_string(),
                    },
                    None => value.to_string(),
                };
                println!(
                    "[BalanceCheck] attempt={}/{} balance={} elapsed_ms={}",
                    idx, attempts, display, elapsed_ms
                );
            }
            Err(err) => {
                println!(
                    "[BalanceCheck] attempt={}/{} balance_error={err:?} elapsed_ms={}",
                    idx, attempts, elapsed_ms
                );
            }
        }
        if idx < attempts {
            tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
        }
    }

    Ok(())
}
