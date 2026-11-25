use anyhow::{Context, Result, anyhow};
use ethers::{
    abi::Abi,
    contract::Contract,
    types::{Address, U256},
    utils::format_units,
    utils::parse_units,
};
use serde_json::from_slice;
use sniper_fourmeme::{
    config::Config,
    rpc_fallback::{build_http_provider, rpc_urls_from_env},
    token_manager,
};
use std::{env, str::FromStr, sync::Arc};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let mut args = env::args().skip(1);
    let token_arg = args
        .next()
        .context("Usage: cargo run --bin try_sell -- <token_address> <amount_tokens> [decimals]")?;
    let amount_arg = args
        .next()
        .context("Usage: cargo run --bin try_sell -- <token_address> <amount_tokens> [decimals]")?;
    let decimals_override = args
        .next()
        .map(|arg| arg.parse::<u8>().context("decimals must be a number"))
        .transpose()?;

    let token = Address::from_str(&token_arg).context("token address is invalid")?;
    let cfg = Config::from_env();
    let rpc_urls = rpc_urls_from_env(cfg.rpc_url.clone(), &["RPC_URL2", "RPC_URL3"]);
    let provider = Arc::new(
        build_http_provider(rpc_urls, std::time::Duration::from_millis(0))
            .context("RPC_URL is invalid")?,
    );

    let token_decimals = match decimals_override {
        Some(decimals) => decimals,
        None => token_manager::fetch_token_decimals(provider.clone(), token)
            .await
            .unwrap_or(18),
    };

    let amount_raw: U256 = parse_units(&amount_arg, u32::from(token_decimals))
        .context("failed to parse amount using token decimals")?
        .into();
    if amount_raw.is_zero() {
        return Err(anyhow!("amount must be greater than zero"));
    }

    let abi: Abi = from_slice(include_bytes!("../abi/TokenManagerHelper3.abi"))
        .context("failed to parse TokenManagerHelper ABI")?;
    let contract = Contract::new(cfg.helper3_address, abi, provider.clone());

    let (token_manager, quote, funds, fee): (Address, Address, U256, U256) = contract
        .method("trySell", (token, amount_raw))
        .context("failed to build trySell call")?
        .call()
        .await
        .context("trySell call failed")?;

    println!(
        "[TrySell] token={:#x} amount_raw={} token_decimals={}",
        token, amount_raw, token_decimals
    );
    println!(
        "[TrySell] tokenManager={:#x} quote={:#x} funds={} fee={}",
        token_manager, quote, funds, fee
    );

    if let (Ok(funds_display), Ok(fee_display)) = (format_units(funds, 18), format_units(fee, 18)) {
        println!(
            "[TrySell] funds_display={} fee_display={} (18 decimals)",
            funds_display, fee_display
        );
    }

    Ok(())
}
