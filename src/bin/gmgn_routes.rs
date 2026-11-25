use anyhow::{Context, Result, anyhow};
use ethers::types::{Address, U256};
use ethers::utils::parse_units;
use serde_json::to_string_pretty;
use std::{env, str::FromStr};

use sniper_fourmeme::config::GmgnSettings;
use sniper_fourmeme::gmgn::GmgnClient;

fn parse_address(input: &str) -> Result<Address> {
    let trimmed = input.trim().to_ascii_lowercase();
    if trimmed == "native" || trimmed == "bnb" || trimmed == "eth" {
        return Ok(Address::zero());
    }
    Address::from_str(input).context("invalid address")
}

fn parse_amount(input: &str) -> Result<U256> {
    let trimmed = input.trim();
    if trimmed.contains('.') {
        let value = parse_units(trimmed, 18).context("failed to parse decimal amount")?;
        return Ok(value.into());
    }
    if trimmed.starts_with("0x") {
        return U256::from_str_radix(trimmed.trim_start_matches("0x"), 16)
            .map_err(|_| anyhow!("invalid hex amount"));
    }
    U256::from_dec_str(trimmed).context("invalid decimal amount")
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let token_in = env::var("GMGN_TOKEN_IN").unwrap_or_else(|_| "native".into());
    let token_out = env::var("GMGN_TOKEN_OUT").unwrap_or_default();
    let amount_in = env::var("GMGN_AMOUNT_IN").unwrap_or_default();
    let chain = env::var("GMGN_CHAIN").unwrap_or_else(|_| "bsc".into());
    let src = env::var("GMGN_TRADE_SRC").unwrap_or_else(|_| "gmgn".into());

    let args: Vec<String> = env::args().collect();
    let token_in = args.get(1).cloned().unwrap_or(token_in);
    let token_out = args.get(2).cloned().unwrap_or(token_out);
    let amount_in = args.get(3).cloned().unwrap_or(amount_in);

    if token_out.trim().is_empty() || amount_in.trim().is_empty() {
        eprintln!(
            "Usage: gmgn_routes <token_in|native> <token_out> <amount_in_wei|amount_in_bnb>\n\
             Or set env GMGN_TOKEN_IN, GMGN_TOKEN_OUT, GMGN_AMOUNT_IN"
        );
        return Ok(());
    }

    let token_in = parse_address(&token_in)?;
    let token_out = parse_address(&token_out)?;
    let amount_in = parse_amount(&amount_in)?;
    let settings = GmgnSettings::from_env();
    let client = GmgnClient::new(settings)?;
    let src = if src.trim().is_empty() {
        None
    } else {
        Some(src.as_str())
    };

    let routes = client
        .fetch_available_routes_exact_in(&chain, &chain, token_in, token_out, amount_in, src)
        .await?;

    println!("routes_count={}", routes.len());
    println!("{}", to_string_pretty(&routes)?);
    Ok(())
}
