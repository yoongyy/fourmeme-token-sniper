use anyhow::{Context, Result, anyhow};
use ethers::types::{Address, U256};
use ethers::utils::{hex, parse_units};
use std::{env, str::FromStr};

use sniper_fourmeme::config::GmgnSettings;
use sniper_fourmeme::gmgn::{GmgnClient, GmgnSwapTx};

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

fn print_tx(tx: &GmgnSwapTx) {
    println!("to={:#x}", tx.to);
    println!("value={}", tx.value);
    println!(
        "gas_limit={}",
        tx.gas_limit
            .map(|v| v.to_string())
            .unwrap_or_else(|| "none".into())
    );
    println!(
        "chain_id={}",
        tx.chain_id
            .map(|v| v.to_string())
            .unwrap_or_else(|| "none".into())
    );
    println!("data=0x{}", hex::encode(tx.data.as_ref()));
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let token_in = env::var("GMGN_TOKEN_IN").unwrap_or_else(|_| "native".into());
    let token_out = env::var("GMGN_TOKEN_OUT").unwrap_or_default();
    let amount_in = env::var("GMGN_AMOUNT_IN").unwrap_or_default();
    let chain = env::var("GMGN_CHAIN").unwrap_or_else(|_| "bsc".into());
    let src = env::var("GMGN_TRADE_SRC").unwrap_or_else(|_| "gmgn".into());
    let from = env::var("GMGN_FROM").unwrap_or_default();
    let slippage = env::var("GMGN_TRADE_SLIPPAGE_PCT")
        .ok()
        .and_then(|val| val.parse::<f64>().ok())
        .unwrap_or(10.0);

    let args: Vec<String> = env::args().collect();
    let token_in = args.get(1).cloned().unwrap_or(token_in);
    let token_out = args.get(2).cloned().unwrap_or(token_out);
    let amount_in = args.get(3).cloned().unwrap_or(amount_in);
    let from = args.get(4).cloned().unwrap_or(from);

    if token_out.trim().is_empty() || amount_in.trim().is_empty() || from.trim().is_empty() {
        eprintln!(
            "Usage: gmgn_swap <token_in|native> <token_out> <amount_in_wei|amount_in_bnb> <from_address>\n\
             Or set env GMGN_TOKEN_IN, GMGN_TOKEN_OUT, GMGN_AMOUNT_IN, GMGN_FROM"
        );
        return Ok(());
    }

    let token_in = parse_address(&token_in)?;
    let token_out = parse_address(&token_out)?;
    let amount_in = parse_amount(&amount_in)?;
    let from = Address::from_str(from.trim()).context("invalid from address")?;

    let settings = GmgnSettings::from_env();
    let client = GmgnClient::new(settings)?;
    let src = if src.trim().is_empty() {
        None
    } else {
        Some(src.as_str())
    };

    let tx = client
        .build_swap_tx_exact_in(
            &chain, &chain, token_in, token_out, amount_in, from, slippage, src,
        )
        .await?;

    print_tx(&tx);
    Ok(())
}
