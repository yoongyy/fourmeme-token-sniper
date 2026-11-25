use anyhow::{Result, anyhow};
use ethabi::{ParamType, decode};
use ethers::types::{Address, Log, U256};
use ethers::utils::keccak256;

use crate::types::TokenCreated;

pub fn keccak256_hex(sig: &str) -> [u8; 32] {
    keccak256(sig)
}

pub fn parse_token_created_log(log: &Log) -> Result<TokenCreated> {
    let decoded = decode(
        &[
            ParamType::Address,
            ParamType::Address,
            ParamType::Uint(256),
            ParamType::String,
            ParamType::String,
            ParamType::Uint(256),
            ParamType::Uint(256),
            ParamType::Uint(256),
        ],
        &log.data.0,
    )?;

    let creator = decoded[0]
        .clone()
        .into_address()
        .ok_or_else(|| anyhow!("missing creator address"))?;
    let token = decoded[1]
        .clone()
        .into_address()
        .ok_or_else(|| anyhow!("missing token address"))?;

    let name = decoded[3].clone().into_string().unwrap_or_default();
    let symbol = decoded[4].clone().into_string().unwrap_or_default();

    Ok(TokenCreated {
        creator,
        token,
        name,
        symbol,
    })
}

pub fn parse_token_purchase_log(log: &Log) -> Result<Address> {
    let decoded = decode(
        &[
            ParamType::Address,
            ParamType::Address,
            ParamType::Uint(256),
            ParamType::Uint(256),
            ParamType::Uint(256),
            ParamType::Uint(256),
            ParamType::Uint(256),
            ParamType::Uint(256),
        ],
        &log.data.0,
    )?;

    decoded[0]
        .clone()
        .into_address()
        .ok_or_else(|| anyhow!("missing token address"))
}

pub struct LiquidityAddedLog {
    pub base: Address,
    pub quote: Address,
    pub funds: U256,
}

pub fn parse_liquidity_added_log(log: &Log) -> Result<LiquidityAddedLog> {
    let decoded = decode(
        &[
            ParamType::Address,
            ParamType::Uint(256),
            ParamType::Address,
            ParamType::Uint(256),
        ],
        &log.data.0,
    )?;

    let base = decoded[0]
        .clone()
        .into_address()
        .ok_or_else(|| anyhow!("missing base address"))?;
    let quote = decoded[2]
        .clone()
        .into_address()
        .ok_or_else(|| anyhow!("missing quote address"))?;
    let funds = decoded[3]
        .clone()
        .into_uint()
        .ok_or_else(|| anyhow!("missing funds value"))?;

    Ok(LiquidityAddedLog { base, quote, funds })
}
