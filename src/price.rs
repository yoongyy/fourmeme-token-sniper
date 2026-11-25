use anyhow::{Context, Result, anyhow};
use ethers::{
    abi::{Abi, AbiParser},
    contract::Contract,
    providers::{JsonRpcClient, Middleware, Provider},
    types::{Address, U256},
    utils::format_units,
};
use once_cell::sync::Lazy;
use std::{str::FromStr, sync::Arc};

static PAIR_ABI: Lazy<Abi> = Lazy::new(|| {
    AbiParser::default()
        .parse(&[
            "function getReserves() view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast)",
            "function token0() view returns (address)",
            "function token1() view returns (address)",
        ])
        .expect("pair abi")
});
static ERC20_ABI: Lazy<Abi> = Lazy::new(|| {
    AbiParser::default()
        .parse(&["function decimals() view returns (uint8)"])
        .expect("erc20 abi")
});
static WBNB_ADDRESS: Lazy<Address> = Lazy::new(|| {
    Address::from_str("0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c").expect("wbnb address")
});
const WBNB_DECIMALS: u8 = 18;

#[derive(Clone)]
pub struct PairPriceContext {
    pair_address: Address,
    token_is_token0: bool,
    token_decimals: u8,
    quote_decimals: u8,
    quote_address: Address,
}

impl PairPriceContext {
    pub fn pair_address(&self) -> Address {
        self.pair_address
    }

    pub fn quote_address(&self) -> Address {
        self.quote_address
    }

    pub fn token_decimals(&self) -> u8 {
        self.token_decimals
    }

    pub fn quote_decimals(&self) -> u8 {
        self.quote_decimals
    }
}

pub async fn build_price_context<C>(
    provider: Arc<Provider<C>>,
    pair_address: Address,
    token_address: Address,
) -> Result<PairPriceContext>
where
    C: JsonRpcClient + 'static,
{
    let (token0, token1) = fetch_pair_tokens(provider.clone(), pair_address).await?;
    let (token_is_token0, quote_address) = if token0 == token_address {
        (true, token1)
    } else if token1 == token_address {
        (false, token0)
    } else {
        return Err(anyhow!(
            "token {:#x} is not part of pair {:#x}",
            token_address,
            pair_address
        ));
    };
    let token_decimals = fetch_token_decimals(provider.clone(), token_address).await;
    let quote_decimals = if quote_address == *WBNB_ADDRESS {
        WBNB_DECIMALS
    } else {
        fetch_token_decimals(provider.clone(), quote_address).await
    };
    Ok(PairPriceContext {
        pair_address,
        token_is_token0,
        token_decimals,
        quote_decimals,
        quote_address,
    })
}

async fn fetch_pair_tokens<C>(
    provider: Arc<Provider<C>>,
    pair_address: Address,
) -> Result<(Address, Address)>
where
    C: JsonRpcClient + 'static,
{
    if pair_address.is_zero() {
        return Err(anyhow!("pair address is zero"));
    }
    let code = provider
        .get_code(pair_address, None)
        .await
        .context("failed to fetch pair code")?;
    if code.0.is_empty() {
        return Err(anyhow!(
            "pair {:#x} has no bytecode deployed yet",
            pair_address
        ));
    }
    let contract = Contract::new(pair_address, PAIR_ABI.clone(), provider.clone());
    let token0: Address = contract
        .method("token0", ())
        .context("build token0 call")?
        .call()
        .await
        .context("token0 call failed")?;
    let token1: Address = contract
        .method("token1", ())
        .context("build token1 call")?
        .call()
        .await
        .context("token1 call failed")?;
    Ok((token0, token1))
}

pub async fn pair_contains_token<C>(
    provider: Arc<Provider<C>>,
    pair_address: Address,
    token_address: Address,
) -> Result<bool>
where
    C: JsonRpcClient + 'static,
{
    let (token0, token1) = fetch_pair_tokens(provider, pair_address).await?;
    Ok(token0 == token_address || token1 == token_address)
}

pub async fn read_pair_price<C>(
    provider: &Arc<Provider<C>>,
    context: &PairPriceContext,
) -> Result<f64>
where
    C: JsonRpcClient + 'static,
{
    let contract = Contract::new(context.pair_address, PAIR_ABI.clone(), provider.clone());
    let (reserve0, reserve1, _): (U256, U256, u32) = contract
        .method("getReserves", ())
        .context("build getReserves call")?
        .call()
        .await
        .context("getReserves call failed")?;
    reserves_to_price(reserve0, reserve1, context)
}

pub struct ReserveSnapshot {
    pub base: f64,
    pub quote: f64,
}

pub async fn debug_reserves<C>(
    provider: &Arc<Provider<C>>,
    context: &PairPriceContext,
) -> Result<ReserveSnapshot>
where
    C: JsonRpcClient + 'static,
{
    let contract = Contract::new(context.pair_address, PAIR_ABI.clone(), provider.clone());
    let (reserve0, reserve1, _): (U256, U256, u32) = contract
        .method("getReserves", ())
        .context("build getReserves call")?
        .call()
        .await
        .context("getReserves call failed")?;
    let base_raw = if context.token_is_token0 {
        reserve0
    } else {
        reserve1
    };
    let quote_raw = if context.token_is_token0 {
        reserve1
    } else {
        reserve0
    };
    Ok(ReserveSnapshot {
        base: u256_to_f64(base_raw, context.token_decimals)?,
        quote: u256_to_f64(quote_raw, context.quote_decimals)?,
    })
}

async fn fetch_token_decimals<C>(provider: Arc<Provider<C>>, token: Address) -> u8
where
    C: JsonRpcClient + 'static,
{
    let contract = Contract::new(token, ERC20_ABI.clone(), provider);
    match contract.method::<_, u8>("decimals", ()) {
        Ok(call) => match call.call().await {
            Ok(value) => value,
            Err(err) => {
                eprintln!("[Price] failed to read decimals for {:#x}: {err:?}", token);
                WBNB_DECIMALS
            }
        },
        Err(err) => {
            eprintln!(
                "[Price] failed to prepare decimals call for {:#x}: {err:?}",
                token
            );
            WBNB_DECIMALS
        }
    }
}

fn reserves_to_price(reserve0: U256, reserve1: U256, context: &PairPriceContext) -> Result<f64> {
    let base_reserve = if context.token_is_token0 {
        reserve0
    } else {
        reserve1
    };
    let quote_reserve = if context.token_is_token0 {
        reserve1
    } else {
        reserve0
    };
    let base = u256_to_f64(base_reserve, context.token_decimals)?;
    let quote = u256_to_f64(quote_reserve, context.quote_decimals)?;
    if base <= f64::EPSILON {
        return Err(anyhow!("base reserve is zero"));
    }
    Ok(quote / base)
}

fn u256_to_f64(value: U256, decimals: u8) -> Result<f64> {
    let formatted =
        format_units(value, decimals as i32).context("failed to format reserve units")?;
    formatted
        .parse::<f64>()
        .context("failed to parse formatted reserve")
}
