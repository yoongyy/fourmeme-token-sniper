use anyhow::{Context, Result, anyhow};
use ethers::{
    abi::{Abi, AbiParser},
    contract::{Contract, EthAbiCodec, EthAbiType},
    providers::{JsonRpcClient, Provider},
    types::{Address, U256},
    utils::format_units,
};
use once_cell::sync::Lazy;
use reqwest::Client;
use serde_json::from_slice;
use std::{env, str::FromStr, sync::Arc, time::Duration};
use tokio::time::sleep;

static TOKEN_MANAGER_ABI: Lazy<Abi> = Lazy::new(|| {
    from_slice(include_bytes!("abi/TokenManager2.lite.abi")).expect("token manager abi")
});
static HELPER_ABI: Lazy<Abi> = Lazy::new(|| {
    from_slice(include_bytes!("abi/TokenManagerHelper3.abi")).expect("token manager helper abi")
});
static FACTORY_ABI: Lazy<Abi> = Lazy::new(|| {
    AbiParser::default()
        .parse(&["function getPair(address,address) view returns (address)"])
        .expect("pancake factory abi")
});
static PAIR_ABI_SIMPLE: Lazy<Abi> = Lazy::new(|| {
    AbiParser::default()
        .parse(&[
            "function getReserves() view returns (uint112,uint112,uint32)",
            "function token0() view returns (address)",
            "function token1() view returns (address)",
        ])
        .expect("pair abi")
});
static BNB_BUSD_PAIR: Lazy<Address> = Lazy::new(|| {
    Address::from_str("0x1b96b92314c44b159149f7e0303511fb2fc4774f").expect("bnb/busd pair")
});
static WBNB_ADDRESS: Lazy<Address> =
    Lazy::new(|| Address::from_str("0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c").expect("wbnb"));
static ERC20_SIMPLE_ABI: Lazy<Abi> = Lazy::new(|| {
    AbiParser::default()
        .parse(&[
            "function balanceOf(address owner) view returns (uint256)",
            "function decimals() view returns (uint8)",
        ])
        .expect("erc20 simple abi")
});
static TOKEN_ABI: Lazy<Abi> = Lazy::new(|| {
    AbiParser::default()
        .parse(&[
            "function name() view returns (string)",
            "function symbol() view returns (string)",
        ])
        .expect("token abi")
});
static PANCAKE_V3_ENABLED: Lazy<bool> = Lazy::new(|| {
    let Ok(raw) = env::var("PANCAKE_V3_ROUTER") else {
        return true;
    };
    let Ok(addr) = Address::from_str(raw.trim()) else {
        return true;
    };
    !addr.is_zero()
});

#[derive(Clone, Debug, Default, EthAbiType, EthAbiCodec)]
pub struct TokenInfo {
    pub base: Address,
    pub quote: Address,
    pub template: U256,
    pub total_supply: U256,
    pub max_offers: U256,
    pub max_raising: U256,
    pub launch_time: U256,
    pub offers: U256,
    pub funds: U256,
    pub last_price: U256,
    pub k: U256,
    pub t: U256,
    pub status: U256,
}

#[derive(Clone, Debug, Default, EthAbiType, EthAbiCodec)]
pub struct HelperTokenInfo {
    pub version: U256,
    pub token_manager: Address,
    pub quote: Address,
    pub last_price: U256,
    pub trading_fee_rate: U256,
    pub min_trading_fee: U256,
    pub launch_time: U256,
    pub offers: U256,
    pub max_offers: U256,
    pub funds: U256,
    pub max_funds: U256,
    pub liquidity_added: bool,
}

#[derive(Clone, Debug, Default)]
pub struct HelperPriceInfo {
    pub quote: Address,
    pub last_price_bnb: Option<f64>,
    pub last_price_usd: Option<f64>,
    pub liquidity_added: bool,
    pub pancake_pair: Option<Address>,
    pub details: HelperTokenInfo,
    pub pancake_factory: Address,
    pub pancake_v3_factory: Address,
}

pub async fn fetch_last_price<C>(
    provider: Arc<Provider<C>>,
    manager_address: Address,
    token: Address,
) -> Result<Option<f64>>
where
    C: JsonRpcClient + 'static,
{
    let contract = Contract::new(manager_address, TOKEN_MANAGER_ABI.clone(), provider.clone());
    let info: TokenInfo = (async {
        let mut backoff_ms = 80u64;
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let call = contract
                .method("_tokenInfos", token)
                .context("build _tokenInfos call")?;
            match call.call().await {
                Ok(info) => break Ok(info),
                Err(err) => {
                    let err_text = format!("{err:?}").to_lowercase();
                    if attempt < 4 && is_rate_limited_error(&err_text) {
                        sleep(Duration::from_millis(backoff_ms)).await;
                        backoff_ms = (backoff_ms * 2).min(1000);
                        continue;
                    }
                    break Err(anyhow!(err));
                }
            }
        }
    })
    .await
    .context("token manager _tokenInfos call failed")?;
    if info.base.is_zero() {
        return Ok(None);
    }
    let price_raw = if !info.last_price.is_zero() {
        info.last_price
    } else {
        (async {
            let mut backoff_ms = 80u64;
            let mut attempt = 0u32;
            loop {
                attempt += 1;
                let call = contract
                    .method::<_, U256>("calcLastPrice", info.clone())
                    .context("build calcLastPrice call")?;
                match call.call().await {
                    Ok(value) => break Ok(value),
                    Err(err) => {
                        let err_text = format!("{err:?}").to_lowercase();
                        if attempt < 4 && is_rate_limited_error(&err_text) {
                            sleep(Duration::from_millis(backoff_ms)).await;
                            backoff_ms = (backoff_ms * 2).min(1000);
                            continue;
                        }
                        break Err(anyhow!(err));
                    }
                }
            }
        })
        .await
        .context("calcLastPrice call failed")?
    };
    if price_raw.is_zero() {
        return Ok(None);
    }
    let price = parse_price(price_raw)?;
    let usd_value = convert_price_to_usd(&provider, price, info.quote).await?;
    Ok(Some(usd_value))
}

pub async fn fetch_helper_info<C>(
    provider: Arc<Provider<C>>,
    helper_address: Address,
    token: Address,
) -> Result<Option<HelperPriceInfo>>
where
    C: JsonRpcClient + 'static,
{
    let contract = Contract::new(helper_address, HELPER_ABI.clone(), provider.clone());
    let info: HelperTokenInfo = (async {
        let mut backoff_ms = 200u64;
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let call = contract
                .method::<_, HelperTokenInfo>("getTokenInfo", token)
                .map_err(anyhow::Error::from)
                .context("build getTokenInfo call")?;
            let result = call.call().await.map_err(anyhow::Error::from);
            match result {
                Ok(info) => break Ok(info),
                Err(err) => {
                    if attempt >= 3 {
                        break Err(err);
                    }
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = backoff_ms.saturating_mul(2);
                }
            }
        }
    })
    .await
    .context("token helper getTokenInfo call failed")?;
    let pancake_pair: Address = contract
        .method("getPancakePair", token)
        .context("build getPancakePair call")?
        .call()
        .await
        .unwrap_or_else(|err| {
            eprintln!(
                "[token-helper] failed to get pancake pair for {:#x}: {err:?}",
                token
            );
            Address::zero()
        });
    let pancake_factory: Address = contract
        .method("PANCAKE_FACTORY", ())
        .context("build PANCAKE_FACTORY call")?
        .call()
        .await
        .unwrap_or_else(|err| {
            eprintln!("[token-helper] failed to read PANCAKE_FACTORY: {err:?}");
            Address::zero()
        });
    let pancake_v3_factory: Address = if *PANCAKE_V3_ENABLED {
        contract
            .method("PANCAKE_V3_FACTORY", ())
            .context("build PANCAKE_V3_FACTORY call")?
            .call()
            .await
            .unwrap_or_else(|err| {
                eprintln!("[token-helper] failed to read PANCAKE_V3_FACTORY: {err:?}");
                Address::zero()
            })
    } else {
        Address::zero()
    };
    let last_price_bnb = if info.last_price.is_zero() {
        None
    } else {
        Some(parse_price(info.last_price)?)
    };
    let last_price_usd = match last_price_bnb {
        Some(raw) => Some(convert_price_to_usd(&provider, raw, info.quote).await?),
        None => None,
    };
    Ok(Some(HelperPriceInfo {
        quote: info.quote,
        last_price_bnb,
        last_price_usd,
        liquidity_added: info.liquidity_added,
        pancake_pair: if pancake_pair.is_zero() {
            None
        } else {
            Some(pancake_pair)
        },
        details: info,
        pancake_factory,
        pancake_v3_factory,
    }))
}

pub async fn fetch_pancake_pair_price_usd<C>(
    provider: Arc<Provider<C>>,
    pair_address: Address,
    token: Address,
) -> Result<Option<f64>>
where
    C: JsonRpcClient + 'static,
{
    if pair_address.is_zero() {
        return Ok(None);
    }
    let pair_contract = Contract::new(pair_address, PAIR_ABI_SIMPLE.clone(), provider.clone());
    let reserves = pair_contract
        .method::<(), (U256, U256, u32)>("getReserves", ())?
        .call()
        .await
        .context("getReserves call failed")?;
    let token0 = pair_contract
        .method::<(), Address>("token0", ())?
        .call()
        .await
        .context("token0 call failed")?;
    let token1 = pair_contract
        .method::<(), Address>("token1", ())?
        .call()
        .await
        .context("token1 call failed")?;
    let (reserve_wbnb, reserve_token) = if token0 == *WBNB_ADDRESS && token1 == token {
        (reserves.0, reserves.1)
    } else if token1 == *WBNB_ADDRESS && token0 == token {
        (reserves.1, reserves.0)
    } else {
        return Ok(None);
    };
    if reserve_wbnb.is_zero() || reserve_token.is_zero() {
        return Ok(None);
    }
    let decimals = Contract::new(token, ERC20_SIMPLE_ABI.clone(), provider.clone())
        .method::<(), u8>("decimals", ())?
        .call()
        .await
        .unwrap_or_else(|err| {
            eprintln!(
                "[token-helper] failed to read token decimals for {:#x}: {err:?}",
                token
            );
            18u8
        });
    let reserve_wbnb = format_units(reserve_wbnb, 18)
        .context("failed to format WBNB reserve")?
        .parse::<f64>()
        .context("failed to parse WBNB reserve")?;
    let reserve_token = format_units(reserve_token, u32::from(decimals))
        .context("failed to format token reserve")?
        .parse::<f64>()
        .context("failed to parse token reserve")?;
    if reserve_token <= std::f64::EPSILON {
        return Ok(None);
    }
    let price_bnb = reserve_wbnb / reserve_token;
    let usd_value = convert_price_to_usd(&provider, price_bnb, *WBNB_ADDRESS).await?;
    Ok(Some(usd_value))
}

pub async fn fetch_pancake_pair_liquidity_usd<C>(
    provider: Arc<Provider<C>>,
    pair_address: Address,
    token: Address,
    quote: Address,
) -> Result<Option<f64>>
where
    C: JsonRpcClient + 'static,
{
    if pair_address.is_zero() {
        return Ok(None);
    }
    let pair_contract = Contract::new(pair_address, PAIR_ABI_SIMPLE.clone(), provider.clone());
    let reserves = pair_contract
        .method::<(), (U256, U256, u32)>("getReserves", ())?
        .call()
        .await
        .context("getReserves call failed")?;
    let token0 = pair_contract
        .method::<(), Address>("token0", ())?
        .call()
        .await
        .context("token0 call failed")?;
    let token1 = pair_contract
        .method::<(), Address>("token1", ())?
        .call()
        .await
        .context("token1 call failed")?;
    let quote_reserve = if token0 == token && token1 == quote {
        reserves.1
    } else if token1 == token && token0 == quote {
        reserves.0
    } else {
        return Ok(None);
    };
    if quote_reserve.is_zero() {
        return Ok(None);
    }
    let quote_decimals = if quote.is_zero() || quote == *WBNB_ADDRESS {
        18u8
    } else {
        fetch_token_decimals(provider.clone(), quote)
            .await
            .unwrap_or(18u8)
    };
    let quote_amount = u256_to_f64(quote_reserve, quote_decimals)?;
    if quote_amount <= f64::EPSILON {
        return Ok(None);
    }
    let quote_usd = convert_price_to_usd(&provider, quote_amount, quote).await?;
    Ok(Some(quote_usd * 2.0))
}

pub async fn derive_pancake_pair<C>(
    provider: Arc<Provider<C>>,
    factory: Address,
    token: Address,
    quote: Address,
) -> Result<Option<Address>>
where
    C: JsonRpcClient + 'static,
{
    if factory.is_zero() {
        return Ok(None);
    }
    let quote_token = if quote.is_zero() {
        *WBNB_ADDRESS
    } else {
        quote
    };
    let contract = Contract::new(factory, FACTORY_ABI.clone(), provider);
    let result: Address = contract
        .method("getPair", (token, quote_token))
        .context("build getPair call")?
        .call()
        .await
        .context("getPair call failed")?;
    if result.is_zero() {
        Ok(None)
    } else {
        Ok(Some(result))
    }
}

pub async fn try_buy_estimate<C>(
    provider: Arc<Provider<C>>,
    helper_address: Address,
    token: Address,
    funds: U256,
) -> Result<Option<U256>>
where
    C: JsonRpcClient + 'static,
{
    if let Ok(Some(helper_info)) = fetch_helper_info(provider.clone(), helper_address, token).await
    {
        if helper_info.liquidity_added {
            if let Some(pair_address) = helper_info.pancake_pair {
                let pair_contract =
                    Contract::new(pair_address, PAIR_ABI_SIMPLE.clone(), provider.clone());
                if let (Ok(reserves), Ok(token0)) = (
                    pair_contract
                        .method::<(), (U256, U256, u32)>("getReserves", ())?
                        .call()
                        .await,
                    pair_contract
                        .method::<(), Address>("token0", ())?
                        .call()
                        .await,
                ) {
                    let (reserve0, reserve1, _) = reserves;
                    let (reserve_in, reserve_out) = if token0 == *WBNB_ADDRESS {
                        (reserve0, reserve1)
                    } else {
                        (reserve1, reserve0)
                    };

                    if !reserve_in.is_zero() && !reserve_out.is_zero() {
                        let amount_in_with_fee = funds * 9975;
                        let numerator = amount_in_with_fee * reserve_out;
                        let denominator = reserve_in * 10000 + amount_in_with_fee;
                        let amount_out = numerator / denominator;
                        return Ok(Some(amount_out));
                    }
                }
            }
        }
    }

    // Fallback to original implementation if not liquidity or other issue
    if helper_address.is_zero() {
        return Ok(None);
    }
    let contract = Contract::new(helper_address, HELPER_ABI.clone(), provider);
    let call = match contract.method::<_, (Address, Address, U256, U256, U256, U256, U256, U256)>(
        "tryBuy",
        (token, U256::zero(), funds),
    ) {
        Ok(builder) => builder,
        Err(err) => {
            eprintln!(
                "[token-helper] tryBuy build failed for {:#x}: {err:?}",
                token
            );
            return Ok(None);
        }
    };
    match call.call().await {
        Ok((_, _, amount, _, _, _, _, _)) => Ok(Some(amount)),
        Err(err) => {
            eprintln!(
                "[token-helper] tryBuy call failed for {:#x}: {err:?}",
                token
            );
            Ok(None)
        }
    }
}

pub fn format_estimated_tokens(raw: U256, token_decimals: u8) -> Option<f64> {
    match u256_to_f64(raw, token_decimals) {
        Ok(value) if value > std::f64::EPSILON => Some(value),
        _ => u256_to_f64(raw, 18)
            .ok()
            .filter(|value| *value > std::f64::EPSILON),
    }
}

pub async fn fetch_token_balance<C>(
    provider: Arc<Provider<C>>,
    token: Address,
    owner: Address,
) -> Result<U256>
where
    C: JsonRpcClient + 'static,
{
    let contract = Contract::new(token, ERC20_SIMPLE_ABI.clone(), provider);
    contract
        .method("balanceOf", owner)
        .context("build balanceOf call")?
        .call()
        .await
        .context("balanceOf call failed")
}

pub async fn fetch_token_decimals<C>(provider: Arc<Provider<C>>, token: Address) -> Result<u8>
where
    C: JsonRpcClient + 'static,
{
    let contract = Contract::new(token, ERC20_SIMPLE_ABI.clone(), provider);
    contract
        .method("decimals", ())
        .context("build decimals call")?
        .call()
        .await
        .context("decimals call failed")
}

pub async fn fetch_token_name_symbol<C>(
    provider: Arc<Provider<C>>,
    token: Address,
) -> Result<(Option<String>, Option<String>)>
where
    C: JsonRpcClient + 'static,
{
    let contract = Contract::new(token, TOKEN_ABI.clone(), provider);
    let name = match contract.method::<_, String>("name", ()) {
        Ok(call) => call.call().await.ok(),
        Err(_) => None,
    };
    let symbol = match contract.method::<_, String>("symbol", ()) {
        Ok(call) => call.call().await.ok(),
        Err(_) => None,
    };
    Ok((clean_token_string(name), clean_token_string(symbol)))
}

fn parse_price(value: U256) -> Result<f64> {
    let formatted = format_units(value, 18).context("failed to format TokenManager price")?;
    formatted
        .parse::<f64>()
        .map_err(|err| anyhow!("failed to parse TokenManager price: {err:?}"))
}

pub async fn convert_price_to_usd<C>(
    provider: &Arc<Provider<C>>,
    price: f64,
    quote: Address,
) -> Result<f64>
where
    C: JsonRpcClient + 'static,
{
    if quote == Address::zero() || quote == *WBNB_ADDRESS {
        let bnb_price = fetch_bnb_usd_price(provider).await?;
        Ok(price * bnb_price)
    } else {
        Ok(price)
    }
}

pub async fn fetch_bnb_usd_price<C>(provider: &Arc<Provider<C>>) -> Result<f64>
where
    C: JsonRpcClient + 'static,
{
    if let Ok(http_price) = fetch_bnb_usd_price_http().await {
        return Ok(http_price);
    }
    let contract = Contract::new(*BNB_BUSD_PAIR, PAIR_ABI_SIMPLE.clone(), provider.clone());
    let token0: Address = contract
        .method("token0", ())
        .context("build token0 call for BNB/BUSD")?
        .call()
        .await
        .context("failed to read BNB/BUSD token0")?;
    let (reserve0, reserve1, _): (U256, U256, u32) = contract
        .method("getReserves", ())
        .context("build BNB/BUSD getReserves call")?
        .call()
        .await
        .context("failed to read BNB/BUSD reserves")?;
    let (bnb_raw, busd_raw) = if token0 == *WBNB_ADDRESS {
        (reserve0, reserve1)
    } else {
        (reserve1, reserve0)
    };
    let busd = u256_to_f64(busd_raw, 18)?;
    let wbnb = u256_to_f64(bnb_raw, 18)?;
    if wbnb <= f64::EPSILON {
        return Err(anyhow!("BNB reserve is zero in BNB/BUSD pair"));
    }
    Ok(busd / wbnb)
}

async fn fetch_bnb_usd_price_http() -> Result<f64> {
    #[derive(serde::Deserialize)]
    struct PriceResponse {
        price: String,
    }
    let client = Client::new();
    let resp: PriceResponse = client
        .get("https://api.binance.com/api/v3/ticker/price?symbol=BNBUSDT")
        .send()
        .await
        .context("failed to query Binance for BNB price")?
        .json()
        .await
        .context("failed to parse Binance price response")?;
    resp.price
        .parse::<f64>()
        .map_err(|err| anyhow!("failed to parse Binance price string: {err:?}"))
}

pub fn u256_to_f64(value: U256, decimals: u8) -> Result<f64> {
    let formatted =
        format_units(value, decimals as i32).context("failed to format reserve value")?;
    formatted
        .parse::<f64>()
        .map_err(|err| anyhow!("failed to parse reserve value: {err:?}"))
}

fn clean_token_string(value: Option<String>) -> Option<String> {
    value
        .map(|val| val.trim().to_string())
        .filter(|val| !val.is_empty())
}

fn is_rate_limited_error(text: &str) -> bool {
    text.contains("rate limit")
        || text.contains("rps limit")
        || text.contains("too many requests")
        || text.contains("code: -32005")
        || text.contains("code: 429")
        || text.contains("status code: 429")
}
