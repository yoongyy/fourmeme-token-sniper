use crate::config::GmgnSettings;
use anyhow::{Context, Result, anyhow};
use dashmap::DashMap;
use ethers::types::{Address, Bytes, U256};
use ethers::utils::hex;
use reqwest::{Client, header::HeaderMap};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{path::Path, process::Stdio, str::FromStr, sync::Arc, time::Instant};
use tokio::{
    process::Command as TokioCommand,
    time::{Duration, sleep},
};

#[derive(Clone, Debug, Default)]
pub struct GmgnTokenInfo {
    pub symbol: Option<String>,
    pub name: Option<String>,
    pub liquidity: Option<f64>,
    pub price: Option<f64>,
    pub holder_count: Option<u64>,
    pub is_honeypot: Option<bool>,
    pub is_open_source: Option<bool>,
    pub is_renounced: Option<bool>,
    pub is_locked: Option<bool>,
    pub pool_address: Option<Address>,
    pub pool_fee: Option<u32>,
    pub tick_spacing: Option<i32>,
    pub swap_type: Option<u8>,
    pub exchange: Option<String>,
    pub quote_symbol: Option<String>,
    pub initial_liquidity: Option<f64>,
    pub creation_timestamp: Option<u64>,
}

impl GmgnTokenInfo {
    pub fn is_complete(&self) -> bool {
        self.pool_address.is_some()
            && self
                .symbol
                .as_ref()
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false)
            && self
                .name
                .as_ref()
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false)
            && self
                .exchange
                .as_ref()
                .map(|s| !s.is_empty())
                .unwrap_or(false)
            && self
                .quote_symbol
                .as_ref()
                .map(|s| !s.is_empty())
                .unwrap_or(false)
            && self.liquidity.is_some()
            && self.initial_liquidity.is_some()
            && self.holder_count.is_some()
            && self.is_honeypot.is_some()
            && self.is_open_source.is_some()
            && self.is_renounced.is_some()
            && self.is_locked.is_some()
    }
}

struct CachedEntry {
    info: GmgnTokenInfo,
    timestamp: Instant,
}

pub struct GmgnClient {
    settings: GmgnSettings,
    http: Client,
    cache: DashMap<Address, CachedEntry>,
}

#[derive(Clone, Debug)]
pub struct GmgnSwapTx {
    pub to: Address,
    pub data: Bytes,
    pub value: U256,
    pub gas_limit: Option<U256>,
    pub chain_id: Option<u64>,
}

impl GmgnClient {
    pub fn new(settings: GmgnSettings) -> Result<Arc<Self>> {
        let http = Client::builder()
            .user_agent(settings.user_agent.clone())
            .build()
            .context("failed to build GMGN HTTP client")?;

        Ok(Arc::new(Self {
            settings,
            http,
            cache: DashMap::new(),
        }))
    }

    pub async fn wait_for_token(
        &self,
        address: Address,
        timeout: Duration,
    ) -> Option<GmgnTokenInfo> {
        if let Some(info) = self.get_cached(address) {
            if info.is_complete() {
                return Some(info);
            }
        }

        let deadline = Instant::now() + timeout;
        let poll_interval = Duration::from_millis(500);
        let mut last_info = self.get_cached(address);

        loop {
            match self.fetch_token_info(address).await {
                Ok(Some(info)) => {
                    if info.is_complete() {
                        return Some(info);
                    }
                    last_info = Some(info);
                }
                Ok(None) => {}
                Err(err) => {
                    eprintln!("[GMGN] HTTP error: {err:?}");
                }
            }

            if Instant::now() >= deadline {
                return last_info.or_else(|| self.get_cached(address));
            }

            sleep(poll_interval).await;
        }
    }

    pub async fn fetch_token_info_fresh(&self, address: Address) -> Option<GmgnTokenInfo> {
        match self.fetch_token_info(address).await {
            Ok(info) => info,
            Err(err) => {
                eprintln!("[GMGN] HTTP error: {err:?}");
                None
            }
        }
    }

    fn get_cached(&self, address: Address) -> Option<GmgnTokenInfo> {
        if let Some(entry) = self.cache.get(&address) {
            if entry.timestamp.elapsed() <= self.settings.cache_ttl() {
                return Some(entry.value().info.clone());
            }
        }
        None
    }

    async fn fetch_token_info(&self, address: Address) -> Result<Option<GmgnTokenInfo>> {
        if let Some(body) = self.try_fetch_http(address).await? {
            if let Some(info) = self.parse_http_response(&body, address) {
                return Ok(Some(info));
            }
        }

        if let Some(info) = self.fetch_via_helper(address).await? {
            return Ok(Some(info));
        }

        Ok(None)
    }

    async fn try_fetch_http(&self, address: Address) -> Result<Option<Value>> {
        let payload = TokenLookupRequest {
            chain: &self.settings.chain,
            addresses: &[format!("{:#x}", address)],
        };

        if self.settings.log_requests {
            eprintln!(
                "[GMGN] calling {} (chain={} address={:#x})",
                self.settings.http_url, self.settings.chain, address
            );
        }

        let response = match self
            .http
            .post(&self.settings.http_url)
            .headers(self.build_headers())
            .json(&payload)
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(err) => {
                eprintln!("[GMGN] HTTP request error: {err:?}");
                return Ok(None);
            }
        };

        if !response.status().is_success() {
            eprintln!(
                "[GMGN] HTTP status {} when querying {}",
                response.status(),
                self.settings.http_url
            );
            return Ok(None);
        }

        let raw = response
            .text()
            .await
            .context("failed to read GMGN HTTP body")?;
        match serde_json::from_str::<Value>(&raw) {
            Ok(value) => Ok(Some(value)),
            Err(err) => {
                if Self::looks_like_cloudflare(&raw) {
                    eprintln!("[GMGN] Cloudflare challenge encountered; using helper");
                    Ok(None)
                } else {
                    Err(err.into())
                }
            }
        }
    }

    async fn fetch_via_helper(&self, address: Address) -> Result<Option<GmgnTokenInfo>> {
        let helper = match self.settings.python_helper() {
            Some(path) => path,
            None => return Ok(None),
        };
        let retries = self.settings.helper_retry_count();
        for attempt in 0..retries {
            if attempt > 0 {
                sleep(Duration::from_millis(500)).await;
            }
            match self.invoke_helper(helper, address).await {
                Ok(Some(info)) => return Ok(Some(info)),
                Ok(None) => continue,
                Err(err) => eprintln!(
                    "[GMGN] helper attempt {}/{} failed: {err:?}",
                    attempt + 1,
                    retries
                ),
            }
        }
        Ok(None)
    }

    pub async fn build_swap_tx_exact_in(
        &self,
        token_in_chain: &str,
        token_out_chain: &str,
        token_in_address: Address,
        token_out_address: Address,
        amount_in: U256,
        from_address: Address,
        slippage: f64,
        src: Option<&str>,
    ) -> Result<GmgnSwapTx> {
        let routes = self
            .fetch_available_routes_exact_in(
                token_in_chain,
                token_out_chain,
                token_in_address,
                token_out_address,
                amount_in,
                src,
            )
            .await?;
        let route = routes
            .into_iter()
            .max_by(|a, b| {
                let a_out = parse_u256_from_value(a.get("amount_out")).unwrap_or_default();
                let b_out = parse_u256_from_value(b.get("amount_out")).unwrap_or_default();
                a_out.cmp(&b_out)
            })
            .ok_or_else(|| anyhow!("GMGN returned no routes"))?;

        let tx = self
            .simulate_route_exact_in(&route, slippage, from_address)
            .await?;
        if let Some(gas_limit) = tx.gas_limit {
            if gas_limit.is_zero() {
                return Err(anyhow!("GMGN simulation returned gas_limit=0"));
            }
        }
        Ok(tx)
    }

    pub async fn fetch_available_routes_exact_in(
        &self,
        token_in_chain: &str,
        token_out_chain: &str,
        token_in_address: Address,
        token_out_address: Address,
        amount_in: U256,
        src: Option<&str>,
    ) -> Result<Vec<Value>> {
        let url = format!(
            "{}/tapi/v1/router/available_routes_exact_in",
            self.settings.trade_api_base.trim_end_matches('/')
        );
        let mut request = self
            .http
            .get(&url)
            .headers(self.build_trade_headers())
            .query(&[
                ("token_in_chain", token_in_chain),
                ("token_out_chain", token_out_chain),
                ("token_in_address", &format!("{:#x}", token_in_address)),
                ("token_out_address", &format!("{:#x}", token_out_address)),
                ("in_amount", &amount_in.to_string()),
            ]);
        if let Some(src_value) = src {
            request = request.query(&[("src", src_value)]);
        }
        if self.settings.log_requests {
            eprintln!("[GMGN] GET {} (amount_in={})", url, amount_in);
        }
        let response = request.send().await.context("GMGN routes request failed")?;
        if !response.status().is_success() {
            return Err(anyhow!("GMGN routes status {}", response.status()));
        }
        let raw = response
            .text()
            .await
            .context("failed to read GMGN routes body")?;
        let body: GmgnRoutesResponse =
            serde_json::from_str(&raw).context("failed to decode GMGN routes response")?;
        if body.code != 0 {
            return Err(anyhow!(
                "GMGN routes error: {} (raw={})",
                body.msg.unwrap_or_else(|| "unknown error".into()),
                raw
            ));
        }
        let routes = body.data.map(|data| data.routes).unwrap_or_default();
        Ok(routes)
    }

    pub async fn simulate_route_exact_in(
        &self,
        route: &Value,
        slippage: f64,
        from_address: Address,
    ) -> Result<GmgnSwapTx> {
        let url = format!(
            "{}/tapi/v1/router/simulate_route_exact_in",
            self.settings.trade_api_base.trim_end_matches('/')
        );
        let payload = serde_json::json!({
            "route": route,
            "slippage": slippage,
            "from_address": format!("{:#x}", from_address),
        });
        if self.settings.log_requests {
            eprintln!("[GMGN] POST {} (slippage={})", url, slippage);
        }
        let response = self
            .http
            .post(&url)
            .headers(self.build_trade_headers())
            .json(&payload)
            .send()
            .await
            .context("GMGN simulate request failed")?;
        if !response.status().is_success() {
            return Err(anyhow!("GMGN simulate status {}", response.status()));
        }
        let raw = response
            .text()
            .await
            .context("failed to read GMGN simulate body")?;
        let body: GmgnSimulateResponse =
            serde_json::from_str(&raw).context("failed to decode GMGN simulate response")?;
        if body.code != 0 {
            return Err(anyhow!(
                "GMGN simulate error: {} (raw={})",
                body.msg.unwrap_or_else(|| "unknown error".into()),
                raw
            ));
        }
        let tx = body
            .data
            .and_then(|data| data.tx)
            .ok_or_else(|| anyhow!("GMGN simulate response missing tx"))?;
        parse_swap_tx(&tx)
    }

    async fn invoke_helper(
        &self,
        helper: &Path,
        address: Address,
    ) -> Result<Option<GmgnTokenInfo>> {
        let mut cmd = TokioCommand::new(self.settings.python_bin());
        cmd.arg(helper)
            .arg("--chain")
            .arg(&self.settings.chain)
            .arg("--address")
            .arg(format!("{:#x}", address))
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        let output = cmd
            .output()
            .await
            .with_context(|| format!("python helper spawn error ({:?})", helper))?;

        if !output.status.success() {
            if self.settings.log_requests {
                eprintln!("[GMGN] python helper exited with {}", output.status);
            }
            return Ok(None);
        }

        if output.stdout.is_empty() {
            return Ok(None);
        }

        let body: Value =
            serde_json::from_slice(&output.stdout).context("invalid GMGN helper JSON response")?;
        Ok(self.parse_http_response(&body, address))
    }

    fn looks_like_cloudflare(body: &str) -> bool {
        body.contains("cf-error-details") || body.contains("Cloudflare Ray ID")
    }

    fn parse_http_response(&self, body: &Value, address: Address) -> Option<GmgnTokenInfo> {
        let data = body.get("data").unwrap_or(body);

        if let Some(array) = data.as_array() {
            for entry in array {
                if let Some(info) = self.parse_entry(entry) {
                    return Some(info);
                }
            }
            return None;
        }

        if let Some(object) = data.as_object() {
            if let Some(entry) = object
                .get(&format!("{:#x}", address))
                .or_else(|| object.get(&address.to_string()))
                .or_else(|| object.values().next())
            {
                return self.parse_entry(entry);
            }
        }

        None
    }

    fn parse_entry(&self, entry: &Value) -> Option<GmgnTokenInfo> {
        let price_hint = entry
            .get("price")
            .and_then(|value| self.parse_numeric_value(value));
        if let Some(pools) = entry.get("p").and_then(Value::as_array) {
            for pool in pools {
                if let Some(mut info) = self.parse_pool(pool) {
                    if info.price.is_none() {
                        info.price = price_hint;
                    }
                    return Some(info);
                }
            }
        }

        if let Some(pool) = entry.get("pool") {
            if let Some(mut info) = self.parse_pool(pool) {
                if info.price.is_none() {
                    info.price = price_hint;
                }
                return Some(info);
            }
            return None;
        }

        None
    }

    fn parse_pool(&self, pool: &Value) -> Option<GmgnTokenInfo> {
        let token_address = self
            .extract_address(pool, &["ba", "base_address", "token_address"])
            .or_else(|| {
                pool.get("bti")
                    .or_else(|| pool.get("base_token_info"))
                    .and_then(|base| self.extract_address(base, &["a", "address"]))
            })?;

        let base_info = pool.get("bti").or_else(|| pool.get("base_token_info"));
        let pool_address = self.extract_address(pool, &["a", "pa", "pool_address"]);
        let pool_fee = self
            .extract_u64(Some(pool), &["fee", "fee_tier", "feeTier", "f"])
            .and_then(|value| u32::try_from(value).ok());
        let tick_spacing = self
            .extract_i64(Some(pool), &["tickSpacing", "tick_spacing", "ts"])
            .and_then(|value| i32::try_from(value).ok());
        let swap_type = self
            .extract_u64(Some(pool), &["swapType", "swap_type", "st"])
            .and_then(|value| u8::try_from(value).ok());
        let exchange = self.extract_string(Some(pool), &["exchange", "ex", "dex"]);
        let quote_symbol = self.extract_string(Some(pool), &["quote_symbol", "qs"]);
        let _initial_liquidity =
            self.extract_number(Some(pool), &["initial_liquidity", "il", "initialLiquidity"]);

        let symbol = self.extract_string(base_info, &["symbol", "s"]);
        let name = self.extract_string(base_info, &["name", "n"]);
        let liquidity = self
            .extract_number(base_info, &["lqdt", "liquidity", "lq"])
            .or_else(|| self.extract_number(Some(pool), &["lqdt", "liquidity", "lq"]));
        let price = self
            .extract_number(
                base_info,
                &[
                    "price",
                    "price_usd",
                    "priceUsd",
                    "price_bnb",
                    "priceBnb",
                    "p",
                ],
            )
            .or_else(|| {
                self.extract_number(
                    Some(pool),
                    &[
                        "price",
                        "price_usd",
                        "priceUsd",
                        "price_bnb",
                        "priceBnb",
                        "p",
                    ],
                )
            });
        let holder_count = self
            .extract_u64(base_info, &["holder_count", "hc"])
            .or_else(|| self.extract_u64(Some(pool), &["holder_count", "hc"]));
        let is_honeypot = self
            .extract_bool(Some(pool), &["is_honeypot", "honeypot"])
            .or_else(|| self.extract_bool(base_info, &["is_honeypot", "hpot"]));
        let is_open_source = self
            .extract_bool(base_info, &["is_open_source", "open_source", "dx_ul"])
            .or_else(|| self.extract_bool(Some(pool), &["is_open_source", "open_source"]));
        let is_renounced = self
            .extract_bool(base_info, &["renounced"])
            .or_else(|| self.extract_bool(Some(pool), &["renounced"]));

        let lock_info = base_info
            .and_then(|info| info.get("lockInfo").or_else(|| info.get("lock_info")))
            .or_else(|| pool.get("lockInfo").or_else(|| pool.get("lock_info")));
        let is_locked =
            lock_info.and_then(|value| self.extract_bool(Some(value), &["isLock", "locked"]));
        let initial_liquidity = self
            .extract_number(Some(pool), &["initial_liquidity", "il", "initialLiquidity"])
            .or_else(|| {
                base_info.and_then(|info| {
                    self.extract_number(
                        Some(info),
                        &["initial_liquidity", "il", "initialLiquidity"],
                    )
                })
            });
        let creation_timestamp = base_info
            .and_then(|info| {
                self.extract_number(
                    Some(info),
                    &["creation_timestamp", "create_timestamp", "ct", "createTime"],
                )
            })
            .or_else(|| {
                self.extract_number(
                    Some(pool),
                    &[
                        "creation_timestamp",
                        "create_timestamp",
                        "open_timestamp",
                        "openTime",
                    ],
                )
            })
            .map(Self::normalize_timestamp);

        let info = GmgnTokenInfo {
            symbol,
            name,
            liquidity,
            price,
            holder_count,
            is_honeypot,
            is_open_source,
            is_renounced,
            is_locked,
            pool_address,
            pool_fee,
            tick_spacing,
            swap_type,
            exchange,
            quote_symbol,
            initial_liquidity,
            creation_timestamp,
        };

        self.cache.insert(
            token_address,
            CachedEntry {
                info: info.clone(),
                timestamp: Instant::now(),
            },
        );

        Some(info)
    }

    fn normalize_timestamp(value: f64) -> u64 {
        let mut ts = value;
        if ts > 10_000_000_000.0 {
            ts /= 1000.0;
        }
        if ts < 0.0 { 0 } else { ts.round() as u64 }
    }

    fn extract_string(&self, value: Option<&Value>, keys: &[&str]) -> Option<String> {
        value.and_then(|val| {
            keys.iter()
                .find_map(|key| val.get(*key))
                .and_then(Value::as_str)
                .map(|s| s.to_string())
        })
    }

    fn extract_number(&self, value: Option<&Value>, keys: &[&str]) -> Option<f64> {
        value.and_then(|val| {
            keys.iter()
                .find_map(|key| val.get(*key))
                .and_then(|v| self.parse_numeric_value(v))
        })
    }

    fn parse_numeric_value(&self, value: &Value) -> Option<f64> {
        match value {
            Value::Number(num) => num.as_f64(),
            Value::String(text) => {
                let cleaned: String = text
                    .replace(',', "")
                    .chars()
                    .filter(|ch| ch.is_ascii_digit() || matches!(ch, '.' | '-' | 'e' | 'E'))
                    .collect();
                if cleaned.is_empty() {
                    None
                } else {
                    cleaned.parse::<f64>().ok()
                }
            }
            Value::Array(items) => items.iter().find_map(|item| self.parse_numeric_value(item)),
            Value::Object(map) => {
                for key in [
                    "price",
                    "price_usd",
                    "priceUsd",
                    "usd_price",
                    "value",
                    "amount",
                ] {
                    if let Some(inner) = map.get(key) {
                        if let Some(parsed) = self.parse_numeric_value(inner) {
                            return Some(parsed);
                        }
                    }
                }
                map.values()
                    .find_map(|inner| self.parse_numeric_value(inner))
            }
            _ => None,
        }
    }

    fn extract_u64(&self, value: Option<&Value>, keys: &[&str]) -> Option<u64> {
        value.and_then(|val| {
            keys.iter()
                .find_map(|key| val.get(*key))
                .and_then(|v| match v {
                    Value::Number(num) => num.as_u64(),
                    Value::String(text) => text.parse::<u64>().ok(),
                    _ => None,
                })
        })
    }

    fn extract_i64(&self, value: Option<&Value>, keys: &[&str]) -> Option<i64> {
        value.and_then(|val| {
            keys.iter()
                .find_map(|key| val.get(*key))
                .and_then(|v| match v {
                    Value::Number(num) => num.as_i64(),
                    Value::String(text) => text.parse::<i64>().ok(),
                    _ => None,
                })
        })
    }

    fn extract_bool(&self, value: Option<&Value>, keys: &[&str]) -> Option<bool> {
        value.and_then(|val| {
            keys.iter()
                .find_map(|key| val.get(*key))
                .and_then(|v| match v {
                    Value::Bool(flag) => Some(*flag),
                    Value::Number(num) => num.as_i64().map(|n| n != 0),
                    Value::String(text) => {
                        let normalized = text.trim().to_ascii_lowercase();
                        match normalized.as_str() {
                            "true" | "yes" | "1" => Some(true),
                            "false" | "no" | "0" => Some(false),
                            _ => None,
                        }
                    }
                    _ => None,
                })
        })
    }

    fn extract_address(&self, value: &Value, keys: &[&str]) -> Option<Address> {
        keys.iter()
            .find_map(|key| value.get(*key))
            .and_then(|val| match val {
                Value::String(addr) => Address::from_str(addr).ok(),
                _ => None,
            })
    }

    fn build_headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "accept",
            "application/json, text/plain, */*".parse().unwrap(),
        );
        headers.insert("accept-language", "en-US,en;q=0.9".parse().unwrap());
        headers.insert("dnt", "1".parse().unwrap());
        headers.insert(
            "referer",
            format!("https://gmgn.ai/?chain={}", self.settings.chain)
                .parse()
                .unwrap(),
        );
        headers.insert("content-type", "application/json".parse().unwrap());
        if let Some(token) = &self.settings.access_token {
            if let Ok(value) = format!("Bearer {}", token).parse() {
                headers.insert("authorization", value);
            }
        }
        headers
    }

    fn build_trade_headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "accept",
            "application/json, text/plain, */*".parse().unwrap(),
        );
        headers.insert("content-type", "application/json".parse().unwrap());
        headers.insert("user-agent", self.settings.user_agent.parse().unwrap());
        if let Some(key) = &self.settings.route_key {
            if let Ok(value) = key.parse() {
                headers.insert("x-route-key", value);
            }
        }
        headers
    }
}

#[derive(Serialize)]
struct TokenLookupRequest<'a> {
    chain: &'a str,
    addresses: &'a [String],
}

#[derive(Debug, Deserialize)]
struct GmgnRoutesResponse {
    code: i64,
    msg: Option<String>,
    data: Option<GmgnRoutesData>,
}

#[derive(Debug, Deserialize)]
struct GmgnRoutesData {
    routes: Vec<Value>,
}

#[derive(Debug, Deserialize)]
struct GmgnSimulateResponse {
    code: i64,
    msg: Option<String>,
    data: Option<GmgnSimulateData>,
}

#[derive(Debug, Deserialize)]
struct GmgnSimulateData {
    tx: Option<Value>,
}

fn parse_swap_tx(value: &Value) -> Result<GmgnSwapTx> {
    let to = value
        .get("to")
        .and_then(Value::as_str)
        .and_then(|addr| Address::from_str(addr).ok())
        .ok_or_else(|| anyhow!("GMGN simulate tx missing to"))?;
    let data = value
        .get("data")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let data = if data.is_empty() {
        Bytes::default()
    } else {
        Bytes::from(hex::decode(data.trim_start_matches("0x"))?)
    };
    let value_u256 = parse_u256_from_value(value.get("value")).unwrap_or_default();
    let gas_limit = parse_u256_from_value(value.get("gas_limit"));
    let chain_id = value.get("chain_id").and_then(Value::as_u64);
    Ok(GmgnSwapTx {
        to,
        data,
        value: value_u256,
        gas_limit,
        chain_id,
    })
}

fn parse_u256_from_value(value: Option<&Value>) -> Option<U256> {
    match value? {
        Value::String(text) => parse_u256_from_str(text),
        Value::Number(num) => num.as_u64().map(U256::from),
        _ => None,
    }
}

fn parse_u256_from_str(text: &str) -> Option<U256> {
    let trimmed = text.trim();
    if trimmed.starts_with("0x") {
        U256::from_str_radix(trimmed.trim_start_matches("0x"), 16).ok()
    } else {
        U256::from_dec_str(trimmed).ok()
    }
}
