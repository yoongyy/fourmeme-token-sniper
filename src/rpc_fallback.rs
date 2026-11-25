use anyhow::{Result, anyhow};
use ethers::providers::RpcError;
use ethers::providers::{
    Http, HttpRateLimitRetryPolicy, JsonRpcClient, Provider, ProviderError, RetryClient,
    RetryClientBuilder,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct RpcHttpClient {
    inner: Arc<RpcHttpClientInner>,
}

#[derive(Debug)]
struct RpcHttpClientInner {
    clients: Vec<RetryClient<Http>>,
    index: AtomicUsize,
}

pub type RpcProvider = Provider<RpcHttpClient>;

pub fn rpc_urls_from_env(primary_url: String, fallback_env_keys: &[&str]) -> Vec<String> {
    let mut urls = Vec::new();
    let primary = primary_url.trim();
    if !primary.is_empty() {
        urls.push(primary.to_string());
    }
    for key in fallback_env_keys {
        if let Ok(value) = std::env::var(key) {
            let value = value.trim();
            if !value.is_empty() && !urls.iter().any(|url| url == value) {
                urls.push(value.to_string());
            }
        }
    }
    urls
}

pub fn build_http_provider(urls: Vec<String>, interval: Duration) -> Result<RpcProvider> {
    let client = RpcHttpClient::new(urls)?;
    Ok(Provider::new(client).interval(interval))
}

impl RpcHttpClient {
    pub fn new(urls: Vec<String>) -> Result<Self> {
        if urls.is_empty() {
            return Err(anyhow!("no RPC urls configured"));
        }
        let clients = urls
            .into_iter()
            .map(|url| build_retry_client(&url))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            inner: Arc::new(RpcHttpClientInner {
                clients,
                index: AtomicUsize::new(0),
            }),
        })
    }
}

fn build_retry_client(url: &str) -> Result<RetryClient<Http>> {
    let http = Http::from_str(url).map_err(|err| anyhow!("invalid RPC url {url}: {err}"))?;
    let client = RetryClientBuilder::default()
        .rate_limit_retries(3)
        .timeout_retries(2)
        .initial_backoff(Duration::from_millis(200))
        .build(http, Box::new(HttpRateLimitRetryPolicy::default()));
    Ok(client)
}

fn is_rate_limited(err: &ProviderError) -> bool {
    if let Some(rpc_err) = err.as_error_response() {
        if rpc_err.code == 429 || rpc_err.code == -32005 {
            return true;
        }
        if rpc_err.code == -32016 && rpc_err.message.to_lowercase().contains("rate limit") {
            return true;
        }
        let msg = rpc_err.message.to_lowercase();
        if msg.contains("rate limit") || msg.contains("rps limit") {
            return true;
        }
    }
    if let ProviderError::HTTPError(err) = err {
        if err
            .status()
            .map(|status| status.as_u16() == 429)
            .unwrap_or(false)
        {
            return true;
        }
    }
    let text = err.to_string().to_lowercase();
    text.contains("rate limit") || text.contains("rps limit") || text.contains("too many requests")
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl JsonRpcClient for RpcHttpClient {
    type Error = ProviderError;

    async fn request<T, R>(&self, method: &str, params: T) -> Result<R, ProviderError>
    where
        T: Debug + Serialize + Send + Sync,
        R: DeserializeOwned + Send,
    {
        let clients = &self.inner.clients;
        let total = clients.len();
        let start = self.inner.index.load(Ordering::Relaxed) % total;

        if std::mem::size_of::<T>() == 0 {
            let _ = params;
            let mut last_err: Option<ProviderError> = None;
            for offset in 0..total {
                let idx = (start + offset) % total;
                match clients[idx].request::<(), R>(method, ()).await {
                    Ok(value) => {
                        self.inner.index.store(idx, Ordering::Relaxed);
                        return Ok(value);
                    }
                    Err(err) => {
                        let err: ProviderError = err.into();
                        if is_rate_limited(&err) && offset + 1 < total {
                            last_err = Some(err);
                            continue;
                        }
                        return Err(err);
                    }
                }
            }
            Err(last_err.unwrap_or_else(|| ProviderError::CustomError("RPC request failed".into())))
        } else {
            let params_value: Value =
                serde_json::to_value(params).map_err(ProviderError::SerdeJson)?;
            let mut last_err: Option<ProviderError> = None;
            for offset in 0..total {
                let idx = (start + offset) % total;
                match clients[idx].request(method, params_value.clone()).await {
                    Ok(value) => {
                        self.inner.index.store(idx, Ordering::Relaxed);
                        return Ok(value);
                    }
                    Err(err) => {
                        let err: ProviderError = err.into();
                        if is_rate_limited(&err) && offset + 1 < total {
                            last_err = Some(err);
                            continue;
                        }
                        last_err = Some(err);
                        break;
                    }
                }
            }
            Err(last_err.unwrap_or_else(|| ProviderError::CustomError("RPC request failed".into())))
        }
    }
}
