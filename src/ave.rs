use crate::config::AveSettings;
use anyhow::{Context, Result, anyhow};
use base64::Engine as _;
use base64::engine::general_purpose::{
    STANDARD as BASE64, STANDARD_NO_PAD as BASE64_NOPAD, URL_SAFE as BASE64_URL,
    URL_SAFE_NO_PAD as BASE64_URL_NOPAD,
};
use chrono::{SecondsFormat, Utc};
use ethers::utils::hex;
use hmac::{Hmac, Mac};
use reqwest::{Client, header};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::Sha256;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::time::sleep;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug)]
pub struct AveClient {
    settings: AveSettings,
    http: Client,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainWalletSwapPayload {
    pub chain: String,
    pub creator_address: String,
    pub in_token_address: String,
    pub out_token_address: String,
    pub in_amount: String,
    pub swap_type: String,
    pub slippage: String,
    pub use_mev: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra_gas: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_gas: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_slippage: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct ChainWalletTx {
    #[serde(rename = "requestTxId", alias = "request_tx_id", alias = "requestId")]
    pub request_tx_id: String,
    #[serde(
        rename = "toAddress",
        alias = "to",
        alias = "to_address",
        alias = "toAddr"
    )]
    pub to_address: Option<String>,
    #[serde(
        rename = "txContent",
        alias = "txContext",
        alias = "data",
        alias = "tx_data",
        alias = "txData"
    )]
    pub tx_content: String,
    #[serde(rename = "gasLimit", alias = "gas", alias = "gas_limit")]
    pub gas_limit: String,
    #[serde(rename = "inAmount", alias = "amount", alias = "inputAmount")]
    pub in_amount: String,
    #[serde(rename = "outTokenAddress", alias = "outToken", alias = "out_token")]
    pub out_token_address: Option<String>,
    #[serde(rename = "minReturn", alias = "minOut", alias = "min_out")]
    pub min_return: Option<String>,
    #[serde(rename = "estimateOut", alias = "estimatedOut")]
    pub estimate_out: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChainWalletCreateResponse {
    status: i64,
    msg: Option<String>,
    data: Option<ChainWalletTx>,
}

#[derive(Debug, Deserialize)]
struct ChainWalletSendData {
    hash: String,
}

#[derive(Debug, Deserialize)]
struct ChainWalletSendResponse {
    status: i64,
    msg: Option<String>,
    data: Option<ChainWalletSendData>,
}

impl AveClient {
    pub fn new(settings: AveSettings) -> Self {
        let http = Client::builder().https_only(true).build().unwrap();
        Self { settings, http }
    }

    pub fn settings(&self) -> &AveSettings {
        &self.settings
    }

    pub async fn create_chain_wallet_tx(
        &self,
        payload: &ChainWalletSwapPayload,
    ) -> Result<ChainWalletTx> {
        let request_path = "/v1/thirdParty/chainWallet/createEvmTx";
        let body_value = canonicalize_value(&serde_json::to_value(payload)?);
        let body_str = serde_json::to_string(&body_value)?;
        let headers = self.build_headers(request_path, &body_str)?;
        let url = format!(
            "{}{}",
            self.settings.base_url.trim_end_matches('/'),
            request_path
        );
        let (_status, _retry_after, body) = self
            .post_with_retry(&url, headers, body_str, "chain wallet request")
            .await?;
        let parsed: ChainWalletCreateResponse = serde_json::from_str(&body)
            .map_err(|err| anyhow!("failed to parse AVE create response: {err}; body={body}"))?;
        if parsed.status != 0 && parsed.status != 200 {
            return Err(anyhow!(
                "AVE chain wallet request failed: {}",
                parsed.msg.unwrap_or_else(|| "unknown".into())
            ));
        }
        parsed
            .data
            .context("AVE chain wallet response missing data")
    }

    pub async fn send_signed_chain_wallet_tx(
        &self,
        chain: &str,
        request_tx_id: &str,
        signed_tx: &str,
        use_mev: bool,
    ) -> Result<String> {
        let request_path = "/v1/thirdParty/chainWallet/sendSignedEvmTx";
        let payload = json!({
            "chain": chain,
            "requestTxId": request_tx_id,
            "signedTx": signed_tx,
            "useMev": use_mev,
        });
        let body_value = canonicalize_value(&payload);
        let body_str = serde_json::to_string(&body_value)?;
        let headers = self.build_headers(request_path, &body_str)?;
        let url = format!(
            "{}{}",
            self.settings.base_url.trim_end_matches('/'),
            request_path
        );
        let (_status, _retry_after, body) = self
            .post_with_retry(&url, headers, body_str, "send request")
            .await?;
        let parsed: ChainWalletSendResponse = serde_json::from_str(&body)
            .map_err(|err| anyhow!("failed to parse AVE send response: {err}; body={body}"))?;
        if parsed.status != 0 && parsed.status != 200 {
            return Err(anyhow!(
                "AVE send request failed: {}",
                parsed.msg.unwrap_or_else(|| "unknown".into())
            ));
        }
        let data = parsed.data.context("AVE send response missing data")?;
        Ok(data.hash)
    }

    pub async fn send_signed_chain_wallet_tx_bytes(
        &self,
        chain: &str,
        request_tx_id: &str,
        signed_bytes: &[u8],
        use_mev: bool,
    ) -> Result<String> {
        let mut formats = vec![
            self.settings.signed_tx_format.clone(),
            "base64".into(),
            "base64nopad".into(),
            "base64url".into(),
            "base64urlpad".into(),
            "base64hex".into(),
            "base64hex0x".into(),
            "base64hexupper".into(),
            "base64hex0xupper".into(),
            "hex".into(),
        ];
        formats.dedup();

        let mut last_err: Option<anyhow::Error> = None;
        for format in formats {
            let signed_tx = encode_signed_tx(signed_bytes, &format);
            match self
                .send_signed_chain_wallet_tx(chain, request_tx_id, &signed_tx, use_mev)
                .await
            {
                Ok(hash) => return Ok(hash),
                Err(err) => {
                    let err_msg = err.to_string().to_lowercase();
                    if err_msg.contains("failed to parse signed tx") {
                        last_err = Some(err);
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("failed to send signed tx")))
    }

    fn build_headers(&self, request_path: &str, body: &str) -> Result<header::HeaderMap> {
        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/json"),
        );
        headers.insert(
            header::HeaderName::from_static("ave-access-key"),
            header::HeaderValue::from_str(&self.settings.api_key).context("invalid AVE API key")?,
        );
        if let Some(secret) = &self.settings.api_secret {
            let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
            let message = format!("{}POST{}{}", timestamp, request_path, body);
            let mut mac =
                HmacSha256::new_from_slice(secret.as_bytes()).context("failed to init AVE HMAC")?;
            mac.update(message.as_bytes());
            let signature = BASE64.encode(mac.finalize().into_bytes());
            headers.insert(
                header::HeaderName::from_static("ave-access-timestamp"),
                header::HeaderValue::from_str(&timestamp).context("invalid AVE timestamp")?,
            );
            headers.insert(
                header::HeaderName::from_static("ave-access-sign"),
                header::HeaderValue::from_str(&signature).context("invalid AVE signature")?,
            );
        }
        Ok(headers)
    }

    async fn post_with_retry(
        &self,
        url: &str,
        headers: header::HeaderMap,
        body: String,
        label: &str,
    ) -> Result<(reqwest::StatusCode, Option<String>, String)> {
        const MAX_RETRIES: usize = 4;
        let mut attempt = 0usize;
        loop {
            let response = self
                .http
                .post(url)
                .headers(headers.clone())
                .body(body.clone())
                .send()
                .await
                .with_context(|| format!("failed to request AVE {label}"))?;
            let status = response.status();
            let retry_after = response
                .headers()
                .get(header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .map(|value| value.to_string());
            let body_text = response
                .text()
                .await
                .with_context(|| format!("failed to read AVE {label} response"))?;
            if status.as_u16() != 429 {
                return Ok((status, retry_after, body_text));
            }
            if attempt + 1 >= MAX_RETRIES {
                return Err(anyhow!(
                    "AVE {} rate limited (429): {}{}",
                    label,
                    body_text.trim(),
                    retry_after
                        .as_deref()
                        .map(|value| format!(" (retry-after {value}s)"))
                        .unwrap_or_default()
                ));
            }
            let sleep_for = retry_after
                .as_deref()
                .and_then(|value| value.parse::<u64>().ok())
                .map(Duration::from_secs)
                .unwrap_or_else(|| {
                    let base = 500u64.saturating_mul(1u64 << attempt.min(6));
                    Duration::from_millis(base)
                });
            attempt += 1;
            sleep(sleep_for).await;
        }
    }
}

fn encode_signed_tx(signed_bytes: &[u8], format: &str) -> String {
    match format {
        "hex" => format!("0x{}", hex::encode(signed_bytes)),
        "base64hex" => {
            let hex_str = hex::encode(signed_bytes);
            BASE64.encode(hex_str.as_bytes())
        }
        "base64hex0x" => {
            let hex_str = format!("0x{}", hex::encode(signed_bytes));
            BASE64.encode(hex_str.as_bytes())
        }
        "base64hexupper" => {
            let hex_str = hex::encode(signed_bytes).to_ascii_uppercase();
            BASE64.encode(hex_str.as_bytes())
        }
        "base64hex0xupper" => {
            let hex_str = format!("0x{}", hex::encode(signed_bytes)).to_ascii_uppercase();
            BASE64.encode(hex_str.as_bytes())
        }
        "base64url" => BASE64_URL_NOPAD.encode(signed_bytes),
        "base64urlpad" => BASE64_URL.encode(signed_bytes),
        "base64nopad" => BASE64_NOPAD.encode(signed_bytes),
        _ => BASE64.encode(signed_bytes),
    }
}

fn canonicalize_value(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(canonicalize_value).collect()),
        Value::Object(map) => {
            let mut sorted = BTreeMap::new();
            for (key, value) in map {
                sorted.insert(key.clone(), canonicalize_value(value));
            }
            let mut ordered = Map::new();
            for (key, value) in sorted {
                ordered.insert(key, value);
            }
            Value::Object(ordered)
        }
        other => other.clone(),
    }
}
