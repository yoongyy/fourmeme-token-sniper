use crate::config::TelegramSettings;
use anyhow::{Context, Result, anyhow};
use futures_util::StreamExt;
use reqwest::Client;
use serde_json::json;
use solana_client::{nonblocking::pubsub_client::PubsubClient, rpc_config::RpcAccountInfoConfig};
use solana_commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_native_token::LAMPORTS_PER_SOL;
use solana_pubkey::Pubkey;
use std::{env, str::FromStr};

const ALERT_THRESHOLD_SOL: u64 = 50;
const ALERT_THRESHOLD_LAMPORTS: u64 = ALERT_THRESHOLD_SOL * LAMPORTS_PER_SOL;

pub async fn trace_wallet() -> Result<()> {
    dotenv::dotenv().ok();
    let cfg = SolTraceConfig::from_env()?;
    let http_client = Client::new();
    let subscription_config = Some(RpcAccountInfoConfig {
        encoding: None,
        data_slice: None,
        commitment: Some(CommitmentConfig {
            commitment: CommitmentLevel::Confirmed,
        }),
        min_context_slot: None,
    });
    let client = PubsubClient::new(&cfg.ws_url).await?;
    println!(
        "[trace-wallet] Listening for SOL transfers on {} (threshold {} SOL)",
        cfg.wallet_label, ALERT_THRESHOLD_SOL
    );
    let (mut updates, _unsubscribe) = client
        .account_subscribe(&cfg.wallet, subscription_config)
        .await?;
    let mut last_balance: Option<u64> = None;
    while let Some(update) = updates.next().await {
        let lamports = update.value.lamports;
        if let Some(previous) = last_balance {
            let delta = lamports as i128 - previous as i128;
            let abs_delta = delta.abs() as u64;
            if abs_delta > ALERT_THRESHOLD_LAMPORTS {
                let direction = if delta >= 0 { "Received" } else { "Sent" };
                let slot = update.context.slot;
                let amount_sol = abs_delta as f64 / LAMPORTS_PER_SOL as f64;
                let balance_sol = lamports as f64 / LAMPORTS_PER_SOL as f64;
                println!(
                    "[trace-wallet] {} {:.4} SOL (slot {})",
                    direction, amount_sol, slot
                );
                let explorer_url = format!("https://solscan.io/account/{}", cfg.wallet_label);
                let message = format!(
                    "<b>SOL Transfer Alert</b>\nWallet: <code>{wallet}</code>\nAction: {direction}\nAmount: {amount:.4} SOL\nNew Balance: {balance:.4} SOL\nSlot: {slot}\n<a href=\"{url}\">View on Solscan</a>",
                    wallet = escape_html(&cfg.wallet_label),
                    direction = direction,
                    amount = amount_sol,
                    balance = balance_sol,
                    slot = slot,
                    url = explorer_url
                );
                if let Err(err) =
                    send_telegram_message(&http_client, &cfg.telegram_token, &cfg.chat_id, &message)
                        .await
                {
                    eprintln!("[trace-wallet] failed to send Telegram alert: {err:?}");
                }
            }
        } else {
            let balance_sol = lamports as f64 / LAMPORTS_PER_SOL as f64;
            println!(
                "[trace-wallet] Tracking wallet {} (initial balance {:.4} SOL)",
                cfg.wallet_label, balance_sol
            );
        }
        last_balance = Some(lamports);
    }
    Err(anyhow!("SOL websocket subscription ended unexpectedly"))
}

struct SolTraceConfig {
    ws_url: String,
    wallet: Pubkey,
    wallet_label: String,
    telegram_token: String,
    chat_id: String,
}

impl SolTraceConfig {
    fn from_env() -> Result<Self> {
        let ws_url =
            read_env_value("SOL_WSS_URL").context("SOL_WSS_URL is required for trace_wallet")?;
        let wallet_label = read_env_value("TRACE_WALLET")
            .or_else(|| read_env_value("trace_wallet"))
            .context("TRACE_WALLET is not configured")?;
        let wallet = Pubkey::from_str(&wallet_label)
            .context("trace_wallet must be a valid Solana address")?;
        let telegram = TelegramSettings::from_env();
        let telegram_token = telegram
            .token()
            .map(|value| value.to_string())
            .ok_or_else(|| anyhow!("SNIPE_TELEGRAM_TOKEN or apiToken is required for alerts"))?;
        let chat_id = read_env_value("chatID_crypto")
            .or_else(|| telegram.watchlist_chat().map(|value| value.to_string()))
            .ok_or_else(|| anyhow!("chatID_crypto (or SNIPE_TELEGRAM_CHAT) is missing"))?;
        Ok(Self {
            ws_url,
            wallet,
            wallet_label,
            telegram_token,
            chat_id,
        })
    }
}

async fn send_telegram_message(
    client: &Client,
    token: &str,
    chat_id: &str,
    text: &str,
) -> Result<()> {
    let url = format!("https://api.telegram.org/bot{token}/sendMessage");
    let payload = json!({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": true,
    });
    client
        .post(url)
        .json(&payload)
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

fn read_env_value(key: &str) -> Option<String> {
    env::var(key).ok().and_then(|value| {
        let cleaned = clean_env_value(value);
        if cleaned.is_empty() {
            None
        } else {
            Some(cleaned)
        }
    })
}

fn clean_env_value(value: String) -> String {
    let trimmed = value.trim();
    if trimmed.len() >= 2 {
        let bytes = trimmed.as_bytes();
        let first = bytes[0];
        let last = bytes[trimmed.len() - 1];
        if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
            return trimmed[1..trimmed.len() - 1].trim().to_string();
        }
    }
    trimmed.to_string()
}

fn escape_html(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}
