use anyhow::{Context, Result};
use futures_util::StreamExt;
use http::HeaderValue;
use redis::AsyncCommands;
use reqwest::Client;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{env, time::Duration};
use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest};

const STREAM_URL: &str = "wss://ws.twitterapi.io/twitter/tweet/websocket";
const REDIS_KEY_PREFIX: &str = "snipe:tweets";
const REDIS_EXPIRE_SECONDS: u64 = 300;

#[derive(Clone)]
struct TelegramSettings {
    token: Option<String>,
    chat: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let api_key = env::var("TWITTERAPI_KEY")
        .or_else(|_| env::var("TWITTER_STREAM_API_KEY"))
        .context("Missing TWITTERAPI_KEY / TWITTER_STREAM_API_KEY")?;

    let telegram = TelegramSettings {
        token: env::var("SNIPE_TELEGRAM_TOKEN")
            .or_else(|_| env::var("apiToken"))
            .ok(),
        chat: env::var("SNIPE_TWEET_CHAT")
            .or_else(|_| env::var("chatID"))
            .ok(),
    };

    let reconnect_base = env::var("STREAM_RECONNECT_BASE")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(5.0);
    let reconnect_max = env::var("STREAM_RECONNECT_MAX")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(300.0);

    let redis_client = init_redis_client().await;
    let mut redis_conn = if let Some(client) = redis_client {
        match client.get_multiplexed_async_connection().await {
            Ok(conn) => Some(conn),
            Err(err) => {
                eprintln!("[redis] failed to create connection: {err:?}");
                None
            }
        }
    } else {
        None
    };

    let http = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("create http client")?;

    let mut retry: u32 = 0;
    loop {
        let mut request = STREAM_URL
            .into_client_request()
            .context("build websocket request")?;
        request.headers_mut().insert(
            "x-api-key",
            HeaderValue::from_str(&api_key).context("invalid api key header")?,
        );

        match connect_async(request).await {
            Ok((mut stream, _)) => {
                println!("[+] Connection established!");
                retry = 0;
                while let Some(message) = stream.next().await {
                    match message {
                        Ok(message) => {
                            if let Some(text) = message.into_text().ok() {
                                handle_message(&text, &mut redis_conn, &telegram, &http).await;
                            }
                        }
                        Err(err) => {
                            eprintln!("[!] Streaming error: {err:?}");
                            break;
                        }
                    }
                }
            }
            Err(err) => {
                eprintln!("[!] Handshake failed: {err:?}");
            }
        }

        retry = retry.saturating_add(1);
        let backoff = (reconnect_base * (2.0_f64.powi(retry.min(6) as i32))).min(reconnect_max);
        println!("[!] Reconnecting in {backoff:.1}s...");
        tokio::time::sleep(Duration::from_secs_f64(backoff)).await;
    }
}

async fn init_redis_client() -> Option<redis::Client> {
    let host = env::var("REDIS_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = env::var("REDIS_PORT").unwrap_or_else(|_| "6379".to_string());
    let password = env::var("REDIS_PASSWORD").ok();
    let port_num = port.parse::<u16>().unwrap_or(6379);

    let redis_url = if let Some(password) = password.as_deref() {
        format!("redis://:{password}@{host}:{port_num}/")
    } else {
        format!("redis://{host}:{port_num}/")
    };

    let client = match redis::Client::open(redis_url) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("[redis] invalid config: {err:?}");
            return None;
        }
    };

    match client.get_multiplexed_async_connection().await {
        Ok(mut conn) => {
            let ping: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut conn).await;
            match ping {
                Ok(_) => Some(client),
                Err(err) => {
                    eprintln!("[redis] ping failed: {err:?}; dedup disabled.");
                    None
                }
            }
        }
        Err(err) => {
            eprintln!("[redis] connection failed: {err:?}; dedup disabled.");
            None
        }
    }
}

async fn handle_message(
    raw: &str,
    redis_conn: &mut Option<redis::aio::MultiplexedConnection>,
    telegram: &TelegramSettings,
    http: &Client,
) {
    let payload = match parse_stream_message(raw) {
        Some(payload) => payload,
        None => return,
    };

    let event_type = payload.get("event_type").and_then(Value::as_str);
    match event_type {
        Some("connected") => {
            println!("[+] Connected to Twitter stream.");
            return;
        }
        Some("ping") => {
            return;
        }
        Some("tweet") => {}
        _ => return,
    }

    let tweets = payload.get("tweets").and_then(Value::as_array);
    let Some(tweets) = tweets else { return };
    for tweet in tweets {
        let Some(text) = get_full_tweet_text(tweet) else {
            continue;
        };
        let author = tweet.get("author").and_then(Value::as_object);
        let username = author
            .and_then(|map| map.get("userName"))
            .and_then(Value::as_str)
            .or_else(|| tweet.get("username").and_then(Value::as_str))
            .unwrap_or("unknown");
        let tweet_id = tweet
            .get("id")
            .and_then(value_to_string)
            .unwrap_or_else(|| "unknown".to_string());
        if record_message(redis_conn, &tweet_id, &text).await {
            continue;
        }
        let created_at = tweet
            .get("createdAt")
            .and_then(value_to_string)
            .or_else(|| payload.get("timestamp").and_then(value_to_string))
            .unwrap_or_else(|| "unknown".to_string());
        println!("-----");
        println!("Account: {username}");
        println!("Tweet ID: {tweet_id}");
        println!("Created at: {created_at}");
        println!("Message: {text}");
        let message = format_telegram_message(username, &tweet_id, &text, &created_at);
        let _ = send_telegram_alert(http, telegram, &message).await;
    }
}

fn parse_stream_message(raw: &str) -> Option<Value> {
    serde_json::from_str(raw).ok()
}

fn format_telegram_message(username: &str, tweet_id: &str, text: &str, created_at: &str) -> String {
    let mut lines = Vec::new();
    lines.push("-----------------------------------------".to_string());
    lines.push(format!("New tweet from @{username}"));
    lines.push(format!("Tweet ID: {tweet_id}"));
    lines.push(format!("Created at: {created_at}"));
    lines.push(String::new());
    lines.push(text.to_string());
    lines.join("\n")
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

async fn send_telegram_alert(http: &Client, telegram: &TelegramSettings, text: &str) -> bool {
    let Some(token) = telegram.token.as_deref() else {
        return false;
    };
    let Some(chat) = telegram.chat.as_deref() else {
        return false;
    };
    if text.is_empty() {
        return false;
    }
    let url = format!("https://api.telegram.org/bot{token}/sendMessage");
    let payload = serde_json::json!({
        "chat_id": chat,
        "text": text,
    });
    match http.post(url).json(&payload).send().await {
        Ok(response) => {
            if response.status().is_success() {
                response
                    .json::<serde_json::Value>()
                    .await
                    .ok()
                    .and_then(|value| value.get("ok").and_then(Value::as_bool))
                    .unwrap_or(false)
            } else {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                let body_snippet: String = body.chars().take(300).collect();
                eprintln!("[telegram] failed: status={status} body={body_snippet}");
                false
            }
        }
        Err(err) => {
            eprintln!("[telegram] request failed: {err:?}");
            false
        }
    }
}

async fn record_message(
    redis_conn: &mut Option<redis::aio::MultiplexedConnection>,
    tweet_id: &str,
    message: &str,
) -> bool {
    let Some(conn) = redis_conn.as_mut() else {
        return false;
    };
    if message.is_empty() {
        return false;
    }
    let payload = serde_json::json!({
        "id": tweet_id,
        "text": message,
    })
    .to_string();
    let key = format!("{REDIS_KEY_PREFIX}:{}", hash_message(message));
    match conn.exists(&key).await {
        Ok(true) => return true,
        Ok(false) => {}
        Err(err) => {
            eprintln!("[redis] exists check failed: {err:?}; skipping dedup.");
            return false;
        }
    }
    if let Err(err) = conn
        .set_ex::<_, _, ()>(&key, payload, REDIS_EXPIRE_SECONDS)
        .await
    {
        eprintln!("[redis] write failed: {err:?}; skipping dedup.");
    }
    false
}

fn hash_message(message: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(message.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn get_full_tweet_text(tweet: &Value) -> Option<String> {
    let mut candidates = Vec::new();
    collect_text_candidates(tweet, &mut candidates);
    candidates.into_iter().max_by_key(|value| value.len())
}

fn collect_text_candidates(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::String(text) => {
            let clean = text.trim();
            if !clean.is_empty() {
                out.push(clean.to_string());
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_text_candidates(item, out);
            }
        }
        Value::Object(map) => {
            for key in ["full_text", "text", "message"] {
                if let Some(value) = map.get(key) {
                    collect_text_candidates(value, out);
                }
            }
            for key in [
                "extended_tweet",
                "data",
                "tweet",
                "message",
                "content",
                "payload",
            ] {
                if let Some(value) = map.get(key) {
                    collect_text_candidates(value, out);
                }
            }
        }
        _ => {}
    }
}
