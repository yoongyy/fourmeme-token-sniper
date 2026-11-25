use crate::{
    config::{GeminiSettings, RedisSettings},
    redis_utils::build_redis_client,
    storage::MongoLogger,
};
use anyhow::{Result, anyhow};
use dashmap::DashMap;
use ethers::types::Address;
use redis::{AsyncCommands, Client as RedisClient, aio::MultiplexedConnection};
use reqwest::Client;
use serde::Deserialize;
use std::{env, sync::Arc, time::Duration};
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct TweetService {
    redis: Option<RedisClient>,
    http: Client,
    gemini: GeminiSettings,
    mongo: Option<Arc<MongoLogger>>,
    cache: Arc<DashMap<String, bool>>,
    semaphore: Arc<Semaphore>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct TweetPayload {
    id: Option<String>,
    text: Option<String>,
    translation: Option<String>,
    translation_done: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct TweetRecord {
    pub id: Option<String>,
    pub text: String,
    pub translation: Option<String>,
}

impl TweetService {
    pub fn new(
        redis_settings: &RedisSettings,
        gemini: GeminiSettings,
        mongo: Option<Arc<MongoLogger>>,
    ) -> Option<Self> {
        if !redis_settings.enabled {
            return None;
        }
        let redis = build_redis_client(redis_settings).ok();
        println!(
            "[tweet] Gemini enabled={} always_query={}",
            gemini.is_enabled(),
            gemini.always_query()
        );
        Some(Self {
            redis,
            http: Client::builder()
                .timeout(Duration::from_secs(15))
                .build()
                .expect("http client"),
            gemini,
            mongo,
            cache: Arc::new(DashMap::new()),
            semaphore: Arc::new(Semaphore::new(4)),
        })
    }

    pub fn new_for_gemini(gemini: GeminiSettings, mongo: Option<Arc<MongoLogger>>) -> Self {
        println!(
            "[tweet] Gemini enabled={} always_query={}",
            gemini.is_enabled(),
            gemini.always_query()
        );
        Self {
            redis: None,
            http: Client::builder()
                .timeout(Duration::from_secs(15))
                .build()
                .expect("http client"),
            gemini,
            mongo,
            cache: Arc::new(DashMap::new()),
            semaphore: Arc::new(Semaphore::new(4)),
        }
    }

    pub async fn related_tweet(
        &self,
        name: &str,
        symbol: &str,
        token_address: Address,
    ) -> Result<Option<TweetRecord>> {
        let Some(redis_client) = self.redis.as_ref() else {
            return Ok(None);
        };
        let mut conn = match redis_client.get_multiplexed_async_connection().await {
            Ok(conn) => conn,
            Err(err) => {
                eprintln!("[tweet] redis connection failed: {err:?}");
                return Ok(None);
            }
        };
        let records = self.fetch_recent_tweets(&mut conn, 40).await?;
        if records.is_empty() {
            return Ok(None);
        }

        let gemini_enabled = self.gemini.is_enabled();
        if self.gemini.always_query() && !gemini_enabled {
            eprintln!("[tweet] GEMINI_ALWAYS_QUERY=true but GEMINI_API_KEY missing");
            return Ok(None);
        }

        for record in records {
            let verdict = self
                .match_text_with_policy(
                    name,
                    symbol,
                    token_address,
                    &record.text,
                    record.translation.as_deref(),
                )
                .await?;
            if verdict {
                return Ok(Some(record));
            }
        }
        Ok(None)
    }

    pub async fn gemini_match_text(
        &self,
        name: &str,
        symbol: &str,
        token_address: Address,
        tweet_text: &str,
        translation: Option<&str>,
    ) -> Result<bool> {
        let record = TweetRecord {
            id: None,
            text: tweet_text.to_string(),
            translation: translation.map(|value| value.to_string()),
        };
        self.match_with_gemini(&record, name, symbol, token_address)
            .await
    }

    pub async fn match_text_with_policy(
        &self,
        name: &str,
        symbol: &str,
        token_address: Address,
        tweet_text: &str,
        translation: Option<&str>,
    ) -> Result<bool> {
        let record = TweetRecord {
            id: None,
            text: tweet_text.to_string(),
            translation: translation.map(|value| value.to_string()),
        };

        if tweet_match_disabled() {
            if !self.gemini.is_enabled() {
                return Ok(true);
            }
            return self
                .match_with_gemini(&record, name, symbol, token_address)
                .await;
        }

        // --- Start of new logic ---
        // 1. Prioritize fast, local matching.
        if self.matches_substring(&record, name, symbol) {
            return Ok(true);
        }
        if self.matches_basic(&record, name, symbol) {
            return Ok(true);
        }

        // 2. If local matching fails, and we must query gemini, do so.
        // This is now a fallback rather than the primary strategy.
        if self.gemini.is_enabled() && self.gemini.always_query() {
            return self
                .match_with_gemini(&record, name, symbol, token_address)
                .await;
        }

        // 3. Default to false if no match is found.
        Ok(false)
        // --- End of new logic ---
    }

    pub fn local_match_text(
        &self,
        name: &str,
        symbol: &str,
        tweet_text: &str,
        translation: Option<&str>,
    ) -> bool {
        let record = TweetRecord {
            id: None,
            text: tweet_text.to_string(),
            translation: translation.map(|value| value.to_string()),
        };
        self.matches_substring(&record, name, symbol)
    }

    pub fn local_match_basic(
        &self,
        name: &str,
        symbol: &str,
        tweet_text: &str,
        translation: Option<&str>,
    ) -> bool {
        let record = TweetRecord {
            id: None,
            text: tweet_text.to_string(),
            translation: translation.map(|value| value.to_string()),
        };
        self.matches_basic(&record, name, symbol)
    }

    async fn fetch_recent_tweets(
        &self,
        conn: &mut MultiplexedConnection,
        limit: usize,
    ) -> Result<Vec<TweetRecord>> {
        let mut cursor: u64 = 0;
        let mut collected = Vec::new();
        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg("snipe:tweets:*")
                .arg("COUNT")
                .arg(50)
                .query_async(conn)
                .await?;

            for key in keys {
                if collected.len() >= limit {
                    return Ok(collected);
                }
                if let Ok(value) = conn.get::<_, String>(key.clone()).await {
                    if let Some(record) = parse_tweet(&value) {
                        collected.push(record);
                    }
                }
            }

            if next_cursor == 0 {
                break;
            }
            cursor = next_cursor;
        }
        Ok(collected)
    }

    fn matches_basic(&self, record: &TweetRecord, name: &str, symbol: &str) -> bool {
        let text = record.text.to_lowercase();
        let translation = record.translation.as_deref().unwrap_or("").to_lowercase();
        let mut normalized = normalize_token(name);
        if normalized.is_empty() {
            normalized = normalize_token(symbol);
        }
        if !normalized.is_empty()
            && (text.contains(&normalized)
                || (!translation.is_empty() && translation.contains(&normalized)))
        {
            return true;
        }
        let tokens = tokenize(name);
        let translation_tokens = tokenize(symbol);
        tokens.iter().any(|tok| text.contains(tok))
            || translation_tokens.iter().any(|tok| text.contains(tok))
            || (!translation.is_empty()
                && (tokens.iter().any(|tok| translation.contains(tok))
                    || translation_tokens
                        .iter()
                        .any(|tok| translation.contains(tok))))
    }

    fn matches_substring(&self, record: &TweetRecord, name: &str, symbol: &str) -> bool {
        let text = record.text.to_lowercase();
        let translation = record.translation.as_deref().unwrap_or("").to_lowercase();
        let name = name.to_lowercase();
        let symbol = symbol.to_lowercase();
        (!name.is_empty()
            && (text.contains(&name) || (!translation.is_empty() && translation.contains(&name))))
            || (!symbol.is_empty()
                && (text.contains(&symbol)
                    || (!translation.is_empty() && translation.contains(&symbol))))
    }

    pub fn get_gemini_prompt(&self, name: &str, symbol: &str, record: &TweetRecord) -> String {
        let keywords = tokenize(name).join(", ");
        format!(
            "You are checking if a tweet is about a token.\n\
Token name: \"{name}\".\nToken symbol: \"{symbol}\".\n\
Steps:\n\
(1) Translate the tweet to English if needed.\n\
(2) The core keywords of the token name are: \"{keywords}\".\n\
(3) If the tweet text OR its translation contains the Token Symbol OR the core keywords of the Token Name (even if they are separated by other words), respond 'yes'.\n\
Otherwise respond 'no'. Only reply with 'yes' or 'no'.\n\
Tweet: {tweet}\nTranslation: {translation}",
            tweet = record.text,
            translation = record.translation.as_deref().unwrap_or("N/A")
        )
    }

    async fn match_with_gemini(
        &self,
        record: &TweetRecord,
        name: &str,
        symbol: &str,
        token_address: Address,
    ) -> Result<bool> {
        let api_key = match self.gemini.api_key() {
            Some(key) => key,
            None => return Ok(false),
        };
        let permit = self.semaphore.acquire().await?;
        let cache_key = format!(
            "{}::{}::{}::{}",
            name,
            symbol,
            record.text,
            record.translation.as_deref().unwrap_or("")
        );
        if !self.gemini.always_query() {
            if let Some(entry) = self.cache.get(&cache_key) {
                drop(permit);
                return Ok(*entry);
            }
        }
        let prompt = self.get_gemini_prompt(name, symbol, record);
        let url = format!(
            "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent",
            self.gemini.model
        );
        let body = serde_json::json!({
            "contents": [{
                "parts": [{
                    "text": prompt
                }]
            }],
            "generationConfig": {
                "temperature": 0.0,
                "maxOutputTokens": 64
            }
        });
        let response = match self
            .http
            .post(&url)
            .query(&[("key", api_key)])
            .json(&body)
            .send()
            .await
        {
            Ok(response) => response,
            Err(err) => {
                if let Some(mongo) = self.mongo.as_ref() {
                    let error_payload = format!("ERROR: {err:?}");
                    if let Err(log_err) = mongo
                        .log_ai_response(
                            name,
                            symbol,
                            token_address,
                            record.id.as_deref(),
                            &record.text,
                            &error_payload,
                        )
                        .await
                    {
                        eprintln!("[tweet] failed to log gemini error response: {log_err:?}");
                    }
                }
                eprintln!("[tweet] gemini request failed: {err:?}");
                return Err(anyhow!("gemini request failed: {err:?}"));
            }
        };
        let status = response.status();
        let body_text = response.text().await.unwrap_or_default();
        if let Some(mongo) = self.mongo.as_ref() {
            if let Err(err) = mongo
                .log_ai_response(
                    name,
                    symbol,
                    token_address,
                    record.id.as_deref(),
                    &record.text,
                    &body_text,
                )
                .await
            {
                eprintln!("[tweet] failed to log gemini response: {err:?}");
            }
        }
        let verdict = if status.is_success() {
            let value: serde_json::Value = serde_json::from_str(&body_text).unwrap_or_default();
            value["candidates"]
                .as_array()
                .and_then(|candidates| candidates.first())
                .and_then(|candidate| candidate["content"]["parts"].as_array())
                .and_then(|parts| parts.first())
                .and_then(|part| part["text"].as_str())
                .map(|text| text.trim().to_ascii_lowercase().starts_with('y'))
                .unwrap_or(false)
        } else {
            let body_snippet: String = body_text.chars().take(300).collect();
            eprintln!(
                "[tweet] gemini request failed: status={} body={}",
                status, body_snippet
            );
            false
        };
        self.cache.insert(cache_key, verdict);
        drop(permit);
        Ok(verdict)
    }
}

fn tweet_match_disabled() -> bool {
    match env::var("TWEET_MATCH_DISABLE").ok().as_deref() {
        Some(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes"
        ),
        None => false,
    }
}

fn parse_tweet(value: &str) -> Option<TweetRecord> {
    if value.trim_start().starts_with('{') {
        if let Ok(payload) = serde_json::from_str::<TweetPayload>(value) {
            let TweetPayload {
                id,
                text,
                mut translation,
                translation_done,
            } = payload;
            if matches!(translation_done, Some(false)) {
                translation = None;
            }
            let text = text.or_else(|| translation.clone())?;
            return Some(TweetRecord {
                id,
                text,
                translation,
            });
        }
    }
    if value.is_empty() {
        None
    } else {
        Some(TweetRecord {
            id: None,
            text: value.to_string(),
            translation: None,
        })
    }
}

fn normalize_token(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_alphanumeric() { ch } else { ' ' })
        .collect::<String>()
        .to_lowercase()
}

fn tokenize(value: &str) -> Vec<String> {
    normalize_token(value)
        .split_whitespace()
        .map(|s| s.to_string())
        .collect()
}
