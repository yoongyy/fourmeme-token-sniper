use crate::config::RedisSettings;
use anyhow::{Context, Result};
use ethers::types::Address;
use redis::Client as RedisClient;
use std::env;

const SMART_FOLLOW_BUY_PENDING_KEY_PREFIX: &str = "smart_follow:buy:pending";
const SMART_FOLLOW_BUY_PENDING_TTL_SECS: usize = 60;
const LIVE_TRADE_STREAM_KEY_DEFAULT: &str = "sniper:live_trade";

pub fn live_trade_stream_key() -> String {
    env::var("LIVE_TRADE_STREAM_KEY").unwrap_or_else(|_| LIVE_TRADE_STREAM_KEY_DEFAULT.into())
}

pub async fn enqueue_live_trade(
    redis: &RedisClient,
    fields: Vec<(&str, String)>,
) -> Result<()> {
    let mut conn = redis.get_multiplexed_async_connection().await?;
    let key = live_trade_stream_key();
    let mut cmd = redis::cmd("XADD");
    cmd.arg(key).arg("*");
    for (field, value) in fields {
        cmd.arg(field).arg(value);
    }
    let _: String = cmd
        .query_async(&mut conn)
        .await
        .context("failed to enqueue live trade")?;
    Ok(())
}

pub fn build_redis_client(settings: &RedisSettings) -> Result<RedisClient> {
    if !settings.enabled {
        return Err(anyhow::anyhow!("redis disabled"));
    }
    let url = if let Some(password) = &settings.password {
        format!("redis://:{}@{}:{}/", password, settings.host, settings.port)
    } else {
        format!("redis://{}:{}/", settings.host, settings.port)
    };
    RedisClient::open(url).context("failed to create redis client")
}

pub async fn set_smart_buy_pending(
    redis: &RedisClient,
    wallet: Address,
    token: Address,
) -> Result<bool> {
    let mut conn = redis.get_multiplexed_async_connection().await?;
    let key = format!("{SMART_FOLLOW_BUY_PENDING_KEY_PREFIX}:{wallet:#x}:{token:#x}");
    let result: Option<String> = redis::cmd("SET")
        .arg(&key)
        .arg("1")
        .arg("NX")
        .arg("EX")
        .arg(SMART_FOLLOW_BUY_PENDING_TTL_SECS)
        .query_async(&mut conn)
        .await
        .context("failed to set smart buy pending key")?;
    Ok(result.is_some())
}

pub async fn clear_smart_buy_pending(
    redis: &RedisClient,
    wallet: Address,
    token: Address,
) -> Result<()> {
    let mut conn = redis.get_multiplexed_async_connection().await?;
    let key = format!("{SMART_FOLLOW_BUY_PENDING_KEY_PREFIX}:{wallet:#x}:{token:#x}");
    let _: () = redis::cmd("DEL")
        .arg(&key)
        .query_async(&mut conn)
        .await
        .context("failed to clear smart buy pending key")?;
    Ok(())
}
