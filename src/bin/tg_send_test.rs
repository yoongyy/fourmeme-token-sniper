use anyhow::{Context, Result};
use sniper_fourmeme::telegram_trade::{TelegramTradeClient, TelegramTradeSettings};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let mut args = env::args().skip(1);
    let mut use_tweet = false;
    let mut message_parts = Vec::new();

    while let Some(arg) = args.next() {
        if arg == "--tweet" {
            use_tweet = true;
            continue;
        }
        message_parts.push(arg);
        message_parts.extend(args);
        break;
    }

    if message_parts.is_empty() {
        return Err(anyhow::anyhow!(
            "Usage: cargo run --bin tg_send_test -- [--tweet] <message>"
        ));
    }

    let message = message_parts.join(" ");
    let settings = if use_tweet {
        TelegramTradeSettings::from_env_tweet()
    } else {
        TelegramTradeSettings::from_env()
    };
    let client = TelegramTradeClient::connect(&settings)
        .await
        .context("failed to connect telegram trade client")?;
    client
        .send_message(&message)
        .await
        .context("failed to send telegram message")?;

    println!("✅ Telegram message sent");
    Ok(())
}
