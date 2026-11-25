use anyhow::{Context, Result, anyhow};
use ethers::types::Address;
use futures_util::TryStreamExt;
use mongodb::{Client, bson::Document};
use sniper_fourmeme::{
    config::{GeminiSettings, MongoSettings},
    tweets::TweetService,
};
use std::{env, str::FromStr};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let args: Vec<String> = env::args().skip(1).collect();
    let usage = "Usage:\n  cargo run --bin gemini_test -- <token_name> <token_symbol> <tweet_text> [--translation <text>]\n  cargo run --bin gemini_test -- --ai-logs";
    let gemini = GeminiSettings::from_env();
    if !gemini.is_enabled() {
        return Err(anyhow!("GEMINI_API_KEY is missing"));
    }
    if args.len() == 1 && args[0] == "--ai-logs" {
        let service = TweetService::new_for_gemini(gemini, None);
        run_ai_logs(&service).await?;
        return Ok(());
    }

    let mut args_iter = args.into_iter();
    let name = args_iter.next().context(usage)?;
    let symbol = args_iter.next().context(usage)?;
    let remaining: Vec<String> = args_iter.collect();
    if remaining.is_empty() {
        return Err(anyhow!(usage));
    }
    let (tweet_text, translation) =
        if let Some(pos) = remaining.iter().position(|arg| arg == "--translation") {
            if pos == 0 || pos + 1 >= remaining.len() {
                return Err(anyhow!(usage));
            }
            let tweet_text = remaining[..pos].join(" ");
            let translation = remaining[pos + 1..].join(" ");
            (tweet_text, Some(translation))
        } else {
            (remaining.join(" "), None)
        };

    let service = TweetService::new_for_gemini(gemini, None);
    run_single(
        &service,
        &name,
        &symbol,
        &tweet_text,
        translation.as_deref(),
        Address::zero(),
        true,
    )
    .await?;
    Ok(())
}

async fn run_ai_logs(service: &TweetService) -> Result<()> {
    let mongo_settings = MongoSettings::from_env();
    let uri = mongo_settings
        .uri()
        .context("MONGO_URI missing; cannot fetch ai_logs")?;
    let client = Client::with_uri_str(uri).await?;
    let db = client.database(&mongo_settings.db_name);
    let ai_logs: mongodb::Collection<Document> = db.collection(&mongo_settings.ai_logs_collection);
    let mut cursor = ai_logs.find(None, None).await?;
    println!("Running match tests for all ai_logs...\n");
    while let Some(doc) = cursor.try_next().await? {
        let token_name = doc.get_str("name").unwrap_or_default().to_string();
        let token_symbol = doc.get_str("symbol").unwrap_or_default().to_string();
        let tweet_text = doc.get_str("tweet_text").unwrap_or_default().to_string();
        let translation = doc.get_str("translation").ok().map(|s| s.to_string());
        let token_address = doc
            .get_str("token")
            .ok()
            .and_then(|value| Address::from_str(value).ok())
            .unwrap_or_else(Address::zero);
        run_single(
            service,
            &token_name,
            &token_symbol,
            &tweet_text,
            translation.as_deref(),
            token_address,
            false,
        )
        .await?;
        println!();
    }
    Ok(())
}

async fn run_single(
    service: &TweetService,
    name: &str,
    symbol: &str,
    tweet_text: &str,
    translation: Option<&str>,
    token_address: Address,
    show_prompt: bool,
) -> Result<()> {
    let record = sniper_fourmeme::tweets::TweetRecord {
        id: None,
        text: tweet_text.to_string(),
        translation: translation.map(|value| value.to_string()),
    };
    if show_prompt {
        let prompt_preview = service.get_gemini_prompt(name, symbol, &record);
        println!("Prompt sent to Gemini:\n{}\n---", prompt_preview);
    }
    let substring = service.local_match_text(name, symbol, tweet_text, translation);
    let basic = service.local_match_basic(name, symbol, tweet_text, translation);
    let verdict = service
        .match_text_with_policy(name, symbol, token_address, tweet_text, translation)
        .await?;
    println!("Token Name: {}", name);
    println!("Token Symbol: {}", symbol);
    println!("Tweet Text: {}", tweet_text);
    println!("substring match: {}", if substring { "yes" } else { "no" });
    println!("basic match: {}", if basic { "yes" } else { "no" });
    println!("Final verdict: {}", if verdict { "yes" } else { "no" });
    Ok(())
}
