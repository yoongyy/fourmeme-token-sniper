use anyhow::{Context, Result, anyhow};
use grammers_client::{Client, Config as TelegramConfig, SignInError};
use grammers_session::Session;
use sniper_fourmeme::telegram_trade::TelegramTradeSettings;
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let use_tweet = env::args().any(|arg| arg == "--tweet");
    let mut settings = if use_tweet {
        TelegramTradeSettings::from_env_tweet()
    } else {
        TelegramTradeSettings::from_env()
    };
    if use_tweet && env::var("TG_SESSION_TWEET").is_err() {
        settings.session_path = "./telegram_tweet.session".into();
    } else if !use_tweet && env::var("TG_SESSION").is_err() {
        settings.session_path = "./telegram.session".into();
    }

    if settings.phone.is_none() {
        settings.phone = Some(prompt("TG_PHONE")?);
    }
    let session_path = resolve_session_path(&settings.session_path)?;
    eprintln!("[tg_login] Saving session to {}", session_path.display());
    let api_id = settings
        .api_id
        .context("TG_APP_ID (or TG_APP_ID_TWEET) is missing")?;
    let api_hash = settings
        .api_hash
        .as_ref()
        .context("TG_APP_HASH (or TG_APP_HASH_TWEET) is missing")?;

    let session = Session::load_file(&session_path).unwrap_or_else(|_| Session::new());
    let client = Client::connect(TelegramConfig {
        session,
        api_id,
        api_hash: api_hash.to_string(),
        params: Default::default(),
    })
    .await
    .context("failed to connect to Telegram")?;

    if !client.is_authorized().await? {
        let phone = settings.phone.as_ref().context("TG_PHONE is required")?;
        println!("📨 Requesting login code to {phone}...");
        let login_token = client
            .request_login_code(phone)
            .await
            .context("failed to request telegram login code")?;

        let code = prompt("TG_LOGIN_CODE")?;
        match client.sign_in(&login_token, &code).await {
            Ok(_) => {}
            Err(SignInError::PasswordRequired(password_token)) => {
                let password = settings
                    .password
                    .or_else(|| env::var("TG_PASSWORD").ok())
                    .ok_or_else(|| anyhow!("TG_PASSWORD is required for 2FA"))?;
                client
                    .check_password(password_token, &password)
                    .await
                    .context("telegram 2FA check failed")?;
            }
            Err(err) => return Err(anyhow!("telegram sign-in failed: {err:?}")),
        }

        ensure_session_file(&session_path)?;
        if let Err(err) = client.session().save_to_file(&session_path) {
            eprintln!(
                "[tg_login] failed to save session to {}: {err:?}",
                session_path.display()
            );
            return Err(anyhow!("failed to save telegram session"));
        }
    }

    println!(
        "✅ Telegram login ok. Session saved to {}",
        session_path.display()
    );
    Ok(())
}

fn prompt(label: &str) -> Result<String> {
    let mut input = String::new();
    print!("{label}: ");
    io::stdout().flush().context("failed to flush stdout")?;
    io::stdin()
        .read_line(&mut input)
        .context("failed to read input")?;
    let value = input.trim().to_string();
    if value.is_empty() {
        return Err(anyhow::anyhow!("{label} is required"));
    }
    Ok(value)
}

fn ensure_parent_dir(path: &std::path::Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).context("failed to create telegram session directory")?;
        }
    }
    Ok(())
}

fn ensure_session_file(path: &std::path::Path) -> Result<()> {
    ensure_parent_dir(path)?;
    if !path.exists() {
        std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(path)
            .context("failed to create telegram session file")?;
    }
    Ok(())
}

fn resolve_session_path(path: &PathBuf) -> Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path.clone());
    }
    let cwd = env::current_dir().context("failed to resolve current directory")?;
    Ok(cwd.join(path))
}
