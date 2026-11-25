use anyhow::{Context, Result, anyhow};
use grammers_client::types::PackedChat;
use grammers_client::{Client, Config as TelegramConfig, SignInError};
use grammers_session::Session;
use std::env;
use std::fs;
use std::path::PathBuf;
use tokio::{
    spawn,
    time::{Duration, Instant, sleep},
};

#[derive(Clone, Debug)]
pub struct TelegramTradeSettings {
    pub api_id: Option<i32>,
    pub api_hash: Option<String>,
    pub session_path: PathBuf,
    pub bot_username: Option<String>,
    pub phone: Option<String>,
    pub login_code: Option<String>,
    pub password: Option<String>,
}

pub struct TelegramTradeClient {
    inner: tokio::sync::Mutex<TelegramTradeInner>,
    send_delay: Duration,
}

struct TelegramTradeInner {
    client: Client,
    bot: PackedChat,
    last_send: Option<Instant>,
}

impl TelegramTradeSettings {
    pub fn from_env() -> Self {
        Self {
            api_id: env_value("TG_APP_ID").and_then(|val| val.parse::<i32>().ok()),
            api_hash: env_value("TG_APP_HASH"),
            session_path: PathBuf::from(env_value("TG_SESSION_PATH").unwrap_or_else(|| {
                env_value("TG_SESSION").unwrap_or_else(|| "./telegram.session".into())
            })),
            bot_username: env_value("GMGN_BOT_ID"),
            phone: env_value("TG_PHONE"),
            login_code: env_value("TG_LOGIN_CODE"),
            password: env_value("TG_PASSWORD"),
        }
    }

    pub fn from_env_tweet() -> Self {
        Self {
            api_id: env_value("TG_APP_ID_TWEET").and_then(|val| val.parse::<i32>().ok()),
            api_hash: env_value("TG_APP_HASH_TWEET"),
            session_path: PathBuf::from(env_value("TG_SESSION_TWEET_PATH").unwrap_or_else(|| {
                env_value("TG_SESSION_TWEET").unwrap_or_else(|| "./telegram_tweet.session".into())
            })),
            bot_username: env_value("GMGN_BOT_ID"),
            phone: env_value("TG_PHONE_TWEET").or_else(|| env_value("TG_PHONE")),
            login_code: env_value("TG_LOGIN_CODE_TWEET").or_else(|| env_value("TG_LOGIN_CODE")),
            password: env_value("TG_PASSWORD_TWEET").or_else(|| env_value("TG_PASSWORD")),
        }
    }
}

impl TelegramTradeClient {
    pub async fn connect(settings: &TelegramTradeSettings) -> Result<Self> {
        let inner = Self::connect_inner(settings).await?;
        let send_delay = send_delay_from_env();
        Ok(Self {
            inner: tokio::sync::Mutex::new(inner),
            send_delay,
        })
    }

    async fn connect_inner(settings: &TelegramTradeSettings) -> Result<TelegramTradeInner> {
        let api_id = settings
            .api_id
            .context("TG_APP_ID (or TG_APP_ID_TWEET) is missing")?;
        let api_hash = settings
            .api_hash
            .as_ref()
            .context("TG_APP_HASH (or TG_APP_HASH_TWEET) is missing")?;
        let bot_username = settings
            .bot_username
            .as_ref()
            .context("GMGN_BOT_ID is required for telegram trading")?;

        let session_path = resolve_session_path(&settings.session_path)?;
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
            Self::sign_in(&client, settings).await?;
            ensure_session_file(&session_path)?;
            client
                .session()
                .save_to_file(&session_path)
                .context("failed to save telegram session")?;
        }

        let username = bot_username.trim_start_matches('@');
        let bot = client
            .resolve_username(username)
            .await
            .context("failed to resolve GMGN_BOT_ID")?
            .map(|chat| chat.pack())
            .ok_or_else(|| anyhow!("GMGN_BOT_ID not found"))?;

        let client_bg = client.clone();
        spawn(async move {
            let _ = client_bg.run_until_disconnected().await;
        });

        Ok(TelegramTradeInner {
            client,
            bot,
            last_send: None,
        })
    }

    pub async fn send_buy(&self, token: &str, amount_bnb: f64) -> Result<()> {
        let msg = format!("/buy {token} {amount:.6}", amount = amount_bnb);
        println!("[telegram-trade] sending to GMGN bot: {msg}");
        self.send_message(&msg)
            .await
            .context("failed to send /buy to GMGN bot")?;
        Ok(())
    }

    pub async fn send_sell_percent(&self, token: &str, percent: u64) -> Result<()> {
        let msg = format!("/sell {token} {percent}");
        println!("[telegram-trade] sending to GMGN bot: {msg}");
        self.send_message(&msg)
            .await
            .context("failed to send /sell to GMGN bot")?;
        Ok(())
    }

    pub async fn send_limit_sell_full(&self, token: &str, price: f64) -> Result<()> {
        if !price.is_finite() || price <= 0.0 {
            return Err(anyhow!("limit sell price must be positive"));
        }
        let price = format_gmgn_price(price);
        let msg = format!("/create limitsell {token} 100@{price}");
        println!("[telegram-trade] sending to GMGN bot: {msg}");
        self.send_message(&msg)
            .await
            .context("failed to send /create limitsell to GMGN bot")?;
        Ok(())
    }

    pub async fn send_message(&self, text: &str) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if let Some(last_send) = inner.last_send {
            let elapsed = last_send.elapsed();
            if elapsed < self.send_delay {
                sleep(self.send_delay - elapsed).await;
            }
        }

        inner
            .client
            .send_message(inner.bot.clone(), text.to_string())
            .await?;
        inner.last_send = Some(Instant::now());

        Ok(())
    }

    async fn sign_in(client: &Client, settings: &TelegramTradeSettings) -> Result<()> {
        let phone = settings
            .phone
            .as_ref()
            .context("TG_PHONE is required to login telegram session")?;
        let login_token = client
            .request_login_code(phone)
            .await
            .context("failed to request telegram login code")?;
        let code = settings
            .login_code
            .as_ref()
            .context("TG_LOGIN_CODE is required to login telegram session")?;
        match client.sign_in(&login_token, code).await {
            Ok(_) => Ok(()),
            Err(SignInError::PasswordRequired(password_token)) => {
                let password = settings
                    .password
                    .as_ref()
                    .context("TG_PASSWORD is required for 2FA")?;
                client
                    .check_password(password_token, password)
                    .await
                    .context("telegram 2FA check failed")?;
                Ok(())
            }
            Err(err) => Err(anyhow!("telegram sign-in failed: {err:?}")),
        }
    }
}

fn env_value(key: &str) -> Option<String> {
    env::var(key).ok().and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn ensure_parent_dir(path: &PathBuf) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).context("failed to create telegram session directory")?;
        }
    }
    Ok(())
}

fn ensure_session_file(path: &PathBuf) -> Result<()> {
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

fn send_delay_from_env() -> Duration {
    let secs = env::var("TG_SEND_DELAY_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(3);
    Duration::from_secs(secs)
}

fn format_gmgn_price(value: f64) -> String {
    if value == 0.0 {
        return "0".into();
    }
    let mut value = format!("{:.12}", value);
    if let Some(dot) = value.find('.') {
        while value.ends_with('0') {
            value.pop();
        }
        if value.len() == dot + 1 {
            value.pop();
        }
    }
    value
}
