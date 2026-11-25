use anyhow::{Context, Result};
use ethers::types::Address;
use reqwest::header::{ACCEPT, ACCEPT_LANGUAGE, CONTENT_TYPE, HeaderMap, HeaderValue, REFERER};
use serde_json::{Value, json};
use sniper_fourmeme::{config::Config, storage::MongoLogger};
use std::{collections::HashMap, process::Stdio, str::FromStr};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let cfg = Config::from_env();
    let open_router_api_key = std::env::var("OPEN_ROUTER_API_KEY").ok();
    let open_router_base_url = std::env::var("OPEN_ROUTER_BASE_URL")
        .unwrap_or_else(|_| "https://openrouter.ai/api/v1".into());
    let open_router_model = std::env::var("OPEN_ROUTER_MODEL").ok();
    let gmgn = cfg.gmgn.clone();
    if !gmgn.enabled {
        anyhow::bail!("GMGN is disabled");
    }
    let mut headers = HeaderMap::new();
    headers.insert(
        ACCEPT,
        HeaderValue::from_static("application/json, text/plain, */*"),
    );
    headers.insert(ACCEPT_LANGUAGE, HeaderValue::from_static("en-US,en;q=0.9"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(
        REFERER,
        HeaderValue::from_str(&format!("https://gmgn.ai/?chain={}", gmgn.chain))
            .unwrap_or_else(|_| HeaderValue::from_static("https://gmgn.ai/")),
    );

    let client = reqwest::Client::builder()
        .user_agent(gmgn.user_agent.clone())
        .default_headers(headers)
        .build()
        .context("build reqwest client")?;

    let timeframe = std::env::var("SMART_MONEY_TIMEFRAME").unwrap_or_else(|_| "7d".to_string());
    let tag = std::env::var("SMART_MONEY_TAG").unwrap_or_else(|_| "smart_degen".to_string());
    let url = format!(
        "https://gmgn.ai/defi/quotation/v1/rank/{}/wallets/{}?tag={}&orderby=pnl_{}&direction=desc",
        gmgn.chain, timeframe, tag, timeframe
    );

    println!("[smart-money] fetching trending wallets (timeframe={timeframe}, tag={tag})");
    let resp = client.get(&url).send().await.context("request failed")?;
    let status = resp.status();
    let body = resp.text().await.context("read body")?;
    let json: Value = if status.is_success() {
        serde_json::from_str(&body).context("parse gmgn json")?
    } else {
        eprintln!(
            "[smart-money] primary request failed (status {}), trying python gmgn client...",
            status
        );
        fetch_via_python(&timeframe, &tag)?
    };
    let data = json.get("data").cloned().unwrap_or(json);

    let mut wallets = Vec::new();
    gather_wallets(&data, &mut wallets);

    // dedup and prefer entries that have names
    let mut by_addr: HashMap<String, SmartWallet> = HashMap::new();
    for wallet in wallets {
        by_addr
            .entry(wallet.address.clone())
            .and_modify(|existing| {
                if existing.name.is_none() && wallet.name.is_some() {
                    existing.name = wallet.name.clone();
                }
                if should_replace_raw(&existing.raw, &wallet.raw) {
                    existing.raw = wallet.raw.clone();
                }
                if existing.balance.is_none() && wallet.balance.is_some() {
                    existing.balance = wallet.balance;
                }
                if existing.follow_count.is_none() && wallet.follow_count.is_some() {
                    existing.follow_count = wallet.follow_count;
                }
                if existing.txs_7d.is_none() && wallet.txs_7d.is_some() {
                    existing.txs_7d = wallet.txs_7d;
                }
                if existing.winrate_7d.is_none() && wallet.winrate_7d.is_some() {
                    existing.winrate_7d = wallet.winrate_7d;
                }
                if existing.pnl_7d.is_none() && wallet.pnl_7d.is_some() {
                    existing.pnl_7d = wallet.pnl_7d;
                }
                if existing.avg_cost_7d.is_none() && wallet.avg_cost_7d.is_some() {
                    existing.avg_cost_7d = wallet.avg_cost_7d;
                }
                if existing.realized_profit_30d.is_none() && wallet.realized_profit_30d.is_some() {
                    existing.realized_profit_30d = wallet.realized_profit_30d;
                }
                if existing.pnl_30d.is_none() && wallet.pnl_30d.is_some() {
                    existing.pnl_30d = wallet.pnl_30d;
                }
            })
            .or_insert(wallet);
    }
    if by_addr.is_empty() {
        anyhow::bail!("no smart wallets found in gmgn response");
    }
    let top_k = env_usize("SMART_MONEY_TOP_K", 10).max(1);
    if open_router_api_key.is_some() && open_router_model.is_some() {
        let wallet_list = build_wallet_json_list(by_addr.values());
        let batch_size = env_usize("SMART_MONEY_AI_BATCH_SIZE", 20).max(1);
        let batches = chunk_wallets(&wallet_list, batch_size);
        println!(
            "[smart-money] openrouter batch_size={} batches={}",
            batch_size,
            batches.len()
        );
        let mut selected: Vec<String> = Vec::new();
        for (idx, batch) in batches.iter().enumerate() {
            let mut relaxed = false;
            for _ in 0..2 {
                let prompt = if relaxed {
                    build_gemini_prompt_relaxed(batch, top_k)
                } else {
                    build_gemini_prompt(batch, top_k)
                };
                println!(
                    "[smart-money] openrouter prompt batch {}/{} (chars={})",
                    idx + 1,
                    batches.len(),
                    prompt.len()
                );
                match filter_with_open_router(
                    open_router_api_key.as_deref(),
                    open_router_base_url.as_str(),
                    open_router_model.as_deref(),
                    &prompt,
                )
                .await
                {
                    Ok(result) => {
                        if result.is_empty() && !relaxed {
                            relaxed = true;
                            continue;
                        }
                        for addr in result {
                            if !selected.contains(&addr) {
                                selected.push(addr);
                            }
                        }
                        break;
                    }
                    Err(err) => {
                        if err.to_string().contains("MAX_TOKENS") {
                            anyhow::bail!(
                                "openrouter finish_reason=MAX_TOKENS (reduce SMART_MONEY_AI_BATCH_SIZE)"
                            );
                        }
                        return Err(err);
                    }
                }
            }
            if selected.len() >= top_k {
                break;
            }
        }
        if selected.is_empty() {
            anyhow::bail!("openrouter returned no wallets");
        }
        let selected: Vec<String> = selected.into_iter().take(top_k).collect();
        by_addr.retain(|addr, _| selected.contains(addr));
        println!(
            "[smart-money] openrouter selected {} wallets (top_k={})",
            selected.len(),
            top_k
        );
    } else {
        println!("[smart-money] openrouter disabled; keeping full list");
    }

    if let Some(mongo) = MongoLogger::connect(cfg.mongo()).await {
        let sim_deleted = mongo.clear_simulations().await?;
        let tx_deleted = mongo.clear_simulation_txs().await?;
        println!(
            "[smart-money] cleared simulations={} simulation_tx={}",
            sim_deleted, tx_deleted
        );
        let smart_deleted = mongo.clear_non_live_smart_money().await?;
        println!(
            "[smart-money] cleared smart_money (non-live)={}",
            smart_deleted
        );
        let mut stored = 0usize;
        for wallet in by_addr.values() {
            let addr = match Address::from_str(&wallet.address) {
                Ok(value) => value,
                Err(_) => continue,
            };
            let name = wallet.name.clone().unwrap_or_default();
            if mongo
                .upsert_smart_money_record(
                    addr,
                    Some(name.as_str()),
                    wallet.balance,
                    wallet.follow_count,
                    wallet.txs_7d,
                    wallet.winrate_7d,
                    wallet.pnl_7d,
                    wallet.avg_cost_7d,
                    wallet.realized_profit_30d,
                    wallet.pnl_30d,
                    true,
                )
                .await
                .is_ok()
            {
                stored += 1;
            }
        }
        println!(
            "[smart-money] stored {} records in mongo collection smart_money",
            stored
        );
    }
    Ok(())
}

fn extract_address(value: &Value) -> Option<String> {
    match value {
        Value::String(addr) => {
            let norm = normalize_address(addr);
            if norm.is_empty() { None } else { Some(norm) }
        }
        Value::Object(map) => {
            let keys = [
                "wallet_address",
                "wallet",
                "address",
                "owner",
                "trader_address",
            ];
            for key in keys {
                if let Some(Value::String(addr)) = map.get(key) {
                    let norm = normalize_address(addr);
                    if !norm.is_empty() {
                        return Some(norm);
                    }
                }
            }
            None
        }
        _ => None,
    }
}

fn normalize_address(addr: &str) -> String {
    let trimmed = addr.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    let clean = trimmed.trim_start_matches("0x").trim_start_matches("0X");
    if clean.len() != 40 {
        return String::new();
    }
    if !clean.chars().all(|c| c.is_ascii_hexdigit()) {
        return String::new();
    }
    let lower = clean.to_ascii_lowercase();
    format!("0x{}", lower)
}

fn extract_name(value: &Value) -> Option<String> {
    let Value::Object(map) = value else {
        return None;
    };
    let keys = [
        "name",
        "wallet_name",
        "nickname",
        "owner_name",
        "trader_name",
    ];
    for key in keys {
        if let Some(Value::String(name)) = map.get(key) {
            let trimmed = name.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn gather_wallets(value: &Value, out: &mut Vec<SmartWallet>) {
    match value {
        Value::Array(items) => {
            for entry in items {
                gather_wallets(entry, out);
            }
        }
        Value::Object(map) => {
            let addr = extract_address(value);
            let name = extract_name(value);
            let balance = extract_balance(value);
            let follow_count = extract_follow_count(value);
            let txs_7d = extract_txs_7d(value);
            let winrate_7d = extract_winrate_7d(value);
            let pnl_7d = extract_pnl_7d(value);
            let avg_cost_7d = extract_avg_cost_7d(value);
            let realized_profit_30d = extract_realized_profit_30d(value);
            let pnl_30d = extract_pnl_30d(value);
            if let Some(addr) = addr {
                out.push(SmartWallet {
                    address: addr,
                    name,
                    raw: value.clone(),
                    balance,
                    follow_count,
                    txs_7d,
                    winrate_7d,
                    pnl_7d,
                    avg_cost_7d,
                    realized_profit_30d,
                    pnl_30d,
                });
            }
            if let Some(inner) = map.get("data") {
                gather_wallets(inner, out);
            }
            for v in map.values() {
                gather_wallets(v, out);
            }
        }
        Value::String(_) => {
            if let Some(addr) = extract_address(value) {
                out.push(SmartWallet {
                    address: addr,
                    name: None,
                    raw: value.clone(),
                    balance: None,
                    follow_count: None,
                    txs_7d: None,
                    winrate_7d: None,
                    pnl_7d: None,
                    avg_cost_7d: None,
                    realized_profit_30d: None,
                    pnl_30d: None,
                });
            }
        }
        _ => {}
    }
}

fn fetch_via_python(timeframe: &str, tag: &str) -> Result<Value> {
    let script = format!(
        "{}/scripts/smart_money_wallets.py",
        env!("CARGO_MANIFEST_DIR")
    );
    let output = std::process::Command::new("python3")
        .arg(&script)
        .arg("--timeframe")
        .arg(timeframe)
        .arg("--tag")
        .arg(tag)
        .stdout(Stdio::piped())
        .output()
        .context("python helper failed to start")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("python helper exited with {}: {}", output.status, stderr);
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let addresses: Value = serde_json::from_str(&stdout).context("parse python helper json")?;
    Ok(addresses)
}

#[derive(Clone, Debug)]
struct SmartWallet {
    address: String,
    name: Option<String>,
    raw: Value,
    balance: Option<f64>,
    follow_count: Option<i64>,
    txs_7d: Option<i64>,
    winrate_7d: Option<f64>,
    pnl_7d: Option<f64>,
    avg_cost_7d: Option<f64>,
    realized_profit_30d: Option<f64>,
    pnl_30d: Option<f64>,
}

fn extract_balance(value: &Value) -> Option<f64> {
    let Value::Object(map) = value else {
        return None;
    };
    let keys = [
        "balance",
        "balance_usd",
        "balanceUsd",
        "total_balance",
        "total_value",
        "assets",
        "asset",
    ];
    for key in keys {
        if let Some(val) = map.get(key) {
            if let Some(num) = value_to_f64(val) {
                return Some(num);
            }
        }
    }
    None
}

fn extract_follow_count(value: &Value) -> Option<i64> {
    let Value::Object(map) = value else {
        return None;
    };
    let keys = [
        "follow_count",
        "followCount",
        "followers",
        "followers_count",
        "follower_count",
    ];
    for key in keys {
        if let Some(val) = map.get(key) {
            if let Some(num) = value_to_i64(val) {
                return Some(num);
            }
        }
    }
    None
}

fn extract_txs_7d(value: &Value) -> Option<i64> {
    let Value::Object(map) = value else {
        return None;
    };
    let keys = ["txs_7d", "tx_7d", "txs7d", "trade_count_7d"];
    for key in keys {
        if let Some(val) = map.get(key) {
            if let Some(num) = value_to_i64(val) {
                return Some(num);
            }
        }
    }
    None
}

fn extract_winrate_7d(value: &Value) -> Option<f64> {
    let Value::Object(map) = value else {
        return None;
    };
    let keys = ["winrate_7d", "win_rate_7d", "winrate7d", "win_rate7d"];
    for key in keys {
        if let Some(val) = map.get(key) {
            if let Some(num) = value_to_f64(val) {
                return Some(num);
            }
        }
    }
    None
}

fn extract_pnl_7d(value: &Value) -> Option<f64> {
    let Value::Object(map) = value else {
        return None;
    };
    let keys = ["pnl_7d", "pnl7d", "pnl_7day"];
    for key in keys {
        if let Some(val) = map.get(key) {
            if let Some(num) = value_to_f64(val) {
                return Some(num);
            }
        }
    }
    None
}

fn extract_avg_cost_7d(value: &Value) -> Option<f64> {
    let Value::Object(map) = value else {
        return None;
    };
    let keys = ["avg_cost_7d", "avg_cost7d", "average_cost_7d"];
    for key in keys {
        if let Some(val) = map.get(key) {
            if let Some(num) = value_to_f64(val) {
                return Some(num);
            }
        }
    }
    None
}

fn extract_realized_profit_30d(value: &Value) -> Option<f64> {
    let Value::Object(map) = value else {
        return None;
    };
    let keys = [
        "realized_profit_30d",
        "realizedProfit_30d",
        "realized_profit30d",
    ];
    for key in keys {
        if let Some(val) = map.get(key) {
            if let Some(num) = value_to_f64(val) {
                return Some(num);
            }
        }
    }
    None
}

fn extract_pnl_30d(value: &Value) -> Option<f64> {
    let Value::Object(map) = value else {
        return None;
    };
    let keys = ["pnl_30d", "pnl30d", "pnl_30day"];
    for key in keys {
        if let Some(val) = map.get(key) {
            if let Some(num) = value_to_f64(val) {
                return Some(num);
            }
        }
    }
    None
}

fn value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(num) => num.as_f64(),
        Value::String(raw) => raw.parse::<f64>().ok(),
        _ => None,
    }
}

fn value_to_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(num) => num.as_i64().or_else(|| num.as_f64().map(|v| v as i64)),
        Value::String(raw) => raw.parse::<i64>().ok(),
        _ => None,
    }
}

fn should_replace_raw(existing: &Value, candidate: &Value) -> bool {
    match (existing, candidate) {
        (Value::Object(existing_map), Value::Object(candidate_map)) => {
            candidate_map.len() > existing_map.len()
        }
        (Value::String(_), Value::Object(_)) => true,
        (Value::Null, Value::Object(_)) => true,
        _ => false,
    }
}

fn build_wallet_json_list<'a>(wallets: impl Iterator<Item = &'a SmartWallet>) -> Vec<Value> {
    wallets.map(|wallet| wallet.raw.clone()).collect()
}

fn chunk_wallets(list: &[Value], batch_size: usize) -> Vec<Vec<Value>> {
    if list.is_empty() {
        return Vec::new();
    }
    let mut batches = Vec::new();
    let mut idx = 0usize;
    while idx < list.len() {
        let end = (idx + batch_size).min(list.len());
        batches.push(list[idx..end].to_vec());
        idx = end;
    }
    batches
}

fn env_f64_value(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_u64_value(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn build_gemini_prompt(wallets: &[Value], top_k: usize) -> String {
    let capital_bnb = env_f64_value("SMART_MONEY_CAPITAL_BNB", 1.0);
    let max_entry_bnb = env_f64_value("SMART_MONEY_MAX_ENTRY_BNB", 0.01);
    let latency_sec = env_u64_value("SMART_MONEY_LATENCY_SEC", 3);
    let slippage_pct = env_f64_value("SMART_MONEY_SLIPPAGE_PCT", 3.0);
    let max_positions = env_usize("SMART_MONEY_MAX_POSITIONS", 5).max(1);
    let wallets_json = serde_json::to_string(wallets).unwrap_or_else(|_| "[]".into());
    format!(
        "I am building a four.meme TokenManager copy-trading simulation bot. Only store the top 10 smart money.\n\n\
PROMPT:\n\
I have a list of smart money wallets from GMGN (JSON objects below).\n\n\
Your job:\n\
1) Filter and rank the BEST wallets for me to simulate and follow.\n\
2) The goal is \"copy-tradable ROI\" under my constraints, not the wallet's own ROI.\n\
3) Reduce load: return only the top K wallets.\n\n\
=== My trading constraints ===\n\
- Chain: BSC\n\
- Platform: four.meme TokenManager (not swap)\n\
- My capital: {capital_bnb} BNB\n\
- Max entry per trade: {max_entry_bnb} BNB\n\
- My expected execution latency: {latency_sec} seconds\n\
- Slippage tolerance: {slippage_pct} %\n\
- Max simultaneous positions: {max_positions}\n\
- I only want wallets that trade actively on four.meme microcaps and are scalable to my small size\n\
- I want stable profit, not lottery / insider spikes\n\n\
=== GMGN wallet JSON fields you may see ===\n\
realized_profit_{{1d,7d,30d}}, pnl_{{1d,7d,30d}}, winrate_{{1d,7d,30d}},\n\
buy_{{1d,7d,30d}}, sell_{{1d,7d,30d}}, txs_{{1d,7d,30d}}, avg_cost_{{1d,7d,30d}},\n\
pnl buckets (pnl_lt_minus_dot5_num_7d, pnl_minus_dot5_0x_num_7d, pnl_lt_2x_num_7d, pnl_2x_5x_num_7d, pnl_gt_5x_num_7d),\n\
balance, tags, follow_count, remark_count\n\n\
=== Selection rules ===\n\
A) Hard filters (reject if any):\n\
- txs_7d < 80  (too inactive)\n\
- buy_7d < 40  (not enough entries)\n\
- winrate_7d < 0.60\n\
- pnl_7d <= 0\n\
- avg_cost_7d > (my MAX_ENTRY_BNB * 8)  (too whale / not copy-scalable)\n\
- pnl_lt_minus_dot5_num_7d is high (> 12)  (too many big losers)\n\
- suspicious profile: tags include \"dev\", \"insider\", \"bot farm\" (if present)\n\n\
B) Scoring goal:\n\
Maximize COPYABLE expected ROI with my latency.\n\
Prefer wallets that:\n\
- have high pnl_7d and pnl_30d but not extreme one-shot variance\n\
- high winrate_7d (>=0.70 is strong)\n\
- many trades (more confidence)\n\
- moderate avg_cost close to my size (0.05–2 BNB)\n\
- low big-loss count\n\
- pnl distribution: more small winners, few catastrophic losses\n\n\
C) Penalize these patterns:\n\
- Extremely high pnl_gt_5x_num_7d combined with low trade count -> lottery sniper\n\
- Very short-term scalp wallets (high txs_1d, but pnl_30d unstable)\n\
- Massive avg_cost (whales) because I can't copy the same entry timing and liquidity\n\n\
=== Output format (STRICT JSON ONLY) ===\n\
Return:\n\
{{\n\
  \"top_wallets\": [\n\
    {{\n\
      \"rank\": 1,\n\
      \"address\": \"0x...\",\n\
      \"copyability_score\": 0,\n\
      \"expected_copy_roi_7d\": \"low/medium/high\",\n\
      \"style\": \"scalper|momentum|swing|mixed\",\n\
      \"why_selected\": [\"reason1\",\"reason2\",\"reason3\"],\n\
      \"risk_flags\": [\"flag1\",\"flag2\"],\n\
      \"recommended_simulation_mode\": {{\n\
        \"latency_sec\": {latency_sec},\n\
        \"entry_bnb\": {max_entry_bnb},\n\
        \"tp_rule\": \"example: +25% or smart-wallet-sell\",\n\
        \"sl_rule\": \"example: -20% from peak or sell-volume spike\",\n\
        \"max_hold_seconds\": 600\n\
      }}\n\
    }}\n\
  ],\n\
  \"rejected_summary\": {{\n\
    \"total_input\": 0,\n\
    \"total_selected\": 0,\n\
    \"rejection_reasons_count\": {{\n\
      \"inactive\": 0,\n\
      \"low_winrate\": 0,\n\
      \"negative_pnl\": 0,\n\
      \"too_whale\": 0,\n\
      \"too_many_big_losses\": 0,\n\
      \"suspicious\": 0\n\
    }}\n\
  }}\n\
}}\n\n\
IMPORTANT:\n\
- Output ONLY JSON, no explanation text.\n\
- Keep why_selected and risk_flags to max 2 items each.\n\
- Use integers and numbers, do not use placeholders like <int> or 0-100.\n\
- Rank must be based on COPYABILITY for me.\n\
- Choose K={top_k} wallets only.\n\
- If too few pass, relax only txs_7d threshold down to 50, keep pnl_7d>0 and winrate_7d>=0.6.\n\
- Never include duplicated wallets.\n\n\
=== Input Wallet List ===\n\
{wallets_json}",
        capital_bnb = capital_bnb,
        max_entry_bnb = max_entry_bnb,
        latency_sec = latency_sec,
        slippage_pct = slippage_pct,
        max_positions = max_positions,
        top_k = top_k,
        wallets_json = wallets_json
    )
}

fn build_gemini_prompt_relaxed(wallets: &[Value], top_k: usize) -> String {
    let capital_bnb = env_f64_value("SMART_MONEY_CAPITAL_BNB", 1.0);
    let max_entry_bnb = env_f64_value("SMART_MONEY_MAX_ENTRY_BNB", 0.01);
    let latency_sec = env_u64_value("SMART_MONEY_LATENCY_SEC", 3);
    let slippage_pct = env_f64_value("SMART_MONEY_SLIPPAGE_PCT", 3.0);
    let max_positions = env_usize("SMART_MONEY_MAX_POSITIONS", 5).max(1);
    let wallets_json = serde_json::to_string(wallets).unwrap_or_else(|_| "[]".into());
    format!(
        "I am building a four.meme TokenManager copy-trading simulation bot. Only store the top {top_k} smart money.\n\n\
Your job:\n\
1) Rank the BEST wallets for me to simulate and follow using whatever metrics are available.\n\
2) If some fields are missing, do NOT reject; infer based on other fields.\n\
3) Return only the top {top_k} wallets.\n\n\
=== My trading constraints ===\n\
- Chain: BSC\n\
- Platform: four.meme TokenManager (not swap)\n\
- My capital: {capital_bnb} BNB\n\
- Max entry per trade: {max_entry_bnb} BNB\n\
- My expected execution latency: {latency_sec} seconds\n\
- Slippage tolerance: {slippage_pct} %\n\
- Max simultaneous positions: {max_positions}\n\
- I only want wallets that trade actively on four.meme microcaps and are scalable to my small size\n\
- I want stable profit, not lottery / insider spikes\n\n\
=== Output format (STRICT JSON ONLY) ===\n\
Return:\n\
{{\n\
  \"top_wallets\": [\n\
    {{\n\
      \"rank\": 1,\n\
      \"address\": \"0x...\",\n\
      \"copyability_score\": 0,\n\
      \"expected_copy_roi_7d\": \"low/medium/high\",\n\
      \"style\": \"scalper|momentum|swing|mixed\",\n\
      \"why_selected\": [\"reason1\",\"reason2\"],\n\
      \"risk_flags\": [\"flag1\",\"flag2\"],\n\
      \"recommended_simulation_mode\": {{\n\
        \"latency_sec\": {latency_sec},\n\
        \"entry_bnb\": {max_entry_bnb},\n\
        \"tp_rule\": \"example: +25% or smart-wallet-sell\",\n\
        \"sl_rule\": \"example: -20% from peak or sell-volume spike\",\n\
        \"max_hold_seconds\": 600\n\
      }}\n\
    }}\n\
  ],\n\
  \"rejected_summary\": {{\n\
    \"total_input\": 0,\n\
    \"total_selected\": 0,\n\
    \"rejection_reasons_count\": {{\n\
      \"inactive\": 0,\n\
      \"low_winrate\": 0,\n\
      \"negative_pnl\": 0,\n\
      \"too_whale\": 0,\n\
      \"too_many_big_losses\": 0,\n\
      \"suspicious\": 0\n\
    }}\n\
  }}\n\
}}\n\n\
IMPORTANT:\n\
- Output ONLY JSON, no explanation text.\n\
- Use integers and numbers, do not use placeholders like <int> or 0-100.\n\
- Never include duplicated wallets.\n\n\
=== Input Wallet List ===\n\
{wallets_json}",
        capital_bnb = capital_bnb,
        max_entry_bnb = max_entry_bnb,
        latency_sec = latency_sec,
        slippage_pct = slippage_pct,
        max_positions = max_positions,
        top_k = top_k,
        wallets_json = wallets_json
    )
}

async fn filter_with_open_router(
    api_key: Option<&str>,
    base_url: &str,
    model: Option<&str>,
    prompt: &str,
) -> Result<Vec<String>> {
    let api_key = api_key.context("OPEN_ROUTER_API_KEY is missing")?;
    let model = model.context("OPEN_ROUTER_MODEL is missing")?;
    let url = format!("{}/chat/completions", base_url.trim_end_matches('/'));
    let body = json!({
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": "You are a strict JSON generator. Output only valid JSON, no markdown or extra text."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        "temperature": 0.0,
        "max_tokens": 4096
    });
    let response = reqwest::Client::new()
        .post(&url)
        .bearer_auth(api_key)
        .header("HTTP-Referer", "https://local.smart_money_refresh")
        .header("X-Title", "smart_money_refresh")
        .json(&body)
        .send()
        .await
        .context("openrouter request failed")?;
    let status = response.status();
    let body_text = response.text().await.unwrap_or_default();
    if !status.is_success() {
        let snippet: String = body_text.chars().take(400).collect();
        anyhow::bail!("openrouter error status={} body={}", status, snippet);
    }
    let json_value: Value =
        serde_json::from_str(&body_text).context("openrouter json parse failed")?;
    if let Some(reason) = json_value["choices"]
        .as_array()
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("finish_reason"))
        .and_then(|value| value.as_str())
    {
        if reason != "stop" {
            eprintln!("[smart-money] openrouter finish_reason={}", reason);
            if reason == "length" {
                anyhow::bail!("openrouter finish_reason=MAX_TOKENS");
            }
        }
    }
    let content = json_value["choices"]
        .as_array()
        .and_then(|choices| choices.first())
        .and_then(|choice| choice["message"]["content"].as_str())
        .context("openrouter response missing content")?;
    let parsed = parse_gemini_json(content).with_context(|| {
        format!(
            "parse openrouter output json failed: {}",
            truncate_text(content, 800)
        )
    })?;
    let top_wallets = parsed
        .get("top_wallets")
        .and_then(|value| value.as_array())
        .context("missing top_wallets")?;
    let mut addresses = Vec::new();
    for entry in top_wallets {
        if let Some(addr) = entry.get("address").and_then(|value| value.as_str()) {
            let norm = normalize_address(addr);
            if !norm.is_empty() && !addresses.contains(&norm) {
                addresses.push(norm);
            }
        }
    }
    Ok(addresses)
}

fn parse_gemini_json(text: &str) -> Option<Value> {
    let trimmed = text.trim();
    let cleaned = strip_code_fence(trimmed);
    let sanitized = sanitize_gemini_json(cleaned);
    if let Ok(value) = serde_json::from_str::<Value>(&sanitized) {
        return Some(value);
    }
    if let Some(slice) = extract_json_object(&sanitized) {
        if let Ok(value) = serde_json::from_str::<Value>(slice) {
            return Some(value);
        }
    }
    if let Some(slice) = extract_json_prefix(&sanitized) {
        let closed = close_json_fragment(slice);
        if let Ok(value) = serde_json::from_str::<Value>(&closed) {
            return Some(value);
        }
    }
    None
}

fn strip_code_fence(text: &str) -> &str {
    let trimmed = text.trim();
    if !trimmed.starts_with("```") {
        return trimmed;
    }
    let without_start = trimmed
        .trim_start_matches("```json")
        .trim_start_matches("```");
    let without_start = without_start.trim_start();
    if let Some(end_idx) = without_start.rfind("```") {
        return without_start[..end_idx].trim();
    }
    without_start
}

fn truncate_text(text: &str, max_len: usize) -> String {
    if text.len() <= max_len {
        return text.to_string();
    }
    text.chars().take(max_len).collect()
}

fn sanitize_gemini_json(text: &str) -> String {
    let mut cleaned = String::with_capacity(text.len());
    for ch in text.chars() {
        if ch.is_control() && !matches!(ch, '\n' | '\r' | '\t') {
            continue;
        }
        cleaned.push(ch);
    }
    let cleaned = cleaned.replace("<int>", "0").replace("0-100", "0");
    strip_trailing_commas(&cleaned)
}

fn strip_trailing_commas(text: &str) -> String {
    let mut out = text.to_string();
    while out.contains(",}") || out.contains(",]") {
        out = out.replace(",}", "}").replace(",]", "]");
    }
    out
}

fn extract_json_prefix(text: &str) -> Option<&str> {
    let start = text.find('{')?;
    Some(&text[start..])
}

fn close_json_fragment(text: &str) -> String {
    let mut in_string = false;
    let mut escape = false;
    let mut brace = 0i32;
    let mut bracket = 0i32;
    for ch in text.chars() {
        if in_string {
            if escape {
                escape = false;
            } else if ch == '\\' {
                escape = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        match ch {
            '"' => in_string = true,
            '{' => brace += 1,
            '}' => brace = (brace - 1).max(0),
            '[' => bracket += 1,
            ']' => bracket = (bracket - 1).max(0),
            _ => {}
        }
    }
    let mut out = text.to_string();
    for _ in 0..bracket {
        out.push(']');
    }
    for _ in 0..brace {
        out.push('}');
    }
    out
}

fn extract_json_object(text: &str) -> Option<&str> {
    let mut in_string = false;
    let mut escape = false;
    let mut depth = 0i32;
    let mut start: Option<usize> = None;
    for (idx, ch) in text.char_indices() {
        if in_string {
            if escape {
                escape = false;
            } else if ch == '\\' {
                escape = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        match ch {
            '"' => in_string = true,
            '{' => {
                if depth == 0 {
                    start = Some(idx);
                }
                depth += 1;
            }
            '}' => {
                if depth > 0 {
                    depth -= 1;
                    if depth == 0 {
                        let begin = start?;
                        return Some(&text[begin..=idx]);
                    }
                }
            }
            _ => {}
        }
    }
    None
}
