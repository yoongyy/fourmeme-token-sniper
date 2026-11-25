use anyhow::{Context, Result};
use axum::{
    Router,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{Html, IntoResponse},
    routing::get,
};
use base64::{Engine as _, engine::general_purpose};
use chrono::{FixedOffset, TimeZone, Utc};
use ethers::types::Address;
use mongodb::bson::{Bson, DateTime as BsonDateTime, Document};
use sniper_fourmeme::{
    config::Config,
    rpc_fallback::{RpcProvider, build_http_provider, rpc_urls_from_env},
    storage::MongoLogger,
    token_manager,
};
use std::{collections::HashMap, env, net::SocketAddr, str::FromStr, sync::Arc, time::Duration};

const MALAYSIA_OFFSET_SECS: i32 = 8 * 3600;
const MALAYSIA_TIME_LABEL: &str = "MYT";

#[derive(Clone)]
struct AppState {
    mongo: Arc<MongoLogger>,
    http_provider: Arc<RpcProvider>,
    token_manager_address: Address,
    username: String,
    password: String,
    live_limit: i64,
    sim_limit: i64,
    smart_money_limit: i64,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let cfg = Config::from_env();
    let username = env::var("WEB_USERNAME").context("WEB_USERNAME is required")?;
    let password = env::var("WEB_PASSWORD").context("WEB_PASSWORD is required")?;
    let bind_addr = env::var("WEB_BIND").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let live_limit = env::var("WEB_LIVE_LIMIT")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(100);
    let sim_limit = env::var("WEB_SIM_LIMIT")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(200);
    let smart_money_limit = env::var("WEB_SMART_MONEY_LIMIT")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(200);
    let mongo = MongoLogger::connect(cfg.mongo())
        .await
        .ok_or_else(|| anyhow::anyhow!("mongo is required for dashboard"))?;

    let rpc_urls = rpc_urls_from_env(cfg.rpc_url.clone(), &["RPC_URL2", "RPC_URL3"]);
    let http_provider = Arc::new(
        build_http_provider(rpc_urls, Duration::from_millis(3000)).context("RPC_URL is invalid")?,
    );

    let state = AppState {
        mongo,
        http_provider,
        token_manager_address: cfg.token_manager_address,
        username,
        password,
        live_limit,
        sim_limit,
        smart_money_limit,
    };

    let app = Router::new().route("/", get(dashboard)).with_state(state);

    let addr: SocketAddr = bind_addr.parse().context("WEB_BIND is invalid")?;
    println!("[dashboard] listening on http://{}", addr);
    axum::serve(tokio::net::TcpListener::bind(addr).await?, app).await?;
    Ok(())
}

async fn dashboard(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if !is_authorized(&headers, &state.username, &state.password) {
        return (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Basic realm=\"dashboard\"")],
            "Unauthorized",
        )
            .into_response();
    }

    let live_orders = state
        .mongo
        .list_live_orders(Some(state.live_limit))
        .await
        .unwrap_or_default();
    let sim_orders = state
        .mongo
        .list_sim_orders(Some(state.sim_limit))
        .await
        .unwrap_or_default();
    let smart_money = state
        .mongo
        .list_smart_money(Some(state.smart_money_limit))
        .await
        .unwrap_or_default();

    let live_wallets = state
        .mongo
        .list_smart_money_live_wallets()
        .await
        .unwrap_or_default();
    let live_names = state
        .mongo
        .list_smart_money_names()
        .await
        .unwrap_or_default();

    let live_table = build_live_trades_table(&state, &live_orders, &live_names).await;
    let sim_table = build_simulation_table(&state, &sim_orders).await;
    let smart_money_table = build_smart_money_table(&smart_money);
    let live_wallets_table = build_live_wallets_table(&live_wallets, &live_names);

    let html = format!(
        r##"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Trade Dashboard</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #0c0f14;
      --card: #111823;
      --text: #f5f7fb;
      --muted: #9aa4b2;
      --accent: #3fb950;
      --border: #263241;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      background: radial-gradient(1200px circle at 10% -10%, #1f2a44 0%, transparent 55%),
                  radial-gradient(1200px circle at 110% -10%, #2a1f44 0%, transparent 55%),
                  var(--bg);
      color: var(--text);
      padding: 24px;
    }}
    h1 {{ margin: 0 0 12px 0; font-size: 28px; }}
    h2 {{ margin: 28px 0 10px 0; font-size: 20px; color: var(--accent); }}
    .card {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 16px;
      overflow: auto;
    }}
    .tabs {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin: 16px 0 12px 0;
    }}
    .toolbar {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: center;
      margin: 12px 0 16px 0;
      color: var(--muted);
      font-size: 12px;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }}
    .refresh-select {{
      background: #0f1725;
      border: 1px solid var(--border);
      color: var(--text);
      padding: 6px 10px;
      border-radius: 8px;
    }}
    .status-select {{
      background: #0f1725;
      border: 1px solid var(--border);
      color: var(--text);
      padding: 6px 10px;
      border-radius: 8px;
    }}
    .tab-btn {{
      background: transparent;
      color: var(--muted);
      border: 1px solid var(--border);
      border-radius: 999px;
      padding: 8px 14px;
      font-weight: 600;
      letter-spacing: 0.04em;
      cursor: pointer;
    }}
    .tab-btn.active {{
      color: var(--text);
      border-color: var(--accent);
      box-shadow: 0 0 0 1px rgba(63, 185, 80, 0.3);
    }}
    .tab-panel {{
      display: none;
      animation: fadeIn 180ms ease-out;
    }}
    .tab-panel.active {{
      display: block;
    }}
    @keyframes fadeIn {{
      from {{ opacity: 0; transform: translateY(4px); }}
      to {{ opacity: 1; transform: translateY(0); }}
    }}
    table {{
      border-collapse: collapse;
      width: 100%;
      min-width: 720px;
      font-size: 13px;
    }}
    th, td {{
      border-bottom: 1px solid var(--border);
      padding: 8px 10px;
      text-align: left;
      white-space: nowrap;
    }}
    th {{
      position: sticky;
      top: 0;
      background: #121a28;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-size: 11px;
    }}
    .muted {{ color: var(--muted); }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 16px;
    }}
  </style>
</head>
<body>
  <h1>Trade Dashboard</h1>

  <div class="tabs">
    <button class="tab-btn active" data-tab="live">Live Trades</button>
    <button class="tab-btn" data-tab="sim">Simulation Trades</button>
    <button class="tab-btn" data-tab="smart">Smart Money</button>
  </div>

  <div class="toolbar">
    <label for="refresh-select">Auto Refresh</label>
    <select id="refresh-select" class="refresh-select">
      <option value="0">Off</option>
      <option value="5">5s</option>
      <option value="10">10s</option>
      <option value="15">15s</option>
      <option value="30" selected>30s</option>
      <option value="60">60s</option>
    </select>
    <label for="status-select">Trade Status</label>
    <select id="status-select" class="status-select">
      <option value="new" selected>new</option>
      <option value="closed">closed</option>
      <option value="skipped">skipped</option>
      <option value="smart_sell">smart_sell</option>
      <option value="all">all</option>
    </select>
  </div>

  <div class="tab-panel active" id="tab-live">
    <h2>Live Trades</h2>
    <div class="card">{live_table}</div>
  </div>

  <div class="tab-panel" id="tab-sim">
    <h2>Simulation Trades</h2>
    <div class="card">{sim_table}</div>
  </div>

  <div class="tab-panel" id="tab-smart">
    <h2>Smart Money (DB)</h2>
    <div class="card">{smart_money_table}</div>
    <h2>Smart Money (Live)</h2>
    <div class="card">{live_wallets_table}</div>
  </div>

  <script>
    const buttons = Array.from(document.querySelectorAll(".tab-btn"));
    const panels = Array.from(document.querySelectorAll(".tab-panel"));
    const storedTab = localStorage.getItem("dashboard-tab") || "live";
    buttons.forEach((btn) => {{
      const target = btn.getAttribute("data-tab");
      btn.classList.toggle("active", target === storedTab);
    }});
    panels.forEach((panel) => {{
      panel.classList.toggle("active", panel.id === `tab-${{storedTab}}`);
    }});
    localStorage.setItem("dashboard-tab", storedTab);
    buttons.forEach((btn) => {{
      btn.addEventListener("click", () => {{
        const target = btn.getAttribute("data-tab");
        buttons.forEach((b) => b.classList.toggle("active", b === btn));
        panels.forEach((panel) => {{
          panel.classList.toggle("active", panel.id === `tab-${{target}}`);
        }});
        localStorage.setItem("dashboard-tab", target);
      }});
    }});

    const refreshSelect = document.getElementById("refresh-select");
    const statusSelect = document.getElementById("status-select");
    const liveRows = Array.from(document.querySelectorAll("#tab-live tbody tr"));
    let refreshTimer = null;
    function scheduleRefresh() {{
      if (refreshTimer) {{
        clearTimeout(refreshTimer);
        refreshTimer = null;
      }}
      const seconds = Number(refreshSelect.value);
      if (Number.isFinite(seconds) && seconds > 0) {{
        refreshTimer = setTimeout(() => {{
          window.location.reload();
        }}, seconds * 1000);
      }}
    }}
    function applyStatusFilter() {{
      const value = statusSelect.value;
      liveRows.forEach((row) => {{
        if (value === "all") {{
          row.style.display = "";
          return;
        }}
        const status = row.getAttribute("data-status") || "";
        row.style.display = status === value ? "" : "none";
      }});
    }}
    refreshSelect.addEventListener("change", scheduleRefresh);
    statusSelect.addEventListener("change", applyStatusFilter);
    scheduleRefresh();
    applyStatusFilter();
  </script>
</body>
</html>"##,
        live_table = live_table,
        sim_table = sim_table,
        smart_money_table = smart_money_table,
        live_wallets_table = live_wallets_table
    );

    Html(html).into_response()
}

fn is_authorized(headers: &HeaderMap, username: &str, password: &str) -> bool {
    let Some(value) = headers.get(header::AUTHORIZATION) else {
        return false;
    };
    let Ok(auth) = value.to_str() else {
        return false;
    };
    let Some(encoded) = auth.strip_prefix("Basic ") else {
        return false;
    };
    let Ok(decoded) = general_purpose::STANDARD.decode(encoded) else {
        return false;
    };
    let Ok(pair) = String::from_utf8(decoded) else {
        return false;
    };
    let mut parts = pair.splitn(2, ':');
    let user = parts.next().unwrap_or("");
    let pass = parts.next().unwrap_or("");
    user == username && pass == password
}

async fn build_live_trades_table(
    state: &AppState,
    orders: &[Document],
    live_names: &HashMap<String, String>,
) -> String {
    let mut token_prices: HashMap<Address, f64> = HashMap::new();
    let mut token_meta: HashMap<Address, (Option<String>, Option<String>)> = HashMap::new();
    let mut bnb_price = None;
    if let Ok(value) = token_manager::fetch_bnb_usd_price(&state.http_provider).await {
        if value > 0.0 {
            bnb_price = Some(value);
        }
    }

    for doc in orders {
        let token_addr_str = doc_string(doc, "token").unwrap_or_default();
        let token = Address::from_str(&token_addr_str).ok();
        if let Some(token) = token {
            if token_prices.contains_key(&token) {
                continue;
            }
            let price = token_manager::fetch_last_price(
                state.http_provider.clone(),
                state.token_manager_address,
                token,
            )
            .await
            .ok()
            .flatten()
            .unwrap_or(0.0);
            token_prices.insert(token, price);

            let (mut name, mut symbol) = normalize_token_meta(
                &token_addr_str,
                clean_token_string(doc_string(doc, "name")),
                clean_token_string(doc_string(doc, "symbol")),
            );
            if name.is_none() || symbol.is_none() {
                if let Ok((db_name, db_symbol)) = state.mongo.token_name_symbol(token).await {
                    let (next_name, next_symbol) =
                        normalize_token_meta(&token_addr_str, db_name, db_symbol);
                    if name.is_none() {
                        name = next_name;
                    }
                    if symbol.is_none() {
                        symbol = next_symbol;
                    }
                }
            }
            if name.is_none() || symbol.is_none() {
                if let Ok((chain_name, chain_symbol)) =
                    token_manager::fetch_token_name_symbol(state.http_provider.clone(), token).await
                {
                    let (next_name, next_symbol) =
                        normalize_token_meta(&token_addr_str, chain_name, chain_symbol);
                    if name.is_none() {
                        name = next_name;
                    }
                    if symbol.is_none() {
                        symbol = next_symbol;
                    }
                }
            }
            token_meta.insert(token, (name, symbol));
        }
    }

    let mut rows = String::new();
    for doc in orders {
        let token_addr = doc_string(doc, "token").unwrap_or_else(|| "n/a".into());
        let token_label = Address::from_str(&token_addr)
            .ok()
            .and_then(|addr| token_meta.get(&addr).cloned())
            .map(|(name, symbol)| format_token_label(name, symbol, &token_addr))
            .unwrap_or_else(|| {
                format_token_label(
                    normalize_token_meta(
                        &token_addr,
                        clean_token_string(doc_string(doc, "name")),
                        clean_token_string(doc_string(doc, "symbol")),
                    )
                    .0,
                    normalize_token_meta(
                        &token_addr,
                        clean_token_string(doc_string(doc, "name")),
                        clean_token_string(doc_string(doc, "symbol")),
                    )
                    .1,
                    &token_addr,
                )
            });
        let smart_wallet_raw = doc_string(doc, "smart_wallet").unwrap_or_else(|| "n/a".into());
        let smart_wallet = live_names
            .get(&smart_wallet_raw)
            .filter(|name| !name.trim().is_empty())
            .map(|name| format!("{name} ({smart_wallet_raw})"))
            .unwrap_or_else(|| smart_wallet_raw.clone());
        let time = doc_datetime(doc, "timestamp");
        let buy_bnb = doc_f64(doc, "trade_amount_bnb").unwrap_or(0.0);
        let buy_usd =
            doc_f64(doc, "entry_value_usd").or_else(|| bnb_price.map(|price| price * buy_bnb));
        let entry_price = doc_f64(doc, "entry_price").unwrap_or(0.0);
        let exit_price = doc_f64(doc, "exit_price").unwrap_or(0.0);
        let status = doc_string(doc, "status").unwrap_or_else(|| "n/a".into());
        let price_now = Address::from_str(&token_addr)
            .ok()
            .and_then(|addr| token_prices.get(&addr).copied())
            .unwrap_or(0.0);
        let pnl = if entry_price > 0.0 && price_now > 0.0 {
            Some((price_now / entry_price - 1.0) * 100.0)
        } else {
            None
        };
        rows.push_str(&format!(
            "<tr data-status=\"{}\">\
                <td>{}</td>\
                <td>{}</td>\
                <td class=\"muted\">{}</td>\
                <td>{}</td>\
                <td>{:.6}</td>\
                <td>{}</td>\
                <td>{}</td>\
                <td>{}</td>\
                <td>{}</td>\
                <td>{}</td>\
            </tr>",
            escape_html(&status),
            escape_html(&time),
            escape_html(&token_label),
            escape_html(&token_addr),
            escape_html(&smart_wallet),
            buy_bnb,
            buy_usd
                .map(|v| format!("{:.2}", v))
                .unwrap_or_else(|| "n/a".into()),
            if entry_price > 0.0 {
                format!("{:.10}", entry_price)
            } else {
                "n/a".into()
            },
            if exit_price > 0.0 {
                format!("{:.10}", exit_price)
            } else {
                "n/a".into()
            },
            pnl.map(|v| format!("{:.2}%", v))
                .unwrap_or_else(|| "n/a".into()),
            escape_html(&status),
        ));
    }

    format!(
        "<table>\
            <thead>\
                <tr>\
                    <th>Time</th>\
                    <th>Token</th>\
                    <th>Address</th>\
                    <th>Smart Money</th>\
                    <th>Buy BNB</th>\
                    <th>Buy USD</th>\
                    <th>Entry</th>\
                    <th>Exit</th>\
                    <th>Est PnL</th>\
                    <th>Status</th>\
                </tr>\
            </thead>\
            <tbody>{}</tbody>\
        </table>",
        rows
    )
}

async fn build_simulation_table(state: &AppState, docs: &[Document]) -> String {
    let allowed_statuses = ["new", "smart_sell", "closed", "skipped"];
    let mut groups: HashMap<String, GroupAccumulator> = HashMap::new();
    let mut token_meta: HashMap<Address, (Option<String>, Option<String>)> = HashMap::new();
    for doc in docs {
        let status = doc_string(doc, "status").unwrap_or_else(|| "n/a".into());
        if !allowed_statuses.contains(&status.as_str()) {
            continue;
        }
        let time = doc_datetime(doc, "timestamp");
        let token = doc_string(doc, "token").unwrap_or_else(|| "n/a".into());
        let (name, symbol) = if let Ok(addr) = Address::from_str(&token) {
            if let Some((cached_name, cached_symbol)) = token_meta.get(&addr).cloned() {
                (cached_name, cached_symbol)
            } else {
                let (mut resolved_name, mut resolved_symbol) = normalize_token_meta(
                    &token,
                    clean_token_string(doc_string(doc, "name")),
                    clean_token_string(doc_string(doc, "symbol")),
                );
                if resolved_name.is_none() || resolved_symbol.is_none() {
                    if let Ok((db_name, db_symbol)) = state.mongo.token_name_symbol(addr).await {
                        let (next_name, next_symbol) =
                            normalize_token_meta(&token, db_name, db_symbol);
                        if resolved_name.is_none() {
                            resolved_name = next_name;
                        }
                        if resolved_symbol.is_none() {
                            resolved_symbol = next_symbol;
                        }
                    }
                }
                if resolved_name.is_none() || resolved_symbol.is_none() {
                    if let Ok((chain_name, chain_symbol)) =
                        token_manager::fetch_token_name_symbol(state.http_provider.clone(), addr)
                            .await
                    {
                        let (next_name, next_symbol) =
                            normalize_token_meta(&token, chain_name, chain_symbol);
                        if resolved_name.is_none() {
                            resolved_name = next_name;
                        }
                        if resolved_symbol.is_none() {
                            resolved_symbol = next_symbol;
                        }
                    }
                }
                token_meta.insert(addr, (resolved_name.clone(), resolved_symbol.clone()));
                (resolved_name, resolved_symbol)
            }
        } else {
            normalize_token_meta(
                &token,
                clean_token_string(doc_string(doc, "name")),
                clean_token_string(doc_string(doc, "symbol")),
            )
        };
        let token_display = format_token_label(name, symbol, &token);
        let wallet = doc_string(doc, "smart_wallet");
        let wallet_display = wallet.clone().unwrap_or_else(|| "n/a".into());
        let volume = doc_f64(doc, "smart_volume_usd").unwrap_or(0.0);
        let smart_bnb = doc_f64(doc, "smart_trade_bnb").unwrap_or(0.0);
        let entry = doc_f64(doc, "entry_price").unwrap_or(0.0);
        let exit = doc_f64(doc, "exit_price").unwrap_or(0.0);
        let profit = doc_f64(doc, "profit_usd");
        let entry_value_usd = doc_f64(doc, "entry_value_usd");
        let wallet_key = wallet.unwrap_or_else(|| "n/a".into());
        let group_key = format!("{}::{}", wallet_key, token);
        let entry_acc = groups.entry(group_key).or_insert_with(|| GroupAccumulator {
            time: time.clone(),
            token_display: token_display.clone(),
            wallet_display: wallet_display.clone(),
            volume_sum: 0.0,
            smart_bnb_sum: 0.0,
            entry_sum: 0.0,
            entry_count: 0,
            exit_sum: 0.0,
            exit_count: 0,
            profit_sum: 0.0,
            entry_value_sum: 0.0,
            trade_count: 0,
        });
        if time > entry_acc.time {
            entry_acc.time = time;
        }
        entry_acc.volume_sum += volume;
        entry_acc.smart_bnb_sum += smart_bnb;
        if entry > 0.0 {
            entry_acc.entry_sum += entry;
            entry_acc.entry_count += 1;
        }
        if exit > 0.0 {
            entry_acc.exit_sum += exit;
            entry_acc.exit_count += 1;
        }
        if let Some(value) = profit {
            entry_acc.profit_sum += value;
        }
        if let Some(value) = entry_value_usd {
            entry_acc.entry_value_sum += value;
        }
        entry_acc.trade_count += 1;
    }

    let mut group_rows: Vec<(f64, String)> = Vec::new();
    for group in groups.values() {
        let avg_entry = if group.entry_count > 0 {
            group.entry_sum / group.entry_count as f64
        } else {
            0.0
        };
        let avg_exit = if group.exit_count > 0 {
            group.exit_sum / group.exit_count as f64
        } else {
            0.0
        };
        let pnl = if avg_entry > 0.0 && avg_exit > 0.0 {
            Some((avg_exit / avg_entry - 1.0) * 100.0)
        } else {
            None
        };
        let row = format!(
            "<tr>\
                <td>{}</td>\
                <td>{}</td>\
                <td>{}</td>\
                <td>{:.2}</td>\
                <td>{:.6}</td>\
                <td>{:.10}</td>\
                <td>{:.10}</td>\
                <td>{}</td>\
                <td>{}</td>\
            </tr>",
            escape_html(&group.time),
            escape_html(&group.token_display),
            escape_html(&group.wallet_display),
            group.volume_sum,
            group.smart_bnb_sum,
            avg_entry,
            avg_exit,
            if group.profit_sum.abs() > 0.0 {
                format!("{:.4}", group.profit_sum)
            } else {
                "n/a".into()
            },
            pnl.map(|v| format!("{:.2}%", v))
                .unwrap_or_else(|| "n/a".into()),
        );
        group_rows.push((pnl.unwrap_or(f64::NEG_INFINITY), row));
    }

    group_rows.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    let rows = group_rows
        .into_iter()
        .map(|(_, row)| row)
        .collect::<String>();

    format!(
        "<table>\
            <thead>\
                <tr>\
                    <th>Time</th>\
                    <th>Token</th>\
                    <th>Smart Money</th>\
                    <th>Volume USD</th>\
                    <th>Smart BNB</th>\
                    <th>Entry</th>\
                    <th>Exit</th>\
                    <th>Profit USD</th>\
                    <th>PnL</th>\
                </tr>\
            </thead>\
            <tbody>{}</tbody>\
        </table>",
        rows
    )
}

fn build_smart_money_table(docs: &[Document]) -> String {
    let mut rows = String::new();
    for doc in docs {
        let wallet = doc_string(doc, "wallet").unwrap_or_else(|| "n/a".into());
        let name = doc_string(doc, "name").unwrap_or_else(|| "unknown".into());
        let balance = doc_f64(doc, "balance");
        let follow = doc_i64(doc, "follow_count");
        let pnl_7d = doc_f64(doc, "pnl_7d");
        let pnl_30d = doc_f64(doc, "pnl_30d");
        let win_7d = doc_f64(doc, "winrate_7d");
        rows.push_str(&format!(
            "<tr>\
                <td>{}</td>\
                <td class=\"muted\">{}</td>\
                <td>{}</td>\
                <td>{}</td>\
                <td>{}</td>\
                <td>{}</td>\
                <td>{}</td>\
            </tr>",
            escape_html(&name),
            escape_html(&wallet),
            balance
                .map(|v| format!("{:.2}", v))
                .unwrap_or_else(|| "n/a".into()),
            follow
                .map(|v| v.to_string())
                .unwrap_or_else(|| "n/a".into()),
            pnl_7d
                .map(|v| format!("{:.2}", v))
                .unwrap_or_else(|| "n/a".into()),
            pnl_30d
                .map(|v| format!("{:.2}", v))
                .unwrap_or_else(|| "n/a".into()),
            win_7d
                .map(|v| format!("{:.2}%", v))
                .unwrap_or_else(|| "n/a".into()),
        ));
    }

    format!(
        "<table>\
            <thead>\
                <tr>\
                    <th>Name</th>\
                    <th>Wallet</th>\
                    <th>Balance</th>\
                    <th>Follow</th>\
                    <th>PnL 7d</th>\
                    <th>PnL 30d</th>\
                    <th>Win 7d</th>\
                </tr>\
            </thead>\
            <tbody>{}</tbody>\
        </table>",
        rows
    )
}

fn build_live_wallets_table(wallets: &[String], names: &HashMap<String, String>) -> String {
    let mut rows = String::new();
    for wallet in wallets {
        let name = names
            .get(wallet)
            .filter(|value| !value.trim().is_empty())
            .cloned()
            .unwrap_or_else(|| wallet.clone());
        rows.push_str(&format!(
            "<tr><td>{}</td><td class=\"muted\">{}</td></tr>",
            escape_html(&name),
            escape_html(wallet),
        ));
    }
    format!(
        "<table>\
            <thead>\
                <tr>\
                    <th>Name</th>\
                    <th>Wallet</th>\
                </tr>\
            </thead>\
            <tbody>{}</tbody>\
        </table>",
        rows
    )
}

fn doc_string(doc: &Document, key: &str) -> Option<String> {
    doc.get(key).and_then(|value| match value {
        Bson::String(value) => Some(value.to_string()),
        _ => value.as_str().map(|v| v.to_string()),
    })
}

fn clean_token_string(value: Option<String>) -> Option<String> {
    value
        .map(|val| val.trim().to_string())
        .filter(|val| !val.is_empty())
}

fn normalize_token_meta(
    token: &str,
    name: Option<String>,
    symbol: Option<String>,
) -> (Option<String>, Option<String>) {
    let token_norm = token.trim().to_ascii_lowercase();
    let name = name.filter(|value| {
        let lower = value.trim().to_ascii_lowercase();
        !lower.is_empty() && lower != "unknown" && lower != token_norm
    });
    let symbol = symbol.filter(|value| {
        let lower = value.trim().to_ascii_lowercase();
        !lower.is_empty() && lower != "unknown" && lower != token_norm
    });
    (name, symbol)
}

fn doc_f64(doc: &Document, key: &str) -> Option<f64> {
    doc.get_f64(key)
        .ok()
        .or_else(|| doc.get_i64(key).ok().map(|v| v as f64))
}

fn doc_i64(doc: &Document, key: &str) -> Option<i64> {
    doc.get_i64(key).ok()
}

fn doc_datetime(doc: &Document, key: &str) -> String {
    let value = doc.get_datetime(key).ok();
    value.map(format_myt).unwrap_or_else(|| "n/a".to_string())
}

fn format_myt(dt: &BsonDateTime) -> String {
    let chrono_dt = Utc
        .timestamp_millis_opt(dt.timestamp_millis())
        .single()
        .unwrap_or_else(Utc::now);
    let offset = FixedOffset::east_opt(MALAYSIA_OFFSET_SECS)
        .unwrap_or_else(|| FixedOffset::east_opt(MALAYSIA_OFFSET_SECS).expect("valid MYT offset"));
    let local = chrono_dt.with_timezone(&offset);
    format!(
        "{} {}",
        local.format("%Y-%m-%d %H:%M:%S"),
        MALAYSIA_TIME_LABEL
    )
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn format_token_label(name: Option<String>, symbol: Option<String>, token: &str) -> String {
    match (name, symbol) {
        (Some(name), Some(symbol)) => format!("{name} ({symbol})"),
        (Some(name), None) => name,
        (None, Some(symbol)) => symbol,
        (None, None) => token.to_string(),
    }
}

struct GroupAccumulator {
    time: String,
    token_display: String,
    wallet_display: String,
    volume_sum: f64,
    smart_bnb_sum: f64,
    entry_sum: f64,
    entry_count: u64,
    exit_sum: f64,
    exit_count: u64,
    profit_sum: f64,
    entry_value_sum: f64,
    trade_count: u64,
}
