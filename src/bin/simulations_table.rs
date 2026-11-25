use anyhow::{Context, Result};
use chrono::{FixedOffset, TimeZone, Utc};
use ethers::types::Address;
use mongodb::bson::{Bson, DateTime as BsonDateTime, Document, oid::ObjectId};
use sniper_fourmeme::{
    config::Config,
    rpc_fallback::{build_http_provider, rpc_urls_from_env},
    storage::MongoLogger,
    token_manager,
};
use std::collections::HashMap;
use std::env;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use unicode_width::UnicodeWidthStr;

const MALAYSIA_OFFSET_SECS: i32 = 8 * 3600;
const MALAYSIA_TIME_LABEL: &str = "MYT";

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let cfg = Config::from_env();
    let mongo = MongoLogger::connect(cfg.mongo())
        .await
        .ok_or_else(|| anyhow::anyhow!("mongo is required for simulations table"))?;
    let rpc_urls = rpc_urls_from_env(cfg.rpc_url.clone(), &["RPC_URL2", "RPC_URL3"]);
    let http_provider = Arc::new(
        build_http_provider(rpc_urls, Duration::from_millis(3000)).context("RPC_URL is invalid")?,
    );

    let mut limit: Option<i64> = None;
    let mut live_mode = false;
    let mut live_reset = false;
    for arg in env::args().skip(1) {
        if arg == "--live" {
            live_mode = true;
        } else if arg == "--reset" {
            live_reset = true;
        } else if let Ok(value) = arg.parse::<i64>() {
            if value > 0 {
                limit = Some(value);
            }
        }
    }

    let wallet_names = load_wallet_names(&mongo).await.unwrap_or_default();
    let docs = mongo.list_sim_orders(limit).await?;
    let sim_ids: Vec<_> = docs
        .iter()
        .filter_map(|doc| doc.get_object_id("_id").ok())
        .collect();
    let sim_txs = mongo.list_simulation_txs_for_ids(&sim_ids).await?;
    let tx_aggregates = build_tx_aggregates(&sim_txs);
    let mut groups: HashMap<String, GroupAccumulator> = HashMap::new();
    let mut summary = Summary::default();
    let mut wallet_stats: HashMap<String, WalletSummary> = HashMap::new();
    let mut token_meta: HashMap<Address, (Option<String>, Option<String>)> = HashMap::new();
    let allowed_statuses = ["smart_sell", "closed"];
    for doc in docs {
        let status = doc_string(&doc, "status").unwrap_or_else(|| "n/a".into());
        if !allowed_statuses.contains(&status.as_str()) {
            continue;
        }
        let time = doc_datetime(&doc, "timestamp");
        let token = doc_string(&doc, "token").unwrap_or_else(|| "n/a".into());
        let (name, symbol) = if let Ok(addr) = Address::from_str(&token) {
            if let Some((cached_name, cached_symbol)) = token_meta.get(&addr).cloned() {
                (cached_name, cached_symbol)
            } else {
                let mut resolved_name = clean_token_string(doc_string(&doc, "name"));
                let mut resolved_symbol = clean_token_string(doc_string(&doc, "symbol"));
                if resolved_name.is_none() || resolved_symbol.is_none() {
                    if let Ok((db_name, db_symbol)) = mongo.token_name_symbol(addr).await {
                        if resolved_name.is_none() {
                            resolved_name = db_name;
                        }
                        if resolved_symbol.is_none() {
                            resolved_symbol = db_symbol;
                        }
                    }
                }
                if resolved_name.is_none() || resolved_symbol.is_none() {
                    if let Ok((chain_name, chain_symbol)) =
                        token_manager::fetch_token_name_symbol(http_provider.clone(), addr).await
                    {
                        if resolved_name.is_none() {
                            resolved_name = chain_name;
                        }
                        if resolved_symbol.is_none() {
                            resolved_symbol = chain_symbol;
                        }
                    }
                }
                token_meta.insert(addr, (resolved_name.clone(), resolved_symbol.clone()));
                (resolved_name, resolved_symbol)
            }
        } else {
            (
                clean_token_string(doc_string(&doc, "name")),
                clean_token_string(doc_string(&doc, "symbol")),
            )
        };
        let token_display = format_token_label(name, symbol, &token);
        let wallet = doc_string(&doc, "smart_wallet");
        let wallet_display = wallet
            .as_deref()
            .and_then(|addr| wallet_names.get(addr))
            .and_then(|name| {
                if name.trim().is_empty() {
                    None
                } else {
                    Some(name.clone())
                }
            })
            .unwrap_or_else(|| wallet.clone().unwrap_or_else(|| "n/a".into()));
        let volume = doc_f64(&doc, "smart_volume_usd").unwrap_or(0.0);
        let smart_bnb = doc_f64(&doc, "smart_trade_bnb").unwrap_or(0.0);
        let sim_id = doc.get_object_id("_id").ok();
        let tx_agg = sim_id.and_then(|id| tx_aggregates.get(&id));
        let entry = doc_f64(&doc, "avg_entry_price")
            .or_else(|| doc_f64(&doc, "entry_price"))
            .or_else(|| tx_agg.and_then(|agg: &TxAggregate| agg.avg_buy_price()))
            .unwrap_or(0.0);
        let exit = doc_f64(&doc, "avg_exit_price")
            .or_else(|| doc_f64(&doc, "exit_price"))
            .or_else(|| tx_agg.and_then(|agg: &TxAggregate| agg.avg_sell_price()))
            .unwrap_or(0.0);
        let tx_profit = tx_agg.and_then(|agg: &TxAggregate| agg.profit_usd());
        let profit = doc_f64(&doc, "profit_usd").or_else(|| tx_profit);
        let entry_value_usd = doc_f64(&doc, "total_buy_usd")
            .or_else(|| doc_f64(&doc, "entry_value_usd"))
            .or_else(|| tx_agg.map(|agg| agg.buy_usd));
        let is_skipped = status == "skipped";
        let wallet_key = wallet.clone().unwrap_or_else(|| "n/a".into());
        let group_key = format!("{}::{}", wallet_key, token);
        let entry_acc = groups.entry(group_key).or_insert_with(|| GroupAccumulator {
            time: time.clone(),
            token_display: token_display.clone(),
            wallet_display: wallet_display.clone(),
            wallet_key: wallet_key.clone(),
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
        if !is_skipped {
            entry_acc.trade_count += 1;
        }
    }

    let mut rows = Vec::new();
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
        rows.push(TableRow {
            time: group.time.clone(),
            token: group.token_display.clone(),
            wallet: group.wallet_display.clone(),
            volume: fmt_opt(group.volume_sum, true),
            smart_bnb: fmt_opt(group.smart_bnb_sum, false),
            entry: fmt_opt(avg_entry, false),
            exit: fmt_opt(avg_exit, false),
            profit: if group.profit_sum.abs() > 0.0 {
                format!("{:.4}", group.profit_sum)
            } else {
                "n/a".into()
            },
            pnl: pnl
                .map(|v| format!("{:.2}%", v))
                .unwrap_or_else(|| "n/a".into()),
            status: "grouped".into(),
            profit_sort: group.profit_sum,
        });

        summary.total_trades += group.trade_count as usize;
        if group.profit_sum > 0.0 {
            summary.positive_trades += 1;
        } else if group.profit_sum < 0.0 {
            summary.negative_trades += 1;
        }
        summary.total_profit += group.profit_sum;
        summary.total_entry_usd += group.entry_value_sum;

        let stats = wallet_stats.entry(group.wallet_key.clone()).or_default();
        if stats.name.is_none() {
            stats.name = wallet_names.get(&group.wallet_key).and_then(|name| {
                if name.trim().is_empty() {
                    None
                } else {
                    Some(name.clone())
                }
            });
        }
        stats.trades += group.trade_count as usize;
        stats.profit += group.profit_sum;
        stats.entry += group.entry_value_sum;
    }

    if let Ok(records) = mongo
        .list_smart_money_by_wallets(&wallet_stats.keys().cloned().collect::<Vec<_>>())
        .await
    {
        for doc in records {
            let wallet = match doc_string(&doc, "wallet") {
                Some(value) => value,
                None => continue,
            };
            if let Some(stats) = wallet_stats.get_mut(&wallet) {
                if stats.name.is_none() {
                    stats.name = doc_string(&doc, "name").filter(|value| !value.trim().is_empty());
                }
                stats.balance = doc_f64(&doc, "balance");
                stats.follow_count = doc_i64(&doc, "follow_count");
            }
        }
    }
    rows.sort_by(|a, b| {
        b.profit_sort
            .partial_cmp(&a.profit_sort)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut widths = [
        display_width("Time"),
        display_width("Token"),
        display_width("SmartMoney"),
        display_width("Volume"),
        display_width("SmartBNB"),
        display_width("Entry"),
        display_width("Exit"),
        display_width("Profit"),
        display_width("PnL%"),
        display_width("Status"),
    ];
    for row in &rows {
        widths[0] = widths[0].max(display_width(&row.time));
        widths[1] = widths[1].max(display_width(&row.token));
        widths[2] = widths[2].max(display_width(&row.wallet));
        widths[3] = widths[3].max(display_width(&row.volume));
        widths[4] = widths[4].max(display_width(&row.smart_bnb));
        widths[5] = widths[5].max(display_width(&row.entry));
        widths[6] = widths[6].max(display_width(&row.exit));
        widths[7] = widths[7].max(display_width(&row.profit));
        widths[8] = widths[8].max(display_width(&row.pnl));
        widths[9] = widths[9].max(display_width(&row.status));
    }
    print_border(&widths);
    print_row(
        &widths,
        &[
            Cell::Left("Time"),
            Cell::Left("Token"),
            Cell::Left("SmartMoney"),
            Cell::Right("Volume"),
            Cell::Right("SmartBNB"),
            Cell::Right("Entry"),
            Cell::Right("Exit"),
            Cell::Right("Profit"),
            Cell::Right("PnL%"),
            Cell::Left("Status"),
        ],
    );
    for row in rows {
        print_border(&widths);
        print_row(
            &widths,
            &[
                Cell::Left(&row.time),
                Cell::Left(&row.token),
                Cell::Left(&row.wallet),
                Cell::Right(&row.volume),
                Cell::Right(&row.smart_bnb),
                Cell::Right(&row.entry),
                Cell::Right(&row.exit),
                Cell::Right(&row.profit),
                Cell::Right(&row.pnl),
                Cell::Left(&row.status),
            ],
        );
    }
    print_border(&widths);

    print_summary(&summary, &wallet_stats);
    if live_mode {
        update_live_wallets(&mongo, &wallet_stats, live_reset).await?;
    }
    Ok(())
}

async fn load_wallet_names(mongo: &MongoLogger) -> Result<HashMap<String, String>> {
    mongo.list_smart_money_names().await
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

fn doc_f64(doc: &Document, key: &str) -> Option<f64> {
    doc.get_f64(key)
        .ok()
        .or_else(|| doc.get_i64(key).ok().map(|v| v as f64))
}

fn doc_datetime(doc: &Document, key: &str) -> String {
    let value = doc.get_datetime(key).ok();
    value.map(format_myt).unwrap_or_else(|| "n/a".to_string())
}

fn format_token_label(name: Option<String>, symbol: Option<String>, token: &str) -> String {
    match (name, symbol) {
        (Some(name), Some(symbol)) => format!("{name} ({symbol})"),
        (Some(name), None) => name,
        (None, Some(symbol)) => symbol,
        (None, None) => token.to_string(),
    }
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

fn fmt_opt(value: f64, trim: bool) -> String {
    if value <= 0.0 {
        return "n/a".into();
    }
    if trim {
        format!("{:.2}", value)
    } else {
        format!("{:.10}", value)
    }
}

#[derive(Default)]
struct TxAggregate {
    buy_tokens: f64,
    buy_usd: f64,
    sell_tokens: f64,
    sell_usd: f64,
}

impl TxAggregate {
    fn avg_buy_price(&self) -> Option<f64> {
        if self.buy_tokens > 0.0 {
            Some(self.buy_usd / self.buy_tokens)
        } else {
            None
        }
    }

    fn avg_sell_price(&self) -> Option<f64> {
        if self.sell_tokens > 0.0 {
            Some(self.sell_usd / self.sell_tokens)
        } else {
            None
        }
    }

    fn profit_usd(&self) -> Option<f64> {
        let avg_buy = self.avg_buy_price()?;
        if avg_buy > 0.0 && self.sell_tokens > 0.0 {
            Some(self.sell_usd - avg_buy * self.sell_tokens)
        } else {
            None
        }
    }
}

fn build_tx_aggregates(txs: &[Document]) -> HashMap<ObjectId, TxAggregate> {
    let mut aggregates: HashMap<ObjectId, TxAggregate> = HashMap::new();
    for doc in txs {
        let Some(sim_id) = doc.get_object_id("simulation_id").ok() else {
            continue;
        };
        let side = doc_string(doc, "side").unwrap_or_else(|| "n/a".into());
        let amount_tokens = doc_f64(doc, "amount_tokens").unwrap_or(0.0);
        let amount_usd = doc_f64(doc, "amount_usd").unwrap_or(0.0);
        let entry = aggregates.entry(sim_id).or_default();
        if side.eq_ignore_ascii_case("buy") {
            entry.buy_tokens += amount_tokens;
            entry.buy_usd += amount_usd;
        } else if side.eq_ignore_ascii_case("sell") {
            entry.sell_tokens += amount_tokens;
            entry.sell_usd += amount_usd;
        }
    }
    aggregates
}

#[derive(Default)]
struct Summary {
    total_profit: f64,
    total_entry_usd: f64,
    total_trades: usize,
    positive_trades: usize,
    negative_trades: usize,
}

#[derive(Default)]
struct WalletSummary {
    profit: f64,
    entry: f64,
    trades: usize,
    name: Option<String>,
    balance: Option<f64>,
    follow_count: Option<i64>,
}

fn print_summary(summary: &Summary, wallet_stats: &HashMap<String, WalletSummary>) {
    let pnl = if summary.total_entry_usd > 0.0 {
        Some(summary.total_profit / summary.total_entry_usd * 100.0)
    } else {
        None
    };
    println!();
    println!("Summary");
    println!(
        "Total trades: {} | Total profit: {:.4} | Total PnL: {} | Positive: {} | Negative: {}",
        summary.total_trades,
        summary.total_profit,
        pnl.map(|v| format!("{:.2}%", v))
            .unwrap_or_else(|| "n/a".into()),
        summary.positive_trades,
        summary.negative_trades
    );

    if wallet_stats.is_empty() {
        return;
    }

    let mut wallets: Vec<WalletRow> = wallet_stats
        .iter()
        .map(|(wallet, stats)| {
            let pnl = if stats.entry > 0.0 {
                Some(stats.profit / stats.entry * 100.0)
            } else {
                None
            };
            WalletRow {
                wallet_address: wallet.to_string(),
                wallet_name: stats.name.clone().filter(|value| !value.trim().is_empty()),
                balance: stats.balance,
                follow_count: stats.follow_count,
                trades: stats.trades,
                profit: format!("{:.4}", stats.profit),
                pnl: pnl
                    .map(|v| format!("{:.2}%", v))
                    .unwrap_or_else(|| "n/a".into()),
                pnl_sort: pnl.unwrap_or(f64::NEG_INFINITY),
            }
        })
        .collect();
    wallets.sort_by(|a, b| {
        b.pnl_sort
            .partial_cmp(&a.pnl_sort)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    println!();
    println!("Smart Money Summary (sorted by PnL)");
    let mut widths = [
        display_width("Wallet"),
        display_width("Follow"),
        display_width("Balance"),
        display_width("Trades"),
        display_width("Profit"),
        display_width("PnL%"),
    ];
    for row in &wallets {
        widths[0] = widths[0].max(
            display_width(row.wallet_name.as_deref().unwrap_or("unknown"))
                .max(display_width(&row.wallet_address)),
        );
        widths[1] = widths[1].max(row.follow_count.map(|v| v.to_string().len()).unwrap_or(3));
        widths[2] = widths[2].max(row.balance.map(|v| format!("{:.2}", v).len()).unwrap_or(3));
        widths[3] = widths[3].max(row.trades.to_string().len());
        widths[4] = widths[4].max(row.profit.len());
        widths[5] = widths[5].max(row.pnl.len());
    }
    print_border(&widths);
    print_row(
        &widths,
        &[
            Cell::Left("Wallet"),
            Cell::Right("Follow"),
            Cell::Right("Balance"),
            Cell::Right("Trades"),
            Cell::Right("Profit"),
            Cell::Right("PnL%"),
        ],
    );
    print_border(&widths);
    for row in wallets {
        let name_line = row.wallet_name.as_deref().unwrap_or("unknown");
        print_row(
            &widths,
            &[
                Cell::Left(name_line),
                Cell::Right(
                    &row.follow_count
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "n/a".into()),
                ),
                Cell::Right(
                    &row.balance
                        .map(|v| format!("{:.2}", v))
                        .unwrap_or_else(|| "n/a".into()),
                ),
                Cell::Right(&row.trades.to_string()),
                Cell::Right(&row.profit),
                Cell::Right(&row.pnl),
            ],
        );
        print_row(
            &widths,
            &[
                Cell::Left(&row.wallet_address),
                Cell::Right(""),
                Cell::Right(""),
                Cell::Right(""),
                Cell::Right(""),
                Cell::Right(""),
            ],
        );
        print_border(&widths);
    }
}

#[derive(Debug)]
struct WalletRow {
    wallet_address: String,
    wallet_name: Option<String>,
    balance: Option<f64>,
    follow_count: Option<i64>,
    trades: usize,
    profit: String,
    pnl: String,
    pnl_sort: f64,
}

fn doc_i64(doc: &Document, key: &str) -> Option<i64> {
    doc.get_i64(key).ok()
}

async fn update_live_wallets(
    mongo: &MongoLogger,
    wallet_stats: &HashMap<String, WalletSummary>,
    reset: bool,
) -> Result<()> {
    let threshold = env::var("SMART_LIVE_PNL_THRESHOLD")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(50.0);
    let trade_threshold = env::var("SMART_LIVE_TRADE_THRESHOLD")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(5);
    let mut keep = Vec::new();
    for (wallet, stats) in wallet_stats {
        if stats.entry <= 0.0 {
            continue;
        }
        if stats.trades < trade_threshold {
            continue;
        }
        let pnl = stats.profit / stats.entry * 100.0;
        if pnl >= threshold {
            keep.push(wallet.to_string());
        }
    }
    mongo.set_smart_money_live_wallets(&keep, reset).await?;
    println!(
        "[live] {} smart_money.live with {} wallets (pnl >= {:.2}%, trades >= {})",
        if reset { "reset" } else { "updated" },
        keep.len(),
        threshold,
        trade_threshold
    );
    Ok(())
}

#[derive(Debug)]
struct TableRow {
    time: String,
    token: String,
    wallet: String,
    volume: String,
    smart_bnb: String,
    entry: String,
    exit: String,
    profit: String,
    pnl: String,
    status: String,
    profit_sort: f64,
}

struct GroupAccumulator {
    time: String,
    token_display: String,
    wallet_display: String,
    wallet_key: String,
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

enum Cell<'a> {
    Left(&'a str),
    Right(&'a str),
}

fn print_border(widths: &[usize]) {
    let mut line = String::new();
    line.push('+');
    for width in widths {
        line.push_str(&"-".repeat(*width + 2));
        line.push('+');
    }
    println!("{line}");
}

fn print_row(widths: &[usize], cells: &[Cell<'_>]) {
    let mut line = String::new();
    line.push('|');
    for (idx, cell) in cells.iter().enumerate() {
        let width = widths.get(idx).copied().unwrap_or(0);
        let content = match cell {
            Cell::Left(value) => pad_left(value, width),
            Cell::Right(value) => pad_right(value, width),
        };
        line.push(' ');
        line.push_str(&content);
        line.push(' ');
        line.push('|');
    }
    println!("{line}");
}

fn display_width(value: &str) -> usize {
    UnicodeWidthStr::width(value)
}

fn pad_left(value: &str, width: usize) -> String {
    let len = display_width(value);
    let pad = width.saturating_sub(len);
    format!("{value}{}", " ".repeat(pad))
}

fn pad_right(value: &str, width: usize) -> String {
    let len = display_width(value);
    let pad = width.saturating_sub(len);
    format!("{}{}", " ".repeat(pad), value)
}
