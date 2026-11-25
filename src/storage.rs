use crate::{
    config::MongoSettings,
    gmgn::GmgnTokenInfo,
    types::{TokenCreated, TokenEvent},
};
use anyhow::{Result, anyhow};
use chrono::{Duration, Utc};
use ethers::types::{Address, H256, Log};
use futures_util::TryStreamExt;
use mongodb::{
    Client, Collection,
    bson::{Bson, DateTime as BsonDateTime, Document, doc, oid::ObjectId},
    options::{
        FindOneAndUpdateOptions, FindOneOptions, FindOptions, ReturnDocument, UpdateOptions,
    },
};
use std::{collections::HashMap, str::FromStr, sync::Arc};

const MALAYSIA_OFFSET_SECS: i64 = 8 * 3600;

#[derive(Clone)]
pub struct MongoLogger {
    tokens: Collection<Document>,
    pairs: Collection<Document>,
    orders: Collection<Document>,
    simulations: Collection<Document>,
    simulation_tx: Collection<Document>,
    order_tx: Collection<Document>,
    smart_money: Collection<Document>,
    ai_logs: Collection<Document>,
}

#[derive(Debug, Clone, Default)]
pub struct SimulationAggregate {
    pub total_buy_tokens: f64,
    pub total_buy_usd: f64,
    pub total_sell_tokens: f64,
    pub total_sell_usd: f64,
    pub avg_buy_price: f64,
    pub avg_sell_price: f64,
    pub profit_usd: f64,
    pub buy_count: i64,
    pub sell_count: i64,
}

impl MongoLogger {
    pub async fn connect(settings: &MongoSettings) -> Option<Arc<Self>> {
        if !settings.is_enabled() {
            eprintln!("[mongo] disabled (missing URI or MONGO_ENABLE=false)");
            return None;
        }
        let uri = match settings.uri() {
            Some(value) => value,
            None => {
                eprintln!("[mongo] URI not configured");
                return None;
            }
        };
        let client = match Client::with_uri_str(uri).await {
            Ok(client) => client,
            Err(err) => {
                eprintln!("[mongo] failed to initialize client: {err:?}");
                return None;
            }
        };
        let db = client.database(&settings.db_name);
        println!(
            "[mongo] connected to database '{}' (collections: tokens={}, pairs={}, orders={}, simulations={}, simulation_tx={}, order_tx={}, smart_money={}, ai_logs={})",
            settings.db_name,
            settings.tokens_collection,
            settings.pairs_collection,
            settings.orders_collection,
            settings.simulations_collection,
            settings.simulation_tx_collection,
            settings.order_tx_collection,
            settings.smart_money_collection,
            settings.ai_logs_collection
        );
        Some(Arc::new(Self {
            tokens: db.collection(&settings.tokens_collection),
            pairs: db.collection(&settings.pairs_collection),
            orders: db.collection(&settings.orders_collection),
            simulations: db.collection(&settings.simulations_collection),
            simulation_tx: db.collection(&settings.simulation_tx_collection),
            order_tx: db.collection(&settings.order_tx_collection),
            smart_money: db.collection(&settings.smart_money_collection),
            ai_logs: db.collection(&settings.ai_logs_collection),
        }))
    }

    pub async fn log_token_creation(
        &self,
        token: &TokenCreated,
        log: &Log,
        gmgn_enabled: bool,
    ) -> Result<()> {
        let document = doc! {
            "timestamp": now_bson(),
            "token": format_address(token.token),
            "name": &token.name,
            "symbol": &token.symbol,
            "creator": format_address(token.creator),
            "tx_hash": log.transaction_hash.map(|h| format_hash(h)),
            "block_number": log.block_number.map(|b| b.as_u64() as i64),
            "log_index": log.log_index.map(|i| i.as_u64() as i64),
            "gmgn_enabled": gmgn_enabled,
        };
        self.tokens.insert_one(document, None).await?;
        Ok(())
    }

    pub async fn token_name_symbol(
        &self,
        token: Address,
    ) -> Result<(Option<String>, Option<String>)> {
        let filter = doc! {
            "token": format_address(token),
        };
        let options = FindOneOptions::builder()
            .sort(doc! { "timestamp": -1 })
            .projection(doc! { "name": 1, "symbol": 1 })
            .build();
        let doc = self.tokens.find_one(filter, options).await?;
        let name = doc
            .as_ref()
            .and_then(|value| value.get_str("name").ok())
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let symbol = doc
            .as_ref()
            .and_then(|value| value.get_str("symbol").ok())
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        Ok((name, symbol))
    }

    pub async fn upsert_pair_details(
        &self,
        token: &TokenCreated,
        info: &GmgnTokenInfo,
        min_liquidity: f64,
    ) -> Result<Option<bool>> {
        let filter = doc! {
            "token": format_address(token.token),
        };
        let update = doc! {
            "$set": {
                "timestamp": now_bson(),
                "token": format_address(token.token),
                "name": &token.name,
                "symbol": &token.symbol,
                "liquidity": info.liquidity,
                "initial_liquidity": info.initial_liquidity,
                "price": info.price,
                "holders": info.holder_count.map(|h| h as i64),
                "pool_address": info.pool_address.map(format_address),
                "exchange": info.exchange.clone(),
                "quote_symbol": info.quote_symbol.clone(),
                "is_honeypot": info.is_honeypot,
                "is_open_source": info.is_open_source,
                "is_renounced": info.is_renounced,
                "is_locked": info.is_locked,
                "min_liquidity": min_liquidity,
            },
            "$setOnInsert": {
                "alert_sent": false,
            }
        };
        let options = FindOneAndUpdateOptions::builder()
            .upsert(true)
            .return_document(ReturnDocument::Before)
            .build();
        let previous = self
            .pairs
            .find_one_and_update(filter, update, options)
            .await?;
        let previous_alert_sent = previous.and_then(|doc| doc.get_bool("alert_sent").ok());
        Ok(previous_alert_sent)
    }

    pub async fn update_token_tx_value(
        &self,
        token: &TokenCreated,
        tx_hash: H256,
        tx_value_bnb: f64,
        tx_value_usd: Option<f64>,
    ) -> Result<()> {
        let mut set_doc = doc! {
            "token": format_address(token.token),
            "name": &token.name,
            "symbol": &token.symbol,
            "tx_value_bnb": tx_value_bnb,
            "tx_value_timestamp": now_bson(),
        };
        if let Some(value) = tx_value_usd {
            set_doc.insert("tx_value_usd", value);
        }
        let filter = doc! {
            "token": format_address(token.token),
            "tx_hash": format_hash(tx_hash),
        };
        let update = doc! {
            "$set": set_doc,
            "$setOnInsert": {
                "creator": format_address(token.creator),
            }
        };
        let options = UpdateOptions::builder().upsert(true).build();
        self.tokens.update_one(filter, update, options).await?;
        Ok(())
    }

    pub async fn upsert_pair_tx_liquidity(
        &self,
        token: &TokenCreated,
        tx_value_bnb: f64,
        tx_value_usd: Option<f64>,
        min_liquidity: f64,
    ) -> Result<(Option<bool>, Option<bool>)> {
        let filter = doc! {
            "token": format_address(token.token),
        };
        let mut set_doc = doc! {
            "timestamp": now_bson(),
            "token": format_address(token.token),
            "name": &token.name,
            "symbol": &token.symbol,
            "tx_value_bnb": tx_value_bnb,
            "min_liquidity": min_liquidity,
            "liquidity_source": "tx_value",
        };
        if let Some(value) = tx_value_usd {
            set_doc.insert("tx_value_usd", value);
            set_doc.insert("liquidity", value);
            set_doc.insert("initial_liquidity", value);
        }
        let update = doc! {
            "$set": set_doc,
            "$setOnInsert": {
                "alert_sent": false,
                "tweet_alert_sent": false,
            }
        };
        let options = FindOneAndUpdateOptions::builder()
            .upsert(true)
            .return_document(ReturnDocument::Before)
            .build();
        let previous = self
            .pairs
            .find_one_and_update(filter, update, options)
            .await?;
        let previous_alert_sent = previous
            .as_ref()
            .and_then(|doc| doc.get_bool("alert_sent").ok());
        let previous_tweet_alert_sent = previous
            .as_ref()
            .and_then(|doc| doc.get_bool("tweet_alert_sent").ok());
        Ok((previous_alert_sent, previous_tweet_alert_sent))
    }

    pub async fn mark_pair_alert_sent(&self, token: Address) -> Result<()> {
        let filter = doc! {
            "token": format_address(token),
        };
        let update = doc! {
            "$set": {
                "alert_sent": true,
                "alert_sent_at": now_bson(),
            }
        };
        let options = UpdateOptions::builder().upsert(true).build();
        self.pairs.update_one(filter, update, options).await?;
        Ok(())
    }

    pub async fn log_ai_response(
        &self,
        token_name: &str,
        token_symbol: &str,
        token_address: Address,
        tweet_id: Option<&str>,
        tweet_text: &str,
        gemini_response: &str,
    ) -> Result<()> {
        let mut document = doc! {
            "timestamp": now_bson(),
            "token": format_address(token_address),
            "name": token_name,
            "symbol": token_symbol,
            "tweet_text": tweet_text,
            "gemini_response": gemini_response,
        };
        if let Some(value) = tweet_id {
            document.insert("tweet_id", value);
        }
        self.ai_logs.insert_one(document, None).await?;
        Ok(())
    }

    pub async fn mark_pair_tweet_alert_sent(&self, token: Address) -> Result<()> {
        let filter = doc! {
            "token": format_address(token),
        };
        let update = doc! {
            "$set": {
                "tweet_alert_sent": true,
                "tweet_alert_sent_at": now_bson(),
            }
        };
        let options = UpdateOptions::builder().upsert(true).build();
        self.pairs.update_one(filter, update, options).await?;
        Ok(())
    }

    pub async fn create_real_order(
        &self,
        token: &TokenCreated,
        token_event: &TokenEvent,
        tx_hash: H256,
        entry_price: f64,
        take_profit_price: f64,
        stop_loss_price: f64,
        alert_type: &str,
        price_source: &str,
        trade_amount_bnb: f64,
        entry_value_usd: Option<f64>,
        token_amount: Option<f64>,
        wallet_name: Option<&str>,
        smart_wallet: Option<Address>,
        smart_trade_bnb: Option<f64>,
    ) -> Result<ObjectId> {
        let buy_tokens = token_amount.or_else(|| {
            entry_value_usd.and_then(|value| {
                if entry_price > 0.0 {
                    Some(value / entry_price)
                } else {
                    None
                }
            })
        });
        let mut document = doc! {
            "timestamp": now_bson(),
            "token": format_address(token.token),
            "name": &token.name,
            "symbol": &token.symbol,
            "ticker": &token_event.ticker,
            "creator": token_event.creator.map(format_address),
            "buy_amount": token_event.buy_amount,
            "tx_hash": format_hash(tx_hash),
            "entry_price": entry_price,
            "take_profit_price": take_profit_price,
            "stop_loss_price": stop_loss_price,
            "alert_type": alert_type,
            "status": "new",
            "simulation": false,
            "price_source": price_source,
            "trade_amount_bnb": trade_amount_bnb,
        };
        if let Some(value) = wallet_name {
            document.insert("wallet_name", value);
        }
        if let Some(value) = smart_wallet {
            document.insert("smart_wallet", format_address(value));
        }
        if let Some(value) = smart_trade_bnb {
            document.insert("smart_trade_bnb", value);
        }
        if let Some(value) = entry_value_usd {
            document.insert("entry_value_usd", value);
        }
        if let Some(value) = token_amount {
            document.insert("token_amount", value);
        }
        if let Some(value) = buy_tokens {
            if value > 0.0 {
                document.insert("remaining_token_amount", value);
                document.insert("sold_token_amount", 0.0f64);
            }
        }
        let result = self.orders.insert_one(document, None).await?;
        let order_id = result
            .inserted_id
            .as_object_id()
            .ok_or_else(|| anyhow!("missing inserted id"))?;
        if let Some(amount_tokens) = buy_tokens {
            if amount_tokens > 0.0 {
                let amount_usd = entry_value_usd.unwrap_or(entry_price * amount_tokens);
                let _ = self
                    .record_order_tx(
                        order_id,
                        token.token,
                        smart_wallet,
                        "buy",
                        entry_price,
                        amount_tokens,
                        amount_usd.max(0.0),
                        Some(&format_hash(tx_hash)),
                        None,
                        None,
                    )
                    .await;
            }
        }
        Ok(order_id)
    }

    pub async fn create_reverted_order(
        &self,
        token: &TokenCreated,
        token_event: &TokenEvent,
        alert_type: &str,
        price_source: &str,
        trade_amount_bnb: f64,
        error: &str,
        wallet_name: Option<&str>,
    ) -> Result<ObjectId> {
        let mut document = doc! {
            "timestamp": now_bson(),
            "token": format_address(token.token),
            "name": &token.name,
            "symbol": &token.symbol,
            "ticker": &token_event.ticker,
            "creator": token_event.creator.map(format_address),
            "buy_amount": token_event.buy_amount,
            "alert_type": alert_type,
            "status": "reverted",
            "simulation": false,
            "price_source": price_source,
            "trade_amount_bnb": trade_amount_bnb,
            "error": error,
        };
        if let Some(value) = wallet_name {
            document.insert("wallet_name", value);
        }
        let result = self.orders.insert_one(document, None).await?;
        result
            .inserted_id
            .as_object_id()
            .ok_or_else(|| anyhow!("missing inserted id"))
    }

    pub async fn create_sim_order(
        &self,
        token: &TokenCreated,
        pair_address: Option<Address>,
        entry_price: f64,
        take_profit_price: f64,
        stop_loss_price: f64,
        price_source: &str,
        trade_amount_bnb: f64,
        entry_value_usd: Option<f64>,
        smart_volume_usd: Option<f64>,
        smart_trade_bnb: Option<f64>,
        smart_wallet: Option<Address>,
    ) -> Result<ObjectId> {
        let mut document = doc! {
            "timestamp": now_bson(),
            "token": format_address(token.token),
            "name": &token.name,
            "symbol": &token.symbol,
            "entry_price": entry_price,
            "take_profit_price": take_profit_price,
            "stop_loss_price": stop_loss_price,
            "status": "new",
            "simulation": true,
            "price_source": price_source,
            "trade_amount_bnb": trade_amount_bnb,
        };
        if let Some(pair) = pair_address {
            document.insert("pair_address", format_address(pair));
        }
        if let Some(entry_value) = entry_value_usd {
            document.insert("entry_value_usd", entry_value);
        }
        if let Some(value) = smart_volume_usd {
            document.insert("smart_volume_usd", value);
        }
        if let Some(value) = smart_trade_bnb {
            document.insert("smart_trade_bnb", value);
        }
        if let Some(wallet) = smart_wallet {
            document.insert("smart_wallet", format_address(wallet));
        }
        let result = self.simulations.insert_one(document, None).await?;
        result
            .inserted_id
            .as_object_id()
            .ok_or_else(|| anyhow!("missing inserted id"))
    }

    pub async fn find_open_sim_order_for_wallet(
        &self,
        token: Address,
        wallet: Address,
    ) -> Result<Option<ObjectId>> {
        let filter = doc! {
            "token": format_address(token),
            "smart_wallet": format_address(wallet),
            "status": "new",
            "simulation": true,
        };
        Ok(self
            .simulations
            .find_one(filter, None)
            .await?
            .and_then(|doc| doc.get_object_id("_id").ok()))
    }

    pub async fn record_simulation_tx(
        &self,
        order_id: ObjectId,
        token: Address,
        smart_wallet: Option<Address>,
        side: &str,
        price_usd: f64,
        amount_tokens: f64,
        amount_usd: f64,
        tx_hash: Option<&str>,
        block_number: Option<i64>,
        log_index: Option<i64>,
    ) -> Result<SimulationAggregate> {
        let mut tx_doc = doc! {
            "timestamp": now_bson(),
            "simulation_id": order_id,
            "token": format_address(token),
            "side": side,
            "price_usd": price_usd,
            "amount_tokens": amount_tokens,
            "amount_usd": amount_usd,
        };
        if let Some(wallet) = smart_wallet {
            tx_doc.insert("smart_wallet", format_address(wallet));
        }
        if let Some(hash) = tx_hash {
            tx_doc.insert("tx_hash", hash.to_string());
        }
        if let Some(value) = block_number {
            tx_doc.insert("block_number", value);
        }
        if let Some(value) = log_index {
            tx_doc.insert("log_index", value);
        }
        self.simulation_tx.insert_one(tx_doc, None).await?;

        let Some(sim_doc) = self
            .simulations
            .find_one(doc! { "_id": order_id }, None)
            .await?
        else {
            return Err(anyhow!("simulation order missing for tx"));
        };
        let mut agg = SimulationAggregate {
            total_buy_tokens: doc_f64(&sim_doc, "total_buy_tokens").unwrap_or(0.0),
            total_buy_usd: doc_f64(&sim_doc, "total_buy_usd").unwrap_or(0.0),
            total_sell_tokens: doc_f64(&sim_doc, "total_sell_tokens").unwrap_or(0.0),
            total_sell_usd: doc_f64(&sim_doc, "total_sell_usd").unwrap_or(0.0),
            avg_buy_price: doc_f64(&sim_doc, "avg_entry_price").unwrap_or(0.0),
            avg_sell_price: doc_f64(&sim_doc, "avg_exit_price").unwrap_or(0.0),
            profit_usd: doc_f64(&sim_doc, "profit_usd").unwrap_or(0.0),
            buy_count: sim_doc.get_i64("buy_count").unwrap_or(0),
            sell_count: sim_doc.get_i64("sell_count").unwrap_or(0),
        };
        let side_lower = side.to_ascii_lowercase();
        if side_lower == "buy" {
            agg.total_buy_tokens += amount_tokens;
            agg.total_buy_usd += amount_usd;
            agg.buy_count = agg.buy_count.saturating_add(1);
            if agg.total_buy_tokens > 0.0 {
                agg.avg_buy_price = agg.total_buy_usd / agg.total_buy_tokens;
            }
        } else if side_lower == "sell" {
            let avg_cost = if agg.total_buy_tokens > 0.0 {
                agg.total_buy_usd / agg.total_buy_tokens
            } else {
                0.0
            };
            let pnl = if avg_cost > 0.0 {
                amount_usd - avg_cost * amount_tokens
            } else {
                0.0
            };
            agg.total_sell_tokens += amount_tokens;
            agg.total_sell_usd += amount_usd;
            agg.sell_count = agg.sell_count.saturating_add(1);
            agg.profit_usd += pnl;
            if agg.total_sell_tokens > 0.0 {
                agg.avg_sell_price = agg.total_sell_usd / agg.total_sell_tokens;
            }
        }

        let mut update_doc = doc! {
            "updated_at": now_bson(),
            "total_buy_tokens": agg.total_buy_tokens,
            "total_buy_usd": agg.total_buy_usd,
            "total_sell_tokens": agg.total_sell_tokens,
            "total_sell_usd": agg.total_sell_usd,
            "avg_entry_price": agg.avg_buy_price,
            "avg_exit_price": agg.avg_sell_price,
            "profit_usd": agg.profit_usd,
            "buy_count": agg.buy_count,
            "sell_count": agg.sell_count,
        };
        if agg.avg_buy_price > 0.0 {
            update_doc.insert("entry_price", agg.avg_buy_price);
        }
        if agg.avg_sell_price > 0.0 {
            update_doc.insert("exit_price", agg.avg_sell_price);
        }
        if agg.total_buy_usd > 0.0 {
            update_doc.insert("entry_value_usd", agg.total_buy_usd);
        }
        self.simulations
            .update_one(doc! { "_id": order_id }, doc! { "$set": update_doc }, None)
            .await?;

        Ok(agg)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn record_order_tx(
        &self,
        order_id: ObjectId,
        token: Address,
        smart_wallet: Option<Address>,
        side: &str,
        price_usd: f64,
        amount_tokens: f64,
        amount_usd: f64,
        tx_hash: Option<&str>,
        block_number: Option<i64>,
        log_index: Option<i64>,
    ) -> Result<()> {
        let mut tx_doc = doc! {
            "timestamp": now_bson(),
            "order_id": order_id,
            "token": format_address(token),
            "side": side,
            "price_usd": price_usd,
            "amount_tokens": amount_tokens,
            "amount_usd": amount_usd,
        };
        if let Some(wallet) = smart_wallet {
            tx_doc.insert("smart_wallet", format_address(wallet));
        }
        if let Some(hash) = tx_hash {
            tx_doc.insert("tx_hash", hash.to_string());
        }
        if let Some(value) = block_number {
            tx_doc.insert("block_number", value);
        }
        if let Some(value) = log_index {
            tx_doc.insert("log_index", value);
        }
        self.order_tx.insert_one(tx_doc, None).await?;
        Ok(())
    }

    pub async fn complete_sim_order(
        &self,
        order_id: ObjectId,
        status: &str,
        exit_price: Option<f64>,
        profit_usd: Option<f64>,
        exit_tx: Option<&str>,
        smart_wallet: Option<Address>,
    ) -> Result<()> {
        let mut update_doc = doc! {
            "status": status,
            "updated_at": now_bson(),
        };
        if let Some(price) = exit_price {
            update_doc.insert("exit_price", price);
        }
        if let Some(profit) = profit_usd {
            update_doc.insert("profit_usd", profit);
        }
        if let Some(tx) = exit_tx {
            update_doc.insert("exit_tx", tx.to_string());
        }
        if let Some(wallet) = smart_wallet {
            update_doc.insert("smart_wallet", format_address(wallet));
        }
        self.simulations
            .update_one(doc! { "_id": order_id }, doc! { "$set": update_doc }, None)
            .await?;
        Ok(())
    }

    pub async fn complete_real_order(
        &self,
        order_id: ObjectId,
        status: &str,
        exit_price: Option<f64>,
        profit_usd: Option<f64>,
    ) -> Result<()> {
        let mut update_doc = doc! {
            "status": status,
            "updated_at": now_bson(),
        };
        if let Some(price) = exit_price {
            update_doc.insert("exit_price", price);
        }
        if let Some(profit) = profit_usd {
            update_doc.insert("profit_usd", profit);
        }
        self.orders
            .update_one(doc! { "_id": order_id }, doc! { "$set": update_doc }, None)
            .await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_real_order_smart_sell_progress(
        &self,
        order_id: ObjectId,
        sold_token_delta: f64,
        remaining_token_amount: f64,
        sold_pct: f64,
        close_status: Option<&str>,
        exit_price: Option<f64>,
        profit_usd: Option<f64>,
        exit_tx: Option<&str>,
    ) -> Result<()> {
        let mut set_doc = doc! {
            "updated_at": now_bson(),
            "remaining_token_amount": remaining_token_amount.max(0.0),
            "sold_pct": sold_pct.clamp(0.0, 100.0),
        };
        if let Some(status) = close_status {
            set_doc.insert("status", status);
        }
        if let Some(price) = exit_price {
            set_doc.insert("exit_price", price);
            set_doc.insert("last_smart_sell_price", price);
        }
        if let Some(value) = profit_usd {
            set_doc.insert("profit_usd", value);
        }
        if let Some(hash) = exit_tx {
            set_doc.insert("last_smart_sell_tx", hash.to_string());
            if close_status.is_some() {
                set_doc.insert("exit_tx", hash.to_string());
            }
        }
        let mut update_doc = doc! { "$set": set_doc };
        if sold_token_delta > 0.0 {
            update_doc.insert(
                "$inc",
                doc! {
                    "sold_token_amount": sold_token_delta,
                },
            );
        }
        self.orders
            .update_one(doc! { "_id": order_id }, update_doc, None)
            .await?;
        Ok(())
    }

    pub async fn find_real_order(&self, order_id: ObjectId) -> Result<Option<Document>> {
        Ok(self.orders.find_one(doc! { "_id": order_id }, None).await?)
    }

    pub async fn find_sim_order(&self, order_id: ObjectId) -> Result<Option<Document>> {
        Ok(self
            .simulations
            .find_one(doc! { "_id": order_id }, None)
            .await?)
    }

    pub async fn find_open_real_orders(&self) -> Result<Vec<Document>> {
        let mut cursor = self.orders.find(doc! { "status": "new" }, None).await?;
        let mut results = Vec::new();
        while let Some(doc) = cursor.try_next().await? {
            results.push(doc);
        }
        Ok(results)
    }

    pub async fn count_open_real_orders(&self, alert_type: Option<&str>) -> Result<u64> {
        let mut filter = doc! {
            "status": "new",
            "simulation": false,
        };
        if let Some(value) = alert_type {
            filter.insert("alert_type", value);
        }
        Ok(self.orders.count_documents(filter, None).await?)
    }

    pub async fn find_open_sim_orders(&self) -> Result<Vec<Document>> {
        let mut cursor = self
            .simulations
            .find(doc! { "status": "new" }, None)
            .await?;
        let mut results = Vec::new();
        while let Some(doc) = cursor.try_next().await? {
            results.push(doc);
        }
        Ok(results)
    }

    pub async fn list_sim_orders(&self, limit: Option<i64>) -> Result<Vec<Document>> {
        let options = FindOptions::builder()
            .sort(doc! { "timestamp": -1 })
            .limit(limit)
            .build();
        let mut cursor = self.simulations.find(doc! {}, options).await?;
        let mut results = Vec::new();
        while let Some(doc) = cursor.try_next().await? {
            results.push(doc);
        }
        Ok(results)
    }

    pub async fn list_simulation_txs_for_ids(&self, ids: &[ObjectId]) -> Result<Vec<Document>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let filter = doc! { "simulation_id": { "$in": ids } };
        let mut cursor = self.simulation_tx.find(filter, None).await?;
        let mut results = Vec::new();
        while let Some(doc) = cursor.try_next().await? {
            results.push(doc);
        }
        Ok(results)
    }

    pub async fn list_live_orders(&self, limit: Option<i64>) -> Result<Vec<Document>> {
        let options = FindOptions::builder()
            .sort(doc! { "timestamp": -1 })
            .limit(limit)
            .build();
        let filter = doc! { "simulation": false };
        let mut cursor = self.orders.find(filter, options).await?;
        let mut results = Vec::new();
        while let Some(doc) = cursor.try_next().await? {
            results.push(doc);
        }
        Ok(results)
    }

    pub async fn list_smart_money(&self, limit: Option<i64>) -> Result<Vec<Document>> {
        let options = FindOptions::builder()
            .sort(doc! { "updated_at": -1 })
            .limit(limit)
            .build();
        let filter = doc! {
            "$or": [
                { "active": true },
                { "active": { "$exists": false } },
            ]
        };
        let mut cursor = self.smart_money.find(filter, options).await?;
        let mut results = Vec::new();
        while let Some(doc) = cursor.try_next().await? {
            results.push(doc);
        }
        Ok(results)
    }

    pub async fn list_smart_money_wallets(&self, live_only: bool) -> Result<Vec<Address>> {
        let mut filter = doc! {
            "$or": [
                { "active": true },
                { "active": { "$exists": false } },
            ]
        };
        if live_only {
            filter.insert("live", true);
        }
        let options = FindOptions::builder()
            .projection(doc! { "wallet": 1 })
            .sort(doc! { "updated_at": -1 })
            .build();
        let mut cursor = self.smart_money.find(filter, options).await?;
        let mut results = Vec::new();
        while let Some(doc) = cursor.try_next().await? {
            let wallet = match doc.get_str("wallet") {
                Ok(value) => value,
                Err(_) => continue,
            };
            let addr = match Address::from_str(wallet) {
                Ok(value) => value,
                Err(_) => continue,
            };
            results.push(addr);
        }
        Ok(results)
    }

    pub async fn list_smart_money_names(&self) -> Result<HashMap<String, String>> {
        let filter = doc! {
            "$or": [
                { "active": true },
                { "active": { "$exists": false } },
            ]
        };
        let options = FindOptions::builder()
            .projection(doc! { "wallet": 1, "name": 1 })
            .build();
        let mut cursor = self.smart_money.find(filter, options).await?;
        let mut results = HashMap::new();
        while let Some(doc) = cursor.try_next().await? {
            let wallet = match doc.get_str("wallet") {
                Ok(value) => value,
                Err(_) => continue,
            };
            let name = doc.get_str("name").unwrap_or("");
            results.insert(wallet.to_string(), name.to_string());
        }
        Ok(results)
    }

    pub async fn list_smart_money_live_wallets(&self) -> Result<Vec<String>> {
        let filter = doc! {
            "live": true,
            "$or": [
                { "active": true },
                { "active": { "$exists": false } },
            ]
        };
        let options = FindOptions::builder()
            .projection(doc! { "wallet": 1 })
            .sort(doc! { "updated_at": -1 })
            .build();
        let mut cursor = self.smart_money.find(filter, options).await?;
        let mut results = Vec::new();
        while let Some(doc) = cursor.try_next().await? {
            if let Ok(wallet) = doc.get_str("wallet") {
                results.push(wallet.to_string());
            }
        }
        Ok(results)
    }

    pub async fn smart_money_name(&self, wallet: Address) -> Result<Option<String>> {
        let filter = doc! {
            "wallet": format_address(wallet),
            "$or": [
                { "active": true },
                { "active": { "$exists": false } },
            ]
        };
        let Some(doc) = self.smart_money.find_one(filter, None).await? else {
            return Ok(None);
        };
        let name = doc.get_str("name").unwrap_or("").trim().to_string();
        if name.is_empty() {
            Ok(None)
        } else {
            Ok(Some(name))
        }
    }

    pub async fn list_smart_money_by_wallets(&self, wallets: &[String]) -> Result<Vec<Document>> {
        if wallets.is_empty() {
            return Ok(Vec::new());
        }
        let filter = doc! { "wallet": { "$in": wallets } };
        let mut cursor = self.smart_money.find(filter, None).await?;
        let mut results = Vec::new();
        while let Some(doc) = cursor.try_next().await? {
            results.push(doc);
        }
        Ok(results)
    }

    pub async fn upsert_smart_money_record(
        &self,
        wallet: Address,
        name: Option<&str>,
        balance: Option<f64>,
        follow_count: Option<i64>,
        txs_7d: Option<i64>,
        winrate_7d: Option<f64>,
        pnl_7d: Option<f64>,
        avg_cost_7d: Option<f64>,
        realized_profit_30d: Option<f64>,
        pnl_30d: Option<f64>,
        active: bool,
    ) -> Result<()> {
        let mut update = doc! {
            "wallet": format_address(wallet),
            "updated_at": now_bson(),
            "active": active,
        };
        update.insert("name", name.unwrap_or(""));
        if let Some(value) = balance {
            update.insert("balance", value);
        }
        if let Some(value) = follow_count {
            update.insert("follow_count", value);
        }
        if let Some(value) = txs_7d {
            update.insert("txs_7d", value);
        }
        if let Some(value) = winrate_7d {
            update.insert("winrate_7d", value);
        }
        if let Some(value) = pnl_7d {
            update.insert("pnl_7d", value);
        }
        if let Some(value) = avg_cost_7d {
            update.insert("avg_cost_7d", value);
        }
        if let Some(value) = realized_profit_30d {
            update.insert("realized_profit_30d", value);
        }
        if let Some(value) = pnl_30d {
            update.insert("pnl_30d", value);
        }
        self.smart_money
            .update_one(
                doc! { "wallet": format_address(wallet) },
                doc! { "$set": update },
                UpdateOptions::builder().upsert(true).build(),
            )
            .await?;
        Ok(())
    }

    pub async fn set_all_smart_money_inactive(&self) -> Result<()> {
        self.smart_money
            .update_many(doc! {}, doc! { "$set": { "active": false } }, None)
            .await?;
        Ok(())
    }

    pub async fn clear_non_live_smart_money(&self) -> Result<u64> {
        let filter = doc! {
            "$or": [
                { "live": { "$ne": true } },
                { "live": { "$exists": false } },
            ]
        };
        let result = self.smart_money.delete_many(filter, None).await?;
        Ok(result.deleted_count)
    }

    pub async fn set_smart_money_live_wallets(
        &self,
        wallets: &[String],
        reset: bool,
    ) -> Result<()> {
        if reset {
            self.smart_money
                .update_many(doc! {}, doc! { "$set": { "live": false } }, None)
                .await?;
        }
        if wallets.is_empty() {
            return Ok(());
        }
        self.smart_money
            .update_many(
                doc! { "wallet": { "$in": wallets } },
                doc! { "$set": { "live": true } },
                None,
            )
            .await?;
        Ok(())
    }

    pub async fn clear_smart_money(&self) -> Result<u64> {
        let result = self.smart_money.delete_many(doc! {}, None).await?;
        Ok(result.deleted_count)
    }

    pub async fn has_sim_order(&self, token: Address) -> Result<bool> {
        let filter = doc! {
            "token": format_address(token),
            "status": "new",
            "simulation": true,
        };
        Ok(self.simulations.find_one(filter, None).await?.is_some())
    }

    pub async fn has_sim_order_for_wallet(&self, token: Address, wallet: Address) -> Result<bool> {
        let filter = doc! {
            "token": format_address(token),
            "smart_wallet": format_address(wallet),
            "status": "new",
            "simulation": true,
        };
        Ok(self.simulations.find_one(filter, None).await?.is_some())
    }

    pub async fn count_open_sim_orders(&self) -> Result<u64> {
        let filter = doc! {
            "status": "new",
            "simulation": true,
        };
        Ok(self.simulations.count_documents(filter, None).await?)
    }

    pub async fn has_any_open_order_or_sim_for_token(&self, token: Address) -> Result<bool> {
        let token_value = format_address(token);
        let filter = doc! { "token": token_value, "status": "new" };
        if self.orders.find_one(filter.clone(), None).await?.is_some() {
            return Ok(true);
        }
        Ok(self.simulations.find_one(filter, None).await?.is_some())
    }

    pub async fn close_all_open_sim_orders(&self) -> Result<u64> {
        let filter = doc! {
            "status": "new",
            "simulation": true,
        };
        let update = doc! {
            "$set": {
                "status": "closed",
                "completed_at": now_bson(),
            }
        };
        let result = self.simulations.update_many(filter, update, None).await?;
        Ok(result.modified_count)
    }

    pub async fn close_sim_orders_for_token(&self, token: Address) -> Result<Vec<Document>> {
        let filter = doc! {
            "token": format_address(token),
            "status": "new",
            "simulation": true,
        };
        let mut cursor = self.simulations.find(filter, None).await?;
        let mut docs = Vec::new();
        while let Some(doc) = cursor.try_next().await? {
            docs.push(doc);
        }
        Ok(docs)
    }

    pub async fn clear_simulations(&self) -> Result<u64> {
        let result = self.simulations.delete_many(doc! {}, None).await?;
        Ok(result.deleted_count)
    }

    pub async fn clear_simulation_txs(&self) -> Result<u64> {
        let result = self.simulation_tx.delete_many(doc! {}, None).await?;
        Ok(result.deleted_count)
    }

    pub async fn delete_old_tokens_and_pairs(
        &self,
        tokens_retention_days: u64,
        pairs_retention_days: u64,
    ) -> Result<(u64, u64)> {
        let tokens_deleted = if tokens_retention_days > 0 {
            let cutoff = cutoff_bson(tokens_retention_days);
            let filter = doc! {
                "$or": [
                    { "timestamp": { "$lt": cutoff } },
                    { "tx_value_timestamp": { "$lt": cutoff } },
                ]
            };
            self.tokens.delete_many(filter, None).await?.deleted_count
        } else {
            0
        };

        let pairs_deleted = if pairs_retention_days > 0 {
            let cutoff = cutoff_bson(pairs_retention_days);
            let filter = doc! { "timestamp": { "$lt": cutoff } };
            self.pairs.delete_many(filter, None).await?.deleted_count
        } else {
            0
        };

        Ok((tokens_deleted, pairs_deleted))
    }
}

fn now_bson() -> Bson {
    let malaysia_now = Utc::now() + Duration::seconds(MALAYSIA_OFFSET_SECS);
    Bson::DateTime(BsonDateTime::from_millis(malaysia_now.timestamp_millis()))
}

fn cutoff_bson(retention_days: u64) -> BsonDateTime {
    let malaysia_now = Utc::now() + Duration::seconds(MALAYSIA_OFFSET_SECS);
    let cutoff = malaysia_now - Duration::days(retention_days as i64);
    BsonDateTime::from_millis(cutoff.timestamp_millis())
}

fn format_address(value: Address) -> String {
    format!("{:#x}", value)
}

fn format_hash(value: H256) -> String {
    format!("{:#x}", value)
}

fn doc_f64(doc: &Document, key: &str) -> Option<f64> {
    doc.get_f64(key)
        .ok()
        .or_else(|| doc.get_i64(key).ok().map(|v| v as f64))
}
