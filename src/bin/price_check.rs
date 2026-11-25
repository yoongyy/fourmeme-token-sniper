use ethers::types::Address;
use sniper_fourmeme::{
    config::Config,
    price::{build_price_context, debug_reserves, read_pair_price},
    rpc_fallback::{build_http_provider, rpc_urls_from_env},
    token_manager,
};
use std::{str::FromStr, sync::Arc};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let cfg = Config::from_env();
    let mut args = std::env::args().skip(1);
    let token_arg = args
        .next()
        .unwrap_or_else(|| panic!("Usage: cargo run --bin price-check -- <token> [pair]"));
    let token = Address::from_str(&token_arg)
        .map_err(|_| anyhow::anyhow!("Invalid token address: {}", token_arg))?;
    let rpc_urls = rpc_urls_from_env(cfg.rpc_url.clone(), &["RPC_URL2", "RPC_URL3"]);
    let provider = Arc::new(build_http_provider(
        rpc_urls,
        std::time::Duration::from_millis(0),
    )?);

    if let Some(pair_arg) = args.next() {
        let pair = Address::from_str(&pair_arg)
            .map_err(|_| anyhow::anyhow!("Invalid pair address: {}", pair_arg))?;
        let context = build_price_context(provider.clone(), pair, token).await?;
        let raw_price = read_pair_price(&provider, &context).await?;
        let reserves = debug_reserves(&provider, &context).await?;
        println!(
            "[PairDebug] token_decimals={} quote_decimals={} raw_price={:.12} reserve_token={} reserve_quote={}",
            context.token_decimals(),
            context.quote_decimals(),
            raw_price,
            reserves.base,
            reserves.quote
        );
        let price =
            token_manager::convert_price_to_usd(&provider, raw_price, context.quote_address())
                .await?;
        println!(
            "[PriceCheck] token={:#x} pair={:#x} price={:.10}",
            token, pair, price
        );
    } else {
        let helper_info =
            token_manager::fetch_helper_info(provider.clone(), cfg.helper3_address, token).await?;
        if let Some(info) = helper_info.clone() {
            println!(
                "[Helper] quote={:#x} liquidity_added={} pancake_pair={} last_price_bnb={} last_price_usd={} pancake_factory={:#x} pancake_v3_factory={:#x}",
                info.quote,
                info.liquidity_added,
                info.pancake_pair
                    .map(|addr| format!("{addr:#x}"))
                    .unwrap_or_else(|| "none".into()),
                info.last_price_bnb
                    .map(|v| format!("{:.10}", v))
                    .unwrap_or_else(|| "none".into()),
                info.last_price_usd
                    .map(|v| format!("{:.10}", v))
                    .unwrap_or_else(|| "none".into()),
                info.pancake_factory,
                info.pancake_v3_factory,
            );
            println!(
                "[Helper:getTokenInfo] version={} tokenManager={:#x} quote={:#x} lastPrice={} tradingFeeRate={} minTradingFee={} launchTime={} offers={} maxOffers={} funds={} maxFunds={}",
                info.details.version,
                info.details.token_manager,
                info.details.quote,
                info.details.last_price,
                info.details.trading_fee_rate,
                info.details.min_trading_fee,
                info.details.launch_time,
                info.details.offers,
                info.details.max_offers,
                info.details.funds,
                info.details.max_funds
            );
            // Prefer V2 factory pair, then helper-reported pair (may be V3), then V3 factory.
            let mut pair = token_manager::derive_pancake_pair(
                provider.clone(),
                info.pancake_factory,
                token,
                info.quote,
            )
            .await
            .ok()
            .flatten();
            if pair.is_none() {
                pair = info.pancake_pair;
            }
            if pair.is_none() {
                pair = token_manager::derive_pancake_pair(
                    provider.clone(),
                    info.pancake_v3_factory,
                    token,
                    info.quote,
                )
                .await
                .ok()
                .flatten();
            }
            if let Some(found_pair) = pair {
                let context = build_price_context(provider.clone(), found_pair, token).await?;
                let raw_price = read_pair_price(&provider, &context).await?;
                let reserves = debug_reserves(&provider, &context).await?;
                println!(
                    "[PairDebug] token_decimals={} quote_decimals={} raw_price={:.12} reserve_token={} reserve_quote={}",
                    context.token_decimals(),
                    context.quote_decimals(),
                    raw_price,
                    reserves.base,
                    reserves.quote
                );
                let price = token_manager::convert_price_to_usd(
                    &provider,
                    raw_price,
                    context.quote_address(),
                )
                .await?;
                println!(
                    "[PriceCheck] token={:#x} pair={:#x} price={:.10}",
                    token, found_pair, price
                );
                return Ok(());
            }
            if let Some(price) = info.last_price_usd {
                println!(
                    "[PriceCheck] token={:#x} helper={:#x} last_price={:.10}",
                    token, cfg.helper3_address, price
                );
                return Ok(());
            }
        } else {
            println!("[Helper] no metadata available");
        }
        match token_manager::fetch_last_price(provider.clone(), cfg.token_manager_address, token)
            .await?
        {
            Some(price) => println!(
                "[PriceCheck] token={:#x} manager={:#x} last_price={:.10}",
                token, cfg.token_manager_address, price
            ),
            None => println!(
                "[PriceCheck] token={:#x} price unavailable via TokenManager",
                token
            ),
        }
    }
    Ok(())
}
