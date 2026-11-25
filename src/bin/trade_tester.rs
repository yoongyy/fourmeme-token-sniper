use anyhow::{Context, Result, anyhow};
use ethers::{
    abi::{Abi, AbiParser},
    contract::Contract,
    middleware::SignerMiddleware,
    prelude::*,
    signers::{LocalWallet, Signer},
    types::{Address, U256},
    utils::keccak256,
    utils::parse_units,
};
use once_cell::sync::Lazy;
use serde_json::from_slice;
use sniper_fourmeme::{
    config::Config,
    rpc_fallback::{RpcProvider, build_http_provider, rpc_urls_from_env},
    token_manager,
};
use std::{
    env,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

type Client = SignerMiddleware<Arc<RpcProvider>, LocalWallet>;
static WBNB_ADDRESS: Lazy<Address> = Lazy::new(|| {
    Address::from_str("0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c").expect("Invalid WBNB address")
});

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let mut args = env::args().skip(1);
    let action = args.next().context(
        "Usage: cargo run --bin trade_tester -- <buy|sell|sell-tm|sell-amount> <token_address> [args]",
    )?;
    let token_arg = args.next().context(
        "Usage: cargo run --bin trade_tester -- <buy|sell|sell-tm|sell-amount> <token_address> [args]",
    )?;
    let token = Address::from_str(&token_arg).context("token address is invalid")?;
    let remaining: Vec<String> = args.collect();

    let cfg = Config::from_env();
    let private_key = cfg
        .private_key
        .clone()
        .context("PRIVATE_KEY missing from environment")?;
    let wallet = private_key
        .parse::<LocalWallet>()
        .context("PRIVATE_KEY is invalid")?
        .with_chain_id(56u64);
    let wallet_address = wallet.address();
    let rpc_urls = rpc_urls_from_env(cfg.rpc_url.clone(), &["RPC_URL2", "RPC_URL3"]);
    let provider = Arc::new(
        build_http_provider(rpc_urls, Duration::from_millis(1500)).context("RPC_URL is invalid")?,
    );
    let client = Arc::new(SignerMiddleware::new(provider.clone(), wallet));
    let abi: Abi = from_slice(include_bytes!("../abi/TokenManager2.lite.abi"))
        .context("failed to parse TokenManager ABI")?;
    let contract = Contract::new(cfg.token_manager_address, abi, client.clone());

    match action.as_str() {
        "buy" => {
            let amount_override = remaining
                .get(0)
                .map(|arg| {
                    arg.parse::<f64>()
                        .context("failed to parse amount argument")
                })
                .transpose()?;
            let default_amt = cfg.liquidity_trade().trade_amount_bnb();
            let buy_amount = amount_override.unwrap_or(default_amt);
            run_buy(
                &contract,
                cfg.helper3_address,
                cfg.pancake_v2_router,
                cfg.pancake_v3_router,
                provider.clone(),
                wallet_address,
                token,
                buy_amount,
            )
            .await?;
        }
        "sell" => {
            let origin_override = remaining
                .get(0)
                .map(|arg| arg.parse::<u64>().context("origin must be a number"))
                .transpose()?;
            let min_funds_override = remaining
                .get(1)
                .map(|arg| arg.parse::<f64>().context("min_funds must be a number"))
                .transpose()?;
            let origin = origin_override.map(U256::from).unwrap_or(U256::zero());
            let min_funds = match min_funds_override {
                Some(value) => {
                    let min_funds_str = format!("{:.18}", value);
                    parse_units(min_funds_str, 18)
                        .context("failed to convert min_funds to wei")?
                        .into()
                }
                None => U256::zero(),
            };
            run_sell(
                &contract,
                provider.clone(),
                wallet_address,
                token,
                cfg.helper3_address,
                cfg.pancake_v2_router,
                cfg.pancake_v3_router,
                origin,
                min_funds,
                None,
                false,
            )
            .await?;
        }
        "sell-amount" => {
            let amount_arg = remaining.get(0).ok_or_else(|| {
                anyhow!(
                    "Usage: cargo run --bin trade_tester -- sell-amount <token_address> <amount_tokens> [min_funds]"
                )
            })?;
            let min_funds_override = remaining
                .get(1)
                .map(|arg| arg.parse::<f64>().context("min_funds must be a number"))
                .transpose()?;
            let min_funds = match min_funds_override {
                Some(value) => {
                    let min_funds_str = format!("{:.18}", value);
                    parse_units(min_funds_str, 18)
                        .context("failed to convert min_funds to wei")?
                        .into()
                }
                None => U256::zero(),
            };
            run_sell(
                &contract,
                provider.clone(),
                wallet_address,
                token,
                cfg.helper3_address,
                cfg.pancake_v2_router,
                cfg.pancake_v3_router,
                U256::zero(),
                min_funds,
                Some(amount_arg.to_string()),
                false,
            )
            .await?;
        }
        "sell-tm" => {
            run_sell(
                &contract,
                provider.clone(),
                wallet_address,
                token,
                cfg.helper3_address,
                cfg.pancake_v2_router,
                cfg.pancake_v3_router,
                U256::zero(),
                U256::zero(),
                None,
                true,
            )
            .await?;
        }
        other => {
            return Err(anyhow!(
                "Unsupported action '{other}'. Use 'buy', 'sell', 'sell-amount', or 'sell-tm'."
            ));
        }
    }

    Ok(())
}

async fn run_buy(
    contract: &Contract<Client>, // <- take by ref (avoid move)
    helper: Address,
    pancake_v2_router: Address,
    pancake_v3_router: Address,
    provider: Arc<RpcProvider>,
    wallet: Address,
    token: Address,
    buy_amount_bnb: f64,
) -> Result<()> {
    if buy_amount_bnb <= 0.0 {
        return Err(anyhow!("buy amount must be greater than zero"));
    }

    let before = token_manager::fetch_token_balance(provider.clone(), token, wallet)
        .await
        .unwrap_or_else(|_| U256::zero());

    let decimals = token_manager::fetch_token_decimals(provider.clone(), token)
        .await
        .unwrap_or(18);

    // Convert BNB -> wei (18 decimals)
    let buy_amount_str = format!("{:.18}", buy_amount_bnb);
    let funds: U256 = parse_units(buy_amount_str, 18)
        .context("failed to convert buy amount to wei")?
        .into();

    if !helper.is_zero() && (!pancake_v2_router.is_zero() || !pancake_v3_router.is_zero()) {
        let helper_abi: Abi = from_slice(include_bytes!("../abi/TokenManagerHelper3.abi"))
            .context("failed to parse TokenManagerHelper ABI")?;
        let helper3 = Contract::new(helper, helper_abi, contract.client().clone());
        let wbnb: Address = helper3
            .method("WETH", ())
            .context("build helper WETH call")?
            .call()
            .await
            .unwrap_or_else(|err| {
                eprintln!("[trade-tester] failed to read WETH: {err:?}");
                Address::zero()
            });
        let wbnb = if wbnb.is_zero() { *WBNB_ADDRESS } else { wbnb };

        if !pancake_v2_router.is_zero() {
            let mut pair = helper3
                .method::<_, Address>("getPancakePair", token)
                .context("build helper getPancakePair call")?
                .call()
                .await
                .unwrap_or_else(|err| {
                    eprintln!(
                        "[trade-tester] failed to read helper pancake pair for {:#x}: {err:?}",
                        token
                    );
                    Address::zero()
                });
            if pair.is_zero() {
                let factory: Address = helper3
                    .method("PANCAKE_FACTORY", ())
                    .context("build helper PANCAKE_FACTORY call")?
                    .call()
                    .await
                    .unwrap_or_else(|err| {
                        eprintln!("[trade-tester] failed to read PANCAKE_FACTORY: {err:?}");
                        Address::zero()
                    });
                if !factory.is_zero() {
                    let factory_abi = AbiParser::default()
                        .parse(&["function getPair(address,address) view returns (address)"])
                        .context("failed to parse Pancake factory ABI")?;
                    let factory_contract =
                        Contract::new(factory, factory_abi, helper3.client().clone());
                    pair = factory_contract
                        .method("getPair", (token, wbnb))
                        .context("build getPair call")?
                        .call()
                        .await
                        .unwrap_or_else(|err| {
                            eprintln!(
                                "[trade-tester] failed to read Pancake pair for {:#x}: {err:?}",
                                token
                            );
                            Address::zero()
                        });
                }
            }
            if !pair.is_zero() && pair_exists(provider.clone(), pair).await {
                println!("[trade-tester] Pancake V2 pair detected, routing buy via router.");
                return pancake_v2_buy_for_eth(
                    pancake_v2_router,
                    contract.client().clone(),
                    wallet,
                    token,
                    funds,
                    U256::zero(),
                    wbnb,
                )
                .await;
            }
        }

        if !pancake_v3_router.is_zero() {
            let v3_factory: Address = helper3
                .method("PANCAKE_V3_FACTORY", ())
                .context("build helper PANCAKE_V3_FACTORY call")?
                .call()
                .await
                .unwrap_or_else(|err| {
                    eprintln!("[trade-tester] failed to read PANCAKE_V3_FACTORY: {err:?}");
                    Address::zero()
                });
            if !v3_factory.is_zero() {
                if let Some(fee) =
                    find_v3_fee_tier(v3_factory, contract.client().clone(), token, wbnb).await
                {
                    println!("[trade-tester] Pancake V3 pool detected, routing buy via router.");
                    return pancake_v3_buy_for_eth(
                        pancake_v3_router,
                        contract.client().clone(),
                        wallet,
                        token,
                        funds,
                        U256::zero(),
                        wbnb,
                        fee,
                    )
                    .await;
                }
            }
        }
    }

    // Default (no slippage protection)
    let mut min_amount: U256 = U256::zero();

    // If you can estimate, set min_amount with slippage protection
    // (e.g. 3% slippage => receive at least 97% of estimate)
    const SLIPPAGE_BPS: u64 = 300; // 300 = 3.00%
    if let Ok(Some(estimate_raw)) =
        token_manager::try_buy_estimate(provider.clone(), helper, token, funds).await
    {
        if let Some(estimate) = token_manager::format_estimated_tokens(estimate_raw, decimals) {
            println!(
                "[trade-tester] Estimated {:.6} tokens for {:.6} BNB",
                estimate, buy_amount_bnb
            );
        }

        // min_amount = estimate_raw * (10000 - slippage_bps) / 10000
        let keep_bps = U256::from(10_000u64 - SLIPPAGE_BPS);
        min_amount =
            estimate_raw.checked_mul(keep_bps).unwrap_or(U256::zero()) / U256::from(10_000u64);
    }

    // --- IMPORTANT: call overloaded function by selector ---
    // selector = keccak256("buyTokenAMAP(address,uint256,uint256)")[0..4]
    let hash = keccak256("buyTokenAMAP(address,uint256,uint256)".as_bytes());
    let selector: [u8; 4] = hash[0..4].try_into().unwrap();

    println!(
        "[trade-tester] Buying token {:#x} with {:.6} BNB... (funds={} wei, min_amount={})",
        token, buy_amount_bnb, funds, min_amount
    );

    let call = contract
        .method_hash::<_, ()>(selector, (token, funds, min_amount))
        .context("failed to build buyTokenAMAP(address,uint256,uint256) call")?
        .value(funds); // payable msg.value

    let pending = call.send().await.context("transaction submission failed")?;
    let tx_hash = pending.tx_hash();
    println!("[trade-tester] buy tx submitted: {tx_hash:?}, waiting...");

    let receipt = pending
        .await
        .context("buy transaction dropped before confirmation")?
        .context("buy transaction receipt missing")?;

    if receipt.status != Some(U64::from(1)) {
        return Err(anyhow!("buy transaction reverted on-chain"));
    }

    let after = token_manager::fetch_token_balance(provider.clone(), token, wallet)
        .await
        .unwrap_or(before);

    let acquired = after.saturating_sub(before);
    let acquired_tokens = token_manager::u256_to_f64(acquired, decimals).unwrap_or_default();

    println!(
        "[trade-tester] Buy confirmed in tx {:?}. Received {:.6} tokens (raw {}).",
        tx_hash, acquired_tokens, acquired
    );

    Ok(())
}

async fn pair_exists(provider: Arc<RpcProvider>, pair: Address) -> bool {
    if pair.is_zero() {
        return false;
    }
    match provider.get_code(pair, None).await {
        Ok(code) => !code.0.is_empty(),
        Err(err) => {
            eprintln!(
                "[trade-tester] failed to read pair code {:#x}: {err:?}",
                pair
            );
            false
        }
    }
}

abigen!(
    ERC20,
    r#"[
        function allowance(address owner, address spender) view returns (uint256)
        function approve(address spender, uint256 value) returns (bool)
    ]"#
);

async fn run_sell(
    contract: &Contract<Client>,
    provider: Arc<RpcProvider>,
    wallet: Address,
    token: Address,
    helper_address: Address,
    pancake_v2_router: Address,
    pancake_v3_router: Address,
    _origin: U256,
    min_funds: U256,
    amount_override: Option<String>,
    force_token_manager: bool,
) -> Result<()> {
    let balance = token_manager::fetch_token_balance(provider.clone(), token, wallet)
        .await
        .context("failed to read wallet balance")?;

    if balance.is_zero() {
        return Err(anyhow!(
            "wallet does not hold token {:#x}, nothing to sell",
            token
        ));
    }

    let decimals = token_manager::fetch_token_decimals(provider.clone(), token)
        .await
        .unwrap_or(18);
    let display = token_manager::u256_to_f64(balance, decimals).unwrap_or_default();
    let sell_amount = if let Some(raw) = amount_override {
        let amount: U256 = parse_units(raw, u32::from(decimals))
            .context("failed to parse amount using token decimals")?
            .into();
        if amount.is_zero() {
            return Err(anyhow!("sell amount must be greater than zero"));
        }
        if amount > balance {
            return Err(anyhow!(
                "sell amount exceeds wallet balance: amount={} balance={}",
                amount,
                balance
            ));
        }
        amount
    } else {
        balance
    };

    // Build ERC20 helper using same signer client as TokenManager contract
    let client = contract.client(); // Arc<Client>
    let erc20 = ERC20::new(token, client.clone());

    if !force_token_manager
        && !helper_address.is_zero()
        && (!pancake_v2_router.is_zero() || !pancake_v3_router.is_zero())
    {
        let helper_abi: Abi = from_slice(include_bytes!("../abi/TokenManagerHelper3.abi"))
            .context("failed to parse TokenManagerHelper ABI")?;
        let helper3 = Contract::new(helper_address, helper_abi, client.clone());
        let helper_info =
            match helper3.method::<_, token_manager::HelperTokenInfo>("getTokenInfo", token) {
                Ok(builder) => match builder.call().await {
                    Ok(info) => Some(info),
                    Err(err) => {
                        eprintln!(
                            "[trade-tester] helper getTokenInfo failed for {:#x}: {err:?}",
                            token
                        );
                        None
                    }
                },
                Err(err) => {
                    eprintln!(
                        "[trade-tester] helper getTokenInfo build failed for {:#x}: {err:?}",
                        token
                    );
                    None
                }
            };
        if let Some(helper_info) = helper_info {
            if helper_info.liquidity_added {
                let factory: Address = helper3
                    .method("PANCAKE_FACTORY", ())
                    .context("build helper PANCAKE_FACTORY call")?
                    .call()
                    .await
                    .unwrap_or_else(|err| {
                        eprintln!("[trade-tester] failed to read PANCAKE_FACTORY: {err:?}");
                        Address::zero()
                    });
                let wbnb: Address = helper3
                    .method("WETH", ())
                    .context("build helper WETH call")?
                    .call()
                    .await
                    .unwrap_or_else(|err| {
                        eprintln!("[trade-tester] failed to read WETH: {err:?}");
                        Address::zero()
                    });
                let wbnb = if wbnb.is_zero() { *WBNB_ADDRESS } else { wbnb };
                if !factory.is_zero() && !pancake_v2_router.is_zero() {
                    let factory_abi = AbiParser::default()
                        .parse(&["function getPair(address,address) view returns (address)"])
                        .context("failed to parse Pancake factory ABI")?;
                    let factory_contract = Contract::new(factory, factory_abi, client.clone());
                    let pair: Address = factory_contract
                        .method("getPair", (token, wbnb))
                        .context("build getPair call")?
                        .call()
                        .await
                        .unwrap_or_else(|err| {
                            eprintln!(
                                "[trade-tester] failed to read Pancake pair for {:#x}: {err:?}",
                                token
                            );
                            Address::zero()
                        });
                    if !pair.is_zero() {
                        println!(
                            "[trade-tester] Migrated token detected with V2 pair. Using Pancake V2 router sell."
                        );
                        return pancake_v2_sell_for_eth(
                            pancake_v2_router,
                            client.clone(),
                            wallet,
                            token,
                            sell_amount,
                            min_funds,
                            wbnb,
                        )
                        .await;
                    }
                }
                let v3_factory: Address = helper3
                    .method("PANCAKE_V3_FACTORY", ())
                    .context("build helper PANCAKE_V3_FACTORY call")?
                    .call()
                    .await
                    .unwrap_or_else(|err| {
                        eprintln!("[trade-tester] failed to read PANCAKE_V3_FACTORY: {err:?}");
                        Address::zero()
                    });
                if !v3_factory.is_zero() && !pancake_v3_router.is_zero() {
                    if let Some(fee) =
                        find_v3_fee_tier(v3_factory, client.clone(), token, wbnb).await
                    {
                        println!(
                            "[trade-tester] Migrated token detected with V3 pool. Using Pancake V3 router sell."
                        );
                        return pancake_v3_sell_for_eth(
                            pancake_v3_router,
                            client.clone(),
                            wallet,
                            token,
                            sell_amount,
                            min_funds,
                            wbnb,
                            fee,
                        )
                        .await;
                    }
                }
            }
        }
    }

    // ✅ Spender is almost always the TokenManager contract itself
    let spender: Address = contract.address();

    // 1) check allowance
    let allowance = erc20
        .allowance(wallet, spender)
        .call()
        .await
        .context("failed to read allowance")?;

    // 2) approve if needed
    if allowance < sell_amount {
        println!(
            "[trade-tester] Allowance {} < amount {}. Approving spender {:#x}...",
            allowance, sell_amount, spender
        );

        // approve max so you don't need to approve again next time
        let approve_call = erc20.approve(spender, U256::MAX);

        let approve_pending = approve_call
            .send()
            .await
            .context("approve submission failed")?;

        let approve_hash = approve_pending.tx_hash();
        println!("[trade-tester] approve tx submitted: {approve_hash:?}, waiting...");

        let approve_receipt = approve_pending
            .await
            .context("approve tx dropped before confirmation")?
            .context("approve tx receipt missing")?;

        if approve_receipt.status != Some(U64::from(1)) {
            return Err(anyhow!("approve transaction reverted on-chain"));
        }

        println!("[trade-tester] Approve confirmed in tx {:?}", approve_hash);
    }

    // 3) now sell
    let min_out: U256 = U256::zero();

    let hash = keccak256("sellToken(address,uint256,uint256)".as_bytes());
    let selector: [u8; 4] = hash[0..4].try_into().unwrap();

    if sell_amount == balance {
        println!(
            "[trade-tester] Selling entire balance of {} tokens (raw {}) for {:#x}",
            display, balance, token
        );
    } else if let Some(amount_display) = token_manager::u256_to_f64(sell_amount, decimals).ok() {
        println!(
            "[trade-tester] Selling {} tokens (raw {}) for {:#x} (balance {:.6})",
            amount_display, sell_amount, token, display
        );
    } else {
        println!(
            "[trade-tester] Selling raw amount {} for {:#x} (balance raw {})",
            sell_amount, token, balance
        );
    }

    let call = contract
        .method_hash::<_, ()>(selector, (token, sell_amount, min_out))
        .context("failed to build sellToken(address,uint256,uint256) call")?;

    let pending = call
        .send()
        .await
        .context("sell transaction submission failed")?;

    let tx_hash = pending.tx_hash();
    println!("[trade-tester] sell tx submitted: {tx_hash:?}, waiting...");

    let receipt = pending
        .await
        .context("sell transaction dropped before confirmation")?
        .context("sell transaction receipt missing")?;

    if receipt.status != Some(U64::from(1)) {
        return Err(anyhow!("sell transaction reverted on-chain"));
    }

    println!("[trade-tester] Sell confirmed in tx {:?}", tx_hash);
    Ok(())
}

async fn pancake_v2_sell_for_eth(
    router: Address,
    client: Arc<Client>,
    wallet: Address,
    token: Address,
    amount: U256,
    min_out: U256,
    wbnb: Address,
) -> Result<()> {
    let erc20 = ERC20::new(token, client.clone());
    let allowance = erc20
        .allowance(wallet, router)
        .call()
        .await
        .context("failed to read allowance")?;
    if allowance < amount {
        println!(
            "[trade-tester] Allowance {} < amount {}. Approving router {:#x}...",
            allowance, amount, router
        );
        let approve_call = erc20.approve(router, U256::MAX);
        let approve_pending = approve_call
            .send()
            .await
            .context("approve submission failed")?;
        let approve_hash = approve_pending.tx_hash();
        println!("[trade-tester] approve tx submitted: {approve_hash:?}, waiting...");
        let approve_receipt = approve_pending
            .await
            .context("approve tx dropped before confirmation")?
            .context("approve tx receipt missing")?;
        if approve_receipt.status != Some(U64::from(1)) {
            return Err(anyhow!("approve transaction reverted on-chain"));
        }
    }

    let router_abi = AbiParser::default()
        .parse(&[
            "function swapExactTokensForETHSupportingFeeOnTransferTokens(uint256,uint256,address[],address,uint256)",
        ])
        .context("failed to parse Pancake router ABI")?;
    let router_contract = Contract::new(router, router_abi, client.clone());
    let deadline = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock error")?
        .as_secs()
        .saturating_add(120);
    let path = vec![token, wbnb];
    let call = router_contract
        .method::<_, ()>(
            "swapExactTokensForETHSupportingFeeOnTransferTokens",
            (amount, min_out, path, wallet, U256::from(deadline)),
        )
        .context("failed to build router sell call")?;
    let pending = call
        .send()
        .await
        .context("router sell transaction submission failed")?;
    let tx_hash = pending.tx_hash();
    println!("[trade-tester] router sell tx submitted: {tx_hash:?}, waiting...");
    let receipt = pending
        .await
        .context("router sell transaction dropped before confirmation")?
        .context("router sell transaction receipt missing")?;
    if receipt.status != Some(U64::from(1)) {
        return Err(anyhow!("router sell transaction reverted on-chain"));
    }
    println!("[trade-tester] router sell confirmed in tx {:?}", tx_hash);
    Ok(())
}

async fn pancake_v2_buy_for_eth(
    router: Address,
    client: Arc<Client>,
    wallet: Address,
    token: Address,
    amount_in: U256,
    min_out: U256,
    wbnb: Address,
) -> Result<()> {
    if amount_in.is_zero() {
        return Err(anyhow!("buy amount must be greater than zero"));
    }

    let router_abi = AbiParser::default()
        .parse(&[
            "function swapExactETHForTokensSupportingFeeOnTransferTokens(uint256,address[],address,uint256)",
        ])
        .context("failed to parse Pancake router ABI")?;
    let router_contract = Contract::new(router, router_abi, client);
    let deadline = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock error")?
        .as_secs()
        .saturating_add(120);
    let path = vec![wbnb, token];
    let call = router_contract
        .method::<_, ()>(
            "swapExactETHForTokensSupportingFeeOnTransferTokens",
            (min_out, path, wallet, U256::from(deadline)),
        )
        .context("failed to build router buy call")?
        .value(amount_in);
    let pending = call
        .send()
        .await
        .context("router buy transaction submission failed")?;
    let tx_hash = pending.tx_hash();
    println!("[trade-tester] router buy tx submitted: {tx_hash:?}, waiting...");
    let receipt = pending
        .await
        .context("router buy transaction dropped before confirmation")?
        .context("router buy transaction receipt missing")?;
    if receipt.status != Some(U64::from(1)) {
        return Err(anyhow!("router buy transaction reverted on-chain"));
    }
    println!("[trade-tester] router buy confirmed in tx {:?}", tx_hash);
    Ok(())
}

async fn find_v3_fee_tier(
    factory: Address,
    client: Arc<Client>,
    token: Address,
    wbnb: Address,
) -> Option<u32> {
    let factory_abi = AbiParser::default()
        .parse(&["function getPool(address,address,uint24) view returns (address)"])
        .ok()?;
    let factory_contract = Contract::new(factory, factory_abi, client);
    for fee in [100u32, 500u32, 2500u32, 10000u32] {
        match factory_contract.method::<_, Address>("getPool", (token, wbnb, fee)) {
            Ok(call) => match call.call().await {
                Ok(pool) if !pool.is_zero() => return Some(fee),
                _ => {}
            },
            Err(_) => {}
        }
    }
    None
}

async fn pancake_v3_sell_for_eth(
    router: Address,
    client: Arc<Client>,
    wallet: Address,
    token: Address,
    amount: U256,
    min_out: U256,
    wbnb: Address,
    fee: u32,
) -> Result<()> {
    let erc20 = ERC20::new(token, client.clone());
    let allowance = erc20
        .allowance(wallet, router)
        .call()
        .await
        .context("failed to read allowance")?;
    if allowance < amount {
        println!(
            "[trade-tester] Allowance {} < amount {}. Approving router {:#x}...",
            allowance, amount, router
        );
        let approve_call = erc20.approve(router, U256::MAX);
        let approve_pending = approve_call
            .send()
            .await
            .context("approve submission failed")?;
        let approve_hash = approve_pending.tx_hash();
        println!("[trade-tester] approve tx submitted: {approve_hash:?}, waiting...");
        let approve_receipt = approve_pending
            .await
            .context("approve tx dropped before confirmation")?
            .context("approve tx receipt missing")?;
        if approve_receipt.status != Some(U64::from(1)) {
            return Err(anyhow!("approve transaction reverted on-chain"));
        }
    }

    let router_abi = AbiParser::default()
        .parse(&["function exactInputSingle(tuple(address,address,uint24,address,uint256,uint256,uint256,uint160)) payable returns (uint256)"])
        .context("failed to parse Pancake V3 router ABI")?;
    let router_contract = Contract::new(router, router_abi, client.clone());
    let deadline = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock error")?
        .as_secs()
        .saturating_add(120);
    let params = (
        token,
        wbnb,
        fee,
        wallet,
        U256::from(deadline),
        amount,
        min_out,
        U256::zero(),
    );
    let call = router_contract
        .method::<_, U256>("exactInputSingle", params)
        .context("failed to build router V3 sell call")?;
    let pending = call
        .send()
        .await
        .context("router V3 sell transaction submission failed")?;
    let tx_hash = pending.tx_hash();
    println!("[trade-tester] router V3 sell tx submitted: {tx_hash:?}, waiting...");
    let receipt = pending
        .await
        .context("router V3 sell transaction dropped before confirmation")?
        .context("router V3 sell transaction receipt missing")?;
    if receipt.status != Some(U64::from(1)) {
        return Err(anyhow!("router V3 sell transaction reverted on-chain"));
    }
    println!(
        "[trade-tester] router V3 sell confirmed in tx {:?}",
        tx_hash
    );
    Ok(())
}

async fn pancake_v3_buy_for_eth(
    router: Address,
    client: Arc<Client>,
    wallet: Address,
    token: Address,
    amount_in: U256,
    min_out: U256,
    wbnb: Address,
    fee: u32,
) -> Result<()> {
    if amount_in.is_zero() {
        return Err(anyhow!("buy amount must be greater than zero"));
    }

    let router_abi = AbiParser::default()
        .parse(&["function exactInputSingle(tuple(address,address,uint24,address,uint256,uint256,uint256,uint160)) payable returns (uint256)"])
        .context("failed to parse Pancake V3 router ABI")?;
    let router_contract = Contract::new(router, router_abi, client);
    let deadline = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock error")?
        .as_secs()
        .saturating_add(120);
    let params = (
        wbnb,
        token,
        fee,
        wallet,
        U256::from(deadline),
        amount_in,
        min_out,
        U256::zero(),
    );
    let call = router_contract
        .method::<_, U256>("exactInputSingle", params)
        .context("failed to build router V3 buy call")?
        .value(amount_in);
    let pending = call
        .send()
        .await
        .context("router V3 buy transaction submission failed")?;
    let tx_hash = pending.tx_hash();
    println!("[trade-tester] router V3 buy tx submitted: {tx_hash:?}, waiting...");
    let receipt = pending
        .await
        .context("router V3 buy transaction dropped before confirmation")?
        .context("router V3 buy transaction receipt missing")?;
    if receipt.status != Some(U64::from(1)) {
        return Err(anyhow!("router V3 buy transaction reverted on-chain"));
    }
    println!("[trade-tester] router V3 buy confirmed in tx {:?}", tx_hash);
    Ok(())
}
