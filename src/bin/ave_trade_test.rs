use anyhow::{Context, Result};
use ethers::{
    contract::abigen,
    middleware::SignerMiddleware,
    providers::Middleware,
    signers::{LocalWallet, Signer},
    types::transaction::eip2718::TypedTransaction,
    types::{Address, BlockNumber, H256, TransactionRequest, U64, U256},
    utils::{hex, parse_units},
};
use sniper_fourmeme::{
    ave::{AveClient, ChainWalletSwapPayload, ChainWalletTx},
    config::Config,
    rpc_fallback::{RpcProvider, build_http_provider, rpc_urls_from_env},
    token_manager,
};
use std::{env, str::FromStr, sync::Arc, time::Duration};

const NATIVE_TOKEN_ADDRESS: &str = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

abigen!(
    ERC20,
    r#"[
        function allowance(address owner, address spender) view returns (uint256)
        function approve(address spender, uint256 value) returns (bool)
    ]"#
);

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let mut args = env::args().skip(1);
    let action = args
        .next()
        .context("Usage: ave_trade_test <buy|sell> <token> <amount|all> [slippage_pct]")?;
    let token = Address::from_str(&args.next().context("token address is required")?)
        .context("invalid token address")?;
    let amount_arg = args.next().context("amount is required")?;
    let slippage_pct = args
        .next()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(f64::NAN);

    let cfg = Config::from_env();
    let settings = cfg.ave();
    if !settings.is_enabled() {
        anyhow::bail!(
            "AVE fallback is not configured: set AVE_AI_API_KEY and enable AVE_AI_ENABLED"
        );
    }
    let client = AveClient::new(settings.clone());
    let wallet = cfg
        .private_key
        .clone()
        .context("PRIVATE_KEY missing from environment")?
        .parse::<LocalWallet>()
        .context("PRIVATE_KEY is invalid")?
        .with_chain_id(56u64);
    let wallet_address = wallet.address();
    let chain_id = wallet.chain_id();

    let rpc_urls = rpc_urls_from_env(cfg.rpc_url.clone(), &["RPC_URL2", "RPC_URL3"]);
    let provider = Arc::new(
        build_http_provider(rpc_urls, Duration::from_millis(0)).context("RPC_URL is invalid")?,
    );

    let (swap_type, amount, slippage_pct) = if action.eq_ignore_ascii_case("buy") {
        let amount_raw = amount_arg
            .parse::<f64>()
            .context("amount must be a number for buy")?;
        let amount_token: U256 = parse_units(format!("{amount_raw:.18}"), 18u32)
            .context("failed to convert buy amount to wei")?
            .into();
        let slippage_pct = if slippage_pct.is_nan() {
            cfg.liquidity_trade().buy_slip_page_pct()
        } else {
            slippage_pct
        };
        ("buy", amount_token, slippage_pct)
    } else if action.eq_ignore_ascii_case("sell") {
        let amount_token: U256 = if matches!(amount_arg.as_str(), "all" | "max") {
            token_manager::fetch_token_balance(provider.clone(), token, wallet_address)
                .await
                .context("failed to read wallet balance for sell")?
        } else {
            let decimals = token_manager::fetch_token_decimals(provider.clone(), token)
                .await
                .unwrap_or(18);
            let amount_raw = amount_arg
                .parse::<f64>()
                .context("amount must be a number for sell")?;
            parse_units(format!("{amount_raw:.18}"), decimals as u32)
                .context("failed to convert sell amount to raw units")?
                .into()
        };
        if amount_token.is_zero() {
            anyhow::bail!("sell amount is zero");
        }
        let slippage_pct = if slippage_pct.is_nan() {
            cfg.liquidity_trade().sell_sl_per_block_pct()
        } else {
            slippage_pct
        };
        ("sell", amount_token, slippage_pct)
    } else {
        anyhow::bail!("action must be 'buy' or 'sell'");
    };

    let slippage_bps = pct_to_bps_string(slippage_pct);
    let (in_token_address, out_token_address) = if swap_type == "buy" {
        (NATIVE_TOKEN_ADDRESS.into(), format!("{token:#x}"))
    } else {
        (format!("{token:#x}"), NATIVE_TOKEN_ADDRESS.into())
    };

    let payload = ChainWalletSwapPayload {
        chain: "bsc".into(),
        creator_address: format!("{:#x}", wallet_address),
        in_token_address,
        out_token_address,
        in_amount: amount.to_string(),
        swap_type: swap_type.into(),
        slippage: slippage_bps.clone(),
        use_mev: settings.use_mev,
        extra_gas: Some(settings.extra_gas.to_string()),
        auto_gas: settings.auto_gas.clone(),
        auto_slippage: settings.auto_slippage.then(|| true),
    };

    if swap_type.eq_ignore_ascii_case("sell") {
        let spender_raw = settings
            .chain_wallet_address
            .as_deref()
            .context("AVE sell requires AVE_AI_CHAIN_WALLET_ADDRESS")?;
        let spender =
            Address::from_str(spender_raw).context("invalid AVE_AI_CHAIN_WALLET_ADDRESS")?;
        let signer = Arc::new(SignerMiddleware::new(provider.clone(), wallet.clone()));
        let erc20 = ERC20::new(token, signer.clone());
        let allowance = erc20
            .allowance(wallet_address, spender)
            .call()
            .await
            .context("failed to read allowance for AVE sell")?;
        if allowance < amount {
            println!(
                "[ave-test] pre-approve allowance {} < amount {}. Approving spender {:#x}...",
                allowance, amount, spender
            );
            let approve_call = erc20.approve(spender, U256::MAX);
            let approve_pending = approve_call
                .send()
                .await
                .context("approve submission failed")?;
            let approve_hash = approve_pending.tx_hash();
            println!("[ave-test] pre-approve tx submitted: {approve_hash:?}, waiting...");
            let approve_receipt = approve_pending
                .await
                .context("approve tx dropped before confirmation")?
                .context("approve tx receipt missing")?;
            if approve_receipt.status != Some(U64::from(1)) {
                return Err(anyhow::anyhow!("approve transaction reverted on-chain"));
            }
        }
    }
    let tx_data = client
        .create_chain_wallet_tx(&payload)
        .await
        .context("AVE chain wallet request failed")?;
    let to_address_raw = tx_data
        .to_address
        .as_deref()
        .or(settings.chain_wallet_address.as_deref())
        .context("AVE missing toAddress: set AVE_AI_CHAIN_WALLET_ADDRESS")?;
    let to_address =
        Address::from_str(to_address_raw).context("invalid AVE to address returned")?;
    let _ = to_address;
    let hash = sign_and_send_chain_wallet_tx(
        &client,
        &wallet,
        provider.clone(),
        settings.chain_wallet_address.as_deref(),
        wallet_address,
        chain_id,
        swap_type,
        slippage_pct,
        tx_data,
    )
    .await?;
    println!("[ave-test] submitted {} tx hash={:#x}", swap_type, hash);
    Ok(())
}

async fn sign_and_send_chain_wallet_tx(
    client: &AveClient,
    wallet: &LocalWallet,
    provider: Arc<RpcProvider>,
    chain_wallet_address: Option<&str>,
    wallet_address: Address,
    chain_id: u64,
    swap_type: &str,
    slippage_pct: f64,
    tx_data: ChainWalletTx,
) -> Result<H256> {
    let slippage_bps = pct_to_bps_string(slippage_pct);
    let to_address_raw = tx_data
        .to_address
        .as_deref()
        .or(chain_wallet_address)
        .context("AVE missing toAddress: set AVE_AI_CHAIN_WALLET_ADDRESS")?;
    let to_address = Address::from_str(to_address_raw)
        .context("invalid AVE to address returned from createEvmTx")?;
    let tx_content = tx_data.tx_content.trim_start_matches("0x");
    let data = hex::decode(tx_content).context("failed to decode AVE txContent for signing")?;
    let gas_limit =
        U256::from_dec_str(&tx_data.gas_limit).context("failed to parse AVE gasLimit")?;
    let in_amount =
        U256::from_dec_str(&tx_data.in_amount).context("failed to parse AVE inAmount")?;
    let value = if swap_type.eq_ignore_ascii_case("buy") {
        in_amount
    } else {
        U256::zero()
    };
    let gas_price = provider
        .get_gas_price()
        .await
        .context("failed to read gas price for AVE transaction")?;
    let request = TransactionRequest::new()
        .to(to_address)
        .data(data)
        .gas(gas_limit)
        .value(value)
        .gas_price(gas_price);
    let mut typed: TypedTransaction = request.into();
    typed.set_chain_id(chain_id);
    let nonce = provider
        .get_transaction_count(wallet_address, Some(BlockNumber::Pending.into()))
        .await
        .context("failed to read nonce for AVE transaction")?;
    typed.set_nonce(nonce);
    let signature = wallet
        .sign_transaction(&typed)
        .await
        .context("failed to sign AVE chain wallet transaction")?;
    let tx_hash_hex = client
        .send_signed_chain_wallet_tx_bytes(
            "bsc",
            &tx_data.request_tx_id,
            typed.rlp_signed(&signature).as_ref(),
            client.settings().use_mev,
        )
        .await
        .context("failed to submit signed AVE transaction")?;
    let hash = H256::from_str(tx_hash_hex.trim_start_matches("0x"))
        .context("invalid AVE transaction hash returned")?;
    println!(
        "[ave-test] fallback {} tx hash={:#x} request={} slippage={}bps",
        swap_type, hash, tx_data.request_tx_id, slippage_bps
    );
    Ok(hash)
}

fn pct_to_bps_string(value: f64) -> String {
    let capped = value.clamp(0.0, 100.0);
    ((capped * 100.0).round() as u64).to_string()
}
