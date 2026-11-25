use sniper_fourmeme::sol_trace;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    sol_trace::trace_wallet().await
}
