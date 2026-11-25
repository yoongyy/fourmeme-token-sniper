use sniper_fourmeme::{config::Config, storage::MongoLogger};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    std::panic::set_hook(Box::new(|info| {
        eprintln!("[panic] {info}");
    }));

    let cfg = Config::from_env();
    if !cfg.mongo().cleanup_enabled() {
        eprintln!("[mongo-cleanup] MONGO_CLEANUP_ENABLE is false or Mongo is disabled");
        return Ok(());
    }

    let Some(storage) = MongoLogger::connect(cfg.mongo()).await else {
        eprintln!("[mongo-cleanup] failed to connect to Mongo");
        return Ok(());
    };

    let tokens_days = cfg.mongo().tokens_retention_days();
    let pairs_days = cfg.mongo().pairs_retention_days();
    println!(
        "[mongo-cleanup] starting (tokens_retention_days={tokens_days}, pairs_retention_days={pairs_days})"
    );

    match storage
        .delete_old_tokens_and_pairs(tokens_days, pairs_days)
        .await
    {
        Ok((tokens_deleted, pairs_deleted)) => {
            println!("[mongo-cleanup] deleted tokens={tokens_deleted}, pairs={pairs_deleted}");
        }
        Err(err) => {
            eprintln!("[mongo-cleanup] cleanup failed: {err:?}");
        }
    }

    Ok(())
}
