use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use troko_adapters_api::{build_router, ApiState};
use troko_adapters_infra::{
    BroadcastEventBus, MarkdownCrashVault, SqliteRepo, TokioPipelineRunner,
};
use troko_application::{ChatOrchestrator, CrashOrchestrator, JobOrchestrator};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let base_dir = std::env::var("TROKO_HUB_HOME")
        .unwrap_or_else(|_| "/home/keyfer/troko-hub".to_string());
    let db_path = format!("{}/data/troko_hub.db", base_dir);
    let crashes_dir = format!("{}/data/crashes", base_dir);

    let repo = SqliteRepo::open(&db_path)?;
    let seeded = repo.seed_real_pipelines(
        Path::new("/home/keyfer/raws/downloads"),
        Path::new("/home/keyfer/raws/kumoyomi_staging"),
        Path::new("/home/keyfer/bilingual_book_maker/test_books"),
    )?;
    info!("Troko Hub SQLite ready at {} (seeded {} new jobs)", db_path, seeded);

    let event_bus = BroadcastEventBus::new(256);
    let runner = Arc::new(TokioPipelineRunner::new(
        repo.clone(),
        event_bus.clone(),
        Duration::from_secs(2),
    ));
    let crash_vault = Arc::new(MarkdownCrashVault::new(&crashes_dir)?);

    let jobs = Arc::new(JobOrchestrator::new(
        Arc::new(repo.clone()),
        runner,
        Arc::new(event_bus.clone()),
    ));
    let crashes = Arc::new(CrashOrchestrator::new(
        Arc::new(repo.clone()),
        crash_vault,
        Arc::new(event_bus.clone()),
    ));
    let chat = Arc::new(ChatOrchestrator::new(
        Arc::new(repo),
        Arc::new(event_bus.clone()),
    ));

    let state = ApiState {
        jobs,
        crashes,
        chat,
        event_tx: event_bus.sender(),
    };

    let app = build_router(state);
    let bind_addr = std::env::var("TROKO_HUB_ADDR").unwrap_or_else(|_| "0.0.0.0:8787".to_string());
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    info!("Troko Hub Daemon listening on http://{}", bind_addr);

    axum::serve(listener, app).await?;
    Ok(())
}
