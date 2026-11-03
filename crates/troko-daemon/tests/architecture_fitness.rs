use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use troko_adapters_infra::{
    BroadcastEventBus, MarkdownCrashVault, SqliteRepo, TokioPipelineRunner,
};
use troko_application::{ChatOrchestrator, CrashOrchestrator, JobOrchestrator};
use troko_domain::{
    ChatRole, CrashPayload, CrashSeverity, JobKind, JobRepositoryPort, JobStatus, TranslationJob,
};

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn ffdd_domain_must_remain_pure_and_framework_free() {
    let root = workspace_root();
    let domain_cargo = fs::read_to_string(root.join("crates/domain/Cargo.toml")).unwrap();
    let domain_src = fs::read_to_string(root.join("crates/domain/src/lib.rs")).unwrap();

    for forbidden in [
        "axum",
        "rusqlite",
        "tokio",
        "troko-application",
        "troko-adapters-infra",
        "troko-adapters-api",
    ] {
        assert!(
            !domain_cargo.contains(forbidden),
            "FFDD Violation: domain Cargo.toml must not depend on {}",
            forbidden
        );
        assert!(
            !domain_src.contains(forbidden),
            "FFDD Violation: domain lib.rs must not import {}",
            forbidden
        );
    }
}

#[test]
fn ffdd_application_must_not_depend_on_adapters_or_frameworks() {
    let root = workspace_root();
    let app_cargo = fs::read_to_string(root.join("crates/application/Cargo.toml")).unwrap();
    let app_src = fs::read_to_string(root.join("crates/application/src/lib.rs")).unwrap();

    for forbidden in [
        "axum",
        "rusqlite",
        "tokio",
        "troko-adapters-infra",
        "troko-adapters-api",
    ] {
        assert!(
            !app_cargo.contains(forbidden),
            "FFDD Violation: application Cargo.toml must not depend on {}",
            forbidden
        );
        assert!(
            !app_src.contains(forbidden),
            "FFDD Violation: application lib.rs must not import {}",
            forbidden
        );
    }
}

#[test]
fn ffdd_adapters_api_must_not_import_adapters_infra() {
    let root = workspace_root();
    let api_cargo = fs::read_to_string(root.join("crates/adapters-api/Cargo.toml")).unwrap();
    let api_src = fs::read_to_string(root.join("crates/adapters-api/src/lib.rs")).unwrap();

    for forbidden in ["troko-adapters-infra", "troko_adapters_infra", "rusqlite"] {
        assert!(
            !api_cargo.contains(forbidden),
            "FFDD Violation: adapters-api Cargo.toml must not depend on {}",
            forbidden
        );
        assert!(
            !api_src.contains(forbidden),
            "FFDD Violation: adapters-api lib.rs must not import {}",
            forbidden
        );
    }
}

#[tokio::test]
async fn integration_sqlite_pipelines_and_crash_vault_work_end_to_end() {
    let tmp_dir = std::env::temp_dir().join(format!("troko_hub_test_{}", std::process::id()));
    let _ = fs::remove_dir_all(&tmp_dir);
    fs::create_dir_all(&tmp_dir).unwrap();

    let db_path = tmp_dir.join("test.db");
    let vault_dir = tmp_dir.join("crashes");

    let repo = SqliteRepo::open(&db_path).unwrap();
    let bus = BroadcastEventBus::new(32);
    let runner = Arc::new(TokioPipelineRunner::new(
        repo.clone(),
        bus.clone(),
        Duration::from_millis(20),
    ));
    let vault = Arc::new(MarkdownCrashVault::new(&vault_dir).unwrap());

    // 1. Insert a Pending Manga job and trigger advance_next_chapter
    repo.upsert_job(&TranslationJob {
        id: "manga:test:ch1".to_string(),
        title: "Gal Yome no Himitsu — Chapter 1".to_string(),
        series_or_book: "Gal Yome no Himitsu".to_string(),
        chapter_order: 1,
        kind: JobKind::MangaChapter,
        source_path: "/tmp/in".to_string(),
        output_path: Some("/tmp/out".to_string()),
        status: JobStatus::Pending,
        progress_pct: 0,
        last_message: None,
        updated_at: chrono::Utc::now().to_rfc3339(),
    })
    .unwrap();

    let job_orch = JobOrchestrator::new(Arc::new(repo.clone()), runner, Arc::new(bus.clone()));
    let started = job_orch.advance_next_chapter(None).unwrap();
    assert_eq!(started.status, JobStatus::Running);

    // Wait for background pipeline to finish
    tokio::time::sleep(Duration::from_millis(150)).await;
    let finished = repo.get_job("manga:test:ch1").unwrap().unwrap();
    assert_eq!(finished.status, JobStatus::Completed);
    assert_eq!(finished.progress_pct, 100);

    // 2. Ingest a crash and verify markdown file + deduplication
    let crash_orch =
        CrashOrchestrator::new(Arc::new(repo.clone()), vault, Arc::new(bus.clone()));
    let payload = CrashPayload {
        app_name: "com.troko.android".to_string(),
        app_version: "1.0.0".to_string(),
        exception_type: "java.lang.IllegalStateException".to_string(),
        message: "Test crash from phone".to_string(),
        stacktrace: "java.lang.IllegalStateException: Test\n\tat com.troko.MainActivity.onCreate(MainActivity.kt:42)".to_string(),
        device_model: "Xiaomi".to_string(),
        android_version: "14".to_string(),
        severity: CrashSeverity::Fatal,
        timestamp: None,
    };
    let inc1 = crash_orch.ingest_crash(payload.clone()).unwrap();
    let inc2 = crash_orch.ingest_crash(payload).unwrap();
    assert_eq!(inc1.fingerprint, inc2.fingerprint);
    assert_eq!(inc2.occurrence_count, 2);
    assert!(PathBuf::from(inc2.markdown_report_path.unwrap()).exists());

    // 3. Record and retrieve chat history
    let chat_orch = ChatOrchestrator::new(Arc::new(repo), Arc::new(bus));
    chat_orch
        .record_message(
            "session-1".to_string(),
            ChatRole::User,
            "Continúa con el siguiente capítulo".to_string(),
            "voice".to_string(),
        )
        .unwrap();
    let recent = chat_orch.list_history(10).unwrap();
    assert_eq!(recent.len(), 1);
    assert_eq!(recent[0].content, "Continúa con el siguiente capítulo");

    let _ = fs::remove_dir_all(&tmp_dir);
}
