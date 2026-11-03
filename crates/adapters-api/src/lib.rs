use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::broadcast;
use tower_http::cors::CorsLayer;
use troko_application::{ChatOrchestrator, CrashOrchestrator, JobOrchestrator};
use troko_domain::{ChatRole, CrashPayload, DomainError, HubEvent, JobKind};

#[derive(Clone)]
pub struct ApiState {
    pub jobs: Arc<JobOrchestrator>,
    pub crashes: Arc<CrashOrchestrator>,
    pub chat: Arc<ChatOrchestrator>,
    pub event_tx: broadcast::Sender<HubEvent>,
}

#[derive(Debug, Deserialize, Default)]
pub struct NextJobRequest {
    pub kind: Option<JobKind>,
}

#[derive(Debug, Deserialize)]
pub struct CreateChatRequest {
    #[serde(default = "default_session")]
    pub session_id: String,
    pub role: ChatRole,
    pub content: String,
    #[serde(default = "default_source")]
    pub source: String,
}

fn default_session() -> String {
    "default".to_string()
}

fn default_source() -> String {
    "android".to_string()
}

#[derive(Debug, Deserialize, Default)]
pub struct LimitQuery {
    pub limit: Option<usize>,
    pub kind: Option<JobKind>,
}

#[derive(Debug, Serialize)]
pub struct ApiErrorResponse {
    pub error: String,
}

fn map_domain_err(err: DomainError) -> (StatusCode, Json<ApiErrorResponse>) {
    let status = match err {
        DomainError::JobNotFound(_) | DomainError::NoPendingJobs => StatusCode::NOT_FOUND,
        DomainError::JobAlreadyRunning(_) | DomainError::InvalidStateTransition { .. } => {
            StatusCode::CONFLICT
        }
        DomainError::StorageError(_) | DomainError::ExecutionError(_) => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
    };
    (
        status,
        Json(ApiErrorResponse {
            error: err.to_string(),
        }),
    )
}

pub fn build_router(state: ApiState) -> Router {
    Router::new()
        .route("/api/v1/health", get(health_handler))
        .route("/api/v1/series", get(list_series_handler))
        .route("/api/v1/jobs", get(list_jobs_handler))
        .route("/api/v1/jobs/next", post(start_next_job_handler))
        .route("/api/v1/jobs/:id/start", post(start_job_handler))
        .route("/api/v1/jobs/:id/cancel", post(cancel_job_handler))
        .route("/api/v1/covers/:name", get(get_cover_handler))
        .route("/api/v1/services/codex-bridge/status", get(codex_status_handler))
        .route("/api/v1/services/codex-bridge/start", post(start_codex_handler))
        .route("/api/v1/services/codex-bridge/stop", post(stop_codex_handler))
        .route(
            "/api/v1/telemetry/crashes",
            get(list_crashes_handler).post(ingest_crash_handler),
        )
        .route(
            "/api/v1/chat",
            get(list_chat_handler).post(append_chat_handler),
        )
        .route("/ws/v1/events", get(ws_events_handler))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

async fn list_series_handler(
    State(state): State<ApiState>,
    Query(q): Query<LimitQuery>,
) -> impl IntoResponse {
    match state.jobs.list_series(q.kind) {
        Ok(series) => (StatusCode::OK, Json(serde_json::to_value(series).unwrap())).into_response(),
        Err(e) => map_domain_err(e).into_response(),
    }
}

async fn get_cover_handler(Path(name): Path<String>) -> impl IntoResponse {
    let clean = name.replace("..", "").replace('/', "");
    let candidates = [
        format!("/home/keyfer/raws/kumoyomi_staging/{}/cover.jpg", clean),
        format!("/home/keyfer/raws/kumoyomi_staging/{}/cover.png", clean),
        format!("/home/keyfer/raws/downloads/{}/cover.jpg", clean),
        format!("/home/keyfer/raws/downloads/{}/cover.png", clean),
        format!("/home/keyfer/raws/downloads/{}/cover.jpg", clean.replace(' ', "_")),
        format!("/home/keyfer/raws/downloads/{}/cover.png", clean.replace(' ', "_")),
        format!("/home/keyfer/raws/kumoyomi_staging/{}/cover.jpg", clean.replace('_', " ")),
        format!("/home/keyfer/raws/kumoyomi_staging/{}/cover.png", clean.replace('_', " ")),
    ];
    for path in &candidates {
        if let Ok(bytes) = std::fs::read(path) {
            let mime = if path.ends_with(".png") { "image/png" } else { "image/jpeg" };
            return (
                StatusCode::OK,
                [(axum::http::header::CONTENT_TYPE, mime)],
                bytes,
            ).into_response();
        }
    }
    StatusCode::NOT_FOUND.into_response()
}

async fn codex_status_handler() -> impl IntoResponse {
    let is_running = tokio::net::TcpStream::connect("127.0.0.1:8765").await.is_ok();
    Json(serde_json::json!({
        "running": is_running,
        "port": 8765,
        "endpoint": "http://127.0.0.1:8765/v1"
    }))
}

async fn start_codex_handler() -> impl IntoResponse {
    if tokio::net::TcpStream::connect("127.0.0.1:8765").await.is_ok() {
        return Json(serde_json::json!({"status": "already_running", "running": true}));
    }
    let _ = tokio::process::Command::new("uv")
        .args([
            "run",
            "--directory",
            "/home/keyfer/bilingual_book_maker",
            "python",
            "/home/keyfer/koharu/scripts/codex_bridge.py",
        ])
        .spawn();
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    let is_running = tokio::net::TcpStream::connect("127.0.0.1:8765").await.is_ok();
    Json(serde_json::json!({"status": "started", "running": is_running}))
}

async fn stop_codex_handler() -> impl IntoResponse {
    let _ = tokio::process::Command::new("pkill")
        .args(["-f", "codex_bridge.py"])
        .status()
        .await;
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    let is_running = tokio::net::TcpStream::connect("127.0.0.1:8765").await.is_ok();
    Json(serde_json::json!({"status": "stopped", "running": is_running}))
}

async fn health_handler() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "ok",
        "service": "troko-hub",
        "version": "0.1.0"
    }))
}

async fn list_jobs_handler(
    State(state): State<ApiState>,
    Query(q): Query<LimitQuery>,
) -> impl IntoResponse {
    match state.jobs.list_jobs(q.kind) {
        Ok(jobs) => (StatusCode::OK, Json(serde_json::to_value(jobs).unwrap())).into_response(),
        Err(e) => map_domain_err(e).into_response(),
    }
}

async fn start_next_job_handler(
    State(state): State<ApiState>,
    body: Option<Json<NextJobRequest>>,
) -> impl IntoResponse {
    let kind = body.and_then(|b| b.0.kind);
    match state.jobs.advance_next_chapter(kind) {
        Ok(job) => (StatusCode::OK, Json(serde_json::to_value(job).unwrap())).into_response(),
        Err(e) => map_domain_err(e).into_response(),
    }
}

async fn start_job_handler(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.jobs.start_job(&id) {
        Ok(job) => (StatusCode::OK, Json(serde_json::to_value(job).unwrap())).into_response(),
        Err(e) => map_domain_err(e).into_response(),
    }
}

async fn cancel_job_handler(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.jobs.cancel_job(&id) {
        Ok(job) => (StatusCode::OK, Json(serde_json::to_value(job).unwrap())).into_response(),
        Err(e) => map_domain_err(e).into_response(),
    }
}

async fn list_crashes_handler(
    State(state): State<ApiState>,
    Query(q): Query<LimitQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(50);
    match state.crashes.list_recent_crashes(limit) {
        Ok(incidents) => (
            StatusCode::OK,
            Json(serde_json::to_value(incidents).unwrap()),
        )
            .into_response(),
        Err(e) => map_domain_err(e).into_response(),
    }
}

async fn ingest_crash_handler(
    State(state): State<ApiState>,
    Json(payload): Json<CrashPayload>,
) -> impl IntoResponse {
    match state.crashes.ingest_crash(payload) {
        Ok(incident) => (
            StatusCode::CREATED,
            Json(serde_json::to_value(incident).unwrap()),
        )
            .into_response(),
        Err(e) => map_domain_err(e).into_response(),
    }
}

async fn list_chat_handler(
    State(state): State<ApiState>,
    Query(q): Query<LimitQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(100);
    match state.chat.list_history(limit) {
        Ok(entries) => (StatusCode::OK, Json(serde_json::to_value(entries).unwrap())).into_response(),
        Err(e) => map_domain_err(e).into_response(),
    }
}

async fn append_chat_handler(
    State(state): State<ApiState>,
    Json(req): Json<CreateChatRequest>,
) -> impl IntoResponse {
    match state
        .chat
        .record_message(req.session_id, req.role, req.content, req.source)
    {
        Ok(entry) => (
            StatusCode::CREATED,
            Json(serde_json::to_value(entry).unwrap()),
        )
            .into_response(),
        Err(e) => map_domain_err(e).into_response(),
    }
}

async fn ws_events_handler(
    ws: WebSocketUpgrade,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    let rx = state.event_tx.subscribe();
    ws.on_upgrade(move |socket| handle_ws_client(socket, rx))
}

async fn handle_ws_client(mut socket: WebSocket, mut rx: broadcast::Receiver<HubEvent>) {
    while let Ok(event) = rx.recv().await {
        if let Ok(json_text) = serde_json::to_string(&event) {
            if socket.send(Message::Text(json_text)).await.is_err() {
                break;
            }
        }
    }
}
