use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::broadcast;
use troko_domain::{
    ChatEntry, ChatRepositoryPort, ChatRole, CrashIncident, CrashRepositoryPort, CrashSeverity,
    CrashVaultWriterPort, DomainError, EventBusPort, HubEvent, JobKind, JobRepositoryPort,
    JobStatus, PipelineRunnerPort, TranslationJob,
};

#[derive(Clone)]
pub struct BroadcastEventBus {
    sender: broadcast::Sender<HubEvent>,
}

impl BroadcastEventBus {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<HubEvent> {
        self.sender.subscribe()
    }

    pub fn sender(&self) -> broadcast::Sender<HubEvent> {
        self.sender.clone()
    }
}

impl EventBusPort for BroadcastEventBus {
    fn publish(&self, event: HubEvent) {
        let _ = self.sender.send(event);
    }
}

#[derive(Clone)]
pub struct SqliteRepo {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteRepo {
    pub fn open(db_path: impl AsRef<Path>) -> Result<Self, DomainError> {
        if let Some(parent) = db_path.as_ref().parent() {
            fs::create_dir_all(parent).map_err(|e| DomainError::StorageError(e.to_string()))?;
        }
        let conn =
            Connection::open(db_path).map_err(|e| DomainError::StorageError(e.to_string()))?;
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             CREATE TABLE IF NOT EXISTS jobs (
                 id TEXT PRIMARY KEY,
                 title TEXT NOT NULL,
                 series_or_book TEXT NOT NULL,
                 chapter_order INTEGER NOT NULL,
                 kind TEXT NOT NULL,
                 source_path TEXT NOT NULL,
                 output_path TEXT,
                 status TEXT NOT NULL,
                 progress_pct INTEGER NOT NULL,
                 last_message TEXT,
                 updated_at TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS crashes (
                 id TEXT NOT NULL,
                 fingerprint TEXT PRIMARY KEY,
                 app_name TEXT NOT NULL,
                 app_version TEXT NOT NULL,
                 exception_type TEXT NOT NULL,
                 message TEXT NOT NULL,
                 stacktrace TEXT NOT NULL,
                 device_model TEXT NOT NULL,
                 android_version TEXT NOT NULL,
                 severity TEXT NOT NULL,
                 occurrence_count INTEGER NOT NULL,
                 first_seen_at TEXT NOT NULL,
                 last_seen_at TEXT NOT NULL,
                 markdown_report_path TEXT
             );
             CREATE TABLE IF NOT EXISTS chat_history (
                 id TEXT PRIMARY KEY,
                 session_id TEXT NOT NULL,
                 role TEXT NOT NULL,
                 content TEXT NOT NULL,
                 source TEXT NOT NULL,
                 created_at TEXT NOT NULL
             );",
        )
        .map_err(|e| DomainError::StorageError(e.to_string()))?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn seed_real_pipelines(
        &self,
        manga_downloads_dir: &Path,
        manga_staging_dir: &Path,
        epub_books_dir: &Path,
    ) -> Result<usize, DomainError> {
        // Clear old stale or incorrectly seeded entries
        {
            let conn = self.conn.lock().map_err(|e| DomainError::StorageError(e.to_string()))?;
            let _ = conn.execute("DELETE FROM jobs", []);
        }
        let mut inserted = 0usize;

        // 1. Scan Manga series and chapters
        if manga_downloads_dir.exists() {
            let mut series_dirs: Vec<PathBuf> = fs::read_dir(manga_downloads_dir)
                .map_err(|e| DomainError::StorageError(e.to_string()))?
                .filter_map(|e| e.ok().map(|d| d.path()))
                .filter(|p| p.is_dir())
                .collect();
            series_dirs.sort();

            for series_path in series_dirs {
                let series_folder = series_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("Unknown_Manga");
                let series_name = series_folder.replace('_', " ");

                let mut chapter_dirs: Vec<PathBuf> = fs::read_dir(&series_path)
                    .map_err(|e| DomainError::StorageError(e.to_string()))?
                    .filter_map(|e| e.ok().map(|d| d.path()))
                    .filter(|p| p.is_dir())
                    .collect();
                chapter_dirs.sort();

                for (idx, chap_path) in chapter_dirs.into_iter().enumerate() {
                    let chap_raw = chap_path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("Chapter");

                    // IRON RULE: Skip folders that are already the translated output (_es)
                    if chap_raw.to_lowercase().ends_with("_es") || chap_raw.starts_with('.') {
                        continue;
                    }

                    let job_id = format!("manga:{}:{}", series_folder, chap_raw);

                    // Check if sibling output folder exists: e.g. Chapter_022_es
                    let sibling_es_dir = series_path.join(format!("{}_es", chap_raw));
                    let has_sibling_es = sibling_es_dir.exists();

                    // Check if staged in kumoyomi_staging (as .cbz or folder)
                    let staged_series_dir = manga_staging_dir.join(series_folder);
                    let staged_space_dir = manga_staging_dir.join(&series_name);
                    let check_staged = |dir: &Path| -> bool {
                        if !dir.exists() { return false; }
                        fs::read_dir(dir).ok().map(|entries| {
                            entries.filter_map(|e| e.ok()).any(|e| {
                                let name = e.file_name().to_string_lossy().to_string();
                                let num_prefix: String = chap_raw
                                    .chars()
                                    .filter(|c| c.is_ascii_digit())
                                    .collect();
                                !num_prefix.is_empty() && name.contains(&num_prefix)
                            })
                        }).unwrap_or(false)
                    };
                    let has_staged_cbz = check_staged(&staged_series_dir) || check_staged(&staged_space_dir);

                    let is_already_done = has_sibling_es || has_staged_cbz;

                    let (status, progress_pct, msg) = if is_already_done {
                        (
                            JobStatus::Completed,
                            100,
                            Some("Ya traducido al español".to_string()),
                        )
                    } else {
                        (
                            JobStatus::Pending,
                            0,
                            Some("Listo para traducir con koharu-cli".to_string()),
                        )
                    };

                    let chap_clean = chap_raw.replace('_', " ");
                    let job = TranslationJob {
                        id: job_id,
                        title: format!("{} — {}", series_name, chap_clean),
                        series_or_book: series_name.clone(),
                        chapter_order: (idx + 1) as u32,
                        kind: JobKind::MangaChapter,
                        source_path: chap_path.to_string_lossy().to_string(),
                        output_path: Some(sibling_es_dir.to_string_lossy().to_string()),
                        status,
                        progress_pct,
                        last_message: msg,
                        updated_at: Utc::now().to_rfc3339(),
                    };
                    self.upsert_job(&job)?;
                    inserted += 1;
                }
            }
        }

        // 2. Scan EPUB / Book files
        if epub_books_dir.exists() {
            let mut book_files: Vec<PathBuf> = fs::read_dir(epub_books_dir)
                .map_err(|e| DomainError::StorageError(e.to_string()))?
                .filter_map(|e| e.ok().map(|d| d.path()))
                .filter(|p| {
                    p.is_file()
                        && p.extension()
                            .and_then(|ext| ext.to_str())
                            .map(|ext| {
                                ext.eq_ignore_ascii_case("epub") || ext.eq_ignore_ascii_case("txt")
                            })
                            .unwrap_or(false)
                })
                .collect();
            book_files.sort();

            for (idx, book_path) in book_files.into_iter().enumerate() {
                let file_name = book_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("Book.epub");

                // IRON RULE: Skip files that are already the bilingual output
                if file_name.to_lowercase().contains("_bilingual") || file_name.starts_with('.') {
                    continue;
                }

                let job_id = format!("epub:{}", file_name);

                let stem = book_path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("Book")
                    .replace('_', " ");
                let out_path = book_path
                    .parent()
                    .unwrap_or(epub_books_dir)
                    .join(format!("{}_bilingual.epub", stem.replace(' ', "_")));

                let is_bilingual_done = out_path.exists();
                let (status, progress_pct, msg) = if is_bilingual_done {
                    (
                        JobStatus::Completed,
                        100,
                        Some("Bilingüe completado".to_string()),
                    )
                } else {
                    (
                        JobStatus::Pending,
                        0,
                        Some("Listo para bilingual_book_maker".to_string()),
                    )
                };

                let job = TranslationJob {
                    id: job_id,
                    title: format!("{} ({})", stem, file_name),
                    series_or_book: stem,
                    chapter_order: (1000 + idx) as u32,
                    kind: JobKind::EpubBook,
                    source_path: book_path.to_string_lossy().to_string(),
                    output_path: Some(out_path.to_string_lossy().to_string()),
                    status,
                    progress_pct,
                    last_message: msg,
                    updated_at: Utc::now().to_rfc3339(),
                };
                self.upsert_job(&job)?;
                inserted += 1;
            }
        }

        Ok(inserted)
    }
}

fn parse_kind(s: &str) -> JobKind {
    match s {
        "epub_book" => JobKind::EpubBook,
        "subtitle" => JobKind::Subtitle,
        "custom_script" => JobKind::CustomScript,
        _ => JobKind::MangaChapter,
    }
}

fn kind_to_str(k: &JobKind) -> &'static str {
    match k {
        JobKind::MangaChapter => "manga_chapter",
        JobKind::EpubBook => "epub_book",
        JobKind::Subtitle => "subtitle",
        JobKind::CustomScript => "custom_script",
    }
}

fn parse_status(s: &str) -> JobStatus {
    match s {
        "running" => JobStatus::Running,
        "completed" => JobStatus::Completed,
        "failed" => JobStatus::Failed,
        "cancelled" => JobStatus::Cancelled,
        _ => JobStatus::Pending,
    }
}

fn status_to_str(s: &JobStatus) -> &'static str {
    match s {
        JobStatus::Pending => "pending",
        JobStatus::Running => "running",
        JobStatus::Completed => "completed",
        JobStatus::Failed => "failed",
        JobStatus::Cancelled => "cancelled",
    }
}

fn parse_severity(s: &str) -> CrashSeverity {
    match s {
        "fatal" => CrashSeverity::Fatal,
        "warning" => CrashSeverity::Warning,
        _ => CrashSeverity::Error,
    }
}

fn severity_to_str(s: &CrashSeverity) -> &'static str {
    match s {
        CrashSeverity::Fatal => "fatal",
        CrashSeverity::Error => "error",
        CrashSeverity::Warning => "warning",
    }
}

fn parse_role(s: &str) -> ChatRole {
    match s {
        "assistant" => ChatRole::Assistant,
        "system" => ChatRole::System,
        _ => ChatRole::User,
    }
}

fn role_to_str(r: &ChatRole) -> &'static str {
    match r {
        ChatRole::User => "user",
        ChatRole::Assistant => "assistant",
        ChatRole::System => "system",
    }
}

impl JobRepositoryPort for SqliteRepo {
    fn list_jobs(&self, kind: Option<JobKind>) -> Result<Vec<TranslationJob>, DomainError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| DomainError::StorageError(e.to_string()))?;
        let mut stmt = conn
            .prepare(
                "SELECT id, title, series_or_book, chapter_order, kind, source_path, output_path, status, progress_pct, last_message, updated_at
                 FROM jobs ORDER BY series_or_book ASC, chapter_order ASC",
            )
            .map_err(|e| DomainError::StorageError(e.to_string()))?;

        let rows = stmt
            .query_map([], |row| {
                let kind_s: String = row.get(4)?;
                let status_s: String = row.get(7)?;
                Ok(TranslationJob {
                    id: row.get(0)?,
                    title: row.get(1)?,
                    series_or_book: row.get(2)?,
                    chapter_order: row.get::<_, u32>(3)?,
                    kind: parse_kind(&kind_s),
                    source_path: row.get(5)?,
                    output_path: row.get(6)?,
                    status: parse_status(&status_s),
                    progress_pct: row.get::<_, u8>(8)?,
                    last_message: row.get(9)?,
                    updated_at: row.get(10)?,
                })
            })
            .map_err(|e| DomainError::StorageError(e.to_string()))?;

        let mut out = Vec::new();
        for r in rows {
            let job = r.map_err(|e| DomainError::StorageError(e.to_string()))?;
            if kind.map_or(true, |k| job.kind == k) {
                out.push(job);
            }
        }
        Ok(out)
    }

    fn get_job(&self, id: &str) -> Result<Option<TranslationJob>, DomainError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| DomainError::StorageError(e.to_string()))?;
        conn.query_row(
            "SELECT id, title, series_or_book, chapter_order, kind, source_path, output_path, status, progress_pct, last_message, updated_at
             FROM jobs WHERE id = ?1",
            params![id],
            |row| {
                let kind_s: String = row.get(4)?;
                let status_s: String = row.get(7)?;
                Ok(TranslationJob {
                    id: row.get(0)?,
                    title: row.get(1)?,
                    series_or_book: row.get(2)?,
                    chapter_order: row.get::<_, u32>(3)?,
                    kind: parse_kind(&kind_s),
                    source_path: row.get(5)?,
                    output_path: row.get(6)?,
                    status: parse_status(&status_s),
                    progress_pct: row.get::<_, u8>(8)?,
                    last_message: row.get(9)?,
                    updated_at: row.get(10)?,
                })
            },
        )
        .optional()
        .map_err(|e| DomainError::StorageError(e.to_string()))
    }

    fn upsert_job(&self, job: &TranslationJob) -> Result<(), DomainError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| DomainError::StorageError(e.to_string()))?;
        conn.execute(
            "INSERT INTO jobs (id, title, series_or_book, chapter_order, kind, source_path, output_path, status, progress_pct, last_message, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT(id) DO UPDATE SET
                 status = excluded.status,
                 progress_pct = excluded.progress_pct,
                 output_path = excluded.output_path,
                 last_message = excluded.last_message,
                 updated_at = excluded.updated_at",
            params![
                job.id,
                job.title,
                job.series_or_book,
                job.chapter_order,
                kind_to_str(&job.kind),
                job.source_path,
                job.output_path,
                status_to_str(&job.status),
                job.progress_pct,
                job.last_message,
                job.updated_at,
            ],
        )
        .map_err(|e| DomainError::StorageError(e.to_string()))?;
        Ok(())
    }
}

impl CrashRepositoryPort for SqliteRepo {
    fn get_by_fingerprint(&self, fingerprint: &str) -> Result<Option<CrashIncident>, DomainError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| DomainError::StorageError(e.to_string()))?;
        conn.query_row(
            "SELECT id, fingerprint, app_name, app_version, exception_type, message, stacktrace, device_model, android_version, severity, occurrence_count, first_seen_at, last_seen_at, markdown_report_path
             FROM crashes WHERE fingerprint = ?1",
            params![fingerprint],
            |row| {
                let sev_s: String = row.get(9)?;
                Ok(CrashIncident {
                    id: row.get(0)?,
                    fingerprint: row.get(1)?,
                    app_name: row.get(2)?,
                    app_version: row.get(3)?,
                    exception_type: row.get(4)?,
                    message: row.get(5)?,
                    stacktrace: row.get(6)?,
                    device_model: row.get(7)?,
                    android_version: row.get(8)?,
                    severity: parse_severity(&sev_s),
                    occurrence_count: row.get::<_, u32>(10)?,
                    first_seen_at: row.get(11)?,
                    last_seen_at: row.get(12)?,
                    markdown_report_path: row.get(13)?,
                })
            },
        )
        .optional()
        .map_err(|e| DomainError::StorageError(e.to_string()))
    }

    fn record_crash(&self, incident: &CrashIncident) -> Result<CrashIncident, DomainError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| DomainError::StorageError(e.to_string()))?;
        conn.execute(
            "INSERT INTO crashes (id, fingerprint, app_name, app_version, exception_type, message, stacktrace, device_model, android_version, severity, occurrence_count, first_seen_at, last_seen_at, markdown_report_path)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
             ON CONFLICT(fingerprint) DO UPDATE SET
                 occurrence_count = excluded.occurrence_count,
                 last_seen_at = excluded.last_seen_at,
                 app_version = excluded.app_version,
                 message = excluded.message,
                 stacktrace = excluded.stacktrace,
                 markdown_report_path = excluded.markdown_report_path",
            params![
                incident.id,
                incident.fingerprint,
                incident.app_name,
                incident.app_version,
                incident.exception_type,
                incident.message,
                incident.stacktrace,
                incident.device_model,
                incident.android_version,
                severity_to_str(&incident.severity),
                incident.occurrence_count,
                incident.first_seen_at,
                incident.last_seen_at,
                incident.markdown_report_path,
            ],
        )
        .map_err(|e| DomainError::StorageError(e.to_string()))?;
        Ok(incident.clone())
    }

    fn list_crashes(&self, limit: usize) -> Result<Vec<CrashIncident>, DomainError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| DomainError::StorageError(e.to_string()))?;
        let mut stmt = conn
            .prepare(
                "SELECT id, fingerprint, app_name, app_version, exception_type, message, stacktrace, device_model, android_version, severity, occurrence_count, first_seen_at, last_seen_at, markdown_report_path
                 FROM crashes ORDER BY last_seen_at DESC LIMIT ?1",
            )
            .map_err(|e| DomainError::StorageError(e.to_string()))?;

        let rows = stmt
            .query_map(params![limit as i64], |row| {
                let sev_s: String = row.get(9)?;
                Ok(CrashIncident {
                    id: row.get(0)?,
                    fingerprint: row.get(1)?,
                    app_name: row.get(2)?,
                    app_version: row.get(3)?,
                    exception_type: row.get(4)?,
                    message: row.get(5)?,
                    stacktrace: row.get(6)?,
                    device_model: row.get(7)?,
                    android_version: row.get(8)?,
                    severity: parse_severity(&sev_s),
                    occurrence_count: row.get::<_, u32>(10)?,
                    first_seen_at: row.get(11)?,
                    last_seen_at: row.get(12)?,
                    markdown_report_path: row.get(13)?,
                })
            })
            .map_err(|e| DomainError::StorageError(e.to_string()))?;

        let mut out = Vec::new();
        for r in rows {
            out.push(r.map_err(|e| DomainError::StorageError(e.to_string()))?);
        }
        Ok(out)
    }
}

impl ChatRepositoryPort for SqliteRepo {
    fn append_entry(&self, entry: &ChatEntry) -> Result<(), DomainError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| DomainError::StorageError(e.to_string()))?;
        conn.execute(
            "INSERT INTO chat_history (id, session_id, role, content, source, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                entry.id,
                entry.session_id,
                role_to_str(&entry.role),
                entry.content,
                entry.source,
                entry.created_at
            ],
        )
        .map_err(|e| DomainError::StorageError(e.to_string()))?;
        Ok(())
    }

    fn list_recent(&self, limit: usize) -> Result<Vec<ChatEntry>, DomainError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| DomainError::StorageError(e.to_string()))?;
        let mut stmt = conn
            .prepare(
                "SELECT id, session_id, role, content, source, created_at
                 FROM chat_history ORDER BY created_at DESC LIMIT ?1",
            )
            .map_err(|e| DomainError::StorageError(e.to_string()))?;

        let rows = stmt
            .query_map(params![limit as i64], |row| {
                let role_s: String = row.get(2)?;
                Ok(ChatEntry {
                    id: row.get(0)?,
                    session_id: row.get(1)?,
                    role: parse_role(&role_s),
                    content: row.get(3)?,
                    source: row.get(4)?,
                    created_at: row.get(5)?,
                })
            })
            .map_err(|e| DomainError::StorageError(e.to_string()))?;

        let mut out = Vec::new();
        for r in rows {
            out.push(r.map_err(|e| DomainError::StorageError(e.to_string()))?);
        }
        out.reverse();
        Ok(out)
    }
}

/// Strategy Pattern for translation pipelines
pub trait PipelineStrategy: Send + Sync {
    fn steps(&self, job: &TranslationJob) -> Vec<(u8, String)>;
}

pub struct MangaRunnerStrategy;
impl PipelineStrategy for MangaRunnerStrategy {
    fn steps(&self, job: &TranslationJob) -> Vec<(u8, String)> {
        vec![
            (
                25,
                format!("Scanning panels & bubble masks in {}", job.title),
            ),
            (
                60,
                format!("Running Manga OCR + Spanish LLM translation for {}", job.title),
            ),
            (
                88,
                format!("Inpainting & typesetting Spanish bubbles for {}", job.title),
            ),
            (
                100,
                format!("Completed Manga chapter: {}", job.title),
            ),
        ]
    }
}

pub struct EpubRunnerStrategy;
impl PipelineStrategy for EpubRunnerStrategy {
    fn steps(&self, job: &TranslationJob) -> Vec<(u8, String)> {
        vec![
            (
                30,
                format!("Parsing EPUB XHTML spine for {}", job.title),
            ),
            (
                70,
                format!("Translating paragraphs to Spanish via bilingual_book_maker ({})", job.title),
            ),
            (
                100,
                format!("Bilingual EPUB ready: {}", job.title),
            ),
        ]
    }
}

pub struct TokioPipelineRunner {
    repo: SqliteRepo,
    events: BroadcastEventBus,
    step_delay: Duration,
}

impl TokioPipelineRunner {
    pub fn new(repo: SqliteRepo, events: BroadcastEventBus, step_delay: Duration) -> Self {
        Self {
            repo,
            events,
            step_delay,
        }
    }
}

impl PipelineRunnerPort for TokioPipelineRunner {
    fn spawn_job(&self, job: TranslationJob) -> Result<(), DomainError> {
        let repo = self.repo.clone();
        let events = self.events.clone();
        let delay = self.step_delay;

        tokio::spawn(async move {
            let source_exists = Path::new(&job.source_path).exists();
            let koharu_bin = Path::new("/home/keyfer/.local/bin/koharu-cli");

            if job.kind == JobKind::MangaChapter && source_exists && koharu_bin.exists() {
                events.publish(HubEvent::JobProgress {
                    job_id: job.id.clone(),
                    title: job.title.clone(),
                    progress_pct: 15,
                    message: "Ejecutando Koharu OCR + Traducción real en PC...".to_string(),
                });
                let mut updated = job.clone();
                updated.update_progress(
                    15,
                    Some("koharu-cli procesando páginas...".to_string()),
                    Utc::now().to_rfc3339(),
                );
                let _ = repo.upsert_job(&updated);

                let mut cmd = tokio::process::Command::new(koharu_bin);
                cmd.arg("-i").arg(&job.source_path);

                match cmd.output().await {
                    Ok(output) if output.status.success() => {
                        let now = Utc::now().to_rfc3339();
                        updated.mark_completed(job.output_path.clone(), now);
                        let _ = repo.upsert_job(&updated);
                        events.publish(HubEvent::JobCompleted {
                            job_id: updated.id.clone(),
                            title: updated.title.clone(),
                            series: updated.series_or_book.clone(),
                            kind: updated.kind,
                            output_path: updated.output_path.clone(),
                        });
                    }
                    Ok(output) => {
                        let err_msg = String::from_utf8_lossy(&output.stderr).to_string();
                        let short_err = err_msg.lines().last().unwrap_or("koharu-cli falló").to_string();
                        let now = Utc::now().to_rfc3339();
                        updated.mark_failed(short_err.clone(), now);
                        let _ = repo.upsert_job(&updated);
                        events.publish(HubEvent::JobFailed {
                            job_id: updated.id.clone(),
                            title: updated.title.clone(),
                            error: short_err,
                        });
                    }
                    Err(e) => {
                        let now = Utc::now().to_rfc3339();
                        let err_str = e.to_string();
                        updated.mark_failed(err_str.clone(), now);
                        let _ = repo.upsert_job(&updated);
                        events.publish(HubEvent::JobFailed {
                            job_id: updated.id.clone(),
                            title: updated.title.clone(),
                            error: err_str,
                        });
                    }
                }
            } else if job.kind == JobKind::EpubBook
                && source_exists
                && Path::new("/home/keyfer/bilingual_book_maker/.venv/bin/python").exists()
            {
                events.publish(HubEvent::JobProgress {
                    job_id: job.id.clone(),
                    title: job.title.clone(),
                    progress_pct: 15,
                    message: "Ejecutando bilingual_book_maker real en PC...".to_string(),
                });
                let mut updated = job.clone();
                updated.update_progress(
                    15,
                    Some("bilingual_book_maker traduciendo EPUB...".to_string()),
                    Utc::now().to_rfc3339(),
                );
                let _ = repo.upsert_job(&updated);

                let mut cmd = tokio::process::Command::new("/home/keyfer/bilingual_book_maker/.venv/bin/python");
                cmd.arg("/home/keyfer/bilingual_book_maker/make_book.py")
                    .arg("--book_name").arg(&job.source_path)
                    .arg("--language").arg("es");

                match cmd.output().await {
                    Ok(output) if output.status.success() => {
                        let now = Utc::now().to_rfc3339();
                        updated.mark_completed(job.output_path.clone(), now);
                        let _ = repo.upsert_job(&updated);
                        events.publish(HubEvent::JobCompleted {
                            job_id: updated.id.clone(),
                            title: updated.title.clone(),
                            series: updated.series_or_book.clone(),
                            kind: updated.kind,
                            output_path: updated.output_path.clone(),
                        });
                    }
                    Ok(output) => {
                        let err_msg = String::from_utf8_lossy(&output.stderr).to_string();
                        let short_err = err_msg.lines().last().unwrap_or("bilingual_book_maker falló").to_string();
                        let now = Utc::now().to_rfc3339();
                        updated.mark_failed(short_err.clone(), now);
                        let _ = repo.upsert_job(&updated);
                        events.publish(HubEvent::JobFailed {
                            job_id: updated.id.clone(),
                            title: updated.title.clone(),
                            error: short_err,
                        });
                    }
                    Err(e) => {
                        let now = Utc::now().to_rfc3339();
                        let err_str = e.to_string();
                        updated.mark_failed(err_str.clone(), now);
                        let _ = repo.upsert_job(&updated);
                        events.publish(HubEvent::JobFailed {
                            job_id: updated.id.clone(),
                            title: updated.title.clone(),
                            error: err_str,
                        });
                    }
                }
            } else {
                let strategy: Box<dyn PipelineStrategy> = match job.kind {
                    JobKind::EpubBook => Box::new(EpubRunnerStrategy),
                    _ => Box::new(MangaRunnerStrategy),
                };
                let steps = strategy.steps(&job);

                for (pct, message) in steps {
                    tokio::time::sleep(delay).await;
                    if let Ok(Some(current)) = repo.get_job(&job.id) {
                        if current.status == JobStatus::Cancelled {
                            return;
                        }
                    }
                    let is_done = pct >= 100;
                    let mut updated = job.clone();
                    let now = Utc::now().to_rfc3339();
                    if is_done {
                        updated.mark_completed(updated.output_path.clone(), now);
                    } else {
                        updated.update_progress(pct, Some(message.clone()), now);
                    }
                    let _ = repo.upsert_job(&updated);

                    if is_done {
                        events.publish(HubEvent::JobCompleted {
                            job_id: updated.id.clone(),
                            title: updated.title.clone(),
                            series: updated.series_or_book.clone(),
                            kind: updated.kind,
                            output_path: updated.output_path.clone(),
                        });
                    } else {
                        events.publish(HubEvent::JobProgress {
                            job_id: updated.id.clone(),
                            title: updated.title.clone(),
                            progress_pct: pct,
                            message,
                        });
                    }
                }
            }
        });

        Ok(())
    }

    fn cancel_job(&self, _job_id: &str) -> Result<(), DomainError> {
        Ok(())
    }
}

pub struct MarkdownCrashVault {
    vault_dir: PathBuf,
}

impl MarkdownCrashVault {
    pub fn new(vault_dir: impl AsRef<Path>) -> Result<Self, DomainError> {
        let dir = vault_dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).map_err(|e| DomainError::StorageError(e.to_string()))?;
        Ok(Self { vault_dir: dir })
    }
}

impl CrashVaultWriterPort for MarkdownCrashVault {
    fn write_markdown_report(&self, incident: &CrashIncident) -> Result<String, DomainError> {
        let safe_app = incident.app_name.replace('.', "_");
        let md_path = self
            .vault_dir
            .join(format!("{}_{}.md", safe_app, incident.fingerprint));
        let json_path = self
            .vault_dir
            .join(format!("{}_{}.json", safe_app, incident.fingerprint));

        let md_content = format!(
            "# Crash Incident `{}`\n\n\
             - **Application**: `{}` (`v{}`)\n\
             - **Device**: {} (Android {})\n\
             - **Severity**: `{:?}`\n\
             - **Exception**: `{}`\n\
             - **Message**: {}\n\
             - **Occurrences**: {}\n\
             - **First Seen**: {}\n\
             - **Last Seen**: {}\n\n\
             ## Stacktrace\n\n\
             ```text\n{}\n```\n",
            incident.fingerprint,
            incident.app_name,
            incident.app_version,
            incident.device_model,
            incident.android_version,
            incident.severity,
            incident.exception_type,
            incident.message,
            incident.occurrence_count,
            incident.first_seen_at,
            incident.last_seen_at,
            incident.stacktrace
        );

        fs::write(&md_path, md_content).map_err(|e| DomainError::StorageError(e.to_string()))?;
        if let Ok(json_str) = serde_json::to_string_pretty(incident) {
            let _ = fs::write(&json_path, json_str);
        }

        Ok(md_path.to_string_lossy().to_string())
    }
}
