use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DomainError {
    #[error("Job not found: {0}")]
    JobNotFound(String),
    #[error("No pending jobs available in queue")]
    NoPendingJobs,
    #[error("Job '{0}' is already running")]
    JobAlreadyRunning(String),
    #[error("Invalid transition from {from:?} to {to:?}")]
    InvalidStateTransition { from: JobStatus, to: JobStatus },
    #[error("Storage error: {0}")]
    StorageError(String),
    #[error("Execution error: {0}")]
    ExecutionError(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    MangaChapter,
    EpubBook,
    Subtitle,
    CustomScript,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TranslationJob {
    pub id: String,
    pub title: String,
    pub series_or_book: String,
    pub chapter_order: u32,
    pub kind: JobKind,
    pub source_path: String,
    pub output_path: Option<String>,
    pub status: JobStatus,
    pub progress_pct: u8,
    pub last_message: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeriesSummary {
    pub id: String,
    pub title: String,
    pub kind: JobKind,
    pub cover_url: Option<String>,
    pub total_count: u32,
    pub completed_count: u32,
    pub pending_count: u32,
    pub next_pending_job_id: Option<String>,
    pub next_pending_title: Option<String>,
}

impl TranslationJob {
    pub fn mark_running(&mut self, timestamp: String) -> Result<(), DomainError> {
        if self.status == JobStatus::Running {
            return Err(DomainError::JobAlreadyRunning(self.id.clone()));
        }
        self.status = JobStatus::Running;
        self.progress_pct = 5;
        self.last_message = Some("Iniciando proceso de traducción en PC...".to_string());
        self.updated_at = timestamp;
        Ok(())
    }

    pub fn update_progress(&mut self, pct: u8, message: Option<String>, timestamp: String) {
        self.progress_pct = pct.min(100);
        if message.is_some() {
            self.last_message = message;
        }
        self.updated_at = timestamp;
    }

    pub fn mark_completed(&mut self, output: Option<String>, timestamp: String) {
        self.status = JobStatus::Completed;
        self.progress_pct = 100;
        self.output_path = output;
        self.last_message = Some("Traducción completada con éxito".to_string());
        self.updated_at = timestamp;
    }

    pub fn mark_failed(&mut self, reason: String, timestamp: String) {
        self.status = JobStatus::Failed;
        self.last_message = Some(reason);
        self.updated_at = timestamp;
    }
}

/// Pure domain selector: picks the next pending job ordered by series and chapter_order.
pub fn select_next_pending_job(jobs: &[TranslationJob], filter_kind: Option<JobKind>) -> Option<TranslationJob> {
    let mut candidates: Vec<&TranslationJob> = jobs
        .iter()
        .filter(|j| j.status == JobStatus::Pending)
        .filter(|j| filter_kind.map_or(true, |k| j.kind == k))
        .collect();

    candidates.sort_by(|a, b| {
        a.series_or_book
            .cmp(&b.series_or_book)
            .then_with(|| a.chapter_order.cmp(&b.chapter_order))
            .then_with(|| a.id.cmp(&b.id))
    });

    candidates.first().map(|&j| j.clone())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrashSeverity {
    Fatal,
    Error,
    Warning,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrashPayload {
    pub app_name: String,
    pub app_version: String,
    pub exception_type: String,
    pub message: String,
    pub stacktrace: String,
    pub device_model: String,
    pub android_version: String,
    pub severity: CrashSeverity,
    pub timestamp: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrashIncident {
    pub id: String,
    pub fingerprint: String,
    pub app_name: String,
    pub app_version: String,
    pub exception_type: String,
    pub message: String,
    pub stacktrace: String,
    pub device_model: String,
    pub android_version: String,
    pub severity: CrashSeverity,
    pub occurrence_count: u32,
    pub first_seen_at: String,
    pub last_seen_at: String,
    pub markdown_report_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChatRole {
    User,
    Assistant,
    System,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatEntry {
    pub id: String,
    pub session_id: String,
    pub role: ChatRole,
    pub content: String,
    pub source: String, // "voice" | "text" | "pc_alert"
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum HubEvent {
    JobStarted {
        job_id: String,
        title: String,
        series: String,
        kind: JobKind,
    },
    JobProgress {
        job_id: String,
        title: String,
        progress_pct: u8,
        message: String,
    },
    JobCompleted {
        job_id: String,
        title: String,
        series: String,
        kind: JobKind,
        output_path: Option<String>,
    },
    JobFailed {
        job_id: String,
        title: String,
        error: String,
    },
    CrashCaptured {
        incident_id: String,
        app_name: String,
        exception_type: String,
        message: String,
        occurrence_count: u32,
    },
    ChatAppended {
        entry: ChatEntry,
    },
}

// ============================================================================
// Hexagonal Ports (Driven / Outbound Interfaces)
// ============================================================================

pub trait JobRepositoryPort: Send + Sync {
    fn list_jobs(&self, kind: Option<JobKind>) -> Result<Vec<TranslationJob>, DomainError>;
    fn get_job(&self, id: &str) -> Result<Option<TranslationJob>, DomainError>;
    fn upsert_job(&self, job: &TranslationJob) -> Result<(), DomainError>;
}

pub trait PipelineRunnerPort: Send + Sync {
    fn spawn_job(&self, job: TranslationJob) -> Result<(), DomainError>;
    fn cancel_job(&self, job_id: &str) -> Result<(), DomainError>;
}

pub trait CrashRepositoryPort: Send + Sync {
    fn record_crash(&self, incident: &CrashIncident) -> Result<CrashIncident, DomainError>;
    fn list_crashes(&self, limit: usize) -> Result<Vec<CrashIncident>, DomainError>;
    fn get_by_fingerprint(&self, fingerprint: &str) -> Result<Option<CrashIncident>, DomainError>;
}

pub trait CrashVaultWriterPort: Send + Sync {
    fn write_markdown_report(&self, incident: &CrashIncident) -> Result<String, DomainError>;
}

pub trait ChatRepositoryPort: Send + Sync {
    fn append_entry(&self, entry: &ChatEntry) -> Result<(), DomainError>;
    fn list_recent(&self, limit: usize) -> Result<Vec<ChatEntry>, DomainError>;
}

pub trait EventBusPort: Send + Sync {
    fn publish(&self, event: HubEvent);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_job(id: &str, series: &str, order: u32, status: JobStatus, kind: JobKind) -> TranslationJob {
        TranslationJob {
            id: id.to_string(),
            title: format!("{} - Cap {}", series, order),
            series_or_book: series.to_string(),
            chapter_order: order,
            kind,
            source_path: format!("/raws/{}/{}", series, order),
            output_path: None,
            status,
            progress_pct: 0,
            last_message: None,
            updated_at: "2026-11-02T15:00:00Z".to_string(),
        }
    }

    #[test]
    fn selects_lowest_chapter_order_next_pending_job() {
        let jobs = vec![
            sample_job("1", "Berserk", 10, JobStatus::Completed, JobKind::MangaChapter),
            sample_job("2", "Berserk", 12, JobStatus::Pending, JobKind::MangaChapter),
            sample_job("3", "Berserk", 11, JobStatus::Pending, JobKind::MangaChapter),
        ];

        let next = select_next_pending_job(&jobs, Some(JobKind::MangaChapter)).expect("Should select a pending job");
        assert_eq!(next.id, "3");
        assert_eq!(next.chapter_order, 11);
    }

    #[test]
    fn prevents_starting_already_running_job() {
        let mut job = sample_job("1", "Avalon Vol 6", 1, JobStatus::Running, JobKind::EpubBook);
        let res = job.mark_running("2026-11-02T16:00:00Z".to_string());
        assert_eq!(res, Err(DomainError::JobAlreadyRunning("1".to_string())));
    }
}
