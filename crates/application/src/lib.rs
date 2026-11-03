use chrono::Utc;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use troko_domain::*;
use uuid::Uuid;

/// Computes a deterministic SHA-256 fingerprint from app_name, exception_type, and top stack frames.
pub fn compute_crash_fingerprint(app_name: &str, exception_type: &str, stacktrace: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(app_name.trim().as_bytes());
    hasher.update(b"::");
    hasher.update(exception_type.trim().as_bytes());
    hasher.update(b"::");

    for line in stacktrace.lines().take(6) {
        let normalized = line.trim();
        if !normalized.is_empty() {
            hasher.update(normalized.as_bytes());
            hasher.update(b"\n");
        }
    }

    format!("{:x}", hasher.finalize())[..16].to_string()
}

pub struct JobOrchestrator {
    repo: Arc<dyn JobRepositoryPort>,
    runner: Arc<dyn PipelineRunnerPort>,
    events: Arc<dyn EventBusPort>,
}

impl JobOrchestrator {
    pub fn new(
        repo: Arc<dyn JobRepositoryPort>,
        runner: Arc<dyn PipelineRunnerPort>,
        events: Arc<dyn EventBusPort>,
    ) -> Self {
        Self { repo, runner, events }
    }

    pub fn list_jobs(&self, kind: Option<JobKind>) -> Result<Vec<TranslationJob>, DomainError> {
        self.repo.list_jobs(kind)
    }

    pub fn list_series(&self, kind: Option<JobKind>) -> Result<Vec<SeriesSummary>, DomainError> {
        let all_jobs = self.repo.list_jobs(kind)?;
        let mut grouped: std::collections::BTreeMap<String, Vec<TranslationJob>> = std::collections::BTreeMap::new();
        for job in all_jobs {
            grouped.entry(job.series_or_book.clone()).or_default().push(job);
        }

        let mut out = Vec::new();
        for (series_name, mut jobs) in grouped {
            jobs.sort_by_key(|j| j.chapter_order);
            let total = jobs.len() as u32;
            let completed = jobs.iter().filter(|j| j.status == JobStatus::Completed).count() as u32;
            let pending = jobs.iter().filter(|j| j.status == JobStatus::Pending).count() as u32;
            let first_job = jobs.first();
            let series_kind = first_job.map(|j| j.kind).unwrap_or(JobKind::MangaChapter);
            let next_pending = jobs.iter().find(|j| j.status == JobStatus::Pending);

            let clean_id = series_name.replace(' ', "_");
            let cover_url = Some(format!("/api/v1/covers/{}", clean_id));

            out.push(SeriesSummary {
                id: clean_id,
                title: series_name,
                kind: series_kind,
                cover_url,
                total_count: total,
                completed_count: completed,
                pending_count: pending,
                next_pending_job_id: next_pending.map(|j| j.id.clone()),
                next_pending_title: next_pending.map(|j| j.title.clone()),
            });
        }

        // Sort series by pending count descending (most active first) then alphabetically
        out.sort_by(|a, b| b.pending_count.cmp(&a.pending_count).then_with(|| a.title.cmp(&b.title)));
        Ok(out)
    }

    pub fn start_job(&self, job_id: &str) -> Result<TranslationJob, DomainError> {
        let mut job = self
            .repo
            .get_job(job_id)?
            .ok_or_else(|| DomainError::JobNotFound(job_id.to_string()))?;

        let now = Utc::now().to_rfc3339();
        job.mark_running(now)?;
        self.repo.upsert_job(&job)?;

        self.events.publish(HubEvent::JobStarted {
            job_id: job.id.clone(),
            title: job.title.clone(),
            series: job.series_or_book.clone(),
            kind: job.kind,
        });

        self.runner.spawn_job(job.clone())?;
        Ok(job)
    }

    pub fn advance_next_chapter(&self, kind: Option<JobKind>) -> Result<TranslationJob, DomainError> {
        let all_jobs = self.repo.list_jobs(kind)?;
        let next_job = select_next_pending_job(&all_jobs, kind).ok_or(DomainError::NoPendingJobs)?;
        self.start_job(&next_job.id)
    }

    pub fn cancel_job(&self, job_id: &str) -> Result<TranslationJob, DomainError> {
        let mut job = self
            .repo
            .get_job(job_id)?
            .ok_or_else(|| DomainError::JobNotFound(job_id.to_string()))?;

        let _ = self.runner.cancel_job(job_id);
        job.status = JobStatus::Cancelled;
        job.last_message = Some("Cancelado por el usuario desde el teléfono".to_string());
        job.updated_at = Utc::now().to_rfc3339();
        self.repo.upsert_job(&job)?;
        Ok(job)
    }
}

pub struct CrashOrchestrator {
    repo: Arc<dyn CrashRepositoryPort>,
    vault: Arc<dyn CrashVaultWriterPort>,
    events: Arc<dyn EventBusPort>,
}

impl CrashOrchestrator {
    pub fn new(
        repo: Arc<dyn CrashRepositoryPort>,
        vault: Arc<dyn CrashVaultWriterPort>,
        events: Arc<dyn EventBusPort>,
    ) -> Self {
        Self { repo, vault, events }
    }

    pub fn ingest_crash(&self, payload: CrashPayload) -> Result<CrashIncident, DomainError> {
        let now = payload.timestamp.clone().unwrap_or_else(|| Utc::now().to_rfc3339());
        let fingerprint = compute_crash_fingerprint(
            &payload.app_name,
            &payload.exception_type,
            &payload.stacktrace,
        );

        let existing = self.repo.get_by_fingerprint(&fingerprint)?;

        let mut incident = match existing {
            Some(mut prev) => {
                prev.occurrence_count += 1;
                prev.last_seen_at = now;
                prev.app_version = payload.app_version;
                prev.message = payload.message;
                prev.stacktrace = payload.stacktrace;
                prev
            }
            None => CrashIncident {
                id: Uuid::new_v4().to_string(),
                fingerprint,
                app_name: payload.app_name,
                app_version: payload.app_version,
                exception_type: payload.exception_type,
                message: payload.message,
                stacktrace: payload.stacktrace,
                device_model: payload.device_model,
                android_version: payload.android_version,
                severity: payload.severity,
                occurrence_count: 1,
                first_seen_at: now.clone(),
                last_seen_at: now,
                markdown_report_path: None,
            },
        };

        let md_path = self.vault.write_markdown_report(&incident)?;
        incident.markdown_report_path = Some(md_path);

        let saved = self.repo.record_crash(&incident)?;

        self.events.publish(HubEvent::CrashCaptured {
            incident_id: saved.id.clone(),
            app_name: saved.app_name.clone(),
            exception_type: saved.exception_type.clone(),
            message: saved.message.clone(),
            occurrence_count: saved.occurrence_count,
        });

        Ok(saved)
    }

    pub fn list_recent_crashes(&self, limit: usize) -> Result<Vec<CrashIncident>, DomainError> {
        self.repo.list_crashes(limit)
    }
}

pub struct ChatOrchestrator {
    repo: Arc<dyn ChatRepositoryPort>,
    events: Arc<dyn EventBusPort>,
}

impl ChatOrchestrator {
    pub fn new(repo: Arc<dyn ChatRepositoryPort>, events: Arc<dyn EventBusPort>) -> Self {
        Self { repo, events }
    }

    pub fn record_message(
        &self,
        session_id: String,
        role: ChatRole,
        content: String,
        source: String,
    ) -> Result<ChatEntry, DomainError> {
        let entry = ChatEntry {
            id: Uuid::new_v4().to_string(),
            session_id,
            role,
            content,
            source,
            created_at: Utc::now().to_rfc3339(),
        };
        self.repo.append_entry(&entry)?;
        self.events.publish(HubEvent::ChatAppended {
            entry: entry.clone(),
        });
        Ok(entry)
    }

    pub fn list_history(&self, limit: usize) -> Result<Vec<ChatEntry>, DomainError> {
        self.repo.list_recent(limit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crash_fingerprint_is_deterministic_for_same_top_frames() {
        let stack1 = "java.lang.NullPointerException: missing chapter\n\tat com.yomikku.Reader.open(Reader.kt:42)\n\tat com.yomikku.Main.run(Main.kt:10)";
        let stack2 = "java.lang.NullPointerException: missing chapter\n\tat com.yomikku.Reader.open(Reader.kt:42)\n\tat com.yomikku.Main.run(Main.kt:10)";
        let fp1 = compute_crash_fingerprint("yomikku", "NullPointerException", stack1);
        let fp2 = compute_crash_fingerprint("yomikku", "NullPointerException", stack2);
        assert_eq!(fp1, fp2);
        assert_eq!(fp1.len(), 16);
    }
}
