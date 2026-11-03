# ADR-001: Rust Hexagonal Workspace for PC-Mobile Hub & Crash Telemetry

## Context
We need a persistent local service on Linux that:
1. Supervises long-running translation pipelines (`bilingual_book_maker` for EPUBs and `koharu` for Manga chapters).
2. Exposes low-latency REST and WebSocket endpoints so the Troko Android client can inspect queues, advance to the next chapter by voice or UI, and receive instant completion notifications.
3. Acts as a personal, self-hosted Sentry alternative (`TrokoSentry`) that ingests, deduplicates (via SHA-256 stacktrace fingerprinting), indexes in SQLite, and exports Markdown crash reports from custom mobile/desktop apps.

## Decision
Adopt a multi-crate **Rust Cargo Workspace** structured as a **Hexagonal (Ports & Adapters) Modular Monolith**, using `tokio` for async process and event orchestration, `axum` for HTTP/WS transport, and `rusqlite` (WAL mode) + Markdown vault for persistence.

## Rationale & Simplest Alternative
- **Why not a single-file Python script?** A single-file script mixes HTTP transport, subprocess management, SQLite queries, and business rules, making boundary verification impossible and consuming ~80 MB RAM idle.
- **Why Rust Multi-Crate Workspace?** Rust enforces crate-level dependency graphs at compile time, uses <5 MB RAM idle, handles child process termination deterministically, and allows mechanical FFDD tests (`architecture_fitness.rs`) to guarantee zero boundary leakage.

## Boundaries & Allowed Dependencies
- `crates/domain`: Pure data and trait contracts. Forbidden from importing async runtimes, HTTP servers, or SQL drivers.
- `crates/application`: Pure orchestration over domain ports.
- `crates/adapters-infra` & `crates/adapters-api`: Sibling adapter crates that never import each other; wired exclusively in `crates/troko-daemon`.
