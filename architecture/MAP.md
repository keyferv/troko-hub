# Architecture Map: Troko Hub (`troko-hub`)

## Archetype
Composite **Modular Monolith with Hexagonal (Ports & Adapters) Core** inside a multi-crate Rust Cargo Workspace.

## High-Level Layers & Modules
1. **Domain Layer (`crates/domain`)**:
   - Pure business entities (`TranslationJob`, `JobKind`, `JobStatus`, `CrashEvent`, `ChatEntry`), domain events, and explicit Port traits (`JobRepositoryPort`, `CrashRepositoryPort`, `ChatRepositoryPort`, `PipelineRunnerPort`, `EventBusPort`).
2. **Application Layer (`crates/application`)**:
   - Use-case orchestrators (`JobOrchestrator`, `CrashOrchestrator`, `ChatOrchestrator`) and stacktrace fingerprinting logic.
3. **Infrastructure Adapters Layer (`crates/adapters-infra`)**:
   - Concrete implementations of domain ports: SQLite WAL repository, filesystem chapter/EPUB scanner, `PipelineRunnerStrategy` subprocess supervisor (`bilingual_book_maker` and `koharu`), and Markdown crash vault writer.
4. **Inbound API Adapters Layer (`crates/adapters-api`)**:
   - `axum` REST controllers and WebSocket event broadcaster (`/api/v1/*` and `/ws/v1/events`).
5. **Composition Root (`crates/troko-daemon`)**:
   - Binary entrypoint wiring adapters into application use cases.

## Allowed Dependency Flow
- `troko-daemon` $\rightarrow$ `adapters-api`, `adapters-infra`, `application`, `domain`
- `adapters-api` $\rightarrow$ `application`, `domain`
- `adapters-infra` $\rightarrow$ `application`, `domain`
- `application` $\rightarrow$ `domain`
- `domain` $\rightarrow$ *none (leaf core)*

## Explicitly Forbidden Dependencies (Enforced by FFDD)
- `domain` $-\times\rightarrow$ `application`, `adapters-infra`, `adapters-api`, `tokio`, `axum`, `rusqlite`
- `application` $-\times\rightarrow$ `adapters-infra`, `adapters-api`, `axum`, `rusqlite`
- `adapters-api` $-\times\rightarrow$ `adapters-infra`
- `adapters-infra` $-\times\rightarrow$ `adapters-api`
