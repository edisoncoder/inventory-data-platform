# Changelog

All notable changes to this project will be documented in this file.

---

## [v0.1.0] - 2026-03-XX

### Added

* CSV data ingestion pipeline
* PostgreSQL storage integration
* Layered architecture (raw, staging, analytics)
* Data quality checks (raw, staging, analytics)
* Idempotent load strategy (snapshot-based)
* Structured logging system (daily logs)
* Alert system for pipeline failures
* Prefect orchestration (flows and tasks)
* Retry mechanism for task resilience

### Changed

* Refactored pipeline execution into modular steps
* Improved separation of concerns across layers

### Fixed

* Logging formatting issues (multi-line fix)
* SQL view creation conflicts (table vs view naming)
* Data quality validation flow (layer separation)
