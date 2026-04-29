# Inventory Data Platform

## 📌 Overview

This project implements a **local data platform for analytics**, simulating real-world Data Engineering practices, including ingestion, transformation, data quality, observability, and orchestration.

---

## 🎯 Problem Statement

Organizations need reliable pipelines to:

* Ingest data from external sources
* Ensure data quality and consistency
* Transform raw data into analytics-ready structures
* Monitor pipeline execution
* Handle failures and ensure reprocessing safety

This project addresses these needs by building a **robust, observable, and idempotent data pipeline**.

---

## 🏗️ Architecture

```
Source (CSV)
   ↓
Ingestion (Python)
   ↓
Raw Layer (PostgreSQL)
   ↓
Quality Checks (Structural)
   ↓
Staging Layer (Transformations)
   ↓
Quality Checks (Business Rules)
   ↓
Analytics Layer (Fact/Dim)
   ↓
Quality Checks (Consistency)
   ↓
Logging + Alerts
   ↓
Orchestration (Prefect)
```

---

## 🧰 Tech Stack

* Python (pipeline logic)
* PostgreSQL (data storage)
* Docker (environment setup)
* WSL (Linux environment)
* Prefect (workflow orchestration)
* Cron (initial scheduling)

---

## ▶️ How to Run

### 1. Start environment

```bash
docker-compose up -d
```

### 2. Run pipeline manually

```bash
python run_pipeline.py
```

### 3. Run with Prefect

```bash
python prefect_pipeline.py
```

### 4. Start Prefect UI

```bash
prefect server start
```

---

## ✅ What is Implemented

* Data ingestion from CSV
* Layered architecture (raw, staging, analytics)
* Idempotent processing (safe re-runs)
* Data quality checks per layer
* Structured logging (daily log files)
* Alert system for pipeline failures
* Orchestration with Prefect
* Retry mechanism for resilience

---

## 🛣️ Roadmap

* v0.2.0 → Prefect as official scheduler
* v0.3.0 → Incremental load
* v0.4.0 → Historical data / SCD
* v0.5.0 → Advanced data modeling
* v0.6.0 → Formal data testing
* v0.7.0 → dbt integration
* v0.8.0 → BI layer
* v0.9.0 → Cloud foundation
* v1.0.0 → Mature data platform

---

## 📊 Current Status

Baseline version (v0.1.0) implements a fully functional local data pipeline with core Data Engineering principles.

---

## 👨‍💻 Author

Project built as part of a structured Data Engineering learning journey.
