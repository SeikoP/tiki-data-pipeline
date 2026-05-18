# Tiki Analytics Normalize Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a new downstream Airflow DAG that creates analysis-ready PostgreSQL marts from already crawled Tiki data.

**Architecture:** Add a focused `pipelines.analytics` module for schema and refresh logic, then add a lightweight Airflow DAG that orchestrates those module functions. Keep analytics SQL idempotent and repeatable so manual reruns do not duplicate facts.

**Tech Stack:** Python 3.11, Airflow, PostgreSQL, psycopg2, pytest, Ruff.

---

### Task 1: Analytics Settings

**Files:**
- Modify: `src/common/settings.py`
- Modify: `.env.example`
- Modify: `docs/01-PARAMETERS/README.md`
- Test: `tests/test_settings_config.py`

- [ ] Add `TIKI_ANALYTICS_SCHEMA` to `SETTINGS_REGISTRY` with default `analytics`.
- [ ] Include it in `airflow_variable_defaults()`.
- [ ] Add it to `.env.example` under a new analytics section.
- [ ] Add it to parameter docs.
- [ ] Extend config tests to assert the setting is available and Airflow-overridable.
- [ ] Run `python scripts/config/sync_config_docs.py --check`.

### Task 2: Analytics Normalizer Module

**Files:**
- Create: `src/pipelines/analytics/__init__.py`
- Create: `src/pipelines/analytics/normalizer.py`
- Test: `tests/test_analytics_normalizer.py`

- [ ] Implement schema identifier validation.
- [ ] Implement idempotent DDL for analytics dimensions and facts.
- [ ] Implement dimension refresh SQL.
- [ ] Implement product daily refresh SQL.
- [ ] Implement category/seller aggregate refresh SQL.
- [ ] Implement price change and crawl quality refresh SQL.
- [ ] Return per-step row count stats for Airflow logs and tests.
- [ ] Add unit tests for validation and DDL table coverage.

### Task 3: Airflow DAG

**Files:**
- Create: `airflow/dags/tiki_analytics_normalize.py`
- Test: `tests/test_analytics_dag_import.py`

- [ ] Add the `tiki_analytics_normalize` DAG.
- [ ] Keep DAG parse lightweight by importing analytics functions inside callables.
- [ ] Resolve optional `crawl_run_id` from `dag_run.conf`.
- [ ] Add tasks for schema, dimensions, product facts, aggregate facts, price changes, crawl quality, and product current.
- [ ] Add DAG import test.

### Task 4: Verification

**Files:**
- No production files.

- [ ] Run `python -m pytest tests/test_analytics_normalizer.py tests/test_analytics_dag_import.py tests/test_settings_config.py -q`.
- [ ] Run `python -m ruff check src/pipelines/analytics airflow/dags/tiki_analytics_normalize.py tests/test_analytics_normalizer.py tests/test_analytics_dag_import.py`.
- [ ] Run `python scripts/config/sync_config_docs.py --check`.
