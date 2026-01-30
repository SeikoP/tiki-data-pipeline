---
name: Airflow Operations Manager
description: Skill for managing Airflow DAGs, fixing import errors, and monitoring Docker-based Tiki pipeline.
---

# Airflow Operations Manager

This skill provides a comprehensive set of procedures and scripts to handle the complex Airflow environment of the Tiki Data Pipeline.

## Capabilities

### 1. Fix DAG Import Errors
Automatically identifies and corrects common Airflow import issues:
- Converts relative imports to absolute imports (`tiki_crawl_products_v2.tasks...`).
- Ensures `sys.path` enrichment for local packages in `src/`.
- Cleans up `sys.modules` from "None" or "fake" module entries that block proper package loading.

### 2. Environment Management
- Monitoring Docker containers: `docker compose ps`, `docker compose logs -f`.
- Restarting services safely: Handles Airflow database initialization and service orchestration.
- Variable management: Tools to sync Airflow Variables from `.env` or configuration files.

### 3. Task Optimization
- Validates TaskGroup structures.
- Checks Dynamic Task Mapping (expand/partial) configurations for potential performance bottlenecks.
- Monitors XCom data sizes to prevent metadata database bloat.

## Standard Procedures

### Fixing "ImportError: attempted relative import with no known parent package"
1. Identify the failing task module in `airflow/dags/tiki_crawl_products_v2/tasks/`.
2. Replace `from .common` with `from tiki_crawl_products_v2.tasks.common`.
3. Restart the scheduler to force a dagbag re-parse.

### Monitoring Health
- Check scheduler heartbeats.
- Verify worker memory usage when running multiple Selenium drivers.
