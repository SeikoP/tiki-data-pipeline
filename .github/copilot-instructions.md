## Purpose

This file tells an AI coding assistant how to be immediately productive in this repository. It focuses on concrete, discoverable conventions, the main runtime/workflow entry points, and where to look for integration details.

## Quick orientation (big picture)

- **Repo purpose**: Full ETL data pipeline (Extract → Transform → Load) that crawls, processes and stores product data from Tiki.vn. Architecture is Airflow-orchestrated with Selenium crawling, Redis caching, and PostgreSQL storage.
- **Key components**: 
  - Airflow DAGs (orchestration with Dynamic Task Mapping for parallel execution)
  - Pipeline code in `src/pipelines/{crawl,transform,load}/` (three-stage ETL)
  - Docker Compose stack (Airflow scheduler/worker/API, PostgreSQL, Redis)
  - Database schemas: `crawl_data` (products/categories) initialized via `airflow/setup/init-crawl-db.sh`
- **Runtime environments**: 
  - Docker (recommended): `docker-compose up -d --build` starts full stack
  - Local Python: each pipeline script has standalone `main()` for testing (e.g., `python src/pipelines/crawl/crawl_products.py`)

## Important paths (examples to open first)

- `airflow/dags/tiki_crawl_products_dag.py` — 5000+ line DAG with Dynamic Task Mapping, TaskGroups, XCom data sharing; shows orchestration patterns
- `src/pipelines/crawl/` — Extract stage: Selenium-based crawlers with resilience patterns and Redis caching
- `src/pipelines/transform/transformer.py` — Transform stage: `DataTransformer` class for normalization, validation, computed fields
- `src/pipelines/load/loader.py` — Load stage: `DataLoader` class for PostgreSQL upserts with batch processing
- `demos/demo_e2e_full.py` — End-to-end example showing Crawl → Transform → Load flow
- `docker-compose.yaml` — Multi-service stack (Postgres, Redis, Airflow with Celery Executor); check volumes and environment variables here
- `airflow/setup/init-crawl-db.sh` — Database schema initialization script (categories/products tables)
- `data/raw/` and `data/demo/` — Canonical directories for pipeline inputs/outputs

## How to run (developer workflows)

- **Quick local run (single process)**:
  - From repo root: `python src/pipelines/crawl/crawl_categories_recursive.py` (crawl categories)
  - From repo root: `python src/pipelines/crawl/crawl_products.py` (crawl products from categories)
  - From repo root: `python src/pipelines/crawl/crawl_products_detail.py` (crawl product details)
  - E2E demo: `python demos/demo_e2e_full.py` (runs complete Crawl → Transform → Load pipeline)
  - Each script has a `main()` function that can be run directly.

- **Docker / multi-service (Airflow - recommended)**:
  - Prerequisites: Create `.env` file from `.env.example` with `POSTGRES_USER`, `POSTGRES_PASSWORD`, `_AIRFLOW_WWW_USER_USERNAME`, `_AIRFLOW_WWW_USER_PASSWORD`
  - From repo root: `docker-compose up -d --build` to start all services (PostgreSQL, Redis, Airflow stack)
  - Access Airflow UI at http://localhost:8080 (default: username `airflow`, password `airflow`)
  - PostgreSQL exposed at `localhost:5432` for external connections
  - Trigger DAG: find `tiki_crawl_products` in UI and click "Play" button

- **Code quality & CI (Makefile/PowerShell)**:
  - Unix/Linux/Mac: `make lint`, `make format`, `make test`, `make validate-dags`
  - Windows PowerShell: `.\scripts\ci.ps1 lint`, `.\scripts\ci.ps1 format`, `.\scripts\ci.ps1 test`
  - Full CI locally: `make ci-local` or `.\scripts\ci.ps1 ci-local` (runs linting, formatting, type-checking, tests, security checks)
  - Tools used: `ruff` (linting), `black` + `isort` (formatting), `mypy` (type-checking), `pytest` (testing), `bandit` (security)

- **Database management**:
  - Schema initialization happens automatically via `airflow/setup/init-crawl-db.sh` on first Postgres startup
  - Manual schema updates: check `airflow/setup/add_computed_fields.sql` or `add_missing_columns.sql`
  - Backup: `.\scripts\backup-postgres.ps1` or `scripts/backup-postgres.sh` (creates backup in `backups/postgres/`)
  - Restore: `.\scripts\restore-postgres.ps1` or similar (from `backups/postgres/`)

- **Debugging tips**:
  - Open the DAG file `airflow/dags/tiki_crawl_products_dag.py` to see task names and Python callable targets — match those names when testing locally
  - Check Airflow Variables in the UI (Admin → Variables) for configuration parameters like crawl limits
  - Inspect `src/pipelines/crawl/config.py` for configuration settings and environment variables
  - View logs: `docker-compose logs -f airflow-scheduler` or check `airflow/logs/` directory
  - For DAG import errors: `docker exec tiki-data-pipeline-airflow-scheduler-1 python -c "from airflow.models import DagBag; print(DagBag().import_errors)"`

## Patterns & conventions specific to this repo

- **Configuration-first**: runtime behavior is controlled via Airflow Variables and environment variables. When adding features, add configuration in `src/pipelines/crawl/config.py` and read from Airflow Variables or environment variables instead of hardcoding.
- **Small, pure functions** in `src/pipelines/{crawl,transform,load}/*`, side-effecting orchestration in DAGs or `main()` functions in pipeline scripts. This keeps unit testing simple.
- **Three-stage ETL architecture**:
  - **Extract** (`src/pipelines/crawl/`): Selenium-based crawlers with `main()` entry points. Uses `resilience/` for retry patterns, `storage/` for atomic file writes
  - **Transform** (`src/pipelines/transform/transformer.py`): `DataTransformer.transform_products()` normalizes JSON, validates fields, computes derived values (e.g., discount_percent)
  - **Load** (`src/pipelines/load/loader.py`): `DataLoader.load_products()` performs batch upserts to PostgreSQL with conflict resolution
- **Data layout**: pipelines write to `data/raw/` (ingest) and `data/demo/` (demo/test outputs). Preserve these locations unless adding an explicit config override.
- **Redis caching**: use `src/pipelines/crawl/redis_cache.py` for caching HTML content and rate limiting (Airflow uses Redis DB 0, pipeline can use DB 1).
- **Dynamic Task Mapping in DAGs**: the main DAG (`tiki_crawl_products_dag.py`) uses Airflow's Dynamic Task Mapping to parallelize crawling across categories. Tasks are grouped in TaskGroups for readability.
- **XCom data sharing**: DAG tasks pass data via XCom (e.g., category lists, product counts). Keep XCom payloads small; write large data to `data/` and pass file paths.
- **Atomic writes**: use `storage/atomic_writer.py` patterns for safe file writes (write to temp, then rename) to prevent corrupt files on failure.
- **Windows + Unix compatibility**: scripts exist in both `.sh` (Unix) and `.ps1` (PowerShell) variants. Makefile for Unix, `scripts/ci.ps1` for Windows.

## Integration points / external dependencies

- **Airflow**: DAGs live in `airflow/dags/`. Assume the runtime uses the compose stack; changes to DAGs usually require restarting the Airflow scheduler service in the compose stack.
  - Custom Airflow image built via `airflow/Dockerfile` (installs Chrome/Chromium for Selenium)
  - Configuration in `airflow/config/airflow.cfg` and `airflow/airflow_local_settings.py`
  - Volumes: dags, logs, plugins, and `src/` + `data/` are mounted for DAG access to pipeline code
  - Executor: CeleryExecutor with Redis as broker (allows distributed task execution)
- **Docker**: `docker-compose.yaml` at root defines local composition — use it to run services like Airflow, PostgreSQL, Redis, or other dependencies.
  - Services: `postgres` (port 5432), `redis` (port 6379), `airflow-scheduler`, `airflow-worker`, `airflow-apiserver`, `airflow-webserver`
  - Shared volume: `postgres-shared-volume` for database persistence
  - Network: `backend` network for inter-service communication
- **Configuration**: 
  - Airflow Variables (set in UI: Admin → Variables) are authoritative for crawling schedules/targets (e.g., `tiki_max_products_per_category`)
  - Environment variables in `.env` file (see `.env.example`) for credentials and service URLs
  - Never commit `.env` file (already in `.gitignore`)
- **Redis**: Used for caching and rate limiting in pipeline code. Configured in `docker-compose.yaml` and accessed via `src/pipelines/crawl/redis_cache.py`.
  - DB 0: Airflow Celery broker/result backend
  - DB 1+: Available for pipeline caching (configure in pipeline code)
- **PostgreSQL**: 
  - Two databases: `airflow` (Airflow metadata) and `crawl_data` (pipeline data)
  - Schema initialization: `airflow/setup/init-crawl-db.sh` runs on first startup (via `docker-entrypoint-initdb.d/`)
  - Tables: `categories`, `products` with JSONB columns for flexible schema
  - Connection from local: `postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:5432/crawl_data`

## What I (the AI) should do and avoid

- Do: Modify or add code in `src/pipelines/crawl/` while following the config-first and small-function conventions. Add/update configuration in `src/pipelines/crawl/config.py` when adding behavior.
- Do: Add tests alongside code changes (where practical). Tests are in `tests/` folder.
- Avoid: Hardcoding credentials or external secrets into repo files. Use environment variables or Airflow Variables and document the expected vars.

## Contract (for changes that add a new pipeline/task)

- Inputs: Airflow Variables or environment variables, configuration in `src/pipelines/crawl/config.py`.
- Outputs: files in `data/raw/` or `data/demo/` and/or metrics/logs.
- Error modes: transient network errors (implement retries in pipeline code), config missing errors (raise clear exceptions), and disk permission errors (document required write permissions to `data/`).

## Where to look for more context

- Start with the file tree: `airflow/dags/`, `src/pipelines/crawl/`, `scripts/`, `docker-compose.yaml`.
- Search the repo for TODO comments and configuration keys when implementing features.
- Check `README.md` for setup instructions and usage examples.

## Short checklist for PRs that touch runtime behavior

1. Add or update configuration in `src/pipelines/crawl/config.py` with default values.
2. Keep business logic in `src/pipelines/crawl/*` as small, testable functions.
3. Update `airflow/dags/*` only to wire new tasks (avoid heavy logic in DAG files).
4. Document new env vars or Airflow Variables in README or `docker-compose.yaml`.

If anything in these instructions is unclear or you'd like me to include CI/test commands, tell me which runner or test framework you prefer and I'll iterate.
