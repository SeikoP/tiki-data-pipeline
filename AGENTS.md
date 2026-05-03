# Repository Guidelines

## Project Structure & Module Organization

This is a Python 3.11 data pipeline for crawling, transforming, and loading Tiki product data. Airflow orchestration lives in `airflow/dags/tiki_crawl_products_v2/`, with the active DAG entry point in `tiki_crawl_fast.py`. The DAG imports task callables from `airflow/dags/tiki_crawl_products_v2/tasks/`, while `bootstrap.py` owns DAG parse-time setup, lazy imports, path setup, shared constants, and local-development Airflow fallbacks.

Core code is under `src/`: shared utilities in `src/common/`, crawl logic in `src/pipelines/crawl/`, transform/load stages in `src/pipelines/transform/` and `src/pipelines/load/`, category extraction in `src/pipelines/extract/`, and standalone tools in `src/tools/`. `src/common/settings.py` is the central runtime configuration registry. `src/common/db_schema.py` and `src/common/pipeline_tracking.py` define the layered PostgreSQL schema and crawl-run/raw/snapshot persistence. Operational scripts are in `scripts/`, examples in `examples/`, documentation in `docs/`, local agent skills in `.agent/skills/`, and tests in `tests/`.

Runtime output such as `data/`, `backups/`, Airflow logs, pytest/ruff caches, Redis/Postgres Docker volumes, and generated cache folders should stay out of git. The disabled `airflow/dags/tiki_enrich_products.py.disabled` file is intentionally not an active DAG.

## Key Entry Points & Data Flow

The main DAG is `tiki_crawl_fast`. Its high-level flow is:

1. `start_crawl_run` creates or resumes a `crawl_runs` row.
2. `pre_crawl.load_categories` reads category JSON, normalizes the tree, filters leaf categories when configured, writes `loaded_categories.json`, and returns only file metadata through XCom.
3. `crawl_categories.prepare_crawl_kwargs` splits categories into mapped batch configs using `TIKI_CATEGORY_NUM_BATCHES`.
4. `crawl_categories.crawl_category` usually calls `crawl_category_batch`, which calls `crawl_single_category` per category, writes batch results to `TIKI_CATEGORY_BATCH_OUTPUT_DIR`, and returns only metadata through XCom. Do not assume full product payloads are in XCom in batch mode.
5. `process_and_save.merge_products` reads category batch output files, merges/deduplicates products, writes merged product files under `/opt/airflow/data`, and saves raw list products when a crawl run id is available.
6. `crawl_product_details` prepares detail candidates, plans detail batches with `common.detail_batch_planner`, crawls details via async API-first tasks, and merges detail output into `products_with_detail.json`.
7. `transform_and_load` loads categories first for FK safety, transforms products with `DataTransformer`, then loads normalized data with `OptimizedDataLoader` into sellers/products/snapshots.
8. `maintenance_and_reports` reconciles categories, optionally sends Discord summaries, logs Redis cache stats, backs up PostgreSQL, and finishes the crawl run.

The database model is layered: `crawl_runs`, `crawl_tasks`, `raw_products`, `sellers`, `categories`, `products`, `product_prices`, `product_metrics`, and `crawl_history`. Schema guards are idempotent and live in `src/common/db_schema.py`; loaders should call them instead of assuming tables already exist.

## Tech Stack & Runtime Services

The runtime stack is Airflow, PostgreSQL 16, Redis 7.2, Python 3.11, requests/aiohttp, BeautifulSoup/lxml, optional Selenium/webdriver-manager, psycopg2, pandas/numpy, psutil, pytest, Ruff, mypy, and Docker Compose. Airflow containers mount `./src` and `./airflow/dags` read-only, `./data` at `/opt/airflow/data`, and `./backups` at `/opt/airflow/backups`.

Docker Compose starts PostgreSQL, Redis, `airflow-webserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer`, and `airflow-init`. `airflow/setup/setup_airflow.py` creates pools, Airflow Variables from the settings registry, and the `tiki_postgres` connection. Local service ports are bound to `127.0.0.1:5432`, `127.0.0.1:6379`, and `127.0.0.1:8080`.

Redis uses the `redis-data` named volume and the crawl cache uses keys with the `tiki:crawl:` prefix. Product listing cache canonicalizes category URLs and drops page parameters for category-level cache hits. PostgreSQL uses the `postgres-shared-volume` named volume. `docker compose down -v` deletes both Postgres data and Redis cache; use plain `docker compose down` or `docker compose stop` when you want to preserve caches.

## Build, Test, and Development Commands

- `Copy-Item .env.example .env`: create local configuration.
- `docker compose up -d --build`: build and run PostgreSQL, Redis, and Airflow locally.
- `docker compose down`: stop and remove containers while preserving named volumes.
- `docker compose down -v`: destructive reset of both PostgreSQL and Redis volumes.
- `.\scripts\ci.ps1`: run container-based lint, format check, mypy, DAG validation, and tests.
- `python scripts/ci.py`: run the Python CI runner when local dependencies are installed.
- `.\.venv\Scripts\python.exe -m pytest tests/test_utils.py -q`: run a narrow local test.
- `.\.venv\Scripts\python.exe -m pytest tests/test_transform_load.py -q`: run transform/load tests after touching data normalization or load code.
- `.\.venv\Scripts\python.exe scripts/config/sync_config_docs.py --check`: verify settings registry, `.env.example`, Airflow defaults, docs, and forbidden credential fallbacks stay in sync.
- `.\scripts\cleanup.ps1 -Cache -Logs -DryRun`: preview cleanup of caches and old logs.

Prefer focused tests for the touched subsystem first, then broader checks when Docker services are available. GitHub Actions run Ruff format/check, non-slow non-integration pytest with coverage, and DAG import validation with `PYTHONPATH=src:airflow/dags`.

## Coding Style & Naming Conventions

Use Python 3.11 style with 4-space indentation and a 100-character line length. Ruff, Black-compatible formatting, isort, and mypy are configured in `pyproject.toml`. Ruff checks include pycodestyle, pyflakes, isort, bugbear, comprehensions, pyupgrade, and performance rules; line length is handled by formatting.

Prefer `snake_case` for functions/modules, `PascalCase` for classes, and uppercase names for constants such as `PRODUCT_BATCH_SIZE`. Keep Airflow task IDs descriptive and stable because they appear in logs, XCom references, task mappings, and historical runs. Avoid expensive imports at DAG parse time; follow existing lazy import patterns in `bootstrap.py` and task modules.

## Configuration & Hardcode Rules

Runtime configuration must go through `src/common/settings.py`. Do not hardcode timeouts, batch sizes, concurrency, pool slots, service hosts, database names, model names, webhook URLs, credential fallbacks, cache TTLs, or output directories directly in DAGs, tasks, pipeline code, or scripts. If a new runtime value is needed, add it to `SETTINGS_REGISTRY`, update `.env.example`, update docs when user-facing, and include validator/test coverage.

Secrets must never have non-empty defaults in code. Use `secret=True` for secret settings and keep only variable names in docs. Inline literals are acceptable only for true domain constants, enum/status values, local test fixtures, or small loop/control values that are not deployment configuration. Run `scripts/config/sync_config_docs.py --check` after changing settings.

Airflow Variables are seeded from `airflow_variable_defaults()`. If a setting needs Airflow runtime override, mark it with `airflow_variable=True` and ensure setup/tests stay consistent.

## Crawling, Caching, and XCom Gotchas

Batch category crawling writes full results to files and returns metadata through XCom. Some inner log messages may mention XCom from legacy single-category paths; verify the actual wrapper return before changing storage behavior. Avoid pushing large product lists through Airflow XCom unless the task is explicitly non-batch and the payload is known to be small.

Redis cache misses after `docker compose down -v` are expected because the Redis named volume is deleted. Cache hit behavior depends on canonicalized URLs, cache type, TTL, and flags such as `TIKI_FORCE_REFRESH` or detail cache refresh settings. Cleanup tasks log Redis stats; they should not flush cache unless deliberately changed.

The crawler is API-first and may fall back to Selenium only when configured. `crawl_products.py` keeps Selenium imports lazy to protect DAG parse time. Rate limiting, circuit breaker, graceful degradation, and dead-letter queue utilities live under `src/pipelines/crawl/resilience/` and `src/pipelines/crawl/storage/`.

## Transform, AI, and Data Quality

`DataTransformer` normalizes product IDs, names, URLs, prices, ratings, seller fields, JSON payloads, category paths, descriptions, and quality metadata. Required quality fields and enrichment fields are declared in `src/pipelines/transform/transformer.py`. Missing enrichment fields should remain NULL-like instead of becoming fake zeroes.

Short product names should be generated by local heuristic rules first. `SHORT_NAME_USE_AI` is optional and should not put external AI calls on the transform critical path. `src/common/ai/summarizer.py` has in-memory process-local caches only; persisted short names live in `products.short_name`. If AI providers return 401/429, code should fail open to local fallback rather than blocking the DAG.

Vectorized transform and optimized loader paths are feature-flagged. Keep baseline behavior and optimized behavior equivalent, and update `tests/profiling/` or transform/load tests when changing shared contracts.

## Loading and Database Rules

Use `OptimizedDataLoader` and `common.pipeline_tracking.load_layered_products` for product loads unless a task has a clear reason not to. Categories must be loaded before products because `products.category_id` references `categories.category_id`. Loaders are responsible for preserving existing non-empty data on upsert and for writing price/metric snapshots by crawl run.

When testing data quality and needing a clean product database while keeping Redis cache, prefer truncating application tables or removing only the Postgres volume. Do not use `docker compose down -v` unless you intentionally want to reset both database and cache.

## Testing Guidelines

Pytest is the test framework. Test files should match `test_*.py` or `*_test.py`, classes `Test*`, and functions `test_*`. Use markers `slow` and `integration` where appropriate. Avoid adding tests that require live network or mutable local data unless clearly marked or isolated.

Use targeted tests for changed areas:

- Settings/config: `tests/test_settings_config.py` and `scripts/config/sync_config_docs.py --check`.
- Transform/load contracts: `tests/test_transform_load.py`, `tests/test_layered_schema.py`, and loader-specific tests.
- Redis/cache behavior: `tests/test_redis_*`, `tests/test_cache_hit_rate_fix.py`, and crawl cache tests.
- DAG/import behavior: `tests/test_bootstrap_lazy_loading.py` and DAG validation scripts.
- Performance/optimization modules: `tests/profiling/`.

Manual/debug scripts in `tests/`, `scripts/helper/`, and `scripts/imp/` are useful for diagnosis but should not become hidden dependencies for normal CI.

## Documentation and Operational Scripts

Documentation is organized under `docs/01-PARAMETERS` through `docs/07-GUIDES` with `docs/INDEX.md` as the index. Update docs and `.env.example` when changing behavior, commands, schema expectations, or environment variables. Configuration docs are partially enforced by `scripts/config/sync_config_docs.py`.

Operational scripts include backup/cleanup, Airflow optimization setup, schema updates, Redis management, profiling, benchmark, and admin repair utilities. Before deleting or renaming scripts, tasks, tests, or docs, verify references with `rg` when available and avoid breaking Airflow task imports or CI scripts.

## Security & Configuration Tips

Never commit `.env`, real credentials, generated data, backups, logs, or local Airflow auth files with real secrets. `passwords.json` and Airflow simple-auth files are runtime-sensitive; treat them as secrets even when templates exist. Keep webhook URLs and AI/API keys out of source code and tests.

Prefer `REDIS_PASSWORD`, `POSTGRES_PASSWORD`, Airflow Variables, and local env files for secrets. The settings registry must keep secret defaults empty. Do not add credential fallback literals for local convenience.

## Commit & Pull Request Guidelines

Prefer concise Conventional Commit subjects such as `fix: handle missing seller name` or `docs: refresh contributor guide`. PRs should include a short description, change type, local test evidence, and screenshots only for UI changes. Update `README.md`, `docs/`, `.env.example`, or config validation tests when behavior, commands, schema, or environment variables change.

Be careful in dirty worktrees. Do not revert unrelated user changes. If a file contains existing edits you did not make, read it and work with those changes rather than resetting the file.


# Agent Instructions
- Always read this file first before exploring the codebase
- Use `rg` for search, never grep
- Ask which module is relevant before reading files blindly