## Purpose

This file tells an AI coding assistant how to be immediately productive in this repository. It focuses on concrete, discoverable conventions, the main runtime/workflow entry points, and where to look for integration details.

## Quick orientation (big picture)

- Repo purpose: a small data pipeline that crawls, processes and stores data from Tiki.vn. Key pieces are Airflow DAGs (scheduling), pipeline code under `src/pipelines/crawl/*`, and Docker Compose for multi-service runs (Airflow/worker stack).
- Primary runtime environments: local Python process for quick runs (each pipeline script has a `main()` function) and Docker (see `docker-compose.yaml` at root) for multi-service runs (Airflow/worker stack).

## Important paths (examples to open first)

- `airflow/dags/tiki_crawl_products_dag.py` — scheduler definitions; shows how pipelines are wired to Airflow.
- `src/pipelines/crawl/` — pipeline implementation folder. Keep logic here and small, testable functions.
- `src/pipelines/crawl/crawl_products.py`, `crawl_products_detail.py`, `crawl_categories_recursive.py` — main pipeline scripts with `main()` functions for local/manual runs.
- `src/pipelines/crawl/config.py` — configuration settings and environment variable handling.
- `src/pipelines/crawl/utils.py` — shared utilities for crawling operations.
- `docker-compose.yaml` — brings up local services. Use this for reproducing multi-service workflows.
- `data/raw/` and `data/demo/` — canonical input/output directories used by pipelines.

## How to run (developer workflows)

- Quick local run (single process):

  - From repo root: `python src/pipelines/crawl/crawl_categories_recursive.py` (crawl categories)
  - From repo root: `python src/pipelines/crawl/crawl_products.py` (crawl products from categories)
  - From repo root: `python src/pipelines/crawl/crawl_products_detail.py` (crawl product details)
  - Each script has a `main()` function that can be run directly.

- Docker / multi-service (Airflow):

  - From repo root: `docker-compose up -d --build` to start all services.
  - Access Airflow UI at http://localhost:8080 (username: `airflow`, password: `airflow`).

- Debugging tips:

  - Open the DAG file `airflow/dags/tiki_crawl_products_dag.py` to see task names and Python callable targets — match those names when testing locally.
  - Check Airflow Variables in the UI (Admin → Variables) for configuration parameters.
  - Inspect `src/pipelines/crawl/config.py` for configuration settings and environment variables.

## Patterns & conventions specific to this repo

- Configuration-first: runtime behavior is controlled via Airflow Variables and environment variables. When adding features, add configuration in `src/pipelines/crawl/config.py` and read from Airflow Variables or environment variables instead of hardcoding.
- Small, pure functions in `src/pipelines/crawl/*`, side-effecting orchestration in DAGs or `main()` functions in pipeline scripts. This keeps unit testing simple.
- Data layout: pipelines write to `data/raw/` (ingest) and `data/demo/` (demo/test outputs). Preserve these locations unless adding an explicit config override.
- Redis caching: use `src/pipelines/crawl/redis_cache.py` for caching HTML content and rate limiting.

## Integration points / external dependencies

- Airflow: DAGs live in `airflow/dags/`. Assume the runtime uses the compose stack; changes to DAGs usually require restarting the Airflow scheduler service in the compose stack.
- Docker: `docker-compose.yaml` at root defines local composition — use it to run services like Airflow, PostgreSQL, Redis, or other dependencies.
- Configuration: Airflow Variables (set in UI: Admin → Variables) and environment variables are authoritative for crawling schedules/targets.
- Redis: Used for caching and rate limiting. Configured in `docker-compose.yaml` and accessed via `src/pipelines/crawl/redis_cache.py`.

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
