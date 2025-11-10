## Purpose

This file tells an AI coding assistant how to be immediately productive in this repository. It focuses on concrete, discoverable conventions, the main runtime/workflow entry points, and where to look for integration details.

## Quick orientation (big picture)

- Repo purpose: a small data pipeline that crawls, processes and stores data. Key pieces are Airflow DAGs (scheduling), pipeline code under `src/pipelines/*`, a backend entrypoint `src/backend/main.py`, and configs under `configs/`.
- Primary runtime environments: local Python process for quick runs and Docker (see `docker/docker-compose.yaml`) for multi-service runs (Airflow/worker stack).

## Important paths (examples to open first)

- `airflow/dags/tiki_daily_crawl.py` — scheduler definitions; shows how pipelines are wired to Airflow.
- `src/pipelines/crawl/`, `src/pipelines/load/`, `src/pipelines/transform/` — pipeline implementation folders. Keep logic here and small, testable functions.
- `src/backend/main.py` — backend/process entrypoint used for local/manual runs.
- `src/utils/http_client.py` — shared HTTP helpers; use these for external requests to keep retry/timeouts consistent.
- `configs/crawler.yaml` — YAML-driven configuration for crawls. Prefer reading settings from these files rather than hardcoding.
- `docker/docker-compose.yaml` — brings up local services. Use this for reproducing multi-service workflows.
- `data/raw/` and `data/processed/` — canonical input/output directories used by pipelines.

## How to run (developer workflows)

- Quick local run (single process):

  - From repo root: `python src/backend/main.py` (this is the simple entrypoint used for manual runs).

- Docker / multi-service (Airflow):

  - From repo root: `docker compose -f docker/docker-compose.yaml up --build` or `docker-compose -f docker/docker-compose.yaml up --build` depending on your Docker CLI.

- Debugging tips:

  - Open the DAG file `airflow/dags/tiki_daily_crawl.py` to see task names and Python callable targets — match those names when testing locally.
  - Inspect `configs/*.yaml` for run parameters used by pipelines.

## Patterns & conventions specific to this repo

- Configuration-first: runtime behavior is controlled via YAML files in `configs/`. When adding features, add a config key and read it at pipeline start instead of scattering literal constants.
- Small, pure functions in `src/pipelines/*`, side-effecting orchestration in `src/backend/main.py` or the DAGs. This keeps unit testing simple.
- Networking centralized: use `src/utils/http_client.py` for HTTP calls so timeout and retry policies are consistent across crawlers.
- Data layout: pipelines write to `data/raw/` (ingest) and `data/processed/` (post-transform). Preserve these locations unless adding an explicit config override.

## Integration points / external dependencies

- Airflow: DAGs live in `airflow/dags/`. Assume the runtime uses the compose stack; changes to DAGs usually require restarting the Airflow scheduler service in the compose stack.
- Docker: `docker/docker-compose.yaml` defines local composition — use it to run services like Airflow, DBs, or other dependencies.
- Configs: `configs/*.yaml` are authoritative for crawling schedules/targets.

## What I (the AI) should do and avoid

- Do: Modify or add code in `src/` while following the config-first and small-function conventions. Add/update `configs/*.yaml` when adding behavior.
- Do: Add tests alongside code changes (where practical). There are no tests currently in the repo — if you add tests, place them under `tests/` and include a README note.
- Avoid: Hardcoding credentials or external secrets into repo files. Use environment variables or Docker secrets and document the expected env vars.

## Contract (for changes that add a new pipeline/task)

- Inputs: configuration YAML key (configs/), optional environment variables.
- Outputs: files in `data/raw/` or `data/processed/` and/or metrics/logs.
- Error modes: transient network errors (use retries in `src/utils/http_client.py`), config missing errors (raise clear exceptions), and disk permission errors (document required write permissions to `data/`).

## Where to look for more context

- Start with the file tree: `airflow/dags/`, `src/pipelines/`, `src/utils/`, `configs/`, `docker/`.
- Search the repo for TODO comments and configuration keys when implementing features.

## Short checklist for PRs that touch runtime behavior

1. Add or update `configs/*.yaml` with default values.
2. Keep business logic in `src/pipelines/*` as small, testable functions.
3. Update `airflow/dags/*` only to wire new tasks (avoid heavy logic in DAG files).
4. Document new env vars in README or `docker/docker-compose.yaml`.

If anything in these instructions is unclear or you'd like me to include CI/test commands, tell me which runner or test framework you prefer and I'll iterate.
