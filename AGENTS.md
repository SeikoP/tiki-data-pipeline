# Repository Guidelines

## Project Structure & Module Organization

This is a Python data pipeline for Tiki crawling and loading. Airflow orchestration lives in `airflow/dags/tiki_crawl_products_v2/`. Core code is under `src/`: shared utilities in `src/common/`, crawl logic in `src/pipelines/crawl/`, transform/load stages in `src/pipelines/transform/` and `src/pipelines/load/`, and standalone tools in `src/tools/`. Operational scripts are in `scripts/`, examples in `examples/`, documentation in `docs/`, and tests in `tests/`. Runtime output such as `data/`, `backups/`, Airflow logs, and cache folders should stay out of git.

## Build, Test, and Development Commands

- `Copy-Item .env.example .env`: create local configuration.
- `docker compose up -d --build`: build and run PostgreSQL, Redis, and Airflow locally.
- `.\scripts\ci.ps1`: run container-based lint, format check, mypy, and tests.
- `.\.venv\Scripts\python.exe -m pytest tests/test_utils.py -q`: run a narrow local test.
- `.\scripts\cleanup.ps1 -Cache -Logs -DryRun`: preview cleanup of caches and old logs.

## Coding Style & Naming Conventions

Use Python 3.11 style with 4-space indentation and a 100-character line length. Ruff, Black-compatible formatting, isort, and mypy are configured in `pyproject.toml`. Prefer `snake_case` for functions/modules, `PascalCase` for classes, and uppercase names for constants such as `PRODUCT_BATCH_SIZE`. Keep Airflow task IDs descriptive and stable because they appear in logs and XCom references.

## Testing Guidelines

Pytest is the test framework. Test files should match `test_*.py` or `*_test.py`, classes `Test*`, and functions `test_*`. Use markers `slow` and `integration` where appropriate. Run focused tests for the touched subsystem first, then broader checks when Docker services are available. Avoid adding tests that require live network or mutable local data unless clearly marked or isolated.

## Commit & Pull Request Guidelines

Prefer concise Conventional Commit subjects such as `fix: handle missing seller name` or `docs: refresh contributor guide`. PRs should include a short description, change type, local test evidence, and screenshots only for UI changes. Update `README.md`, `docs/`, or `.env.example` when behavior, commands, or environment variables change.

## Security & Configuration Tips

Never commit `.env`, credentials, generated data, backups, or logs. Keep secrets in local env files and document only variable names. Before deleting scripts, tests, or docs, verify references with `rg` and avoid reverting unrelated dirty work.

## Configuration & Hardcode Rules

Runtime configuration must go through `src/common/settings.py`. Do not hardcode timeouts, batch sizes, concurrency, pool slots, service hosts, database names, model names, webhook URLs, or credential fallbacks directly in DAGs, tasks, pipeline code, or scripts. If a new runtime value is needed, add it to the settings registry, update `.env.example` and docs, and include validator/test coverage. Inline literals are acceptable only for true domain constants, enum/status values, local test fixtures, or small loop/control values that are not deployment configuration. Secrets must never have non-empty defaults in code.
