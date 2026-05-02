# Documentation Index

Updated: 2026-05-03

This docs tree is intentionally compact. Historical optimization reports, duplicate parameter tables and one-off completion summaries were consolidated into the topic READMEs below.

## Entry Points

| Topic | File | Use when |
| --- | --- | --- |
| Parameters | [01-PARAMETERS/README.md](01-PARAMETERS/README.md) | Tuning crawl size, cache TTL, concurrency, timeout and data quality thresholds |
| Optimization | [02-OPTIMIZATION/README.md](02-OPTIMIZATION/README.md) | Understanding current speed-focused choices and next cleanup work |
| Architecture | [03-ARCHITECTURE/README.md](03-ARCHITECTURE/README.md) | Understanding the Airflow DAG and data flow |
| Configuration | [04-CONFIGURATION/README.md](04-CONFIGURATION/README.md) | Setting up Docker, Airflow, PostgreSQL, Redis and local env |
| Performance | [05-PERFORMANCE/README.md](05-PERFORMANCE/README.md) | Checking expected metrics, bottlenecks and validation commands |
| Analysis | [06-ANALYSIS/README.md](06-ANALYSIS/README.md) | Cleanup candidates, data quality checks and repo hygiene |
| Guides | [07-GUIDES/README.md](07-GUIDES/README.md) | Common local workflows and troubleshooting |
| Crawl module | [../src/pipelines/crawl/README.md](../src/pipelines/crawl/README.md) | Running or changing crawl code directly |
| Tests | [../tests/README.md](../tests/README.md) | Choosing which test set to run |

## Current Repo Shape

```text
tiki-data-pipeline/
├── airflow/
│   ├── dags/tiki_crawl_products_v2/
│   └── setup/
├── docs/
├── examples/
├── scripts/
├── src/
│   ├── common/
│   ├── pipelines/
│   └── tools/
└── tests/
```

## Cleanup Policy

- Keep docs short and close to the current code.
- Prefer updating an existing README over adding a new Markdown file.
- Do not document generated data, local secrets, logs or cache output.
- Before deleting script/test/code files, verify references with `rg` and run the narrow tests that cover the area.

## Validation Checklist

```powershell
rg "OLD_FILE_NAME.md|old_script_name" README.md docs src scripts tests airflow .github .kiro
pytest tests -q
ruff check src/ airflow/dags/ scripts/ tests/
git status --short
```
