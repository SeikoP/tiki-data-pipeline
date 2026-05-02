# Analysis And Cleanup

This page replaces the previous dead-code, data-cleaning and optimization summary documents.

## Repo Cleanup Rules

- Do not delete code, tests or scripts only because they look old.
- First check references with `rg`.
- For tests, also check `pyproject.toml`, CI workflows and `tests/README.md`.
- For scripts, check docs, workflows, Docker Compose, Airflow tasks and scheduled maintenance usage.
- Delete generated caches/logs freely when they are ignored and reproducible.

## Safe Cleanup

```powershell
.\scripts\cleanup.ps1 -Cache -Logs -DryRun
.\scripts\cleanup.ps1 -Cache -Logs
```

Safe targets:

- `.mypy_cache`
- `.pytest_cache`
- `.ruff_cache`
- `__pycache__`
- old files under `airflow/logs`

## Risky Cleanup

Review before deleting:

- `data/raw`, `data/processed`, `data/demo`, `data/test_output`
- `backups`
- manual tests under `tests/manual` and database probes under `tests/pg`
- examples that demonstrate active `src/common` or `src/pipelines` modules
- one-off admin scripts that repair categories or database state

## Data Quality Checks

Focus on fields used by load and reporting:

- `product_id`
- `name`
- `price`
- `category_id`
- `seller_name`
- `brand`
- `sales_count`
- `review_count`

Useful tests:

```powershell
pytest tests/test_transform_load.py tests/test_sales_count.py -q
pytest tests/test_leaf_filtering.py tests/test_hierarchy_map.py -q
```
