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
- `review_count`

`sales_count` and `rating_average` are nullable when Tiki does not return sales data or when
`review_count = 0`. Keep those values NULL instead of coercing them to zero.

`stock_available` is a verified availability signal, not a default assumption. Use `true` only
when detail/listing data explicitly says the product is available, use `false` only for trusted
out-of-stock signals such as `inventory_status = out_of_stock`, `stock_item.qty = 0`, or clear
HTML out-of-stock text. Use `NULL` with `stock_status = unknown` when the source does not expose a
reliable stock signal. `stock_source` records whether the signal came from detail API, detail HTML,
listing API, or an explicit normalized payload.

Product detail data loaded into normalized tables should come from the Tiki detail API. HTML detail
scraping is disabled for the fast async detail path because it has not provided reliable
`rating_average` or stock signals in recent crawls. Products that cannot be loaded through the API
should be marked failed/skipped instead of silently loading partial HTML-derived detail data.

`products.specifications` is intentionally removed from the normalized product table. Raw specs
from any source remain available in `raw_products.raw_json` for debugging or future enrichment.

Useful tests:

```powershell
pytest tests/test_transform_load.py tests/test_sales_count.py -q
pytest tests/test_leaf_filtering.py tests/test_hierarchy_map.py -q
```

Latest crawl database audit:

```powershell
python scripts/quality/check_latest_crawl_quality.py --crawl-run-id latest
python scripts/quality/check_latest_crawl_quality.py --crawl-run-id latest --format json
```
