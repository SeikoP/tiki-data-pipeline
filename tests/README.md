# Tests

The test suite mixes unit tests, integration-style tests, profiling tests and manual checks. Use narrow test commands when changing a specific subsystem.

## Active Top-Level Tests

| Area | Files |
| --- | --- |
| Crawl | `test_crawl_products.py`, `test_tiki_crawl_api.py`, `test_crawl_retry_logic.py` |
| Transform/load | `test_transform_load.py`, `test_postgres_bulk.py`, `test_sales_count.py` |
| Cache/Redis | `test_cache_hit_rate_fix.py`, `test_redis_cache_fix.py`, `test_full_redis_flow.py`, `test_redis_*` |
| Airflow/bootstrap | `test_bootstrap_lazy_loading.py`, `test_airflow_optimization_config.py`, `test_apply_airflow_optimization.py` |
| Monitoring/notifications | `test_aggregate_and_notify.py`, `test_ai_summary_discord.py` |
| Category hierarchy | `test_hierarchy_map.py`, `test_leaf_filtering.py` |
| Performance/startup | `test_parallel_benchmark.py`, `test_task_startup_time.py`, `test_task_startup_metrics.py` |
| Utilities/env | `test_utils.py`, `test_environment_loader.py` |

## Subdirectories

| Path | Purpose |
| --- | --- |
| `tests/profiling/` | Profiling, optimization and property-style checks |
| `tests/manual/` | Manual/import checks not intended for every CI run |
| `tests/pg/` | PostgreSQL local probes and data loading checks |

## Common Commands

```powershell
pytest tests -q
pytest tests/test_transform_load.py tests/test_sales_count.py -q
pytest tests/test_crawl_products.py tests/test_crawl_retry_logic.py -q
pytest tests/profiling -q
```

Run `.\scripts\ci.ps1` when the Docker/Airflow stack is running and you want the same broad checks used locally.

## Cleanup Rule

Before deleting a test, check references:

```powershell
rg "test_file_name_without_py|test_file_name.py" .github docs scripts src airflow tests pyproject.toml
```

If no reference exists, run the nearest remaining tests and update this README in the same change.
