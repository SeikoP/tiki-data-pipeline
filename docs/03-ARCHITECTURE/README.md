# Architecture

The active Airflow DAG is `tiki_crawl_fast` in `airflow/dags/tiki_crawl_products_v2/tiki_crawl_fast.py`.

## Data Flow

```text
pre_crawl
  cleanup_incomplete_products
  load_categories

crawl_categories
  prepare_crawl_kwargs
  crawl_category

process
  merge_products

detail
  prepare_products_for_detail
  crawl_product_batch_fast
  merge_product_details

transform_load
  load_categories_to_db
  transform_products
  load_products

maintenance_and_reports
  reconcile_categories
  cleanup_categories
  cleanup_old_history
  aggregate_and_notify
  cleanup_redis_cache
  backup_database
```

## Main Components

| Component | Location | Responsibility |
| --- | --- | --- |
| Airflow DAG | `airflow/dags/tiki_crawl_products_v2/` | Task orchestration and runtime dependencies |
| Crawl pipeline | `src/pipelines/crawl/` | Tiki API/Selenium crawling, retry, cache, storage helpers |
| Transform pipeline | `src/pipelines/transform/` | Normalize and validate product records |
| Load pipeline | `src/pipelines/load/` | PostgreSQL insert/update and connection pool code |
| Shared modules | `src/common/` | Cache, monitoring, profiling, data quality, notifications |
| Tools/scripts | `scripts/`, `src/tools/` | Admin checks, category repair, profiling and maintenance helpers |

## Storage

- PostgreSQL stores Airflow metadata and crawl data schemas initialized from `airflow/setup/`.
- Redis supports Airflow/runtime caching and crawl cache acceleration.
- `data/` contains local generated inputs and outputs and is not intended for git.
- `backups/` contains generated database backups and is not intended for git except placeholder files.
