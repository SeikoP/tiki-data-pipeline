# Crawl Pipeline

This module contains the Tiki crawl implementation used by the Airflow DAG and local scripts.

## Main Files

| File | Purpose |
| --- | --- |
| `config.py` | Crawl, cache, retry and data quality constants |
| `extract_category_link_selenium.py` | Initial/root category extraction |
| `crawl_categories_optimized.py` | Optimized category crawl |
| `crawl_products.py` | Product list crawl helpers |
| `crawl_products_detail.py` | Product detail extraction |
| `storage/redis_cache.py` | Redis cache integration |
| `storage/postgres_storage.py` | PostgreSQL storage helpers |
| `resilience/` | Circuit breaker, DLQ, graceful degradation and exceptions |

## Local Category Bootstrap

```powershell
python src/pipelines/crawl/extract_category_link_selenium.py
python src/pipelines/crawl/crawl_categories_optimized.py
python src/tools/build_category_hierarchy.py
```

Generated files are expected under `data/raw/`.

## Airflow Run

```powershell
docker compose up -d --build
```

Open `http://localhost:8080/ui/` and trigger `tiki_crawl_fast`.

## Key Runtime Controls

| Control | Purpose |
| --- | --- |
| `TIKI_CATEGORY_NUM_BATCHES` | Number of mapped category batches |
| `TIKI_FORCE_REFRESH` | Bypass cache for stale-data investigations |
| `PRODUCT_BATCH_SIZE` | Products per detail batch |
| `REDIS_CACHE_TTL_PRODUCT_DETAIL` | Detail cache TTL |
| `DATA_QUALITY_MIN_SCORE` | Minimum product completeness score |

## Validation

```powershell
pytest tests/test_crawl_products.py tests/test_tiki_crawl_api.py -q
pytest tests/test_crawl_retry_logic.py tests/test_cache_hit_rate_fix.py -q
```
