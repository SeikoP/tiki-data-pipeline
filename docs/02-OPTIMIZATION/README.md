# Optimization

Optimization docs are consolidated here. Older roadmap, progress and completion files were removed because they duplicated stale benchmark claims.

## Current Strategy

- Prefer Tiki API for the speed layer and use Selenium only where rendered detail data is needed.
- Batch categories and products to reduce Airflow scheduling overhead.
- Reuse HTTP, Redis and PostgreSQL connections instead of creating connections per item.
- Cache product lists, product detail responses and HTML with Redis/file fallback where supported.
- Fail fast on network stalls, then retry only for missing important fields such as seller and brand.

## Applied Areas

| Area | Current shape |
| --- | --- |
| Category crawling | Dynamic task mapping with category batches |
| Product crawling | Larger product batches and async/API-heavy fetches |
| Product detail | Selenium pool plus retry path for incomplete critical fields |
| Cache | Redis TTL split by product list, detail and HTML stability |
| Load | Bulk upsert into PostgreSQL with history preservation logic |
| Maintenance | Reconcile categories, cleanup orphan/redundant categories, cache stats and backups |

## Next Cleanup Targets

- Keep benchmark scripts only when they are referenced by active tests or CI.
- Move one-off investigation scripts out of active paths after grep verification.
- Keep performance claims in docs tied to current code and reproducible commands.
- Avoid adding new roadmap Markdown unless it replaces this README.

## Validation

```powershell
pytest tests/test_parallel_benchmark.py -q
pytest tests/test_cache_hit_rate_fix.py -q
pytest tests/test_task_startup_time.py tests/test_task_startup_metrics.py -q
```
