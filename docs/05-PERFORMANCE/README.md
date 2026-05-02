# Performance

This README keeps only current, verifiable performance guidance. Historical benchmark claims were removed from the active docs.

## Current Levers

| Lever | Where | Impact |
| --- | --- | --- |
| Category batching | Airflow Variable `TIKI_CATEGORY_NUM_BATCHES` | Controls mapped category task count |
| Product batch size | `PRODUCT_BATCH_SIZE` | Balances throughput, memory and retry cost |
| HTTP connector limits | `HTTP_CONNECTOR_LIMIT*` | Controls API concurrency |
| Redis TTLs | `REDIS_CACHE_TTL_*` | Improves repeat-run speed and reduces duplicate fetches |
| Data quality retry | `PRODUCT_RETRY_*`, wait settings | Improves completeness for seller/brand fields |
| Airflow parallelism | `docker-compose.yaml` | Controls local task scheduling capacity |

## Expected Bottlenecks

- Tiki network/API latency and rate limiting.
- Selenium startup and rendered detail extraction.
- PostgreSQL bulk load and history writes.
- Airflow scheduling overhead for too many tiny tasks.
- Excessive logging or cache bypass during local debugging.

## Checks

```powershell
pytest tests/test_parallel_benchmark.py -q
pytest tests/test_cache_hit_rate_fix.py -q
pytest tests/profiling -q
```

Use benchmark output as local evidence only. If hardware, network, Docker resources or Tiki responses change, update any performance notes before publishing numbers.
