# Parameters

This file is the canonical parameter reference for the current compact docs set.
Runtime defaults are defined in `src/common/settings.py`; Airflow setup, docs and
`.env.example` should stay in sync with that registry.

## Airflow and Run Size

| Parameter | Default/current source | Purpose |
| --- | --- | --- |
| `TIKI_CATEGORY_NUM_BATCHES` | Airflow Variable, fallback `6` | Splits categories for dynamic task mapping |
| `TIKI_PRODUCTS_PER_DAY` | Airflow/env dependent | Limits daily product volume when enabled by tasks |
| `TIKI_DETAIL_ACTIVE_BATCHES` | Airflow Variable, fallback `3` | Detail mapped batches to fill one Airflow wave |
| `TIKI_DETAIL_BATCH_SIZE` | Airflow Variable, fallback `300` | Soft target before planner allows more than one wave |
| `TIKI_DETAIL_MAX_CONCURRENT_TASKS` | Airflow Variable, fallback `4` | Async requests inside each detail task |
| `TIKI_DETAIL_CONCURRENCY_MIN` | Airflow Variable, fallback `2` | Minimum clamp for per-task async concurrency |
| `TIKI_DETAIL_CONCURRENCY_MAX` | Airflow Variable, fallback `6` | Maximum clamp for per-task async concurrency |
| `TIKI_SAVE_BATCH_SIZE` | Registry/env fallback `1000` | Database save batch size for local stability |
| `TIKI_LOAD_MODE` | Airflow Variable, fallback `auto` | Selects safe or optimized DB batch sizing |
| `TIKI_FORCE_REFRESH` | env/Airflow usage | Bypass cache when stale or incomplete data must be refreshed |
| `DISCORD_ENABLED` | env, default `false` | Enables final aggregate Discord notification |

## Crawl Config

Defined in `src/pipelines/crawl/config.py`.

| Setting | Current value | Purpose |
| --- | ---: | --- |
| `MAX_CATEGORY_LEVELS` | `5` | Maximum Tiki category depth |
| `CATEGORY_BATCH_SIZE` | `20` | Categories per crawl batch |
| `CATEGORY_CONCURRENT_REQUESTS` | `10` | Parallel category API requests |
| `PRODUCT_BATCH_SIZE` | `50` | Products per detail batch |
| `PRODUCT_POOL_SIZE` | `12` | Selenium driver pool target for product detail |
| `HTTP_CONNECTOR_LIMIT` | `300` | Total aiohttp connections |
| `HTTP_CONNECTOR_LIMIT_PER_HOST` | `50` | Per-host aiohttp limit |
| `HTTP_TIMEOUT_TOTAL` | `15` | Total HTTP timeout in seconds |

## Cache Config

| Setting | Current value | Purpose |
| --- | ---: | --- |
| `REDIS_CACHE_TTL_PRODUCT_DETAIL` | `604800` | Product detail cache TTL, 7 days |
| `REDIS_CACHE_TTL_PRODUCT_LIST` | `43200` | Product list cache TTL, 12 hours |
| `REDIS_CACHE_TTL_HTML` | `604800` | HTML cache TTL, 7 days |
| `CACHE_ACCEPT_PARTIAL_DATA` | `True` | Allows partial cached records when useful |

## Data Quality

| Setting | Current value | Purpose |
| --- | --- | --- |
| `DATA_QUALITY_CRITICAL_FIELDS` | `name`, `price`, `product_id` | Required to save a product |
| `DATA_QUALITY_IMPORTANT_FIELDS` | `seller_name`, `brand`, `category_id` | Missing values can trigger retry |
| `DATA_QUALITY_OPTIONAL_FIELDS` | `rating_average`, `sales_count`, `stock_quantity` | Nice-to-have fields |
| `DATA_QUALITY_MIN_SCORE` | `0.7` | Minimum completeness score |
| `PRODUCT_RETRY_MAX_ATTEMPTS` | `2` | Retry count for missing important fields |
| `TIKI_AUDIT_HTTP_TIMEOUT` | `20` | Live Tiki API timeout for DB audit |
| `TIKI_AUDIT_REQUEST_DELAY` | `0.1` | Delay between live audit requests |
| `TIKI_AUDIT_MAX_PRODUCTS` | `0` | Optional product cap for live audit; `0` means no cap |

## Tuning Rules

- Increase `PRODUCT_BATCH_SIZE` only when memory and target-site stability are acceptable.
- Increase `HTTP_CONNECTOR_LIMIT` with care; it can improve API throughput but can also amplify rate limits.
- Use `TIKI_FORCE_REFRESH=true` for stale cache investigations, then turn it off for normal runs.
- Keep Redis TTL high for detail pages and lower for product lists because list contents change more often.
