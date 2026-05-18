# Tiki Data Pipeline

Pipeline thu thập, chuẩn hóa và lưu dữ liệu sản phẩm Tiki bằng Airflow, PostgreSQL và Redis. Repo hiện tập trung vào DAG `tiki_crawl_fast`: crawl danh mục qua API, crawl chi tiết sản phẩm theo batch, transform dữ liệu, load vào PostgreSQL, cleanup dữ liệu phụ trợ và gửi thông báo nếu bật Discord.

## Quick Start

```powershell
Copy-Item .env.example .env
docker compose up -d --build
```

Airflow UI chạy tại `http://localhost:8080/ui/`. Sau khi các service healthy, trigger DAG `tiki_crawl_fast`.

## Main Flow

```text
load_categories + cleanup_incomplete_products
  -> crawl_categories
  -> merge_products
  -> prepare_products_for_detail
  -> crawl_product_batch_fast
  -> merge_product_details
  -> transform_products
  -> load_categories_to_db
  -> load_products
  -> reconcile/cleanup/report
```

## Project Layout

| Path | Purpose |
| --- | --- |
| `airflow/dags/tiki_crawl_products_v2/` | Airflow DAG, task wrappers and orchestration |
| `src/pipelines/crawl/` | Crawl configuration, Tiki API/Selenium helpers, cache and resilience code |
| `src/pipelines/transform/` | Product normalization and transform helpers |
| `src/pipelines/load/` | PostgreSQL loaders and DB pool helpers |
| `src/common/` | Shared monitoring, caching, profiling, notifications and utility modules |
| `scripts/` | Admin, CI, cache, backup and maintenance scripts |
| `tests/` | Unit, integration, profiling and manual validation tests |
| `docs/` | Compact project documentation index and topic READMEs |

## Common Commands

```powershell
# Run full local stack
docker compose up -d --build

# Run checks inside the Airflow container
.\scripts\ci.ps1

# Run tests directly when dependencies are installed locally
pytest tests -q

# Clear Python/tool caches and old logs
.\scripts\cleanup.ps1 -Cache -Logs
```

## Documentation

Start at [docs/INDEX.md](docs/INDEX.md).

| Topic | Link |
| --- | --- |
| Parameters | [docs/01-PARAMETERS/README.md](docs/01-PARAMETERS/README.md) |
| Optimization | [docs/02-OPTIMIZATION/README.md](docs/02-OPTIMIZATION/README.md) |
| Architecture | [docs/03-ARCHITECTURE/README.md](docs/03-ARCHITECTURE/README.md) |
| Configuration | [docs/04-CONFIGURATION/README.md](docs/04-CONFIGURATION/README.md) |
| Performance | [docs/05-PERFORMANCE/README.md](docs/05-PERFORMANCE/README.md) |
| Analysis and cleanup | [docs/06-ANALYSIS/README.md](docs/06-ANALYSIS/README.md) |
| Guides | [docs/07-GUIDES/README.md](docs/07-GUIDES/README.md) |
| Crawl module | [src/pipelines/crawl/README.md](src/pipelines/crawl/README.md) |
| Tests | [tests/README.md](tests/README.md) |

## Runtime Notes

- `.env` is local-only. Keep secrets out of git.
- `data/`, `backups/`, Airflow logs and Python cache folders are runtime artifacts.
- Redis is used for Airflow support and crawl/cache acceleration.
- PostgreSQL stores Airflow metadata and crawl data schemas initialized by `airflow/setup/`.
