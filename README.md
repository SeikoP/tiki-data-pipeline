# Tiki Data Pipeline

Pipeline thu thập, chuẩn hóa và lưu dữ liệu sản phẩm Tiki bằng Airflow, PostgreSQL
và Redis. Luồng chính hiện nằm ở DAG `tiki_crawl_fast`, ưu tiên crawl qua API,
ghi dữ liệu trung gian theo batch ra file, rồi transform và load vào PostgreSQL theo
mô hình bảng nhiều lớp.

## Tổng Quan

Repo này phục vụ các việc chính:

- Crawl cây danh mục và danh sách sản phẩm Tiki.
- Crawl chi tiết sản phẩm theo batch bất đồng bộ.
- Chuẩn hóa dữ liệu sản phẩm, người bán, giá, chỉ số và lịch sử crawl.
- Load dữ liệu vào PostgreSQL với schema idempotent.
- Dùng Redis để tăng tốc cache crawl và giảm request lặp.
- Điều phối bằng Airflow, có task bảo trì, backup và báo cáo sau mỗi lần chạy.

## Luồng Dữ Liệu Chính

```text
start_crawl_run
  -> load_categories
  -> crawl_categories
  -> merge_products
  -> prepare_products_for_detail
  -> crawl_product_batch_fast
  -> merge_product_details
  -> transform_products
  -> load_categories_to_db
  -> load_products
  -> reconcile / cleanup / report
```

Điểm cần lưu ý: ở chế độ batch, task crawl không đẩy toàn bộ product payload qua
XCom. Dữ liệu đầy đủ được ghi ra thư mục output, còn XCom chỉ giữ metadata để Airflow
không bị phình database metadata.

## Cấu Trúc Dự Án

| Đường dẫn | Vai trò |
| --- | --- |
| `airflow/dags/tiki_crawl_products_v2/` | DAG `tiki_crawl_fast`, task wrappers và bootstrap parse-time |
| `airflow/setup/` | Khởi tạo Airflow pools, Variables, connection và PostgreSQL |
| `src/common/` | Settings registry, schema guard, tracking, cache, monitoring và tiện ích chung |
| `src/pipelines/crawl/` | Logic crawl API/Selenium fallback, cache, resilience và storage |
| `src/pipelines/transform/` | Chuẩn hóa product payload thành dữ liệu load được |
| `src/pipelines/load/` | Loader PostgreSQL và database pool |
| `scripts/` | CI, backup, cleanup, config sync và công cụ vận hành |
| `tests/` | Unit test, integration test, profiling và kiểm tra hợp đồng dữ liệu |
| `docs/` | Tài liệu tham số, cấu hình, kiến trúc, tối ưu và hướng dẫn vận hành |

## Yêu Cầu Runtime

- Python 3.11.
- Docker Desktop hoặc Docker Engine có Compose plugin.
- PostgreSQL 16 và Redis 7.2 chạy qua `docker compose`.
- Airflow chạy trong container `airflow-webserver`, `airflow-scheduler`,
  `airflow-dag-processor` và `airflow-triggerer`.

Các port mặc định chỉ bind local:

| Service | URL/Port |
| --- | --- |
| Airflow UI | `http://localhost:8080/ui/` |
| PostgreSQL | `127.0.0.1:5432` |
| Redis | `127.0.0.1:6379` |

## Thiết Lập Nhanh

```powershell
Copy-Item .env.example .env
docker compose up -d --build
```

Sau khi các container healthy, mở Airflow UI tại `http://localhost:8080/ui/` và
trigger DAG `tiki_crawl_fast`.

Không dùng `docker compose down -v` nếu muốn giữ dữ liệu PostgreSQL và Redis cache.
Lệnh đó xóa cả named volume của database và cache.

## Lệnh Thường Dùng

```powershell
# Chạy toàn bộ stack local
docker compose up -d --build

# Dừng container nhưng giữ volume PostgreSQL/Redis
docker compose down

# Chạy kiểm tra CI trong container Airflow
.\scripts\ci.ps1

# Chạy CI Python khi đã cài dependency local
python scripts/ci.py ci-fast

# Chạy test nhanh, bỏ slow/integration
.\.venv\Scripts\python.exe -m pytest tests -m "not integration and not slow" -q

# Kiểm tra settings registry, .env.example, Airflow defaults và docs
.\.venv\Scripts\python.exe scripts/config/sync_config_docs.py --check

# Xem trước cleanup cache/log
.\scripts\cleanup.ps1 -Cache -Logs -DryRun
```

## CI

CI trên GitHub Actions gồm các nhóm kiểm tra chính:

- Ruff format và lint cho `src/`, `airflow/dags/`, `scripts/`, `tests/`.
- Kiểm tra đồng bộ cấu hình bằng `scripts/config/sync_config_docs.py --check`.
- Pytest với marker `not integration and not slow` và coverage cho `src/`.
- Validate import Airflow DAG với `PYTHONPATH=src:airflow/dags`.
- Code quality bổ sung: Pylint, Vulture, Bandit, dependency audit và secret scan.

Local CI chính cho môi trường Docker là:

```powershell
.\scripts\ci.ps1
```

Script này yêu cầu service `airflow-webserver` đang chạy vì các kiểm tra được thực thi
bên trong container để khớp runtime Airflow.

## Cấu Hình

Runtime configuration đi qua `src/common/settings.py`. Khi thêm hoặc đổi biến cấu
hình, cần cập nhật các nơi liên quan:

- `SETTINGS_REGISTRY` trong `src/common/settings.py`.
- `.env.example`.
- Airflow Variable defaults nếu biến cần override lúc chạy DAG.
- Tài liệu trong `docs/` nếu biến có tác động vận hành.
- Test hoặc validator cấu hình tương ứng.

Secrets như password, webhook URL và API key không được có default thật trong code,
docs hoặc test fixture.

## Dữ Liệu Và Cache

- `data/`, `backups/`, Airflow logs, cache của pytest/ruff/mypy và Docker volumes là
  runtime artifact, không commit.
- Redis dùng key prefix `tiki:crawl:` cho crawl cache.
- PostgreSQL lưu các bảng lớp như `crawl_runs`, `raw_products`, `sellers`,
  `categories`, `products`, `product_prices`, `product_metrics` và `crawl_history`.
- Muốn reset dữ liệu sản phẩm nhưng giữ Redis cache, ưu tiên truncate bảng ứng dụng
  hoặc chỉ reset Postgres volume thay vì chạy `docker compose down -v`.

## Tài Liệu

Bắt đầu tại [docs/INDEX.md](docs/INDEX.md).

| Chủ đề | Link |
| --- | --- |
| Tham số | [docs/01-PARAMETERS/README.md](docs/01-PARAMETERS/README.md) |
| Tối ưu | [docs/02-OPTIMIZATION/README.md](docs/02-OPTIMIZATION/README.md) |
| Kiến trúc | [docs/03-ARCHITECTURE/README.md](docs/03-ARCHITECTURE/README.md) |
| Cấu hình | [docs/04-CONFIGURATION/README.md](docs/04-CONFIGURATION/README.md) |
| Hiệu năng | [docs/05-PERFORMANCE/README.md](docs/05-PERFORMANCE/README.md) |
| Phân tích và cleanup | [docs/06-ANALYSIS/README.md](docs/06-ANALYSIS/README.md) |
| Hướng dẫn | [docs/07-GUIDES/README.md](docs/07-GUIDES/README.md) |
| Crawl module | [src/pipelines/crawl/README.md](src/pipelines/crawl/README.md) |
| Tests | [tests/README.md](tests/README.md) |
