# Tiki Data Pipeline

Pipeline thu thập, chuẩn hóa và phân tích dữ liệu sản phẩm Tiki bằng Airflow,
PostgreSQL, Redis và Python.

Mục tiêu chính: crawl dữ liệu sản phẩm, lưu theo mô hình nhiều lớp, tạo dữ liệu
sạch cho phân tích/dashboard Power BI và theo dõi chất lượng crawl.

## Luồng Chính

```text
Tiki API
-> raw files / raw_products
-> products / categories / sellers / price snapshots
-> analytics mart
-> Power BI
```

DAG crawl chính: `tiki_crawl_fast`.

Điểm quan trọng: dữ liệu batch lớn được ghi ra file, XCom chỉ giữ metadata để
tránh làm phình Airflow metadata database.

## Thành Phần Chính

| Đường dẫn | Vai trò |
| --- | --- |
| `airflow/dags/tiki_crawl_products_v2/` | DAG crawl sản phẩm Tiki |
| `airflow/dags/tiki_analytics_normalize.py` | DAG tạo analytics mart |
| `src/common/` | Settings, schema, tracking, cache và tiện ích chung |
| `src/pipelines/crawl/` | Crawl category, product list và product detail |
| `src/pipelines/transform/` | Chuẩn hóa dữ liệu sản phẩm |
| `src/pipelines/load/` | Load dữ liệu vào PostgreSQL |
| `src/pipelines/analytics/` | Tạo bảng/view phục vụ phân tích |
| `scripts/` | CI, cleanup, backup và công cụ vận hành |
| `tests/` | Test và kiểm tra hợp đồng dữ liệu |
| `docs/` | Tài liệu kỹ thuật |
| `graduation_docs/` | Tài liệu đồ án tốt nghiệp, không commit |

## Chạy Local

Yêu cầu:

- Python 3.11.
- Docker Desktop hoặc Docker Engine có Compose plugin.

Thiết lập:

```powershell
Copy-Item .env.example .env
docker compose up -d --build
```

Sau đó mở Airflow UI:

```text
http://localhost:8080/ui/
```

Trigger DAG:

- `tiki_crawl_fast`: crawl và load dữ liệu sản phẩm.
- `tiki_analytics_normalize`: tạo analytics mart cho dashboard.

Không dùng `docker compose down -v` nếu muốn giữ dữ liệu PostgreSQL và Redis
cache. Lệnh đó xóa volume.

## Lệnh Hay Dùng

```powershell
# Chạy stack local
docker compose up -d --build

# Dừng container, giữ volume
docker compose down

# CI trong container Airflow
.\scripts\ci.ps1

# CI nhanh bằng Python local
python scripts/ci.py ci-fast

# Test nhanh
.\.venv\Scripts\python.exe -m pytest tests -m "not integration and not slow" -q

# Kiểm tra cấu hình và docs
.\.venv\Scripts\python.exe scripts/config/sync_config_docs.py --check
```

## Cấu Hình

Runtime configuration đi qua `src/common/settings.py`.

Khi thêm biến cấu hình, cập nhật đồng bộ:

- `SETTINGS_REGISTRY`.
- `.env.example`.
- Airflow Variable defaults nếu cần override khi chạy DAG.
- Tài liệu liên quan trong `docs/`.
- Test/validator cấu hình.

Secrets như password, webhook URL và API key không được có default thật trong
code, docs hoặc test fixture.

## Dữ Liệu

- `data/`, `backups/`, Airflow logs, cache và Docker volumes là runtime artifact.
- Redis dùng prefix `tiki:crawl:` cho cache crawl.
- PostgreSQL lưu dữ liệu theo lớp: raw, core normalized và analytics mart.
- Muốn reset dữ liệu nhưng giữ Redis cache, ưu tiên truncate bảng ứng dụng thay
  vì chạy `docker compose down -v`.

## Tài Liệu

- Tài liệu kỹ thuật: [docs/INDEX.md](docs/INDEX.md)
- Tài liệu tham số: [docs/01-PARAMETERS/README.md](docs/01-PARAMETERS/README.md)
- Tài liệu crawl module: [src/pipelines/crawl/README.md](src/pipelines/crawl/README.md)
- Tài liệu test: [tests/README.md](tests/README.md)
- Tài liệu đồ án: `graduation_docs/README.md`
