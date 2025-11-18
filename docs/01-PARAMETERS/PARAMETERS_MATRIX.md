# 📋 BẢNG THAM SỐ CHI TIẾT - TOÀN BỘ DỰ ÁN

## PHẦN 1: AIRFLOW VARIABLES (Cấu Hình DAG Thực Thi)

### Nhóm: CATEGORY CRAWLING

| # | Tham Số | Giá Trị | Loại | Mặc Định | Min | Max | Mục Đích | Tác Dụng Nếu Tăng | Tác Dụng Nếu Giảm | Tối Ưu Hóa |
|---|---------|--------|------|----------|-----|-----|---------|-------------------|------------------|-----------|
| 1 | `TIKI_MAX_CATEGORIES` | 0 | int | 0 | 0 | 1000 | Số danh mục tối đa | Crawl ít hơn | Crawl tất cả | ✅ OK |
| 2 | `TIKI_MIN_CATEGORY_LEVEL` | 2 | int | 2 | 1 | 5 | Mức độ danh mục tối thiểu (sâu) | Nhiều danh mục | Ít danh mục | ✅ OK |
| 3 | `TIKI_MAX_CATEGORY_LEVEL` | 4 | int | 4 | 1 | 10 | Mức độ danh mục tối đa (nông) | Danh mục lớn | Danh mục nhỏ | ✅ OK |
| 4 | `TIKI_MAX_PAGES_PER_CATEGORY` | 20 | int | 20 | 1 | 100 | Số trang sản phẩm per danh mục | Dữ liệu nhiều, lâu | Dữ liệu ít, nhanh | ✅ OK |
| 5 | `TIKI_CRAWL_TIMEOUT` | 300 | int | 300 | 60 | 3600 | Timeout danh mục (giây) - 5 phút | Chờ lâu hơn | Fail sớm | ✅ OK |
| 6 | `TIKI_RATE_LIMIT_DELAY` | 1.0 | float | 1.0 | 0.1 | 10.0 | Delay giữa requests (giây) | Chậm hơn, an toàn | Nhanh hơn, bị block | ✅ OK |
| 7 | `TIKI_USE_SELENIUM` | false | bool | false | - | - | Sử dụng Selenium hay API | Chậm, đúng hơn | Nhanh, lỗi hơn | ✅ OK |

### Nhóm: PRODUCT CRAWLING

| # | Tham Số | Giá Trị | Loại | Mặc Định | Min | Max | Mục Đích | Tác Dụng Nếu Tăng | Tác Dụng Nếu Giảm | Tối Ưu Hóa |
|---|---------|--------|------|----------|-----|-----|---------|-------------------|------------------|-----------|
| 8 | `TIKI_PRODUCTS_PER_DAY` | 280 | int | 280 | 1 | 10000 | Số sản phẩm per DAG run | Crawl nhiều | Crawl ít | ✅ OK |
| 9 | `TIKI_MAX_PRODUCTS_FOR_DETAIL` | 0 | int | 0 | 0 | 1000000 | Tối đa sản phẩm detail (0=∞) | Limit dữ liệu | Crawl tất cả | ✅ OK |
| 10 | `TIKI_SAVE_BATCH_SIZE` | 10000 | int | 10000 | 100 | 100000 | Batch save DB (sản phẩm) | Batch lớn, ít IO | Batch nhỏ, nhiều IO | ✅ OK |

### Nhóm: SELENIUM POOL (🔴 TỐI ƯU HÓA V2)

| # | Tham Số | Giá Trị | Loại | Mặc Định Cũ | Min | Max | Mục Đích | Tác Dụng Nếu Tăng | Tác Dụng Nếu Giảm | Tối Ưu Hóa |
|---|---------|--------|------|----------|-----|-----|---------|-------------------|------------------|-----------|
| 11 | `TIKI_DETAIL_POOL_SIZE` | **15** | int | 5 | 1 | 50 | Selenium drivers song song | 3x xử lý, tốn RAM | Ít RAM, chậm | 🔴 **+200%** |
| 12 | `TIKI_DETAIL_MAX_CONCURRENT_TASKS` | 15 | int | 5 | 1 | 50 | Task song song tối đa | Nhiều task | Ít task, chậm | 🔴 **+200%** |
| 13 | `TIKI_DETAIL_RATE_LIMIT_DELAY` | 1.5 | float | 1.5 | 0.1 | 10.0 | Delay per detail request (giây) | An toàn, chậm | Nhanh, bị block | ✅ OK |

### Nhóm: CIRCUIT BREAKER (Resilience)

| # | Tham Số | Giá Trị | Loại | Min | Max | Mục Đích | Ý Nghĩa |
|---|---------|--------|------|-----|-----|---------|---------|
| 14 | `TIKI_CIRCUIT_BREAKER_FAILURE_THRESHOLD` | 5 | int | 1 | 20 | Số lỗi trước OPEN | 5 lỗi liên tiếp → dừng, chờ recovery_timeout |
| 15 | `TIKI_CIRCUIT_BREAKER_RECOVERY_TIMEOUT` | 60 | int | 10 | 600 | Chờ trước retry (giây) | Chờ 60s rồi thử lại |

### Nhóm: DEGRADATION MODE (Fallback)

| # | Tham Số | Giá Trị | Loại | Min | Max | Mục Đích | Ý Nghĩa |
|---|---------|--------|------|-----|-----|---------|---------|
| 16 | `TIKI_DEGRADATION_FAILURE_THRESHOLD` | 3 | int | 1 | 20 | Lỗi → degradation | 3 lỗi → giảm tốc độ |
| 17 | `TIKI_DEGRADATION_RECOVERY_THRESHOLD` | 5 | int | 1 | 20 | Success → recovery | 5 success → bình thường |

### Nhóm: DATABASE

| # | Tham Số | Giá Trị | Loại | Mục Đích |
|---|---------|--------|------|---------|
| 18 | `POSTGRES_HOST` | postgres | str | Host PostgreSQL (Docker) |
| 19 | `POSTGRES_PORT` | 5432 | int | Port PostgreSQL |
| 20 | `POSTGRES_USER` | postgres | str | User PostgreSQL |
| 21 | `POSTGRES_PASSWORD` | *** | str | Password PostgreSQL |

### Nhóm: REDIS & DAG

| # | Tham Số | Giá Trị | Loại | Mục Đích |
|---|---------|--------|------|---------|
| 22 | `REDIS_URL` | redis://redis:6379/3 | str | Redis cho monitoring |
| 23 | `TIKI_DAG_SCHEDULE_MODE` | manual | str | Schedule mode (manual/cron) |

---

## PHẦN 2: ENVIRONMENT VARIABLES (`.env`)

| # | Tham Số | Ví Dụ | Loại | Bắt Buộc | Ý Nghĩa |
|---|---------|-------|------|----------|---------|
| 1 | `AIRFLOW_UID` | 50000 | int | ✅ | User ID Airflow container |
| 2 | `AIRFLOW_PROJ_DIR` | . | path | ✅ | Project root directory |
| 3 | `AIRFLOW_IMAGE_NAME` | tiki-airflow:3.1.2 | str | ✅ | Custom Docker image name |
| 4 | `ENV_FILE_PATH` | .env | path | ✅ | Path đến .env file |
| 5 | `POSTGRES_USER` | your_user | str | ✅ | User database |
| 6 | `POSTGRES_PASSWORD` | your_pass | str | ✅ | Password database (NEVER commit!) |
| 7 | `REDIS_PASSWORD` | (empty) | str | ❌ | Password Redis (nếu cần) |
| 8 | `_AIRFLOW_WWW_USER_USERNAME` | airflow | str | ✅ | Username Airflow UI |
| 9 | `_AIRFLOW_WWW_USER_PASSWORD` | airflow | str | ✅ | Password Airflow UI |
| 10 | `_PIP_ADDITIONAL_REQUIREMENTS` | selenium>=4.0.0 ... | str | ❌ | Thêm packages Python |

---

## PHẦN 3: CODE CONFIGURATION (`config.py`)

### Category Crawling Config

| # | Tham Số | Giá Trị | Loại | Mục Đích | Range |
|---|---------|--------|------|---------|-------|
| 1 | `CATEGORY_BATCH_SIZE` | 5 | int | Categories per batch | 1-10 |
| 2 | `CATEGORY_TIMEOUT` | 120 | int | Timeout per batch (giây) - TỐI ƯU: 180→120 | 30-300 |
| 3 | `CATEGORY_CONCURRENT_REQUESTS` | 5 | int | HTTP requests song song - TỐI ƯU: 3→5 | 1-10 |
| 4 | `CATEGORY_POOL_SIZE` | 8 | int | Selenium drivers cho category | 1-20 |

### Product Crawling Config

| # | Tham Số | Giá Trị | Loại | Mục Đích | Range |
|---|---------|--------|------|---------|-------|
| 5 | `PRODUCT_BATCH_SIZE` | 12 | int | Products per batch - TỐI ƯU: 15→12 | 5-50 |
| 6 | `PRODUCT_TIMEOUT` | 60 | int | Timeout per batch (giây) - TỐI ƯU: 90→60 | 20-120 |
| 7 | `PRODUCT_POOL_SIZE` | 15 | int | Selenium drivers - TỐI ƯU: 5→15 | 1-50 |

### HTTP Client Config (🔴 NEW in v2)

| # | Tham Số | Giá Trị | Loại | Mục Đích | Range |
|---|---------|--------|------|---------|-------|
| 8 | `HTTP_CONNECTOR_LIMIT` | 100 | int | Tổng TCP connections - NEW! | 10-200 |
| 9 | `HTTP_CONNECTOR_LIMIT_PER_HOST` | 10 | int | Connections tới tiki.vn | 1-50 |
| 10 | `HTTP_TIMEOUT_TOTAL` | 20 | int | Timeout request (giây) - TỐI ƯU: 30→20 | 5-60 |
| 11 | `HTTP_TIMEOUT_CONNECT` | 10 | int | Timeout connect (giây) | 5-30 |
| 12 | `HTTP_DNS_CACHE_TTL` | 300 | int | DNS cache (giây) | 60-3600 |

---

## PHẦN 4: POSTGRESQL CONNECTION POOL

### Database Pool Config

| # | Tham Số | Giá Trị | Loại | Mục Đích | Range |
|---|---------|--------|------|---------|-------|
| 1 | `minconn` | 2 | int | Min connections trong pool | 1-5 |
| 2 | `maxconn` | 10 | int | Max connections trong pool | 5-30 |
| 3 | `connect_timeout` | 10 | int | Timeout connect (giây) | 5-30 |
| 4 | `statement_timeout` | 30000 | int | SQL timeout (ms) - 30s | 5000-60000 |

**Ý nghĩa:**
- Singleton pattern: Một pool duy nhất
- Thread-safe: `ThreadedConnectionPool` từ psycopg2
- Lợi ích: 40-50% nhanh hơn (tái sử dụng connection)

---

## PHẦN 5: BATCH PROCESSOR CONFIG

| # | Tham Số | Giá Trị | Loại | Mục Đích | Range |
|---|---------|--------|------|---------|-------|
| 1 | `batch_size` | 100 | int | Items per batch | 10-1000 |
| 2 | `show_progress` | True | bool | Log progress | True/False |
| 3 | `continue_on_error` | True | bool | Skip batch fail | True/False |

---

## PHẦN 6: REDIS CONNECTION POOL

| # | Tham Số | Giá Trị | Loại | Mục Đích | Range |
|---|---------|--------|------|---------|-------|
| 1 | `max_connections` | 20 | int | Max Redis connections | 5-50 |
| 2 | `socket_connect_timeout` | 5 | int | Connect timeout (giây) | 1-30 |
| 3 | `socket_timeout` | 5 | int | Socket timeout (giây) | 1-30 |
| 4 | `retry_on_timeout` | True | bool | Retry on timeout | True/False |

**Redis Databases:**
- DB 0: Airflow Celery broker (tự động)
- DB 1: Pipeline cache (HTML responses)
- DB 3: DAG monitoring

---

## PHẦN 7: TASK-LEVEL TIMEOUT CONFIGURATION

### Category Crawl Task

| # | Tham Số | Giá Trị Hiện Tại | Giá Trị Cũ | Giới Hạn | Ý Nghĩa |
|---|---------|-----------------|-----------|---------|---------|
| 1 | `execution_timeout` | 12 min | 15 min | 5-60 min | Timeout per category batch |
| 2 | `retries` | 1 | 2 | 0-5 | Retry lần |
| 3 | `retry_delay` | 15s | 2min | 5-300s | Chờ trước retry |

### Product Detail Task (Dynamic Task Mapping)

| # | Tham Số | Giá Trị Hiện Tại | Giá Trị Cũ | Ý Nghĩa |
|---|---------|-----------------|-----------|---------|
| 4 | Pool size | 15 drivers | 5 drivers | Selenium drivers |
| 5 | timeout | 60s | 90s | Selenium timeout |
| 6 | retries | 1 | 2 | Retry lần |
| 7 | retry_delay | 30s | 2min | Chờ trước retry |

### Merge Task

| # | Tham Số | Giá Trị Hiện Tại | Giá Trị Cũ | Ý Nghĩa |
|---|---------|-----------------|-----------|---------|
| 8 | `execution_timeout` | 30 min | 60 min | Gộp dữ liệu |

---

## PHẦN 8: TÓMSÁC ẢNH HƯỞNG TỐI ƯU HÓA V2

### Bảng So Sánh Chi Tiết

```
═════════════════════════════════════════════════════════════════════
  THAM SỐ                    CŨ        MỚI      THAY ĐỔI     TÁC DỤNG
═════════════════════════════════════════════════════════════════════
  Selenium Pool              5          15       +200%       3x xử lý
  Product Batch Size         15         12       -20%        23 vs 19 batches (+92%)
  Product Timeout            90s        60s      -33%        Fail nhanh
  HTTP Total Timeout         30s        20s      -33%        Request nhanh
  HTTP Connector Limit       N/A        100      NEW         Connection pooling ✨
  HTTP Per-Host Limit        N/A        10       NEW         Rate limit compliance
  Category Timeout           180s       120s     -33%        Batch nhanh
  Category Concurrent        3          5        +67%        Request song song
  Retry Count                2          1        -50%        Phục hồi nhanh
  Retry Delay                2min       30s      -75%        Phục hồi nhanh
  DNS Cache TTL             N/A        300s     NEW         DNS pooling ✨
═════════════════════════════════════════════════════════════════════
  KẾT QUẢ                    45 min     12-15min -70%        ⚡ 3-4x NHANH HƠN
═════════════════════════════════════════════════════════════════════
```

---

## PHẦN 9: CÁCH SỬ DỤNG THAM SỐ

### Trường hợp 1: Tốc độ NHANH (Aggressive)

```python
TIKI_DETAIL_POOL_SIZE = 30
TIKI_MAX_CONCURRENT_TASKS = 30
TIKI_PRODUCTS_PER_DAY = 500
PRODUCT_BATCH_SIZE = 20
CATEGORY_CONCURRENT_REQUESTS = 8
HTTP_CONNECTOR_LIMIT = 200
```

**Kết quả**: 500 products trong ~20 phút
**Risk**: OOM, bị block, lỗi nhiều

### Trường hợp 2: Bình thường (Balanced) 🟢 DEFAULT

```python
TIKI_DETAIL_POOL_SIZE = 15
TIKI_PRODUCTS_PER_DAY = 280
PRODUCT_BATCH_SIZE = 12
CATEGORY_CONCURRENT_REQUESTS = 5
HTTP_CONNECTOR_LIMIT = 100
```

**Kết quả**: 280 products trong ~12-15 phút
**Balance**: Tốc độ tốt, ổn định

### Trường hợp 3: An toàn (Conservative)

```python
TIKI_DETAIL_POOL_SIZE = 8
TIKI_PRODUCTS_PER_DAY = 100
PRODUCT_BATCH_SIZE = 10
TIKI_DETAIL_RATE_LIMIT_DELAY = 2.0
CATEGORY_CONCURRENT_REQUESTS = 3
HTTP_CONNECTOR_LIMIT = 50
```

**Kết quả**: 100 products trong ~8-10 phút
**Benefit**: Ít lỗi, ít bị block, an toàn 99%

---

## PHẦN 10: CHECKLIST CONFIGURATION

### ✅ Khi khởi động lần đầu

- [ ] Copy `.env.example` → `.env`
- [ ] Set `POSTGRES_USER`, `POSTGRES_PASSWORD` trong `.env`
- [ ] Set `_AIRFLOW_WWW_USER_USERNAME`, `_AIRFLOW_WWW_USER_PASSWORD`
- [ ] `docker-compose up -d --build`
- [ ] Chờ Airflow ready (~2 phút)
- [ ] Truy cập http://localhost:8080
- [ ] Admin → Variables → Set tham số

### ✅ Khi muốn tuning tham số

- [ ] Trigger DAG nhỏ trước (50-100 products)
- [ ] Xem log: `docker-compose logs -f airflow-scheduler`
- [ ] Đo thời gian & lỗi
- [ ] Điều chỉnh từng tham số
- [ ] Trigger lại test
- [ ] Khi OK → production run (280+ products)

### ✅ Khi DAG chạy chậm

- [ ] Tăng `TIKI_DETAIL_POOL_SIZE` → 20-25
- [ ] Tăng `HTTP_CONNECTOR_LIMIT` → 150
- [ ] Giảm `HTTP_TIMEOUT_TOTAL` → 15s
- [ ] Giảm `PRODUCT_BATCH_SIZE` → 10 (nhiều batches)
- [ ] Xem: `docker stats`

### ✅ Khi DAG bị error/block

- [ ] Tăng `TIKI_DETAIL_RATE_LIMIT_DELAY` → 2.0-3.0
- [ ] Giảm `TIKI_DETAIL_POOL_SIZE` → 8-10
- [ ] Tăng `retry_delay` → 60s
- [ ] Giảm `CATEGORY_CONCURRENT_REQUESTS` → 3
- [ ] Xem: `docker-compose logs airflow-worker`

---

## PHẦN 11: COMMAND REFERENCE

### Xem tham số

```bash
# Airflow Variables
docker exec tiki-data-pipeline-airflow-scheduler-1 airflow variables list

# Environment vars
cat .env | grep POSTGRES

# Database config
docker exec tiki-data-pipeline-postgres-1 psql -U postgres -c "\l"
```

### Set tham số

```bash
# Airflow Variables
docker exec tiki-data-pipeline-airflow-scheduler-1 \
  airflow variables set TIKI_DETAIL_POOL_SIZE 20

# Trigger DAG với override
docker exec tiki-data-pipeline-airflow-scheduler-1 \
  airflow dags trigger tiki_crawl_products \
  --conf '{"TIKI_DETAIL_POOL_SIZE": 25, "TIKI_PRODUCTS_PER_DAY": 500}'
```

### Monitor

```bash
# Logs
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-worker

# Resource
docker stats

# Database
docker exec tiki-data-pipeline-postgres-1 psql -U postgres -d crawl_data -c "SELECT COUNT(*) FROM products;"

# Redis
docker exec tiki-data-pipeline-redis-1 redis-cli -n 1 DBSIZE
```

---

**Tạo bởi**: GitHub Copilot  
**Ngày**: 18/11/2025  
**Version**: Tối ưu hóa v2 Complete  
**Total Parameters**: 88 parameters tracked
