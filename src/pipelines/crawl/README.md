# Crawl Pipeline

Module này chứa phần crawl Tiki dùng cho cả chạy local và DAG Airflow `tiki_crawl_fast`.

## Vai Tro Cac File

| File | Vai tro | Khi nao chay truc tiep |
| --- | --- | --- |
| `extract_category_link_selenium.py` | Helper Selenium de mo trang category va parse link con | Chi chay de debug parse root category; khong phai buoc bat buoc cua full flow |
| `crawl_categories_optimized.py` | File chinh de crawl cay category tu root URLs | Chay truoc Airflow de tao `data/raw/categories_recursive_optimized.json` |
| `crawl_products.py` | Crawl danh sach san pham tu categories da crawl | Chay smoke test local truoc khi trigger Airflow |
| `crawl_products_detail.py` | Crawl chi tiet tung san pham | Thuong chay qua Airflow task, chi chay truc tiep khi debug detail crawler |
| `config.py` | Hang so crawl/cache/retry/data quality | Khong chay truc tiep |
| `storage/redis_cache.py` | Redis cache integration | Khong chay truc tiep |
| `storage/postgres_storage.py` | PostgreSQL storage helpers | Khong chay truc tiep |
| `resilience/` | Circuit breaker, DLQ, graceful degradation, exceptions | Khong chay truc tiep |

## Chuan Bi Truoc Khi Chay

Chay cac lenh tu project root:

```powershell
cd E:\Project\tiki-data-pipeline
```

Neu dung virtualenv local:

```powershell
.\.venv\Scripts\python.exe --version
```

Neu can Redis/Postgres cho cache hoac storage:

```powershell
docker compose up -d postgres redis
```

## 1. Khai Bao Root Categories

File category crawl chinh doc root URLs tu `data/raw/root_categories.json`.

Vi du:

```json
[
  "https://tiki.vn/thoi-trang-nam/c915",
  "https://tiki.vn/thoi-trang-nu/c931"
]
```

Co the dung bien moi truong `TIKI_ROOT_CATEGORIES` neu khong muon tao file:

```powershell
$env:TIKI_ROOT_CATEGORIES="https://tiki.vn/thoi-trang-nam/c915,https://tiki.vn/thoi-trang-nu/c931"
```

## 2. Debug Helper Root Category Neu Can

`extract_category_link_selenium.py` chi la helper cho parse category link bang Selenium. Chay file nay khi can kiem tra Selenium/browser hoac logic parse tren mot root page.

```powershell
.\.venv\Scripts\python.exe src\pipelines\crawl\extract_category_link_selenium.py
```

Output debug:

| File | Y nghia |
| --- | --- |
| `data/raw/categories.json` | Category con parse duoc tu root page |
| `full_page.html` | HTML debug neu script duoc cau hinh luu HTML |

Neu file nay parse duoc 0 category, kiem tra root URL, Chrome/Selenium, hoac Tiki page layout. Full flow category van nen chay qua `crawl_categories_optimized.py`.

## 3. Crawl Category Tree

Day la buoc local quan trong truoc khi chay full Airflow flow.

```powershell
.\.venv\Scripts\python.exe src\pipelines\crawl\crawl_categories_optimized.py
```

Output chinh:

| File | Y nghia |
| --- | --- |
| `data/raw/categories_recursive_optimized.json` | Cay category da crawl de dung cho product crawl va Airflow |
| `data/raw/cache/categories/` | Cache category crawl |

Neu lenh loi, sua loi o buoc nay truoc khi chay product crawl hoac Airflow. Khong trigger Airflow khi file category output chua co hoac rong.

## 4. Build Category Hierarchy

Sau khi co `categories_recursive_optimized.json`, build map cha-con:

```powershell
.\.venv\Scripts\python.exe src\tools\build_category_hierarchy.py
```

Output:

| File | Y nghia |
| --- | --- |
| `data/raw/category_hierarchy_map.json` | Map hierarchy dung de gan parent/root category cho san pham |

## 5. Smoke Test Product List Crawl

Chay product list crawler sau khi category tree da co du lieu:

```powershell
.\.venv\Scripts\python.exe src\pipelines\crawl\crawl_products.py
```

Output:

| File | Y nghia |
| --- | --- |
| `data/demo/products/products.json` hoac thu muc products cau hinh trong `crawl_utils.py` | Danh sach san pham crawl tu categories |
| `data/raw/cache/product_lists/` | Cache product listing |

Nen gioi han so category/page khi debug local neu crawl qua lau. Cac bien runtime lien quan nen khai bao qua `.env`/settings thay vi hardcode trong code.

## 6. Chay Full Flow Bang Airflow

Chi trigger Airflow sau khi cac buoc local toi thieu sau da OK:

1. `data/raw/root_categories.json` da dung root URLs.
2. `crawl_categories_optimized.py` chay xong.
3. `data/raw/categories_recursive_optimized.json` ton tai va co du lieu.
4. `build_category_hierarchy.py` chay xong.
5. `crawl_products.py` smoke test khong loi import/runtime co ban.

Start stack:

```powershell
docker compose up -d --build
```

Kiem tra service:

```powershell
docker compose ps
docker compose exec -T airflow-scheduler airflow dags list
docker compose exec -T airflow-scheduler airflow dags list-import-errors
```

Mo UI:

```text
http://localhost:8080/ui/
```

Trigger DAG:

```powershell
docker compose exec -T airflow-scheduler airflow dags trigger tiki_crawl_fast
```

## Thu Tu Chay Khuyen Nghi

```powershell
# Optional debug helper
.\.venv\Scripts\python.exe src\pipelines\crawl\extract_category_link_selenium.py

# Required before Airflow full flow
.\.venv\Scripts\python.exe src\pipelines\crawl\crawl_categories_optimized.py
.\.venv\Scripts\python.exe src\tools\build_category_hierarchy.py

# Product list smoke test
.\.venv\Scripts\python.exe src\pipelines\crawl\crawl_products.py

# Then Airflow
docker compose up -d --build
docker compose exec -T airflow-scheduler airflow dags trigger tiki_crawl_fast
```

## Runtime Controls

| Control | Muc dich |
| --- | --- |
| `TIKI_ROOT_CATEGORIES` | Root category URLs, cach nhau bang dau phay |
| `TIKI_CATEGORY_NUM_BATCHES` | So batch category mapped trong DAG |
| `TIKI_FORCE_REFRESH` | Bo qua cache khi can crawl lai |
| `PRODUCT_BATCH_SIZE` | So product moi detail batch |
| `REDIS_CACHE_TTL_PRODUCT_DETAIL` | TTL cache product detail |
| `DATA_QUALITY_MIN_SCORE` | Diem day du toi thieu cua product |

## Validation

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_crawl_products.py tests/test_tiki_crawl_api.py -q
.\.venv\Scripts\python.exe -m pytest tests/test_crawl_retry_logic.py tests/test_cache_hit_rate_fix.py -q
```
