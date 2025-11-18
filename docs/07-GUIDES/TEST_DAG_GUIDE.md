# Hướng Dẫn Sử Dụng DAG Test E2E

## Tổng Quan

DAG `tiki_crawl_products_test` là phiên bản test của DAG chính với cấu hình tối giản để test luồng E2E nhanh chóng.

## Khác Biệt So Với DAG Chính

### 1. Cấu Hình Crawl

| Tham Số | DAG Chính | DAG Test | Ghi Chú |
|---------|-----------|----------|---------|
| **Categories** | Tất cả (hoặc theo Variable) | **3 categories** (hardcode) | Giảm để test nhanh |
| **Pages/Category** | 20 (hoặc theo Variable) | **2 pages** (hardcode) | Giảm để test nhanh |
| **Products Detail** | Tất cả (hoặc theo Variable) | **10 products** (hardcode) | Giảm để test nhanh |
| **max_active_tasks** | 10 | **3** | Giảm để dễ theo dõi |
| **Retries** | 3 | **1** | Giảm để test nhanh |
| **Timeout** | 300s (5 phút) | **120s (2 phút)** | Giảm để test nhanh |

### 2. Output Paths

| Loại File | DAG Chính | DAG Test |
|-----------|-----------|----------|
| **Products** | `data/raw/products/` | `data/test_output/products/` |
| **Processed** | `data/processed/` | `data/test_output/processed/` |
| **Cache** | `data/raw/products/cache/` | `data/test_output/products/cache/` |
| **Detail Cache** | `data/raw/products/detail/cache/` | `data/test_output/products/detail/cache/` |

**Lưu ý:** DAG test không conflict với DAG chính vì dùng thư mục riêng.

## Cách Sử Dụng

### 1. Trigger DAG Test

```bash
# Trigger từ CLI
docker-compose exec airflow-scheduler airflow dags trigger tiki_crawl_products_test

# Hoặc trigger từ Airflow UI
# Vào http://localhost:8080 -> DAGs -> tiki_crawl_products_test -> Trigger DAG
```

### 2. Xem Log

```bash
# Xem log scheduler
docker-compose logs airflow-scheduler --tail 50 | Select-String "tiki_crawl_products_test"

# Xem log worker
docker-compose logs airflow-worker --tail 50 | Select-String "tiki_crawl_products_test"

# Xem log trong Airflow UI
# Vào DAG -> Click vào DAG Run -> Click vào task để xem log
```

### 3. Kiểm Tra Output

```bash
# Xem products đã crawl
cat data/test_output/products/products.json

# Xem products với detail
cat data/test_output/products/products_with_detail.json

# Xem transformed products
cat data/test_output/processed/products_transformed.json

# Xem final products
cat data/test_output/processed/products_final.json
```

## Cấu Hình Test

### Hardcode Values (Không Cần Variables)

DAG test đã hardcode các giá trị sau, không cần set Airflow Variables:

- **3 categories**: Tự động giới hạn 3 categories đầu tiên
- **2 pages**: Mỗi category chỉ crawl 2 trang đầu
- **10 products detail**: Chỉ crawl detail cho 10 products đầu tiên

### Có Thể Override (Nếu Cần)

Một số cấu hình vẫn có thể override bằng Variables:

- `TIKI_USE_SELENIUM`: Có dùng Selenium không (default: false)
- `TIKI_RATE_LIMIT_DELAY`: Delay giữa requests (default: 1.0s)
- `TIKI_DETAIL_RATE_LIMIT_DELAY`: Delay cho detail crawl (default: 2.0s)

## Luồng E2E Test

DAG test sẽ chạy đầy đủ luồng E2E:

1. **Extract & Load Categories** → Load categories vào database
2. **Load Categories** → Load 3 categories để crawl
3. **Crawl Categories** → Crawl 2 pages từ mỗi category (3 tasks song song)
4. **Merge Products** → Gộp products từ tất cả categories
5. **Save Products** → Lưu vào `data/test_output/products/products.json`
6. **Prepare Products for Detail** → Chọn 10 products để crawl detail
7. **Crawl Product Details** → Crawl detail cho 10 products (10 tasks song song)
8. **Merge Product Details** → Gộp details vào products
9. **Save Products with Detail** → Lưu vào `data/test_output/products/products_with_detail.json`
10. **Transform Products** → Transform và lưu vào `data/test_output/processed/products_transformed.json`
11. **Load Products** → Load vào database và lưu `data/test_output/processed/products_final.json`
12. **Validate Data** → Validate dữ liệu
13. **Aggregate and Notify** → Tổng hợp và gửi thông báo (nếu có)

## Thời Gian Ước Tính

Với cấu hình test:
- **3 categories × 2 pages** ≈ 6-12 phút
- **10 products detail** ≈ 20-30 phút (với delay 2s)
- **Transform & Load** ≈ 2-5 phút
- **Tổng cộng**: ~30-50 phút

## Lưu Ý

1. **Không dùng cho production**: DAG test chỉ để test luồng, không crawl đủ dữ liệu
2. **Output riêng biệt**: Tất cả output ở `data/test_output/`, không ảnh hưởng DAG chính
3. **Có thể chạy song song**: Có thể chạy DAG test và DAG chính cùng lúc vì output khác nhau
4. **Cache riêng**: Cache cũng ở thư mục riêng, không dùng chung với DAG chính

## Troubleshooting

### DAG không xuất hiện trong UI

```bash
# Kiểm tra DAG có được parse không
docker-compose logs airflow-dag-processor --tail 100 | Select-String "tiki_crawl_products_test"

# Restart DAG processor nếu cần
docker-compose restart airflow-dag-processor
```

### Tasks bị stuck

```bash
# Kiểm tra worker
docker-compose logs airflow-worker --tail 100

# Kiểm tra scheduler
docker-compose logs airflow-scheduler --tail 100
```

### Output không có

```bash
# Kiểm tra thư mục output
ls -la data/test_output/products/

# Kiểm tra permissions
docker-compose exec airflow-worker ls -la /opt/airflow/data/test_output/
```

## So Sánh Với DAG Chính

| Tiêu Chí | DAG Chính | DAG Test |
|----------|-----------|----------|
| **Mục đích** | Production crawl | Test E2E |
| **Số categories** | Tất cả | 3 |
| **Số pages** | 20+ | 2 |
| **Số products detail** | Tất cả | 10 |
| **Thời gian** | Vài giờ | ~30-50 phút |
| **Output** | `data/raw/` | `data/test_output/` |
| **max_active_tasks** | 10 | 3 |
| **Retries** | 3 | 1 |

## Tùy Chỉnh Cấu Hình Test

Nếu muốn thay đổi số lượng test, sửa trực tiếp trong file:

```python
# airflow/dags/tiki_crawl_products_test_dag.py

# Dòng 983: Số categories
max_categories = 3  # Thay đổi số này

# Dòng 1070: Số pages
max_pages = 2  # Thay đổi số này

# Dòng 1594: Số products detail
max_products = 10  # Thay đổi số này
```

Sau đó restart DAG processor:

```bash
docker-compose restart airflow-dag-processor
```

