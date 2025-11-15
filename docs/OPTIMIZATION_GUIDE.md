# Hướng Dẫn Tối Ưu Crawl 11k Sản Phẩm

## 📊 Phân Tích Hiện Trạng

Với **11,000 sản phẩm** và cấu hình hiện tại:
- **Rate limit**: 2 giây/product
- **Thời gian crawl tuần tự**: 11,000 × 2s = **22,000 giây = 6.1 giờ**
- **Dynamic Task Mapping**: Tạo 11k tasks song song (có thể quá tải)

## 🗓️ Multi-Day Crawling (Khuyến Nghị)

**Crawl trong nhiều ngày** là cách tối ưu nhất cho 11k sản phẩm:

### Ưu điểm:
- ✅ Tránh quá tải server và bị block IP
- ✅ Phân tán tải đều trong nhiều ngày
- ✅ Có thể resume nếu lỗi
- ✅ Tự động track progress
- ✅ Không cần chạy liên tục 6+ giờ

### Cấu hình:
```python
# Airflow Variables
TIKI_PRODUCTS_PER_DAY = 1000  # Crawl 1000 products mỗi ngày
TIKI_DETAIL_RATE_LIMIT_DELAY = 1.0  # 1 giây delay
```

### Thời gian:
- **11,000 products ÷ 1,000 products/ngày = 11 ngày**
- **Mỗi ngày**: 1,000 products × 1s = 1,000s = **16.7 phút** (với 4 parallel batches)
- **Tổng thời gian**: 11 ngày × 16.7 phút = **~3 giờ thực tế** (phân tán trong 11 ngày)

## 🚀 Giải Pháp Tối Ưu

### 1. **Multi-Day Crawling với Progress Tracking** ⭐ (Khuyến nghị nhất)

Crawl trong nhiều ngày với tracking progress tự động:

**Cấu hình:**
```python
# Airflow Variables
TIKI_PRODUCTS_PER_DAY = 1000  # Crawl 1000 products mỗi ngày
TIKI_DETAIL_RATE_LIMIT_DELAY = 1.0  # 1 giây delay
```

**Tính năng:**
- ✅ Tự động track progress vào `crawl_progress.json`
- ✅ Chỉ crawl products chưa crawl
- ✅ Resume từ điểm dừng nếu lỗi
- ✅ DAG chạy hàng ngày, tự động crawl batch tiếp theo
- ✅ Không cần can thiệp thủ công

**Ví dụ với 11k products:**
- Ngày 1: Crawl products 0-999 (1000 products)
- Ngày 2: Crawl products 1000-1999 (1000 products)
- ...
- Ngày 11: Crawl products 10000-10999 (1000 products)

### 2. **Batch Processing với Chunking**

Chia 11k sản phẩm thành batches nhỏ, xử lý từng batch:

**Ưu điểm:**
- Tránh quá tải Airflow với 11k tasks
- Dễ quản lý và monitor
- Có thể resume nếu lỗi
- Tối ưu memory và CPU

**Cấu hình:**
```python
# Airflow Variables
TIKI_DETAIL_BATCH_SIZE = 500  # Mỗi batch 500 products
TIKI_DETAIL_MAX_PARALLEL_BATCHES = 4  # 4 batches song song
TIKI_DETAIL_RATE_LIMIT_DELAY = 1.0  # Giảm xuống 1s (nếu server cho phép)
```

**Thời gian ước tính:**
- Mỗi batch: 500 products × 1s = 500s = 8.3 phút
- 4 batches song song: 8.3 phút
- Tổng batches: 11,000 / 500 = 22 batches
- Thời gian tổng: 22 / 4 × 8.3 = **45.6 phút** (thay vì 6.1 giờ)

### 2. **Tăng Parallelism**

**Cấu hình Airflow:**
```yaml
# docker-compose.yaml
AIRFLOW__CELERY__WORKER_CONCURRENCY: 16  # Mỗi worker xử lý 16 tasks
AIRFLOW__CORE__PARALLELISM: 32  # Tổng số tasks song song
AIRFLOW__CELERY__WORKER_PREFETCH_MULTIPLIER: 4
```

**Scale workers:**
```yaml
# Thêm nhiều workers
airflow-worker-2:
  <<: *airflow-common
  command: celery worker
  # ...
```

### 3. **Smart Caching**

Chỉ crawl products chưa có cache:
- Kiểm tra cache trước khi crawl
- Skip products đã có detail đầy đủ
- Giảm thời gian crawl đáng kể

**Ước tính:**
- Nếu 50% đã có cache: chỉ crawl 5,500 products
- Thời gian: 5,500 × 1s / 4 batches = **22.9 phút**

### 4. **Giảm Rate Limit Delay**

**Phân tích:**
- Delay 2s/product: quá thận trọng
- Delay 1s/product: hợp lý cho hầu hết server
- Delay 0.5s/product: nếu server cho phép

**Test và điều chỉnh:**
```python
# Bắt đầu với 1s, giảm dần nếu không bị block
TIKI_DETAIL_RATE_LIMIT_DELAY = 1.0  # → 0.8 → 0.5
```

### 5. **Priority Queue**

Ưu tiên crawl products quan trọng trước:
- Products có sales_count cao
- Products mới
- Products chưa có detail

### 6. **Retry Mechanism**

Tự động retry failed products:
```python
# Airflow task config
retries=2
retry_delay=timedelta(minutes=5)
```

### 7. **Progress Tracking**

Lưu progress vào database/file để resume:
- Track products đã crawl
- Resume từ điểm dừng
- Tránh crawl lại products đã xong

## 📝 Cấu Hình Khuyến Nghị

### Airflow Variables

```python
# Multi-day crawling (Khuyến nghị)
TIKI_PRODUCTS_PER_DAY = 1000  # Số products crawl mỗi ngày
TIKI_DETAIL_RATE_LIMIT_DELAY = 1.0  # 1 giây delay giữa các requests

# Batch processing (nếu không dùng multi-day)
TIKI_DETAIL_BATCH_SIZE = 500
TIKI_DETAIL_MAX_PARALLEL_BATCHES = 4

# Timeout
TIKI_DETAIL_CRAWL_TIMEOUT = 60  # 1 phút/product

# Giới hạn (0 = không giới hạn, chỉ dùng khi test)
TIKI_MAX_PRODUCTS_FOR_DETAIL = 0
```

### Docker Compose

```yaml
environment:
  AIRFLOW__CELERY__WORKER_CONCURRENCY: 16
  AIRFLOW__CORE__PARALLELISM: 32
  AIRFLOW__CELERY__WORKER_PREFETCH_MULTIPLIER: 4
```

## 🎯 Kế Hoạch Thực Hiện

### Bước 1: Cấu hình Multi-Day Crawling (Khuyến nghị)
- Set `TIKI_PRODUCTS_PER_DAY = 1000` trong Airflow Variables
- DAG sẽ tự động chạy hàng ngày và crawl batch tiếp theo
- Progress được lưu tự động vào `data/raw/products/crawl_progress.json`

### Bước 2: Monitor Progress
- Kiểm tra file `crawl_progress.json` để xem tiến độ
- Xem logs của DAG để biết số products đã crawl
- Điều chỉnh `TIKI_PRODUCTS_PER_DAY` nếu cần

### Bước 3: Tối ưu Rate Limit
- Bắt đầu với delay 1s
- Giảm dần nếu không bị block (0.8s → 0.5s)

### Bước 4: Scale nếu cần
- Nếu muốn crawl nhanh hơn: tăng `TIKI_PRODUCTS_PER_DAY`
- Nếu bị block: giảm `TIKI_PRODUCTS_PER_DAY` hoặc tăng `TIKI_DETAIL_RATE_LIMIT_DELAY`

## ⏱️ Thời Gian Ước Tính

| Cấu hình | Thời gian | Ghi chú |
|----------|-----------|---------|
| Tuần tự (2s delay) | 6.1 giờ | Không tối ưu |
| **Multi-day: 1000/ngày, 1s delay** | **11 ngày** | **⭐ Khuyến nghị nhất** |
| Multi-day: 2000/ngày, 1s delay | 6 ngày | Nhanh hơn nhưng rủi ro hơn |
| Batch 500, 4 parallel, 1s delay | 45 phút | Một lần, cần chạy liên tục |
| Batch 500, 8 parallel, 1s delay | 23 phút | Cần nhiều resources |

## ⚠️ Lưu Ý

1. **Rate Limiting**: Không giảm delay quá thấp → có thể bị block IP
2. **Memory**: Mỗi Selenium instance tốn ~200-500MB RAM
3. **CPU**: Cần đủ CPU cho parallel tasks
4. **Network**: Đảm bảo bandwidth đủ cho nhiều requests
5. **Database**: Airflow metadata DB cần đủ capacity

## 🔧 Troubleshooting

### Lỗi: Out of Memory
- Giảm `WORKER_CONCURRENCY`
- Giảm batch size
- Tăng memory cho workers

### Lỗi: Too many tasks
- Giảm parallelism
- Tăng batch size
- Dùng batch processing thay vì Dynamic Task Mapping trực tiếp

### Lỗi: Rate limit/Blocked
- Tăng `RATE_LIMIT_DELAY`
- Giảm số parallel batches
- Dùng proxy rotation (nếu có)

