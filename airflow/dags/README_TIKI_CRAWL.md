# Tiki Crawler - Best Practices & Architecture

## 🏗️ Kiến trúc tổng quan

### 1. **Chia nhỏ và Parallel Processing**
- Sử dụng **Dynamic Task Mapping** để chia nhỏ tasks và chạy song song
- Mỗi category được crawl độc lập, không block nhau
- Batch processing để tối ưu throughput

### 2. **Rate Limiting & Retry Logic**
- **Rate Limiter**: Giới hạn requests/phút và requests/giờ
- **Exponential Backoff**: Tự động tăng delay khi gặp lỗi
- **Jitter**: Thêm randomness để tránh thundering herd

### 3. **Incremental Updates**
- Chỉ crawl những gì thay đổi
- So sánh với metadata từ lần crawl trước
- Tiết kiệm tài nguyên và thời gian

### 4. **Error Handling**
- Retry với backoff thông minh
- Không fail toàn bộ pipeline khi 1 category lỗi
- Logging chi tiết để debug

## 📊 DAGs Overview

### 1. `tiki_crawl_categories`
- **Schedule**: Mỗi tuần (7 ngày)
- **Mục đích**: Crawl toàn bộ categories và sub-categories
- **Strategy**: 
  - Chia categories thành batches
  - Mỗi batch crawl song song
  - Merge kết quả và tạo file hợp nhất

### 2. `tiki_crawl_products`
- **Schedule**: Hàng ngày
- **Mục đích**: Crawl products từ tất cả categories
- **Strategy**:
  - Dynamic task mapping cho mỗi category
  - Rate limiting để tránh bị block
  - Lưu products theo category để dễ quản lý

### 3. `tiki_incremental_update`
- **Schedule**: Mỗi 6 giờ
- **Mục đích**: Cập nhật chỉ những gì thay đổi
- **Strategy**:
  - So sánh với metadata trước đó
  - Chỉ crawl categories/products mới
  - Cập nhật metadata sau mỗi lần crawl

## ⚙️ Configuration

### Environment Variables

```bash
# Rate Limiting
TIKI_MAX_REQ_PER_MIN=30          # Max requests/phút
TIKI_MAX_REQ_PER_HOUR=1000      # Max requests/giờ
TIKI_BURST_SIZE=5               # Burst size
TIKI_BACKOFF_FACTOR=2.0         # Backoff multiplier
TIKI_MAX_BACKOFF=300            # Max backoff (giây)

# Retry
TIKI_MAX_RETRIES=3              # Số lần retry
TIKI_INITIAL_DELAY=1.0          # Delay ban đầu (giây)
TIKI_MAX_DELAY=60.0             # Max delay (giây)

# Crawl
TIKI_BATCH_SIZE=50               # Số categories mỗi batch
TIKI_MAX_WORKERS=10              # Số workers song song
TIKI_TIMEOUT=60                 # Timeout (giây)
TIKI_MAX_AGE=172800000          # Cache age (ms)

# Airflow
TIKI_POOL_NAME=tiki_crawler_pool
TIKI_POOL_SLOTS=20              # Số slots trong pool
TIKI_TASK_TIMEOUT=3600         # Task timeout (giây)
TIKI_MAX_ACTIVE_TASKS=50        # Max active tasks
```

## 🚀 Tối ưu hóa

### 1. **Resource Management**
- Sử dụng Airflow Pools để giới hạn concurrent tasks
- Set CPU/memory limits cho workers
- Monitor resource usage

### 2. **Data Storage**
- Lưu raw data theo category để dễ quản lý
- Compress data nếu cần
- Archive data cũ

### 3. **Monitoring**
- Logging chi tiết ở mỗi bước
- Metrics cho success rate, throughput
- Alerts khi có lỗi nghiêm trọng

### 4. **Scaling**
- Tăng số workers khi cần
- Sử dụng Celery với multiple workers
- Consider Kubernetes executor cho scale lớn

## 📝 Best Practices

### 1. **Idempotency**
- Tasks phải idempotent (chạy nhiều lần cho cùng kết quả)
- Sử dụng timestamps và checksums

### 2. **Error Recovery**
- Không fail toàn bộ pipeline khi 1 task lỗi
- Retry với exponential backoff
- Dead letter queue cho failed items

### 3. **Data Quality**
- Validate data trước khi lưu
- Check duplicates
- Monitor data quality metrics

### 4. **Performance**
- Batch processing thay vì từng item
- Parallel processing khi có thể
- Cache khi phù hợp

## 🔧 Troubleshooting

### Rate Limiting Issues
- Giảm `TIKI_MAX_REQ_PER_MIN` nếu bị block
- Tăng `TIKI_BACKOFF_FACTOR` nếu có nhiều lỗi
- Check logs để xem pattern

### Memory Issues
- Giảm `TIKI_BATCH_SIZE`
- Giảm `TIKI_MAX_WORKERS`
- Increase worker memory limits

### Timeout Issues
- Tăng `TIKI_TIMEOUT`
- Tăng `TIKI_TASK_TIMEOUT`
- Check network latency

## 📈 Monitoring Metrics

- **Success Rate**: % tasks thành công
- **Throughput**: Số items crawl/phút
- **Error Rate**: % tasks lỗi
- **Average Duration**: Thời gian trung bình mỗi task
- **Queue Depth**: Số tasks đang chờ

## 🎯 Next Steps

1. Implement product parsing logic
2. Add data validation
3. Set up monitoring dashboard
4. Implement data quality checks
5. Add alerting system

