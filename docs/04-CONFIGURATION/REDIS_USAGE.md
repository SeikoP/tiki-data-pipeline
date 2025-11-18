# Redis Usage Guide cho Tiki Data Pipeline

## Tổng quan

Redis hiện tại đang được sử dụng cho **Celery message broker** (database 0). Chúng ta có thể tận dụng Redis cho nhiều mục đích khác để tối ưu hóa pipeline.

## Cấu trúc Redis Databases

- **Database 0**: Celery message broker (đã dùng)
- **Database 1**: Cache (HTML, products, categories) - **Đề xuất**
- **Database 2**: Rate limiting & Distributed locking - **Đề xuất**

## Các tính năng có thể áp dụng

### 1. ✅ Caching (Database 1)

**Lợi ích:**
- Tăng tốc độ crawl bằng cách cache HTML/JSON responses
- Giảm số lượng requests đến Tiki
- Distributed cache - chia sẻ giữa các workers
- Tự động expire sau TTL

**Cách sử dụng:**
```python
from src.pipelines.crawl.redis_cache import get_redis_cache

# Lấy cache instance
cache = get_redis_cache("redis://redis:6379/1")

# Cache HTML
cache.cache_html(url, html_content, ttl=86400)  # Cache 24 giờ

# Lấy cached HTML
cached_html = cache.get_cached_html(url)
if cached_html:
    # Dùng cached HTML thay vì crawl lại
    pass

# Cache products
cache.cache_products(category_url, products_list, ttl=86400)

# Cache product detail
cache.cache_product_detail(product_id, detail_dict, ttl=86400)
```

**Tích hợp vào code hiện tại:**
- Thay thế file-based cache trong `crawl_products.py`
- Cache HTML responses trước khi parse
- Cache products và details để tránh crawl lại

### 2. ✅ Distributed Rate Limiting (Database 2)

**Lợi ích:**
- Kiểm soát rate limit chung cho tất cả workers
- Tránh bị Tiki block do quá nhiều requests
- Có thể điều chỉnh rate limit theo domain/IP

**Cách sử dụng:**
```python
from src.pipelines.crawl.redis_cache import get_redis_rate_limiter

# Lấy rate limiter (10 requests/phút)
rate_limiter = get_redis_rate_limiter("redis://redis:6379/2")

# Kiểm tra trước khi crawl
if rate_limiter.is_allowed("tiki.vn"):
    # Crawl được phép
    html = get_page_with_requests(url)
else:
    # Đợi đến khi được phép
    rate_limiter.wait_if_needed("tiki.vn")
    html = get_page_with_requests(url)
```

**Tích hợp vào code:**
- Thay thế `time.sleep()` bằng rate limiter thông minh
- Kiểm soát rate limit chung cho tất cả workers
- Có thể điều chỉnh rate limit theo domain

### 3. ✅ Distributed Locking (Database 2)

**Lợi ích:**
- Tránh crawl trùng lặp khi nhiều workers chạy cùng lúc
- Đảm bảo chỉ một worker crawl một URL tại một thời điểm
- Tự động release lock sau timeout

**Cách sử dụng:**
```python
from src.pipelines.crawl.redis_cache import get_redis_lock

lock = get_redis_lock("redis://redis:6379/2")

# Acquire lock trước khi crawl
if lock.acquire(f"crawl:{url}", timeout=300):
    try:
        # Crawl URL
        html = get_page_with_requests(url)
        # Process...
    finally:
        # Release lock
        lock.release(f"crawl:{url}")
else:
    # Lock đã được acquire bởi worker khác, skip
    print(f"URL {url} đang được crawl bởi worker khác")
```

**Tích hợp vào code:**
- Bọc crawl operations với lock
- Tránh crawl trùng lặp trong multi-worker environment

### 4. ✅ Session Storage

**Lợi ích:**
- Lưu session cookies/headers giữa các requests
- Chia sẻ session giữa các workers
- Tự động expire sau TTL

**Cách sử dụng:**
```python
cache = get_redis_cache("redis://redis:6379/1")

# Lưu session
cache.set("session:tiki", {
    "cookies": {...},
    "headers": {...},
    "user_agent": "..."
}, ttl=3600)

# Lấy session
session = cache.get("session:tiki")
```

## Cấu hình Docker Compose

Redis hiện tại đã được cấu hình trong `docker-compose.yaml`. Không cần thay đổi gì.

## Cài đặt Dependencies

Thêm vào `requirements.txt` hoặc `airflow/Dockerfile`:

```txt
redis>=5.0.0
```

## Migration Plan

### Phase 1: Thêm Redis Cache (Optional)
- Giữ file-based cache làm fallback
- Thử Redis cache trước, nếu fail thì dùng file cache
- So sánh performance

### Phase 2: Thay thế Rate Limiter
- Thay `time.sleep()` bằng Redis rate limiter
- Điều chỉnh rate limit theo thực tế

### Phase 3: Thêm Distributed Locking
- Thêm lock cho các crawl operations quan trọng
- Đảm bảo không crawl trùng lặp

## Monitoring

Có thể monitor Redis bằng:
- Redis CLI: `docker exec -it <redis-container> redis-cli`
- Redis Insight (GUI tool)
- Airflow metrics (nếu có)

## Best Practices

1. **TTL hợp lý:**
   - HTML cache: 24 giờ (86400s)
   - Products cache: 12 giờ (43200s)
   - Product detail cache: 7 ngày (604800s)

2. **Error Handling:**
   - Luôn có fallback về file cache nếu Redis fail
   - Log errors nhưng không crash pipeline

3. **Memory Management:**
   - Set maxmemory cho Redis
   - Sử dụng eviction policy (allkeys-lru)

4. **Connection Pooling:**
   - Reuse Redis connections
   - Set connection timeout hợp lý

## Performance Benefits

- **Giảm crawl time:** Cache hit rate có thể đạt 30-50%
- **Giảm server load:** Ít requests hơn đến Tiki
- **Tăng throughput:** Distributed cache cho phép workers chia sẻ data
- **Better coordination:** Distributed locking tránh duplicate work

## Kết luận

Redis có thể cải thiện đáng kể performance và reliability của pipeline. Bắt đầu với caching, sau đó thêm rate limiting và locking khi cần.

