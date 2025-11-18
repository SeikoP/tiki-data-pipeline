# Cấu hình Cache cho Tiki Data Pipeline

## Dual Cache Strategy

Pipeline sử dụng **dual cache strategy**:
1. **Redis Cache** (ưu tiên): Nhanh, distributed, tự động expire
2. **File Cache** (fallback): Persistent, backup khi Redis không available

## Cấu hình Cache

### Mặc định: Bật cả Redis và File Cache

```python
# Trong DAG
cache_dir=str(CACHE_DIR)  # File cache được bật
use_redis_cache=True      # Redis cache được bật
```

### Tùy chọn 1: Chỉ dùng Redis Cache (tắt File Cache)

Để tắt file cache, set `cache_dir=None`:

```python
# Trong tiki_crawl_products_dag.py
# Thay đổi dòng 473:
cache_dir=None  # Tắt file cache, chỉ dùng Redis
```

Và comment/remove các dòng tạo folder cache:

```python
# CACHE_DIR.mkdir(parents=True, exist_ok=True)  # Comment dòng này
# DETAIL_CACHE_DIR.mkdir(parents=True, exist_ok=True)  # Comment dòng này
```

### Tùy chọn 2: Chỉ dùng File Cache (tắt Redis Cache)

Để tắt Redis cache, set `use_redis_cache=False`:

```python
# Trong các hàm crawl
use_redis_cache=False  # Tắt Redis cache, chỉ dùng file cache
```

## So sánh

| Tính năng | Redis Cache | File Cache |
|-----------|-------------|------------|
| Tốc độ | ⚡ Rất nhanh | 🐢 Chậm hơn |
| Distributed | ✅ Có | ❌ Không |
| Persistent | ❌ Mất khi restart | ✅ Giữ nguyên |
| Disk Space | ✅ Không tốn | ❌ Tốn disk |
| Fallback | ❌ Không có | ✅ Có |
| Tự động expire | ✅ Có (TTL) | ❌ Không |

## Khuyến nghị

### Production Environment:
- **Giữ cả 2**: Redis cache chính, file cache làm backup
- Lý do: An toàn, có fallback, không mất data khi Redis restart

### Development/Testing:
- **Chỉ Redis**: Nếu chắc chắn Redis luôn available
- Lý do: Nhanh hơn, tiết kiệm disk space

### High Memory Environment:
- **Chỉ Redis**: Nếu có đủ memory cho Redis
- Lý do: Redis nhanh hơn nhiều so với file I/O

## Xóa Folder Cache

Nếu đã chuyển sang chỉ dùng Redis và muốn xóa folder cache:

```bash
# Xóa folder cache
rm -rf data/raw/products/cache
rm -rf data/raw/products/detail/cache

# Hoặc trong Windows
rmdir /s /q data\raw\products\cache
rmdir /s /q data\raw\products\detail\cache
```

**Lưu ý**: Sau khi xóa, nếu Redis không available, pipeline sẽ không có cache và phải crawl lại từ đầu.

## Monitoring Cache Performance

### Kiểm tra Redis Cache Hit Rate:

```python
# Trong code, có thể log cache hit/miss
logger.info(f"[Redis Cache] ✅ Hit cache cho {url}")
logger.info(f"[File Cache] ✅ Hit cache cho {url}")
```

### Kiểm tra Redis Memory Usage:

```bash
# Trong container Redis
docker exec -it <redis-container> redis-cli
> INFO memory
> DBSIZE  # Số keys trong database 1 (cache)
```

## Troubleshooting

### Redis không available:
- Pipeline tự động fallback về file cache
- Không cần action, chỉ cần đảm bảo file cache folder tồn tại

### File cache không được tạo:
- Kiểm tra quyền ghi vào folder
- Kiểm tra disk space
- Kiểm tra `cache_dir` parameter có được set đúng không

### Cache không hoạt động:
- Kiểm tra Redis connection: `redis://redis:6379/1`
- Kiểm tra Redis container đang chạy: `docker ps | grep redis`
- Kiểm tra logs để xem có lỗi Redis không

