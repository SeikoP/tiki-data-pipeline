# Phân Tích Các Module Chưa Tích Hợp

**Ngày phân tích:** 2025-11-18  
**Tool sử dụng:** Vulture (dead code detector)  
**Confidence threshold:** 60%+

## ✅ Đã Fix (100% confidence)

- ✅ Unused exception handler variables (`exc_val`, `exc_tb`) → Đổi thành `_exc_val`, `_exc_tb`
- ✅ Unused parameter `postfix` trong FakeTqdm → Đổi thành `_postfix`
- ✅ Unused parameter `save_to_db` trong loader → Removed

## 🔍 Module Đã Tích Hợp Nhưng Chưa Dùng Hết

### 1. **CircuitBreaker** (`src/pipelines/crawl/resilience/circuit_breaker.py`)
- ✅ **Đã tích hợp:** DAG sử dụng trong `tiki_crawl_products_dag.py`
- ⚠️ **Chưa dùng:**
  - `reset()` method (line 140) - Có thể dùng để reset circuit breaker sau maintenance
  - `get_state()` method (line 149) - Có thể dùng cho monitoring/dashboard

**Khuyến nghị:** Giữ lại, có thể cần cho monitoring trong tương lai

### 2. **GracefulDegradation** (`src/pipelines/crawl/resilience/graceful_degradation.py`)
- ✅ **Đã tích hợp:** DAG sử dụng stub implementation
- ⚠️ **Chưa dùng:**
  - `get_service()` method (line 196)
  - `is_service_available()` method (line 200)
  - `get_all_stats()` method (line 205)

**Khuyến nghị:** Tích hợp đầy đủ vào health check system

### 3. **RedisCache** (`src/pipelines/crawl/storage/redis_cache.py`)
- ✅ **Đã tích hợp:** DAG sử dụng `cache_product_detail()`
- ⚠️ **Chưa dùng:**
  - `get_cached_product_detail()` method (line 154) - Lấy cached detail
  - `reset()` method (line 219) - Clear cache
  - `release()` method (line 278) - Cleanup connections

**Khuyến nghị:** 
- Tích hợp `get_cached_product_detail()` để tránh crawl lại products đã có
- Thêm task cleanup định kỳ dùng `reset()`

### 4. **MultiLevelCache** (`src/pipelines/crawl/storage/multi_level_cache.py`)
- ❌ **Chưa tích hợp:** Module hoàn chỉnh nhưng không được dùng
- Features:
  - Memory + Redis + Disk caching (3 levels)
  - Auto-promotion/demotion
  - TTL management

**Khuyến nghị:** Module này tốt hơn RedisCache, nên migration sang dùng

### 5. **Config System** (`src/pipelines/crawl/config.py`)
- ⚠️ **Unused:** `get_config()` function (line 9)

**Khuyến nghị:** Kiểm tra lại, có thể dùng Airflow Variables thay thế

### 6. **PostgresStorage Advanced Features** (`src/pipelines/crawl/storage/postgres_storage.py`)
- ⚠️ **Chưa dùng:**
  - `get_products_by_category()` (line 551) - Query products theo category
  - `get_pool_stats()` (line 592) - Monitor connection pool

**Khuyến nghị:** Tích hợp vào monitoring dashboard

### 7. **DBPool** (`src/pipelines/load/db_pool.py`)
- ⚠️ **Unused:** `close_all()` method (line 98)

**Khuyến nghị:** Dùng trong cleanup tasks hoặc shutdown hooks

### 8. **PerformanceMetrics** (`src/common/monitoring.py`)
- ⚠️ **Unused:** `print_summary()` method (line 171)

**Khuyến nghị:** Dùng cho end-of-pipeline reporting

## 📦 Module Example/Demo (Có thể xóa)

### 9. **error_handling_example.py** (`src/pipelines/crawl/resilience/`)
- ❌ **Chưa dùng:** 
  - `fetch_page_with_circuit_breaker()` (line 17)
  - `crawl_category_with_error_handling()` (line 28)

**Khuyến nghị:** XÓA - Đây là example code, không dùng trong production

## 🎯 Action Items (Ưu tiên cao → thấp)

### Priority 1 - Quick Wins (1-2h)
- [ ] **Xóa** `error_handling_example.py` (demo code)
- [ ] **Tích hợp** `get_cached_product_detail()` vào crawl detail flow để skip products đã crawl
- [ ] **Thêm** cleanup task dùng `RedisCache.reset()` chạy hàng tuần

### Priority 2 - Performance Improvement (3-5h)
- [ ] **Migration** từ RedisCache sang MultiLevelCache
  - Cải thiện hit rate với 3-level caching
  - Giảm load lên Redis
  - Tiết kiệm chi phí

### Priority 3 - Monitoring & Observability (2-3h)
- [ ] **Tích hợp** `CircuitBreaker.get_state()` vào monitoring
- [ ] **Tích hợp** `PostgresStorage.get_pool_stats()` vào health check
- [ ] **Tích hợp** `GracefulDegradation` methods vào service health dashboard
- [ ] **Dùng** `PerformanceMetrics.print_summary()` ở cuối mỗi DAG run

### Priority 4 - Code Cleanup (1h)
- [ ] **Review** `config.py` - xem có cần `get_config()` không
- [ ] **Thêm** `DBPool.close_all()` vào DAG cleanup
- [ ] **Document** các unused methods trong code comments

## 📊 Statistics

- **Total findings:** 61 items (confidence 60%+)
- **Fixed:** 7 items (100% confidence)
- **False positives:** ~15 items (attributes, imports)
- **Legitimate unused code:** ~20 items
- **Potentially useful:** ~10 items

## 🔧 Vulture Integration

**Khuyến nghị KHÔNG thêm vào CI/CD** vì:
- Nhiều false positives (dynamic imports, DAG loading, etc.)
- Cần review thủ công

**Khuyến nghị chạy định kỳ:**
```powershell
# Chạy hàng tháng để cleanup
vulture src --min-confidence 80 --sort-by-size > docs/vulture-report.txt
```

## 📝 Notes

- Một số methods "unused" thực ra được dùng qua dynamic imports trong DAG
- Một số methods "unused" là public API cần giữ cho extensibility
- Example/demo code nên xóa khỏi production codebase
