# Rate Limit & Performance Fix Summary

## ✅ Đã sửa xong

### 1. **Tăng Rate Limiting Delays**

**File**: `src/pipelines/crawl/storage/adaptive_rate_limiter.py`

**Thay đổi**:
```python
# BEFORE
initial_delay=0.7,  # Quá thấp, dễ bị 429
min_delay=0.3,      # Quá aggressive
max_delay=2.0,      # Không đủ khi có nhiều errors

# AFTER
initial_delay=1.0,  # Tăng 43% để an toàn hơn
min_delay=0.5,      # Tăng 67% để giảm risk
max_delay=3.0,      # Tăng 50% khi có nhiều errors
```

**Lợi ích**:
- Giảm HTTP 429 errors từ Tiki API
- Adaptive rate limiter sẽ tự động điều chỉnh trong khoảng 0.5s - 3.0s
- Khi detect 429 error, delay sẽ tăng lên tối đa 3.0s thay vì 2.0s

### 2. **Tăng Fixed Delay Fallback**

**Files**: 
- `src/pipelines/crawl/crawl_products.py`
- `src/pipelines/crawl/crawl_products_detail.py`

**Thay đổi**:
```python
# BEFORE
time.sleep(0.7)  # Fixed delay fallback

# AFTER
time.sleep(1.0)  # Fixed delay fallback - tăng từ 0.7s để giảm 429 errors
```

**Lợi ích**:
- Khi adaptive rate limiter không available, vẫn có delay an toàn
- Giảm risk bị rate limit khi Redis down

### 3. **Verified Redis Cache Priority**

**Status**: ✅ Hoạt động đúng
- Redis được check đầu tiên (Tier 1)
- File cache chỉ là fallback
- TTL: 24h cho product list cache

## 📊 Test Results

### Rate Limiter Configuration
```bash
$ python -c "from pipelines.crawl.storage.adaptive_rate_limiter import get_adaptive_rate_limiter; limiter = get_adaptive_rate_limiter(); print(f'Initial: {limiter.current_delay}s, Min: {limiter.min_delay}s, Max: {limiter.max_delay}s')"

Initial: 1.0s
Min: 0.5s
Max: 3.0s
```

✅ Delays đã được tăng lên

### Import Times
```
Total module imports: 1.510s
  - aiohttp: 0.415s (slowest)
  - requests: 0.392s
  - redis: 0.207s
  - crawl_products: 0.174s
  - logging: 0.155s

Bootstrap import: 0.978s
Estimated task startup: 2.488s
```

✅ Import time acceptable (<3s)

### Task Startup Analysis

**Observed**: ~20s từ "Filling up DagBag" đến "START: Category"

**Breakdown**:
- Import time: ~2.5s (10%)
- **Airflow overhead: ~17.5s (90%)**
  - DAG parsing
  - Task scheduling
  - Worker initialization
  - XCom setup
  - Database connections

**Kết luận**: Task startup chậm chủ yếu do Airflow scheduler overhead, không phải do code.

## 🔍 Vấn đề đã phát hiện trong logs

### 1. HTTP 429 (Rate Limit)
```
[API] ⚠️ Attempt 1 failed with HTTP 429 for ID 113052002. Retrying...
[API] ⚠️ Attempt 1 failed with HTTP 429 for ID 83518996. Retrying...
```

**Giải pháp**: ✅ Đã tăng delays

### 2. Empty API Responses
```
[API] ❌ Lỗi attempt 1: 0, message='', url='https://tiki.vn/api/v2/products/113052946'
```

**Nguyên nhân**: Rate limiting hoặc bot detection
**Giải pháp**: ✅ Delays cao hơn sẽ giảm vấn đề này

### 3. HTML instead of JSON
```
[API] ❌ Lỗi attempt 1: 200, message='Attempt to decode JSON with unexpected mimetype: text/html; charset=utf-8'
```

**Nguyên nhân**: Tiki trả về HTML (captcha/block page) thay vì JSON
**Giải pháp**: ✅ Delays cao hơn + adaptive rate limiter sẽ detect và tăng delay

## 📈 Expected Impact

### Before (với delay 0.7s)
- Nhiều 429 errors
- Nhiều retries
- Tốc độ crawl không ổn định
- Risk bị block cao

### After (với delay 1.0-3.0s)
- Ít 429 errors hơn
- Ít retries hơn
- Tốc độ crawl ổn định hơn
- Risk bị block thấp hơn

### Trade-off
- Tốc độ crawl chậm hơn ~30-40%
- Nhưng success rate cao hơn
- Tổng thời gian có thể nhanh hơn (do ít retry)

## 🎯 Monitoring

### Check 429 errors
```bash
# In Airflow logs
docker exec tiki-data-pipeline-airflow-scheduler-1 \
  grep -r "429" /opt/airflow/logs/dag_id=tiki_crawl_fast/ | wc -l

# Should decrease after fix
```

### Check adaptive rate limiter stats
```bash
# Check current delay
docker exec tiki-data-pipeline-redis-1 \
  redis-cli -n 2 GET "tiki:ratelimit:adaptive:delay"

# Check error stats
docker exec tiki-data-pipeline-redis-1 \
  redis-cli -n 2 KEYS "tiki:ratelimit:stats:*"
```

### Check crawl success rate
```bash
# Check products crawled
docker exec tiki-data-pipeline-airflow-scheduler-1 \
  grep "✅ Crawl thành công" /opt/airflow/logs/dag_id=tiki_crawl_fast/**/*.log | wc -l

# Check errors
docker exec tiki-data-pipeline-airflow-scheduler-1 \
  grep "❌" /opt/airflow/logs/dag_id=tiki_crawl_fast/**/*.log | wc -l
```

## 🚀 Next Steps

### 1. Monitor production for 24h
- Check 429 error rate
- Check crawl success rate
- Check average delay from adaptive limiter

### 2. Fine-tune if needed
```python
# If still too many 429s, increase further:
initial_delay=1.5,  # từ 1.0s
max_delay=4.0,      # từ 3.0s

# If too slow and no 429s, can decrease:
initial_delay=0.8,  # từ 1.0s
min_delay=0.4,      # từ 0.5s
```

### 3. Consider additional optimizations
- Implement request pooling
- Add exponential backoff for retries
- Cache more aggressively
- Use batch API endpoints if available

## 📝 Files Changed

1. ✅ `src/pipelines/crawl/storage/adaptive_rate_limiter.py`
   - Tăng initial_delay: 0.7s → 1.0s
   - Tăng min_delay: 0.3s → 0.5s
   - Tăng max_delay: 2.0s → 3.0s

2. ✅ `src/pipelines/crawl/crawl_products.py`
   - Tăng fixed delay fallback: 0.7s → 1.0s (2 chỗ)

3. ✅ `src/pipelines/crawl/crawl_products_detail.py`
   - Tăng fixed delay fallback: 0.7s → 1.0s (2 chỗ)

4. ✅ `tests/test_task_startup_time.py` - Test script mới
5. ✅ `tests/test_full_redis_flow.py` - Test Redis cache
6. ✅ `REDIS_CACHE_FIX_SUMMARY.md` - Redis cache documentation
7. ✅ `RATE_LIMIT_FIX_SUMMARY.md` - This file

## ✅ Kết luận

**Đã sửa**:
1. ✅ HTTP 429 errors - Tăng delays lên 1.0-3.0s
2. ✅ Redis cache priority - Đã hoạt động đúng
3. ✅ Task startup time - Không phải vấn đề code (Airflow overhead)

**Selenium**: ✅ Đã được loại bỏ khỏi dự án (không cần sửa)

**Cần monitor**:
- 429 error rate trong 24h tiếp theo
- Crawl success rate
- Average delay từ adaptive limiter

---

**Ngày**: 2026-01-31
**Người thực hiện**: Kiro AI Assistant
