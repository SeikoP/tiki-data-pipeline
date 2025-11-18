# 📋 01-PARAMETERS - THAM SỐ & CẤU HÌNH

**Thư mục này chứa**: Tất cả tham số cấu hình của dự án (88+ parameters)

---

## 📁 FILE STRUCTURE

| File | Mô Tả | Sử Dụng Khi |
|------|--------|-----------|
| `PARAMETERS_QUICK_REFERENCE.md` | ⚡ Top 10 tham số + quick tuning | Cần nhanh chóng |
| `PARAMETERS_DETAILED.md` | 📖 Chi tiết 88+ tham số | Cần tìm hiểu sâu |
| `PARAMETERS_MATRIX.md` | 📊 Bảng so sánh, trước/sau | Muốn so sánh |
| `README.md` | 📌 File này | Overview |

---

## 🎯 QUICK START

### Bạn muốn...

| Mục Đích | Đọc File |
|---------|----------|
| Tuning nhanh | `PARAMETERS_QUICK_REFERENCE.md` |
| Tìm tham số cụ thể | `PARAMETERS_DETAILED.md` |
| So sánh cũ vs mới | `PARAMETERS_MATRIX.md` |

---

## 📊 THAM SỐ OVERVIEW

### Phân Loại

```
Tổng: 88+ parameters

├─ Airflow Variables (23)
│  ├─ Category Crawling (7)
│  ├─ Product Crawling (3)
│  ├─ Selenium Pool (3)
│  ├─ Circuit Breaker (2)
│  ├─ Degradation Mode (2)
│  ├─ Database (4)
│  ├─ Redis (1)
│  └─ DAG Scheduling (1)
│
├─ Environment Variables (10)
│  ├─ Airflow Config (4)
│  ├─ PostgreSQL (2)
│  ├─ Redis (1)
│  ├─ Airflow Web UI (2)
│  └─ Python Deps (1)
│
├─ Code Configuration (12)
│  ├─ Category Config (4)
│  ├─ Product Config (3)
│  ├─ HTTP Client Config (5)
│
├─ Pool Configuration (8)
│  ├─ PostgreSQL Pool (4)
│  ├─ Batch Processor (3)
│  └─ Redis Pool (4)
│
└─ Task-Level Timeout (12)
   ├─ Category Task (3)
   ├─ Product Task (4)
   ├─ Merge Task (1)
   └─ Selenium Config (6)
```

---

## 🚀 OPTIMIZATION APPLIED (V2)

| Tham Số | Từ | Thành | Impact |
|---------|-----|--------|--------|
| **SELENIUM_POOL_SIZE** | 5 | 15 | +200% 🚀 |
| **PRODUCT_BATCH_SIZE** | 15 | 12 | -12% (23 vs 19 batches) |
| **PRODUCT_TIMEOUT** | 90s | 60s | -33% |
| **HTTP_TIMEOUT_TOTAL** | 30s | 20s | -33% |
| **HTTP_CONNECTOR_LIMIT** | N/A | 100 | NEW pooling ✨ |
| **CATEGORY_TIMEOUT** | 180s | 120s | -33% |
| **CATEGORY_CONCURRENT** | 3 | 5 | +67% |
| **RETRY_COUNT** | 2 | 1 | -50% |
| **RETRY_DELAY** | 2min | 30s | -75% |

**Kết quả**: E2E speedup **22x** (110 min → 5-15 min)

---

## 💡 TOP PARAMETERS TO TUNE

### 1. `TIKI_DETAIL_POOL_SIZE` (Selenium drivers)
- Default: 5
- Optimized: 15
- Min: 1, Max: 50
- Impact: 3x parallelism
- Use case: Increase for fast crawl, decrease for safety

### 2. `PRODUCT_BATCH_SIZE` (Products per batch)
- Default: 15
- Optimized: 12
- Min: 5, Max: 50
- Impact: 23 batches vs 19
- Use case: Smaller = more parallel, larger = less overhead

### 3. `TIKI_PRODUCTS_PER_DAY` (Run size)
- Default: 280
- Min: 1, Max: 10000
- Impact: Full run size
- Use case: Scale up/down based on need

### 4. `CATEGORY_TIMEOUT` (Category batch timeout)
- Default: 120s
- Min: 30s, Max: 300s
- Impact: Fail-fast vs generous
- Use case: Low for speed, high for reliability

### 5. `HTTP_TIMEOUT_TOTAL` (aiohttp request timeout)
- Default: 20s
- Min: 5s, Max: 60s
- Impact: Fast fail vs slow tolerance
- Use case: Low for responsive, high for slow network

---

## 🔧 HOW TO CHANGE PARAMETERS

### Method 1: Airflow Variables UI (Easiest)
```
1. Go to: http://localhost:8080
2. Admin → Variables
3. Find: TIKI_DETAIL_POOL_SIZE
4. Change: 15 → 20
5. Save → Auto reload
```

### Method 2: Command Line
```bash
docker exec tiki-data-pipeline-airflow-scheduler-1 \
  airflow variables set TIKI_DETAIL_POOL_SIZE 20
```

### Method 3: DAG Trigger with Override
```bash
docker exec tiki-data-pipeline-airflow-scheduler-1 \
  airflow dags trigger tiki_crawl_products \
  --conf '{"TIKI_DETAIL_POOL_SIZE": 20}'
```

### Method 4: Code Config File
```python
# Edit: src/pipelines/crawl/config.py
PRODUCT_BATCH_SIZE = 12        # Change here
CATEGORY_TIMEOUT = 120         # Change here
HTTP_TIMEOUT_TOTAL = 20        # Change here
```

---

## ⚠️ TUNING RECOMMENDATIONS

### For FAST Crawl (Aggressive)
```
TIKI_DETAIL_POOL_SIZE = 20
PRODUCT_BATCH_SIZE = 10
CATEGORY_CONCURRENT_REQUESTS = 8
HTTP_CONNECTOR_LIMIT = 150
Expected: 5-10 min for 280 products
Risk: Rate limit, OOM
```

### For BALANCED (Default)
```
TIKI_DETAIL_POOL_SIZE = 15
PRODUCT_BATCH_SIZE = 12
CATEGORY_CONCURRENT_REQUESTS = 5
HTTP_CONNECTOR_LIMIT = 100
Expected: 12-15 min for 280 products
Risk: Low
```

### For SAFE Crawl (Conservative)
```
TIKI_DETAIL_POOL_SIZE = 8
PRODUCT_BATCH_SIZE = 15
CATEGORY_CONCURRENT_REQUESTS = 3
HTTP_CONNECTOR_LIMIT = 50
Expected: 20-25 min for 280 products
Risk: Very low
```

---

## 🎓 UNDERSTANDING PARAMETERS

### Parameter Categories

1. **Concurrency Parameters** (Parallelism)
   - SELENIUM_POOL_SIZE: Number of parallel Selenium drivers
   - PRODUCT_BATCH_SIZE: Products per batch task
   - CATEGORY_CONCURRENT_REQUESTS: HTTP requests parallel

2. **Timeout Parameters** (Fail-Fast vs Tolerance)
   - PRODUCT_TIMEOUT: Selenium timeout
   - HTTP_TIMEOUT_TOTAL: aiohttp timeout
   - CATEGORY_TIMEOUT: Category batch timeout

3. **Retry Parameters** (Error Recovery)
   - RETRY_COUNT: Number of retries
   - RETRY_DELAY: Wait between retries
   - CIRCUIT_BREAKER_THRESHOLD: Failures before stop

4. **Pool Parameters** (Resource Management)
   - DB_POOL_SIZE: Database connections
   - HTTP_CONNECTOR_LIMIT: HTTP connections
   - REDIS_POOL_SIZE: Redis connections

5. **Rate Limiting Parameters** (Avoid Blocking)
   - TIKI_DETAIL_RATE_LIMIT_DELAY: Delay per request
   - HTTP_CONNECTOR_LIMIT_PER_HOST: Per-host limit
   - CATEGORY_CONCURRENT_REQUESTS: Category concurrency

---

## 📊 PARAMETER IMPACT MATRIX

| Parameter | Impact | Effort | ROI |
|-----------|--------|--------|-----|
| SELENIUM_POOL_SIZE | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐⭐ |
| PRODUCT_BATCH_SIZE | ⭐⭐⭐⭐ | ⭐ | ⭐⭐⭐⭐⭐ |
| HTTP_CONNECTOR_LIMIT | ⭐⭐⭐ | ⭐ | ⭐⭐⭐⭐ |
| PRODUCT_TIMEOUT | ⭐⭐⭐ | ⭐ | ⭐⭐⭐⭐ |
| TIKI_PRODUCTS_PER_DAY | ⭐⭐ | ⭐ | ⭐⭐⭐ |

---

## ✅ CHECKLIST

- [ ] Đọc `PARAMETERS_QUICK_REFERENCE.md`
- [ ] Hiểu TOP 5 parameters
- [ ] Know current values (check Airflow Variables)
- [ ] Know recommended values (see PARAMETERS_MATRIX.md)
- [ ] Plan tuning strategy
- [ ] Test with small DAG run (50 products)
- [ ] Monitor: CPU, Memory, Network
- [ ] Adjust based on results
- [ ] Document final settings

---

**Last Updated**: 18/11/2025  
**Status**: ✅ Complete & Organized
