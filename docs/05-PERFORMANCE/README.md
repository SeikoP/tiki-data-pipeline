# 📊 05-PERFORMANCE - HIỆU SUẤT & MONITORING

**Thư mục này chứa**: Phân tích hiệu suất, metrics, monitoring

---

## 📁 FILE STRUCTURE

| File | Mô Tả | Sử Dụng Khi |
|------|--------|-----------|
| `PERFORMANCE_ANALYSIS.md` | 📊 Metrics & KPIs | Analyze performance |
| `README.md` | 📌 File này | Overview |

---

## 🎯 QUICK START

### Bạn muốn...

| Mục Đích | Đọc File |
|---------|----------|
| Xem metrics | `PERFORMANCE_ANALYSIS.md` |
| Hiểu KPI | Xem section dưới |

---

## 📈 KEY PERFORMANCE INDICATORS (KPI)

### Speed Metrics

| Metric | Baseline | Optimized | Target |
|--------|----------|-----------|--------|
| E2E Time | 110 min | 5-15 min | < 15 min |
| Crawl Time | 87 min | 13 min | < 15 min |
| Transform Time | 12 min | 2 min | < 5 min |
| Load Time | 8 min | 1.5 min | < 2 min |

### Resource Metrics

| Metric | Baseline | Optimized | Limit |
|--------|----------|-----------|-------|
| CPU Usage | 15% | 58% | 80% |
| Memory | 2.1 GB | 5.2 GB | 8 GB |
| Network | 450 MB | 280 MB | 1 GB |
| Disk I/O | 12 MB/s | 8 MB/s | 50 MB/s |

### Quality Metrics

| Metric | Baseline | Optimized | Target |
|--------|----------|-----------|--------|
| Success Rate | 96.8% | 99.2% | > 98% |
| Error Rate | 3.2% | 0.8% | < 1% |
| Cache Hit Ratio | N/A | 38% | > 30% |
| Data Quality | 94% | 99.1% | > 98% |

---

## 🔍 MONITORING DASHBOARD

### Real-time Metrics

```
┌──────────────────────────────────────────┐
│ AIRFLOW TASK EXECUTION MONITOR           │
├──────────────────────────────────────────┤
│ DAG: tiki_crawl_products                 │
│ Status: Running                          │
│ Progress: 18/23 tasks completed (78%)    │
│                                          │
│ Current Task: crawl_products[13]         │
│ Duration: 2min 34sec                     │
│ Estimated Remaining: 4min 26sec          │
│                                          │
│ Resource Usage:                          │
│ • CPU: 54% (12 threads active)          │
│ • Memory: 4.2 GB / 8 GB (52%)           │
│ • Network: 12 MB/s (downloading)        │
│ • Disk I/O: 6 MB/s (writing)            │
└──────────────────────────────────────────┘
```

### Database Metrics

```
PostgreSQL Connection Pool:
├─ Active: 8/20 connections
├─ Idle: 12/20 connections
├─ Reuse Rate: 78%
├─ Avg Query Time: 234ms
└─ Cache Hit Ratio: 91%

Redis Connection Pool:
├─ Active: 6/20 connections
├─ Cache Size: 850 MB / 1.2 GB (71%)
├─ Hit Ratio: 38% (2,340 hits / 6,157 requests)
├─ TTL: 1 hour (refresh at 30 min mark)
└─ Eviction Policy: LRU
```

### Network Metrics

```
HTTP Requests (aiohttp):
├─ Requests/sec: 45-60 req/s
├─ Success Rate: 99.2% (2,840/2,860 requests)
├─ Timeouts: 8 (0.28%)
├─ Retries: 12 (0.42%)
├─ Avg Response Time: 340ms
├─ P95 Response Time: 520ms
└─ P99 Response Time: 1,240ms

Selenium Browser:
├─ Active Drivers: 15/15 (all busy)
├─ Pages Loaded/sec: 3-4 pages/s
├─ Avg Page Load: 2.4 sec
├─ Timeouts: 0 (good!)
└─ Memory per Driver: 220 MB
```

---

## 📊 BOTTLENECK ANALYSIS

### Current Bottlenecks (Optimized)

1. **Network I/O** (~40% of time)
   - Tiki.vn response time: 300-600ms
   - Mitigation: Redis caching (34-42% hit rate)
   - Further improvement: CDN proxy (not feasible)

2. **Selenium WebDriver** (~30% of time)
   - Browser startup overhead
   - Mitigation: Pool 15 drivers, reuse connections
   - Further improvement: Lighter crawlers (risky - need JS)

3. **Database Inserts** (~15% of time)
   - Batch processing 50 products at a time
   - Mitigation: Connection pooling (78% reuse)
   - Further improvement: Parallel insert with sharding

4. **Data Transformation** (~10% of time)
   - JSON parsing + validation
   - Mitigation: Async pre-validation
   - Further improvement: Pre-compiled patterns

5. **Logging & Overhead** (~5% of time)
   - Reduced logging in V6
   - Mitigation: Async logging
   - Further improvement: Zero-overhead tracing

---

## 🎯 PERFORMANCE COMPARISON

### By Week

```
Week | E2E Time | Crawl | Transform | Load | Speedup | Bottleneck
-----|----------|-------|-----------|------|---------|------------------
W0   | 110 min  | 87m   | 12m       | 8m   | 1x      | Selenium (seq)
W1   | 62 min   | 48m   | 10m       | 4m   | 1.8x    | Selenium pool
W2   | 55 min   | 42m   | 9m        | 4m   | 2.0x    | Batch size
W3   | 47 min   | 35m   | 8m        | 4m   | 2.3x    | Connection pool
W4   | 33 min   | 21m   | 7m        | 5m   | 3.3x    | Timeouts
W5   | 21 min   | 6m    | 10m       | 5m   | 5.2x    | Transform time
W6   | 12 min   | 6m    | 3m        | 3m   | 9.2x    | Balanced ✓
```

---

## 🚀 PERFORMANCE OPTIMIZATION TIPS

### 1. Monitor CPU
```bash
# Check CPU usage in real-time
docker stats --no-stream

# Alert if > 80%
if cpu_usage > 80:
    reduce_selenium_pool_size()
    reduce_batch_size()
```

### 2. Monitor Memory
```bash
# Check memory usage
free -h

# If > 80% of limit (6.4 GB):
if memory_usage > 6.4GB:
    reduce_cache_size()
    reduce_pool_sizes()
```

### 3. Monitor Network
```bash
# Check network throughput
iftop -n

# If > 500 MB/s:
if network_throughput > 500MB_s:
    reduce_concurrent_requests()
    enable_compression()
```

### 4. Monitor Disk
```bash
# Check disk I/O
iostat -x 1

# If write time > 5%:
if disk_write_percent > 5:
    reduce_logging()
    increase_batch_size()
```

---

## 📈 SCALABILITY ANALYSIS

### Horizontal Scaling (More Machines)

```
Current: 1 machine (8 CPU, 8 GB RAM)
├─ Airflow Scheduler: 1 CPU
├─ Airflow Worker: 4 CPU
└─ Database: shared

Scaled: 3 machines
├─ Scheduler: 1 machine (2 CPU, 2 GB RAM)
├─ Workers: 2 machines (8 CPU, 8 GB RAM each)
└─ Database: 1 dedicated machine

Expected improvement:
├─ CPU available: 8 → 18 CPU
├─ Memory available: 8 → 18 GB
├─ Parallelism: 5 concurrent → 12 concurrent
├─ E2E time: 12 min → 5 min
└─ Cost: 3x infrastructure

Not recommended for this scale (overkill).
```

### Vertical Scaling (Bigger Machine)

```
Current: 8 CPU, 8 GB RAM
├─ CPU-bound: Limited by crawl parallelism
├─ Memory-bound: Redis cache (1.2 GB) + Pool overhead (2 GB)

Upgrade: 16 CPU, 16 GB RAM
├─ CPU increase: 8 → 16 (2x)
├─ Memory increase: 8 → 16 GB (2x)
├─ New bottleneck: Tiki.vn rate limiting (network)
├─ Expected improvement: 5-10% (not 2x)

Better approach: Optimize further or use distributed cache.
```

---

## ⚠️ PERFORMANCE DEGRADATION SIGNALS

| Signal | Threshold | Action |
|--------|-----------|--------|
| E2E Time > 20 min | 20 min | Check network/DB |
| Success Rate < 98% | 98% | Review error logs |
| Cache Hit < 30% | 30% | Increase TTL |
| CPU > 80% | 80% | Reduce pool sizes |
| Memory > 80% | 80% | Reduce pool/cache |
| Error Rate > 1% | 1% | Enable circuit breaker |

---

## ✅ MONITORING CHECKLIST

- [ ] Setup Prometheus metrics export
- [ ] Create Grafana dashboard
- [ ] Configure CloudWatch/Datadog alerts
- [ ] Enable audit logging
- [ ] Track performance over time
- [ ] Weekly performance review
- [ ] Document any performance degradation
- [ ] Plan for scaling if needed

---

**Last Updated**: 18/11/2025  
**Status**: ✅ Performance Analysis Complete
