# 🚀 ROADMAP TỐI ƯU TỐC ĐỘ CRAWL - TIKI DATA PIPELINE

**Ngày tạo**: 2025-12-01  
**Phiên bản**: 1.0  
**Trạng thái**: 📋 Kế Hoạch Tối Ưu  
**Mục tiêu**: Tăng tốc độ crawl từ 2.8 products/min → 50-100 products/min

---

## 📊 TÓM TẮT ĐIỀU HÀNH

### Hiện Trạng Tốc Độ Crawl

| Metric | Giá Trị Hiện Tại | Mục Tiêu | Cải Thiện Cần Thiết |
|--------|------------------|----------|---------------------|
| **Tốc độ crawl** | 2.8 products/min | 50-100 products/min | **18-35x nhanh hơn** |
| **Thời gian E2E** | 110 phút (280 products) | 3-6 phút | **18-36x nhanh hơn** |
| **Selenium pool** | 15 drivers | 20-30 drivers | +33-100% |
| **Cache hit rate** | 35-40% | 60-80% | +25-40% |
| **Batch size** | 12 products | 8-10 products | Tối ưu hơn |
| **Rate limit delay** | 0.7s | 0.3-0.5s | Giảm 30-60% |

### Bottleneck Chính Hiện Tại

1. **Selenium WebDriver** (~50% thời gian)
   - Khởi tạo browser: 2-3 giây
   - Load page & render JS: 2-4 giây
   - Scroll để load content: 1-2 giây
   - **Tổng: 5-9 giây/product**

2. **Rate Limiting** (~20% thời gian)
   - Delay giữa requests: 0.7s
   - Tránh bị block bởi Tiki.vn

3. **Network Latency** (~15% thời gian)
   - DNS lookup: 50-200ms
   - TCP connection: 100-300ms
   - SSL handshake: 200-500ms
   - HTTP request/response: 500-1000ms

4. **Cache Miss** (~15% thời gian)
   - Cache hit rate chỉ 35-40%
   - 60-65% products phải crawl lại

---

## 🎯 PHASE 1: TỐI ƯU SELENIUM (Tuần 1-2)

**Mục tiêu**: Giảm thời gian Selenium từ 5-9s → 2-4s/product

### 1.1 Tối Ưu Browser Configuration

#### 🔧 Task 1.1.1: Headless Mode Optimization
**Priority**: High  
**Effort**: 2-3 ngày  
**Expected Impact**: -30-40% thời gian browser

**Actions**:
- [ ] Sử dụng headless mode với flags tối ưu:
  ```python
  options.add_argument('--headless=new')
  options.add_argument('--disable-gpu')
  options.add_argument('--no-sandbox')
  options.add_argument('--disable-dev-shm-usage')
  options.add_argument('--disable-software-rasterizer')
  ```
- [ ] Tắt images và CSS không cần thiết:
  ```python
  prefs = {
      'profile.managed_default_content_settings.images': 2,  # Block images
      'profile.default_content_setting_values.stylesheets': 2  # Block CSS
  }
  ```
- [ ] Disable JavaScript không cần thiết (nếu có thể)

**Metrics**:
- Browser startup time: < 1s (từ 2-3s)
- Page load time: < 2s (từ 2-4s)
- Total time/product: < 3s (từ 5-9s)

#### 🔧 Task 1.1.2: Browser Pooling & Reuse
**Priority**: High  
**Effort**: 3-4 ngày  
**Expected Impact**: -20-30% overhead

**Actions**:
- [ ] Implement browser reuse (không khởi tạo mới mỗi request)
- [ ] Maintain warm browser pool (pre-initialized browsers)
- [ ] Reuse sessions giữa các requests
- [ ] Implement browser health checks và auto-restart

**Metrics**:
- Browser reuse rate: > 80%
- Pool warmup time: < 5s
- Browser lifetime: > 1000 requests

#### 🔧 Task 1.1.3: Smart Waiting Strategies
**Priority**: Medium  
**Effort**: 2 ngày  
**Expected Impact**: -15-25% wait time

**Actions**:
- [ ] Sử dụng explicit waits thay vì time.sleep()
- [ ] Wait cho specific elements (không wait full page)
- [ ] Reduce wait time cho non-critical elements
- [ ] Implement conditional waits (skip nếu không cần)

**Metrics**:
- Average wait time: < 1s (từ 2-3s)
- Wait efficiency: > 90%
- False timeout rate: < 1%

---

### 1.2 Parallel Selenium Scaling

#### 🔧 Task 1.2.1: Tăng Selenium Pool Size
**Priority**: High  
**Effort**: 1-2 ngày  
**Expected Impact**: +50-100% throughput

**Actions**:
- [ ] Tăng `PRODUCT_POOL_SIZE` từ 15 → 20-30
- [ ] Monitor memory usage (không vượt quá limit)
- [ ] Implement dynamic pool sizing dựa trên workload
- [ ] Test với different pool sizes để tìm sweet spot

**Cấu hình đề xuất**:
```python
# Hiện tại
PRODUCT_POOL_SIZE = 15

# Mục tiêu
PRODUCT_POOL_SIZE = 25  # +67% capacity

# Nếu memory cho phép
PRODUCT_POOL_SIZE = 30  # +100% capacity
```

**Metrics**:
- Concurrent browsers: 25-30 (từ 15)
- Memory usage: < 6GB (monitor closely)
- CPU usage: 70-85% (optimal range)

#### 🔧 Task 1.2.2: Distributed Selenium (Future)
**Priority**: Low  
**Effort**: 2-3 tuần  
**Expected Impact**: Unlimited scaling

**Actions**:
- [ ] Setup Selenium Grid hoặc Docker Swarm
- [ ] Distribute browsers across multiple machines
- [ ] Load balancing giữa các nodes
- [ ] Centralized browser management

**Metrics**:
- Scaling capacity: Unlimited
- Latency overhead: < 100ms
- Node health: > 99%

---

## 🔄 PHASE 2: TỐI ƯU CACHING (Tuần 2-3)

**Mục tiêu**: Tăng cache hit rate từ 35-40% → 60-80%

### 2.1 Intelligent Caching Strategy

#### 🔧 Task 2.1.1: Cache TTL Optimization
**Priority**: High  
**Effort**: 1-2 ngày  
**Expected Impact**: +20-30% hit rate

**Actions**:
- [ ] Phân tích cache hit/miss patterns
- [ ] Adjust TTL dựa trên product update frequency:
  - Popular products: 24h (thay đổi thường xuyên)
  - Normal products: 48-72h
  - Less popular: 168h (1 tuần)
- [ ] Implement dynamic TTL based on last_update time
- [ ] Cache invalidation strategy (invalidate khi detect changes)

**Cấu hình đề xuất**:
```python
# Hiện tại
REDIS_CACHE_TTL_PRODUCT_DETAIL = 604800  # 7 days (fixed)

# Mục tiêu (dynamic)
if product.popularity_score > 80:
    cache_ttl = 86400  # 1 day
elif product.popularity_score > 50:
    cache_ttl = 172800  # 2 days
else:
    cache_ttl = 604800  # 7 days
```

**Metrics**:
- Cache hit rate: > 60% (từ 35-40%)
- Cache efficiency: > 80%
- Stale data rate: < 5%

#### 🔧 Task 2.1.2: Cache Key Optimization
**Priority**: Medium  
**Effort**: 1-2 ngày  
**Expected Impact**: +5-10% hit rate

**Actions**:
- [ ] Optimize cache key structure (ngắn gọn, unique)
- [ ] Use consistent keys across requests
- [ ] Implement cache key versioning
- [ ] Cache pre-warming cho popular products

**Metrics**:
- Cache key size: < 100 bytes
- Key collision rate: < 0.1%
- Pre-warm coverage: > 20% products

#### 🔧 Task 2.1.3: Partial Cache & Incremental Updates
**Priority**: Medium  
**Effort**: 2-3 ngày  
**Expected Impact**: +10-15% effective cache usage

**Actions**:
- [ ] Cache partial data (chỉ fields thay đổi)
- [ ] Merge cached data với fresh data
- [ ] Incremental updates (chỉ update changed fields)
- [ ] Smart cache refresh (update khi cần)

**Metrics**:
- Partial cache hit rate: > 30%
- Data freshness: > 95%
- Update efficiency: +50%

---

### 2.2 Cache Infrastructure

#### 🔧 Task 2.2.1: Redis Optimization
**Priority**: Medium  
**Effort**: 1-2 ngày  
**Expected Impact**: +10-20% cache performance

**Actions**:
- [ ] Tối ưu Redis memory (compression, eviction policy)
- [ ] Setup Redis clustering (nếu cần scale)
- [ ] Monitor cache memory usage
- [ ] Implement cache warming strategy

**Metrics**:
- Redis memory usage: < 80%
- Cache lookup time: < 5ms
- Cache write time: < 10ms

---

## ⚡ PHASE 3: TỐI ƯU NETWORK & I/O (Tuần 3-4)

**Mục tiêu**: Giảm network overhead từ 15% → 5-8%

### 3.1 HTTP Optimization

#### 🔧 Task 3.1.1: Connection Pooling Tối Ưu
**Priority**: High  
**Effort**: 1-2 ngày  
**Expected Impact**: -30-40% connection overhead

**Actions**:
- [ ] Tăng `HTTP_CONNECTOR_LIMIT` từ 100 → 150-200
- [ ] Optimize `limit_per_host` (test với 15-20)
- [ ] Implement connection keep-alive
- [ ] Monitor connection reuse rate

**Cấu hình đề xuất**:
```python
# Hiện tại
HTTP_CONNECTOR_LIMIT = 100
HTTP_CONNECTOR_LIMIT_PER_HOST = 10

# Mục tiêu
HTTP_CONNECTOR_LIMIT = 200  # +100%
HTTP_CONNECTOR_LIMIT_PER_HOST = 15  # +50%
```

**Metrics**:
- Connection reuse rate: > 95% (từ 85%)
- Connection creation time: < 50ms
- Pool efficiency: > 90%

#### 🔧 Task 3.1.2: DNS & Network Caching
**Priority**: Medium  
**Effort**: 1 ngày  
**Expected Impact**: -20-30% DNS lookup time

**Actions**:
- [ ] Tăng DNS cache TTL từ 300s → 600-1800s
- [ ] Use system DNS cache (nếu có thể)
- [ ] Implement DNS prefetching
- [ ] Monitor DNS lookup time

**Metrics**:
- DNS cache hit rate: > 98%
- DNS lookup time: < 10ms
- Cache efficiency: > 95%

#### 🔧 Task 3.1.3: Request Compression & Optimization
**Priority**: Low  
**Effort**: 1-2 ngày  
**Expected Impact**: -10-15% bandwidth

**Actions**:
- [ ] Enable gzip compression cho requests
- [ ] Reduce request headers size
- [ ] Use HTTP/2 nếu Tiki hỗ trợ
- [ ] Optimize request payloads

**Metrics**:
- Bandwidth usage: -10-15%
- Request size: < 1KB average
- Response time: -5-10%

---

### 3.2 Rate Limiting Optimization

#### 🔧 Task 3.2.1: Dynamic Rate Limiting
**Priority**: High  
**Effort**: 2-3 ngày  
**Expected Impact**: -30-50% delay time

**Actions**:
- [ ] Implement adaptive rate limiting (tự động điều chỉnh)
- [ ] Monitor Tiki response times (detect rate limiting)
- [ ] Giảm delay khi không detect blocking:
  - Start với 0.7s
  - Giảm xuống 0.5s nếu stable
  - Tiếp tục giảm xuống 0.3s nếu vẫn OK
- [ ] Increase delay khi detect 429 errors

**Cấu hình đề xuất**:
```python
# Hiện tại
RATE_LIMIT_DELAY = 0.7  # fixed

# Mục tiêu (adaptive)
if no_errors_in_last_100_requests:
    rate_limit_delay = 0.3  # Aggressive
elif error_rate < 1%:
    rate_limit_delay = 0.5  # Moderate
else:
    rate_limit_delay = 0.7  # Conservative
```

**Metrics**:
- Average rate limit delay: < 0.5s (từ 0.7s)
- 429 error rate: < 0.1%
- Throughput improvement: +30-50%

#### 🔧 Task 3.2.2: IP Rotation (Advanced)
**Priority**: Low  
**Effort**: 1 tuần  
**Expected Impact**: +50-100% throughput (nếu có multiple IPs)

**Actions**:
- [ ] Setup proxy rotation (nếu có multiple IPs)
- [ ] Distribute requests across IPs
- [ ] Monitor IP-specific rate limits
- [ ] Auto-switch khi IP bị block

**Metrics**:
- IP utilization: Balanced
- Block rate per IP: < 1%
- Throughput: +50-100%

---

## 🔀 PHASE 4: TỐI ƯU BATCH PROCESSING (Tuần 4-5)

**Mục tiêu**: Tối ưu batch size và parallelism

### 4.1 Batch Size Optimization

#### 🔧 Task 4.1.1: Dynamic Batch Sizing
**Priority**: Medium  
**Effort**: 2-3 ngày  
**Expected Impact**: +10-20% parallelism

**Actions**:
- [ ] Phân tích optimal batch size dựa trên:
  - Product complexity
  - Network conditions
  - Available resources
- [ ] Implement dynamic batch sizing:
  - Small batches (8-10) cho products phức tạp
  - Medium batches (12-15) cho products thông thường
  - Large batches (20-25) cho products đơn giản
- [ ] Monitor và adjust tự động

**Cấu hình đề xuất**:
```python
# Hiện tại
PRODUCT_BATCH_SIZE = 12  # fixed

# Mục tiêu (dynamic)
if product_complexity == "high":
    batch_size = 8
elif product_complexity == "medium":
    batch_size = 12
else:
    batch_size = 15
```

**Metrics**:
- Optimal batch size: Tự động tìm
- Parallelism: +10-20%
- Batch efficiency: > 90%

#### 🔧 Task 4.1.2: Batch Prioritization
**Priority**: Low  
**Effort**: 1-2 ngày  
**Expected Impact**: +5-10% priority products crawled

**Actions**:
- [ ] Prioritize batches với popular products
- [ ] Process high-priority batches first
- [ ] Queue low-priority batches
- [ ] Dynamic priority adjustment

---

### 4.2 Parallelism Optimization

#### 🔧 Task 4.2.1: Task Scheduling Optimization
**Priority**: Medium  
**Effort**: 2-3 ngày  
**Expected Impact**: +15-25% efficiency

**Actions**:
- [ ] Optimize task distribution
- [ ] Balance load across workers
- [ ] Minimize idle time
- [ ] Implement work stealing

**Metrics**:
- Worker utilization: > 90%
- Idle time: < 5%
- Load balance: ±10%

---

## 🎯 PHASE 5: ADVANCED OPTIMIZATIONS (Tuần 5-6)

**Mục tiêu**: Các tối ưu tiên tiến để đạt 50-100 products/min

### 5.1 Alternative Crawling Strategies

#### 🔧 Task 5.1.1: Hybrid Crawling (Selenium + HTTP)
**Priority**: High  
**Effort**: 1 tuần  
**Expected Impact**: +50-100% speed

**Actions**:
- [ ] Phân tích khi nào cần Selenium vs HTTP
- [ ] Use HTTP cho simple pages (product list)
- [ ] Use Selenium chỉ cho dynamic content
- [ ] Smart routing dựa trên content type

**Metrics**:
- HTTP-only ratio: > 40%
- Selenium usage: < 60%
- Speed improvement: +50-100%

#### 🔧 Task 5.1.2: API-Based Crawling (Nếu có)
**Priority**: High  
**Effort**: 1-2 tuần  
**Expected Impact**: +200-500% speed

**Actions**:
- [ ] Phát hiện API endpoints của Tiki
- [ ] Reverse engineer API calls
- [ ] Implement API-based crawling
- [ ] Fallback to Selenium nếu cần

**Metrics**:
- API usage: > 80% (nếu available)
- Speed improvement: +200-500%
- Data quality: Maintain

---

### 5.2 Resource Optimization

#### 🔧 Task 5.2.1: Memory Optimization
**Priority**: Medium  
**Effort**: 3-4 ngày  
**Expected Impact**: Support more concurrent browsers

**Actions**:
- [ ] Profile memory usage per browser
- [ ] Optimize browser memory footprint
- [ ] Implement memory limits và cleanup
- [ ] Garbage collection tuning

**Metrics**:
- Memory per browser: < 200MB (từ 250-300MB)
- Total memory: < 6GB cho 30 browsers
- Memory leaks: 0

#### 🔧 Task 5.2.2: CPU Optimization
**Priority**: Low  
**Effort**: 2-3 ngày  
**Expected Impact**: Better CPU utilization

**Actions**:
- [ ] Profile CPU usage
- [ ] Optimize CPU-intensive operations
- [ ] Use async/await effectively
- [ ] Reduce blocking operations

**Metrics**:
- CPU utilization: 70-85% (optimal)
- CPU per browser: < 5%
- Idle CPU: < 10%

---

## 📊 TỔNG KẾT ROADMAP

### Timeline Tổng Thể

```
Tuần 1-2: Tối Ưu Selenium
├── Browser optimization (-30-40%)
├── Browser pooling (-20-30%)
├── Smart waiting (-15-25%)
└── Pool scaling (+50-100%)

Tuần 2-3: Tối Ưu Caching
├── Cache TTL optimization (+20-30% hit rate)
├── Cache key optimization (+5-10% hit rate)
├── Partial cache (+10-15% usage)
└── Redis optimization (+10-20% performance)

Tuần 3-4: Tối Ưu Network
├── Connection pooling (-30-40% overhead)
├── DNS caching (-20-30% lookup time)
├── Request optimization (-10-15% bandwidth)
└── Rate limiting (-30-50% delay)

Tuần 4-5: Batch Optimization
├── Dynamic batch sizing (+10-20% parallelism)
└── Task scheduling (+15-25% efficiency)

Tuần 5-6: Advanced
├── Hybrid crawling (+50-100% speed)
├── API-based (nếu có) (+200-500% speed)
├── Memory optimization
└── CPU optimization
```

### Expected Performance Improvement

| Phase | Current Speed | Target Speed | Improvement |
|-------|--------------|--------------|-------------|
| **Baseline** | 2.8 products/min | - | - |
| **After Phase 1** | 4-5 products/min | - | +43-79% |
| **After Phase 2** | 7-10 products/min | - | +150-257% |
| **After Phase 3** | 12-18 products/min | - | +329-543% |
| **After Phase 4** | 18-25 products/min | - | +543-793% |
| **After Phase 5** | 30-60 products/min | 50-100 products/min | +971-3471% |

**Final Target**: **50-100 products/min** (18-35x faster)

### Priority Matrix

#### 🔴 High Priority (Must Do)
1. Selenium pool scaling (Phase 1.2.1)
2. Cache TTL optimization (Phase 2.1.1)
3. Dynamic rate limiting (Phase 3.2.1)
4. Hybrid crawling (Phase 5.1.1)

#### 🟡 Medium Priority (Should Do)
5. Browser optimization (Phase 1.1.1)
6. Connection pooling (Phase 3.1.1)
7. Dynamic batch sizing (Phase 4.1.1)
8. Memory optimization (Phase 5.2.1)

#### 🟢 Low Priority (Nice to Have)
9. Distributed Selenium (Phase 1.2.2)
10. IP rotation (Phase 3.2.2)
11. API-based crawling (Phase 5.1.2)
12. CPU optimization (Phase 5.2.2)

---

## 📈 SUCCESS METRICS

### Performance Metrics
- [ ] Crawl speed: **50-100 products/min** (từ 2.8)
- [ ] E2E time: **3-6 phút** cho 280 products (từ 110 phút)
- [ ] Selenium time: **< 3s/product** (từ 5-9s)
- [ ] Cache hit rate: **> 60%** (từ 35-40%)
- [ ] Rate limit delay: **< 0.5s** (từ 0.7s)

### Resource Metrics
- [ ] Memory usage: **< 6GB** cho 30 browsers
- [ ] CPU usage: **70-85%** (optimal)
- [ ] Network efficiency: **> 95%** connection reuse
- [ ] Browser reuse: **> 80%**

### Quality Metrics
- [ ] Success rate: **> 95%** (maintain hoặc improve)
- [ ] Data accuracy: **> 98%** (maintain)
- [ ] Error rate: **< 2%**
- [ ] Retry rate: **< 5%**

---

## ⚠️ RISKS & MITIGATION

### Technical Risks

| Risk | Probability | Impact | Mitigation |
|------|------------|--------|------------|
| **Tiki rate limiting** | High | Critical | Monitor 429 errors, adaptive rate limiting |
| **Memory exhaustion** | Medium | High | Monitor memory, set limits, optimize browsers |
| **Browser crashes** | Medium | Medium | Health checks, auto-restart, error handling |
| **Cache inconsistency** | Low | Medium | Cache validation, TTL management |
| **Network instability** | Medium | Medium | Retry logic, circuit breaker |

### Mitigation Strategies

1. **Gradual Rollout**: Test từng optimization riêng lẻ
2. **Monitoring**: Real-time monitoring cho metrics
3. **Rollback Plan**: Có thể rollback nhanh nếu có vấn đề
4. **A/B Testing**: So sánh before/after cho mỗi change
5. **Staging Environment**: Test trước khi deploy production

---

## 🚀 IMPLEMENTATION PLAN

### Week 1: Quick Wins
- [ ] Task 1.1.1: Browser optimization
- [ ] Task 3.2.1: Dynamic rate limiting (start conservative)
- [ ] Task 2.1.1: Cache TTL optimization

**Expected**: +50-70% speed improvement

### Week 2-3: Scaling
- [ ] Task 1.2.1: Selenium pool scaling (15 → 25)
- [ ] Task 3.1.1: Connection pooling optimization
- [ ] Task 2.1.2: Cache key optimization

**Expected**: +100-150% speed improvement (cumulative)

### Week 4-5: Advanced
- [ ] Task 5.1.1: Hybrid crawling
- [ ] Task 4.1.1: Dynamic batch sizing
- [ ] Task 1.1.2: Browser pooling

**Expected**: +200-300% speed improvement (cumulative)

### Week 6: Polish & Optimize
- [ ] Task 5.2.1: Memory optimization
- [ ] Fine-tuning dựa trên metrics
- [ ] Performance testing

**Expected**: **50-100 products/min** (target achieved)

---

## 📚 TÀI LIỆU THAM KHẢO

### Code References
- `src/pipelines/crawl/config.py` - Cấu hình hiện tại
- `src/pipelines/crawl/crawl_products_detail.py` - Crawl logic
- `airflow/dags/tiki_crawl_products_dag.py` - DAG implementation

### Related Documentation
- `OPTIMIZATION_ROADMAP.md` - Performance optimization history
- `OPTIMIZATION_COMPLETED.md` - Completed optimizations
- `PERFORMANCE_ANALYSIS.md` - Performance analysis

---

## ✅ NEXT STEPS

### Immediate (Tuần này)
1. [ ] Review và approve roadmap
2. [ ] Setup tracking cho metrics
3. [ ] Bắt đầu Phase 1 Task 1.1.1 (Browser optimization)

### Short Term (2 tuần tới)
1. [ ] Complete Phase 1 & 2
2. [ ] Đo baseline metrics
3. [ ] Start Phase 3

### Medium Term (1 tháng tới)
1. [ ] Complete tất cả phases
2. [ ] Đạt target 50-100 products/min
3. [ ] Document results

---

**Roadmap Owner**: Development Team  
**Review Frequency**: Hàng tuần  
**Last Updated**: 2025-12-01  
**Next Review**: 2025-12-08

