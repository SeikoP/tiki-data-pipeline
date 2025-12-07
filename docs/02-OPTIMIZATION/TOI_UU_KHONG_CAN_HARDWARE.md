# ⚡ TỐI ƯU NGAY - KHÔNG CẦN THAY ĐỔI HARDWARE

**Ngày tạo**: 2025-12-01  
**Mục tiêu**: Các tối ưu hóa có thể triển khai NGAY LẬP TỨC mà không cần upgrade hardware, thêm RAM, CPU hay infrastructure

---

## 🎯 TÓM TẮT ĐIỀU HÀNH

### Các Tối Ưu Có Thể Bắt Đầu Ngay

| Loại Tối Ưu | Expected Impact | Effort | Có Thể Bắt Đầu |
|-------------|----------------|--------|----------------|
| **Configuration Tuning** | +20-40% | 1-2 giờ | ✅ NGAY |
| **Caching Strategy** | +30-50% | 2-4 giờ | ✅ NGAY |
| **Rate Limiting Adaptive** | +20-30% | 3-6 giờ | ✅ NGAY |
| **Browser Flags Optimization** | +20-30% | 1-2 giờ | ✅ NGAY |
| **Smart Waiting** | +15-25% | 2-4 giờ | ✅ NGAY |
| **Code Algorithm** | +10-20% | 4-8 giờ | ✅ NGAY |
| **Data Structure** | +10-15% | 2-3 giờ | ✅ NGAY |
| **Connection Reuse** | +15-20% | 1-2 giờ | ✅ NGAY |

**Tổng Expected Improvement**: **+50-100% tốc độ** (không cần hardware)

---

## 🚀 1. CONFIGURATION TUNING (1-2 giờ)

### 1.1 Tối Ưu Các Tham Số Hiện Có

**Không cần code changes, chỉ cần thay đổi config!**

#### ✅ Task 1.1.1: Tăng DNS Cache TTL
**Impact**: +10-15%  
**Effort**: 5 phút  
**Risk**: Thấp

**Hiện tại**:
```python
HTTP_DNS_CACHE_TTL = 300  # 5 phút
```

**Đề xuất**:
```python
HTTP_DNS_CACHE_TTL = 1800  # 30 phút (tăng 6x)
```

**Lý do**: DNS lookup cho Tiki.vn không thay đổi thường xuyên. Cache lâu hơn giảm số lần lookup.

**Cách làm**:
1. Sửa `src/pipelines/crawl/config.py`
2. Thay đổi `HTTP_DNS_CACHE_TTL = 1800`
3. Restart service

**Expected**: DNS lookup time giảm từ 50-200ms → 5-10ms (cache hit)

---

#### ✅ Task 1.1.2: Tối Ưu Connection Pool Limits
**Impact**: +15-20%  
**Effort**: 15 phút  
**Risk**: Thấp

**Hiện tại**:
```python
HTTP_CONNECTOR_LIMIT = 100
HTTP_CONNECTOR_LIMIT_PER_HOST = 10
```

**Đề xuất**:
```python
HTTP_CONNECTOR_LIMIT = 150  # Tăng 50%
HTTP_CONNECTOR_LIMIT_PER_HOST = 15  # Tăng 50%
```

**Lý do**: Connection pooling hiện tại có thể chưa tối ưu. Tăng limit giúp reuse connections tốt hơn.

**Cách làm**:
1. Sửa `src/pipelines/crawl/config.py`
2. Monitor connection usage
3. Tăng từ từ nếu stable

**Expected**: Connection reuse rate tăng từ 85% → 92-95%

---

#### ✅ Task 1.1.3: Adaptive Rate Limiting
**Impact**: +20-30%  
**Effort**: 2-4 giờ  
**Risk**: Trung bình (cần test cẩn thận)

**Hiện tại**:
```python
# Fixed rate limit
RATE_LIMIT_DELAY = 0.7  # Fixed 0.7s
```

**Đề xuất**: Dynamic rate limiting dựa trên response

```python
# Adaptive rate limiting
def get_rate_limit_delay(consecutive_successes, error_rate):
    """Tự động điều chỉnh delay dựa trên performance"""
    if consecutive_successes > 100 and error_rate < 0.5%:
        return 0.3  # Aggressive - ít delay nhất
    elif consecutive_successes > 50 and error_rate < 1%:
        return 0.5  # Moderate
    elif error_rate < 2%:
        return 0.7  # Current default
    else:
        return 1.0  # Conservative khi có nhiều errors
```

**Cách làm**:
1. Implement tracking cho consecutive_successes và error_rate
2. Update rate limit delay động
3. Monitor và adjust thresholds

**Expected**: Average delay giảm từ 0.7s → 0.4-0.5s (khi stable)

---

## 💾 2. CACHING STRATEGY (2-4 giờ)

### 2.1 Smart Cache Key Strategy

**Impact**: +10-15% cache hit rate  
**Effort**: 2-3 giờ

#### ✅ Task 2.1.1: URL Normalization Cải Tiến

**Hiện tại**: Có thể có nhiều URLs cho cùng 1 product
```
https://tiki.vn/product?spid=12345
https://tiki.vn/product/12345?utm_source=google
https://tiki.vn/product/12345?ref=category
```

**Đề xuất**: Normalize URL tốt hơn

```python
def normalize_product_url(url: str) -> str:
    """Chuẩn hóa URL để cache key consistent"""
    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
    
    parsed = urlparse(url)
    
    # Chỉ giữ các query params quan trọng
    important_params = ['spid', 'id', 'sku']
    query_params = parse_qs(parsed.query)
    filtered_params = {
        k: v[0] for k, v in query_params.items() 
        if k.lower() in important_params
    }
    
    # Rebuild URL
    new_query = urlencode(filtered_params)
    normalized = urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        new_query,
        ''  # Remove fragment
    ))
    
    return normalized
```

**Expected**: Cache hit rate tăng 10-15% (giảm duplicate cache entries)

---

#### ✅ Task 2.1.2: Cache Pre-Warming

**Impact**: +5-10% initial speed  
**Effort**: 1-2 giờ

**Ý tưởng**: Pre-cache các products phổ biến trước khi crawl

```python
def warm_cache(popular_product_ids: list[str]):
    """Pre-cache popular products"""
    for product_id in popular_product_ids[:100]:  # Top 100
        url = f"https://tiki.vn/product/{product_id}"
        # Crawl và cache (không block main crawl)
        asyncio.create_task(crawl_and_cache(url))
```

**Cách làm**:
1. Identify top popular products (từ DB hoặc previous crawl)
2. Background task để pre-cache
3. Chạy trước khi main crawl start

**Expected**: Khi crawl start, 20-30% products đã có cache

---

#### ✅ Task 2.1.3: Partial Cache Strategy

**Impact**: +15-20% effective cache  
**Effort**: 2-3 giờ

**Hiện tại**: Cache toàn bộ product data hoặc không cache gì cả

**Đề xuất**: Cache từng phần (fields)

```python
# Cache structure
{
    "product:12345:basic": {...},  # name, price, rating
    "product:12345:details": {...},  # description, specs
    "product:12345:images": [...],  # images
}

# Khi crawl, chỉ fetch phần chưa có
if cached_basic:
    use_cached_basic()
    fetch_only_details_and_images()
```

**Expected**: Cache hit rate tăng 15-20% (có thể dùng partial data)

---

## 🌐 3. NETWORK OPTIMIZATION (1-2 giờ)

### 3.1 Connection Reuse Tối Ưu

**Impact**: +15-20%  
**Effort**: 1 giờ

#### ✅ Task 3.1.1: HTTP/2 Support (Nếu Tiki hỗ trợ)

```python
import aiohttp

connector = aiohttp.TCPConnector(
    limit=150,
    limit_per_host=15,
    ttl_dns_cache=1800,
    force_close=False,  # Keep connections alive
    enable_cleanup_closed=True,
    use_dns_cache=True,
)

# Enable HTTP/2 nếu server hỗ trợ
async with aiohttp.ClientSession(
    connector=connector,
    version=aiohttp.ClientHttpVersion.HTTP_2  # Thử HTTP/2
) as session:
    ...
```

**Expected**: Multiplexing requests, giảm latency 10-15%

---

#### ✅ Task 3.1.2: Request Header Optimization

**Impact**: +5-10%  
**Effort**: 30 phút

```python
# Minimize headers
headers = {
    'User-Agent': 'Mozilla/5.0...',
    'Accept': 'text/html,application/xhtml+xml',
    'Accept-Language': 'vi-VN,vi;q=0.9',
    # Bỏ các headers không cần thiết
    # 'Accept-Encoding': 'gzip, deflate' - let aiohttp handle
    # 'Connection': 'keep-alive' - default
}
```

**Expected**: Request size nhỏ hơn, faster transmission

---

## 🖥️ 4. BROWSER OPTIMIZATION (1-2 giờ)

### 4.1 Chrome Flags Tối Ưu

**Impact**: +20-30% browser speed  
**Effort**: 1 giờ

#### ✅ Task 4.1.1: Add Performance Flags

**Hiện tại**: Có thể chưa có đủ flags

**Đề xuất**:
```python
from selenium.webdriver.chrome.options import Options

options = Options()
options.add_argument('--headless=new')
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

# THÊM CÁC FLAGS MỚI:
options.add_argument('--disable-software-rasterizer')
options.add_argument('--disable-extensions')
options.add_argument('--disable-plugins')
options.add_argument('--disable-images')  # Block images
options.add_argument('--blink-settings=imagesEnabled=false')
options.add_argument('--disable-javascript')  # Nếu không cần JS
options.add_argument('--disable-css')  # Nếu không cần CSS
options.add_argument('--disable-background-networking')
options.add_argument('--disable-background-timer-throttling')
options.add_argument('--disable-renderer-backgrounding')
options.add_argument('--disable-backgrounding-occluded-windows')
options.add_argument('--disable-ipc-flooding-protection')

# Memory optimization
options.add_argument('--memory-pressure-off')
options.add_argument('--max_old_space_size=4096')

# Performance
options.add_argument('--disable-features=TranslateUI')
options.add_argument('--disable-ipc-flooding-protection')
```

**Expected**: Browser load time giảm 20-30%

---

#### ✅ Task 4.1.2: Block Unnecessary Resources

**Impact**: +15-20% page load  
**Effort**: 1 giờ

```python
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

caps = DesiredCapabilities.CHROME
caps['goog:chromeOptions'] = {
    'prefs': {
        'profile.managed_default_content_settings.images': 2,  # Block images
        'profile.default_content_setting_values.stylesheets': 2,  # Block CSS
        'profile.default_content_setting_values.javascript': 2,  # Block JS (nếu không cần)
        'profile.default_content_setting_values.plugins': 2,  # Block plugins
        'profile.default_content_setting_values.media_stream': 2,  # Block media
    }
}

# Hoặc dùng Chrome DevTools Protocol
driver.execute_cdp_cmd('Network.setBlockedURLs', {
    'urls': [
        '*://*.google-analytics.com/*',
        '*://*.googletagmanager.com/*',
        '*://*.facebook.com/*',
        '*://*.doubleclick.net/*',
        # Block ads & tracking
    ]
})
```

**Expected**: Page load size giảm 50-70%, load time giảm 15-20%

---

## ⏱️ 5. SMART WAITING (2-4 giờ)

### 5.1 Replace time.sleep() với Explicit Waits

**Impact**: +15-25%  
**Effort**: 2-3 giờ

#### ✅ Task 5.1.1: Conditional Waits

**Hiện tại**: Có thể có nhiều `time.sleep()` fixed

**Đề xuất**: Wait cho specific elements

```python
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# Thay vì
time.sleep(3)  # Fixed wait

# Dùng
wait = WebDriverWait(driver, timeout=5)
try:
    # Chờ element xuất hiện (tối đa 5s)
    element = wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, '.product-price'))
    )
    # Element đã load, tiếp tục ngay
except TimeoutException:
    # Element không load, timeout sau 5s (thay vì wait đủ 3s)
    pass
```

**Expected**: Average wait time giảm từ 3s → 1-1.5s (nếu page load nhanh)

---

#### ✅ Task 5.1.2: Skip Unnecessary Waits

**Impact**: +10-15%  
**Effort**: 1-2 giờ

**Ý tưởng**: Chỉ wait khi thực sự cần

```python
def extract_product_detail_smart(driver, url):
    # 1. Load page
    driver.get(url)
    
    # 2. Chờ price element (critical)
    try:
        price = wait_for_element(driver, '.product-price', timeout=3)
    except:
        return None  # Fail fast nếu không có price
    
    # 3. Chỉ wait cho description nếu cần
    if need_description:
        description = wait_for_element(driver, '.product-description', timeout=2)
    else:
        description = None  # Skip wait
    
    # 4. Không wait cho reviews (non-critical)
    reviews = driver.find_elements(By.CSS_SELECTOR, '.reviews')
    # Process nếu có, không thì None
    
    return {...}
```

**Expected**: 30-40% products không cần wait đủ time

---

## 🔄 6. CODE ALGORITHM (4-8 giờ)

### 6.1 Batch Processing Optimization

**Impact**: +10-20%  
**Effort**: 2-3 giờ

#### ✅ Task 6.1.1: Dynamic Batch Sizing

**Hiện tại**: Fixed batch size = 12

**Đề xuất**: Adaptive batch size

```python
def calculate_optimal_batch_size(
    product_count: int,
    available_workers: int,
    avg_processing_time: float
) -> int:
    """Tính optimal batch size dựa trên context"""
    
    # Mục tiêu: Mỗi batch xử lý trong 30-60 giây
    target_batch_time = 45  # seconds
    
    # Ước tính products per batch
    products_per_batch = target_batch_time / avg_processing_time
    
    # Đảm bảo không quá nhỏ hoặc quá lớn
    min_batch = 5
    max_batch = 20
    
    optimal = max(min_batch, min(max_batch, int(products_per_batch)))
    
    return optimal
```

**Expected**: Batch efficiency tăng 10-15%

---

#### ✅ Task 6.2.2: Parallel Processing Optimization

**Impact**: +10-15%  
**Effort**: 2-3 giờ

**Hiện tại**: Có thể có sequential processing

**Đề xuất**: Parallelize tất cả independent tasks

```python
# Thay vì sequential
for product in products:
    detail = crawl_detail(product.url)
    transform = transform_product(detail)
    save(transform)

# Dùng parallel
async def process_product(product):
    detail = await crawl_detail(product.url)
    transform = transform_product(detail)
    await save(transform)

# Run parallel
tasks = [process_product(p) for p in products]
await asyncio.gather(*tasks, return_exceptions=True)
```

**Expected**: Throughput tăng 10-15%

---

### 6.2 Data Structure Optimization

**Impact**: +10-15%  
**Effort**: 2-3 giờ

#### ✅ Task 6.2.1: Use Generators Thay Vì Lists

```python
# Thay vì
def get_products():
    products = []
    for category in categories:
        products.extend(get_products_from_category(category))
    return products  # Load tất cả vào memory

# Dùng generator
def get_products():
    for category in categories:
        yield from get_products_from_category(category)
        # Stream processing, không load hết vào memory
```

**Expected**: Memory usage giảm, có thể process nhiều products hơn

---

#### ✅ Task 6.2.2: Cache Data Structures

**Impact**: +5-10%  
**Effort**: 1-2 giờ

```python
# Cache parsed HTML trees
@lru_cache(maxsize=1000)
def parse_html(html_content: str):
    return BeautifulSoup(html_content, 'html.parser')

# Cache regex patterns
PRICE_PATTERN = re.compile(r'(\d{1,3}(?:\.\d{3})*)')
# Reuse thay vì compile mỗi lần
```

**Expected**: Parsing speed tăng 5-10%

---

## 📋 IMPLEMENTATION PRIORITY

### 🔴 HIGH PRIORITY (Bắt Đầu Ngay)

1. **Configuration Tuning** (1-2 giờ)
   - Tăng DNS cache TTL → 1800s
   - Optimize connection pool limits
   - ✅ **Impact**: +20-30%, **Risk**: Thấp

2. **Adaptive Rate Limiting** (2-4 giờ)
   - Dynamic delay based on success/error rate
   - ✅ **Impact**: +20-30%, **Risk**: Trung bình

3. **Browser Flags Optimization** (1 giờ)
   - Add performance flags
   - Block unnecessary resources
   - ✅ **Impact**: +20-30%, **Risk**: Thấp

### 🟡 MEDIUM PRIORITY (Tuần này)

4. **Smart Waiting** (2-4 giờ)
   - Replace time.sleep() với explicit waits
   - Skip unnecessary waits
   - ✅ **Impact**: +15-25%, **Risk**: Thấp

5. **Caching Strategy** (2-4 giờ)
   - URL normalization
   - Partial cache
   - ✅ **Impact**: +15-20%, **Risk**: Thấp

6. **Code Algorithm** (4-8 giờ)
   - Dynamic batch sizing
   - Parallel processing
   - ✅ **Impact**: +10-20%, **Risk**: Thấp

### 🟢 LOW PRIORITY (Tuần sau)

7. **Data Structure** (2-3 giờ)
   - Use generators
   - Cache data structures
   - ✅ **Impact**: +10-15%, **Risk**: Thấp

8. **Network Optimization** (1-2 giờ)
   - HTTP/2 support
   - Request header optimization
   - ✅ **Impact**: +10-15%, **Risk**: Thấp

---

## 🚀 QUICK START PLAN

### Ngày 1 (2-3 giờ)
- [x] Task 1.1.1: Tăng DNS cache TTL
- [x] Task 1.1.2: Tối Ưu connection pool
- [x] Task 4.1.1: Add browser flags

**Expected**: +30-40% improvement

### Ngày 2 (4-6 giờ)
- [x] Task 3.2.1: Adaptive rate limiting
- [x] Task 5.1.1: Smart waiting
- [x] Task 4.1.2: Block resources

**Expected**: +40-50% cumulative improvement

### Ngày 3 (4-6 giờ)
- [x] Task 2.1.1: URL normalization
- [x] Task 2.1.3: Partial cache
- [x] Task 6.1.1: Dynamic batch sizing

**Expected**: +60-70% cumulative improvement

---

## 📊 EXPECTED RESULTS

### After All Optimizations

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| **Tốc độ crawl** | 2.8 products/min | **5-8 products/min** | **+79-186%** |
| **Cache hit rate** | 35-40% | **50-60%** | **+15-20%** |
| **Average delay** | 0.7s | **0.4-0.5s** | **-30-40%** |
| **Browser load time** | 5-9s | **3-6s** | **-30-40%** |
| **Memory usage** | Current | **Same/Reduced** | - |

### Timeline

- **Tuần 1**: Implement High Priority tasks → +50-70% improvement
- **Tuần 2**: Implement Medium Priority tasks → +70-90% cumulative
- **Tuần 3**: Implement Low Priority tasks → +90-100% cumulative

**Total Expected**: **2-3x faster** mà không cần hardware changes!

---

## ⚠️ LƯU Ý

### Risks & Mitigation

1. **Adaptive Rate Limiting**
   - Risk: Có thể bị block nếu quá aggressive
   - Mitigation: Start conservative, monitor errors, adjust dần

2. **Browser Flags**
   - Risk: Một số flags có thể break functionality
   - Mitigation: Test từng flag, rollback nếu có vấn đề

3. **Configuration Changes**
   - Risk: Có thể gây unexpected behavior
   - Mitigation: Test trong staging trước, monitor metrics

---

## ✅ CHECKLIST

### Immediate (Hôm nay)
- [ ] Tăng DNS cache TTL → 1800s
- [ ] Optimize connection pool limits
- [ ] Add browser performance flags

### This Week
- [ ] Implement adaptive rate limiting
- [ ] Replace time.sleep() với explicit waits
- [ ] Block unnecessary browser resources
- [ ] URL normalization cho cache

### Next Week
- [ ] Partial cache strategy
- [ ] Dynamic batch sizing
- [ ] Parallel processing optimization
- [ ] Data structure improvements

---

**Tổng kết**: Với các tối ưu trên, có thể đạt **2-3x improvement** mà không cần bất kỳ thay đổi hardware nào. Tất cả đều là code/configuration changes và có thể implement ngay!

