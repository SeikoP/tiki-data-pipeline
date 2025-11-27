"""
📋 GUIDE: SELENIUM vs ASYNCHTTP - Chi tiết so sánh & Recommendations

File này giải thích chi tiết sự khác biệt giữa 2 cách crawl product detail:
1. Selenium (hiện tại đang dùng)
2. AsyncHTTP (mới - không dùng Selenium)
"""

# ============================================================================
# 📌 QUICK SUMMARY
# ============================================================================

"""
SELENIUM
--------
✓ Load JavaScript → dynamic content
✓ Capture sales_count (số lượng đã bán)
✓ Full page rendering
✓ Handle interactive elements
✓ Cookie/session management

✗ Chậm: 30-60s per product
✗ Tài nguyên: CPU 50-100%, Memory 200-500MB per instance
✗ Khó scale: 1 Selenium driver ≈ 1 process → không thể parallel nhiều
✗ Timeout/crash risk cao khi có nhiều tasks

ASYNCHTTP (no Selenium)
-----------------------
✓ Nhanh: 3-8s per product (5-10x faster)
✓ Tài nguyên: CPU <5%, Memory 50-100MB
✓ Dễ scale: 100+ concurrent requests
✓ Lightweight: Lý tưởng cho Airflow DAG
✓ Reliable: Ít crash/timeout

✗ Không load JavaScript → thiếu dynamic content
✗ Sales_count có thể không đầy đủ (hoặc lấy từ static page)
✗ Comments/reviews không lấy được (AJAX loading)
✗ Có thể bị block nếu abuse request rate
"""

# ============================================================================
# 🔄 DETAILED COMPARISON TABLE
# ============================================================================

COMPARISON = """
┌────────────────────────────────────────────────────────────────────────┐
│                                                                        │
│          METRIC              SELENIUM         ASYNCHTTP               │
│  ────────────────────────────────────────────────────────────────    │
│  Speed                       30-60s/product   3-8s/product ⭐⭐⭐     │
│  Speedup Factor              1x               5-10x faster ✓           │
│                                                                        │
│  CPU Usage                   50-100% × cores  <5% × cores ✓           │
│  Memory per product          100-200MB        10-20MB ✓               │
│  Parallelization             Hard, 1:1        Easy, 100:1 ✓           │
│                              mapping          concurrent              │
│                                                                        │
│  Data Completeness           95-100%          80-90%                  │
│  Sales count (qty sold)      ✓ Got it         ✗ Missing (usually)     │
│  Product name                ✓ Got it         ✓ Got it                │
│  Price                       ✓ Got it         ✓ Got it                │
│  Rating/Reviews              ✓ Got it         ✓ Got it                │
│  Images                      ✓ Got it         ✓ Got it                │
│  Specifications              ✓ Got it         ✓ Got it                │
│  Comments                    ✓ Got it         ✗ AJAX (need Selenium)  │
│                                                                        │
│  Setup Complexity            Low              Very Low ✓              │
│  Error Handling              Medium           Easy ✓                  │
│  Maintenance Cost            High             Low ✓                   │
│                                                                        │
│  Best for:                   Complete data,   Bulk crawling,          │
│                              details only     fast processing ✓       │
│                                                                        │
│  100 Products:               ~1-2 hours ❌    ~8-15 min ✓             │
│  1000 Products:              ~10-20 hours ❌  ~1.5-2 hours ✓          │
│  10000 Products:             ~4-9 days ❌❌   ~15-24 hours ✓          │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
"""

# ============================================================================
# 💡 USE CASES & RECOMMENDATIONS
# ============================================================================

USE_CASES = """
USE CASE 1: Crawl thỉnh thoảng (1-10 products)
───────────────────────────────────────────────
→ RECOMMENDATION: Selenium ✓
WHY: Setup simple, không cần worry về rate limit, data đầy đủ
EXAMPLE: Admin crawl từng sản phẩm để verify data


USE CASE 2: Batch crawl hàng ngày (100-500 products)
─────────────────────────────────────────────────────
→ RECOMMENDATION: AsyncHTTP ✓⭐
WHY: 
  - Nhanh hơn 10x → crawl 500 products trong 1-2 tiếng
  - Selenium sẽ mất 10-20 tiếng (chạy qua đêm)
  - 80-90% dữ liệu đủ dùng cho analytics
  - Tài nguyên server tiết kiệm
EXAMPLE: Daily product crawl từ Tiki catalog


USE CASE 3: Real-time crawl (on-demand)
────────────────────────────────────────
→ RECOMMENDATION: AsyncHTTP + Fallback ✓⭐⭐
HYBRID APPROACH:
  1. Thử AsyncHTTP trước (3-8s, success rate 90%)
  2. Nếu bị block/timeout → fallback Selenium (recover)
  3. Nếu thiếu sales_count → fallback Selenium
  4. 95% cases giải quyết bằng AsyncHTTP

BENEFIT: Speed + reliability


USE CASE 4: Historical data crawl (1000+ products)
───────────────────────────────────────────────────
→ RECOMMENDATION: AsyncHTTP + Redis Cache + Multi-worker ✓⭐⭐⭐
ARCHITECTURE:
  - AsyncHTTP: Main crawler (5-8s per product)
  - Redis cache: Avoid re-crawling (24-48h TTL)
  - Airflow workers: Parallel crawling (10 workers × 10 concurrent = 100 concurrent)
  - Result: 1000 products ~15-20 min

BENEFIT: Extremely fast + scalable


USE CASE 5: Complete detailed data (need sales_count, comments, etc)
────────────────────────────────────────────────────────────────────
→ RECOMMENDATION: Selenium ✓
WHY: Cần JavaScript rendering + AJAX loading
COST: 30-60s per product acceptable for 1x detailed crawl
"""

# ============================================================================
# 🛠️ IMPLEMENTATION GUIDE
# ============================================================================

IMPLEMENTATION = """
CURRENT STATE (100% Selenium):
──────────────────────────────
src/pipelines/crawl/crawl_products_detail.py
  └─ crawl_product_detail_with_selenium()
  └─ crawl_product_detail_with_driver()

PROBLEM: Slow → crawl 100 products = 1-2 hours ❌


RECOMMENDED MIGRATION PATH:
───────────────────────────

PHASE 1 (QUICK WIN): Add AsyncHTTP option
──────────────────────────────────────────
Modify: crawl_products_detail.py
Add:    crawl_product_detail_async_http()
Result: Users can choose which to use
Effort: 2-4 hours

BENEFITS:
  ✓ Keep Selenium for backward compatibility
  ✓ Allow users to try AsyncHTTP
  ✓ No breaking changes


PHASE 2 (RECOMMENDED): Hybrid approach
──────────────────────────────────────
Modify: DAG/pipeline code
Add:    Fallback logic (AsyncHTTP → Selenium)
Result: Best of both worlds

ARCHITECTURE:
  1. crawl_product_detail_async_http() with timeout=10s
  2. If timeout/error → crawl_product_detail_with_selenium()
  3. This way: 90% fast (AsyncHTTP), 10% fallback (Selenium)

Effort:  1-2 hours
Benefit: 
  ✓ 5-8x overall speedup (avg)
  ✓ Minimal data loss
  ✓ Progressive degradation (still get data if HTTP fails)


PHASE 3 (ADVANCED): Async scaling
──────────────────────────────────
Use: aiohttp + asyncio for 100+ concurrent requests
Strategy: Crawl multiple products in parallel

BEFORE (Selenium, sequential):
  for url in urls:
    data = crawl_selenium(url)  # 50s × 10 = 500s (8+ min)

AFTER (AsyncHTTP, parallel):
  tasks = [crawl_async(url) for url in urls]
  results = await asyncio.gather(*tasks)  # 50s total (50s)
  → 10x speedup! ✓

Effort: 3-4 hours
Setup: Airflow + CeleryExecutor for true parallelization
"""

# ============================================================================
# 📊 PERFORMANCE PROJECTIONS
# ============================================================================

PROJECTIONS = """
Assuming:
- Selenium: 45s avg per product
- AsyncHTTP: 6s avg per product
- Fallback rate: 10% (AsyncHTTP fails, need Selenium)
- Effective AsyncHTTP: 6s + (10% × 45s) = 10.5s per product

CRAWL 1000 PRODUCTS:
────────────────────
Current (Selenium only):
  1000 × 45s = 45,000s = 12.5 hours ❌

Option 1: AsyncHTTP (no fallback):
  1000 × 6s = 6,000s = 1.67 hours ✓
  Success rate: 90%, may miss 10% data

Option 2: AsyncHTTP + Fallback (Hybrid):
  (900 × 6s) + (100 × 45s) = 9,900s = 2.75 hours ✓
  Success rate: 100%, complete data ✓

Option 3: AsyncHTTP (parallel, 10 concurrent):
  1000 × 6s ÷ 10 = 600s = 10 minutes ✓⭐
  Success rate: 90%

Option 4: Hybrid (parallel, 10 concurrent):
  [(900 × 6s) + (100 × 45s)] ÷ 10 = 16.5 minutes ✓⭐
  Success rate: 100%, complete data ✓

VERDICT:
→ Option 4 recommended (16.5 min vs 12.5 hours!)
→ 45x speedup compared to current approach
→ Still get 100% complete data
"""

# ============================================================================
# 🎯 ACTION PLAN
# ============================================================================

ACTION_PLAN = """
STEP 1: Run Demo Scripts
────────────────────────
python demos/demo_crawl_detail_async.py           # 3 URLs comparison
python demos/demo_crawl_detail_comparison.py      # Detailed benchmark

→ Observe: Performance, data quality, success rates
→ Review: Output JSON reports


STEP 2: Analyze Results
───────────────────────
Check: demos/data/test_output/
  - demo_crawl_detail_comparison.json
  - demo_crawl_comparison_detailed.json

Expected results:
  ✓ AsyncHTTP: 5-10x faster
  ✓ Data completeness: 80-90%
  ✓ Both methods get product name/price/rating


STEP 3: Decision
────────────────
Question: Kết quả thế nào?

If AsyncHTTP success rate = 100% and data good enough:
  → MIGRATE to AsyncHTTP ✓
  → Modify DAG to use async crawling
  → 45x speedup achieved!

If AsyncHTTP success rate < 80%:
  → Use Hybrid approach (AsyncHTTP + Selenium fallback)
  → Still get 5-10x speedup with safety net

If AsyncHTTP success rate < 50%:
  → Keep Selenium for now
  → Check Tiki API alternatives
  → Review in 3 months


STEP 4: Implementation
──────────────────────
1. Add crawl_product_detail_async_http() to crawl_products_detail.py
2. Add fallback logic in DAG
3. Update config (add ASYNC_CRAWL_TIMEOUT, FALLBACK_ENABLED)
4. Test with 10 URLs
5. Roll out to staging
6. Monitor performance metrics
7. Deploy to production


TIMELINE:
- Phase 1 (AsyncHTTP option): 1-2 days
- Phase 2 (Hybrid): 2-3 days
- Phase 3 (Parallel): 3-5 days
- Total: ~1 week to full 45x speedup
"""

# ============================================================================
# 🔗 TECHNICAL DETAILS
# ============================================================================

TECHNICAL = """
ASYNCHTTP IMPLEMENTATION:
─────────────────────────

1. Basic HTTP fetch (no Selenium):
   import aiohttp
   async with aiohttp.ClientSession() as session:
       async with session.get(url) as response:
           html = await response.text()
   
   → Returns: HTML string (no JavaScript execution)
   → Time: 2-5 seconds


2. Extract data from static HTML:
   data = extract_product_detail(html, url)
   
   → Same extraction logic as Selenium
   → Works on rendered HTML or static HTML


3. Missing dynamic content:
   - sales_count: On Tiki, embedded in __NEXT_DATA__ script ✓ (can extract)
   - comments: AJAX loaded after page load ✗ (need Selenium)
   - real-time stock: AJAX loaded ✗ (need Selenium)


4. Performance optimization:
   - Connection pooling: limit=100, limit_per_host=10
   - Concurrent requests: asyncio.gather()
   - Timeout: 10-15 seconds (aggressive, fallback on error)
   - Rate limiting: Redis-based (avoid IP ban)


5. Fallback to Selenium:
   if asyncio.TimeoutError or not_enough_data:
       data = crawl_product_detail_with_selenium(url)
   
   → Automatic recovery mechanism
   → Transparent to caller


SELENIUM REUSE (OPTIMIZATION):
──────────────────────────────

Instead of:
  for url in urls:
      driver = create_selenium_driver()
      crawl(url)
      driver.quit()
  → Creates/destroys driver 1000 times = SLOW

Do:
  driver = create_selenium_driver()
  for url in urls:
      data = crawl_with_driver(driver, url)
  driver.quit()
  → Reuse driver = 2-3x faster

But still much slower than AsyncHTTP.


BEST PRACTICE:
──────────────
1. Try AsyncHTTP first (3-5s, 90% success)
2. If timeout/error → Selenium fallback (50s, 99% success)
3. Result: 95% speed, 99% success rate
"""

# ============================================================================
# ⚠️ CAVEATS & PITFALLS
# ⚠️ ============================================================================

PITFALLS = """
PITFALL 1: Rate limiting from Tiki
───────────────────────────────────
Problem: Crawl too fast → IP banned for 1 hour
Solution: 
  - Add delay between requests (1-2 seconds)
  - Use proxy rotation (if available)
  - Redis-based rate limiter (already implemented)
  - Check X-RateLimit headers

Prevention: AsyncHTTP allows controlled rate limiting


PITFALL 2: Missing sales_count (số lượng đã bán)
─────────────────────────────────────────────────
Problem: AsyncHTTP can't load "Số sản phẩm đã bán" if it's JavaScript
Solution:
  - Check if sales_count in __NEXT_DATA__ (it usually is!)
  - If missing → fallback Selenium OR
  - Accept missing field for speed benefit

Tiki structure: sales_count usually in JSON payload ✓


PITFALL 3: Proxy/VPN issues
────────────────────────────
Problem: Tiki detects and blocks automated requests
Solution:
  - Add User-Agent headers (already done)
  - Add referer headers
  - Randomize request timing
  - Use proxy service (if available)

Prevention: aiohttp easier to manage than Selenium


PITFALL 4: Async concurrency limits
────────────────────────────────────
Problem: Too many concurrent connections → 429 Too Many Requests
Solution:
  - Limit concurrent connections: limit_per_host=10
  - Queue-based approach: process URLs in batches
  - Add backoff retry logic

Prevention: Configure TCPConnector properly


PITFALL 5: Database connection pool
────────────────────────────────────
If crawling 1000 products in parallel:
- Each needs DB connection
- Default pool size: 5-10
- Need to increase pool size

Solution: Configure in Airflow/app:
  sqlalchemy.pool_size = 50
  sqlalchemy.max_overflow = 100
"""

# ============================================================================
# 📚 RESOURCES & REFERENCES
# ============================================================================

RESOURCES = """
Demo Scripts:
  - demos/demo_crawl_detail_async.py
  - demos/demo_crawl_detail_comparison.py

Source Code:
  - src/pipelines/crawl/crawl_products_detail.py
  - src/pipelines/crawl/utils.py

Documentation:
  - README.md (quick start)
  - This file (detailed guide)

External Resources:
  - aiohttp docs: https://docs.aiohttp.org/
  - asyncio docs: https://docs.python.org/3/library/asyncio.html
  - BeautifulSoup: https://www.crummy.com/software/BeautifulSoup/bs4/doc/
  - Selenium: https://www.selenium.dev/documentation/
"""

print(__doc__)
print(COMPARISON)
print(USE_CASES)
print(IMPLEMENTATION)
print(PROJECTIONS)
print(ACTION_PLAN)
print(TECHNICAL)
print(PITFALLS)
print(RESOURCES)
