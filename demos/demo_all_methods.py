"""
Demo: Thử TẤT CẢ các cách crawl product detail

5+ methods comparison:
1. Selenium (browser automation - heaviest)
2. AsyncHTTP (async HTTP - lightweight)
3. HTTP Requests (sync HTTP - simple)
4. Playwright (modern browser - fast)
5. BeautifulSoup chuyên sâu (parsing - no fetch)
6. Subprocess Curl (system command - ultra fast)
"""

import json
import subprocess
import time
from pathlib import Path
from typing import Optional
from datetime import datetime

project_root = Path(__file__).parent.parent

print("=" * 90)
print("🔬 THỬ TẤT CẢ CÁC CÁCH CRAWL PRODUCT DETAIL")
print("=" * 90)
print()

results = {
    "timestamp": datetime.now().isoformat(),
    "methods": {},
    "comparison": [],
}

# URL test
test_url = "https://tiki.vn/binh-giu-nhiet-inox-304-elmich-el-8013ol-dung-tich-480ml-p120552065.html"

print(f"🔗 Test URL: {test_url[:70]}...\n")

# ============================================================================
# METHOD 1: CURL (System command - siêu nhanh)
# ============================================================================
print("1️⃣ CURL (System Command)")
print("-" * 90)
start = time.time()
try:
    result = subprocess.run(
        ["curl", "-s", "-L", "-H", "User-Agent: Mozilla/5.0", test_url],
        capture_output=True,
        timeout=15,
        text=True
    )
    elapsed = time.time() - start
    
    if result.returncode == 0 and len(result.stdout) > 1000:
        print(f"✅ Success ({elapsed:.2f}s, {len(result.stdout)} bytes)")
        results["methods"]["curl"] = {"success": True, "time": elapsed, "size": len(result.stdout)}
    else:
        print(f"⚠️  Failed ({elapsed:.2f}s)")
        results["methods"]["curl"] = {"success": False, "time": elapsed}
except Exception as e:
    elapsed = time.time() - start
    print(f"❌ Error: {e} ({elapsed:.2f}s)")
    results["methods"]["curl"] = {"success": False, "time": elapsed, "error": str(e)}

# ============================================================================
# METHOD 2: WGET (System command)
# ============================================================================
print("\n2️⃣ WGET (System Command)")
print("-" * 90)
start = time.time()
try:
    result = subprocess.run(
        ["wget", "-q", "-U", "Mozilla/5.0", "-O", "-", test_url],
        capture_output=True,
        timeout=15,
        text=True
    )
    elapsed = time.time() - start
    
    if result.returncode == 0 and len(result.stdout) > 1000:
        print(f"✅ Success ({elapsed:.2f}s, {len(result.stdout)} bytes)")
        results["methods"]["wget"] = {"success": True, "time": elapsed, "size": len(result.stdout)}
    else:
        print(f"⚠️  Failed ({elapsed:.2f}s)")
        results["methods"]["wget"] = {"success": False, "time": elapsed}
except Exception as e:
    elapsed = time.time() - start
    print(f"❌ Error: {e} ({elapsed:.2f}s)")
    results["methods"]["wget"] = {"success": False, "time": elapsed, "error": str(e)}

# ============================================================================
# METHOD 3: PYTHON REQUESTS (Sync HTTP - simple)
# ============================================================================
print("\n3️⃣ PYTHON REQUESTS (Sync HTTP)")
print("-" * 90)
start = time.time()
try:
    import requests
    response = requests.get(
        test_url,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=15
    )
    elapsed = time.time() - start
    
    if response.status_code == 200 and len(response.text) > 1000:
        print(f"✅ Success ({elapsed:.2f}s, {len(response.text)} bytes)")
        results["methods"]["requests"] = {"success": True, "time": elapsed, "size": len(response.text)}
    else:
        print(f"⚠️  HTTP {response.status_code} ({elapsed:.2f}s)")
        results["methods"]["requests"] = {"success": False, "time": elapsed, "status": response.status_code}
except Exception as e:
    elapsed = time.time() - start
    print(f"❌ Error: {e} ({elapsed:.2f}s)")
    results["methods"]["requests"] = {"success": False, "time": elapsed, "error": str(e)}

# ============================================================================
# METHOD 4: AIOHTTP (Async HTTP - fast)
# ============================================================================
print("\n4️⃣ AIOHTTP (Async HTTP)")
print("-" * 90)
start = time.time()
try:
    import aiohttp
    import asyncio
    
    async def fetch_aiohttp():
        connector = aiohttp.TCPConnector(ssl=False)
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            async with session.get(test_url) as response:
                if response.status == 200:
                    text = await response.text()
                    return len(text)
        return None
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    size = loop.run_until_complete(fetch_aiohttp())
    loop.close()
    elapsed = time.time() - start
    
    if size and size > 1000:
        print(f"✅ Success ({elapsed:.2f}s, {size} bytes)")
        results["methods"]["aiohttp"] = {"success": True, "time": elapsed, "size": size}
    else:
        print(f"⚠️  Failed ({elapsed:.2f}s)")
        results["methods"]["aiohttp"] = {"success": False, "time": elapsed}
except Exception as e:
    elapsed = time.time() - start
    print(f"❌ Error: {e} ({elapsed:.2f}s)")
    results["methods"]["aiohttp"] = {"success": False, "time": elapsed, "error": str(e)}

# ============================================================================
# METHOD 5: HTTPX (Modern HTTP client)
# ============================================================================
print("\n5️⃣ HTTPX (Modern HTTP Client)")
print("-" * 90)
start = time.time()
try:
    import httpx
    with httpx.Client() as client:
        response = client.get(
            test_url,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=15
        )
        elapsed = time.time() - start
        
        if response.status_code == 200 and len(response.text) > 1000:
            print(f"✅ Success ({elapsed:.2f}s, {len(response.text)} bytes)")
            results["methods"]["httpx"] = {"success": True, "time": elapsed, "size": len(response.text)}
        else:
            print(f"⚠️  HTTP {response.status_code} ({elapsed:.2f}s)")
            results["methods"]["httpx"] = {"success": False, "time": elapsed, "status": response.status_code}
except ImportError:
    print("❌ Not installed (pip install httpx)")
    results["methods"]["httpx"] = {"success": False, "error": "Not installed"}
except Exception as e:
    elapsed = time.time() - start
    print(f"❌ Error: {e} ({elapsed:.2f}s)")
    results["methods"]["httpx"] = {"success": False, "time": elapsed, "error": str(e)}

# ============================================================================
# METHOD 6: URLLIB (Python built-in)
# ============================================================================
print("\n6️⃣ URLLIB (Python Built-in)")
print("-" * 90)
start = time.time()
try:
    import urllib.request
    req = urllib.request.Request(
        test_url,
        headers={"User-Agent": "Mozilla/5.0"}
    )
    with urllib.request.urlopen(req, timeout=15) as response:
        text = response.read().decode('utf-8')
        elapsed = time.time() - start
        
        if len(text) > 1000:
            print(f"✅ Success ({elapsed:.2f}s, {len(text)} bytes)")
            results["methods"]["urllib"] = {"success": True, "time": elapsed, "size": len(text)}
        else:
            print(f"⚠️  Empty response ({elapsed:.2f}s)")
            results["methods"]["urllib"] = {"success": False, "time": elapsed}
except Exception as e:
    elapsed = time.time() - start
    print(f"❌ Error: {e} ({elapsed:.2f}s)")
    results["methods"]["urllib"] = {"success": False, "time": elapsed, "error": str(e)}

# ============================================================================
# METHOD 7: SELENIUM (Browser automation)
# ============================================================================
print("\n7️⃣ SELENIUM (Browser Automation)")
print("-" * 90)
start = time.time()
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    driver = webdriver.Chrome(options=options)
    driver.get(test_url)
    
    # Wait for page load
    WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located((By.TAG_NAME, "body"))
    )
    
    html = driver.page_source
    driver.quit()
    
    elapsed = time.time() - start
    
    if len(html) > 1000:
        print(f"✅ Success ({elapsed:.2f}s, {len(html)} bytes)")
        results["methods"]["selenium"] = {"success": True, "time": elapsed, "size": len(html)}
    else:
        print(f"⚠️  Empty page ({elapsed:.2f}s)")
        results["methods"]["selenium"] = {"success": False, "time": elapsed}
except ImportError:
    print("❌ Not installed (pip install selenium)")
    results["methods"]["selenium"] = {"success": False, "error": "Not installed"}
except Exception as e:
    elapsed = time.time() - start
    print(f"❌ Error: {e} ({elapsed:.2f}s)")
    results["methods"]["selenium"] = {"success": False, "time": elapsed, "error": str(e)}

# ============================================================================
# METHOD 8: PLAYWRIGHT (Modern headless browser)
# ============================================================================
print("\n8️⃣ PLAYWRIGHT (Modern Browser)")
print("-" * 90)
start = time.time()
try:
    from playwright.sync_api import sync_playwright
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(test_url, wait_until="networkidle")
        html = page.content()
        browser.close()
        
        elapsed = time.time() - start
        
        if len(html) > 1000:
            print(f"✅ Success ({elapsed:.2f}s, {len(html)} bytes)")
            results["methods"]["playwright"] = {"success": True, "time": elapsed, "size": len(html)}
        else:
            print(f"⚠️  Empty page ({elapsed:.2f}s)")
            results["methods"]["playwright"] = {"success": False, "time": elapsed}
except ImportError:
    print("❌ Not installed (pip install playwright)")
    results["methods"]["playwright"] = {"success": False, "error": "Not installed"}
except Exception as e:
    elapsed = time.time() - start
    print(f"❌ Error: {e} ({elapsed:.2f}s)")
    results["methods"]["playwright"] = {"success": False, "time": elapsed, "error": str(e)}

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 90)
print("📊 SUMMARY - TẤT CẢ CÁC CÁCH")
print("=" * 90)
print()

# Sort by speed
successful = {k: v for k, v in results["methods"].items() if v.get("success") and v.get("time")}
sorted_methods = sorted(successful.items(), key=lambda x: x[1]["time"])

print(f"{'METHOD':<20} {'TIME':<12} {'SIZE':<15} {'RANK':<10}")
print("-" * 90)

for rank, (method, data) in enumerate(sorted_methods, 1):
    time_str = f"{data['time']:.2f}s"
    size_str = f"{data.get('size', 'N/A')} bytes"
    speedup = successful[sorted_methods[0][0]]["time"] / data["time"] if sorted_methods else 0
    
    if rank == 1:
        print(f"{method:<20} {time_str:<12} {size_str:<15} 🏆 FASTEST")
    else:
        print(f"{method:<20} {time_str:<12} {size_str:<15} {speedup:.1f}x slower")

# Failed methods
print()
print("❌ FAILED:")
failed = {k: v for k, v in results["methods"].items() if not v.get("success")}
for method, data in failed.items():
    error = data.get("error", "Unknown error")
    print(f"   {method:<20} {error}")

# Recommendations
print()
print("=" * 90)
print("💡 KHUYẾN NGHỊ")
print("=" * 90)
print()

if sorted_methods:
    fastest = sorted_methods[0][0]
    print(f"⭐ CÁC NHANH NHẤT:")
    for i, (method, data) in enumerate(sorted_methods[:3], 1):
        speedup = successful[sorted_methods[0][0]]["time"] / data["time"] if sorted_methods else 0
        print(f"   {i}. {method:.<20} {data['time']:>6.2f}s  ({speedup:.1f}x base)")

print()
print("🎯 ĐỀ XUẤT:")
print("   • FASTEST: Curl/Wget (command line)")
print("   • BEST BALANCE: Requests/HTTPX/aiohttp")
print("   • JAVASCRIPT: Selenium/Playwright")
print("   • RECOMMENDED FOR PRODUCTION: AsyncHTTP + Selenium fallback")

# Save results
output_file = project_root / "data/test_output/all_methods_comparison.json"
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

print()
print(f"💾 Saved: {output_file}")
print()
print("=" * 90)
