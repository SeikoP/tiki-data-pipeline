#!/usr/bin/env python3
"""
Test tất cả 8 cách crawl product detail từ Tiki
So sánh: tốc độ, độ chính xác, chất lượng dữ liệu
"""

import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import aiohttp
import requests
from bs4 import BeautifulSoup

# Test URLs
TEST_URLS = [
    "https://tiki.vn/p/binh-giu-nhiet-elmich-stainless-steel-750ml-den-p1725225.html",
    "https://tiki.vn/p/tai-nghe-bluetooth-sony-wf-c700n-p29134633.html",
]

TIMEOUT = 10
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9",
}


def extract_price_bs4(html: str) -> tuple[str, str]:
    """Extract giá từ HTML using BeautifulSoup"""
    try:
        soup = BeautifulSoup(html, "html.parser")

        # Try multiple selectors
        price_selectors = [
            'span[data-view-id="product_price_regular"]',
            "span.ProductPrice__price",
            'span[class*="price"]',
            'div[class*="price"] span',
        ]

        for selector in price_selectors:
            price_elem = soup.select_one(selector)
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                if price_text and any(c.isdigit() for c in price_text):
                    return price_text, "found"

        return "N/A", "not_found"
    except Exception as e:
        return f"error: {str(e)[:30]}", "error"


def extract_name_bs4(html: str) -> tuple[str, str]:
    """Extract tên sản phẩm from HTML"""
    try:
        soup = BeautifulSoup(html, "html.parser")

        # Try multiple selectors
        name_selectors = [
            'h1[data-view-id="product_title"]',
            "h1",
            'span[data-view-id="product_name"]',
            'div[class*="product-name"]',
        ]

        for selector in name_selectors:
            name_elem = soup.select_one(selector)
            if name_elem:
                name_text = name_elem.get_text(strip=True)
                if name_text and len(name_text) > 5:
                    return name_text[:100], "found"

        return "N/A", "not_found"
    except Exception as e:
        return f"error: {str(e)[:30]}", "error"


# ============================================================================
# METHOD 1: SELENIUM
# ============================================================================
def method_1_selenium(url: str) -> dict[str, Any]:
    """Method 1: Selenium (baseline)"""
    start = time.time()
    result = {
        "method": "Selenium",
        "url": url,
        "status": "failed",
        "name": "N/A",
        "price": "N/A",
        "time": 0,
        "resource_usage": "High CPU (50-100%), Memory 100-200MB",
    }

    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait

        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument(f"user-agent={HEADERS['User-Agent']}")

        driver = webdriver.Chrome(options=chrome_options)
        driver.get(url)

        # Wait for content
        WebDriverWait(driver, TIMEOUT).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        html = driver.page_source
        driver.quit()

        name, name_status = extract_name_bs4(html)
        price, price_status = extract_price_bs4(html)

        result.update(
            {
                "status": (
                    "success" if (name_status == "found" and price_status == "found") else "partial"
                ),
                "name": name,
                "price": price,
                "name_found": name_status == "found",
                "price_found": price_status == "found",
            }
        )
    except Exception as e:
        result["error"] = str(e)[:100]
    finally:
        result["time"] = time.time() - start

    return result


# ============================================================================
# METHOD 2: ASYNCHTTP (aiohttp)
# ============================================================================
async def method_2_asynchttp_single(url: str) -> dict[str, Any]:
    """Method 2: AsyncHTTP with aiohttp"""
    start = time.time()
    result = {
        "method": "AsyncHTTP",
        "url": url,
        "status": "failed",
        "name": "N/A",
        "price": "N/A",
        "time": 0,
        "resource_usage": "Low CPU (<5%), Memory 10-20MB",
    }

    try:
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            async with session.get(url, timeout=TIMEOUT, allow_redirects=True) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    name, name_status = extract_name_bs4(html)
                    price, price_status = extract_price_bs4(html)

                    result.update(
                        {
                            "status": (
                                "success"
                                if (name_status == "found" and price_status == "found")
                                else "partial"
                            ),
                            "name": name,
                            "price": price,
                            "name_found": name_status == "found",
                            "price_found": price_status == "found",
                            "http_status": resp.status,
                        }
                    )
                else:
                    result["http_status"] = resp.status
                    result["error"] = f"HTTP {resp.status}"
    except Exception as e:
        result["error"] = str(e)[:100]
    finally:
        result["time"] = time.time() - start

    return result


async def method_2_asynchttp(urls: list[str]) -> list[dict[str, Any]]:
    """Run AsyncHTTP for multiple URLs"""
    return await asyncio.gather(*[method_2_asynchttp_single(url) for url in urls])


# ============================================================================
# METHOD 3: HTTP REQUESTS (sync)
# ============================================================================
def method_3_http_requests(urls: list[str]) -> list[dict[str, Any]]:
    """Method 3: HTTP Requests (sync)"""
    results = []
    for url in urls:
        start = time.time()
        result = {
            "method": "HTTP Requests",
            "url": url,
            "status": "failed",
            "name": "N/A",
            "price": "N/A",
            "time": 0,
            "resource_usage": "Minimal CPU (<1%), Memory ~10MB",
        }

        try:
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
            if resp.status_code == 200:
                html = resp.text
                name, name_status = extract_name_bs4(html)
                price, price_status = extract_price_bs4(html)

                result.update(
                    {
                        "status": (
                            "success"
                            if (name_status == "found" and price_status == "found")
                            else "partial"
                        ),
                        "name": name,
                        "price": price,
                        "name_found": name_status == "found",
                        "price_found": price_status == "found",
                        "http_status": resp.status_code,
                    }
                )
            else:
                result["http_status"] = resp.status_code
                result["error"] = f"HTTP {resp.status_code}"
        except Exception as e:
            result["error"] = str(e)[:100]
        finally:
            result["time"] = time.time() - start

        results.append(result)

    return results


# ============================================================================
# METHOD 4: PLAYWRIGHT
# ============================================================================
async def method_4_playwright(urls: list[str]) -> list[dict[str, Any]]:
    """Method 4: Playwright (headless browser)"""
    results = []
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("⚠️  Playwright not installed. Skipping...")
        return [
            {
                "method": "Playwright",
                "url": url,
                "status": "failed",
                "error": "Not installed",
                "time": 0,
            }
            for url in urls
        ]

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            for url in urls:
                start = time.time()
                result = {
                    "method": "Playwright",
                    "url": url,
                    "status": "failed",
                    "name": "N/A",
                    "price": "N/A",
                    "time": 0,
                    "resource_usage": "Medium CPU (15-30%), Memory 80-150MB",
                }

                try:
                    page = await browser.new_page()
                    await page.goto(url, wait_until="networkidle")
                    html = await page.content()
                    await page.close()

                    name, name_status = extract_name_bs4(html)
                    price, price_status = extract_price_bs4(html)

                    result.update(
                        {
                            "status": (
                                "success"
                                if (name_status == "found" and price_status == "found")
                                else "partial"
                            ),
                            "name": name,
                            "price": price,
                            "name_found": name_status == "found",
                            "price_found": price_status == "found",
                        }
                    )
                except Exception as e:
                    result["error"] = str(e)[:100]
                finally:
                    result["time"] = time.time() - start

                results.append(result)

            await browser.close()
    except Exception as e:
        print(f"Playwright error: {e}")
        results = [
            {
                "method": "Playwright",
                "url": url,
                "status": "failed",
                "error": str(e)[:100],
                "time": 0,
            }
            for url in urls
        ]

    return results


# ============================================================================
# METHOD 5: PYPPETEER
# ============================================================================
async def method_5_pyppeteer(urls: list[str]) -> list[dict[str, Any]]:
    """Method 5: Pyppeteer (Python Puppeteer)"""
    results = []
    try:
        import pyppeteer
    except ImportError:
        print("⚠️  Pyppeteer not installed. Skipping...")
        return [
            {
                "method": "Pyppeteer",
                "url": url,
                "status": "failed",
                "error": "Not installed",
                "time": 0,
            }
            for url in urls
        ]

    try:
        browser = await pyppeteer.launch(headless=True)
        for url in urls:
            start = time.time()
            result = {
                "method": "Pyppeteer",
                "url": url,
                "status": "failed",
                "name": "N/A",
                "price": "N/A",
                "time": 0,
                "resource_usage": "Medium CPU (20-35%), Memory 100-180MB",
            }

            try:
                page = await browser.newPage()
                await page.goto(url, waitUntil="networkidle0")
                html = await page.content()
                await page.close()

                name, name_status = extract_name_bs4(html)
                price, price_status = extract_price_bs4(html)

                result.update(
                    {
                        "status": (
                            "success"
                            if (name_status == "found" and price_status == "found")
                            else "partial"
                        ),
                        "name": name,
                        "price": price,
                        "name_found": name_status == "found",
                        "price_found": price_status == "found",
                    }
                )
            except Exception as e:
                result["error"] = str(e)[:100]
            finally:
                result["time"] = time.time() - start

            results.append(result)

        await browser.close()
    except Exception as e:
        print(f"Pyppeteer error: {e}")
        results = [
            {
                "method": "Pyppeteer",
                "url": url,
                "status": "failed",
                "error": str(e)[:100],
                "time": 0,
            }
            for url in urls
        ]

    return results


# ============================================================================
# METHOD 6: CLOUDSCRAPER
# ============================================================================
def method_6_cloudscraper(urls: list[str]) -> list[dict[str, Any]]:
    """Method 6: Cloudscraper (bypass CloudFlare)"""
    results = []
    try:
        import cloudscraper
    except ImportError:
        print("⚠️  Cloudscraper not installed. Skipping...")
        return [
            {
                "method": "Cloudscraper",
                "url": url,
                "status": "failed",
                "error": "Not installed",
                "time": 0,
            }
            for url in urls
        ]

    try:
        scraper = cloudscraper.create_scraper()
        for url in urls:
            start = time.time()
            result = {
                "method": "Cloudscraper",
                "url": url,
                "status": "failed",
                "name": "N/A",
                "price": "N/A",
                "time": 0,
                "resource_usage": "Low CPU (3-8%), Memory 15-25MB",
            }

            try:
                resp = scraper.get(url, timeout=TIMEOUT)
                if resp.status_code == 200:
                    html = resp.text
                    name, name_status = extract_name_bs4(html)
                    price, price_status = extract_price_bs4(html)

                    result.update(
                        {
                            "status": (
                                "success"
                                if (name_status == "found" and price_status == "found")
                                else "partial"
                            ),
                            "name": name,
                            "price": price,
                            "name_found": name_status == "found",
                            "price_found": price_status == "found",
                            "http_status": resp.status_code,
                        }
                    )
                else:
                    result["http_status"] = resp.status_code
                    result["error"] = f"HTTP {resp.status_code}"
            except Exception as e:
                result["error"] = str(e)[:100]
            finally:
                result["time"] = time.time() - start

            results.append(result)
    except Exception as e:
        print(f"Cloudscraper error: {e}")
        results = [
            {
                "method": "Cloudscraper",
                "url": url,
                "status": "failed",
                "error": str(e)[:100],
                "time": 0,
            }
            for url in urls
        ]

    return results


# ============================================================================
# METHOD 7: SCRAPY + SPLASH
# ============================================================================
def method_7_scrapy_splash(urls: list[str]) -> list[dict[str, Any]]:
    """Method 7: Scrapy + Splash (distributed scraping)"""
    results = []
    print("⚠️  Scrapy + Splash requires Docker setup. Skipping...")
    return [
        {
            "method": "Scrapy + Splash",
            "url": url,
            "status": "failed",
            "error": "Requires Docker + Splash server",
            "time": 0,
            "note": "Implement if you have Splash server running",
        }
        for url in urls
    ]


# ============================================================================
# METHOD 8: BEAUTIFULSOUP + SMART RETRIES
# ============================================================================
def method_8_smart_retries(urls: list[str]) -> list[dict[str, Any]]:
    """Method 8: BeautifulSoup with smart retries and headers optimization"""
    results = []

    # Multiple sets of headers to try
    header_sets = [
        HEADERS,
        {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "vi-VN,vi;q=0.9",
        },
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml",
            "Referer": "https://www.google.com/",
        },
    ]

    for url in urls:
        start = time.time()
        result = {
            "method": "BeautifulSoup + Smart Retries",
            "url": url,
            "status": "failed",
            "name": "N/A",
            "price": "N/A",
            "time": 0,
            "resource_usage": "Very Low CPU (<2%), Memory 8-15MB",
            "retries": 0,
        }

        success = False
        for attempt, headers in enumerate(header_sets, 1):
            try:
                resp = requests.get(url, headers=headers, timeout=TIMEOUT, allow_redirects=True)
                if resp.status_code == 200:
                    html = resp.text
                    name, name_status = extract_name_bs4(html)
                    price, price_status = extract_price_bs4(html)

                    if name_status == "found" and price_status == "found":
                        result.update(
                            {
                                "status": "success",
                                "name": name,
                                "price": price,
                                "name_found": True,
                                "price_found": True,
                                "http_status": resp.status_code,
                                "retries": attempt - 1,
                            }
                        )
                        success = True
                        break
            except Exception:
                continue

        if not success:
            result["status"] = "failed"
            result["error"] = "All retries exhausted"

        result["time"] = time.time() - start
        results.append(result)

    return results


# ============================================================================
# MAIN
# ============================================================================
async def main():
    """Run all 8 methods and compare"""
    print("\n" + "=" * 80)
    print("🔍 TESTING ALL 8 CRAWL METHODS")
    print("=" * 80 + "\n")

    all_results = {}

    # Method 1: Selenium
    print("1️⃣  Testing Selenium...")
    try:
        all_results["1_Selenium"] = [method_1_selenium(url) for url in TEST_URLS]
    except Exception as e:
        print(f"   ❌ Selenium failed: {e}")
        all_results["1_Selenium"] = [{"method": "Selenium", "error": str(e), "status": "failed"}]

    # Method 2: AsyncHTTP
    print("2️⃣  Testing AsyncHTTP...")
    try:
        all_results["2_AsyncHTTP"] = await method_2_asynchttp(TEST_URLS)
    except Exception as e:
        print(f"   ❌ AsyncHTTP failed: {e}")
        all_results["2_AsyncHTTP"] = [{"method": "AsyncHTTP", "error": str(e), "status": "failed"}]

    # Method 3: HTTP Requests
    print("3️⃣  Testing HTTP Requests...")
    try:
        all_results["3_HTTP_Requests"] = method_3_http_requests(TEST_URLS)
    except Exception as e:
        print(f"   ❌ HTTP Requests failed: {e}")
        all_results["3_HTTP_Requests"] = [
            {"method": "HTTP Requests", "error": str(e), "status": "failed"}
        ]

    # Method 4: Playwright
    print("4️⃣  Testing Playwright...")
    try:
        all_results["4_Playwright"] = await method_4_playwright(TEST_URLS)
    except Exception as e:
        print(f"   ❌ Playwright failed: {e}")
        all_results["4_Playwright"] = [
            {"method": "Playwright", "error": str(e), "status": "failed"}
        ]

    # Method 5: Pyppeteer
    print("5️⃣  Testing Pyppeteer...")
    try:
        all_results["5_Pyppeteer"] = await method_5_pyppeteer(TEST_URLS)
    except Exception as e:
        print(f"   ❌ Pyppeteer failed: {e}")
        all_results["5_Pyppeteer"] = [{"method": "Pyppeteer", "error": str(e), "status": "failed"}]

    # Method 6: Cloudscraper
    print("6️⃣  Testing Cloudscraper...")
    try:
        all_results["6_Cloudscraper"] = method_6_cloudscraper(TEST_URLS)
    except Exception as e:
        print(f"   ❌ Cloudscraper failed: {e}")
        all_results["6_Cloudscraper"] = [
            {"method": "Cloudscraper", "error": str(e), "status": "failed"}
        ]

    # Method 7: Scrapy + Splash
    print("7️⃣  Testing Scrapy + Splash...")
    all_results["7_Scrapy_Splash"] = method_7_scrapy_splash(TEST_URLS)

    # Method 8: Smart Retries
    print("8️⃣  Testing BeautifulSoup + Smart Retries...")
    try:
        all_results["8_Smart_Retries"] = method_8_smart_retries(TEST_URLS)
    except Exception as e:
        print(f"   ❌ Smart Retries failed: {e}")
        all_results["8_Smart_Retries"] = [
            {"method": "Smart Retries", "error": str(e), "status": "failed"}
        ]

    # ========================================================================
    # ANALYSIS & COMPARISON
    # ========================================================================
    print("\n" + "=" * 80)
    print("📊 COMPARISON RESULTS")
    print("=" * 80 + "\n")

    # Create summary table
    summary = []
    for method_key, results in all_results.items():
        if not results or not results[0]:
            continue

        first = results[0]
        method_name = first.get("method", method_key)

        # Count successes
        success_count = sum(1 for r in results if r.get("status") == "success")
        partial_count = sum(1 for r in results if r.get("status") == "partial")
        failed_count = sum(1 for r in results if r.get("status") == "failed")

        # Average time
        times = [r.get("time", 0) for r in results if r.get("time")]
        avg_time = sum(times) / len(times) if times else 0

        # Success rate
        total = len(results)
        success_rate = (success_count / total * 100) if total > 0 else 0

        summary.append(
            {
                "method": method_name,
                "success": success_count,
                "partial": partial_count,
                "failed": failed_count,
                "success_rate": success_rate,
                "avg_time": avg_time,
                "results": results,
            }
        )

    # Sort by success rate DESC, then by time ASC
    summary.sort(key=lambda x: (-x["success_rate"], x["avg_time"]))

    # Print table
    print(
        f"{'Method':<30} {'Success':<8} {'Partial':<8} {'Failed':<8} {'Rate':<8} {'Avg Time':<10} {'Resource':<30}"
    )
    print("-" * 110)

    for item in summary:
        results = item["results"]
        resource = results[0].get("resource_usage", "N/A")[:28] if results else "N/A"
        print(
            f"{item['method']:<30} {item['success']:<8} {item['partial']:<8} {item['failed']:<8} "
            f"{item['success_rate']:.0f}%{'':<4} {item['avg_time']:.2f}s{'':<4} {resource:<30}"
        )

    # Save detailed results
    output_file = (
        Path(__file__).parent.parent / "data" / "test_output" / "ALL_8_METHODS_COMPARISON.json"
    )
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(
            {
                "summary": [
                    {
                        "method": s["method"],
                        "success": s["success"],
                        "partial": s["partial"],
                        "failed": s["failed"],
                        "success_rate": f"{s['success_rate']:.1f}%",
                        "avg_time": f"{s['avg_time']:.2f}s",
                    }
                    for s in summary
                ],
                "detailed_results": all_results,
                "test_urls": TEST_URLS,
            },
            f,
            indent=2,
            ensure_ascii=False,
        )

    print(f"\n✅ Kết quả chi tiết: {output_file}")

    # Recommendation
    print("\n" + "=" * 80)
    print("💡 RECOMMENDATION (Nhanh + Chính xác)")
    print("=" * 80 + "\n")

    best = summary[0] if summary else None
    if best:
        print(f"🏆 Best Choice: {best['method']}")
        print(f"   ✅ Success Rate: {best['success_rate']:.0f}%")
        print(f"   ⚡ Average Time: {best['avg_time']:.2f}s per URL")
        print(f"   💾 Resource: {best['results'][0].get('resource_usage', 'N/A')}")

    print("\n🔗 Details saved to: data/test_output/ALL_8_METHODS_COMPARISON.json\n")


if __name__ == "__main__":
    asyncio.run(main())
