"""
Demo: Crawl Product Detail không dùng Selenium (aiohttp async)

Cách tiếp cận hybrid:
1. Thử crawl bằng aiohttp (async HTTP) trước - nhanh hơn
2. Nếu thiếu dữ liệu động (sales_count), fallback về Selenium

Ưu điểm:
- Tối ưu tốc độ: aiohttp cho static content, Selenium cho dynamic content
- Tiết kiệm tài nguyên: ít CPU/memory hơn Selenium-only approach
- Linh hoạt: có thể crawl nhiều sản phẩm song song

Nhược điểm:
- Phức tạp hơn Selenium-only
- Cần xử lý fallback logic
"""

import asyncio
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

# Fix encoding cho Windows console
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Thêm src vào path
project_root = Path(__file__).parent.parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

# Import
try:
    from pipelines.crawl.crawl_products_detail import (
        extract_product_detail,
        crawl_product_detail_with_selenium,
    )
except ImportError as e:
    print(f"❌ Lỗi import: {e}")
    sys.exit(1)


async def crawl_product_detail_async_http(
    url: str,
    session=None,
    timeout: int = 15,
    verbose: bool = False,
) -> tuple[dict | None, float]:
    """Crawl product detail bằng aiohttp (async HTTP) - KHÔNG dùng Selenium

    Args:
        url: URL sản phẩm cần crawl
        session: aiohttp ClientSession
        timeout: Timeout cho request (giây)
        verbose: Có in log không

    Returns:
        Tuple (product_data, elapsed_time)
    """
    try:
        import aiohttp
    except ImportError:
        if verbose:
            print("[AsyncHTTP] ❌ aiohttp chưa được cài đặt")
        return None, 0

    start_time = time.time()
    create_session = session is None

    if create_session:
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=10,
            ttl_dns_cache=300,
            ssl=False,
        )
        timeout_obj = aiohttp.ClientTimeout(total=timeout, connect=10)
        session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout_obj,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            },
        )

    try:
        if verbose:
            print(f"[AsyncHTTP] 📡 Fetching {url[:60]}...")

        async with session.get(url) as response:
            if response.status != 200:
                if verbose:
                    print(f"[AsyncHTTP] ⚠️  HTTP {response.status}")
                elapsed = time.time() - start_time
                return None, elapsed

            html_content = await response.text()

            if verbose:
                print(f"[AsyncHTTP] ✅ Got HTML ({len(html_content)} bytes)")

            # Extract product detail từ HTML
            product_data = extract_product_detail(html_content, url, verbose=False)

            elapsed = time.time() - start_time
            return product_data, elapsed

    except asyncio.TimeoutError:
        if verbose:
            print(f"[AsyncHTTP] ⏱️  Timeout sau {timeout}s")
        elapsed = time.time() - start_time
        return None, elapsed
    except Exception as e:
        if verbose:
            print(f"[AsyncHTTP] ❌ Lỗi: {e}")
        elapsed = time.time() - start_time
        return None, elapsed
    finally:
        if create_session and session:
            await session.close()


def crawl_product_detail_sync_selenium(
    url: str,
    verbose: bool = False,
) -> tuple[dict | None, float]:
    """Crawl product detail bằng Selenium

    Args:
        url: URL sản phẩm cần crawl
        verbose: Có in log không

    Returns:
        Tuple (product_data, elapsed_time)
    """
    start_time = time.time()

    try:
        if verbose:
            print(f"[Selenium] 🌐 Opening {url[:60]}...")

        html_content = crawl_product_detail_with_selenium(
            url,
            max_retries=2,
            timeout=30,
            verbose=False,
            save_html=False,
        )

        if not html_content:
            elapsed = time.time() - start_time
            return None, elapsed

        if verbose:
            print(f"[Selenium] ✅ Got HTML ({len(html_content)} bytes)")

        product_data = extract_product_detail(html_content, url, verbose=False)
        elapsed = time.time() - start_time
        return product_data, elapsed

    except Exception as e:
        if verbose:
            print(f"[Selenium] ❌ Lỗi: {e}")
        elapsed = time.time() - start_time
        return None, elapsed


def compare_data_quality(data_selenium: dict, data_async: dict) -> dict:
    """So sánh chất lượng dữ liệu giữa 2 cách crawl

    Args:
        data_selenium: Product data từ Selenium
        data_async: Product data từ aiohttp async

    Returns:
        Dict comparison result
    """
    comparison = {
        "name": {
            "selenium": data_selenium.get("name", ""),
            "async": data_async.get("name", ""),
            "match": data_selenium.get("name") == data_async.get("name"),
        },
        "price": {
            "selenium_current": data_selenium.get("price", {}).get("current_price"),
            "async_current": data_async.get("price", {}).get("current_price"),
            "match_current": (
                data_selenium.get("price", {}).get("current_price")
                == data_async.get("price", {}).get("current_price")
            ),
            "selenium_original": data_selenium.get("price", {}).get("original_price"),
            "async_original": data_async.get("price", {}).get("original_price"),
            "match_original": (
                data_selenium.get("price", {}).get("original_price")
                == data_async.get("price", {}).get("original_price")
            ),
        },
        "rating": {
            "selenium_avg": data_selenium.get("rating", {}).get("average"),
            "async_avg": data_async.get("rating", {}).get("average"),
            "match_avg": (
                data_selenium.get("rating", {}).get("average")
                == data_async.get("rating", {}).get("average")
            ),
            "selenium_count": data_selenium.get("rating", {}).get("total_reviews"),
            "async_count": data_async.get("rating", {}).get("total_reviews"),
            "match_count": (
                data_selenium.get("rating", {}).get("total_reviews")
                == data_async.get("rating", {}).get("total_reviews")
            ),
        },
        "sales_count": {
            "selenium": data_selenium.get("sales_count"),
            "async": data_async.get("sales_count"),
            "match": data_selenium.get("sales_count") == data_async.get("sales_count"),
        },
        "images": {
            "selenium_count": len(data_selenium.get("images", [])),
            "async_count": len(data_async.get("images", [])),
        },
        "specifications": {
            "selenium_count": len(data_selenium.get("specifications", {})),
            "async_count": len(data_async.get("specifications", {})),
        },
    }

    return comparison


async def run_comparison_async(urls: list, verbose: bool = True) -> dict:
    """Chạy so sánh crawl bằng cả 2 phương pháp (async)

    Args:
        urls: Danh sách URLs cần crawl
        verbose: Có in log không

    Returns:
        Dict kết quả so sánh
    """
    results = {
        "timestamp": datetime.now().isoformat(),
        "urls": urls,
        "selenium_total_time": 0,
        "async_total_time": 0,
        "items": [],
        "summary": {},
    }

    for idx, url in enumerate(urls, 1):
        print(f"\n{'=' * 80}")
        print(f"📊 ITEM {idx}/{len(urls)}: {url[:70]}")
        print("=" * 80)

        item_result = {
            "url": url,
            "selenium": {},
            "async": {},
            "comparison": {},
        }

        # Crawl bằng Selenium
        print("\n🌐 [Selenium] Crawling...")
        data_selenium, time_selenium = crawl_product_detail_sync_selenium(url, verbose=verbose)

        if data_selenium:
            print(f"✅ [Selenium] Hoàn thành trong {time_selenium:.2f}s")
            item_result["selenium"]["data"] = data_selenium
            item_result["selenium"]["time"] = time_selenium
            item_result["selenium"]["success"] = True
            results["selenium_total_time"] += time_selenium
        else:
            print(f"❌ [Selenium] Thất bại sau {time_selenium:.2f}s")
            item_result["selenium"]["time"] = time_selenium
            item_result["selenium"]["success"] = False

        # Crawl bằng aiohttp async
        print("\n📡 [AsyncHTTP] Crawling...")
        data_async, time_async = await crawl_product_detail_async_http(url, verbose=verbose)

        if data_async:
            print(f"✅ [AsyncHTTP] Hoàn thành trong {time_async:.2f}s")
            item_result["async"]["data"] = data_async
            item_result["async"]["time"] = time_async
            item_result["async"]["success"] = True
            results["async_total_time"] += time_async
        else:
            print(f"❌ [AsyncHTTP] Thất bại sau {time_async:.2f}s")
            item_result["async"]["time"] = time_async
            item_result["async"]["success"] = False

        # So sánh
        if data_selenium and data_async:
            comparison = compare_data_quality(data_selenium, data_async)
            item_result["comparison"] = comparison

            # In chi tiết so sánh
            print(f"\n📋 COMPARISON RESULTS:")
            print("-" * 80)
            print(f"📝 Product Name:")
            print(f"   Selenium: {comparison['name']['selenium'][:50]}")
            print(f"   AsyncHTTP: {comparison['name']['async'][:50]}")
            print(f"   ✓ Match: {comparison['name']['match']}")

            print(f"\n💰 Price (Current):")
            print(f"   Selenium: {comparison['price']['selenium_current']:,} VND")
            print(f"   AsyncHTTP: {comparison['price']['async_current']:,} VND")
            print(f"   ✓ Match: {comparison['price']['match_current']}")

            print(f"\n⭐ Rating:")
            print(f"   Selenium: {comparison['rating']['selenium_avg']}/5 ({comparison['rating']['selenium_count']} reviews)")
            print(f"   AsyncHTTP: {comparison['rating']['async_avg']}/5 ({comparison['rating']['async_count']} reviews)")
            print(f"   ✓ Match Avg: {comparison['rating']['match_avg']}")

            print(f"\n📊 Sales Count:")
            print(f"   Selenium: {comparison['sales_count']['selenium']}")
            print(f"   AsyncHTTP: {comparison['sales_count']['async']}")
            print(f"   ✓ Match: {comparison['sales_count']['match']}")

            print(f"\n🖼️  Images:")
            print(f"   Selenium: {comparison['images']['selenium_count']} images")
            print(f"   AsyncHTTP: {comparison['images']['async_count']} images")

            print(f"\n⚙️  Specifications:")
            print(f"   Selenium: {comparison['specifications']['selenium_count']} specs")
            print(f"   AsyncHTTP: {comparison['specifications']['async_count']} specs")

            print(f"\n⏱️  PERFORMANCE:")
            print(f"   Selenium: {time_selenium:.2f}s")
            print(f"   AsyncHTTP: {time_async:.2f}s")
            speedup = time_selenium / time_async if time_async > 0 else 0
            print(f"   Speedup: {speedup:.1f}x faster with AsyncHTTP")

        results["items"].append(item_result)

    # Tính tóm tắt
    selenium_success = sum(1 for item in results["items"] if item["selenium"].get("success"))
    async_success = sum(1 for item in results["items"] if item["async"].get("success"))

    results["summary"] = {
        "total_items": len(urls),
        "selenium_success": selenium_success,
        "selenium_success_rate": f"{(selenium_success / len(urls) * 100):.1f}%",
        "async_success": async_success,
        "async_success_rate": f"{(async_success / len(urls) * 100):.1f}%",
        "selenium_total_time": f"{results['selenium_total_time']:.2f}s",
        "async_total_time": f"{results['async_total_time']:.2f}s",
        "selenium_avg_time": f"{(results['selenium_total_time'] / len(urls)):.2f}s",
        "async_avg_time": f"{(results['async_total_time'] / len(urls)):.2f}s",
        "overall_speedup": f"{(results['selenium_total_time'] / results['async_total_time']):.1f}x"
        if results["async_total_time"] > 0
        else "N/A",
        "recommendation": (
            "✅ AsyncHTTP đủ tốt - Nên dùng AsyncHTTP để tiết kiệm tài nguyên"
            if async_success == selenium_success and results['async_total_time'] < results['selenium_total_time']
            else "⚠️  AsyncHTTP thiếu dữ liệu - Cần dùng Selenium fallback"
            if async_success < selenium_success
            else "✅ AsyncHTTP tối ưu - Dùng AsyncHTTP cho tốc độ"
        ),
    }

    return results


def main():
    print("=" * 80)
    print("🔄 DEMO: CRAWL DETAIL - ASYNC HTTP vs SELENIUM")
    print("=" * 80)
    print()
    print("So sánh 2 cách crawl sản phẩm:")
    print("  1. 🌐 Selenium - load JavaScript, dynamic content")
    print("  2. 📡 AsyncHTTP - fetch HTML directly, nhanh hơn")
    print()

    # Test URLs
    urls = [
        "https://tiki.vn/binh-giu-nhiet-inox-304-elmich-el-8013ol-dung-tich-480ml-p120552065.html",
        "https://tiki.vn/dieu-hoa-daikin-ftka35urv1-1-2-hp-inverter-chinh-hang-p97968265.html",
        "https://tiki.vn/kem-danh-rang-crest-3d-white-brilliant-mint-116g-p100856370.html",
    ]

    print(f"📋 Test URLs ({len(urls)} sản phẩm):")
    for i, url in enumerate(urls, 1):
        domain = url.split('/')[2]
        product_name = url.split('/')[3].replace('-', ' ')[:40]
        print(f"   {i}. {product_name}...")
    print()

    # Chạy so sánh
    try:
        results = asyncio.run(run_comparison_async(urls, verbose=True))

        # Lưu kết quả
        output_dir = project_root / "data" / "test_output"
        output_dir.mkdir(parents=True, exist_ok=True)

        output_file = output_dir / "demo_crawl_detail_comparison.json"
        with open(output_file, "w", encoding="utf-8") as f:
            # Simplify data for JSON serialization
            json_results = {
                "timestamp": results["timestamp"],
                "summary": results["summary"],
                "selenium_total_time": results["selenium_total_time"],
                "async_total_time": results["async_total_time"],
                "items_count": len(results["items"]),
                "items": [
                    {
                        "url": item["url"],
                        "selenium_time": item["selenium"].get("time", 0),
                        "selenium_success": item["selenium"].get("success", False),
                        "async_time": item["async"].get("time", 0),
                        "async_success": item["async"].get("success", False),
                        "comparison": item["comparison"],
                    }
                    for item in results["items"]
                ],
            }
            json.dump(json_results, f, ensure_ascii=False, indent=2)

        print("\n" + "=" * 80)
        print("📊 SUMMARY")
        print("=" * 80)
        for key, value in results["summary"].items():
            print(f"{key:.<40} {value}")
        print("=" * 80)

        print(f"\n💾 Kết quả đã lưu vào: {output_file}")

    except Exception as e:
        print(f"❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
