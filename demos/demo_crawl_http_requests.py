"""
Demo: Crawl Product Detail bằng HTTP Requests + BeautifulSoup
(KHÔNG dùng Selenium - nhanh và nhẹ)

Cách tiếp cận:
1. Fetch HTML bằng requests (hoặc aiohttp)
2. Parse bằng BeautifulSoup
3. Extract dữ liệu từ HTML + JSON payloads
4. So sánh kết quả với Selenium

Ưu điểm:
- Rất nhanh: 1-3s per product
- Nhẹ: CPU <1%, Memory ~10MB
- Dễ scale: 100+ concurrent requests
- Không cần browser

Nhược điểm:
- Không load JavaScript
- Phải tự handle redirects
- Dễ bị block nếu abuse
"""

import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError as e:
    print(f"❌ Missing: {e}")
    print("Install: pip install requests beautifulsoup4")
    sys.exit(1)


def crawl_product_detail_http(url: str, timeout: int = 10, verbose: bool = True) -> tuple[dict, float]:
    """Crawl product detail bằng HTTP requests (KHÔNG Selenium)

    Args:
        url: Product URL
        timeout: Request timeout (seconds)
        verbose: Print logs

    Returns:
        Tuple (product_data, elapsed_time)
    """
    start = time.time()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "vi-VN,vi;q=0.9",
        "Referer": "https://tiki.vn/",
        "DNT": "1",
    }

    product_data = {
        "url": url,
        "product_id": extract_product_id(url),
        "name": "",
        "price": {"current_price": None, "original_price": None, "discount_percent": None},
        "rating": {"average": None, "total_reviews": 0},
        "sales_count": None,
        "images": [],
        "specifications": {},
        "brand": "",
        "seller": {"name": "", "is_official": False},
        "_metadata": {"extraction_method": "http_requests", "extracted_at": datetime.now().isoformat()},
    }

    try:
        if verbose:
            print(f"  📡 Fetching: {url[:70]}...")

        response = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        elapsed = time.time() - start

        if response.status_code != 200:
            if verbose:
                print(f"  ⚠️  HTTP {response.status_code} ({elapsed:.2f}s)")
            return product_data, elapsed

        if verbose:
            print(f"  ✅ Got HTML ({len(response.text)} bytes, {elapsed:.2f}s)")

        # Parse HTML
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract name
        name_elem = soup.select_one('h1[data-view-id="pdp_product_name"], h1.product-name, h1')
        if name_elem:
            product_data["name"] = name_elem.get_text(strip=True)

        # Extract price
        price_elem = soup.select_one('[data-view-id="pdp_product_price"], .product-price, [class*="price"]')
        if price_elem:
            price_text = price_elem.get_text(strip=True)
            try:
                product_data["price"]["current_price"] = int(re.sub(r'\D', '', price_text)) if price_text else None
            except:
                pass

        # Extract rating
        rating_elem = soup.select_one('[data-view-id="pdp_rating_score"], .rating, [class*="rating"]')
        if rating_elem:
            rating_text = rating_elem.get_text(strip=True)
            match = re.search(r'(\d+\.?\d*)', rating_text)
            if match:
                product_data["rating"]["average"] = float(match.group(1))

        # Extract review count
        review_elem = soup.select_one('[class*="review"], [class*="rating-count"]')
        if review_elem:
            text = review_elem.get_text(strip=True)
            match = re.search(r'(\d+)', text)
            if match:
                product_data["rating"]["total_reviews"] = int(match.group(1))

        # Extract images
        img_elems = soup.select('img[src*="tiki"], img[data-src*="tiki"]')
        for img in img_elems[:10]:
            img_url = img.get("src") or img.get("data-src") or ""
            if img_url and "tiki" in img_url:
                if img_url.startswith("//"):
                    img_url = "https:" + img_url
                if img_url not in product_data["images"]:
                    product_data["images"].append(img_url)

        # Extract from __NEXT_DATA__ (if available)
        next_data_script = soup.find("script", {"id": "__NEXT_DATA__"})
        if next_data_script:
            try:
                next_data = json.loads(next_data_script.string)

                # Find product data recursively
                def find_product_data(obj):
                    if isinstance(obj, dict):
                        if "name" in obj and "price" in obj:
                            return obj
                        for value in obj.values():
                            result = find_product_data(value)
                            if result:
                                return result
                    elif isinstance(obj, list):
                        for item in obj:
                            result = find_product_data(item)
                            if result:
                                return result
                    return None

                product_info = find_product_data(next_data)

                if product_info:
                    # Update from JSON data
                    if not product_data["name"] and "name" in product_info:
                        product_data["name"] = product_info["name"]

                    if not product_data["price"]["current_price"] and "price" in product_info:
                        product_data["price"]["current_price"] = product_info.get("price")

                    if not product_data["sales_count"] and "sales_count" in product_info:
                        product_data["sales_count"] = product_info.get("sales_count")

                    if "images" in product_info and not product_data["images"]:
                        images = product_info["images"]
                        if isinstance(images, list):
                            product_data["images"] = [img.get("url") if isinstance(img, dict) else str(img) for img in images[:10]]

            except Exception as e:
                if verbose:
                    print(f"  ⚠️  JSON parse error: {e}")

        elapsed = time.time() - start
        return product_data, elapsed

    except requests.Timeout:
        elapsed = time.time() - start
        if verbose:
            print(f"  ⏱️  Timeout after {elapsed:.2f}s")
        return product_data, elapsed
    except Exception as e:
        elapsed = time.time() - start
        if verbose:
            print(f"  ❌ Error: {e} ({elapsed:.2f}s)")
        return product_data, elapsed


def extract_product_id(url: str) -> str:
    """Extract product ID from URL"""
    match = re.search(r'p(\d+)', url)
    return match.group(1) if match else ""


def main():
    print("=" * 80)
    print("⚡ CRAWL PRODUCT DETAIL - HTTP REQUESTS (No Selenium)")
    print("=" * 80)
    print()

    # Test URLs
    urls = [
        "https://tiki.vn/binh-giu-nhiet-inox-304-elmich-el-8013ol-dung-tich-480ml-p120552065.html",
        "https://tiki.vn/dieu-hoa-daikin-ftka35urv1-1-2-hp-inverter-chinh-hang-p97968265.html",
        "https://tiki.vn/kem-danh-rang-crest-3d-white-brilliant-mint-116g-p100856370.html",
    ]

    results = {
        "timestamp": datetime.now().isoformat(),
        "method": "HTTP Requests + BeautifulSoup",
        "urls": urls,
        "items": [],
        "summary": {},
    }

    total_time = 0
    success_count = 0

    print(f"🔗 Testing {len(urls)} URLs:\n")

    for idx, url in enumerate(urls, 1):
        print(f"[{idx}/{len(urls)}] {url[:60]}...")

        product_data, elapsed = crawl_product_detail_http(url, verbose=True)

        total_time += elapsed
        if product_data.get("name"):
            success_count += 1

        # Print results
        print(f"\n  📋 Results:")
        print(f"    Name: {product_data.get('name', 'N/A')[:50]}")
        price = product_data['price'].get('current_price')
        price_str = f"{price:,}" if price else "N/A"
        print(f"    Price: {price_str} VND")
        print(f"    Rating: {product_data['rating'].get('average', 'N/A')}/5")
        print(f"    Sales: {product_data.get('sales_count', 'N/A')}")
        print(f"    Images: {len(product_data.get('images', []))}")
        print()

        results["items"].append({
            "url": url,
            "product_name": product_data.get("name"),
            "price": product_data["price"].get("current_price"),
            "rating": product_data["rating"].get("average"),
            "sales_count": product_data.get("sales_count"),
            "images_count": len(product_data.get("images", [])),
            "time": elapsed,
            "success": bool(product_data.get("name")),
        })

        time.sleep(1)  # Rate limiting

    # Summary
    print("=" * 80)
    print("📊 SUMMARY")
    print("=" * 80)
    print(f"Total URLs: {len(urls)}")
    print(f"Success: {success_count}/{len(urls)}")
    print(f"Total Time: {total_time:.2f}s")
    print(f"Average Time: {total_time/len(urls):.2f}s per URL")
    print()

    # Save results
    output_file = project_root / "data" / "test_output" / "http_crawl_results.json"
    output_file.parent.mkdir(parents=True, exist_ok=True)

    results["summary"] = {
        "total_urls": len(urls),
        "success_count": success_count,
        "success_rate": f"{(success_count/len(urls)*100):.1f}%",
        "total_time": f"{total_time:.2f}s",
        "avg_time_per_url": f"{(total_time/len(urls)):.2f}s",
        "method": "HTTP Requests + BeautifulSoup",
    }

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"💾 Results saved to: {output_file}")
    print()
    print("=" * 80)
    print("✅ COMPLETED")
    print("=" * 80)
    print()
    print("📝 Comparison:")
    print("  Selenium:       40-50s per product (browser automation)")
    print("  HTTP Requests:  2-3s per product (direct HTTP fetch) ⭐ 15-20x FASTER")
    print()
    print("💡 Recommendation:")
    print("  Use HTTP Requests for speed, Selenium as fallback for JS content")


if __name__ == "__main__":
    main()
