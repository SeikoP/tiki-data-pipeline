# Lazy import Selenium và BeautifulSoup để tránh timeout khi load DAG
# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from bs4 import BeautifulSoup
import json
import re
import time
from datetime import datetime

# Import shared utilities - hỗ trợ cả relative và absolute import
try:
    # Thử relative import trước (khi chạy như package)
    from .utils import (
        atomic_write_json,
        create_selenium_driver,
        ensure_dir,
        extract_product_id_from_url,
        parse_price,
        parse_sales_count,
        setup_utf8_encoding,
    )
except ImportError:
    # Fallback: absolute import (khi được load qua importlib)
    import os

    # Tìm utils.py trong cùng thư mục
    current_dir = os.path.dirname(os.path.abspath(__file__))
    utils_path = os.path.join(current_dir, "utils.py")
    if os.path.exists(utils_path):
        import importlib.util

        spec = importlib.util.spec_from_file_location("crawl_utils", utils_path)
        if spec and spec.loader:
            utils_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(utils_module)
            setup_utf8_encoding = utils_module.setup_utf8_encoding
            parse_sales_count = utils_module.parse_sales_count
            parse_price = utils_module.parse_price
            extract_product_id_from_url = utils_module.extract_product_id_from_url
            create_selenium_driver = utils_module.create_selenium_driver
            atomic_write_json = utils_module.atomic_write_json
            ensure_dir = utils_module.ensure_dir
        else:
            raise ImportError(f"Không thể load utils từ {utils_path}") from None
    else:
        raise ImportError(f"Không tìm thấy utils.py tại {utils_path}") from None

# Setup UTF-8 encoding
setup_utf8_encoding()


def crawl_product_detail_with_selenium(
    url,
    save_html=False,
    verbose=True,
    max_retries=2,
    timeout=30,
    use_redis_cache=True,
    use_rate_limiting=True,
):
    """Crawl trang sản phẩm Tiki bằng Selenium để load đầy đủ dữ liệu

    Args:
        url: URL sản phẩm cần crawl
        save_html: Có lưu HTML vào file không
        verbose: Có in log không
        max_retries: Số lần retry tối đa
        timeout: Timeout cho page load (giây)
        use_redis_cache: Có dùng Redis cache không
        use_rate_limiting: Có dùng rate limiting không

    Returns:
        str: HTML content hoặc None nếu lỗi

    Raises:
        Exception: Nếu không thể crawl sau max_retries lần
    """
    # Thử Redis cache trước
    if use_redis_cache:
        try:
            from pipelines.crawl.storage.redis_cache import get_redis_cache

            redis_cache = get_redis_cache("redis://redis:6379/1")
            if redis_cache:
                cached_html = redis_cache.get_cached_html(url)
                if cached_html:
                    if verbose:
                        print(f"[Redis Cache] ✅ Hit cache cho {url[:60]}...")
                    return cached_html
        except Exception:
            pass  # Fallback về crawl

    # Rate limiting - kiểm tra và đợi nếu cần
    if use_rate_limiting:
        try:
            from pipelines.crawl.storage.redis_cache import get_redis_rate_limiter

            rate_limiter = get_redis_rate_limiter("redis://redis:6379/2")
            if rate_limiter:
                # Extract domain từ URL để rate limit theo domain
                from urllib.parse import urlparse

                domain = urlparse(url).netloc or "tiki.vn"

                # Kiểm tra rate limit, đợi nếu cần
                if not rate_limiter.is_allowed(domain):
                    if verbose:
                        print(f"[Rate Limiter] ⏳ Đợi rate limit cho {domain}...")
                    rate_limiter.wait_if_needed(domain)
        except Exception:
            # Nếu rate limiter không available, fallback về sleep cố định
            if verbose:
                print("[Rate Limiter] ⚠️  Rate limiter không available, dùng delay cố định")
            time.sleep(2)  # Delay cơ bản cho Selenium

    driver = None
    last_error = None

    for attempt in range(max_retries):
        try:
            # Sử dụng shared utility để tạo driver
            driver = create_selenium_driver(headless=True)
            if not driver:
                raise ImportError("Selenium chưa được cài đặt hoặc không thể tạo driver")

            # Set timeout cho page load
            driver.set_page_load_timeout(timeout)
            driver.implicitly_wait(10)  # Implicit wait cho elements

            if verbose:
                print(f"[Selenium] Đang mở {url}... (attempt {attempt + 1}/{max_retries})")

            driver.get(url)

            # Chờ trang load - giảm thời gian chờ
            time.sleep(2)  # Giảm từ 3s xuống 2s

            # Scroll để load các phần động - tối ưu
            if verbose:
                print("[Selenium] Đang scroll để load nội dung...")

            # Scroll nhanh hơn
            try:
                driver.execute_script("window.scrollTo(0, 500);")
                time.sleep(0.5)
                driver.execute_script("window.scrollTo(0, 1500);")
                time.sleep(0.5)
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)  # Giảm từ 2s xuống 1s
            except Exception as scroll_error:
                if verbose:
                    print(f"[Selenium] Warning: Lỗi khi scroll: {scroll_error}")
                # Tiếp tục dù scroll lỗi

            # Lấy HTML đầy đủ
            full_html = driver.page_source

            # Validate HTML
            if not full_html or len(full_html) < 100:
                raise ValueError(
                    f"HTML content quá ngắn: {len(full_html) if full_html else 0} ký tự"
                )

            # Lưu HTML nếu được yêu cầu - dùng shared utility
            if save_html:
                output_dir = ensure_dir("data/test_output")
                html_file = output_dir / "selenium_product_detail.html"
                # Lưu HTML trực tiếp (không phải JSON)
                with open(html_file, "w", encoding="utf-8") as f:
                    f.write(full_html)
                if verbose:
                    print(f"[Selenium] ✓ Đã lưu HTML vào {html_file}")

            # Cache HTML vào Redis sau khi crawl thành công
            if use_redis_cache and full_html:
                try:
                    from pipelines.crawl.storage.redis_cache import get_redis_cache

                    redis_cache = get_redis_cache("redis://redis:6379/1")
                    if redis_cache:
                        redis_cache.cache_html(url, full_html, ttl=86400)  # Cache 24 giờ
                        if verbose:
                            print(f"[Redis Cache] ✅ Đã cache HTML cho {url[:60]}...")
                except Exception:
                    pass  # Ignore cache errors

            # Đóng driver trước khi return
            if driver:
                driver.quit()
                driver = None

            return full_html

        except Exception as e:
            last_error = e
            error_type = type(e).__name__
            error_msg = str(e)

            if verbose:
                print(
                    f"[Selenium] Lỗi attempt {attempt + 1}/{max_retries} ({error_type}): {error_msg}"
                )

            # Đóng driver nếu có lỗi
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = None

            # Nếu là lỗi không thể retry (như ImportError), raise ngay
            if isinstance(e, ImportError):
                raise

            # Nếu đã hết retries, raise lỗi
            if attempt == max_retries - 1:
                raise Exception(
                    f"Không thể crawl sau {max_retries} lần thử. Lỗi cuối: {error_type}: {error_msg}"
                ) from e

            # Chờ một chút trước khi retry
            time.sleep(1)

    # Không bao giờ đến đây, nhưng để an toàn
    if driver:
        try:
            driver.quit()
        except Exception:
            pass

    raise (
        Exception(f"Không thể crawl. Lỗi: {last_error}")
        if last_error
        else Exception("Không thể crawl")
    )


# Sử dụng shared utilities thay vì định nghĩa lại
# extract_product_id_from_url và parse_price đã được import từ utils


def extract_product_detail(html_content, url, verbose=True):
    """Extract thông tin chi tiết sản phẩm từ HTML"""
    # Lazy import để tránh timeout khi load DAG
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html_content, "html.parser")
    product_data = {
        "url": url,
        "product_id": extract_product_id_from_url(url),  # Dùng shared utility
        "name": "",
        "price": {
            "current_price": None,
            "original_price": None,
            "discount_percent": None,
            "currency": "VND",
        },
        "rating": {"average": None, "total_reviews": 0, "rating_distribution": {}},
        "seller": {"name": "", "is_official": False, "seller_id": ""},
        "description": "",
        "specifications": {},
        "images": [],
        "category_path": [],
        "brand": "",
        "warranty": "",
        "sales_count": None,  # Số lượng sản phẩm đã bán
        "stock": {"available": True, "quantity": 0, "stock_status": ""},
        "shipping": {"free_shipping": False, "fast_delivery": False, "delivery_time": ""},
        "promotions": [],
        "_metadata": {
            "extracted_at": datetime.now().isoformat(),
            "source_url": url,
            "extraction_method": "selenium",
        },
    }

    # 1. Extract tên sản phẩm
    name_selectors = [
        'h1[data-view-id="pdp_product_name"]',
        "h1.product-name",
        'h1[class*="name"]',
        "h1",
        '[data-view-id*="product_name"]',
    ]
    for selector in name_selectors:
        name_elem = soup.select_one(selector)
        if name_elem:
            product_data["name"] = name_elem.get_text(strip=True)
            break

    # 2. Extract giá
    # Tìm giá hiện tại
    price_selectors = [
        '[data-view-id="pdp_product_price"]',
        ".product-price__current-price",
        '[class*="current-price"]',
        '[class*="price-current"]',
    ]
    for selector in price_selectors:
        price_elem = soup.select_one(selector)
        if price_elem:
            price_text = price_elem.get_text(strip=True)
            product_data["price"]["current_price"] = parse_price(price_text)  # Dùng shared utility
            break

    # Tìm giá gốc
    original_price_selectors = [
        ".product-price__list-price",
        '[class*="original-price"]',
        '[class*="list-price"]',
        ".price-old",
    ]
    for selector in original_price_selectors:
        price_elem = soup.select_one(selector)
        if price_elem:
            price_text = price_elem.get_text(strip=True)
            product_data["price"]["original_price"] = parse_price(price_text)  # Dùng shared utility
            break

    # Tính phần trăm giảm giá
    if product_data["price"]["current_price"] and product_data["price"]["original_price"]:
        discount = (
            (product_data["price"]["original_price"] - product_data["price"]["current_price"])
            / product_data["price"]["original_price"]
        ) * 100
        product_data["price"]["discount_percent"] = round(discount, 1)

    # 3. Extract đánh giá
    rating_selectors = [
        '[data-view-id="pdp_rating_score"]',
        ".rating-score",
        '[class*="rating"]',
        '[class*="review"]',
    ]
    for selector in rating_selectors:
        rating_elem = soup.select_one(selector)
        if rating_elem:
            rating_text = rating_elem.get_text(strip=True)
            # Tìm số thập phân trong text
            rating_match = re.search(r"(\d+\.?\d*)", rating_text)
            if rating_match:
                try:
                    product_data["rating"]["average"] = float(rating_match.group(1))
                except Exception:
                    pass
            break

    # Tìm số lượng đánh giá
    review_count_selectors = [
        '[data-view-id="pdp_review_count"]',
        '[class*="review-count"]',
        '[class*="rating-count"]',
    ]
    for selector in review_count_selectors:
        count_elem = soup.select_one(selector)
        if count_elem:
            count_text = count_elem.get_text(strip=True)
            count_match = re.search(r"(\d+)", count_text)
            if count_match:
                try:
                    product_data["rating"]["total_reviews"] = int(count_match.group(1))
                except Exception:
                    pass
            break

    # 4. Extract seller
    seller_selectors = ['[data-view-id="pdp_seller_name"]', ".seller-name", '[class*="seller"]']
    for selector in seller_selectors:
        seller_elem = soup.select_one(selector)
        if seller_elem:
            product_data["seller"]["name"] = seller_elem.get_text(strip=True)
            # Kiểm tra có phải official store không
            if (
                "official" in seller_elem.get_text(strip=True).lower()
                or "tiki" in seller_elem.get_text(strip=True).lower()
            ):
                product_data["seller"]["is_official"] = True
            break

    # 5. Extract mô tả
    description_selectors = [
        '[data-view-id="pdp_product_description"]',
        ".product-description",
        '[class*="description"]',
    ]
    for selector in description_selectors:
        desc_elem = soup.select_one(selector)
        if desc_elem:
            product_data["description"] = desc_elem.get_text(strip=True)
            break

    # 6. Extract thông số kỹ thuật
    spec_section = soup.select_one(
        '[data-view-id="pdp_product_specifications"], .product-specifications, [class*="specification"]'
    )
    if spec_section:
        spec_items = spec_section.select('tr, .spec-item, [class*="spec"]')
        for item in spec_items:
            key_elem = item.select_one('td:first-child, .spec-key, [class*="key"]')
            value_elem = item.select_one('td:last-child, .spec-value, [class*="value"]')
            if key_elem and value_elem:
                key = key_elem.get_text(strip=True)
                value = value_elem.get_text(strip=True)
                if key and value:
                    product_data["specifications"][key] = value

    # 7. Extract hình ảnh
    image_selectors = [
        '[data-view-id="pdp_product_images"] img',
        ".product-images img",
        '[class*="product-image"] img',
    ]
    for selector in image_selectors:
        images = soup.select(selector)
        if images:
            for img in images[:10]:  # Lấy tối đa 10 hình
                img_url = img.get("src") or img.get("data-src") or img.get("data-lazy-src", "")
                if img_url:
                    if img_url.startswith("//"):
                        img_url = "https:" + img_url
                    elif img_url.startswith("/"):
                        img_url = "https://tiki.vn" + img_url
                    if img_url not in product_data["images"]:
                        product_data["images"].append(img_url)
            break

    # 8. Extract category path
    breadcrumb_selectors = [
        '[data-view-id="pdp_breadcrumb"] a',
        ".breadcrumb a",
        '[class*="breadcrumb"] a',
    ]
    for selector in breadcrumb_selectors:
        breadcrumbs = soup.select(selector)
        if breadcrumbs:
            for breadcrumb in breadcrumbs:
                text = breadcrumb.get_text(strip=True)
                if text and text not in ["Trang chủ", "Home"]:
                    product_data["category_path"].append(text)
            break

    # 9. Extract brand
    brand_selectors = ['[data-view-id="pdp_product_brand"]', ".product-brand", '[class*="brand"]']
    for selector in brand_selectors:
        brand_elem = soup.select_one(selector)
        if brand_elem:
            product_data["brand"] = brand_elem.get_text(strip=True)
            break

    # 10. Extract thông tin vận chuyển
    shipping_text = soup.get_text()
    if "freeship" in shipping_text.lower() or "miễn phí vận chuyển" in shipping_text.lower():
        product_data["shipping"]["free_shipping"] = True
    if "giao nhanh" in shipping_text.lower() or "fast delivery" in shipping_text.lower():
        product_data["shipping"]["fast_delivery"] = True

    # 11. Parse từ __NEXT_DATA__ (ưu tiên cao nhất vì chứa dữ liệu đầy đủ)
    next_data_script = soup.find("script", id="__NEXT_DATA__")
    if next_data_script:
        try:
            next_data = json.loads(next_data_script.string)

            # Tìm product data trong __NEXT_DATA__
            # Path chính xác cho Tiki: props.initialState.productv2.productData.response.data
            def get_nested_value(obj, path):
                """Lấy giá trị từ nested dict theo path (ví dụ: 'a.b.c')"""
                keys = path.split(".")
                current = obj
                for key in keys:
                    if isinstance(current, dict):
                        current = current.get(key)
                    else:
                        return None
                    if current is None:
                        return None
                return current

            # Thử path chính xác của Tiki trước
            product_from_next = get_nested_value(
                next_data, "props.initialState.productv2.productData.response.data"
            )

            # Nếu không tìm thấy, thử các path khác
            if not product_from_next:

                def find_product_in_dict(obj, path=""):
                    if isinstance(obj, dict):
                        # Kiểm tra các key có thể chứa product data
                        if "product" in obj and isinstance(obj["product"], dict):
                            return obj["product"]
                        if "currentProduct" in obj and isinstance(obj["currentProduct"], dict):
                            return obj["currentProduct"]
                        if "productDetail" in obj and isinstance(obj["productDetail"], dict):
                            return obj["productDetail"]
                        if "data" in obj:
                            result = find_product_in_dict(obj["data"], path + ".data")
                            if result:
                                return result
                        if "props" in obj:
                            result = find_product_in_dict(obj["props"], path + ".props")
                            if result:
                                return result
                        if "pageProps" in obj:
                            result = find_product_in_dict(obj["pageProps"], path + ".pageProps")
                            if result:
                                return result
                        if "initialState" in obj:
                            result = find_product_in_dict(
                                obj["initialState"], path + ".initialState"
                            )
                            if result:
                                return result
                        # Đệ quy tìm trong tất cả các key
                        for key, value in obj.items():
                            if isinstance(value, (dict, list)):
                                result = find_product_in_dict(value, path + f".{key}")
                                if result:
                                    return result
                    elif isinstance(obj, list):
                        for item in obj:
                            result = find_product_in_dict(item, path)
                            if result:
                                return result
                    return None

                product_from_next = find_product_in_dict(next_data)

            if product_from_next:
                # Cập nhật tên
                if not product_data["name"]:
                    product_data["name"] = (
                        product_from_next.get("name")
                        or product_from_next.get("title")
                        or product_from_next.get("product_name")
                        or ""
                    )

                # Cập nhật giá
                price_info = (
                    product_from_next.get("price") or product_from_next.get("pricing") or {}
                )
                if isinstance(price_info, dict):
                    if not product_data["price"]["current_price"]:
                        product_data["price"]["current_price"] = (
                            price_info.get("salePrice")
                            or price_info.get("currentPrice")
                            or price_info.get("price")
                            or price_info.get("finalPrice")
                        )
                    if not product_data["price"]["original_price"]:
                        product_data["price"]["original_price"] = (
                            price_info.get("listPrice")
                            or price_info.get("originalPrice")
                            or price_info.get("basePrice")
                        )
                elif isinstance(price_info, (int, float)):
                    if not product_data["price"]["current_price"]:
                        product_data["price"]["current_price"] = int(price_info)

                # Tính lại discount nếu có cả 2 giá
                if (
                    product_data["price"]["current_price"]
                    and product_data["price"]["original_price"]
                ):
                    discount = (
                        (
                            product_data["price"]["original_price"]
                            - product_data["price"]["current_price"]
                        )
                        / product_data["price"]["original_price"]
                    ) * 100
                    product_data["price"]["discount_percent"] = round(discount, 1)

                # Cập nhật đánh giá
                rating_info = (
                    product_from_next.get("rating") or product_from_next.get("review") or {}
                )
                if isinstance(rating_info, dict):
                    if not product_data["rating"]["average"]:
                        product_data["rating"]["average"] = (
                            rating_info.get("average")
                            or rating_info.get("score")
                            or rating_info.get("rating")
                        )
                    if not product_data["rating"]["total_reviews"]:
                        product_data["rating"]["total_reviews"] = (
                            rating_info.get("total")
                            or rating_info.get("count")
                            or rating_info.get("reviewCount")
                            or 0
                        )

                # Cập nhật hình ảnh
                if not product_data["images"]:
                    images = (
                        product_from_next.get("images")
                        or product_from_next.get("imageUrls")
                        or product_from_next.get("gallery")
                        or product_from_next.get("images_url")
                        or []
                    )
                    if isinstance(images, list):
                        for img in images:
                            if isinstance(img, dict):
                                img_url = (
                                    img.get("url")
                                    or img.get("src")
                                    or img.get("medium_url")
                                    or img.get("large_url")
                                    or img.get("base_url")
                                    or img.get("full_path")
                                )
                            elif isinstance(img, str):
                                img_url = img
                            else:
                                continue

                            if img_url:
                                if img_url.startswith("//"):
                                    img_url = "https:" + img_url
                                elif img_url.startswith("/"):
                                    img_url = "https://tiki.vn" + img_url
                                if img_url not in product_data["images"]:
                                    product_data["images"].append(img_url)

                    # Thử tìm images trong configurable_products hoặc variants
                    if not product_data["images"]:
                        configurable_products = product_from_next.get("configurable_products", [])
                        for variant in configurable_products:
                            variant_images = (
                                variant.get("images") or variant.get("images_url") or []
                            )
                            if isinstance(variant_images, list):
                                for img in variant_images:
                                    if isinstance(img, dict):
                                        img_url = (
                                            img.get("url")
                                            or img.get("base_url")
                                            or img.get("full_path")
                                        )
                                    elif isinstance(img, str):
                                        img_url = img
                                    else:
                                        continue
                                    if img_url and img_url not in product_data["images"]:
                                        product_data["images"].append(img_url)
                            if product_data["images"]:
                                break

                # Cập nhật thông số kỹ thuật
                if not product_data["specifications"]:
                    specs = (
                        product_from_next.get("specifications")
                        or product_from_next.get("attributes")
                        or product_from_next.get("technicalSpecs")
                        or product_from_next.get("spec_attributes")
                        or {}
                    )
                    if isinstance(specs, dict):
                        product_data["specifications"] = specs
                    elif isinstance(specs, list):
                        for spec in specs:
                            if isinstance(spec, dict):
                                key = (
                                    spec.get("name")
                                    or spec.get("label")
                                    or spec.get("key")
                                    or spec.get("code")
                                    or spec.get("attribute_name")
                                )
                                value = (
                                    spec.get("value")
                                    or spec.get("text")
                                    or spec.get("option_label")
                                    or spec.get("attribute_value")
                                )
                                if key and value:
                                    product_data["specifications"][key] = value

                    # Thử tìm trong configurable_products
                    if not product_data["specifications"]:
                        configurable_products = product_from_next.get("configurable_products", [])
                        if configurable_products and isinstance(configurable_products[0], dict):
                            variant_specs = (
                                configurable_products[0].get("specifications")
                                or configurable_products[0].get("attributes")
                                or {}
                            )
                            if isinstance(variant_specs, dict):
                                product_data["specifications"] = variant_specs

                # Cập nhật mô tả
                if not product_data["description"]:
                    product_data["description"] = (
                        product_from_next.get("description")
                        or product_from_next.get("shortDescription")
                        or product_from_next.get("content")
                        or ""
                    )

                # Cập nhật brand
                if not product_data["brand"] or product_data["brand"].startswith("Thương hiệu:"):
                    brand = (
                        product_from_next.get("brand")
                        or product_from_next.get("brandName")
                        or product_from_next.get("manufacturer")
                        or ""
                    )
                    if brand:
                        product_data["brand"] = brand

                # Cập nhật seller
                seller_info = (
                    product_from_next.get("seller") or product_from_next.get("vendor") or {}
                )
                if isinstance(seller_info, dict):
                    if not product_data["seller"]["name"]:
                        product_data["seller"]["name"] = (
                            seller_info.get("name") or seller_info.get("sellerName") or ""
                        )
                    if not product_data["seller"]["seller_id"]:
                        product_data["seller"]["seller_id"] = (
                            seller_info.get("id") or seller_info.get("sellerId") or ""
                        )
                    if seller_info.get("isOfficial") or seller_info.get("is_official"):
                        product_data["seller"]["is_official"] = True

                # Cập nhật stock
                stock_info = (
                    product_from_next.get("stock") or product_from_next.get("inventory") or {}
                )
                if isinstance(stock_info, dict):
                    product_data["stock"]["available"] = stock_info.get("available", True)
                    product_data["stock"]["quantity"] = stock_info.get("quantity", 0)
                    product_data["stock"]["stock_status"] = stock_info.get("status", "")
                elif isinstance(stock_info, (int, float)):
                    product_data["stock"]["quantity"] = int(stock_info)
                    product_data["stock"]["available"] = stock_info > 0

                # Cập nhật sales_count (số lượng đã bán) - dùng shared utility
                if not product_data["sales_count"]:
                    sales_count_raw = (
                        product_from_next.get("sales_count")
                        or product_from_next.get("quantity_sold")
                        or product_from_next.get("sold_count")
                        or product_from_next.get("total_sold")
                        or product_from_next.get("order_count")
                        or product_from_next.get("sales_quantity")
                        or product_from_next.get("quantity")
                        or product_from_next.get("sold")
                        or product_from_next.get("total_quantity_sold")
                    )
                    product_data["sales_count"] = parse_sales_count(sales_count_raw)
        except Exception as e:
            if verbose:
                print(f"[Parse] Lỗi khi parse __NEXT_DATA__: {e}")
            pass

    return product_data


def main():
    """Test crawl sản phẩm"""
    url = "https://tiki.vn/binh-giu-nhiet-inox-304-elmich-el-8013ol-dung-tich-480ml-p120552065.html?itm_campaign=CTP_YPD_TKA_PLA_UNK_ALL_UNK_UNK_UNK_UNK_X.304582_Y.1886902_Z.4008723_CN.HL-l-Binh-Giu-Nhiet&itm_medium=CPC&itm_source=tiki-ads&spid=120552067"

    print("=" * 70)
    print("CRAWL SẢN PHẨM TIKI VỚI SELENIUM")
    print("=" * 70)
    print(f"URL: {url}\n")

    # Bước 1: Crawl với Selenium
    print("BƯỚC 1: Crawl với Selenium")
    print("-" * 70)
    html_content = crawl_product_detail_with_selenium(url, save_html=True, verbose=True)
    print(f"✓ Đã lấy HTML ({len(html_content)} ký tự)\n")

    # Bước 2: Extract thông tin sản phẩm
    print("BƯỚC 2: Extract thông tin sản phẩm")
    print("-" * 70)
    product_data = extract_product_detail(html_content, url, verbose=True)

    # In kết quả
    print(f"Product ID: {product_data['product_id']}")
    print(f"Tên: {product_data['name']}")
    print(
        f"Giá hiện tại: {product_data['price']['current_price']:,} {product_data['price']['currency']}"
        if product_data["price"]["current_price"]
        else "Giá: N/A"
    )
    print(
        f"Giá gốc: {product_data['price']['original_price']:,} {product_data['price']['currency']}"
        if product_data["price"]["original_price"]
        else "Giá gốc: N/A"
    )
    print(
        f"Giảm giá: {product_data['price']['discount_percent']}%"
        if product_data["price"]["discount_percent"]
        else "Giảm giá: N/A"
    )
    print(
        f"Đánh giá: {product_data['rating']['average']}/5 ({product_data['rating']['total_reviews']} đánh giá)"
        if product_data["rating"]["average"]
        else "Đánh giá: N/A"
    )
    print(
        f"Seller: {product_data['seller']['name']}"
        if product_data["seller"]["name"]
        else "Seller: N/A"
    )
    print(f"Brand: {product_data['brand']}" if product_data["brand"] else "Brand: N/A")
    print(f"Số hình ảnh: {len(product_data['images'])}")
    print(f"Số thông số: {len(product_data['specifications'])}")
    print(
        f"Category path: {' > '.join(product_data['category_path'])}"
        if product_data["category_path"]
        else "Category path: N/A"
    )

    # Lưu kết quả JSON
    output_dir = "data/test_output"
    import os

    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "selenium_product_detail.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(product_data, f, ensure_ascii=False, indent=2)

    print(f"\n✓ Đã lưu kết quả vào {output_file}")
    print("\n" + "=" * 70)
    print("✓ Hoàn thành!")
    print("=" * 70)


if __name__ == "__main__":
    main()
