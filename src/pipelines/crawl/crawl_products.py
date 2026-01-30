import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from urllib.parse import parse_qs, urljoin

import requests

# Lazy import BeautifulSoup để tránh timeout khi load DAG
# from bs4 import BeautifulSoup  # Moved to function level

# Import shared utilities - hỗ trợ cả relative và absolute import
try:
    # Thử relative import trước (khi chạy như package)
    from .utils import (
        DEFAULT_CACHE_DIR,
        DEFAULT_DATA_DIR,
        DEFAULT_PRODUCT_LIST_CACHE_DIR,
        DEFAULT_PRODUCTS_DIR,
        RateLimiter,
        atomic_write_json,
        ensure_dir,
        extract_category_id_from_url,
        extract_product_id_from_url,
        normalize_url,
        parse_price,
        parse_sales_count,
        safe_read_json,
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
            ensure_dir = utils_module.ensure_dir
            atomic_write_json = utils_module.atomic_write_json
            safe_read_json = utils_module.safe_read_json
            extract_category_id_from_url = utils_module.extract_category_id_from_url
            extract_product_id_from_url = utils_module.extract_product_id_from_url
            normalize_url = utils_module.normalize_url
            RateLimiter = utils_module.RateLimiter
            DEFAULT_DATA_DIR = utils_module.DEFAULT_DATA_DIR
            DEFAULT_CACHE_DIR = utils_module.DEFAULT_CACHE_DIR
            DEFAULT_PRODUCT_LIST_CACHE_DIR = utils_module.DEFAULT_PRODUCT_LIST_CACHE_DIR
            DEFAULT_PRODUCTS_DIR = utils_module.DEFAULT_PRODUCTS_DIR
        else:
            raise ImportError(f"Không thể load utils từ {utils_path}") from None
    else:
        raise ImportError(f"Không tìm thấy utils.py tại {utils_path}") from None

# Setup UTF-8 encoding
setup_utf8_encoding()


# Lazy import tqdm để tránh timeout khi load DAG
# Sẽ import trong functions khi cần
def _get_tqdm():
    """Lazy import tqdm - chỉ import khi cần"""
    try:
        from tqdm import tqdm

        return tqdm
    except ImportError:
        # Fallback: tạo fake tqdm class nếu không có
        class FakeTqdm:
            def __init__(self, iterable=None, total=None, desc="", **kwargs):
                self.iterable = iterable
                self.total = total or (len(iterable) if iterable else 0)
                self.desc = desc
                self.n = 0
                self.start_time = time.time()

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def __iter__(self):
                if self.iterable:
                    for item in self.iterable:
                        self.n += 1
                        self.update(1)
                        yield item
                else:
                    return self

            def update(self, n=1):
                self.n += n
                if self.total > 0:
                    pct = (self.n / self.total) * 100
                    elapsed = time.time() - self.start_time
                    if self.n > 0:
                        rate = self.n / elapsed if elapsed > 0 else 0
                        eta = (self.total - self.n) / rate if rate > 0 else 0
                        print(
                            f"\r{self.desc} {self.n}/{self.total} ({pct:.1f}%) | "
                            f"Tốc độ: {rate:.2f}/s | ETA: {eta:.0f}s",
                            end="",
                            flush=True,
                        )

            def set_postfix(self, _postfix=None):
                pass

        return FakeTqdm


# Lazy import Selenium và webdriver_manager để tránh timeout khi load DAG
# Không import ở top level - sẽ import trong functions khi cần
HAS_SELENIUM = None  # Sẽ được check khi cần
HAS_WEBDRIVER_MANAGER = None  # Sẽ được check khi cần


def _check_selenium_available():
    """
    Check xem Selenium có sẵn không (lazy check)
    """
    global HAS_SELENIUM, HAS_WEBDRIVER_MANAGER
    if HAS_SELENIUM is not None:
        return HAS_SELENIUM

    try:
        from selenium import webdriver  # noqa: F401

        HAS_SELENIUM = True

        # Thử import webdriver-manager
        try:
            from webdriver_manager.chrome import ChromeDriverManager  # noqa: F401

            HAS_WEBDRIVER_MANAGER = True
        except ImportError:
            HAS_WEBDRIVER_MANAGER = False
    except ImportError:
        HAS_SELENIUM = False
        HAS_WEBDRIVER_MANAGER = False

    return HAS_SELENIUM


# Tạo thư mục output
# Tạo thư mục output
os.makedirs(DEFAULT_PRODUCTS_DIR, exist_ok=True)
os.makedirs(DEFAULT_PRODUCT_LIST_CACHE_DIR, exist_ok=True)

# Thread-safe locks và stats
stats_lock = Lock()
stats = {
    "total_categories": 0,
    "total_products": 0,
    "total_pages": 0,
    "total_success": 0,
    "total_failed": 0,
    "start_time": time.time(),
}


def get_page_with_selenium(url, timeout=30, use_redis_cache=True, use_rate_limiting=True):
    """Lấy HTML của trang với Selenium (cho dynamic content)

    Args:
        url: URL cần crawl
        timeout: Timeout cho page load
        use_redis_cache: Có dùng Redis cache không
        use_rate_limiting: Có dùng rate limiting không
    """
    # Thử Redis cache trước
    if use_redis_cache:
        try:
            from pipelines.crawl.storage.redis_cache import get_redis_cache

            redis_cache = get_redis_cache("redis://redis:6379/1")
            if redis_cache:
                cached_html = redis_cache.get_cached_html(url)
                if cached_html:
                    return cached_html
        except Exception:
            pass  # Fallback về crawl

    # Adaptive Rate Limiting
    adaptive_limiter = None
    if use_rate_limiting:
        try:
            from urllib.parse import urlparse

            from pipelines.crawl.storage.adaptive_rate_limiter import get_adaptive_rate_limiter

            adaptive_limiter = get_adaptive_rate_limiter("redis://redis:6379/2")
            if adaptive_limiter:
                domain = urlparse(url).netloc or "tiki.vn"
                adaptive_limiter.wait(domain)
        except Exception:
            time.sleep(0.7)  # Fixed delay fallback

    # Lazy import để tránh timeout khi load DAG
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service

    # Check Selenium availability
    if not _check_selenium_available():
        raise ImportError("Selenium chưa được cài đặt")

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_setting_values.notifications": 2,
    }
    chrome_options.add_experimental_option("prefs", prefs)
    # Faster page load
    try:
        chrome_options.page_load_strategy = "eager"
    except Exception:
        pass

    # Ưu tiên dùng ChromeDriver có sẵn trong PATH (nhanh nhất)
    try:
        driver = webdriver.Chrome(options=chrome_options)
    except Exception as e:
        # Nếu không có ChromeDriver trong PATH, thử webdriver-manager
        error_msg = str(e).lower()
        if "chromedriver" in error_msg or "driver" in error_msg:
            if HAS_WEBDRIVER_MANAGER:
                try:
                    # Tắt log của webdriver_manager để giảm noise
                    import logging
                    import os
                    import stat

                    from webdriver_manager.chrome import ChromeDriverManager

                    wdm_logger = logging.getLogger("WDM")
                    wdm_logger.setLevel(logging.WARNING)

                    # Install ChromeDriver
                    driver_path = ChromeDriverManager().install()

                    # QUAN TRỌNG: Set quyền thực thi cho ChromeDriver (fix lỗi status code 127)
                    # Đặc biệt cần thiết trong WSL2/Linux
                    try:
                        os.chmod(
                            driver_path,
                            os.stat(driver_path).st_mode
                            | stat.S_IEXEC
                            | stat.S_IXGRP
                            | stat.S_IXOTH,
                        )
                    except Exception:
                        pass  # Nếu không set được quyền, vẫn thử tiếp

                    service = Service(driver_path)
                    driver = webdriver.Chrome(service=service, options=chrome_options)
                except Exception:
                    # Nếu webdriver-manager cũng fail, raise lỗi gốc
                    raise e from None
            else:
                raise e from None
        else:
            # Lỗi khác, raise ngay
            raise
    try:
        driver.set_page_load_timeout(timeout)
        driver.get(url)
        time.sleep(0.5)  # Chờ JavaScript load (optimized)

        # Scroll để load lazy images (optimized)
        try:
            driver.execute_script("window.scrollTo(0, 500);")
            time.sleep(0.3)
            driver.execute_script("window.scrollTo(0, 1500);")
            time.sleep(0.5)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
        except Exception:
            pass

        html = driver.page_source

        # Cache HTML vào Redis sau khi crawl thành công (với canonical URL)
        if use_redis_cache and html:
            try:
                from pipelines.crawl.config import REDIS_CACHE_TTL_HTML
                from pipelines.crawl.storage.redis_cache import get_redis_cache

                redis_cache = get_redis_cache("redis://redis:6379/1")
                if redis_cache:
                    # CRITICAL: Chuẩn hóa URL trước khi cache để maximize hit rate
                    canonical_url = redis_cache._canonicalize_url(url)
                    redis_cache.cache_html(canonical_url, html, ttl=REDIS_CACHE_TTL_HTML)  # 7 days
            except Exception:
                pass  # Ignore cache errors

        return html
    finally:
        driver.quit()


def get_page_with_requests(url, max_retries=3, use_redis_cache=True, use_rate_limiting=True):
    """Lấy HTML của trang với requests (nhanh hơn nhưng không hỗ trợ JS)

    Args:
        url: URL cần crawl
        max_retries: Số lần retry tối đa
        use_redis_cache: Có dùng Redis cache không
        use_rate_limiting: Có dùng rate limiting không
    """
    # Thử Redis cache trước
    if use_redis_cache:
        try:
            from pipelines.crawl.storage.redis_cache import get_redis_cache

            redis_cache = get_redis_cache("redis://redis:6379/1")
            if redis_cache:
                cached_html = redis_cache.get_cached_html(url)
                if cached_html:
                    return cached_html
        except Exception:
            pass  # Fallback về crawl

    # Adaptive Rate Limiting
    adaptive_limiter = None
    if use_rate_limiting:
        try:
            from urllib.parse import urlparse

            from pipelines.crawl.storage.adaptive_rate_limiter import get_adaptive_rate_limiter

            adaptive_limiter = get_adaptive_rate_limiter("redis://redis:6379/2")
            if adaptive_limiter:
                domain = urlparse(url).netloc or "tiki.vn"
                adaptive_limiter.wait(domain)
        except Exception:
            time.sleep(0.7)  # Fixed delay fallback

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            response.encoding = "utf-8"
            html = response.text

            # Cache HTML vào Redis sau khi crawl thành công (với canonical URL)
            if use_redis_cache and html:
                try:
                    from pipelines.crawl.config import REDIS_CACHE_TTL_HTML
                    from pipelines.crawl.storage.redis_cache import get_redis_cache

                    redis_cache = get_redis_cache("redis://redis:6379/1")
                    if redis_cache:
                        # CRITICAL: Chuẩn hóa URL trước khi cache để maximize hit rate
                        canonical_url = redis_cache._canonicalize_url(url)
                        redis_cache.cache_html(
                            canonical_url, html, ttl=REDIS_CACHE_TTL_HTML
                        )  # 7 days
                except Exception:
                    pass  # Ignore cache errors

            # Record success cho adaptive rate limiter
            if adaptive_limiter:
                try:
                    from urllib.parse import urlparse

                    domain = urlparse(url).netloc or "tiki.vn"
                    adaptive_limiter.record_success(domain)
                except Exception:
                    pass

            return html
        except requests.exceptions.RequestException as e:
            # Record error cho adaptive rate limiter
            if adaptive_limiter:
                try:
                    from urllib.parse import urlparse

                    domain = urlparse(url).netloc or "tiki.vn"
                    error_type_str = None
                    if "429" in str(e) or "Too Many Requests" in str(e):
                        error_type_str = "429"
                    elif "timeout" in str(e).lower():
                        error_type_str = "timeout"
                    adaptive_limiter.record_error(domain, error_type=error_type_str)
                except Exception:
                    pass

            if attempt == max_retries - 1:
                raise
            time.sleep(2**attempt)  # Exponential backoff

    return None


def parse_products_from_next_data(html_content):
    """
    Parse sản phẩm từ __NEXT_DATA__ (Next.js)
    """
    # Lazy import để tránh timeout khi load DAG
    from bs4 import BeautifulSoup

    products = []

    try:
        # Tìm script tag chứa __NEXT_DATA__
        soup = BeautifulSoup(html_content, "html.parser")
        next_data_script = soup.find("script", id="__NEXT_DATA__")

        if not next_data_script:
            return []

        # Parse JSON
        next_data = json.loads(next_data_script.string)

        # Đi sâu vào cấu trúc Next.js để tìm product data
        # Tiki có thể lưu products ở nhiều nơi trong cấu trúc
        def find_products_in_dict(obj, path=""):
            """
            Đệ quy tìm products trong nested dict.
            """
            if isinstance(obj, dict):
                # Kiểm tra các key có thể chứa products
                if "products" in obj and isinstance(obj["products"], list):
                    return obj["products"]
                if "items" in obj and isinstance(obj["items"], list):
                    # Kiểm tra xem có phải product items không
                    if obj["items"] and isinstance(obj["items"][0], dict):
                        if any(
                            key in obj["items"][0] for key in ["id", "product_id", "name", "price"]
                        ):
                            return obj["items"]
                if "data" in obj:
                    result = find_products_in_dict(obj["data"], path + ".data")
                    if result:
                        return result
                if "props" in obj:
                    result = find_products_in_dict(obj["props"], path + ".props")
                    if result:
                        return result
                if "pageProps" in obj:
                    result = find_products_in_dict(obj["pageProps"], path + ".pageProps")
                    if result:
                        return result
                if "initialState" in obj:
                    result = find_products_in_dict(obj["initialState"], path + ".initialState")
                    if result:
                        return result

                # Đệ quy tất cả values
                for key, value in obj.items():
                    result = find_products_in_dict(value, f"{path}.{key}")
                    if result:
                        return result

            elif isinstance(obj, list):
                # Nếu là list, kiểm tra phần tử đầu tiên
                if obj and isinstance(obj[0], dict):
                    # Kiểm tra xem có phải product objects không
                    first_item = obj[0]
                    if any(key in first_item for key in ["id", "product_id", "name", "price"]):
                        return obj

                # Đệ quy các phần tử
                for i, item in enumerate(obj):
                    result = find_products_in_dict(item, f"{path}[{i}]")
                    if result:
                        return result

            return None

        product_data = find_products_in_dict(next_data)

        if product_data:
            for item in product_data:
                try:
                    # Extract thông tin sản phẩm
                    product_id = str(
                        item.get("id") or item.get("product_id") or item.get("sku") or ""
                    )
                    if not product_id:
                        continue

                    name = item.get("name") or item.get("title") or ""

                    # Lấy URL
                    url = item.get("url") or item.get("link") or ""
                    if not url or not url.startswith("http"):
                        if product_id:
                            url = f"https://tiki.vn/p/{product_id}"

                    # Lấy image (để có thể dùng preview)
                    image_url = (
                        item.get("image_url")
                        or item.get("thumbnail_url")
                        or item.get("images", [{}])[0].get("url", "")
                        if isinstance(item.get("images"), list)
                        else ""
                    )

                    # Extract số lượng bán - dùng shared utility
                    sales_count_raw = (
                        item.get("sales_count")
                        or item.get("quantity_sold")
                        or item.get("sold_count")
                        or item.get("total_sold")
                        or item.get("order_count")
                        or item.get("sales_quantity")
                        or item.get("quantity")
                        or item.get("sold")
                        or item.get("total_quantity_sold")
                    )
                    sales_count = parse_sales_count(sales_count_raw)

                    product = {
                        "product_id": product_id,
                        "name": name,
                        "url": url,
                        "image_url": image_url,
                        "sales_count": sales_count,
                    }

                    if product_id and name:
                        products.append(product)

                except Exception:
                    continue

    except Exception:
        pass

    return products


def parse_products_from_html(html_content, category_url):
    """
    Parse danh sách sản phẩm từ HTML.
    """
    # Lazy import để tránh timeout khi load DAG
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html_content, "html.parser")
    products = []

    # Cách 1: Parse từ __NEXT_DATA__ (ưu tiên)
    next_data_products = parse_products_from_next_data(html_content)
    if next_data_products:
        # Thêm category_url và đảm bảo sales_count có trong mỗi product
        for product in next_data_products:
            product["category_url"] = category_url
            product["crawled_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            # Đảm bảo sales_count luôn có (kể cả None)
            if "sales_count" not in product:
                product["sales_count"] = None
        products.extend(next_data_products)
        return products

    # Cách 2: Parse từ HTML elements (fallback)
    # Tìm tất cả link có pattern /p/
    all_links = soup.find_all("a", href=re.compile(r"/p/\d+"))

    seen_product_ids = set()

    for link in all_links:
        try:
            product_url = link.get("href", "")
            if not product_url:
                continue

            # Chuẩn hóa URL
            if product_url.startswith("/"):
                product_url = urljoin("https://tiki.vn", product_url)
            elif not product_url.startswith("http"):
                continue

            # Extract product ID từ URL
            product_id_match = re.search(r"/p/(\d+)", product_url)
            if not product_id_match:
                continue

            product_id = product_id_match.group(1)
            if product_id in seen_product_ids:
                continue

            seen_product_ids.add(product_id)

            # Tìm parent container
            parent = link.find_parent()
            if not parent:
                parent = link

            # Lấy tên sản phẩm từ parent hoặc link
            name = ""
            # Thử từ title
            name = link.get("title", "") or link.get("aria-label", "")

            # Thử tìm trong parent
            if not name:
                title_elem = parent.find(
                    ["h3", "h2", "div"], class_=re.compile(r"title|name", re.I)
                )
                if title_elem:
                    name = title_elem.get_text(strip=True)

            if not name:
                # Lấy text từ link
                name = link.get_text(strip=True)

            # Lấy hình ảnh (để có thể dùng preview)
            image_url = ""
            img_elem = parent.find("img") or link.find("img")
            if img_elem:
                image_url = (
                    img_elem.get("src", "")
                    or img_elem.get("data-src", "")
                    or img_elem.get("data-lazy-src", "")
                )
                if image_url:
                    if image_url.startswith("//"):
                        image_url = "https:" + image_url
                    elif image_url.startswith("/"):
                        image_url = urljoin("https://tiki.vn", image_url)

            # Extract số lượng bán từ HTML
            sales_count = None
            # Tìm text chứa "đã bán", "bán", "sold"
            sales_text = ""
            sales_elem = parent.find(string=re.compile(r"đã\s*bán|bán|sold", re.I))
            if sales_elem:
                sales_text = sales_elem.strip()
            else:
                # Tìm trong các thẻ con
                for elem in parent.find_all(
                    ["span", "div", "p"], string=re.compile(r"đã\s*bán|bán|sold", re.I)
                ):
                    sales_text = elem.get_text(strip=True)
                    break

            if sales_text:
                # Parse số từ text - dùng shared utility
                sales_count = parse_sales_count(sales_text)

            # Tạo object sản phẩm
            product = {
                "product_id": product_id,
                "name": name,
                "url": product_url,
                "category_url": category_url,
                "image_url": image_url,
                "sales_count": sales_count,
                "crawled_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }

            # Chỉ thêm nếu có đủ thông tin cơ bản
            if product_id and name:
                products.append(product)

        except Exception:
            continue

    return products


def get_total_pages(html_content):
    """
    Lấy tổng số trang từ HTML.
    """
    # Lazy import để tránh timeout khi load DAG
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html_content, "html.parser")

    # Tìm phần phân trang
    pagination_selectors = [
        ".pagination",
        '[class*="pagination"]',
        '[data-view-id="product_list_pagination"]',
    ]

    max_page = 1
    for selector in pagination_selectors:
        pagination = soup.select_one(selector)
        if pagination:
            # Tìm các link phân trang
            page_links = pagination.find_all("a")
            for link in page_links:
                page_text = link.get_text(strip=True)
                if page_text.isdigit():
                    try:
                        page_num = int(page_text)
                        max_page = max(max_page, page_num)
                    except Exception:
                        pass

    # Hoặc thử tìm từ text "Trang X/Y"
    page_info = soup.find(string=re.compile(r"trang\s*\d+", re.I))
    if page_info:
        page_match = re.search(r"trang\s*\d+.*?(\d+)", page_info, re.I)
        if page_match:
            try:
                max_page = int(page_match.group(1))
            except Exception:
                pass

    return max_page


def get_category_page_url(category_url, page=1):
    """
    Tạo URL trang phân trang của danh mục.
    """
    if "?" in category_url:
        # Nếu đã có query params
        base_url, query_string = category_url.split("?", 1)
        params = parse_qs(query_string)
        params["page"] = [str(page)]
        new_query = "&".join([f"{k}={v[0]}" for k, v in params.items()])
        return f"{base_url}?{new_query}"
    else:
        return f"{category_url}?page={page}"


def get_products_via_listing_api(category_url, max_pages=5, use_rate_limiting=True):
    """Lấy danh sách sản phẩm qua Tiki Listing API (Cực nhanh).

    Args:
        category_url: URL danh mục
        max_pages: Số trang tối đa
        use_rate_limiting: Có dùng rate limiting không

    Returns:
        List[Dict]: Danh sách sản phẩm
    """
    category_id = extract_category_id_from_url(category_url)
    if not category_id:
        return None

    # Strip 'c' from category_id if present (e.g., 'c1882' -> '1882')
    numeric_cat_id = re.sub(r"\D", "", category_id)

    all_products = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "X-Guest-Token": "no-token",  # Tiki API often works without token but headers help
    }

    # Adaptive Rate Limiting
    adaptive_limiter = None
    if use_rate_limiting:
        try:
            from pipelines.crawl.storage.adaptive_rate_limiter import get_adaptive_rate_limiter

            adaptive_limiter = get_adaptive_rate_limiter("redis://redis:6379/2")
        except Exception:
            pass

    for page in range(1, max_pages + 1):
        if adaptive_limiter:
            adaptive_limiter.wait("tiki.vn")

        api_url = f"https://tiki.vn/api/v2/products?limit=48&include=advertisement&aggregations=2&version=home-perso&category={numeric_cat_id}&page={page}"

        try:
            response = requests.get(api_url, headers=headers, timeout=15)
            if response.status_code != 200:
                break

            data = response.json()
            products_data = data.get("data", [])
            if not products_data:
                break

            for item in products_data:
                # Map API fields...
                # (keep existing item mapping logic)
                product_id = str(item.get("id"))
                product_url = item.get("url_path")
                if product_url and not product_url.startswith("http"):
                    product_url = f"https://tiki.vn/{product_url}"

                all_products.append(
                    {
                        "product_id": product_id,
                        "name": item.get("name"),
                        "url": product_url or f"https://tiki.vn/p/{product_id}.html",
                        "price": item.get("price"),
                        "original_price": item.get("original_price"),
                        "sales_count": item.get("quantity_sold")
                        and int(item.get("quantity_sold").get("value", 0))
                        if isinstance(item.get("quantity_sold"), dict)
                        else item.get("quantity_sold"),
                        "image_url": item.get("thumbnail_url"),
                        "stock": {"available": True}
                        if item.get("inventory_status") == "available"
                        else {"available": False},
                        "shipping": item.get("badges_new") or item.get("badges"),
                        "thumbnail_url": item.get("thumbnail_url"),
                        "rating_average": item.get("rating_average"),
                        "review_count": item.get("review_count"),
                        "extraction_method": "listing_api",
                    }
                )

            # Phân trang thông minh dựa trên paging metadata
            paging = data.get("paging", {})
            current_page = paging.get("current_page", page)
            last_page = paging.get("last_page", 1)
            
            if current_page >= last_page:
                break
                
            # Fallback nếu không có paging data
            if len(products_data) < 20: # Tiki trả về 40, nếu < 20 chắc chắn là hết
                break

        except Exception as e:
            print(f"⚠️ Error fetching listing API (page {page}): {e}")
            break

    return all_products


def crawl_category_products(
    category_url,
    max_pages=None,
    use_selenium=False,
    cache_dir=DEFAULT_PRODUCT_LIST_CACHE_DIR,
    use_redis_cache=True,
    use_rate_limiting=True,
    allow_selenium_fallback=True,  # Tối ưu: Cho phép tắt fallback Selenium
    logger: logging.Logger | None = None,
    force_refresh=False,
    cache_ttl=86400, # 24 giờ mặc định
):
    """Crawl tất cả sản phẩm từ một danh mục.

    Args:
        category_url: URL của category
        max_pages: Số trang tối đa để crawl
        use_selenium: Có dùng Selenium không
        cache_dir: Thư mục cache (fallback nếu Redis không available)
        use_redis_cache: Có dùng Redis cache không (mặc định True)
    """
    logger = logger or logging.getLogger(__name__)

    all_products = []

    # Tier 1: Thử Redis cache (Ưu tiên nhất)
    # Tier 1: Thử Redis cache (Ưu tiên nhất)
    redis_cache = None
    if use_redis_cache:
        try:
            from pipelines.crawl.storage.redis_cache import get_redis_cache
            redis_cache = get_redis_cache("redis://redis:6379/1")
            
            if redis_cache and not force_refresh:
                cached_products = redis_cache.get_cached_products(category_url)
                if cached_products:
                    logger.info(f"[Redis Cache] ✅ Hit cache cho {category_url[:60]}... ({len(cached_products)} products)")
                    return cached_products
        except Exception as e:
            logger.debug(f"[Redis Cache] Error: {e}")

    # Tier 2: Thử File cache
    cache_file = None
    if cache_dir:
        import hashlib
        url_hash = hashlib.md5(category_url.encode()).hexdigest()
        cache_file = os.path.join(cache_dir, f"{url_hash}.json")

        if os.path.exists(cache_file) and not force_refresh:
            # Kiểm tra TTL của file
            file_age = time.time() - os.path.getmtime(cache_file)
            if file_age < cache_ttl:
                cached_data = safe_read_json(cache_file)
                if cached_data and "products" in cached_data:
                    cached_products = cached_data["products"]
                    logger.info(f"[File Cache] ✅ Hit cache ({int(file_age/3600)}h cũ) cho {category_url[:60]}... ({len(cached_products)} products)")
                    return cached_products
            else:
                logger.info(f"[File Cache] 🔄 Cache quá hạn ({int(file_age/3600)}h), tiến hành crawl mới...")

    # Tier 3: Thử Listing API (Nhanh nhất nếu cache miss)
    # Tăng mặc định lên cao hơn để lấy nhiều sản phẩm hơn (comprehensive)
    limit_max_pages = max_pages if max_pages is not None else 10
    api_products = get_products_via_listing_api(
        category_url, max_pages=limit_max_pages, use_rate_limiting=use_rate_limiting
    )
    if api_products:  # Fallback to HTML if API returns empty list
        logger.info(f"[Listing API] ✅ Lấy được {len(api_products)} sản phẩm cho {category_url[:60]}...")
        # Inject category_url for downstream usage
        for p in api_products:
            p["category_url"] = category_url
            
        # Tối ưu: Cache ngay lập tức sau khi lấy từ API thành công
        if redis_cache:
            try:
                redis_cache.cache_products(category_url, api_products, ttl=43200) # 12h
            except Exception:
                pass
                
        # Bug fix: Save to File Cache as well to update timestamp and prevent re-crawl loop
        if cache_file:
            try:
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump(
                        {
                            "category_url": category_url,
                            "products": api_products,
                            "crawled_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                        },
                        f,
                        ensure_ascii=False,
                        indent=2,
                    )
            except Exception as e:
                logger.error(f"[File Cache] ❌ Error updating cache file {cache_file}: {e}")

        return api_products

    try:
        # Lấy trang đầu để xác định số trang
        html = None
        if use_selenium:
            if _check_selenium_available():
                html = get_page_with_selenium(
                    category_url,
                    use_redis_cache=use_redis_cache,
                    use_rate_limiting=use_rate_limiting,
                )
            else:
                logger.warning("⚠️  Selenium chưa được cài đặt, dùng requests thay thế")
                html = get_page_with_requests(
                    category_url,
                    use_redis_cache=use_redis_cache,
                    use_rate_limiting=use_rate_limiting,
                )
        else:
            html = get_page_with_requests(
                category_url, use_redis_cache=use_redis_cache, use_rate_limiting=use_rate_limiting
            )
            # Nếu không tìm thấy sản phẩm với requests, thử Selenium (nếu cho phép)
            if html:
                products_test = parse_products_from_html(html, category_url)
                if not products_test and allow_selenium_fallback and _check_selenium_available():
                    logger.warning("⚠️  Không tìm thấy sản phẩm với requests, thử Selenium fallback...")
                    html = get_page_with_selenium(category_url)
                    use_selenium = True  # Đánh dấu đã dùng Selenium

        if not html:
            return []

        # Parse sản phẩm từ trang đầu
        products = parse_products_from_html(html, category_url)
        all_products.extend(products)

        # Nếu không tìm thấy sản phẩm, return sớm
        if not products:
            # Lưu cache rỗng để đánh dấu đã thử
            if cache_file:
                try:
                    with open(cache_file, "w", encoding="utf-8") as f:
                        json.dump(
                            {
                                "category_url": category_url,
                                "products": [],
                                "crawled_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                            },
                            f,
                            ensure_ascii=False,
                            indent=2,
                        )
                except Exception:
                    pass
            return []

        # Lấy tổng số trang
        total_pages = get_total_pages(html)
        if max_pages:
            total_pages = min(total_pages, max_pages)

        # Crawl các trang tiếp theo
        for page in range(2, total_pages + 1):
            try:
                page_url = get_category_page_url(category_url, page)
                if use_selenium and _check_selenium_available():
                    html = get_page_with_selenium(
                        page_url,
                        use_redis_cache=use_redis_cache,
                        use_rate_limiting=use_rate_limiting,
                    )
                else:
                    html = get_page_with_requests(
                        page_url,
                        use_redis_cache=use_redis_cache,
                        use_rate_limiting=use_rate_limiting,
                    )

                if html:
                    products = parse_products_from_html(html, category_url)
                    if products:
                        all_products.extend(products)
                    else:
                        # Nếu không tìm thấy sản phẩm ở trang này, có thể đã hết
                        break

                # Rate limiting đã được xử lý trong get_page_with_requests/selenium
                # Chỉ sleep nếu không dùng rate limiting
                if not use_rate_limiting:
                    time.sleep(1)  # Delay giữa các trang

            except Exception:
                continue

        # Loại bỏ trùng lặp theo product_id
        seen_ids = set()
        unique_products = []
        for product in all_products:
            if product["product_id"] not in seen_ids:
                seen_ids.add(product["product_id"])
                unique_products.append(product)

        # Lưu cache - ưu tiên Redis, fallback về file
        # Redis cache (nhanh, distributed)
        if redis_cache:
            try:
                redis_cache.cache_products(category_url, unique_products, ttl=43200)  # 12 giờ
                logger.info(
                    f"[Redis Cache] ✅ Đã cache {len(unique_products)} products cho {category_url[:60]}..."
                )
            except Exception as e:
                logger.error(f"[Redis Cache] ⚠️  Lỗi khi cache vào Redis: {e}")

        # File cache (fallback)
        if cache_file:
            try:
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump(
                        {
                            "category_url": category_url,
                            "products": unique_products,
                            "crawled_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                        },
                        f,
                        ensure_ascii=False,
                        indent=2,
                    )
            except Exception as e:
                if logger:
                    logger.error(f"[File Cache] ❌ Lỗi khi ghi cache file {cache_file}: {e}")
                else:
                    print(f"[File Cache] ❌ Lỗi khi ghi cache file {cache_file}: {e}")

        return unique_products

    except Exception:
        return []


def crawl_single_category(category, max_pages=None, use_selenium=False):
    """
    Crawl sản phẩm từ một danh mục (wrapper cho threading)
    """
    global stats

    category_url = category.get("url", "")
    if not category_url:
        with stats_lock:
            stats["total_failed"] += 1
        return None, []

    try:
        products = crawl_category_products(
            category_url, max_pages=max_pages, use_selenium=use_selenium
        )

        with stats_lock:
            stats["total_categories"] += 1
            stats["total_products"] += len(products)
            stats["total_success"] += 1

        return category, products

    except Exception:
        with stats_lock:
            stats["total_categories"] += 1
            stats["total_failed"] += 1
        return category, []


def crawl_products_from_categories(
    categories_file,
    output_file=None,
    max_categories=None,
    max_pages_per_category=None,
    max_workers=5,
    use_selenium=False,
    categories_filter=None,
):
    """Crawl sản phẩm từ file danh mục.

    Args:
        categories_file: Đường dẫn file JSON chứa danh mục
        output_file: File output (mặc định: data/demo/products/products.json)
        max_categories: Số danh mục tối đa để crawl (None = tất cả)
        max_pages_per_category: Số trang tối đa mỗi danh mục (None = tất cả)
        max_workers: Số thread song song
        use_selenium: Có dùng Selenium không (chậm hơn nhưng chính xác hơn)
        categories_filter: Function filter danh mục (cat) -> bool
    """
    global stats

    # Reset stats
    stats = {
        "total_categories": 0,
        "total_products": 0,
        "total_pages": 0,
        "total_success": 0,
        "total_failed": 0,
        "start_time": time.time(),
    }

    # Đọc danh mục
    print(f"📖 Đang đọc danh mục từ: {categories_file}")
    try:
        with open(categories_file, encoding="utf-8") as f:
            categories = json.load(f)
        print(f"✓ Đã đọc {len(categories)} danh mục")
    except Exception as e:
        print(f"❌ Lỗi khi đọc file: {e}")
        return []

    # Lọc danh mục nếu có filter
    if categories_filter:
        categories = [cat for cat in categories if categories_filter(cat)]
        print(f"✓ Sau khi lọc: {len(categories)} danh mục")

    # Giới hạn số danh mục
    if max_categories:
        categories = categories[:max_categories]
        print(f"✓ Giới hạn: {len(categories)} danh mục")

    # Crawl song song
    print("\n🚀 Bắt đầu crawl sản phẩm...")
    print(f"📊 Số thread: {max_workers}")
    print(f"🔧 Sử dụng Selenium: {use_selenium}")
    print(f"📄 Trang tối đa mỗi danh mục: {max_pages_per_category or 'Tất cả'}")
    print("=" * 70)

    all_products = []
    category_results = {}

    # Lazy import tqdm
    tqdm = _get_tqdm()
    with tqdm(total=len(categories), desc="Crawl danh mục", unit="danh mục") as pbar:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tasks
            future_to_category = {}
            for category in categories:
                future = executor.submit(
                    crawl_single_category,
                    category,
                    max_pages=max_pages_per_category,
                    use_selenium=use_selenium,
                )
                future_to_category[future] = category

            # Xử lý kết quả
            for future in as_completed(future_to_category):
                category = future_to_category[future]
                try:
                    cat, products = future.result(timeout=300)
                    if products:
                        all_products.extend(products)
                        category_results[category.get("url", "")] = len(products)

                    pbar.set_postfix(
                        {
                            "✅": stats["total_success"],
                            "❌": stats["total_failed"],
                            "📦": stats["total_products"],
                        }
                    )
                except Exception:
                    with stats_lock:
                        stats["total_failed"] += 1
                    pbar.set_postfix(
                        {
                            "✅": stats["total_success"],
                            "❌": stats["total_failed"],
                            "📦": stats["total_products"],
                        }
                    )

                pbar.update(1)

    # Loại bỏ trùng lặp theo product_id
    seen_ids = set()
    unique_products = []
    for product in all_products:
        if product["product_id"] not in seen_ids:
            seen_ids.add(product["product_id"])
            unique_products.append(product)

    # Lưu kết quả
    if not output_file:
        output_file = DEFAULT_PRODUCTS_DIR / "products.json"

    print(f"\n💾 Đang lưu kết quả vào: {output_file}")
    print("📝 Lưu ý: Crawl thông tin cơ bản (ID, tên, URL, hình, số lượng bán)")
    print("          Giá, đánh giá chi tiết sẽ được crawl detail sau")

    # Đảm bảo tất cả products có sales_count (kể cả None)
    for product in unique_products:
        if "sales_count" not in product:
            product["sales_count"] = None

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(
            {
                "total_products": len(unique_products),
                "total_categories": stats["total_categories"],
                "crawled_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "note": "Crawl thông tin cơ bản bao gồm số lượng bán (sales_count) - giá và đánh giá chi tiết sẽ crawl sau",
                "products": unique_products,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

    # In thống kê
    elapsed = time.time() - stats["start_time"]
    print("\n" + "=" * 70)
    print("📈 THỐNG KÊ")
    print("=" * 70)
    print(f"⏱  Thời gian: {elapsed:.1f}s")
    print(f"📁 Danh mục đã crawl: {stats['total_categories']}")
    print(f"✅ Thành công: {stats['total_success']}")
    print(f"❌ Thất bại: {stats['total_failed']}")
    print(f"📦 Tổng sản phẩm: {len(unique_products)}")
    print(f"⚡ Tốc độ: {stats['total_products'] / elapsed:.2f} sản phẩm/s" if elapsed > 0 else "")
    print("=" * 70)

    return unique_products


def main():
    """
    Hàm main.
    """
    categories_file = "data/raw/categories_recursive_optimized.json"
    output_file = DEFAULT_PRODUCTS_DIR / "products.json"

    # Tùy chọn
    max_categories = 10  # None để crawl tất cả
    max_pages_per_category = 3  # None để crawl tất cả trang
    max_workers = 5  # Số thread song song
    use_selenium = False  # True nếu cần JS rendering

    print("=" * 70)
    print("🛍️  CRAWL SẢN PHẨM TỪ DANH MỤC TIKI")
    print("=" * 70)
    print(f"📁 File danh mục: {categories_file}")
    print(f"📁 File output: {output_file}")
    print(f"📊 Số danh mục tối đa: {max_categories or 'Tất cả'}")
    print(f"📄 Trang tối đa mỗi danh mục: {max_pages_per_category or 'Tất cả'}")
    print(f"⚙️  Số thread: {max_workers}")
    print(f"🔧 Sử dụng Selenium: {use_selenium}")
    print("=" * 70)

    crawl_products_from_categories(
        categories_file=categories_file,
        output_file=output_file,
        max_categories=max_categories,
        max_pages_per_category=max_pages_per_category,
        max_workers=max_workers,
        use_selenium=use_selenium,
    )


if __name__ == "__main__":
    main()
