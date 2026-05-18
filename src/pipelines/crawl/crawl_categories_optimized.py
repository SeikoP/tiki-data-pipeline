# Import từ file cùng thư mục
import importlib.util
import json
import logging
import os
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock

logger = logging.getLogger(__name__)

# Try importing constants from crawl_utils
try:
    from crawl_utils import DEFAULT_CATEGORY_CACHE_DIR, DEFAULT_DATA_DIR
except ImportError:
    try:
        from src.pipelines.crawl.crawl_utils import DEFAULT_CATEGORY_CACHE_DIR, DEFAULT_DATA_DIR
    except ImportError:
        # Fallback constants if imports fail
        DEFAULT_DATA_DIR = Path("data/raw")
        DEFAULT_CATEGORY_CACHE_DIR = DEFAULT_DATA_DIR / "cache" / "categories"

# Lazy-load selenium extraction module to avoid import-time overhead
_extract_module = None


def _get_extract_module():
    global _extract_module
    if _extract_module is None:
        spec = importlib.util.spec_from_file_location(
            "extract_category_link_selenium",
            os.path.join(os.path.dirname(__file__), "extract_category_link_selenium.py"),
        )
        _extract_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(_extract_module)
    return _extract_module


def crawl_with_selenium(*args, **kwargs):
    return _get_extract_module().crawl_with_selenium(*args, **kwargs)


def parse_categories(*args, **kwargs):
    return _get_extract_module().parse_categories(*args, **kwargs)


# Set UTF-8 encoding cho stdout trên Windows
if sys.platform == "win32":
    try:
        import io

        if hasattr(sys.stdout, "buffer") and not sys.stdout.closed:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        try:
            import io

            if hasattr(sys.stdout, "buffer"):
                sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        except Exception:
            pass

# Thử import tqdm, nếu không có thì dùng fallback
try:
    from tqdm import tqdm

    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    logger.warning("⚠️  Khuyến nghị cài đặt tqdm để hiển thị progress bar: pip install tqdm")

    tqdm = None


class SafeProgress:
    """Minimal tqdm-compatible progress helper for non-interactive or closed stdout runs."""

    def __init__(self, iterable=None, total=None, desc="", **kwargs):
        self.iterable = iterable
        self.total = total or (len(iterable) if iterable else 0)
        self.desc = desc
        self.n = 0
        self.start_time = time.time()
        self.last_print_time = 0.0
        self.print_interval = 5.0

    def __enter__(self):
        print(f"{self.desc}: starting {self.total} items", flush=True)
        return self

    def __exit__(self, *args):
        print(f"{self.desc}: completed {self.n}/{self.total} items", flush=True)

    def __iter__(self):
        if self.iterable:
            for item in self.iterable:
                self.update(1)
                yield item
        else:
            return self

    def update(self, n=1):
        self.n += n
        now = time.time()
        if self.n == self.total or now - self.last_print_time >= self.print_interval:
            pct = (self.n / self.total * 100) if self.total else 100.0
            elapsed = now - self.start_time
            rate = self.n / elapsed if elapsed > 0 else 0.0
            print(
                f"{self.desc}: {self.n}/{self.total} ({pct:.1f}%) | {rate:.2f} items/s",
                flush=True,
            )
            self.last_print_time = now

    def set_description(self, desc):
        self.desc = desc

    def set_postfix(self, _postfix=None, **kwargs):
        pass


def progress_bar(*args, **kwargs):
    if not HAS_TQDM or tqdm is None or sys.stdout.closed:
        return SafeProgress(*args, **kwargs)
    try:
        return tqdm(*args, file=sys.stdout, **kwargs)
    except Exception as exc:
        logger.warning("Progress bar unavailable, continuing without tqdm: %s", exc)
        return SafeProgress(*args, **kwargs)


# Tạo thư mục output (deferred to avoid import-time side effects)
def _ensure_output_dirs():
    os.makedirs(DEFAULT_DATA_DIR, exist_ok=True)


# Thread-safe locks và counters
stats_lock = Lock()
stats = {
    "total_crawled": 0,
    "total_success": 0,
    "total_failed": 0,
    "total_categories": 0,
    "by_level": defaultdict(int),
    "start_time": time.time(),
}


def crawl_single_category(
    url,
    parent_url,
    level,
    max_level,
    visited_urls,
    cache_dir=DEFAULT_CATEGORY_CACHE_DIR,
    driver_pool=None,
):
    """Crawl một danh mục đơn lẻ (thread-safe)

    Args:
        driver_pool: Optional SeleniumDriverPool for driver reuse

    Returns:
        tuple: (success: bool, categories: list, error: str)
    """
    global stats

    # Kiểm tra cache
    cache_file = None
    if cache_dir:
        os.makedirs(cache_dir, exist_ok=True)
        import hashlib

        url_hash = hashlib.md5(url.encode(), usedforsecurity=False).hexdigest()
        cache_file = os.path.join(cache_dir, f"{url_hash}.json")

        if os.path.exists(cache_file):
            try:
                with open(cache_file, encoding="utf-8") as f:
                    cached_data = json.load(f)
                cached_categories = cached_data.get("categories", [])
                with stats_lock:
                    stats["total_crawled"] += 1
                    stats["total_success"] += 1
                    stats["total_categories"] += len(cached_categories)
                return True, cached_categories, None
            except Exception as e:
                # Nếu cache file bị lỗi, tiếp tục crawl lại
                logger.warning(f"  ⚠️  Cache file lỗi, sẽ crawl lại: {cache_file} - {str(e)}")

    try:
        # Crawl với Selenium (ưu tiên driver pool nếu có)
        html_content = None
        if driver_pool is not None:
            driver = driver_pool.get_driver()
            if driver is not None:
                try:
                    # Import crawl_with_driver nếu có
                    try:
                        from extract_category_link_selenium import crawl_with_driver

                        html_content = crawl_with_driver(
                            driver, url, save_html=False, verbose=False
                        )
                    except (ImportError, AttributeError):
                        # Fallback: crawl_with_driver chưa có
                        pass
                finally:
                    driver_pool.return_driver(driver)

        # Fallback: tạo driver riêng nếu pool không có hoặc fail
        if html_content is None:
            html_content = crawl_with_selenium(url, save_html=False, verbose=False)

        # Parse danh mục con
        child_categories = parse_categories(html_content, parent_url=url, level=level + 1)

        # Lọc chỉ lấy các danh mục có hình ảnh
        categories_with_images = [
            cat for cat in child_categories if cat.get("image_url", "").strip()
        ]

        # Lưu cache
        if cache_file:
            try:
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump(
                        {"url": url, "categories": categories_with_images},
                        f,
                        ensure_ascii=False,
                        indent=2,
                    )
            except Exception:
                pass

        # Update stats
        with stats_lock:
            stats["total_crawled"] += 1
            stats["total_success"] += 1
            stats["total_categories"] += len(categories_with_images)
            stats["by_level"][level + 1] += len(categories_with_images)

        return True, categories_with_images, None

    except Exception as e:
        error_msg = str(e)
        error_type = type(e).__name__
        with stats_lock:
            stats["total_crawled"] += 1
            stats["total_failed"] += 1
        # Log chi tiết hơn cho debugging
        logger.error(f"  ⚠️  Lỗi crawl {url}: [{error_type}] {error_msg}")
        return False, [], error_msg


def crawl_level_parallel(
    urls_to_crawl, parent_urls, level, max_level, visited_urls, max_workers=3, driver_pool=None
):
    """Crawl song song nhiều danh mục cùng level.

    Args:
        urls_to_crawl: List các URL cần crawl
        parent_urls: List các parent URL tương ứng
        level: Level hiện tại
        max_level: Độ sâu tối đa
        visited_urls: Set các URL đã crawl
        max_workers: Số thread tối đa (giới hạn để tránh quá tải)
        driver_pool: Optional SeleniumDriverPool for driver reuse

    Returns:
        dict: {url: (success, categories, error)}
    """
    results = {}

    # Lọc các URL chưa crawl
    tasks = []
    queued_urls = set()
    for url, parent_url in zip(urls_to_crawl, parent_urls, strict=False):
        if url not in visited_urls and url not in queued_urls:
            tasks.append((url, parent_url))
            queued_urls.add(url)

    if not tasks:
        return results

    # Tạo progress bar
    desc = f"Level {level}"
    with progress_bar(total=len(tasks), desc=desc, unit="danh mục") as pbar:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tất cả tasks
            future_to_url = {}
            for url, parent_url in tasks:
                future = executor.submit(
                    crawl_single_category,
                    url,
                    parent_url,
                    level,
                    max_level,
                    visited_urls,
                    DEFAULT_CATEGORY_CACHE_DIR,
                    driver_pool,
                )
                future_to_url[future] = (url, parent_url)

            # Xử lý kết quả khi hoàn thành
            for future in as_completed(future_to_url):
                url, parent_url = future_to_url[future]
                try:
                    success, categories, error = future.result(timeout=300)  # Timeout 5 phút
                    results[url] = (success, categories, error)

                    # Đánh dấu đã crawl (dù thành công hay thất bại)
                    visited_urls.add(url)

                    if success:
                        with stats_lock:
                            pbar.set_postfix({"✅": len(categories), "❌": stats["total_failed"]})
                    else:
                        with stats_lock:
                            pbar.set_postfix(
                                {"✅": stats["total_success"], "❌": stats["total_failed"]}
                            )
                        logger.error(f"\n  ❌ Lỗi crawl {url}: {error}")
                except Exception as e:
                    error_msg = str(e)
                    error_type = type(e).__name__
                    results[url] = (False, [], error_msg)
                    visited_urls.add(url)  # Đánh dấu đã thử crawl
                    with stats_lock:
                        stats["total_failed"] += 1
                    logger.error(f"\n  ❌ Exception khi crawl {url}: [{error_type}] {error_msg}")

                pbar.update(1)

    return results


def crawl_category_recursive_optimized(
    root_urls, max_level=3, max_workers=3, visited_urls=None, all_categories=None
):
    """Crawl đệ quy các danh mục với tối ưu song song.

    Args:
        root_urls: URL danh mục gốc (str) hoặc danh sách các URL gốc (list[str])
        max_level: Độ sâu tối đa
        max_workers: Số thread tối đa cho mỗi level
        visited_urls: Set các URL đã crawl
        all_categories: List tất cả các danh mục đã crawl
    """
    if visited_urls is None:
        visited_urls = set()
    if all_categories is None:
        all_categories = []

    # Hỗ trợ cả single URL và list URLs
    if isinstance(root_urls, str):
        root_urls = [root_urls]

    # Initialize driver pool for reuse
    driver_pool = None
    try:
        # Try to import SeleniumDriverPool
        try:
            spec = importlib.util.spec_from_file_location(
                "crawl_utils",
                os.path.join(os.path.dirname(__file__), "crawl_utils.py"),
            )
            if spec and spec.loader:
                utils_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(utils_module)
                SeleniumDriverPool = getattr(utils_module, "SeleniumDriverPool", None)
                if SeleniumDriverPool:
                    driver_pool = SeleniumDriverPool(
                        pool_size=max_workers, headless=True, timeout=90
                    )
                    logger.info(f"✅ Đã khởi tạo driver pool với {max_workers} drivers")
        except Exception:
            pass  # Fallback: không dùng pool

        # Queue các URL cần crawl theo level
        # Format: {level: [(url, parent_url), ...]}
        queue = defaultdict(list)
        queue[0] = [(url, None) for url in root_urls]

        # QUAN TRỌNG: Thêm root categories vào đầu kết quả
        # Để đảm bảo category hierarchy đầy đủ (root -> children -> grandchildren)
        import re

        # Đảm bảo không thêm trùng root category theo URL
        existing_root_urls = {c["url"] for c in all_categories if c.get("url")}
        for root_url in root_urls:
            if root_url in existing_root_urls:
                # Bỏ qua URL trùng, tránh thêm duplicate root category
                continue

            match = re.search(r"/([^/]+)/(c\d+)", root_url)
            if match:
                root_slug = match.group(1)
                root_cat_id = match.group(2)
                root_name = root_slug.replace("-", " ").title()

                root_category = {
                    "name": root_name,
                    "slug": root_slug,
                    "url": root_url,
                    "image_url": "",
                    "parent_url": "",  # Root không có parent
                    "level": 0,
                }
                all_categories.append(root_category)
                existing_root_urls.add(root_url)
                logger.info(f"✅ Đã thêm root category: {root_name} ({root_cat_id})")

        # Crawl từng level một
        for current_level in range(max_level + 1):
            if current_level not in queue or not queue[current_level]:
                continue

            urls_to_crawl = [url for url, _ in queue[current_level]]
            parent_urls = [parent for _, parent in queue[current_level]]

            # Lọc các URL chưa crawl
            new_urls = []
            new_parents = []
            for url, parent in zip(urls_to_crawl, parent_urls, strict=False):
                if url not in visited_urls:
                    new_urls.append(url)
                    new_parents.append(parent)

            if not new_urls:
                continue

            logger.info(f"\n{'=' * 70}")
            logger.info(f"Level {current_level}: Dang crawl {len(new_urls)} danh muc...")
            logger.info(f"{'=' * 70}")

            # Crawl song song
            results = crawl_level_parallel(
                new_urls,
                new_parents,
                current_level,
                max_level,
                visited_urls,
                max_workers=max_workers,
                driver_pool=driver_pool,
            )

            # Xử lý kết quả và chuẩn bị level tiếp theo
            queued_next_urls = {next_url for next_url, _ in queue[current_level + 1]}
            for url, (success, categories, error) in results.items():
                if success:
                    # Thêm vào danh sách tổng
                    all_categories.extend(categories)

                    # Thêm các danh mục con vào queue level tiếp theo
                    if current_level < max_level:
                        for cat in categories:
                            child_url = cat["url"]
                            if child_url not in visited_urls and child_url not in queued_next_urls:
                                queue[current_level + 1].append((child_url, url))
                                queued_next_urls.add(child_url)
                else:
                    logger.error(f"  ❌ Lỗi crawl {url}: {error}")

        return all_categories
    finally:
        # Cleanup driver pool
        if driver_pool is not None:
            try:
                driver_pool.cleanup()
                logger.info("✅ Đã cleanup driver pool")
            except Exception:
                pass


def print_stats():
    """
    In thống kê real-time.
    """
    global stats
    with stats_lock:
        elapsed = time.time() - stats["start_time"]
        rate = stats["total_crawled"] / elapsed if elapsed > 0 else 0

        logger.info(f"\n{'=' * 70}")
        logger.info("📈 THỐNG KÊ")
        logger.info(f"{'=' * 70}")
        logger.info(f"⏱  Thời gian: {elapsed:.1f}s")
        logger.info(f"📥 Đã crawl: {stats['total_crawled']} danh mục")
        logger.info(f"✅ Thành công: {stats['total_success']}")
        logger.error(f"❌ Thất bại: {stats['total_failed']}")
        logger.info(f"📊 Tổng danh mục tìm được: {stats['total_categories']}")
        logger.info(f"⚡ Tốc độ: {rate:.2f} danh mục/s")

        if stats["by_level"]:
            logger.info("\n📋 Theo level:")
            for level in sorted(stats["by_level"].keys()):
                logger.info(f"  Level {level}: {stats['by_level'][level]} danh mục")


def main():
    """
    Hàm main để crawl đệ quy với tối ưu.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    # Hỗ trợ nhiều root categories từ config file hoặc tham số
    # Có thể cấu hình qua:
    # 1. File JSON: data/raw/root_categories.json
    # 2. Biến môi trường: TIKI_ROOT_CATEGORIES (comma-separated URLs)
    # 3. Default: danh sách mặc định

    root_urls = []

    # Thử đọc từ file config
    config_file = "data/raw/root_categories.json"
    if os.path.exists(config_file):
        try:
            with open(config_file, encoding="utf-8") as f:
                config_data = json.load(f)
                if isinstance(config_data, list):
                    root_urls = config_data
                elif isinstance(config_data, dict) and "root_urls" in config_data:
                    root_urls = config_data["root_urls"]
                else:
                    print(f"⚠️  Config file có cấu trúc không hợp lệ: {config_file}")
                    root_urls = []

                # Chỉ print success message nếu thực sự load được URLs
                if root_urls:
                    print(f"✅ Đã load {len(root_urls)} root categories từ {config_file}")
        except Exception as e:
            print(f"⚠️  Không thể đọc config file: {e}")

    # Thử đọc từ biến môi trường
    if not root_urls:
        env_urls = os.getenv("TIKI_ROOT_CATEGORIES", "")
        if env_urls:
            root_urls = [url.strip() for url in env_urls.split(",") if url.strip()]
            print(f"✅ Đã load {len(root_urls)} root categories từ biến môi trường")

    # Default: danh sách mặc định nếu không có config
    if not root_urls:
        root_urls = [
            # "https://tiki.vn/thoi-trang-nu/c931",
            # Có thể thêm các root categories khác ở đây
            "https://tiki.vn/nha-cua-doi-song/c1883",
            "https://tiki.vn/dien-tu-dien-lanh/c4221",
        ]
        print("ℹ️  Sử dụng root categories mặc định")

    # Độ sâu tối đa
    max_level = int(os.getenv("TIKI_MAX_CATEGORY_LEVEL", "5"))

    # Số thread song song (giới hạn để tránh quá tải server)
    max_workers = int(os.getenv("TIKI_CRAWL_MAX_WORKERS", "3"))

    print("=" * 70)
    print("🚀 CRAWL ĐỆ QUY CÁC DANH MỤC TIKI (TỐI ƯU)")
    print("=" * 70)
    print(f"Số root categories: {len(root_urls)}")
    for i, url in enumerate(root_urls, 1):
        print(f"  {i}. {url}")
    print(f"Độ sâu tối đa: {max_level}")
    print(f"Số thread song song: {max_workers}")
    print(f"Cache: {DEFAULT_CATEGORY_CACHE_DIR}")
    print("=" * 70)

    # Reset stats
    global stats
    stats = {
        "total_crawled": 0,
        "total_success": 0,
        "total_failed": 0,
        "total_categories": 0,
        "by_level": defaultdict(int),
        "start_time": time.time(),
    }

    # Crawl đệ quy với tối ưu
    all_categories = crawl_category_recursive_optimized(
        root_urls, max_level=max_level, max_workers=max_workers
    )

    # Loại bỏ trùng lặp theo URL (giữ lại bản đầu tiên)
    unique_categories = []
    seen_urls = set()
    for cat in all_categories:
        if cat["url"] not in seen_urls:
            unique_categories.append(cat)
            seen_urls.add(cat["url"])

    # Sắp xếp theo level và tên
    unique_categories.sort(key=lambda x: (x.get("level", 0), x["name"]))

    # Lưu kết quả
    output_file = "data/raw/categories_recursive_optimized.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(unique_categories, f, ensure_ascii=False, indent=2)

    # In thống kê
    print_stats()

    print(f"\n💾 Đã lưu vào: {output_file}")
    print(f"📦 Tổng số danh mục unique: {len(unique_categories)}")

    # Thống kê theo level
    level_counts = defaultdict(int)
    for cat in unique_categories:
        level = cat.get("level", 0)
        level_counts[level] += 1

    if level_counts:
        print("\n📋 Thống kê theo level:")
        for level in sorted(level_counts.keys()):
            print(f"  Level {level}: {level_counts[level]} danh mục")


if __name__ == "__main__":
    main()
