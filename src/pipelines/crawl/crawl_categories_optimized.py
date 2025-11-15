import json
import sys
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from collections import defaultdict

# Import từ file cùng thư mục
import importlib.util

spec = importlib.util.spec_from_file_location(
    "extract_category_link_selenium",
    os.path.join(os.path.dirname(__file__), "extract_category_link_selenium.py"),
)
extract_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(extract_module)

crawl_with_selenium = extract_module.crawl_with_selenium
parse_categories = extract_module.parse_categories

# Set UTF-8 encoding cho stdout trên Windows
if sys.platform == "win32":
    try:
        import io

        if hasattr(sys.stdout, "buffer") and not sys.stdout.closed:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except:
        try:
            import io

            if hasattr(sys.stdout, "buffer"):
                sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        except:
            pass

# Thử import tqdm, nếu không có thì dùng fallback
try:
    from tqdm import tqdm

    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    print("⚠️  Khuyến nghị cài đặt tqdm để hiển thị progress bar: pip install tqdm")

    # Fallback progress bar đơn giản
    class tqdm:
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

        def set_description(self, desc):
            self.desc = desc


# Tạo thư mục output nếu chưa có
os.makedirs("data/raw", exist_ok=True)

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
    url, parent_url, level, max_level, visited_urls, cache_dir="data/raw/cache"
):
    """
    Crawl một danh mục đơn lẻ (thread-safe)

    Returns:
        tuple: (success: bool, categories: list, error: str)
    """
    global stats

    # Kiểm tra cache
    cache_file = None
    if cache_dir:
        os.makedirs(cache_dir, exist_ok=True)
        import hashlib

        url_hash = hashlib.md5(url.encode()).hexdigest()
        cache_file = os.path.join(cache_dir, f"{url_hash}.json")

        if os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    cached_data = json.load(f)
                with stats_lock:
                    stats["total_crawled"] += 1
                    stats["total_success"] += 1
                return True, cached_data.get("categories", []), None
            except:
                pass

    try:
        # Crawl với Selenium (không verbose để tránh spam log)
        html_content = crawl_with_selenium(url, save_html=False, verbose=False)

        # Parse danh mục con
        child_categories = parse_categories(html_content, parent_url=url, level=level + 1)

        # Lọc chỉ lấy các danh mục có hình ảnh
        categories_with_images = []
        for cat in child_categories:
            if cat.get("image_url", "").strip():
                categories_with_images.append(cat)

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
            except:
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
        with stats_lock:
            stats["total_crawled"] += 1
            stats["total_failed"] += 1
        return False, [], error_msg


def crawl_level_parallel(urls_to_crawl, parent_urls, level, max_level, visited_urls, max_workers=3):
    """
    Crawl song song nhiều danh mục cùng level

    Args:
        urls_to_crawl: List các URL cần crawl
        parent_urls: List các parent URL tương ứng
        level: Level hiện tại
        max_level: Độ sâu tối đa
        visited_urls: Set các URL đã crawl
        max_workers: Số thread tối đa (giới hạn để tránh quá tải)

    Returns:
        dict: {url: (success, categories, error)}
    """
    results = {}

    # Lọc các URL chưa crawl
    tasks = []
    for url, parent_url in zip(urls_to_crawl, parent_urls):
        if url not in visited_urls:
            tasks.append((url, parent_url))

    if not tasks:
        return results

    # Tạo progress bar
    desc = f"Level {level}"
    with tqdm(total=len(tasks), desc=desc, unit="danh mục") as pbar:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tất cả tasks
            future_to_url = {}
            for url, parent_url in tasks:
                future = executor.submit(
                    crawl_single_category, url, parent_url, level, max_level, visited_urls
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
                        print(f"\n  ❌ Lỗi crawl {url}: {error}")
                except Exception as e:
                    error_msg = str(e)
                    results[url] = (False, [], error_msg)
                    visited_urls.add(url)  # Đánh dấu đã thử crawl
                    with stats_lock:
                        stats["total_failed"] += 1
                    print(f"\n  ❌ Exception khi crawl {url}: {error_msg}")

                pbar.update(1)

    return results


def crawl_category_recursive_optimized(
    root_url, max_level=3, max_workers=3, visited_urls=None, all_categories=None
):
    """
    Crawl đệ quy các danh mục với tối ưu song song

    Args:
        root_url: URL danh mục gốc
        max_level: Độ sâu tối đa
        max_workers: Số thread tối đa cho mỗi level
        visited_urls: Set các URL đã crawl
        all_categories: List tất cả các danh mục đã crawl
    """
    if visited_urls is None:
        visited_urls = set()
    if all_categories is None:
        all_categories = []

    # Queue các URL cần crawl theo level
    # Format: {level: [(url, parent_url), ...]}
    queue = defaultdict(list)
    queue[0] = [(root_url, None)]

    # Crawl từng level một
    for current_level in range(max_level + 1):
        if current_level not in queue or not queue[current_level]:
            continue

        urls_to_crawl = [url for url, _ in queue[current_level]]
        parent_urls = [parent for _, parent in queue[current_level]]

        # Lọc các URL chưa crawl
        new_urls = []
        new_parents = []
        for url, parent in zip(urls_to_crawl, parent_urls):
            if url not in visited_urls:
                new_urls.append(url)
                new_parents.append(parent)

        if not new_urls:
            continue

        print(f"\n{'='*70}")
        print(f"📊 Level {current_level}: Đang crawl {len(new_urls)} danh mục...")
        print(f"{'='*70}")

        # Crawl song song
        results = crawl_level_parallel(
            new_urls, new_parents, current_level, max_level, visited_urls, max_workers=max_workers
        )

        # Xử lý kết quả và chuẩn bị level tiếp theo
        for url, (success, categories, error) in results.items():
            if success:
                # Thêm vào danh sách tổng
                all_categories.extend(categories)

                # Thêm các danh mục con vào queue level tiếp theo
                if current_level < max_level:
                    for cat in categories:
                        child_url = cat["url"]
                        if child_url not in visited_urls:
                            queue[current_level + 1].append((child_url, url))
            else:
                print(f"  ❌ Lỗi crawl {url}: {error}")

    return all_categories


def print_stats():
    """In thống kê real-time"""
    global stats
    with stats_lock:
        elapsed = time.time() - stats["start_time"]
        rate = stats["total_crawled"] / elapsed if elapsed > 0 else 0

        print(f"\n{'='*70}")
        print("📈 THỐNG KÊ")
        print(f"{'='*70}")
        print(f"⏱  Thời gian: {elapsed:.1f}s")
        print(f"📥 Đã crawl: {stats['total_crawled']} danh mục")
        print(f"✅ Thành công: {stats['total_success']}")
        print(f"❌ Thất bại: {stats['total_failed']}")
        print(f"📊 Tổng danh mục tìm được: {stats['total_categories']}")
        print(f"⚡ Tốc độ: {rate:.2f} danh mục/s")

        if stats["by_level"]:
            print(f"\n📋 Theo level:")
            for level in sorted(stats["by_level"].keys()):
                print(f"  Level {level}: {stats['by_level'][level]} danh mục")


def main():
    """Hàm main để crawl đệ quy với tối ưu"""

    # URL danh mục gốc
    root_url = "https://tiki.vn/nha-cua-doi-song/c1883"

    # Độ sâu tối đa
    max_level = 3

    # Số thread song song (giới hạn để tránh quá tải server)
    max_workers = 3

    print("=" * 70)
    print("🚀 CRAWL ĐỆ QUY CÁC DANH MỤC TIKI (TỐI ƯU)")
    print("=" * 70)
    print(f"URL gốc: {root_url}")
    print(f"Độ sâu tối đa: {max_level}")
    print(f"Số thread song song: {max_workers}")
    print(f"Cache: data/raw/cache/")
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
        root_url, max_level=max_level, max_workers=max_workers
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
        print(f"\n📋 Thống kê theo level:")
        for level in sorted(level_counts.keys()):
            print(f"  Level {level}: {level_counts[level]} danh mục")


if __name__ == "__main__":
    main()
