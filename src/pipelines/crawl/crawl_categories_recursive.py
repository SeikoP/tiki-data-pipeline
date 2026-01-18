# Import từ file cùng thư mục
import importlib.util
import json
import os
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

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
    sys.stdout.reconfigure(encoding="utf-8")

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


def crawl_single_category(url, parent_url, level, max_level, visited_urls):
    """
    Crawl một danh mục đơn lẻ (thread-safe)
    
    Returns:
        tuple: (success: bool, categories: list, error: str)
    """
    global stats
    
    try:
        # Crawl với Selenium
        html_content = crawl_with_selenium(url, save_html=False, verbose=False)
        
        # Parse danh mục con
        child_categories = parse_categories(html_content, parent_url=url, level=level + 1)
        
        # Lọc chỉ lấy các danh mục có hình ảnh
        categories_with_images = [
            cat for cat in child_categories if cat.get("image_url", "").strip()
        ]
        
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
        max_workers: Số thread tối đa
    
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
    
    print(f"\n{'='*70}")
    print(f"📊 Level {level}: Đang crawl {len(tasks)} danh mục...")
    print(f"{'='*70}")
    
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
            )
            future_to_url[future] = (url, parent_url)
        
        # Xử lý kết quả khi hoàn thành
        completed = 0
        for future in as_completed(future_to_url):
            url, parent_url = future_to_url[future]
            try:
                success, categories, error = future.result(timeout=300)  # Timeout 5 phút
                results[url] = (success, categories, error)
                
                # Đánh dấu đã crawl
                visited_urls.add(url)
                
                completed += 1
                if success:
                    print(f"  ✅ [{completed}/{len(tasks)}] {url}: {len(categories)} danh mục con")
                else:
                    print(f"  ❌ [{completed}/{len(tasks)}] {url}: Lỗi - {error}")
            except Exception as e:
                error_msg = str(e)
                results[url] = (False, [], error_msg)
                visited_urls.add(url)
                with stats_lock:
                    stats["total_failed"] += 1
                completed += 1
                print(f"  ❌ [{completed}/{len(tasks)}] {url}: Exception - {error_msg}")
    
    return results


def crawl_category_recursive_optimized(
    root_url, max_level=4, max_workers=3, visited_urls=None, all_categories=None
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
        
        # Crawl song song
        results = crawl_level_parallel(
            new_urls,
            new_parents,
            current_level,
            max_level,
            visited_urls,
            max_workers=max_workers,
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
            print("\n📋 Theo level:")
            for level in sorted(stats["by_level"].keys()):
                print(f"  Level {level}: {stats['by_level'][level]} danh mục")


def load_env_file():
    """Load biến môi trường từ file .env ở root project"""
    try:
        # Tìm file .env: Từ file này (src/pipelines/crawl/...) ra root
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
        env_path = os.path.join(project_root, '.env')
        
        if os.path.exists(env_path):
            print(f"📄 Loading config from {env_path}")
            with open(env_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, val = line.split('=', 1)
                        if key not in os.environ: # Không override nếu đã set
                            os.environ[key] = val
    except Exception as e:
        print(f"⚠️  Could not load .env file: {e}")

def main():
    """Hàm main để crawl đệ quy với tối ưu"""
    
    # Load env file first
    load_env_file()
    
    # URL danh mục gốc từ Env Var
    default_url = "https://tiki.vn/nha-cua-doi-song/c1883"
    root_url = os.getenv("CRAWL_ROOT_CATEGORY_URL", default_url)
    
    # Độ sâu tối đa (tăng lên 4 để bao quát hết)
    max_level = 4
    
    # Số thread song song (tăng lên 5 để crawl nhanh hơn)
    max_workers = 5
    
    print("=" * 70)
    print("🚀 CRAWL ĐỆ QUY CÁC DANH MỤC TIKI (TỐI ƯU)")
    print("=" * 70)
    print(f"URL gốc: {root_url}")
    print(f"Độ sâu tối đa: {max_level}")
    print(f"Số thread song song: {max_workers}")
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
    
    # Lưu kết quả vào file mà DAG sử dụng
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
