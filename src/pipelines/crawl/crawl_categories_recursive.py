# Import từ file cùng thư mục
import importlib.util
import json
import os
import sys

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
    import io

    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# Tạo thư mục output nếu chưa có
os.makedirs("data/raw", exist_ok=True)


def crawl_category_recursive(
    url, parent_url=None, level=0, max_level=3, visited_urls=None, all_categories=None
):
    """
    Crawl đệ quy các danh mục con từ một URL danh mục

    Args:
        url: URL danh mục cần crawl
        parent_url: URL danh mục cha (None nếu là danh mục gốc)
        level: Độ sâu hiện tại (0 là gốc)
        max_level: Độ sâu tối đa để crawl
        visited_urls: Set các URL đã crawl để tránh trùng lặp
        all_categories: List tất cả các danh mục đã crawl
    """
    if visited_urls is None:
        visited_urls = set()
    if all_categories is None:
        all_categories = []

    # Kiểm tra độ sâu
    if level >= max_level:
        print(f"[Recursive] Đã đạt độ sâu tối đa ({max_level}) cho {url}")
        return all_categories

    # Kiểm tra đã crawl chưa
    if url in visited_urls:
        print(f"[Recursive] Đã crawl {url}, bỏ qua")
        return all_categories

    # Đánh dấu đã crawl
    visited_urls.add(url)

    print(f"\n{'  ' * level}[Level {level}] Đang crawl: {url}")
    if parent_url:
        print(f"{'  ' * level}  Parent: {parent_url}")

    try:
        # Crawl với Selenium
        html_content = crawl_with_selenium(url)

        # Parse danh mục con
        child_categories = parse_categories(html_content, parent_url=url, level=level + 1)

        # Lọc chỉ lấy các danh mục có hình ảnh
        categories_with_images = []
        for cat in child_categories:
            if cat.get("image_url", "").strip():
                categories_with_images.append(cat)

        print(
            f"{'  ' * level}[Level {level}] Tìm thấy {len(categories_with_images)} danh mục con có hình ảnh"
        )

        # Thêm vào danh sách tổng
        all_categories.extend(categories_with_images)

        # Đệ quy crawl các danh mục con
        for category in categories_with_images:
            child_url = category["url"]
            crawl_category_recursive(
                child_url,
                parent_url=url,
                level=level + 1,
                max_level=max_level,
                visited_urls=visited_urls,
                all_categories=all_categories,
            )

    except Exception as e:
        print(f"{'  ' * level}[Level {level}] ❌ Lỗi khi crawl {url}: {e}")

    return all_categories


def main():
    """Hàm main để crawl đệ quy từ danh mục gốc"""

    # URL danh mục gốc
    root_url = "https://tiki.vn/nha-cua-doi-song/c1883"

    # Độ sâu tối đa (0 = chỉ crawl gốc, 1 = crawl gốc + con, 2 = crawl gốc + con + cháu, ...)
    max_level = 3

    print("=" * 70)
    print("CRAWL ĐỆ QUY CÁC DANH MỤC TIKI")
    print("=" * 70)
    print(f"URL gốc: {root_url}")
    print(f"Độ sâu tối đa: {max_level}")
    print("=" * 70)

    # Crawl đệ quy
    all_categories = crawl_category_recursive(
        root_url, parent_url=None, level=0, max_level=max_level
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
    output_file = "data/raw/categories_recursive.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(unique_categories, f, ensure_ascii=False, indent=2)

    print("\n" + "=" * 70)
    print("✓ HOÀN THÀNH!")
    print("=" * 70)
    print(f"Tổng số danh mục đã crawl: {len(unique_categories)}")
    print(f"Đã lưu vào: {output_file}")

    # Thống kê theo level
    print("\nThống kê theo level:")
    level_counts = {}
    for cat in unique_categories:
        level = cat.get("level", 0)
        level_counts[level] = level_counts.get(level, 0) + 1

    for level in sorted(level_counts.keys()):
        print(f"  Level {level}: {level_counts[level]} danh mục")

    # In vài danh mục đầu
    print(f"\nVài danh mục đầu tiên:")
    for category in unique_categories[:10]:
        indent = "  " * category.get("level", 0)
        level = category.get("level", 0)
        print(f"{indent}- [Level {level}] {category['name']}: {category['url']}")


if __name__ == "__main__":
    main()
