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

# Tạo thư mục output nếu chưa có
os.makedirs("data/raw", exist_ok=True)


def crawl_category_recursive(
    url,
    parent_url=None,
    level=0,
    max_level=2,
    max_categories_per_level=3,
    visited_urls=None,
    all_categories=None,
):
    """
    Crawl đệ quy các danh mục con từ một URL danh mục (phiên bản test với giới hạn)

    Args:
        url: URL danh mục cần crawl
        parent_url: URL danh mục cha (None nếu là danh mục gốc)
        level: Độ sâu hiện tại (0 là gốc)
        max_level: Độ sâu tối đa để crawl
        max_categories_per_level: Số danh mục tối đa crawl ở mỗi level (để test nhanh)
        visited_urls: Set các URL đã crawl để tránh trùng lặp
        all_categories: List tất cả các danh mục đã crawl
    """
    if visited_urls is None:
        visited_urls = set()
    if all_categories is None:
        all_categories = []

    # Kiểm tra độ sâu
    if level >= max_level:
        print(f"{'  ' * level}[Level {level}] ⏹ Đã đạt độ sâu tối đa ({max_level}) cho {url}")
        return all_categories

    # Kiểm tra đã crawl chưa
    if url in visited_urls:
        print(f"{'  ' * level}[Level {level}] ⏭ Đã crawl {url}, bỏ qua")
        return all_categories

    # Đánh dấu đã crawl
    visited_urls.add(url)

    print(f"\n{'  ' * level}{'=' * 60}")
    print(f"{'  ' * level}[Level {level}] 🔍 Đang crawl: {url}")
    if parent_url:
        print(f"{'  ' * level}  📁 Parent: {parent_url}")
    print(f"{'  ' * level}{'=' * 60}")

    try:
        # Crawl với Selenium
        html_content = crawl_with_selenium(url)

        # Parse danh mục con
        child_categories = parse_categories(html_content, parent_url=url, level=level + 1)

        # Lọc chỉ lấy các danh mục có hình ảnh
        categories_with_images = [
            cat for cat in child_categories if cat.get("image_url", "").strip()
        ]

        # Giới hạn số lượng danh mục để test nhanh
        if level < max_level - 1:  # Không giới hạn ở level cuối
            categories_with_images = categories_with_images[:max_categories_per_level]
            if len(categories_with_images) < len(
                [c for c in child_categories if c.get("image_url", "").strip()]
            ):
                print(
                    f"{'  ' * level}  ⚠ Giới hạn chỉ crawl {max_categories_per_level} danh mục đầu tiên (để test)"
                )

        print(
            f"{'  ' * level}[Level {level}] ✅ Tìm thấy {len(categories_with_images)} danh mục con có hình ảnh"
        )

        # In danh sách danh mục con
        for i, cat in enumerate(categories_with_images, 1):
            print(f"{'  ' * level}  {i}. {cat['name']} - {cat['url']}")

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
                max_categories_per_level=max_categories_per_level,
                visited_urls=visited_urls,
                all_categories=all_categories,
            )

    except Exception as e:
        print(f"{'  ' * level}[Level {level}] ❌ Lỗi khi crawl {url}: {e}")
        import traceback

        traceback.print_exc()

    return all_categories


def main():
    """Hàm main để test crawl đệ quy với giới hạn"""

    # URL danh mục gốc
    root_url = "https://tiki.vn/nha-cua-doi-song/c1883"

    # Độ sâu tối đa (0 = chỉ crawl gốc, 1 = crawl gốc + con, 2 = crawl gốc + con + cháu)
    max_level = 2

    # Số danh mục tối đa crawl ở mỗi level (để test nhanh)
    max_categories_per_level = 2

    print("=" * 70)
    print("🧪 TEST CRAWL ĐỆ QUY CÁC DANH MỤC TIKI")
    print("=" * 70)
    print(f"URL gốc: {root_url}")
    print(f"Độ sâu tối đa: {max_level}")
    print(f"Số danh mục tối đa mỗi level: {max_categories_per_level}")
    print("=" * 70)
    print("⚠️  Đây là phiên bản TEST với giới hạn để chạy nhanh")
    print("=" * 70)

    # Crawl đệ quy
    all_categories = crawl_category_recursive(
        root_url,
        parent_url=None,
        level=0,
        max_level=max_level,
        max_categories_per_level=max_categories_per_level,
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
    output_file = "data/raw/categories_test.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(unique_categories, f, ensure_ascii=False, indent=2)

    print("\n" + "=" * 70)
    print("✅ HOÀN THÀNH TEST!")
    print("=" * 70)
    print(f"📊 Tổng số danh mục đã crawl: {len(unique_categories)}")
    print(f"💾 Đã lưu vào: {output_file}")

    # Thống kê theo level
    print("\n📈 Thống kê theo level:")
    level_counts = {}
    for cat in unique_categories:
        level = cat.get("level", 0)
        level_counts[level] = level_counts.get(level, 0) + 1

    for level in sorted(level_counts.keys()):
        print(f"  Level {level}: {level_counts[level]} danh mục")

    # In cây danh mục
    print("\n🌳 Cây danh mục (mẫu):")
    for category in unique_categories[:15]:  # Chỉ in 15 danh mục đầu
        indent = "  " * category.get("level", 0)
        level = category.get("level", 0)
        parent_info = (
            f" (Parent: {category.get('parent_url', 'N/A')})" if category.get("parent_url") else ""
        )
        print(f"{indent}├─ [Level {level}] {category['name']}{parent_info}")
        print(f"{indent}   └─ {category['url']}")

    if len(unique_categories) > 15:
        print(f"\n  ... và {len(unique_categories) - 15} danh mục khác")


if __name__ == "__main__":
    main()
