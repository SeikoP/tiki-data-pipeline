import json
import sys
import os

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


def build_category_tree(categories):
    """
    Xây dựng cây phân cấp từ danh sách danh mục phẳng

    Args:
        categories: List các danh mục với parent_url và level

    Returns:
        dict: Cây phân cấp với cấu trúc {category: {children: [...]}}
    """
    # Tạo dictionary để tra cứu nhanh theo URL
    category_map = {}
    for cat in categories:
        category_map[cat["url"]] = cat.copy()
        category_map[cat["url"]]["children"] = []

    # Tìm root categories (không có parent_url hoặc parent_url không có trong danh sách)
    root_categories = []
    for cat in categories:
        parent_url = cat.get("parent_url")
        if not parent_url or parent_url not in category_map:
            root_categories.append(cat["url"])

    # Xây dựng cây: thêm children vào parent
    for cat in categories:
        parent_url = cat.get("parent_url")
        if parent_url and parent_url in category_map:
            # Thêm vào children của parent
            if "children" not in category_map[parent_url]:
                category_map[parent_url]["children"] = []
            category_map[parent_url]["children"].append(category_map[cat["url"]])

    # Sắp xếp children theo tên
    def sort_children(node):
        if "children" in node and node["children"]:
            node["children"].sort(key=lambda x: x.get("name", ""))
            for child in node["children"]:
                sort_children(child)

    # Tạo cây từ root categories
    tree = []
    for root_url in root_categories:
        root_node = category_map[root_url]
        sort_children(root_node)
        tree.append(root_node)

    # Sắp xếp root theo tên
    tree.sort(key=lambda x: x.get("name", ""))

    return tree


def print_tree(node, indent=0, max_depth=None, current_depth=0):
    """
    In cây phân cấp ra console (để debug/preview)

    Args:
        node: Node hiện tại
        indent: Số lượng space để indent
        max_depth: Độ sâu tối đa để in (None = in hết)
        current_depth: Độ sâu hiện tại
    """
    if max_depth is not None and current_depth >= max_depth:
        return

    prefix = "  " * indent
    name = node.get("name", "N/A")
    url = node.get("url", "")
    level = node.get("level", 0)
    children_count = len(node.get("children", []))

    print(f"{prefix}├─ {name} [Level {level}]")
    print(f"{prefix}│  └─ {url}")
    if children_count > 0:
        print(f"{prefix}│     ({children_count} danh mục con)")

    # In children
    children = node.get("children", [])
    for i, child in enumerate(children):
        is_last = i == len(children) - 1
        if is_last:
            print_tree(child, indent + 1, max_depth, current_depth + 1)
        else:
            print_tree(child, indent + 1, max_depth, current_depth + 1)


def get_tree_stats(tree):
    """
    Tính thống kê về cây phân cấp

    Returns:
        dict: Thống kê về số lượng nodes, độ sâu, etc.
    """

    def count_nodes(node):
        count = 1
        max_depth = node.get("level", 0)
        for child in node.get("children", []):
            child_count, child_depth = count_nodes(child)
            count += child_count
            max_depth = max(max_depth, child_depth)
        return count, max_depth

    total_nodes = 0
    max_depth = 0
    level_counts = {}

    for root in tree:
        count, depth = count_nodes(root)
        total_nodes += count
        max_depth = max(max_depth, depth)

        # Đếm theo level
        def count_by_level(node):
            level = node.get("level", 0)
            level_counts[level] = level_counts.get(level, 0) + 1
            for child in node.get("children", []):
                count_by_level(child)

        count_by_level(root)

    return {
        "total_nodes": total_nodes,
        "max_depth": max_depth,
        "level_counts": level_counts,
        "root_count": len(tree),
    }


def main():
    """Hàm main để build category tree"""

    input_file = "data/raw/categories_recursive_optimized.json"
    output_file = "data/raw/categories_tree.json"

    print("=" * 70)
    print("🌳 BUILD CATEGORY TREE")
    print("=" * 70)

    # Đọc dữ liệu từ file
    print(f"📖 Đang đọc: {input_file}")
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            categories = json.load(f)
        print(f"✓ Đã đọc {len(categories)} danh mục")
    except FileNotFoundError:
        print(f"❌ Không tìm thấy file: {input_file}")
        return
    except Exception as e:
        print(f"❌ Lỗi khi đọc file: {e}")
        return

    # Xây dựng cây phân cấp
    print(f"\n🔨 Đang xây dựng cây phân cấp...")
    tree = build_category_tree(categories)

    # Tính thống kê
    stats = get_tree_stats(tree)

    print(f"✓ Đã xây dựng cây với {stats['root_count']} root categories")
    print(f"✓ Tổng số nodes: {stats['total_nodes']}")
    print(f"✓ Độ sâu tối đa: {stats['max_depth']}")

    # In thống kê theo level
    print(f"\n📊 Thống kê theo level:")
    for level in sorted(stats["level_counts"].keys()):
        print(f"  Level {level}: {stats['level_counts'][level]} danh mục")

    # Lưu cây vào file
    print(f"\n💾 Đang lưu vào: {output_file}")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(tree, f, ensure_ascii=False, indent=2)
    print(f"✓ Đã lưu thành công!")

    # In preview cây (chỉ 3 level đầu)
    print(f"\n🌳 Preview cây phân cấp (3 level đầu):")
    print("=" * 70)
    for root in tree[:5]:  # Chỉ in 5 root đầu
        print_tree(root, max_depth=3)
        if tree.index(root) < len(tree) - 1 and tree.index(root) < 4:
            print()

    if len(tree) > 5:
        print(f"\n  ... và {len(tree) - 5} root categories khác")

    print("\n" + "=" * 70)
    print("✅ HOÀN THÀNH!")
    print("=" * 70)
    print(f"📁 File output: {output_file}")
    print(f"📊 Tổng số danh mục: {stats['total_nodes']}")
    print(f"🌲 Số root categories: {stats['root_count']}")


if __name__ == "__main__":
    main()
