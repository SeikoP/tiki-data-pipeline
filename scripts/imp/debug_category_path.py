#!/usr/bin/env python3
"""
Script để phân tích và debug category_path trong database.
"""

import json
import os
import sys
from pathlib import Path

# Fix encoding cho Windows
if sys.platform == "win32":
    import codecs

    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, "strict")
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, "strict")

# Thêm src vào path
project_root = Path(__file__).parent.parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    print("❌ Cần cài đặt psycopg2: pip install psycopg2-binary")
    sys.exit(1)


def get_db_connection():
    """
    Kết nối đến database.
    """
    # Thử localhost trước (cho Windows), sau đó mới dùng POSTGRES_HOST
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    if db_host == "postgres":
        # Nếu là Docker hostname, thử localhost
        db_host = "localhost"

    db_port = int(os.getenv("POSTGRES_PORT", "5432"))
    db_name = os.getenv("POSTGRES_DB", "tiki")
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")

    if not db_user or not db_password:
        print("❌ POSTGRES_USER và POSTGRES_PASSWORD phải được set trong environment")
        return None

    print(f"🔗 Đang kết nối: host={db_host}, port={db_port}, db={db_name}, user={db_user}")

    try:
        conn = psycopg2.connect(
            host=db_host, port=db_port, database=db_name, user=db_user, password=db_password
        )
        return conn
    except Exception as e:
        print(f"❌ Lỗi kết nối database: {e}")
        return None


def analyze_categories(conn):
    """
    Phân tích categories trong database.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # 1. Lấy tất cả categories
        cur.execute("""
            SELECT
                id,
                category_id,
                name,
                url,
                parent_url,
                level,
                category_path,
                level_1,
                level_2,
                level_3,
                level_4,
                level_5,
                root_category_name,
                is_leaf,
                product_count,
                created_at,
                updated_at
            FROM categories
            ORDER BY created_at DESC
        """)

        categories = cur.fetchall()

        print("=" * 80)
        print(f"📊 TỔNG QUAN: Tìm thấy {len(categories)} categories")
        print("=" * 80)

        # 2. Phân tích từng category
        issues = []
        for cat in categories:
            cat.get("category_id", "N/A")
            cat.get("name", "N/A")
            url = cat.get("url", "N/A")
            parent_url = cat.get("parent_url")
            level = cat.get("level", 0)
            category_path = cat.get("category_path", [])
            level_1 = cat.get("level_1")
            level_2 = cat.get("level_2")
            level_3 = cat.get("level_3")
            level_4 = cat.get("level_4")
            level_5 = cat.get("level_5")

            # Kiểm tra vấn đề
            issues_found = []

            # Vấn đề 1: category_path rỗng hoặc None
            if not category_path or len(category_path) == 0:
                issues_found.append("❌ category_path rỗng hoặc None")

            # Vấn đề 2: category_path không khớp với level
            if category_path and len(category_path) != level:
                issues_found.append(
                    f"⚠️  category_path có {len(category_path)} phần tử nhưng level={level}"
                )

            # Vấn đề 3: level_1 đến level_5 không khớp với category_path
            path_levels = [
                category_path[0] if len(category_path) > 0 else None,
                category_path[1] if len(category_path) > 1 else None,
                category_path[2] if len(category_path) > 2 else None,
                category_path[3] if len(category_path) > 3 else None,
                category_path[4] if len(category_path) > 4 else None,
            ]

            if path_levels[0] != level_1:
                issues_found.append(f"⚠️  level_1 không khớp: path={path_levels[0]}, DB={level_1}")
            if path_levels[1] != level_2:
                issues_found.append(f"⚠️  level_2 không khớp: path={path_levels[1]}, DB={level_2}")
            if path_levels[2] != level_3:
                issues_found.append(f"⚠️  level_3 không khớp: path={path_levels[2]}, DB={level_3}")
            if path_levels[3] != level_4:
                issues_found.append(f"⚠️  level_4 không khớp: path={path_levels[3]}, DB={level_4}")
            if path_levels[4] != level_5:
                issues_found.append(f"⚠️  level_5 không khớp: path={path_levels[4]}, DB={level_5}")

            # Vấn đề 4: Có parent_url nhưng parent không có trong DB
            if parent_url:
                cur.execute(
                    "SELECT COUNT(*) as count FROM categories WHERE url = %s", (parent_url,)
                )
                result = cur.fetchone()
                parent_exists = result["count"] > 0 if result else False
                if not parent_exists:
                    issues_found.append(f"⚠️  parent_url={parent_url} không tồn tại trong DB")

            # Vấn đề 5: category_path không bắt đầu từ root
            if category_path and len(category_path) > 0:
                root_name = category_path[0]
                if root_name != level_1:
                    issues_found.append("⚠️  root_category_name không khớp với level_1")

            if issues_found:
                issues.append({"category": cat, "issues": issues_found})

        # 3. Hiển thị kết quả
        print("\n🔍 PHÂN TÍCH CHI TIẾT:\n")

        if not issues:
            print("✅ Không tìm thấy vấn đề nào!")
        else:
            print(f"⚠️  Tìm thấy {len(issues)} categories có vấn đề:\n")

            for i, issue_data in enumerate(issues, 1):
                cat = issue_data["category"]
                cat_issues = issue_data["issues"]

                print(f"\n{'=' * 80}")
                print(f"📌 Category #{i}: {cat.get('name', 'N/A')}")
                print(f"{'=' * 80}")
                print(f"  ID: {cat.get('id')}")
                print(f"  Category ID: {cat.get('category_id')}")
                print(f"  URL: {cat.get('url')}")
                print(f"  Parent URL: {cat.get('parent_url', 'None')}")
                print(f"  Level: {cat.get('level')}")
                print(f"  Is Leaf: {cat.get('is_leaf')}")
                print(f"  Category Path: {cat.get('category_path')}")
                print(f"  Level 1: {cat.get('level_1')}")
                print(f"  Level 2: {cat.get('level_2')}")
                print(f"  Level 3: {cat.get('level_3')}")
                print(f"  Level 4: {cat.get('level_4')}")
                print(f"  Level 5: {cat.get('level_5')}")
                print(f"  Root Name: {cat.get('root_category_name')}")
                print("\n  ⚠️  VẤN ĐỀ:")
                for issue in cat_issues:
                    print(f"    {issue}")

        # 4. Phân tích parent chain
        print(f"\n{'=' * 80}")
        print("🔗 PHÂN TÍCH PARENT CHAIN:")
        print(f"{'=' * 80}\n")

        for cat in categories:
            url = cat.get("url")
            parent_url = cat.get("parent_url")
            category_path = cat.get("category_path", [])

            if parent_url:
                # Kiểm tra parent chain
                chain = []
                current_url = url
                visited = set()
                depth = 0

                while current_url and depth < 10:
                    if current_url in visited:
                        break
                    visited.add(current_url)

                    cur.execute(
                        "SELECT name, url, parent_url FROM categories WHERE url = %s",
                        (current_url,),
                    )
                    row = cur.fetchone()
                    if row:
                        chain.append(
                            {
                                "name": row["name"],
                                "url": row["url"],
                                "parent_url": row["parent_url"],
                            }
                        )
                        current_url = row["parent_url"]
                    else:
                        break
                    depth += 1

                print(f"📂 Category: {cat.get('name')}")
                print(f"   URL: {url}")
                print("   Parent Chain (từ DB):")
                for i, link in enumerate(chain, 1):
                    print(f"     {i}. {link['name']} ({link['url']})")
                print(f"   Category Path (từ DB): {category_path}")
                print(f"   Expected Path: {[link['name'] for link in reversed(chain)]}")

                if category_path != [link["name"] for link in reversed(chain)]:
                    print("   ❌ MISMATCH!")
                print()

        return issues


def fix_category_paths(conn, dry_run=True):
    """
    Sửa category_path cho các categories có vấn đề.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Lấy tất cả categories
        cur.execute("SELECT id, url, parent_url, name FROM categories")
        categories = cur.fetchall()

        # Build URL -> category map
        url_to_cat = {cat["url"]: cat for cat in categories}

        fixed_count = 0

        for cat in categories:
            url = cat["url"]
            parent_url = cat.get("parent_url")

            # Build path đệ quy
            path = []
            current = cat
            visited = set()
            depth = 0

            while current and depth < 10:
                if current["url"] in visited:
                    break
                visited.add(current["url"])

                name = current.get("name", "")
                if name:
                    path.insert(0, name)

                parent_url = current.get("parent_url")
                if not parent_url:
                    break

                # Tìm parent
                if parent_url in url_to_cat:
                    current = url_to_cat[parent_url]
                else:
                    # Query từ DB
                    cur.execute(
                        "SELECT name, url, parent_url FROM categories WHERE url = %s", (parent_url,)
                    )
                    row = cur.fetchone()
                    if row:
                        current = {"name": row[0], "url": row[1], "parent_url": row[2]}
                        url_to_cat[row[1]] = current
                    else:
                        break
                depth += 1

            # Update category_path
            if path:
                level_1 = path[0] if len(path) > 0 else None
                level_2 = path[1] if len(path) > 1 else None
                level_3 = path[2] if len(path) > 2 else None
                level_4 = path[3] if len(path) > 3 else None
                level_5 = path[4] if len(path) > 4 else None
                calculated_level = len(path)
                root_name = path[0] if path else None

                if dry_run:
                    print(f"🔧 Sẽ sửa category: {cat['name']}")
                    print(f"   Old path: {cat.get('category_path', [])}")
                    print(f"   New path: {path}")
                else:
                    cur.execute(
                        """
                        UPDATE categories
                        SET
                            category_path = %s,
                            level_1 = %s,
                            level_2 = %s,
                            level_3 = %s,
                            level_4 = %s,
                            level_5 = %s,
                            level = %s,
                            root_category_name = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE url = %s
                    """,
                        (
                            json.dumps(path, ensure_ascii=False),
                            level_1,
                            level_2,
                            level_3,
                            level_4,
                            level_5,
                            calculated_level,
                            root_name,
                            url,
                        ),
                    )
                    fixed_count += 1

        if not dry_run:
            conn.commit()
            print(f"\n✅ Đã sửa {fixed_count} categories")
        else:
            print(f"\n⚠️  DRY RUN: Sẽ sửa {len(categories)} categories khi chạy với --fix")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Phân tích và sửa category_path trong database")
    parser.add_argument("--fix", action="store_true", help="Sửa các category_path có vấn đề")
    args = parser.parse_args()

    print("🔍 Đang kết nối đến database...")
    conn = get_db_connection()

    if not conn:
        print("❌ Không thể kết nối đến database")
        sys.exit(1)

    try:
        print("✅ Đã kết nối thành công\n")

        # Phân tích
        analyze_categories(conn)

        # Sửa nếu cần
        if args.fix:
            print(f"\n{'=' * 80}")
            print("🔧 BẮT ĐẦU SỬA LỖI...")
            print(f"{'=' * 80}\n")
            fix_category_paths(conn, dry_run=False)
        else:
            print("\n💡 Để sửa tự động, chạy: python scripts/debug_category_path.py --fix")

    finally:
        conn.close()
        print("\n✅ Đã đóng kết nối database")


if __name__ == "__main__":
    main()
