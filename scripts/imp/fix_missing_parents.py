#!/usr/bin/env python3
"""
Script để load các parent categories còn thiếu vào DB
"""

import json
import os
import sys
from pathlib import Path

# Load .env file từ project root
try:
    from dotenv import load_dotenv
except ImportError:
    print("⚠️  Cảnh báo: python-dotenv không được cài đặt")
    print("   Cài đặt: pip install python-dotenv")

    def load_dotenv(*args, **kwargs):
        pass


# Fix encoding cho Windows
if sys.platform == "win32":
    import codecs

    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, "strict")
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, "strict")

# Thêm src vào path
# Script nằm trong scripts/imp/, nên cần lên 2 cấp để đến project root
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent.parent
src_path = project_root / "src"

# Load .env file từ project root
load_dotenv(project_root / ".env")

# Thêm vào path với nhiều fallback
paths_to_add = [
    str(src_path),
    str(project_root / "src"),
    str(Path(__file__).parent.parent.parent / "src"),
]

for path in paths_to_add:
    if os.path.exists(path) and path not in sys.path:
        sys.path.insert(0, path)

# Debug: In ra đường dẫn để kiểm tra
if os.getenv("DEBUG", "").lower() == "true":
    print(f"🔍 Debug paths:")
    print(f"   Script dir: {script_dir}")
    print(f"   Project root: {project_root}")
    print(f"   Src path: {src_path}")
    print(f"   Src exists: {src_path.exists()}")
    print(f"   Sys.path (first 3): {sys.path[:3]}")

try:
    from pipelines.crawl.storage.postgres_storage import PostgresStorage
except ImportError as e:
    print(f"❌ Lỗi import: {e}")
    print(f"   Đang thử tìm pipelines module...")
    # Thử tìm pipelines module
    for root, dirs, files in os.walk(project_root):
        if "pipelines" in dirs:
            pipelines_path = os.path.join(root, "pipelines")
            if os.path.exists(os.path.join(pipelines_path, "__init__.py")):
                parent_path = os.path.dirname(pipelines_path)
                if parent_path not in sys.path:
                    sys.path.insert(0, parent_path)
                    print(f"   ✅ Tìm thấy pipelines tại: {parent_path}")
                    break

    # Thử import lại
    try:
        from pipelines.crawl.storage.postgres_storage import PostgresStorage
    except ImportError:
        print(f"❌ Vẫn không thể import pipelines module")
        print(f"   Vui lòng chạy script từ project root hoặc đảm bảo src/ trong PYTHONPATH")
        sys.exit(1)

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    print("❌ Cần cài đặt psycopg2: pip install psycopg2-binary")
    sys.exit(1)


def get_db_connection():
    """Kết nối đến database"""
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    if db_host == "postgres":
        db_host = "localhost"

    db_port = int(os.getenv("POSTGRES_PORT", "5432"))
    db_name = os.getenv("POSTGRES_DB", "tiki")
    db_user = os.getenv("POSTGRES_USER", "")
    db_password = os.getenv("POSTGRES_PASSWORD", "")

    if not db_user or not db_password:
        print("❌ Lỗi: POSTGRES_USER và POSTGRES_PASSWORD phải được set trong environment")
        print("   Vui lòng tạo file .env từ .env.example và điền thông tin database")
        return None

    try:
        conn = psycopg2.connect(
            host=db_host, port=db_port, database=db_name, user=db_user, password=db_password
        )
        return conn
    except Exception as e:
        print(f"❌ Lỗi kết nối database: {e}")
        return None


def main():
    # Set environment variables for local connection
    if "POSTGRES_HOST" not in os.environ or os.environ["POSTGRES_HOST"] == "postgres":
        os.environ["POSTGRES_HOST"] = "localhost"

    # 1. Load file JSON
    json_files = [
        project_root / "data" / "raw" / "categories_recursive_optimized.json",
        project_root / "data" / "raw" / "categories_recursive.json",
        project_root / "data" / "raw" / "categories.json",
    ]

    json_file = None
    for f in json_files:
        if f.exists():
            json_file = f
            break

    if not json_file:
        print("❌ Không tìm thấy file categories JSON")
        return

    print(f"📂 Đang đọc file: {json_file}")
    with open(json_file, encoding="utf-8") as f:
        categories = json.load(f)

    url_to_cat = {cat.get("url"): cat for cat in categories}
    print(f"📊 Loaded {len(categories)} categories từ file JSON")

    # 2. Tìm các parent categories còn thiếu trong DB
    conn = get_db_connection()
    if not conn:
        return

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Lấy tất cả categories trong DB
            cur.execute("SELECT url, parent_url FROM categories")
            db_cats = cur.fetchall()
            db_urls = {cat["url"] for cat in db_cats}

            # Tìm các parent URLs cần thiết
            missing_parents = set()
            for db_cat in db_cats:
                parent_url = db_cat.get("parent_url")
                if parent_url and parent_url not in db_urls and parent_url in url_to_cat:
                    missing_parents.add(parent_url)

            print(f"\n🔍 Tìm thấy {len(missing_parents)} parent categories còn thiếu:")
            for url in missing_parents:
                cat = url_to_cat[url]
                print(f"   - [{cat.get('level', '?')}] {cat.get('name')} ({url})")

            if not missing_parents:
                print("✅ Không có parent categories nào còn thiếu!")
                return

            # 3. Load các parent categories còn thiếu trực tiếp vào DB
            print(f"\n💾 Đang load {len(missing_parents)} parent categories vào DB...")

            import re

            def normalize_category_id(cat_id):
                if not cat_id:
                    return None
                if isinstance(cat_id, int):
                    return f"c{cat_id}"
                cat_id_str = str(cat_id).strip()
                if cat_id_str.startswith("c"):
                    return cat_id_str
                return f"c{cat_id_str}"

            saved_count = 0
            for url in missing_parents:
                cat = url_to_cat[url]

                # Extract category_id
                cat_id = cat.get("category_id")
                if not cat_id and url:
                    match = re.search(r"c?(\d+)", url)
                    if match:
                        cat_id = match.group(1)
                cat_id = normalize_category_id(cat_id)

                # Build parent chain để có category_path
                path = []
                current = cat
                visited = set()
                depth = 0
                while current and depth < 10:
                    if current.get("url") in visited:
                        break
                    visited.add(current.get("url"))
                    name = current.get("name", "")
                    if name:
                        path.insert(0, name)
                    parent_url = current.get("parent_url")
                    if not parent_url:
                        break
                    if parent_url in url_to_cat:
                        current = url_to_cat[parent_url]
                    elif parent_url in db_urls:
                        # Query từ DB
                        cur.execute(
                            "SELECT name, url, parent_url FROM categories WHERE url = %s",
                            (parent_url,),
                        )
                        row = cur.fetchone()
                        if row:
                            current = {
                                "name": row["name"],
                                "url": row["url"],
                                "parent_url": row["parent_url"],
                            }
                        else:
                            break
                    else:
                        break
                    depth += 1

                # Insert vào DB
                level_1 = path[0] if len(path) > 0 else None
                level_2 = path[1] if len(path) > 1 else None
                level_3 = path[2] if len(path) > 2 else None
                level_4 = path[3] if len(path) > 3 else None
                level_5 = path[4] if len(path) > 4 else None
                calculated_level = len(path) if path else 0
                root_name = path[0] if path else None

                # Check if leaf (không có children)
                parent_urls_in_db = {c.get("parent_url") for c in db_cats if c.get("parent_url")}
                is_leaf = url not in parent_urls_in_db

                try:
                    cur.execute(
                        """
                        INSERT INTO categories (
                            category_id, name, url, image_url, parent_url, level,
                            category_path, level_1, level_2, level_3, level_4, level_5,
                            root_category_name, is_leaf
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (url) DO UPDATE SET
                            category_path = EXCLUDED.category_path,
                            level_1 = EXCLUDED.level_1,
                            level_2 = EXCLUDED.level_2,
                            level_3 = EXCLUDED.level_3,
                            level_4 = EXCLUDED.level_4,
                            level_5 = EXCLUDED.level_5,
                            level = EXCLUDED.level,
                            root_category_name = EXCLUDED.root_category_name,
                            updated_at = CURRENT_TIMESTAMP
                    """,
                        (
                            cat_id,
                            cat.get("name"),
                            url,
                            cat.get("image_url"),
                            cat.get("parent_url"),
                            calculated_level,
                            json.dumps(path, ensure_ascii=False),
                            level_1,
                            level_2,
                            level_3,
                            level_4,
                            level_5,
                            root_name,
                            is_leaf,
                        ),
                    )
                    saved_count += 1
                    print(f"   ✅ Đã load: {cat.get('name')}")
                except Exception as e:
                    print(f"   ❌ Lỗi khi load {cat.get('name')}: {e}")

            conn.commit()
            print(f"\n✅ Đã load {saved_count} parent categories vào DB")

            # 4. Rebuild category_path cho tất cả categories có parent_url trỏ đến parent vừa load
            print(f"\n🔧 Đang rebuild category_path cho các categories liên quan...")

            # Reload categories từ DB để rebuild paths
            cur.execute("SELECT url FROM categories")
            all_db_urls = [row["url"] for row in cur.fetchall()]

            categories_to_rebuild = []
            for url in all_db_urls:
                if url in url_to_cat:
                    categories_to_rebuild.append(url_to_cat[url])

            if categories_to_rebuild:
                # Use PostgresStorage với connection parameters
                storage = PostgresStorage(
                    host=os.getenv("POSTGRES_HOST", "localhost"),
                    port=int(os.getenv("POSTGRES_PORT", "5432")),
                    database=os.getenv("POSTGRES_DB", "tiki"),
                    user=os.getenv("POSTGRES_USER", ""),
                    password=os.getenv("POSTGRES_PASSWORD", ""),
                )
                saved_count = storage.save_categories(
                    categories_to_rebuild, only_leaf=False, sync_with_products=False
                )
                print(f"✅ Đã rebuild {saved_count} categories")
                storage.close()

    finally:
        conn.close()


if __name__ == "__main__":
    main()
