"""
Script E2E để load dữ liệu đã crawl trước đó vào database
- Extract và load categories từ categories_tree.json
- Load products từ products.json
- Đảm bảo liên kết giữa products và categories
"""

import json
import os
import sys
from pathlib import Path
from typing import Any

# Thêm src vào path
# File này ở scripts/helper/, nên cần lên 2 cấp để đến project root
project_root = Path(__file__).parent.parent.parent
src_path = project_root / "src"

# Kiểm tra src_path có tồn tại không
if not src_path.exists():
    # Thử cách khác: tìm project root bằng cách tìm thư mục có src/
    current = Path(__file__).parent
    found = False
    while current != current.parent:  # Dừng khi đến root của filesystem
        if (current / "src").exists():
            project_root = current
            src_path = project_root / "src"
            found = True
            break
        current = current.parent

    if not found:
        raise FileNotFoundError(f"Không tìm thấy thư mục src. Đã thử: {src_path}")

sys.path.insert(0, str(src_path))

from pipelines.extract.extract_categories import extract_categories_from_tree_file
from pipelines.load.loader import DataLoader
from pipelines.transform.transformer import DataTransformer


def load_categories_e2e(loader: DataLoader, tree_file: Path) -> dict[str, Any]:
    """Load categories từ categories_tree.json vào database"""
    print("\n" + "=" * 70)
    print("📁 BƯỚC 1: EXTRACT & LOAD CATEGORIES")
    print("=" * 70)

    if not tree_file.exists():
        print(f"⚠️  Không tìm thấy file: {tree_file}")
        print("   Bỏ qua bước này...")
        return {"skipped": True, "total_loaded": 0, "db_loaded": 0}

    try:
        # Extract categories
        print(f"📖 Đang extract categories từ: {tree_file}")
        categories = extract_categories_from_tree_file(tree_file)
        print(f"✅ Đã extract {len(categories)} categories")

        # Load vào database
        print("💾 Đang load categories vào database...")
        stats = loader.load_categories(
            categories,
            save_to_file=None,
            upsert=True,
            validate_before_load=True,
        )

        print(f"✅ Đã load {stats['db_loaded']} categories vào database")
        print(f"   - Tổng số: {stats['total_loaded']}")
        print(f"   - Thành công: {stats['success_count']}")
        print(f"   - Thất bại: {stats['failed_count']}")

        if stats.get("errors"):
            print(f"⚠️  Có {len(stats['errors'])} lỗi (hiển thị 5 đầu tiên):")
            for error in stats["errors"][:5]:
                print(f"   - {error}")

        return stats

    except Exception as e:
        print(f"❌ Lỗi khi load categories: {e}")
        import traceback

        traceback.print_exc()
        return {"error": str(e), "total_loaded": 0, "db_loaded": 0}


def load_products_from_cache(cache_dir: Path) -> dict[str, dict[str, Any]]:
    """Load products từ cache folder (detail/cache)"""
    cache_products = {}

    if not cache_dir.exists():
        return cache_products

    print(f"📂 Đang quét cache folder: {cache_dir}")
    cache_files = list(cache_dir.glob("*.json"))
    print(f"   Tìm thấy {len(cache_files)} file cache")

    loaded_count = 0
    error_count = 0

    for cache_file in cache_files:
        try:
            with open(cache_file, encoding="utf-8") as f:
                product_detail = json.load(f)

            product_id = product_detail.get("product_id")
            if not product_id:
                # Thử extract từ tên file
                product_id = cache_file.stem

            if product_id:
                cache_products[product_id] = product_detail
                loaded_count += 1
        except Exception as e:
            error_count += 1
            if error_count <= 5:  # Chỉ log 5 lỗi đầu tiên
                print(f"   ⚠️  Lỗi khi đọc {cache_file.name}: {e}")

    print(f"✅ Đã load {loaded_count} products từ cache")
    if error_count > 5:
        print(f"   ⚠️  Có thêm {error_count - 5} lỗi khác")

    return cache_products


def load_products_e2e(
    loader: DataLoader,
    cache_dir: Path | None = None,
    products_with_detail_file: Path | None = None,
    products_file: Path | None = None,
) -> dict[str, Any]:
    """Load products có detail đầy đủ vào database (từ cache hoặc products_with_detail.json)"""
    print("\n" + "=" * 70)
    print("📦 BƯỚC 2: LOAD PRODUCTS (CHỈ LOAD DỮ LIỆU CÓ DETAIL)")
    print("=" * 70)

    # Bước 0: Load category_url mapping từ products.json (nếu có) - để bổ sung category_url
    category_url_mapping = {}  # product_id -> category_url
    if products_file and products_file.exists():
        print(f"📖 Đang đọc category_url mapping từ: {products_file}")
        try:
            with open(products_file, encoding="utf-8") as f:
                data = json.load(f)

            products_list = []
            if isinstance(data, list):
                products_list = data
            elif isinstance(data, dict):
                if "products" in data:
                    products_list = data["products"]
                elif "data" in data and isinstance(data["data"], dict):
                    products_list = data["data"].get("products", [])

            for product in products_list:
                product_id = product.get("product_id")
                category_url = product.get("category_url")
                if product_id and category_url:
                    category_url_mapping[product_id] = category_url

            print(f"✅ Đã load {len(category_url_mapping)} category_url mappings từ products.json")
        except Exception as e:
            print(f"⚠️  Lỗi khi đọc products.json: {e}")

    # Bước 1: Load từ cache folder (nếu có) - đầy đủ nhất
    cache_products = {}
    if cache_dir and cache_dir.exists():
        cache_products = load_products_from_cache(cache_dir)

    # Bước 2: Load từ products_with_detail.json (nếu có) - đầy đủ
    products_with_detail = []
    if products_with_detail_file and products_with_detail_file.exists():
        print(f"📖 Đang đọc products_with_detail từ: {products_with_detail_file}")
        try:
            with open(products_with_detail_file, encoding="utf-8") as f:
                data = json.load(f)

            if isinstance(data, list):
                products_with_detail = data
            elif isinstance(data, dict) and "products" in data:
                products_with_detail = data["products"]

            print(f"✅ Đã đọc {len(products_with_detail)} products từ products_with_detail.json")
        except Exception as e:
            print(f"⚠️  Lỗi khi đọc products_with_detail.json: {e}")

    # Bước 3: Merge và loại bỏ duplicate (ưu tiên cache > products_with_detail)
    print("\n🔄 Đang merge và loại bỏ duplicate...")
    merged_products = {}
    duplicate_count = 0
    seen_in_detail = set()  # Đếm products unique từ products_with_detail

    # Ưu tiên 1: Cache (đầy đủ nhất) - mỗi file cache là unique product_id
    for product_id, product in cache_products.items():
        # Bổ sung category_url từ mapping nếu chưa có
        if not product.get("category_url") and product_id in category_url_mapping:
            product["category_url"] = category_url_mapping[product_id]
        merged_products[product_id] = product

    # Ưu tiên 2: products_with_detail (nếu chưa có trong cache)
    # Loại bỏ duplicate trong cùng list products_with_detail
    for product in products_with_detail:
        product_id = product.get("product_id")
        if not product_id:
            continue

        # Nếu đã có trong cache, bỏ qua
        if product_id in merged_products:
            duplicate_count += 1
            continue

        # Nếu đã thấy trong products_with_detail list, bỏ qua (duplicate trong cùng file)
        if product_id in seen_in_detail:
            duplicate_count += 1
            continue

        seen_in_detail.add(product_id)
        # Bổ sung category_url từ mapping nếu chưa có
        if not product.get("category_url") and product_id in category_url_mapping:
            product["category_url"] = category_url_mapping[product_id]
        merged_products[product_id] = product

    products = list(merged_products.values())

    if duplicate_count > 0:
        print(f"   ⚠️  Đã loại bỏ {duplicate_count} products duplicate")

    if not products:
        print("⚠️  Không tìm thấy products nào có detail để load")
        print("   💡 Lưu ý: products.json chỉ chứa danh sách cơ bản, không có detail")
        print("   💡 Cần có dữ liệu từ cache folder hoặc products_with_detail.json")
        return {"skipped": True, "total_loaded": 0, "db_loaded": 0}

    print(f"✅ Tổng hợp: {len(products)} products unique có detail")
    print(f"   - Từ cache: {len(cache_products)}")
    print(f"   - Từ products_with_detail: {len(seen_in_detail)}")
    if duplicate_count > 0:
        print(f"   - Đã loại bỏ duplicate: {duplicate_count}")

    try:
        # Validate và chuẩn bị products
        print("🔍 Đang validate products...")
        valid_products = []
        invalid_count = 0

        for product in products:
            # Kiểm tra required fields
            if not product.get("product_id") and not product.get("url"):
                invalid_count += 1
                continue

            # Extract product_id từ URL nếu chưa có
            if not product.get("product_id") and product.get("url"):
                try:
                    from pipelines.crawl.utils import extract_product_id_from_url

                    product_id = extract_product_id_from_url(product["url"])
                    if product_id:
                        product["product_id"] = product_id
                    else:
                        invalid_count += 1
                        continue
                except Exception:
                    invalid_count += 1
                    continue

            # Đảm bảo có category_url (có thể None)
            # Nếu chưa có, thử lấy từ mapping hoặc để None
            if "category_url" not in product or not product.get("category_url"):
                product_id = product.get("product_id")
                if product_id and product_id in category_url_mapping:
                    product["category_url"] = category_url_mapping[product_id]
                else:
                    product["category_url"] = None

            # Extract category_id từ category_url nếu chưa có
            if not product.get("category_id") and product.get("category_url"):
                try:
                    from pipelines.crawl.utils import extract_category_id_from_url

                    category_id = extract_category_id_from_url(product["category_url"])
                    if category_id:
                        product["category_id"] = category_id
                except Exception:
                    pass  # Nếu không import được, bỏ qua

            # Đảm bảo category_path được giữ lại (nếu có trong cache)
            # category_path đã có sẵn từ cache, không cần xử lý thêm

            valid_products.append(product)

        print(f"✅ Đã validate: {len(valid_products)} valid, {invalid_count} invalid")

        # Transform products từ nested format sang flat format cho database
        print("\n🔄 Đang transform products (nested → flat format)...")
        transformer = DataTransformer()
        transformed_products = []
        transform_failed = 0

        for product in valid_products:
            try:
                # Transform product (flatten nested dicts: price, rating, seller, stock)
                transformed = transformer.transform_product(product)
                if transformed:
                    transformed_products.append(transformed)
                else:
                    transform_failed += 1
            except Exception as e:
                transform_failed += 1
                if transform_failed <= 5:  # Chỉ log 5 lỗi đầu tiên
                    print(
                        f"   ⚠️  Lỗi transform product {product.get('product_id', 'unknown')}: {e}"
                    )

        if transform_failed > 0:
            print(f"   ⚠️  Có {transform_failed} products transform thất bại")
        print(f"✅ Đã transform {len(transformed_products)} products thành công")

        if not transformed_products:
            print("⚠️  Không có products nào để load sau khi transform")
            return {"skipped": True, "total_loaded": 0, "db_loaded": 0}

        # Load vào database
        print("\n💾 Đang load products vào database...")
        print("   📌 Đảm bảo không duplicate:")
        print("      - Đã loại bỏ duplicate trong memory (dựa trên product_id)")
        print("      - Database có UNIQUE constraint trên product_id")
        print("      - Sử dụng UPSERT (ON CONFLICT UPDATE) để update nếu đã tồn tại")
        stats = loader.load_products(
            transformed_products,
            save_to_file=None,
            upsert=True,  # UPDATE nếu product_id đã tồn tại (không tạo duplicate)
            validate_before_load=False,  # Đã validate và transform ở trên
        )

        print(f"✅ Đã load {stats['db_loaded']} products vào database")
        print(f"   - Tổng số: {stats['total_loaded']}")
        print(f"   - Thành công: {stats['success_count']}")
        print(f"   - Thất bại: {stats['failed_count']}")

        if stats.get("errors"):
            print(f"⚠️  Có {len(stats['errors'])} lỗi (hiển thị 5 đầu tiên):")
            for error in stats["errors"][:5]:
                print(f"   - {error}")

        return stats

    except Exception as e:
        print(f"❌ Lỗi khi load products: {e}")
        import traceback

        traceback.print_exc()
        return {"error": str(e), "total_loaded": 0, "db_loaded": 0}


def verify_data_links(loader: DataLoader) -> dict[str, Any]:
    """Verify liên kết giữa products và categories"""
    print("\n" + "=" * 70)
    print("🔗 BƯỚC 3: VERIFY DATA LINKS")
    print("=" * 70)

    try:
        # Kiểm tra connection
        if not loader.enable_db or not loader.db_storage:
            print("⚠️  Database không available, bỏ qua verification")
            return {"skipped": True}

        from pipelines.crawl.storage.postgres_storage import PostgresStorage

        storage: PostgresStorage = loader.db_storage

        # Đếm categories
        with storage.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM categories")
                category_count = cur.fetchone()[0]

                # Đếm products
                cur.execute("SELECT COUNT(*) FROM products")
                product_count = cur.fetchone()[0]

                # Đếm products có category_url
                cur.execute("SELECT COUNT(*) FROM products WHERE category_url IS NOT NULL")
                products_with_category = cur.fetchone()[0]

                # Đếm products có category_url match với categories
                cur.execute(
                    """
                    SELECT COUNT(DISTINCT p.id)
                    FROM products p
                    INNER JOIN categories c ON p.category_url = c.url
                """
                )
                products_linked = cur.fetchone()[0]

                # Đếm products có category_url nhưng không match
                cur.execute(
                    """
                    SELECT COUNT(DISTINCT p.id)
                    FROM products p
                    LEFT JOIN categories c ON p.category_url = c.url
                    WHERE p.category_url IS NOT NULL AND c.url IS NULL
                """
                )
                products_unlinked = cur.fetchone()[0]

                # Lấy sample các category_url không match (để debug)
                cur.execute(
                    """
                    SELECT DISTINCT p.category_url
                    FROM products p
                    LEFT JOIN categories c ON p.category_url = c.url
                    WHERE p.category_url IS NOT NULL AND c.url IS NULL
                    LIMIT 10
                """
                )
                unlinked_urls = [row[0] for row in cur.fetchall()]

                # Thống kê theo level categories
                cur.execute(
                    """
                    SELECT level, COUNT(*) as count
                    FROM categories
                    GROUP BY level
                    ORDER BY level
                """
                )
                categories_by_level = {row[0]: row[1] for row in cur.fetchall()}

        print("📊 Thống kê:")
        print(f"   - Tổng số categories: {category_count}")
        if categories_by_level:
            print("   - Categories theo level:")
            for level in sorted(categories_by_level.keys()):
                print(f"     Level {level}: {categories_by_level[level]} categories")
        print(f"   - Tổng số products: {product_count}")
        print(f"   - Products có category_url: {products_with_category}")
        print(f"   - Products đã link với categories: {products_linked}")
        print(f"   - Products chưa link (category_url không tồn tại): {products_unlinked}")

        if products_unlinked > 0:
            print(
                f"\n⚠️  Có {products_unlinked} products có category_url nhưng không tìm thấy category tương ứng"
            )
            if unlinked_urls:
                print("   Sample category_urls không tìm thấy (10 đầu tiên):")
                for url in unlinked_urls[:5]:
                    print(f"     - {url}")
            print("   Có thể do:")
            print("   - Category chưa được load vào database")
            print("   - URL không khớp (có thể do format khác)")

        # Tính tỷ lệ link
        if products_with_category > 0:
            link_rate = (products_linked / products_with_category) * 100
            print(
                f"\n   📈 Tỷ lệ link: {link_rate:.2f}% ({products_linked}/{products_with_category})"
            )

        return {
            "category_count": category_count,
            "product_count": product_count,
            "products_with_category": products_with_category,
            "products_linked": products_linked,
            "products_unlinked": products_unlinked,
            "categories_by_level": categories_by_level,
            "unlinked_urls_sample": unlinked_urls,
        }

    except Exception as e:
        print(f"❌ Lỗi khi verify: {e}")
        import traceback

        traceback.print_exc()
        return {"error": str(e)}


def check_required_files() -> tuple[bool, list[str]]:
    """Kiểm tra các file cần thiết có tồn tại không"""
    missing_files = []

    # Kiểm tra folder data/raw
    data_raw = project_root / "data" / "raw"
    if not data_raw.exists():
        missing_files.append(f"Folder: {data_raw}")
        return False, missing_files

    # Kiểm tra categories_tree.json (không bắt buộc)
    tree_file = data_raw / "categories_tree.json"
    if not tree_file.exists():
        missing_files.append(f"File (optional): {tree_file}")

    # Kiểm tra folder products
    products_dir = data_raw / "products"
    if not products_dir.exists():
        missing_files.append(f"Folder: {products_dir}")
        return False, missing_files

    # Kiểm tra các nguồn dữ liệu có detail (ít nhất 1 trong 2 phải có)
    cache_dir = products_dir / "detail" / "cache"
    products_with_detail_file = products_dir / "products_with_detail.json"

    has_cache = cache_dir.exists() and any(cache_dir.glob("*.json"))
    has_products_with_detail = products_with_detail_file.exists()

    if not (has_cache or has_products_with_detail):
        missing_files.append("Ít nhất một trong các nguồn sau (có detail đầy đủ):")
        missing_files.append(f"  - Cache folder: {cache_dir} (có file .json)")
        missing_files.append(f"  - File: {products_with_detail_file}")
        missing_files.append("")
        missing_files.append("  ⚠️  Lưu ý: products.json chỉ chứa danh sách cơ bản, không có detail")

    return (
        len(
            [
                f
                for f in missing_files
                if not f.startswith("  -") and not f.startswith("File (optional)")
            ]
        )
        == 0,
        missing_files,
    )


def main():
    """Main function E2E"""
    print("=" * 70)
    print("🚀 E2E: LOAD EXISTING DATA TO DATABASE")
    print("=" * 70)
    print("\nScript này sẽ:")
    print("  1. Extract và load categories từ categories_tree.json")
    print("  2. Load products có detail đầy đủ (từ cache folder hoặc products_with_detail.json)")
    print("  3. Verify liên kết giữa products và categories")
    print("\n📌 Lưu ý:")
    print("   - Chỉ load dữ liệu có detail đầy đủ (từ cache hoặc products_with_detail.json)")
    print("   - products.json chỉ chứa danh sách cơ bản, không được sử dụng")
    print("   - Dữ liệu từ cache folder sẽ được ưu tiên nếu có")

    # Kiểm tra files cần thiết
    print("\n🔍 Kiểm tra files cần thiết...")
    files_ok, missing_files = check_required_files()

    if not files_ok:
        print("❌ Thiếu các file/folder sau:")
        for file in missing_files:
            print(f"   - {file}")
        print("\n💡 Tạo các folder cần thiết...")

        # Tạo các folder nếu chưa có
        (project_root / "data" / "raw").mkdir(parents=True, exist_ok=True)
        (project_root / "data" / "raw" / "products").mkdir(parents=True, exist_ok=True)

        print("✅ Đã tạo các folder cần thiết")
        print("⚠️  Vui lòng đảm bảo ít nhất một trong các nguồn sau tồn tại:")
        print("   - data/raw/products/detail/cache/*.json (có file .json)")
        print("   - data/raw/products/products_with_detail.json")
        print("\n   Lưu ý: products.json chỉ chứa danh sách cơ bản, không có detail")
        return 1

    print("✅ Tất cả files cần thiết đều tồn tại")

    # Đường dẫn files
    tree_file = project_root / "data" / "raw" / "categories_tree.json"
    products_dir = project_root / "data" / "raw" / "products"
    products_file = products_dir / "products.json"  # Để lấy category_url mapping
    products_with_detail_file = products_dir / "products_with_detail.json"
    cache_dir = products_dir / "detail" / "cache"

    # Khởi tạo DataLoader
    print("\n🔌 Đang kết nối database...")

    # Lấy credentials từ environment hoặc .env file
    postgres_host = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_user = os.getenv("POSTGRES_USER", "airflow_user")
    postgres_password = os.getenv("POSTGRES_PASSWORD", "")
    postgres_db = os.getenv("POSTGRES_DB", "crawl_data")

    # Thử đọc từ .env file nếu có
    env_file = project_root / ".env"
    if env_file.exists():
        print(f"📄 Đang đọc .env từ: {env_file}")
        try:
            # Thử dùng python-dotenv nếu có
            try:
                from dotenv import load_dotenv

                load_dotenv(env_file, override=True)
                postgres_host = os.getenv("POSTGRES_HOST", postgres_host)
                postgres_port = int(os.getenv("POSTGRES_PORT", postgres_port))
                postgres_user = os.getenv("POSTGRES_USER", postgres_user)
                postgres_password = os.getenv("POSTGRES_PASSWORD", postgres_password)
                postgres_db = os.getenv("POSTGRES_DB", postgres_db)
                print("✅ Đã load .env bằng python-dotenv")
            except ImportError:
                # Fallback: đọc thủ công
                with open(env_file, encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#"):
                            if "=" in line:
                                key, value = line.split("=", 1)
                                key = key.strip()
                                value = value.strip().strip('"').strip("'")
                                if key == "POSTGRES_PASSWORD":
                                    postgres_password = value
                                elif key == "POSTGRES_HOST":
                                    postgres_host = value
                                elif key == "POSTGRES_USER":
                                    postgres_user = value
                                elif key == "POSTGRES_DB":
                                    postgres_db = value
                                elif key == "POSTGRES_PORT":
                                    try:
                                        postgres_port = int(value)
                                    except ValueError:
                                        pass
                print("✅ Đã load .env thủ công")
        except Exception as e:
            print(f"⚠️  Không thể đọc .env: {e}")

    # Hiển thị thông tin kết nối (ẩn password)
    print("\n📋 Thông tin kết nối:")
    print(f"   - Host: {postgres_host}")
    print(f"   - Port: {postgres_port}")
    print(f"   - User: {postgres_user}")
    print(f"   - Database: {postgres_db}")
    print(f"   - Password: {'***' if postgres_password else '(chưa set)'}")

    loader = DataLoader(
        database=postgres_db,
        host=postgres_host,
        port=postgres_port,
        user=postgres_user,
        password=postgres_password,
        batch_size=100,
        enable_db=True,
    )

    if not loader.enable_db:
        print("❌ Không thể kết nối database!")
        print("\n💡 Hướng dẫn:")
        print("   1. Đảm bảo PostgreSQL đang chạy")
        print("   2. Set environment variables hoặc tạo file .env:")
        print("      POSTGRES_HOST=localhost")
        print("      POSTGRES_PORT=5432")
        print("      POSTGRES_USER=airflow_user")
        print("      POSTGRES_PASSWORD=your_password")
        print("      POSTGRES_DB=crawl_data")
        print("\n   3. Hoặc chạy trong Docker với:")
        print("      docker-compose up -d postgres")
        print("\n⚠️  Script sẽ chỉ validate dữ liệu, không load vào database")

        # Chạy ở chế độ validate only
        print("\n" + "=" * 70)
        print("📋 VALIDATE MODE (Không có database)")
        print("=" * 70)

        # Validate categories
        try:
            categories = extract_categories_from_tree_file(tree_file)
            print(f"✅ Categories: {len(categories)} categories hợp lệ")
        except Exception as e:
            print(f"❌ Lỗi validate categories: {e}")

        # Validate products (chỉ từ cache hoặc products_with_detail)
        products_count = 0
        cache_products = {}
        try:
            # Thử load từ cache
            if cache_dir.exists():
                cache_products = load_products_from_cache(cache_dir)
                products_count += len(cache_products)
                if cache_products:
                    print(f"✅ Products từ cache: {len(cache_products)} products hợp lệ")

            # Thử load từ products_with_detail
            if products_with_detail_file.exists():
                with open(products_with_detail_file, encoding="utf-8") as f:
                    data = json.load(f)
                products_detail = data.get("products", []) if isinstance(data, dict) else data
                # Đếm products chưa có trong cache
                cache_ids = set(cache_products.keys())
                new_products = [p for p in products_detail if p.get("product_id") not in cache_ids]
                products_count += len(new_products)
                if new_products:
                    print(
                        f"✅ Products từ products_with_detail: {len(new_products)} products hợp lệ (chưa có trong cache)"
                    )

            if products_count == 0:
                print("⚠️  Không tìm thấy products có detail để validate")
                print("   💡 Cần có dữ liệu từ cache folder hoặc products_with_detail.json")
            else:
                print(f"✅ Tổng cộng: {products_count} products có detail hợp lệ")
        except Exception as e:
            print(f"❌ Lỗi validate products: {e}")

        return 1

    print("✅ Đã kết nối database")

    try:
        # Bước 1: Load categories
        categories_stats = load_categories_e2e(loader, tree_file)

        # Bước 2: Load products có detail (từ cache hoặc products_with_detail.json)
        # Truyền products_file để lấy category_url mapping
        products_stats = load_products_e2e(
            loader,
            cache_dir=cache_dir,
            products_with_detail_file=products_with_detail_file,
            products_file=products_file,
        )

        # Bước 3: Verify links
        verify_stats = verify_data_links(loader)

        # Tổng kết
        print("\n" + "=" * 70)
        print("📊 TỔNG KẾT")
        print("=" * 70)
        print("\nCategories:")
        print(f"  - Đã load: {categories_stats.get('db_loaded', 0)}")
        print(f"  - Thành công: {categories_stats.get('success_count', 0)}")
        print(f"  - Thất bại: {categories_stats.get('failed_count', 0)}")

        print("\nProducts:")
        print(f"  - Đã load: {products_stats.get('db_loaded', 0)}")
        print(f"  - Thành công: {products_stats.get('success_count', 0)}")
        print(f"  - Thất bại: {products_stats.get('failed_count', 0)}")

        if verify_stats and not verify_stats.get("skipped"):
            print("\nLinks:")
            print(f"  - Categories: {verify_stats.get('category_count', 0)}")
            print(f"  - Products: {verify_stats.get('product_count', 0)}")
            print(f"  - Products linked: {verify_stats.get('products_linked', 0)}")
            print(f"  - Products unlinked: {verify_stats.get('products_unlinked', 0)}")

        print("\n✅ HOÀN THÀNH!")
        return 0

    except Exception as e:
        print(f"\n❌ Lỗi trong quá trình xử lý: {e}")
        import traceback

        traceback.print_exc()
        return 1

    finally:
        loader.close()
        print("\n🔌 Đã đóng kết nối database")


if __name__ == "__main__":
    sys.exit(main())
