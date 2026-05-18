from __future__ import annotations

from common.settings import get_pipeline_settings

# Import all bootstrap globals (paths, config, dynamic imports, singletons).
# This preserves legacy behavior without renaming any globals referenced by task callables.
from tiki_crawl_products_v2.bootstrap import (
    CATEGORIES_FILE,
    DATA_DIR,
    Any,
    dag_file_dir,
    get_load_categories_db_func,
    get_logger,
    json,
    os,
    sys,
)
from tiki_crawl_products_v2.tasks.common import _fix_sys_path_for_pipelines_import  # noqa: F401
from tiki_crawl_products_v2.tasks.optimizations import profile_task


def _resolve_product_load_mode(raw_mode: str | None, product_count: int, threshold: int) -> str:
    """Resolve the load strategy label used by the Airflow task."""
    mode = (raw_mode or "auto").strip().lower()
    threshold = max(1, threshold)

    if mode in {"safe", "traditional"}:
        return "safe"
    if mode in {"optimized", "bulk"}:
        return "optimized"
    if mode == "auto":
        return "optimized" if product_count >= threshold else "safe"
    raise ValueError("TIKI_LOAD_MODE must be one of: auto, safe, traditional, optimized, bulk")


def _get_crawl_run_id(context: dict[str, Any]) -> int | None:
    ti = context.get("ti")
    if not ti:
        return None
    for task_id in ("start_crawl_run", "pipeline_tracking.start_crawl_run"):
        try:
            result = ti.xcom_pull(task_ids=task_id)
        except Exception:
            continue
        if isinstance(result, dict) and result.get("crawl_run_id"):
            return int(result["crawl_run_id"])
        if isinstance(result, int):
            return result
    return None


def fix_missing_parent_categories(**context) -> dict[str, Any]:
    """Fix missing parent categories và rebuild category_path đầy đủ.

    Logic từ scripts/imp/fix_missing_parents.py
    """
    logger = get_logger(context)

    try:
        import re

        from pipelines.crawl.storage.postgres_storage import PostgresStorage

        logger.info("=" * 70)
        logger.info("🔧 FIX MISSING PARENT CATEGORIES")
        logger.info("=" * 70)

        # 1. Load file JSON categories
        json_file = str(CATEGORIES_FILE)
        if not os.path.exists(json_file):
            logger.warning(
                f"⚠️  File categories không tồn tại: {json_file}, bỏ qua fix missing parents"
            )
            return {"status": "skipped", "message": "File not found", "fixed_count": 0}

        logger.info(f"📂 Đang đọc file: {json_file}")
        with open(json_file, encoding="utf-8") as f:
            categories = json.load(f)

        url_to_cat = {cat.get("url"): cat for cat in categories}
        logger.info(f"📊 Loaded {len(categories)} categories từ file JSON")

        # 2. Tìm các parent categories còn thiếu trong DB
        storage = PostgresStorage()

        with storage.get_connection() as conn:
            with conn.cursor() as cur:
                # Lấy tất cả categories trong DB
                cur.execute("SELECT url, parent_url FROM categories")
                db_cats = cur.fetchall()
                db_urls = {cat[0] for cat in db_cats}

                # Tìm các parent URLs cần thiết
                missing_parents = set()
                for db_cat in db_cats:
                    parent_url = db_cat[1] if len(db_cat) > 1 else None
                    if parent_url and parent_url not in db_urls and parent_url in url_to_cat:
                        missing_parents.add(parent_url)

                if not missing_parents:
                    logger.info("✅ Không có parent categories nào còn thiếu!")
                    return {"status": "success", "fixed_count": 0}

                logger.info(f"🔍 Tìm thấy {len(missing_parents)} parent categories còn thiếu")

                # 3. Load các parent categories còn thiếu
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
                                current = {"name": row[0], "url": row[1], "parent_url": row[2]}
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

                    # Check if leaf
                    parent_urls_in_db = {c[1] for c in db_cats if len(c) > 1 and c[1]}
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
                        # logger.info(f"   ✅ Đã load: {cat.get('name')}")
                    except Exception as e:
                        logger.warning(f"   ⚠️  Lỗi khi load {cat.get('name')}: {e}")

                conn.commit()
                logger.info(f"✅ Đã load {saved_count} parent categories vào DB")

        # 4. Rebuild category_path cho tất cả categories (sau khi đóng connection)
        if saved_count > 0:
            logger.info("🔧 Đang rebuild category_path cho tất cả categories...")

            # Tạo storage mới để rebuild
            storage_rebuild = PostgresStorage()
            try:
                with storage_rebuild.get_connection() as conn_rebuild:
                    with conn_rebuild.cursor() as cur_rebuild:
                        cur_rebuild.execute("SELECT url FROM categories")
                        all_db_urls = [row[0] for row in cur_rebuild.fetchall()]

                categories_to_rebuild = [
                    url_to_cat[url] for url in all_db_urls if url in url_to_cat
                ]

                if categories_to_rebuild:
                    # Rebuild paths bằng cách gọi save_categories lại
                    # (sẽ tự động rebuild paths với logic đã được sửa)
                    rebuild_count = storage_rebuild.save_categories(
                        categories_to_rebuild, only_leaf=False, sync_with_products=False
                    )
                    logger.info(f"✅ Đã rebuild {rebuild_count} categories")
            finally:
                storage_rebuild.close()

        logger.info("=" * 70)
        return {"status": "success", "fixed_count": saved_count}

    except Exception as e:
        logger.error(f"❌ Lỗi khi fix missing parent categories: {e}", exc_info=True)
        return {"status": "error", "message": str(e), "fixed_count": 0}


def _count_product_category_refs(product_file: str) -> int:
    try:
        from common.category_utils import normalize_category_id

        with open(product_file, encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list):
            products = data
        elif isinstance(data, dict):
            products = data.get("products", [])
            if not isinstance(products, list) and isinstance(data.get("data"), dict):
                products = data["data"].get("products", [])
        else:
            products = []

        category_ids = {
            normalize_category_id(product.get("category_id"), product.get("category_url"))
            for product in products
            if isinstance(product, dict)
        }
        return len({cat_id for cat_id in category_ids if cat_id})
    except Exception:
        return 0


def _resolve_current_product_file(context: dict[str, Any], logger) -> str | None:
    def usable_product_file(product_file: str | None, *, authoritative: bool = False) -> str | None:
        if not product_file or not os.path.exists(product_file):
            return None
        ref_count = _count_product_category_refs(product_file)
        if ref_count > 0:
            logger.info("📂 Product file có %s category refs: %s", ref_count, product_file)
            return product_file
        if authoritative:
            raise ValueError(f"Current product file does not contain category refs: {product_file}")
        logger.info("⏭️ Bỏ qua product file không có category refs: %s", product_file)
        return None

    ti = context.get("ti")
    if ti:
        for task_id, keys in (
            ("transform_and_load.transform_products", ("transformed_file",)),
            ("transform_products", ("transformed_file",)),
            ("process_and_save.merge_products", ("products_file", "output_file")),
            ("crawl_product_details.merge_product_details", ("output_file", "products_file")),
            ("merge_product_details", ("output_file", "products_file")),
            ("merge_products", ("products_file", "output_file")),
        ):
            try:
                result = ti.xcom_pull(task_ids=task_id)
            except Exception:
                continue
            if isinstance(result, dict):
                for key in keys:
                    product_file = usable_product_file(
                        result.get(key),
                        authoritative=task_id
                        in {
                            "transform_and_load.transform_products",
                            "transform_products",
                        },
                    )
                    if product_file:
                        return product_file
            elif isinstance(result, str):
                product_file = usable_product_file(
                    result,
                    authoritative=task_id
                    in {"transform_and_load.transform_products", "transform_products"},
                )
                if product_file:
                    return product_file

    for candidate in (
        DATA_DIR / "processed" / "products_transformed.json",
        DATA_DIR / "processed" / "merged_products.json",
        DATA_DIR / "processed" / "products_with_detail.json",
    ):
        product_file = usable_product_file(str(candidate))
        if product_file:
            logger.info("📂 Dùng fallback product file để match categories: %s", product_file)
            return product_file

    return None


def load_categories_to_db_wrapper(**context):
    """Task wrapper to load categories from JSON file into PostgreSQL database.

    Sau khi load, tự động fix missing parent categories và rebuild paths.
    """
    logger = get_logger(context)

    try:
        json_file = str(CATEGORIES_FILE)
        if not os.path.exists(json_file):
            logger.error(f"❌ File categories không tồn tại: {json_file}")
            return {"status": "error", "message": "File not found", "count": 0}

        try:
            from common.db_schema import ensure_crawl_schema
            from pipelines.crawl.storage.postgres_storage import PostgresStorage

            storage = PostgresStorage()
            with storage.get_connection() as conn:
                with conn.cursor() as cur:
                    ensure_crawl_schema(cur)
                conn.commit()
            storage.close()
        except Exception as schema_error:
            logger.warning(f"⚠️  Schema preflight trước load categories có vấn đề: {schema_error}")

        load_categories_db_func = get_load_categories_db_func()
        if not load_categories_db_func:
            logger.error("❌ load_categories_db_func not available")
            return {"status": "error", "message": "Import failed"}

        product_file = _resolve_current_product_file(context, logger)
        if product_file:
            logger.info(
                "🚀 Loading matched categories to DB from %s using products %s",
                json_file,
                product_file,
            )
            load_result = load_categories_db_func(json_file, product_file)
        else:
            logger.warning(
                "⚠️  Không tìm thấy current product file; category loader sẽ dùng fallback nội bộ"
            )
            load_result = load_categories_db_func(json_file)

        if isinstance(load_result, dict) and load_result.get("status") == "error":
            raise RuntimeError(load_result.get("message", "load_categories failed"))

        # Sau khi load, fix missing parent categories
        logger.info("🔧 Fixing missing parent categories...")
        fix_result = fix_missing_parent_categories(**context)

        if fix_result.get("status") == "success":
            fixed_count = fix_result.get("fixed_count", 0)
            if fixed_count > 0:
                logger.info(f"✅ Đã fix {fixed_count} missing parent categories")
            else:
                logger.info("✅ Không có parent categories nào cần fix")
        else:
            logger.warning(
                f"⚠️  Fix missing parents có vấn đề: {fix_result.get('message', 'Unknown error')}"
            )

        return {"status": "success", "fixed_parents": fix_result.get("fixed_count", 0)}

    except Exception as e:
        logger.error(f"❌ Lỗi khi load categories vào DB: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}


def _import_postgres_storage():
    """Helper function để import PostgresStorage với fallback logic Hỗ trợ cả môi trường Airflow
    (importlib) và môi trường bình thường.

    Returns:
        PostgresStorage class hoặc None nếu không thể import
    """
    try:
        # Thử import từ __init__.py của storage module
        from pipelines.crawl.storage import PostgresStorage

        return PostgresStorage
    except ImportError:
        try:
            # Thử import trực tiếp từ file
            from pipelines.crawl.storage.postgres_storage import PostgresStorage

            return PostgresStorage
        except ImportError:
            try:
                import importlib.util
                from pathlib import Path

                # Tìm đường dẫn đến postgres_storage.py
                possible_paths = [
                    # Từ /opt/airflow/src (Docker default - ưu tiên)
                    Path("/opt/airflow/src/pipelines/crawl/storage/postgres_storage.py"),
                    # Từ dag_file_dir
                    Path(dag_file_dir).parent.parent.parent
                    / "src"
                    / "pipelines"
                    / "crawl"
                    / "storage"
                    / "postgres_storage.py",
                    # Từ current working directory
                    Path(os.getcwd())
                    / "src"
                    / "pipelines"
                    / "crawl"
                    / "storage"
                    / "postgres_storage.py",
                    # Từ workspace root
                    Path("/workspace/src/pipelines/crawl/storage/postgres_storage.py"),
                ]

                postgres_storage_path = None
                for path in possible_paths:
                    if path.exists() and path.is_file():
                        postgres_storage_path = path
                        break

                if postgres_storage_path:
                    # Sử dụng importlib để load trực tiếp từ file
                    spec = importlib.util.spec_from_file_location(
                        "postgres_storage", postgres_storage_path
                    )
                    if spec and spec.loader:
                        postgres_storage_module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(postgres_storage_module)
                        return postgres_storage_module.PostgresStorage

                # Nếu không tìm thấy file, thử thêm src vào path và import absolute
                src_paths = [
                    Path("/opt/airflow/src"),
                    Path(dag_file_dir).parent.parent.parent / "src",
                    Path(os.getcwd()) / "src",
                ]

                for src_path in src_paths:
                    if src_path.exists() and str(src_path) not in sys.path:
                        sys.path.insert(0, str(src_path))
                        try:
                            from pipelines.crawl.storage import PostgresStorage

                            return PostgresStorage
                        except ImportError:
                            try:
                                from pipelines.crawl.storage.postgres_storage import PostgresStorage

                                return PostgresStorage
                            except ImportError:
                                continue

                return None
            except Exception:
                return None


@profile_task("load_products")
def load_products(**context) -> dict[str, Any]:
    """
    Task: Load dữ liệu đã transform vào database

    Returns:
        Dict: Kết quả load với stats
    """
    logger = get_logger(context)
    logger.info("=" * 70)
    logger.info("💾 TASK: Load Products to Database")
    logger.info("=" * 70)

    try:
        ti = context["ti"]

        # Lấy transformed file từ transform_products task
        transform_result = None
        try:
            transform_result = ti.xcom_pull(task_ids="transform_and_load.transform_products")
        except Exception:
            try:
                transform_result = ti.xcom_pull(task_ids="transform_products")
            except Exception:
                pass

        if not transform_result:
            # Fallback: tìm file transformed
            processed_dir = DATA_DIR / "processed"
            transformed_file = processed_dir / "products_transformed.json"
            if transformed_file.exists():
                transform_result = {"transformed_file": str(transformed_file)}
            else:
                raise ValueError("Không tìm thấy transform result từ XCom hoặc file")

        transformed_file = transform_result.get("transformed_file")
        if not transformed_file or not os.path.exists(transformed_file):
            raise FileNotFoundError(f"Không tìm thấy file transformed: {transformed_file}")

        logger.info(f"📂 Đang đọc transformed file: {transformed_file}")

        # Đọc transformed products
        with open(transformed_file, encoding="utf-8") as f:
            data = json.load(f)

        products = data.get("products", [])
        logger.info(f"📊 Tổng số products để load: {len(products)}")
        crawl_run_id = _get_crawl_run_id(context)
        if crawl_run_id:
            logger.info("🧾 Loading snapshots for crawl_run_id=%s", crawl_run_id)

        # Import OptimizedDataLoader
        try:
            # Tìm đường dẫn load module
            # Ưu tiên load từ file src/pipelines/load/loader.py
            from pipelines.load.loader import OptimizedDataLoader

            settings = get_pipeline_settings()
            db_host = settings.database.host
            db_port = settings.database.port
            db_name = settings.database.database
            db_user = settings.database.user
            db_password = settings.database.password

            # Prepare DB Config for OptimizedDataLoader
            db_config = {
                "host": db_host,
                "port": db_port,
                "database": db_name,
                "user": db_user,
                "password": db_password,
            }
            allow_file_only = settings.load.allow_file_only_on_db_failure
            load_mode = _resolve_product_load_mode(
                settings.load.mode,
                len(products),
                settings.load.optimized_threshold,
            )
            configured_batch_size = settings.load.save_batch_size
            if load_mode == "optimized":
                batch_size = max(configured_batch_size, 1000)
            else:
                batch_size = configured_batch_size
            logger.info(
                "⚙️ Load mode=%s batch_size=%s products=%s",
                load_mode,
                batch_size,
                len(products),
            )

            # Initialize OptimizedDataLoader
            # Note: OptimizedDataLoader uses connection pooling internally
            loader = OptimizedDataLoader(
                batch_size=batch_size,
                enable_db=True,
                db_config=db_config,
                show_progress=True,
                continue_on_error=allow_file_only,
                require_db=not allow_file_only,
            )

            try:
                # Lưu vào processed directory
                processed_dir = DATA_DIR / "processed"
                processed_dir.mkdir(parents=True, exist_ok=True)
                final_file = processed_dir / "products_final.json"

                # Khởi tạo biến để lưu số lượng products
                count_before = None
                count_after = None

                # Kiểm tra số lượng products trong DB trước khi load (for stats)
                try:
                    PostgresStorage = _import_postgres_storage()
                    if PostgresStorage is not None:
                        storage = PostgresStorage(
                            host=db_host,
                            port=db_port,
                            database=db_name,
                            user=db_user,
                            password=db_password,
                        )
                        with storage.get_connection() as conn:
                            with conn.cursor() as cur:
                                cur.execute("SELECT COUNT(*) FROM products;")
                                count_before = cur.fetchone()[0]
                        storage.close()
                        logger.info(f"📊 Số products trong DB trước khi load: {count_before}")
                except Exception as e:
                    logger.warning(f"⚠️  Không thể kiểm tra số products trong DB: {e}")
                    count_before = None

                # Sử dụng OptimizedDataLoader.load_products
                # Signature: load_products(products, upsert=True, validate_before_load=True, save_to_file=None)
                load_stats = loader.load_products(
                    products,
                    upsert=True,
                    validate_before_load=True,
                    save_to_file=str(final_file),
                    raise_on_db_error=not allow_file_only,
                    crawl_run_id=crawl_run_id,
                )
                if not allow_file_only and load_stats.get("db_loaded", 0) < len(products):
                    raise RuntimeError(
                        "Database load incomplete: "
                        f"db_loaded={load_stats.get('db_loaded', 0)} "
                        f"expected={len(products)} failed={load_stats.get('failed_count', 0)} "
                        f"errors={load_stats.get('errors', [])[:3]}"
                    )
                load_stats["load_mode"] = load_mode
                load_stats["batch_size"] = batch_size

                # Kiểm tra số lượng products trong DB sau khi load
                try:
                    PostgresStorage = _import_postgres_storage()
                    if PostgresStorage is not None:
                        storage = PostgresStorage(
                            host=db_host,
                            port=db_port,
                            database=db_name,
                            user=db_user,
                            password=db_password,
                        )
                        with storage.get_connection() as conn:
                            with conn.cursor() as cur:
                                cur.execute("SELECT COUNT(*) FROM products;")
                                count_after = cur.fetchone()[0]
                        storage.close()
                        logger.info(f"📊 Số products trong DB sau khi load: {count_after}")
                        if count_before is not None:
                            diff = count_after - count_before
                            if diff > 0:
                                logger.info(f"✅ Đã thêm {diff} products mới vào DB")
                            elif diff == 0:
                                logger.info(
                                    "ℹ️  Không có products mới (chỉ UPDATE các products đã có)"
                                )
                except Exception as e:
                    logger.warning(f"⚠️  Không thể kiểm tra số lượng products sau khi load: {e}")
                    count_after = None

                logger.info("=" * 40)
                logger.info("💾 KẾT QUẢ LƯU DỮ LIỆU DB")
                logger.info("-" * 40)
                logger.info(f"• Thành công:  {load_stats.get('db_loaded', 0)}")
                logger.info(f"• Insert mới:  {load_stats.get('inserted_count', 0)}")
                logger.info(f"• Cập nhật:    {load_stats.get('updated_count', 0)}")
                logger.info(f"• Lỗi/Thất bại: {load_stats.get('failed_count', 0)}")
                if load_stats.get("errors"):
                    logger.warning(f"• Mẫu lỗi: {str(load_stats['errors'][0])[:100]}...")
                logger.info("=" * 40)

                if load_stats.get("errors"):
                    logger.warning(
                        f"⚠️  Errors ({len(load_stats['errors'])}): {load_stats['errors'][:5]}..."
                    )

                return {
                    "final_file": str(final_file),
                    "load_stats": load_stats,
                }

            finally:
                loader.close()

        except ImportError as e:
            logger.error(f"❌ Không thể import DataLoader: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"❌ Lỗi khi load products: {e}", exc_info=True)
            raise

    except Exception as e:
        logger.error(f"❌ Lỗi trong load_products task: {e}", exc_info=True)
        raise
