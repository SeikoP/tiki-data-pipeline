from __future__ import annotations

# Import all bootstrap globals (paths, config, dynamic imports, singletons).
# This preserves legacy behavior without renaming any globals referenced by task callables.
from ..bootstrap import (
    CATEGORIES_FILE,
    DATA_DIR,
    OUTPUT_DIR,
    OUTPUT_FILE,
    OUTPUT_FILE_WITH_DETAIL,
    Any,
    dag_file_dir,
    datetime,
    ensure_output_dirs,
    json,
    os,
    re,
)
from .common import _fix_sys_path_for_pipelines_import  # noqa: F401
from .common import atomic_write_file  # noqa: F401
from .common import get_logger  # noqa: F401


def merge_products(**context) -> dict[str, Any]:
    """
    Task 3: Merge sản phẩm từ tất cả các danh mục

    Returns:
        Dict: Tổng hợp sản phẩm và thống kê
    """
    logger = get_logger(context)
    logger.info("🔄 TASK: Merge Products")

    try:
        ensure_output_dirs()

        ti = context["ti"]

        # Lấy categories từ task load_categories (trong TaskGroup load_and_prepare)
        # Thử nhiều cách để lấy categories
        categories = None

        # Cách 1: Lấy từ task_id với TaskGroup prefix (pre_crawl.load_categories)
        try:
            categories = ti.xcom_pull(task_ids="pre_crawl.load_categories")
            logger.info(
                f"Lấy categories từ 'pre_crawl.load_categories': {len(categories) if categories else 0} items"
            )
        except Exception as e:
            logger.warning(f"Không lấy được từ 'pre_crawl.load_categories': {e}")

        # Fallback: Các check cũ để tương thích ngược
        if not categories:
            try:
                categories = ti.xcom_pull(task_ids="load_and_prepare.load_categories")
                logger.info(
                    f"Lấy categories từ 'load_and_prepare.load_categories': {len(categories) if categories else 0} items"
                )
            except Exception:
                pass

        # Cách 2: Thử không có prefix
        if not categories:
            try:
                categories = ti.xcom_pull(task_ids="load_categories")
                logger.info(
                    f"Lấy categories từ 'load_categories': {len(categories) if categories else 0} items"
                )
            except Exception as e:
                logger.warning(f"Không lấy được từ 'load_categories': {e}")

        if not categories:
            raise ValueError("Không tìm thấy categories từ XCom")

        logger.info(f"Đang merge kết quả từ {len(categories)} danh mục...")

        # Lấy kết quả từ các task crawl (Dynamic Task Mapping)
        # Với Dynamic Task Mapping, cần lấy từ task_id với map_index
        all_products = []
        stats = {
            "total_categories": len(categories),
            "success_categories": 0,
            "failed_categories": 0,
            "timeout_categories": 0,
            "total_products": 0,
            "unique_products": 0,
        }

        # Lấy kết quả từ các task crawl (Dynamic Task Mapping)
        # Với Dynamic Task Mapping trong Airflow 2.x, cần lấy từ task_id với map_index
        task_id = "crawl_categories.crawl_category"

        # Lấy từ XCom - thử nhiều cách
        try:
            # Cách 1: Lấy tất cả kết quả từ XCom (Airflow 2.x có thể trả về list)
            all_results = ti.xcom_pull(task_ids=task_id, key="return_value")

            # Xử lý kết quả
            if isinstance(all_results, list):
                # Nếu là list, xử lý từng phần tử
                # Với batch processing, mỗi phần tử có thể là list of results (từ 1 batch)
                for result in all_results:
                    # Check if result is a list (batch result) hoặc dict (single category result)
                    if isinstance(result, list):
                        # Batch result - flatten it
                        for single_result in result:
                            if single_result and isinstance(single_result, dict):
                                if single_result.get("status") == "success":
                                    stats["success_categories"] += 1
                                    products = single_result.get("products", [])
                                    all_products.extend(products)
                                    stats["total_products"] += len(products)
                                elif single_result.get("status") == "timeout":
                                    stats["timeout_categories"] += 1
                                    logger.warning(
                                        f"⏱️  Category {single_result.get('category_name')} timeout"
                                    )
                                else:
                                    stats["failed_categories"] += 1
                                    logger.warning(
                                        f"❌ Category {single_result.get('category_name')} failed: {single_result.get('error')}"
                                    )
                    elif result and isinstance(result, dict):
                        # Single category result
                        if result.get("status") == "success":
                            stats["success_categories"] += 1
                            products = result.get("products", [])
                            all_products.extend(products)
                            stats["total_products"] += len(products)
                        elif result.get("status") == "timeout":
                            stats["timeout_categories"] += 1
                            logger.warning(f"⏱️  Category {result.get('category_name')} timeout")
                        else:
                            stats["failed_categories"] += 1
                            logger.warning(
                                f"❌ Category {result.get('category_name')} failed: {result.get('error')}"
                            )
            elif isinstance(all_results, dict):
                # Nếu là dict, có thể key là map_index hoặc category_id
                for result in all_results.values():
                    if result and isinstance(result, dict):
                        if result.get("status") == "success":
                            stats["success_categories"] += 1
                            products = result.get("products", [])
                            all_products.extend(products)
                            stats["total_products"] += len(products)
                        elif result.get("status") == "timeout":
                            stats["timeout_categories"] += 1
                            logger.warning(f"⏱️  Category {result.get('category_name')} timeout")
                        else:
                            stats["failed_categories"] += 1
                            logger.warning(
                                f"❌ Category {result.get('category_name')} failed: {result.get('error')}"
                            )
            elif all_results and isinstance(all_results, dict):
                # Nếu chỉ có 1 kết quả (dict)
                if all_results.get("status") == "success":
                    stats["success_categories"] += 1
                    products = all_results.get("products", [])
                    all_products.extend(products)
                    stats["total_products"] += len(products)
                elif all_results.get("status") == "timeout":
                    stats["timeout_categories"] += 1
                    logger.warning(f"⏱️  Category {all_results.get('category_name')} timeout")
                else:
                    stats["failed_categories"] += 1
                    logger.warning(
                        f"❌ Category {all_results.get('category_name')} failed: {all_results.get('error')}"
                    )

            # Nếu không lấy được, thử lấy từng map_index
            if not all_results or (isinstance(all_results, (list, dict)) and len(all_results) == 0):
                # Try fetching individual map_index results
                for map_index in range(len(categories)):
                    try:
                        result = ti.xcom_pull(
                            task_ids=task_id, key="return_value", map_indexes=[map_index]
                        )

                        if result and isinstance(result, dict):
                            if result.get("status") == "success":
                                stats["success_categories"] += 1
                                products = result.get("products", [])
                                all_products.extend(products)
                                stats["total_products"] += len(products)
                            elif result.get("status") == "timeout":
                                stats["timeout_categories"] += 1
                                logger.warning(f"⏱️  Category {result.get('category_name')} timeout")
                            else:
                                stats["failed_categories"] += 1
                                logger.warning(
                                    f"❌ Category {result.get('category_name')} failed: {result.get('error')}"
                                )
                    except Exception as e:
                        stats["failed_categories"] += 1
                        logger.warning(f"Không thể lấy kết quả từ map_index {map_index}: {e}")

        except Exception as e:
            logger.error(f"Không thể lấy kết quả từ XCom: {e}", exc_info=True)
            # Nếu không lấy được, đánh dấu tất cả là failed
            stats["failed_categories"] = len(categories)

        # Loại bỏ trùng lặp theo product_id
        seen_ids = set()
        unique_products = []
        products_with_sales_count = 0
        for product in all_products:
            product_id = product.get("product_id")
            if product_id and product_id not in seen_ids:
                seen_ids.add(product_id)
                # Đảm bảo sales_count luôn có trong product (kể cả None)
                if "sales_count" not in product:
                    product["sales_count"] = None
                elif product.get("sales_count") is not None:
                    products_with_sales_count += 1
                unique_products.append(product)

        # Log thống kê sales_count
        logger.info(
            f"📊 Products có sales_count: {products_with_sales_count}/{len(unique_products)} ({products_with_sales_count / len(unique_products) * 100:.1f}%)"
            if unique_products
            else "📊 Products có sales_count: 0/0"
        )

        stats["unique_products"] = len(unique_products)

        logger.info("=" * 70)
        logger.info("📊 THỐNG KÊ")
        logger.info("=" * 70)
        logger.info(f"📁 Tổng danh mục: {stats['total_categories']}")
        logger.info(f"✅ Thành công: {stats['success_categories']}")
        logger.info(f"❌ Thất bại: {stats['failed_categories']}")
        logger.info(f"⏱️  Timeout: {stats['timeout_categories']}")
        logger.info(f"📦 Tổng sản phẩm (trước dedup): {stats['total_products']}")
        logger.info(f"📦 Sản phẩm unique: {stats['unique_products']}")
        logger.info("=" * 70)

        result = {
            "products": unique_products,
            "stats": stats,
            "merged_at": datetime.now().isoformat(),
        }

        return result

    except Exception as e:
        logger.error(f"❌ Lỗi khi merge products: {e}", exc_info=True)
        raise


def save_products(**context) -> str:
    """
    Task 4: Lưu sản phẩm vào file (atomic write)

    Tối ưu hóa cho dữ liệu lớn:
    - Batch processing: chia nhỏ và lưu từng batch
    - Atomic write: tránh corrupt file
    - Compression: có thể nén file nếu cần

    Returns:
        str: Đường dẫn file đã lưu
    """
    logger = get_logger(context)
    logger.info("=" * 70)
    logger.info("💾 TASK: Save Products")
    logger.info("=" * 70)

    try:
        # Lấy kết quả từ task merge_products (trong TaskGroup process_and_save)
        ti = context["ti"]
        merge_result = None

        # Cách 1: Lấy từ task_id với TaskGroup prefix
        try:
            merge_result = ti.xcom_pull(task_ids="process_and_save.merge_products")
            # Get merge result from upstream task
        except Exception as e:
            logger.warning(f"Không lấy được từ 'process_and_save.merge_products': {e}")

        # Cách 2: Thử không có prefix
        if not merge_result:
            try:
                merge_result = ti.xcom_pull(task_ids="merge_products")
                # Fallback to merge_products without prefix
            except Exception as e:
                logger.warning(f"Không lấy được từ 'merge_products': {e}")

        if not merge_result:
            raise ValueError("Không tìm thấy kết quả merge từ XCom")

        products = merge_result.get("products", [])
        stats = merge_result.get("stats", {})

        logger.info(f"Đang lưu {len(products)} sản phẩm...")

        # Batch processing cho dữ liệu lớn
        batch_size = int(get_variable("TIKI_SAVE_BATCH_SIZE", default="10000"))

        if len(products) > batch_size:
            logger.info(f"Chia nhỏ thành batches (mỗi batch {batch_size} sản phẩm)...")
            # Lưu từng batch vào file riêng, sau đó merge
            batch_files = []
            for i in range(0, len(products), batch_size):
                batch = products[i : i + batch_size]
                batch_file = OUTPUT_DIR / f"products_batch_{i // batch_size}.json"
                batch_data = {
                    "batch_index": i // batch_size,
                    "total_batches": (len(products) + batch_size - 1) // batch_size,
                    "products": batch,
                }
                atomic_write_file(str(batch_file), batch_data, **context)
                batch_files.append(batch_file)
                logger.info(f"✓ Đã lưu batch {i // batch_size + 1}: {len(batch)} sản phẩm")

        # Chuẩn bị dữ liệu để lưu
        output_data = {
            "total_products": len(products),
            "stats": stats,
            "crawled_at": datetime.now().isoformat(),
            "note": "Crawl từ Airflow DAG với Dynamic Task Mapping",
            "products": products,
        }

        # Atomic write
        output_file = str(OUTPUT_FILE)
        atomic_write_file(output_file, output_data, **context)

        logger.info(f"✅ Đã lưu {len(products)} sản phẩm vào: {output_file}")

        return output_file

    except Exception as e:
        logger.error(f"❌ Lỗi khi save products: {e}", exc_info=True)
        raise


def transform_products(**context) -> dict[str, Any]:
    """
    Task: Transform dữ liệu sản phẩm (normalize, validate, compute fields)

    Returns:
        Dict: Kết quả transform với transformed products và stats
    """
    logger = get_logger(context)
    logger.info("🔄 TASK: Transform Products")

    try:
        ensure_output_dirs()
        ti = context["ti"]

        # Lấy file từ save_products_with_detail
        output_file = None
        try:
            output_file = ti.xcom_pull(task_ids="crawl_product_details.save_products_with_detail")
        except Exception:
            try:
                output_file = ti.xcom_pull(task_ids="save_products_with_detail")
            except Exception:
                pass

        if not output_file:
            output_file = str(OUTPUT_FILE_WITH_DETAIL)

        if not os.path.exists(output_file):
            raise FileNotFoundError(f"Không tìm thấy file: {output_file}")

        logger.info(f"📂 Đang đọc file: {output_file}")

        # Đọc products từ file
        with open(output_file, encoding="utf-8") as f:
            data = json.load(f)

        products = data.get("products", [])
        stats = data.get("stats", {})
        logger.info(f"📊 Tổng số products trong file: {len(products)}")

        # Log thông tin về crawl detail nếu có
        crawled_count = stats.get("crawled_count", 0)
        if crawled_count > 0:
            logger.info(f"🔄 Products được crawl detail: {crawled_count}")
            logger.info(f"✅ Products có detail (success): {stats.get('with_detail', 0)}")

        # Bổ sung category_url và category_id trước khi transform
        logger.info("🔗 Đang bổ sung category_url và category_id...")

        # Bước 1: Load category_url mapping từ products.json (nếu có)
        category_url_mapping = {}  # product_id -> category_url
        products_file = OUTPUT_DIR / "products.json"
        if products_file.exists():
            try:
                logger.info(f"📖 Đang đọc category_url mapping từ: {products_file}")
                with open(products_file, encoding="utf-8") as f:
                    products_data = json.load(f)

                products_list = []
                if isinstance(products_data, list):
                    products_list = products_data
                elif isinstance(products_data, dict):
                    if "products" in products_data:
                        products_list = products_data["products"]
                    elif "data" in products_data and isinstance(products_data["data"], dict):
                        products_list = products_data["data"].get("products", [])

                for product in products_list:
                    product_id = product.get("product_id")
                    category_url = product.get("category_url")
                    if product_id and category_url:
                        category_url_mapping[product_id] = category_url

                logger.info(
                    f"✅ Đã load {len(category_url_mapping)} category_url mappings từ products.json"
                )
            except Exception as e:
                logger.warning(f"⚠️  Lỗi khi đọc products.json: {e}")

        # Bước 2: Import utility để extract category_id
        try:
            # Tìm đường dẫn utils module
            utils_paths = [
                "/opt/airflow/src/pipelines/crawl/utils.py",
                os.path.abspath(
                    os.path.join(dag_file_dir, "..", "..", "src", "pipelines", "crawl", "utils.py")
                ),
                os.path.join(os.getcwd(), "src", "pipelines", "crawl", "utils.py"),
            ]

            utils_path = None
            for path in utils_paths:
                if os.path.exists(path):
                    utils_path = path
                    break

            if utils_path:
                import importlib.util

                spec = importlib.util.spec_from_file_location("crawl_utils", utils_path)
                utils_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(utils_module)
                extract_category_id_from_url = utils_module.extract_category_id_from_url
            else:
                # Fallback: định nghĩa hàm đơn giản
                import re

                def extract_category_id_from_url(url: str) -> str | None:
                    if not url:
                        return None
                    match = re.search(r"/c(\d+)", url)
                    if match:
                        return f"c{match.group(1)}"
                    return None

        except Exception as e:
            logger.warning(f"⚠️  Không thể import extract_category_id_from_url: {e}")
            import re

            def extract_category_id_from_url(url: str) -> str | None:
                if not url:
                    return None
                match = re.search(r"/c(\d+)", url)
                if match:
                    return f"c{match.group(1)}"
                return None

        # Bước 3: Bổ sung category_url, category_id và ENRICH category_path cho products
        updated_count = 0
        category_id_added = 0
        category_path_count = 0
        category_path_enriched = 0

        # Bước 3a: Build category_path lookup từ categories file
        category_path_lookup: dict[str, list] = {}  # category_id -> category_path

        if CATEGORIES_FILE.exists():
            try:
                logger.info(f"📖 Đang load categories từ: {CATEGORIES_FILE}")
                with open(CATEGORIES_FILE, encoding="utf-8") as cf:
                    raw_categories = json.load(cf)

                for cat in raw_categories:
                    cat_id = cat.get("category_id")
                    cat_path = cat.get("category_path")

                    # Chỉ thêm vào lookup nếu có category_id và category_path
                    if cat_id and cat_path:
                        category_path_lookup[cat_id] = cat_path

                logger.info(f"✅ Loaded {len(category_path_lookup)} category_path từ file")
            except Exception as e:
                logger.warning(f"⚠️ Lỗi đọc categories file: {e}")
        else:
            logger.warning(f"⚠️ Categories file không tồn tại: {CATEGORIES_FILE}")

        for product in products:
            product_id = product.get("product_id")

            # Bổ sung category_url nếu chưa có
            if not product.get("category_url") and product_id in category_url_mapping:
                product["category_url"] = category_url_mapping[product_id]
                updated_count += 1

            # Extract category_id từ category_url nếu có
            category_url = product.get("category_url")
            if category_url and not product.get("category_id"):
                category_id = extract_category_id_from_url(category_url)
                if category_id:
                    product["category_id"] = category_id
                    category_id_added += 1

            # Enrich category_path từ lookup map (nếu chưa có)
            if product.get("category_id") and not product.get("category_path"):
                cat_id = product["category_id"]
                if cat_id in category_path_lookup:
                    product["category_path"] = category_path_lookup[cat_id]
                    category_path_enriched += 1

            # Đảm bảo category_path được giữ lại
            if product.get("category_path"):
                category_path_count += 1

        if updated_count > 0:
            logger.info(f"✅ Đã bổ sung category_url cho {updated_count} products")
        if category_id_added > 0:
            logger.info(f"✅ Đã bổ sung category_id cho {category_id_added} products")
        if category_path_enriched > 0:
            logger.info(f"✅ Đã enrich category_path cho {category_path_enriched} products")
        if category_path_count > 0:
            logger.info(f"✅ Tổng products có category_path: {category_path_count}/{len(products)}")

        # Import DataTransformer
        try:
            # Tìm đường dẫn transform module
            transform_paths = [
                "/opt/airflow/src/pipelines/transform/transformer.py",
                os.path.abspath(
                    os.path.join(
                        dag_file_dir, "..", "..", "src", "pipelines", "transform", "transformer.py"
                    )
                ),
                os.path.join(os.getcwd(), "src", "pipelines", "transform", "transformer.py"),
            ]

            transformer_path = None
            for path in transform_paths:
                if os.path.exists(path):
                    transformer_path = path
                    break

            if not transformer_path:
                raise ImportError("Không tìm thấy transformer.py")

            import importlib.util

            spec = importlib.util.spec_from_file_location("transformer", transformer_path)
            transformer_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(transformer_module)
            DataTransformer = transformer_module.DataTransformer

            # Transform products
            transformer = DataTransformer(
                strict_validation=False, remove_invalid=True, normalize_fields=True
            )

            transformed_products, transform_stats = transformer.transform_products(
                products, validate=True
            )

            logger.info(
                f"📊 TRANSFORM: Valid={transform_stats['valid_products']} | Invalid={transform_stats['invalid_products']} | Dupes={transform_stats['duplicates_removed']}"
            )

            # Lưu transformed products vào file
            processed_dir = DATA_DIR / "processed"
            processed_dir.mkdir(parents=True, exist_ok=True)
            transformed_file = processed_dir / "products_transformed.json"

            output_data = {
                "transformed_at": datetime.now().isoformat(),
                "source_file": output_file,
                "total_products": len(products),
                "transform_stats": transform_stats,
                "products": transformed_products,
            }

            atomic_write_file(str(transformed_file), output_data, **context)
            logger.info(
                f"✅ Đã lưu {len(transformed_products)} transformed products vào: {transformed_file}"
            )

            return {
                "transformed_file": str(transformed_file),
                "transformed_count": len(transformed_products),
                "transform_stats": transform_stats,
            }

        except ImportError as e:
            logger.error(f"❌ Không thể import DataTransformer: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"❌ Lỗi khi transform products: {e}", exc_info=True)
            raise

    except Exception as e:
        logger.error(f"❌ Lỗi trong transform_products task: {e}", exc_info=True)
        raise
