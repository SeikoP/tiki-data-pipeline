from __future__ import annotations

# Import all bootstrap globals (paths, config, dynamic imports, singletons).
# This preserves legacy behavior without renaming any globals referenced by task callables.
from tiki_crawl_products_v2.bootstrap import (
    CATEGORIES_FILE,
    DATA_DIR,
    OUTPUT_DIR,
    OUTPUT_FILE,
    OUTPUT_FILE_WITH_DETAIL,
    Any,
    atomic_write_file,
    dag_file_dir,
    datetime,
    ensure_output_dirs,
    get_logger,
    json,
    os,
)
from tiki_crawl_products_v2.tasks.common import _fix_sys_path_for_pipelines_import  # noqa: F401
from tiki_crawl_products_v2.tasks.optimizations import (
    MetricsCollectorWrapper,
    VectorizedTransformerWrapper,
    profile_task,
)


def _get_crawl_run_id(context: dict[str, Any]) -> int | None:
    ti = context.get("ti")
    if not ti:
        return None
    for task_id in ("start_crawl_run", "pipeline_tracking.start_crawl_run"):
        try:
            value = ti.xcom_pull(task_ids=task_id)
        except Exception:
            continue
        if isinstance(value, dict) and value.get("crawl_run_id"):
            return int(value["crawl_run_id"])
        if isinstance(value, int):
            return value
    return None


@profile_task("merge_products")
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

        # Lấy danh sách categories từ file
        result = None
        for task_id in [
            "pre_crawl.load_categories",
            "load_and_prepare.load_categories",
            "load_categories",
        ]:
            result = ti.xcom_pull(task_ids=task_id)
            if result:
                logger.info(f"📂 Đã lấy categories metadata từ task: {task_id}")
                break

        if not result:
            raise ValueError("Không tìm thấy dữ liệu categories từ các task trước đó.")

        # Đọc categories từ file
        categories_file = result.get("categories_file")
        if categories_file:
            with open(categories_file, encoding="utf-8") as f:
                categories = json.load(f)
            logger.info(f"📂 Đọc {len(categories)} categories từ {categories_file}")
        else:
            # Fallback: legacy XCom
            categories = result if isinstance(result, list) else []
            logger.warning("⚠️ Sử dụng legacy XCom format cho categories")

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
            "raw_products": 0,
            "unique_products": 0,
            "total_saved_directly": 0,
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
                                    stats["total_saved_directly"] += single_result.get(
                                        "saved_count", 0
                                    )
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
                    stats["total_saved_directly"] += all_results.get("saved_count", 0)
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
                                stats["total_saved_directly"] += result.get("saved_count", 0)
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

        stats["raw_products"] = stats["total_products"]
        stats["unique_products"] = len(unique_products)
        crawl_run_id = _get_crawl_run_id(context)
        if crawl_run_id:
            try:
                from common.pipeline_tracking import save_raw_products

                raw_saved = save_raw_products(
                    unique_products, crawl_run_id=crawl_run_id, source_stage="list"
                )
                stats["raw_products_saved"] = raw_saved
                logger.info(
                    "🧾 Đã lưu %s raw list products cho crawl_run_id=%s", raw_saved, crawl_run_id
                )
            except Exception as raw_error:
                logger.warning("⚠️ Không thể lưu raw list products: %s", raw_error)

        logger.info("=" * 40)
        logger.info("📊 TỔNG KẾT GIAI ĐOẠN CRAWL")
        logger.info("-" * 40)
        logger.info(f"• Tổng danh mục:   {stats['total_categories']}")
        logger.info(f"• Thành công:      {stats['success_categories']}")
        logger.info(
            f"• Thất bại/Lỗi:    {stats['failed_categories'] + stats['timeout_categories']}"
        )
        logger.info(f"• Tổng sản phẩm:   {stats['total_products']}")
        logger.info(f"• Sau khi lọc trùng: {stats['unique_products']}")
        logger.info(f"• Saved Directly:    {stats['total_saved_directly']}")
        logger.info("=" * 40)

        # Lưu products vào file merged_products.json
        from pathlib import Path

        merged_file = Path("/opt/airflow/data/processed") / "merged_products.json"
        merged_file.parent.mkdir(parents=True, exist_ok=True)

        with open(merged_file, "w", encoding="utf-8") as f:
            json.dump({"products": unique_products, "stats": stats}, f, ensure_ascii=False)

        logger.info(f"💾 Đã lưu {len(unique_products)} products vào {merged_file}")

        # Lưu vào file output chính (products.json) với metadata đầy đủ
        output_data = {
            "total_products": len(unique_products),
            "stats": stats,
            "crawled_at": datetime.now().isoformat(),
            "note": "Crawl từ Airflow DAG với Dynamic Task Mapping",
            "products": unique_products,
        }

        output_file = str(OUTPUT_FILE)
        atomic_write_file(output_file, output_data, **context)

        logger.info(f"✅ Đã lưu {len(unique_products)} sản phẩm vào: {output_file}")

        # Return file paths và metadata
        result = {
            "products_file": str(merged_file),
            "output_file": output_file,
            "stats": stats,
            "merged_at": datetime.now().isoformat(),
            "product_count": len(unique_products),
        }

        return result

    except Exception as e:
        logger.error(f"❌ Lỗi khi merge and save products: {e}", exc_info=True)
        raise


@profile_task("transform_products")
def transform_products(**context) -> dict[str, Any]:
    """
    Task: Transform dữ liệu sản phẩm (normalize, validate, compute fields)

    Supports vectorized transformation if enabled via USE_VECTORIZED_TRANSFORM flag.

    Returns:
        Dict: Kết quả transform với transformed products và stats
    """
    logger = get_logger(context)
    logger.info("🔄 TASK: Transform Products")

    try:
        ensure_output_dirs()
        ti = context["ti"]

        # Lấy file từ merge_product_details
        output_file = None
        try:
            detail_result = ti.xcom_pull(task_ids="crawl_product_details.merge_product_details")
            if isinstance(detail_result, dict):
                output_file = detail_result.get("output_file")
            elif isinstance(detail_result, str):
                output_file = detail_result
        except Exception:
            try:
                detail_result = ti.xcom_pull(task_ids="merge_product_details")
                if isinstance(detail_result, dict):
                    output_file = detail_result.get("output_file")
                elif isinstance(detail_result, str):
                    output_file = detail_result
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
        crawl_run_id = _get_crawl_run_id(context)
        if crawl_run_id:
            try:
                from common.pipeline_tracking import save_raw_products

                raw_saved = save_raw_products(
                    products, crawl_run_id=crawl_run_id, source_stage="detail"
                )
                stats["raw_detail_products_saved"] = raw_saved
                logger.info(
                    "🧾 Đã lưu %s raw detail products cho crawl_run_id=%s",
                    raw_saved,
                    crawl_run_id,
                )
            except Exception as raw_error:
                logger.warning("⚠️ Không thể lưu raw detail products: %s", raw_error)

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

        # Bước 2: Import utility để extract/normalize category metadata
        try:
            from common.category_utils import build_category_lookup, extract_category_id_from_url
        except Exception as e:
            logger.warning(f"⚠️  Không thể import category_utils: {e}")
            import re

            def build_category_lookup(
                categories: list[dict[str, Any]],
            ) -> dict[str, dict[str, Any]]:
                return {}

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

                category_lookup = build_category_lookup(raw_categories)
                for cat_id, cat in category_lookup.items():
                    cat_path = cat.get("category_path")
                    if cat_path:
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

        # Check if vectorized transformer is enabled
        VectorizedTransformer = VectorizedTransformerWrapper.get_transformer()
        use_vectorized = VectorizedTransformer is not None

        if use_vectorized:
            logger.info("🚀 Using VECTORIZED transformer for better performance")
            try:
                import pandas as pd

                # Convert to DataFrame for vectorized operations
                df = pd.DataFrame(products)

                # Initialize vectorized transformer
                vectorized_transformer = VectorizedTransformer()

                # Transform using vectorized operations
                transformed_df = vectorized_transformer.transform_batch(df)

                # Convert back to list of dicts
                transformed_products = transformed_df.to_dict("records")

                transform_stats = {
                    "valid_products": len(transformed_products),
                    "invalid_products": 0,
                    "duplicates_removed": 0,
                    "method": "vectorized",
                }

                logger.info(
                    f"📊 VECTORIZED TRANSFORM: Processed {len(transformed_products)} products"
                )

            except Exception as e:
                logger.warning(
                    f"⚠️  Vectorized transform failed: {e}. Falling back to standard transformer."
                )
                use_vectorized = False

        if not use_vectorized:
            logger.info("📝 Using STANDARD transformer")
            # Import DataTransformer
            try:
                # Tìm đường dẫn transform module
                transform_paths = [
                    "/opt/airflow/src/pipelines/transform/transformer.py",
                    os.path.abspath(
                        os.path.join(
                            dag_file_dir,
                            "..",
                            "..",
                            "..",
                            "src",
                            "pipelines",
                            "transform",
                            "transformer.py",
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
                transform_stats["method"] = "standard"

                logger.info(
                    f"📊 TRANSFORM: Valid={transform_stats['valid_products']} | Invalid={transform_stats['invalid_products']} | Dupes={transform_stats['duplicates_removed']}"
                )

            except ImportError as e:
                logger.error(f"❌ Không thể import DataTransformer: {e}", exc_info=True)
                raise
            except Exception as e:
                logger.error(f"❌ Lỗi khi transform products: {e}", exc_info=True)
                raise

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

        # Collect metrics
        MetricsCollectorWrapper.collect_metrics(
            "transform_products",
            {
                "total_products": len(products),
                "transformed_products": len(transformed_products),
                "method": transform_stats.get("method", "unknown"),
                "valid_products": transform_stats.get("valid_products", 0),
                "invalid_products": transform_stats.get("invalid_products", 0),
            },
            **context,
        )

        return {
            "transformed_file": str(transformed_file),
            "transformed_count": len(transformed_products),
            "transform_stats": transform_stats,
        }

    except Exception as e:
        logger.error(f"❌ Lỗi trong transform_products task: {e}", exc_info=True)
        raise
