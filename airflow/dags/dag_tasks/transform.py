"""
Transform and load tasks for Tiki crawl products DAG
"""
import json
import os
import sys
import importlib.util
from datetime import datetime
from pathlib import Path
from typing import Any

from ..dag_helpers.config import DATA_DIR, OUTPUT_DIR, OUTPUT_FILE_WITH_DETAIL, get_dag_file_dir
from ..dag_helpers.utils import Variable, get_logger

# Get DAG file directory
dag_file_dir = get_dag_file_dir()


def transform_products(**context) -> dict[str, Any]:
    """
    Task: Transform dữ liệu sản phẩm (normalize, validate, compute fields)

    Returns:
        Dict: Kết quả transform với transformed products và stats
    """
    logger = get_logger(context)
    logger.info("=" * 70)
    logger.info("🔄 TASK: Transform Products")
    logger.info("=" * 70)

    try:
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
                
                logger.info(f"✅ Đã load {len(category_url_mapping)} category_url mappings từ products.json")
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
        
        # Bước 3: Bổ sung category_url, category_id và đảm bảo category_path cho products
        updated_count = 0
        category_id_added = 0
        category_path_count = 0
        
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
            
            # Đảm bảo category_path được giữ lại (đã có từ cache, không cần xử lý)
            if product.get("category_path"):
                category_path_count += 1
        
        if updated_count > 0:
            logger.info(f"✅ Đã bổ sung category_url cho {updated_count} products")
        if category_id_added > 0:
            logger.info(f"✅ Đã bổ sung category_id cho {category_id_added} products")
        if category_path_count > 0:
            logger.info(f"✅ Có {category_path_count} products có category_path (breadcrumb)")
        
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

            logger.info("=" * 70)
            logger.info("📊 TRANSFORM RESULTS")
            logger.info("=" * 70)
            logger.info(f"✅ Valid products: {transform_stats['valid_products']}")
            logger.info(f"❌ Invalid products: {transform_stats['invalid_products']}")
            logger.info(f"🔄 Duplicates removed: {transform_stats['duplicates_removed']}")
            logger.info("=" * 70)

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

        # Import DataLoader
        try:
            # Tìm đường dẫn load module
            load_paths = [
                "/opt/airflow/src/pipelines/load/loader.py",
                os.path.abspath(
                    os.path.join(dag_file_dir, "..", "..", "src", "pipelines", "load", "loader.py")
                ),
                os.path.join(os.getcwd(), "src", "pipelines", "load", "loader.py"),
            ]

            loader_path = None
            for path in load_paths:
                if os.path.exists(path):
                    loader_path = path
                    break

            if not loader_path:
                raise ImportError("Không tìm thấy loader.py")

            import importlib.util

            spec = importlib.util.spec_from_file_location("loader", loader_path)
            loader_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(loader_module)
            DataLoader = loader_module.DataLoader

            # Lấy database config từ Airflow Variables hoặc environment variables
            # Ưu tiên: Airflow Variables > Environment Variables > Default
            db_host = Variable.get("POSTGRES_HOST", default_var=os.getenv("POSTGRES_HOST", "postgres"))
            db_port = int(Variable.get("POSTGRES_PORT", default_var=os.getenv("POSTGRES_PORT", "5432")))
            db_name = Variable.get("POSTGRES_DB", default_var=os.getenv("POSTGRES_DB", "crawl_data"))
            db_user = Variable.get("POSTGRES_USER", default_var=os.getenv("POSTGRES_USER", "postgres"))
            db_password = Variable.get("POSTGRES_PASSWORD", default_var=os.getenv("POSTGRES_PASSWORD", "postgres"))

            # Load vào database
            loader = DataLoader(
                host=db_host,
                port=db_port,
                database=db_name,
                user=db_user,
                password=db_password,
                batch_size=100,
                enable_db=True,
            )

            try:
                # Lưu vào processed directory
                processed_dir = DATA_DIR / "processed"
                processed_dir.mkdir(parents=True, exist_ok=True)
                final_file = processed_dir / "products_final.json"

                load_stats = loader.load_products(
                    products,
                    save_to_file=str(final_file),
                    upsert=True,
                    validate_before_load=True,
                )

                logger.info("=" * 70)
                logger.info("📊 LOAD RESULTS")
                logger.info("=" * 70)
                logger.info(f"✅ DB loaded: {load_stats['db_loaded']}")
                logger.info(f"✅ File loaded: {load_stats['file_loaded']}")
                logger.info(f"❌ Failed: {load_stats['failed_count']}")
                logger.info("=" * 70)

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