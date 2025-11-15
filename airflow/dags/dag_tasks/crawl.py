"""
Crawl tasks for Tiki crawl products DAG
"""
import json
import os
import shutil
import time
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any

from ..dag_helpers.config import CACHE_DIR, OUTPUT_DIR, OUTPUT_FILE
from ..dag_helpers.shared_state import (
    tiki_circuit_breaker,
    tiki_dlq,
    tiki_degradation,
    crawl_category_products,
    CircuitBreakerOpenError,
    classify_error,
)
from ..dag_helpers.utils import Variable, get_logger, atomic_write_file


def crawl_single_category(category: dict[str, Any] = None, **context) -> dict[str, Any]:
    """
    Task 2: Crawl sản phẩm từ một danh mục (Dynamic Task Mapping)

    Tối ưu hóa:
    - Rate limiting: delay giữa các request
    - Caching: sử dụng cache để tránh crawl lại
    - Error handling: tiếp tục với danh mục khác khi lỗi
    - Timeout: giới hạn thời gian crawl

    Args:
        category: Thông tin danh mục (từ expand_kwargs)
        context: Airflow context

    Returns:
        Dict: Kết quả crawl với products và metadata
    """
    logger = get_logger(context)

    # Lấy category từ keyword argument hoặc từ op_kwargs trong context
    # Khi sử dụng expand với op_kwargs, category sẽ được truyền qua op_kwargs
    if not category:
        # Thử lấy từ ti.op_kwargs (cách chính xác nhất)
        ti = context.get("ti")
        if ti:
            # op_kwargs được truyền vào function thông qua ti
            op_kwargs = getattr(ti, "op_kwargs", {})
            if op_kwargs:
                category = op_kwargs.get("category")

        # Fallback: thử lấy từ context trực tiếp
        if not category:
            category = context.get("category") or context.get("op_kwargs", {}).get("category")

    if not category:
        # Debug: log context để tìm lỗi
        logger.error(f"Không tìm thấy category. Context keys: {list(context.keys())}")
        ti = context.get("ti")
        if ti:
            logger.error(f"ti.op_kwargs: {getattr(ti, 'op_kwargs', 'N/A')}")
        raise ValueError("Không tìm thấy category. Kiểm tra expand với op_kwargs.")

    category_url = category.get("url", "")
    category_name = category.get("name", "Unknown")
    category_id = category.get("id", "")

    logger.info("=" * 70)
    logger.info(f"🛍️  TASK: Crawl Category - {category_name}")
    logger.info(f"🔗 URL: {category_url}")
    logger.info("=" * 70)

    result = {
        "category_id": category_id,
        "category_name": category_name,
        "category_url": category_url,
        "products": [],
        "status": "failed",
        "error": None,
        "crawled_at": datetime.now().isoformat(),
        "pages_crawled": 0,
        "products_count": 0,
    }

    try:
        # Kiểm tra graceful degradation
        if tiki_degradation.should_skip():
            result["error"] = "Service đang ở trạng thái FAILED, skip crawl"
            result["status"] = "degraded"
            logger.warning(f"⚠️  Service degraded, skip category {category_name}")
            return result

        # Lấy cấu hình từ Airflow Variables
        max_pages = int(
            Variable.get("TIKI_MAX_PAGES_PER_CATEGORY", default_var="20")
        )  # Mặc định 20 trang để tránh timeout
        use_selenium = Variable.get("TIKI_USE_SELENIUM", default_var="false").lower() == "true"
        timeout = int(Variable.get("TIKI_CRAWL_TIMEOUT", default_var="300"))  # 5 phút mặc định
        rate_limit_delay = float(
            Variable.get("TIKI_RATE_LIMIT_DELAY", default_var="1.0")
        )  # Delay 1s giữa các request

        # Rate limiting: delay trước khi crawl
        if rate_limit_delay > 0:
            time.sleep(rate_limit_delay)

        # Crawl với timeout và circuit breaker
        start_time = time.time()

        def _crawl_with_params():
            """Wrapper function để gọi với circuit breaker"""
            return crawl_category_products(
                category_url,
                max_pages=max_pages if max_pages > 0 else None,
                use_selenium=use_selenium,
                cache_dir=str(CACHE_DIR),
                use_redis_cache=True,  # Sử dụng Redis cache
                use_rate_limiting=True,  # Sử dụng rate limiting
            )

        try:
            # Gọi với circuit breaker
            products = tiki_circuit_breaker.call(_crawl_with_params)
            tiki_degradation.record_success()
        except CircuitBreakerOpenError as e:
            # Circuit breaker đang mở
            result["error"] = f"Circuit breaker open: {str(e)}"
            result["status"] = "circuit_breaker_open"
            logger.warning(f"⚠️  Circuit breaker open cho category {category_name}: {e}")
            # Thêm vào DLQ
            try:
                crawl_error = classify_error(
                    e, context={"category_url": category_url, "category_id": category_id}
                )
                tiki_dlq.add(
                    task_id=f"crawl_category_{category_id}",
                    task_type="crawl_category",
                    error=crawl_error,
                    context={
                        "category_url": category_url,
                        "category_name": category_name,
                        "category_id": category_id,
                    },
                    retry_count=0,
                )
                logger.info(f"📬 Đã thêm vào DLQ: crawl_category_{category_id}")
            except Exception as dlq_error:
                logger.warning(f"⚠️  Không thể thêm vào DLQ: {dlq_error}")
            return result
        except Exception:
            # Ghi nhận failure
            tiki_degradation.record_failure()
            raise  # Re-raise để xử lý bên dưới

        elapsed = time.time() - start_time

        if elapsed > timeout:
            raise TimeoutError(f"Crawl vượt quá timeout {timeout}s")

        result["products"] = products
        result["status"] = "success"
        result["products_count"] = len(products)
        result["elapsed_time"] = elapsed

        logger.info(f"✅ Crawl thành công: {len(products)} sản phẩm trong {elapsed:.1f}s")

    except TimeoutError as e:
        result["error"] = str(e)
        result["status"] = "timeout"
        tiki_degradation.record_failure()
        logger.error(f"⏱️  Timeout: {e}")
        # Thêm vào DLQ
        try:
            crawl_error = classify_error(
                e, context={"category_url": category_url, "category_id": category_id}
            )
            tiki_dlq.add(
                task_id=f"crawl_category_{category_id}",
                task_type="crawl_category",
                error=crawl_error,
                context={
                    "category_url": category_url,
                    "category_name": category_name,
                    "category_id": category_id,
                },
                retry_count=0,
            )
            logger.info(f"📬 Đã thêm vào DLQ: crawl_category_{category_id}")
        except Exception as dlq_error:
            logger.warning(f"⚠️  Không thể thêm vào DLQ: {dlq_error}")
        # Không raise để tiếp tục với danh mục khác

    except Exception as e:
        result["error"] = str(e)
        result["status"] = "failed"
        tiki_degradation.record_failure()
        logger.error(f"❌ Lỗi khi crawl category {category_name}: {e}", exc_info=True)
        # Thêm vào DLQ
        try:
            crawl_error = classify_error(
                e, context={"category_url": category_url, "category_id": category_id}
            )
            tiki_dlq.add(
                task_id=f"crawl_category_{category_id}",
                task_type="crawl_category",
                error=crawl_error,
                context={
                    "category_url": category_url,
                    "category_name": category_name,
                    "category_id": category_id,
                },
                retry_count=0,
            )
            logger.info(f"📬 Đã thêm vào DLQ: crawl_category_{category_id}")
        except Exception as dlq_error:
            logger.warning(f"⚠️  Không thể thêm vào DLQ: {dlq_error}")
        # Không raise để tiếp tục với danh mục khác

    return result

def merge_products(**context) -> dict[str, Any]:
    """
    Task 3: Merge sản phẩm từ tất cả các danh mục

    Returns:
        Dict: Tổng hợp sản phẩm và thống kê
    """
    logger = get_logger(context)
    logger.info("=" * 70)
    logger.info("🔄 TASK: Merge Products")
    logger.info("=" * 70)

    try:

        ti = context["ti"]

        # Lấy categories từ task load_categories (trong TaskGroup load_and_prepare)
        # Thử nhiều cách để lấy categories
        categories = None

        # Cách 1: Lấy từ task_id với TaskGroup prefix
        try:
            categories = ti.xcom_pull(task_ids="load_and_prepare.load_categories")
            logger.info(
                f"Lấy categories từ 'load_and_prepare.load_categories': {len(categories) if categories else 0} items"
            )
        except Exception as e:
            logger.warning(f"Không lấy được từ 'load_and_prepare.load_categories': {e}")

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
                for result in all_results:
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
                logger.info("Thử lấy từng map_index...")
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
            f"📊 Products có sales_count: {products_with_sales_count}/{len(unique_products)} ({products_with_sales_count/len(unique_products)*100:.1f}%)"
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
            logger.info("Lấy merge_result từ 'process_and_save.merge_products'")
        except Exception as e:
            logger.warning(f"Không lấy được từ 'process_and_save.merge_products': {e}")

        # Cách 2: Thử không có prefix
        if not merge_result:
            try:
                merge_result = ti.xcom_pull(task_ids="merge_products")
                logger.info("Lấy merge_result từ 'merge_products'")
            except Exception as e:
                logger.warning(f"Không lấy được từ 'merge_products': {e}")

        if not merge_result:
            raise ValueError("Không tìm thấy kết quả merge từ XCom")

        products = merge_result.get("products", [])
        stats = merge_result.get("stats", {})

        logger.info(f"Đang lưu {len(products)} sản phẩm...")

        # Batch processing cho dữ liệu lớn
        batch_size = int(Variable.get("TIKI_SAVE_BATCH_SIZE", default_var="10000"))

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


def prepare_crawl_kwargs(**context):
    """Helper function để prepare op_kwargs cho Dynamic Task Mapping"""
    import logging

    logger = logging.getLogger("airflow.task")

    ti = context["ti"]

    # Thử nhiều cách lấy categories từ XCom
    categories = None

    # Cách 1: Lấy từ task_id với TaskGroup prefix
    try:
        categories = ti.xcom_pull(task_ids="load_and_prepare.load_categories")
        logger.info(
            f"Lấy categories từ 'load_and_prepare.load_categories': {len(categories) if categories else 0} items"
        )
    except Exception as e:
        logger.warning(f"Không lấy được từ 'load_and_prepare.load_categories': {e}")

    # Cách 2: Thử không có prefix
    if not categories:
        try:
            categories = ti.xcom_pull(task_ids="load_categories")
            logger.info(
                f"Lấy categories từ 'load_categories': {len(categories) if categories else 0} items"
            )
        except Exception as e:
            logger.warning(f"Không lấy được từ 'load_categories': {e}")

    # Cách 3: Thử lấy từ upstream task (đơn giản hóa để tránh timeout)
    if not categories:
        try:
            # Lấy từ task trong cùng DAG run - đơn giản hóa
            from airflow.models import TaskInstance

            dag_run = context["dag_run"]
            # Lấy DAG từ context thay vì dùng biến global
            dag_obj = context.get("dag")
            if dag_obj:
                upstream_task = dag_obj.get_task("load_and_prepare.load_categories")
                upstream_ti = TaskInstance(task=upstream_task, run_id=dag_run.run_id)
                categories = upstream_ti.xcom_pull(key="return_value")
                logger.info(
                    f"Lấy categories từ TaskInstance: {len(categories) if categories else 0} items"
                )
        except Exception as e:
            logger.warning(f"Không lấy được từ TaskInstance: {e}")

    if not categories:
        logger.error("❌ Không thể lấy categories từ XCom!")
        return []

    if not isinstance(categories, list):
        logger.error(f"❌ Categories không phải list: {type(categories)}")
        return []

    logger.info(
        f"✅ Đã lấy {len(categories)} categories, tạo {len(categories)} tasks cho Dynamic Task Mapping"
    )

    # Trả về list các dict để expand
    return [{"category": cat} for cat in categories]