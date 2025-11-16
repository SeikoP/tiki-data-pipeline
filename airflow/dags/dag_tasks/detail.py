"""
Detail crawl tasks for Tiki crawl products DAG
"""
import os
import sys

# CRITICAL: Phải thêm sys.path TRƯỚC tất cả imports khác
_dags_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _dags_dir not in sys.path:
    sys.path.insert(0, _dags_dir)

import json
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Any

# Sử dụng absolute imports thay vì relative imports
from dag_helpers.config import DETAIL_CACHE_DIR, OUTPUT_FILE, OUTPUT_FILE_WITH_DETAIL, PROGRESS_FILE
from dag_helpers.shared_state import (
    tiki_circuit_breaker,
    tiki_dlq,
    tiki_degradation,
    crawl_product_detail_with_selenium,
    extract_product_detail,
    CircuitBreakerOpenError,
    classify_error,
)
from dag_helpers.utils import Variable, get_logger, atomic_write_file


def prepare_products_for_detail(**context) -> list[dict[str, Any]]:
    """
    Task: Chuẩn bị danh sách products để crawl detail

    Tối ưu cho multi-day crawling:
    - Chỉ crawl products chưa có detail
    - Chia thành batches theo ngày (có thể crawl trong nhiều ngày)
    - Kiểm tra cache và progress để tránh crawl lại
    - Track progress để resume từ điểm dừng

    Returns:
        List[Dict]: List các dict chứa product info cho Dynamic Task Mapping
    """
    logger = get_logger(context)
    logger.info("=" * 70)
    logger.info("📋 TASK: Prepare Products for Detail Crawling (Multi-Day Support)")
    logger.info("=" * 70)

    try:
        ti = context["ti"]

        # Lấy products từ task save_products
        merge_result = None
        try:
            merge_result = ti.xcom_pull(task_ids="process_and_save.merge_products")
        except Exception:
            try:
                merge_result = ti.xcom_pull(task_ids="merge_products")
            except Exception:
                pass

        if not merge_result:
            # Thử lấy từ file output
            if OUTPUT_FILE.exists():
                with open(OUTPUT_FILE, encoding="utf-8") as f:
                    data = json.load(f)
                    merge_result = {"products": data.get("products", [])}

        if not merge_result:
            raise ValueError("Không tìm thấy products từ XCom hoặc file")

        products = merge_result.get("products", [])
        logger.info(f"📊 Tổng số products: {len(products)}")

        # Đọc progress file để biết đã crawl đến đâu
        progress = {
            "crawled_product_ids": set(),
            "last_crawled_index": 0,
            "total_crawled": 0,
            "last_updated": None,
        }

        if PROGRESS_FILE.exists():
            try:
                with open(PROGRESS_FILE, encoding="utf-8") as f:
                    saved_progress = json.load(f)
                    progress["crawled_product_ids"] = set(
                        saved_progress.get("crawled_product_ids", [])
                    )
                    progress["last_crawled_index"] = saved_progress.get("last_crawled_index", 0)
                    progress["total_crawled"] = saved_progress.get("total_crawled", 0)
                    progress["last_updated"] = saved_progress.get("last_updated")
                    logger.info(
                        f"📂 Đã load progress: {len(progress['crawled_product_ids'])} products đã crawl"
                    )
            except Exception as e:
                logger.warning(f"⚠️  Không đọc được progress file: {e}")

        # Lọc products cần crawl detail
        products_to_crawl = []
        cache_hits = 0
        already_crawled = 0

        # Lấy cấu hình cho multi-day crawling
        # Tính toán: 500 products ~ 52.75 phút -> 280 products ~ 30 phút
        products_per_day = int(
            Variable.get("TIKI_PRODUCTS_PER_DAY", default_var="245")
        )  # Mặc định 280 products/ngày (~30 phút)
        max_products = int(
            Variable.get("TIKI_MAX_PRODUCTS_FOR_DETAIL", default_var="0")
        )  # 0 = không giới hạn

        logger.info(
            f"⚙️  Cấu hình: {products_per_day} products/ngày, max: {max_products if max_products > 0 else 'không giới hạn'}"
        )

        # Bắt đầu từ index đã crawl
        start_index = progress["last_crawled_index"]
        products_to_check = products[start_index:]

        logger.info(
            f"🔄 Bắt đầu từ index {start_index} (đã crawl {progress['total_crawled']} products)"
        )

        for idx, product in enumerate(products_to_check):
            product_id = product.get("product_id")
            product_url = product.get("url")

            if not product_id or not product_url:
                continue

            # Kiểm tra xem đã crawl chưa (từ progress)
            if product_id in progress["crawled_product_ids"]:
                already_crawled += 1
                continue

            # Kiểm tra cache
            cache_file = DETAIL_CACHE_DIR / f"{product_id}.json"
            has_valid_cache = False
            if cache_file.exists():
                try:
                    with open(cache_file, encoding="utf-8") as f:
                        cached_detail = json.load(f)
                        # Kiểm tra cache có đầy đủ không: cần có price và sales_count
                        has_price = cached_detail.get("price", {}).get("current_price")
                        has_sales_count = cached_detail.get("sales_count") is not None

                        # Nếu đã có detail đầy đủ (có price và sales_count), đánh dấu đã crawl
                        if has_price and has_sales_count:
                            cache_hits += 1
                            progress["crawled_product_ids"].add(product_id)
                            already_crawled += 1
                            has_valid_cache = True
                        # Nếu cache thiếu sales_count, vẫn cần crawl lại
                except Exception:
                    pass

            # Nếu chưa có cache hợp lệ, thêm vào danh sách crawl
            if not has_valid_cache:
                products_to_crawl.append(
                    {
                        "product_id": product_id,
                        "url": product_url,
                        "name": product.get("name", ""),
                        "product": product,  # Giữ nguyên product data
                        "index": start_index + idx,  # Lưu index để track progress
                    }
                )

            # Giới hạn số lượng products crawl trong ngày này
            if len(products_to_crawl) >= products_per_day:
                logger.info(f"✓ Đã đạt giới hạn {products_per_day} products cho ngày hôm nay")
                break

            # Giới hạn tổng số (nếu có)
            if max_products > 0 and len(products_to_crawl) >= max_products:
                logger.info(f"✓ Đã đạt giới hạn tổng {max_products} products")
                break

        logger.info(f"✅ Products cần crawl hôm nay: {len(products_to_crawl)}")
        logger.info(f"📦 Cache hits: {cache_hits}")
        logger.info(f"✓ Đã crawl trước đó: {already_crawled}")
        logger.info(f"📈 Tổng đã crawl: {progress['total_crawled'] + already_crawled}")
        logger.info(
            f"📉 Còn lại: {len(products) - (progress['total_crawled'] + already_crawled + len(products_to_crawl))}"
        )

        # Lưu progress (sẽ được cập nhật sau khi crawl xong)
        if products_to_crawl:
            # Lưu index của product cuối cùng sẽ được crawl
            last_index = products_to_crawl[-1]["index"]
            progress["last_crawled_index"] = last_index + 1
            progress["last_updated"] = datetime.now().isoformat()

            # Lưu progress vào file
            try:
                with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
                    json.dump(
                        {
                            "crawled_product_ids": list(progress["crawled_product_ids"]),
                            "last_crawled_index": progress["last_crawled_index"],
                            "total_crawled": progress["total_crawled"] + already_crawled,
                            "last_updated": progress["last_updated"],
                        },
                        f,
                        ensure_ascii=False,
                        indent=2,
                    )
                logger.info(f"💾 Đã lưu progress: index {progress['last_crawled_index']}")
            except Exception as e:
                logger.warning(f"⚠️  Không lưu được progress: {e}")

        # Debug: Log một vài products đầu tiên
        if products_to_crawl:
            logger.info("📋 Sample products (first 3):")
            for i, p in enumerate(products_to_crawl[:3]):
                logger.info(
                    f"  {i+1}. Product ID: {p.get('product_id')}, URL: {p.get('url')[:80]}..."
                )
        else:
            logger.warning("⚠️  Không có products nào cần crawl detail hôm nay!")
            logger.info("💡 Tất cả products đã được crawl hoặc có cache hợp lệ")

        logger.info(f"🔢 Trả về {len(products_to_crawl)} products cho Dynamic Task Mapping")

        return products_to_crawl

    except Exception as e:
        logger.error(f"❌ Lỗi khi prepare products: {e}", exc_info=True)
        raise

def crawl_single_product_detail(product_info: dict[str, Any] = None, **context) -> dict[str, Any]:
    """
    Task: Crawl detail cho một product (Dynamic Task Mapping)

    Tối ưu:
    - Sử dụng cache để tránh crawl lại
    - Rate limiting
    - Error handling: tiếp tục với product khác khi lỗi
    - Atomic write cache

    Args:
        product_info: Thông tin product (từ expand_kwargs)
        context: Airflow context

    Returns:
        Dict: Kết quả crawl với detail và metadata
    """
    # Khởi tạo result mặc định
    default_result = {
        "product_id": "unknown",
        "url": "",
        "status": "failed",
        "error": None,
        "detail": None,
        "crawled_at": datetime.now().isoformat(),
    }

    try:
        logger = get_logger(context)
    except Exception as e:
        # Nếu không thể tạo logger, vẫn tiếp tục với default result
        import logging

        logger = logging.getLogger("airflow.task")
        logger.error(f"Không thể tạo logger từ context: {e}")

    # Lấy product_info từ keyword argument hoặc context
    if not product_info:
        ti = context.get("ti")
        if ti:
            op_kwargs = getattr(ti, "op_kwargs", {})
            if op_kwargs:
                product_info = op_kwargs.get("product_info")

        if not product_info:
            product_info = context.get("product_info") or context.get("op_kwargs", {}).get(
                "product_info"
            )

    if not product_info:
        logger.error(f"Không tìm thấy product_info. Context keys: {list(context.keys())}")
        # Return result với status failed thay vì raise exception
        return {
            "product_id": "unknown",
            "url": "",
            "status": "failed",
            "error": "Không tìm thấy product_info trong context",
            "detail": None,
            "crawled_at": datetime.now().isoformat(),
        }

    product_id = product_info.get("product_id", "")
    product_url = product_info.get("url", "")
    product_name = product_info.get("name", "Unknown")

    logger.info("=" * 70)
    logger.info(f"🔍 TASK: Crawl Product Detail - {product_name}")
    logger.info(f"🔗 URL: {product_url}")
    logger.info("=" * 70)

    result = {
        "product_id": product_id,
        "url": product_url,
        "status": "failed",
        "error": None,
        "detail": None,
        "crawled_at": datetime.now().isoformat(),
    }

    # Kiểm tra cache trước - ưu tiên Redis, fallback về file
    # Thử Redis cache trước (nhanh hơn, distributed)
    redis_cache = None
    try:
        from pipelines.crawl.storage.redis_cache import get_redis_cache

        redis_cache = get_redis_cache("redis://redis:6379/1")
        if redis_cache:
            cached_detail = redis_cache.get_cached_product_detail(product_id)
            if cached_detail:
                # Kiểm tra cache có đầy đủ không: cần có price và sales_count
                has_price = cached_detail.get("price", {}).get("current_price")
                has_sales_count = cached_detail.get("sales_count") is not None

                # Nếu đã có detail đầy đủ (có price và sales_count), dùng cache
                if has_price and has_sales_count:
                    logger.info(
                        f"[Redis Cache] ✅ Hit cache cho product {product_id} (có price và sales_count)"
                    )
                    result["detail"] = cached_detail
                    result["status"] = "cached"
                    return result
                elif has_price:
                    # Cache có price nhưng thiếu sales_count → crawl lại để lấy sales_count
                    logger.info(
                        f"[Redis Cache] ⚠️  Cache thiếu sales_count cho product {product_id}, sẽ crawl lại"
                    )
                else:
                    # Cache không đầy đủ → crawl lại
                    logger.info(
                        f"[Redis Cache] ⚠️  Cache không đầy đủ cho product {product_id}, sẽ crawl lại"
                    )
    except Exception:
        # Redis không available, fallback về file cache
        pass

    # Fallback: Kiểm tra file cache
    cache_file = DETAIL_CACHE_DIR / f"{product_id}.json"
    if cache_file.exists():
        try:
            with open(cache_file, encoding="utf-8") as f:
                cached_detail = json.load(f)
                # Kiểm tra cache có đầy đủ không: cần có price và sales_count
                has_price = cached_detail.get("price", {}).get("current_price")
                has_sales_count = cached_detail.get("sales_count") is not None

                # Nếu đã có detail đầy đủ (có price và sales_count), dùng cache
                if has_price and has_sales_count:
                    logger.info(
                        f"[File Cache] ✅ Hit cache cho product {product_id} (có price và sales_count)"
                    )
                    result["detail"] = cached_detail
                    result["status"] = "cached"
                    return result
                elif has_price:
                    # Cache có price nhưng thiếu sales_count → crawl lại để lấy sales_count
                    logger.info(
                        f"[File Cache] ⚠️  Cache thiếu sales_count cho product {product_id}, sẽ crawl lại"
                    )
                else:
                    # Cache không đầy đủ → crawl lại
                    logger.info(
                        f"[File Cache] ⚠️  Cache không đầy đủ cho product {product_id}, sẽ crawl lại"
                    )
        except Exception as e:
            logger.warning(f"Không đọc được cache: {e}")

    try:
        # Kiểm tra graceful degradation
        if tiki_degradation.should_skip():
            result["error"] = "Service đang ở trạng thái FAILED, skip crawl"
            result["status"] = "degraded"
            logger.warning(f"⚠️  Service degraded, skip product {product_id}")
            return result

        # Validate URL
        if not product_url or not product_url.startswith("http"):
            raise ValueError(f"URL không hợp lệ: {product_url}")

        # Lấy cấu hình
        rate_limit_delay = float(
            Variable.get("TIKI_DETAIL_RATE_LIMIT_DELAY", default_var="2.0")
        )  # Delay 2s cho detail
        timeout = int(
            Variable.get("TIKI_DETAIL_CRAWL_TIMEOUT", default_var="180")
        )  # 3 phút mỗi product (tăng từ 120s để tránh timeout)

        # Rate limiting
        if rate_limit_delay > 0:
            time.sleep(rate_limit_delay)

        # Crawl với timeout và circuit breaker
        start_time = time.time()

        # Sử dụng Selenium để crawl detail (cần thiết cho dynamic content)
        html_content = None
        try:
            # Wrapper function để gọi với circuit breaker
            def _crawl_detail_with_params():
                """Wrapper function để gọi với circuit breaker"""
                return crawl_product_detail_with_selenium(
                    product_url,
                    save_html=False,
                    verbose=False,  # Không verbose trong Airflow
                    max_retries=3,  # Retry 3 lần (tăng từ 2)
                    timeout=60,  # Timeout 60s (tăng từ 25s để đủ thời gian cho Selenium)
                    use_redis_cache=True,  # Sử dụng Redis cache
                    use_rate_limiting=True,  # Sử dụng rate limiting
                )

            try:
                # Gọi với circuit breaker
                html_content = tiki_circuit_breaker.call(_crawl_detail_with_params)
                tiki_degradation.record_success()
            except CircuitBreakerOpenError as e:
                # Circuit breaker đang mở
                result["error"] = f"Circuit breaker open: {str(e)}"
                result["status"] = "circuit_breaker_open"
                logger.warning(f"⚠️  Circuit breaker open cho product {product_id}: {e}")
                # Thêm vào DLQ
                try:
                    crawl_error = classify_error(
                        e, context={"product_url": product_url, "product_id": product_id}
                    )
                    tiki_dlq.add(
                        task_id=f"crawl_detail_{product_id}",
                        task_type="crawl_product_detail",
                        error=crawl_error,
                        context={
                            "product_url": product_url,
                            "product_name": product_name,
                            "product_id": product_id,
                        },
                        retry_count=0,
                    )
                    logger.info(f"📬 Đã thêm vào DLQ: crawl_detail_{product_id}")
                except Exception as dlq_error:
                    logger.warning(f"⚠️  Không thể thêm vào DLQ: {dlq_error}")
                return result
            except Exception:
                # Ghi nhận failure
                tiki_degradation.record_failure()
                raise  # Re-raise để xử lý bên dưới

            if not html_content or len(html_content) < 100:
                raise ValueError(
                    f"HTML content quá ngắn hoặc rỗng: {len(html_content) if html_content else 0} ký tự"
                )

        except Exception as selenium_error:
            # Log lỗi Selenium chi tiết
            error_type = type(selenium_error).__name__
            error_msg = str(selenium_error)

            # Rút gọn error message nếu quá dài
            if len(error_msg) > 200:
                error_msg = error_msg[:200] + "..."

            logger.error(f"❌ Lỗi Selenium ({error_type}): {error_msg}")

            # Kiểm tra các lỗi phổ biến và phân loại
            error_msg_lower = error_msg.lower()
            if (
                "chrome" in error_msg_lower
                or "driver" in error_msg_lower
                or "webdriver" in error_msg_lower
            ):
                result["error"] = f"Chrome/Driver error: {error_msg}"
                result["status"] = "selenium_error"
            elif (
                "timeout" in error_msg_lower
                or "timed out" in error_msg_lower
                or "time-out" in error_msg_lower
            ):
                result["error"] = f"Timeout: {error_msg}"
                result["status"] = "timeout"
            elif (
                "connection" in error_msg_lower
                or "network" in error_msg_lower
                or "refused" in error_msg_lower
            ):
                result["error"] = f"Network error: {error_msg}"
                result["status"] = "network_error"
            elif "memory" in error_msg_lower or "out of memory" in error_msg_lower:
                result["error"] = f"Memory error: {error_msg}"
                result["status"] = "memory_error"
            else:
                result["error"] = f"Selenium error: {error_msg}"
                result["status"] = "failed"

            # Ghi nhận failure và thêm vào DLQ
            tiki_degradation.record_failure()
            try:
                crawl_error = classify_error(
                    selenium_error, context={"product_url": product_url, "product_id": product_id}
                )
                tiki_dlq.add(
                    task_id=f"crawl_detail_{product_id}",
                    task_type="crawl_product_detail",
                    error=crawl_error,
                    context={
                        "product_url": product_url,
                        "product_name": product_name,
                        "product_id": product_id,
                    },
                    retry_count=0,
                )
                logger.info(f"📬 Đã thêm vào DLQ: crawl_detail_{product_id}")
            except Exception as dlq_error:
                logger.warning(f"⚠️  Không thể thêm vào DLQ: {dlq_error}")
            # Không raise, return result với status failed
            return result

        # Extract detail
        try:
            detail = extract_product_detail(html_content, product_url, verbose=False)

            if not detail:
                raise ValueError("Không extract được detail từ HTML")

        except Exception as extract_error:
            error_type = type(extract_error).__name__
            error_msg = str(extract_error)
            logger.error(f"❌ Lỗi khi extract detail ({error_type}): {error_msg}")
            result["error"] = f"Extract error: {error_msg}"
            result["status"] = "extract_error"
            # Ghi nhận failure và thêm vào DLQ
            tiki_degradation.record_failure()
            try:
                crawl_error = classify_error(
                    extract_error, context={"product_url": product_url, "product_id": product_id}
                )
                tiki_dlq.add(
                    task_id=f"crawl_detail_{product_id}",
                    task_type="crawl_product_detail",
                    error=crawl_error,
                    context={
                        "product_url": product_url,
                        "product_name": product_name,
                        "product_id": product_id,
                    },
                    retry_count=0,
                )
                logger.info(f"📬 Đã thêm vào DLQ: crawl_detail_{product_id}")
            except Exception as dlq_error:
                logger.warning(f"⚠️  Không thể thêm vào DLQ: {dlq_error}")
            return result

        elapsed = time.time() - start_time

        if elapsed > timeout:
            raise TimeoutError(
                f"Crawl detail vượt quá timeout {timeout}s (elapsed: {elapsed:.1f}s)"
            )

        result["detail"] = detail
        result["status"] = "success"
        result["elapsed_time"] = elapsed

        # Lưu vào cache - ưu tiên Redis, fallback về file
        # Redis cache (nhanh, distributed)
        if redis_cache:
            try:
                redis_cache.cache_product_detail(product_id, detail, ttl=604800)  # 7 ngày
                logger.info(f"[Redis Cache] ✅ Đã cache detail cho product {product_id}")
            except Exception as e:
                logger.warning(f"[Redis Cache] ⚠️  Lỗi khi cache vào Redis: {e}")

        # File cache (fallback)
        try:
            # Đảm bảo thư mục cache tồn tại
            DETAIL_CACHE_DIR.mkdir(parents=True, exist_ok=True)

            temp_file = cache_file.with_suffix(".tmp")
            logger.debug(f"💾 Đang lưu cache vào: {cache_file}")

            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(detail, f, ensure_ascii=False, indent=2)

            # Atomic move
            if os.name == "nt":  # Windows
                if cache_file.exists():
                    cache_file.unlink()
                shutil.move(str(temp_file), str(cache_file))
            else:  # Unix/Linux
                os.rename(str(temp_file), str(cache_file))

            # Verify cache file was created
            if cache_file.exists():
                logger.info(f"✅ Crawl thành công: {elapsed:.1f}s, đã cache vào {cache_file}")
                # Log sales_count nếu có
                if detail.get("sales_count") is not None:
                    logger.info(f"   📊 sales_count: {detail.get('sales_count')}")
                else:
                    logger.warning("   ⚠️  sales_count: None (không tìm thấy)")
            else:
                logger.error(f"❌ Cache file không được tạo: {cache_file}")
        except Exception as e:
            logger.error(f"❌ Không lưu được cache: {e}", exc_info=True)
            # Không fail task vì đã crawl thành công, chỉ không lưu được cache

    except TimeoutError as e:
        result["error"] = str(e)
        result["status"] = "timeout"
        tiki_degradation.record_failure()
        logger.error(f"⏱️  Timeout: {e}")
        # Thêm vào DLQ
        try:
            crawl_error = classify_error(
                e, context={"product_url": product_url, "product_id": product_id}
            )
            tiki_dlq.add(
                task_id=f"crawl_detail_{product_id}",
                task_type="crawl_product_detail",
                error=crawl_error,
                context={
                    "product_url": product_url,
                    "product_name": product_name,
                    "product_id": product_id,
                },
                retry_count=0,
            )
            logger.info(f"📬 Đã thêm vào DLQ: crawl_detail_{product_id}")
        except Exception as dlq_error:
            logger.warning(f"⚠️  Không thể thêm vào DLQ: {dlq_error}")

    except ValueError as e:
        result["error"] = str(e)
        result["status"] = "validation_error"
        tiki_degradation.record_failure()
        logger.error(f"❌ Validation error: {e}")
        # Thêm vào DLQ
        try:
            crawl_error = classify_error(
                e, context={"product_url": product_url, "product_id": product_id}
            )
            tiki_dlq.add(
                task_id=f"crawl_detail_{product_id}",
                task_type="crawl_product_detail",
                error=crawl_error,
                context={
                    "product_url": product_url,
                    "product_name": product_name,
                    "product_id": product_id,
                },
                retry_count=0,
            )
            logger.info(f"📬 Đã thêm vào DLQ: crawl_detail_{product_id}")
        except Exception as dlq_error:
            logger.warning(f"⚠️  Không thể thêm vào DLQ: {dlq_error}")

    except Exception as e:
        result["error"] = str(e)
        result["status"] = "failed"
        tiki_degradation.record_failure()
        error_type = type(e).__name__
        logger.error(f"❌ Lỗi khi crawl detail ({error_type}): {e}", exc_info=True)
        # Thêm vào DLQ
        try:
            crawl_error = classify_error(
                e, context={"product_url": product_url, "product_id": product_id}
            )
            tiki_dlq.add(
                task_id=f"crawl_detail_{product_id}",
                task_type="crawl_product_detail",
                error=crawl_error,
                context={
                    "product_url": product_url,
                    "product_name": product_name,
                    "product_id": product_id,
                },
                retry_count=0,
            )
            logger.info(f"📬 Đã thêm vào DLQ: crawl_detail_{product_id}")
        except Exception as dlq_error:
            logger.warning(f"⚠️  Không thể thêm vào DLQ: {dlq_error}")
        # Không raise để tiếp tục với product khác

    # Đảm bảo luôn return result, không bao giờ raise exception
    try:
        return result
    except Exception as e:
        # Nếu có lỗi khi return (không thể xảy ra nhưng để an toàn)
        logger.error(f"❌ Lỗi khi return result: {e}", exc_info=True)
        default_result["error"] = f"Lỗi khi return result: {str(e)}"
        return default_result

def merge_product_details(**context) -> dict[str, Any]:
    """
    Task: Merge product details vào products list

    Returns:
        Dict: Products với detail đã merge
    """
    logger = get_logger(context)
    logger.info("=" * 70)
    logger.info("🔄 TASK: Merge Product Details")
    logger.info("=" * 70)

    try:
        ti = context["ti"]

        # Lấy products gốc
        merge_result = None
        try:
            merge_result = ti.xcom_pull(task_ids="process_and_save.merge_products")
        except Exception:
            try:
                merge_result = ti.xcom_pull(task_ids="merge_products")
            except Exception:
                pass

        if not merge_result:
            # Thử lấy từ file
            if OUTPUT_FILE.exists():
                with open(OUTPUT_FILE, encoding="utf-8") as f:
                    data = json.load(f)
                    merge_result = {"products": data.get("products", [])}

        if not merge_result:
            raise ValueError("Không tìm thấy products từ XCom hoặc file")

        products = merge_result.get("products", [])
        logger.info(f"Tổng số products: {len(products)}")

        # Lấy số lượng products thực tế được crawl từ prepare_products_for_detail
        # Đây là số lượng map_index thực tế, không phải tổng số products
        products_to_crawl = None
        try:
            products_to_crawl = ti.xcom_pull(
                task_ids="crawl_product_details.prepare_products_for_detail"
            )
        except Exception:
            try:
                products_to_crawl = ti.xcom_pull(task_ids="prepare_products_for_detail")
            except Exception:
                pass

        # Số lượng products thực tế được crawl (map_index count)
        expected_crawl_count = len(products_to_crawl) if products_to_crawl else 0
        logger.info(
            f"📊 Số products dự kiến được crawl (từ prepare_products_for_detail): {expected_crawl_count}"
        )

        # Tự động phát hiện số lượng map_index thực tế có sẵn bằng cách thử lấy XCom
        # Điều này giúp xử lý trường hợp một số tasks đã fail hoặc chưa chạy xong
        actual_crawl_count = expected_crawl_count
        if expected_crawl_count > 0:
            # Thử lấy XCom từ map_index cuối cùng để xác định số lượng thực tế
            # Tìm map_index cao nhất có XCom
            task_id = "crawl_product_details.crawl_product_detail"
            max_found_index = -1

            # Binary search để tìm map_index cao nhất có XCom (tối ưu hơn linear search)
            # Thử một số điểm để tìm max index
            logger.info(
                f"🔍 Đang phát hiện số lượng map_index thực tế (dự kiến: {expected_crawl_count})..."
            )
            test_indices = []
            if expected_crawl_count > 1000:
                # Với số lượng lớn, test một số điểm để tìm max
                step = max(100, expected_crawl_count // 20)
                test_indices = list(range(0, expected_crawl_count, step))
                test_indices.append(expected_crawl_count - 1)
            elif expected_crawl_count > 100:
                # Với số lượng trung bình, test nhiều điểm hơn
                step = max(50, expected_crawl_count // 10)
                test_indices = list(range(0, expected_crawl_count, step))
                test_indices.append(expected_crawl_count - 1)
            else:
                # Với số lượng nhỏ, test tất cả
                test_indices = list(range(expected_crawl_count))

            # Tìm từ cuối về đầu để tìm max index nhanh hơn
            for test_idx in reversed(test_indices):
                try:
                    result = ti.xcom_pull(
                        task_ids=task_id, key="return_value", map_indexes=[test_idx]
                    )
                    if result:
                        max_found_index = test_idx
                        logger.info(f"✅ Tìm thấy XCom tại map_index {test_idx}")
                        break
                except Exception as e:
                    logger.debug(f"   Không có XCom tại map_index {test_idx}: {e}")
                    pass

            if max_found_index >= 0:
                # Tìm chính xác map_index cao nhất bằng cách tìm từ max_found_index
                # Chỉ thử thêm tối đa 200 map_index tiếp theo để tránh quá lâu
                logger.info(f"🔍 Đang tìm chính xác max index từ {max_found_index}...")
                search_range = min(max_found_index + 200, expected_crawl_count)
                for idx in range(max_found_index + 1, search_range):
                    try:
                        result = ti.xcom_pull(
                            task_ids=task_id, key="return_value", map_indexes=[idx]
                        )
                        if result:
                            max_found_index = idx
                        else:
                            # Nếu không có result, dừng lại (có thể đã đến cuối)
                            break
                    except Exception as e:
                        # Nếu exception, có thể là hết map_index
                        logger.debug(f"   Không có XCom tại map_index {idx}: {e}")
                        break

                actual_crawl_count = max_found_index + 1
                logger.info(
                    f"✅ Phát hiện {actual_crawl_count} map_index thực tế có XCom (dự kiến: {expected_crawl_count})"
                )
            else:
                logger.warning(
                    f"⚠️  Không tìm thấy XCom nào, sử dụng expected_crawl_count: {expected_crawl_count}. "
                    f"Có thể tất cả tasks đã fail hoặc chưa chạy xong."
                )
                actual_crawl_count = expected_crawl_count

        if actual_crawl_count == 0:
            logger.warning("⚠️  Không có products nào được crawl detail, bỏ qua merge detail")
            # Trả về products gốc không có detail
            return {
                "products": products,
                "stats": {
                    "total_products": len(products),
                    "with_detail": 0,
                    "cached": 0,
                    "failed": 0,
                    "timeout": 0,
                },
                "merged_at": datetime.now().isoformat(),
            }

        # Lấy detail results từ Dynamic Task Mapping
        task_id = "crawl_product_details.crawl_product_detail"
        all_detail_results = []

        # Lấy tất cả results bằng cách lấy từng map_index để tránh giới hạn XCom
        # CHỈ lấy từ map_index 0 đến actual_crawl_count - 1 (không phải len(products))
        logger.info(f"Bắt đầu lấy detail results từ {actual_crawl_count} crawled products...")

        # Lấy theo batch để tối ưu
        batch_size = 100
        total_batches = (actual_crawl_count + batch_size - 1) // batch_size
        logger.info(
            f"📦 Sẽ lấy {actual_crawl_count} results trong {total_batches} batches (mỗi batch {batch_size})"
        )

        for batch_num, start_idx in enumerate(range(0, actual_crawl_count, batch_size), 1):
            end_idx = min(start_idx + batch_size, actual_crawl_count)
            batch_map_indexes = list(range(start_idx, end_idx))

            # Heartbeat: log mỗi batch để Airflow biết task vẫn đang chạy
            if batch_num % 5 == 0 or batch_num == 1:
                logger.info(
                    f"💓 [Heartbeat] Đang xử lý batch {batch_num}/{total_batches} (index {start_idx}-{end_idx-1})..."
                )

            try:
                batch_results = ti.xcom_pull(
                    task_ids=task_id, key="return_value", map_indexes=batch_map_indexes
                )

                if batch_results:
                    if isinstance(batch_results, list):
                        # List results theo thứ tự map_indexes
                        all_detail_results.extend([r for r in batch_results if r])
                    elif isinstance(batch_results, dict):
                        # Dict với key là map_index hoặc string
                        # Lấy tất cả values, sắp xếp theo map_index nếu có thể
                        values = [v for v in batch_results.values() if v]
                        all_detail_results.extend(values)
                    else:
                        # Single result
                        all_detail_results.append(batch_results)

                # Log progress mỗi 5 batches hoặc mỗi 10% progress
                if batch_num % max(5, total_batches // 10) == 0:
                    progress_pct = (
                        (len(all_detail_results) / actual_crawl_count * 100)
                        if actual_crawl_count > 0
                        else 0
                    )
                    logger.info(
                        f"📊 Đã lấy {len(all_detail_results)}/{actual_crawl_count} results ({progress_pct:.1f}%)..."
                    )
            except Exception as e:
                logger.warning(f"⚠️  Lỗi khi lấy batch {start_idx}-{end_idx}: {e}")
                logger.warning("   Sẽ thử lấy từng map_index riêng lẻ trong batch này...")
                # Thử lấy từng map_index riêng lẻ trong batch này
                for map_index in batch_map_indexes:
                    try:
                        result = ti.xcom_pull(
                            task_ids=task_id, key="return_value", map_indexes=[map_index]
                        )
                        if result:
                            if isinstance(result, list):
                                all_detail_results.extend([r for r in result if r])
                            elif isinstance(result, dict):
                                all_detail_results.append(result)
                            else:
                                all_detail_results.append(result)
                    except Exception as e2:
                        # Bỏ qua nếu không lấy được (có thể task chưa chạy xong hoặc failed)
                        logger.debug(f"   Không lấy được map_index {map_index}: {e2}")
                        pass

        logger.info(
            f"✅ Lấy được {len(all_detail_results)} detail results qua batch (mong đợi {actual_crawl_count})"
        )

        # Nếu không lấy đủ hoặc có lỗi khi lấy batch, thử lấy từng map_index một để bù vào phần thiếu
        # KHÔNG reset all_detail_results, chỉ lấy thêm những map_index chưa có
        if len(all_detail_results) < actual_crawl_count * 0.8:  # Nếu thiếu hơn 20%
            # Log cảnh báo nếu thiếu nhiều
            missing_pct = (
                ((actual_crawl_count - len(all_detail_results)) / actual_crawl_count * 100)
                if actual_crawl_count > 0
                else 0
            )
            if missing_pct > 30:
                logger.warning(
                    f"⚠️  Thiếu {missing_pct:.1f}% results ({actual_crawl_count - len(all_detail_results)}/{actual_crawl_count}), "
                    f"có thể do nhiều tasks failed hoặc timeout"
                )
            logger.warning(
                f"⚠️  Chỉ lấy được {len(all_detail_results)}/{actual_crawl_count} results qua batch, "
                f"thử lấy từng map_index để bù vào phần thiếu..."
            )

            # Tạo set các product_id đã có để tránh duplicate
            existing_product_ids = set()
            for result in all_detail_results:
                if isinstance(result, dict) and result.get("product_id"):
                    existing_product_ids.add(result.get("product_id"))

            missing_count = actual_crawl_count - len(all_detail_results)
            logger.info(
                f"📊 Cần lấy thêm ~{missing_count} results từ {actual_crawl_count} map_indexes"
            )

            # Heartbeat: log thường xuyên trong vòng lặp dài
            fetched_count = 0
            for map_index in range(actual_crawl_count):  # CHỈ lấy từ 0 đến actual_crawl_count - 1
                # Heartbeat mỗi 100 items để tránh timeout
                if map_index % 100 == 0 and map_index > 0:
                    logger.info(
                        f"💓 [Heartbeat] Đang lấy từng map_index: {map_index}/{actual_crawl_count} "
                        f"(đã lấy {len(all_detail_results)}/{actual_crawl_count})..."
                    )

                try:
                    result = ti.xcom_pull(
                        task_ids=task_id, key="return_value", map_indexes=[map_index]
                    )
                    if result:
                        # Chỉ thêm nếu chưa có (tránh duplicate)
                        product_id_to_check = None
                        if isinstance(result, dict):
                            product_id_to_check = result.get("product_id")
                        elif (
                            isinstance(result, list)
                            and len(result) > 0
                            and isinstance(result[0], dict)
                        ):
                            product_id_to_check = result[0].get("product_id")

                        # Chỉ thêm nếu product_id chưa có trong danh sách
                        if (
                            not product_id_to_check
                            or product_id_to_check not in existing_product_ids
                        ):
                            if isinstance(result, list):
                                for r in result:
                                    if isinstance(r, dict) and r.get("product_id"):
                                        existing_product_ids.add(r.get("product_id"))
                                all_detail_results.extend([r for r in result if r])
                            elif isinstance(result, dict):
                                if product_id_to_check:
                                    existing_product_ids.add(product_id_to_check)
                                all_detail_results.append(result)
                            else:
                                all_detail_results.append(result)
                            fetched_count += 1

                    # Log progress mỗi 200 items
                    if (map_index + 1) % 200 == 0:
                        progress_pct = (
                            (len(all_detail_results) / actual_crawl_count * 100)
                            if actual_crawl_count > 0
                            else 0
                        )
                        logger.info(
                            f"📊 Đã lấy tổng {len(all_detail_results)}/{actual_crawl_count} results ({progress_pct:.1f}%) từng map_index..."
                        )
                except Exception as e:
                    # Bỏ qua nếu không lấy được (có thể task chưa chạy xong hoặc failed)
                    logger.debug(f"   Không lấy được map_index {map_index}: {e}")
                    pass

            logger.info(
                f"✅ Sau khi lấy từng map_index: tổng {len(all_detail_results)} detail results (lấy thêm {fetched_count})"
            )

        # Tạo dict để lookup nhanh
        detail_dict = {}
        stats = {
            "total_products": len(products),
            "crawled_count": 0,  # Số lượng products thực sự được crawl detail
            "with_detail": 0,
            "cached": 0,
            "failed": 0,
            "timeout": 0,
            "degraded": 0,
            "circuit_breaker_open": 0,
        }

        logger.info(f"📊 Đang xử lý {len(all_detail_results)} detail results...")

        # Kiểm tra nếu có quá nhiều kết quả None hoặc invalid
        valid_results = 0
        error_details = {}  # Thống kê chi tiết các loại lỗi
        failed_products = []  # Danh sách products bị fail để phân tích

        for detail_result in all_detail_results:
            if detail_result and isinstance(detail_result, dict):
                product_id = detail_result.get("product_id")
                if product_id:
                    detail_dict[product_id] = detail_result
                    valid_results += 1
                    status = detail_result.get("status", "failed")
                    error = detail_result.get("error")

                    # Đếm số lượng products được crawl (tất cả các status trừ "not_crawled")
                    if status in ["success", "cached", "failed", "timeout", "degraded", "circuit_breaker_open", "selenium_error", "network_error", "extract_error", "validation_error", "memory_error"]:
                        stats["crawled_count"] += 1
                    
                    if status == "success":
                        stats["with_detail"] += 1
                    elif status == "cached":
                        stats["cached"] += 1
                    elif status == "timeout":
                        stats["timeout"] += 1
                        error_details["timeout"] = error_details.get("timeout", 0) + 1
                        failed_products.append(
                            {"product_id": product_id, "status": status, "error": error}
                        )
                    elif status == "degraded":
                        stats["degraded"] += 1
                        error_details["degraded"] = error_details.get("degraded", 0) + 1
                        failed_products.append(
                            {"product_id": product_id, "status": status, "error": error}
                        )
                    elif status == "circuit_breaker_open":
                        stats["circuit_breaker_open"] += 1
                        error_details["circuit_breaker_open"] = (
                            error_details.get("circuit_breaker_open", 0) + 1
                        )
                        failed_products.append(
                            {"product_id": product_id, "status": status, "error": error}
                        )
                    elif status == "selenium_error":
                        stats["failed"] += 1
                        error_details["selenium_error"] = error_details.get("selenium_error", 0) + 1
                        failed_products.append(
                            {"product_id": product_id, "status": status, "error": error}
                        )
                    elif status == "extract_error":
                        stats["failed"] += 1
                        error_details["extract_error"] = error_details.get("extract_error", 0) + 1
                        failed_products.append(
                            {"product_id": product_id, "status": status, "error": error}
                        )
                    elif status == "network_error":
                        stats["failed"] += 1
                        error_details["network_error"] = error_details.get("network_error", 0) + 1
                        failed_products.append(
                            {"product_id": product_id, "status": status, "error": error}
                        )
                    elif status == "memory_error":
                        stats["failed"] += 1
                        error_details["memory_error"] = error_details.get("memory_error", 0) + 1
                        failed_products.append(
                            {"product_id": product_id, "status": status, "error": error}
                        )
                    elif status == "validation_error":
                        stats["failed"] += 1
                        error_details["validation_error"] = (
                            error_details.get("validation_error", 0) + 1
                        )
                        failed_products.append(
                            {"product_id": product_id, "status": status, "error": error}
                        )
                    else:
                        stats["failed"] += 1
                        error_type = status if status else "unknown"
                        error_details[error_type] = error_details.get(error_type, 0) + 1
                        failed_products.append(
                            {"product_id": product_id, "status": status, "error": error}
                        )

        logger.info(
            f"📊 Có {valid_results} detail results hợp lệ từ {len(all_detail_results)} results"
        )

        if valid_results < len(all_detail_results):
            logger.warning(
                f"⚠️  Có {len(all_detail_results) - valid_results} results không hợp lệ hoặc thiếu product_id"
            )

        # Log chi tiết về các lỗi
        if error_details:
            logger.info("=" * 70)
            logger.info("📋 PHÂN TÍCH CÁC LOẠI LỖI")
            logger.info("=" * 70)
            for error_type, count in sorted(
                error_details.items(), key=lambda x: x[1], reverse=True
            ):
                logger.info(f"  ❌ {error_type}: {count} products")
            logger.info("=" * 70)

            # Log một số products bị fail đầu tiên để phân tích
            if failed_products:
                logger.info(f"📝 Mẫu {min(10, len(failed_products))} products bị fail đầu tiên:")
                for i, failed in enumerate(failed_products[:10], 1):
                    logger.info(
                        f"  {i}. Product ID: {failed['product_id']}, Status: {failed['status']}, Error: {failed.get('error', 'N/A')[:100]}"
                    )

        # Lưu thông tin lỗi vào stats để phân tích sau
        stats["error_details"] = error_details
        stats["failed_products_count"] = len(failed_products)

        # Merge detail vào products
        # CHỈ lưu products có detail VÀ status == "success" (không lưu cached hoặc failed)
        products_with_detail = []
        products_without_detail = 0
        products_cached = 0
        products_failed = 0

        for product in products:
            product_id = product.get("product_id")
            detail_result = detail_dict.get(product_id)

            if detail_result and detail_result.get("detail"):
                status = detail_result.get("status", "failed")

                # CHỈ lưu products có status == "success" (đã crawl thành công, không phải từ cache)
                if status == "success":
                    # Merge detail vào product
                    detail = detail_result["detail"]
                    product_with_detail = {**product}

                    # Update các trường từ detail
                    if detail.get("price"):
                        product_with_detail["price"] = detail["price"]
                    if detail.get("rating"):
                        product_with_detail["rating"] = detail["rating"]
                    if detail.get("description"):
                        product_with_detail["description"] = detail["description"]
                    if detail.get("specifications"):
                        product_with_detail["specifications"] = detail["specifications"]
                    if detail.get("images"):
                        product_with_detail["images"] = detail["images"]
                    if detail.get("brand"):
                        product_with_detail["brand"] = detail["brand"]
                    if detail.get("seller"):
                        product_with_detail["seller"] = detail["seller"]
                    if detail.get("stock"):
                        product_with_detail["stock"] = detail["stock"]
                    if detail.get("shipping"):
                        product_with_detail["shipping"] = detail["shipping"]
                    # Cập nhật sales_count: ưu tiên từ detail, nếu không có thì dùng từ product gốc
                    # Chỉ cần có trong một trong hai là đủ
                    if detail.get("sales_count") is not None:
                        product_with_detail["sales_count"] = detail["sales_count"]
                    elif product.get("sales_count") is not None:
                        product_with_detail["sales_count"] = product["sales_count"]
                    # Nếu cả hai đều không có, giữ None (đã có trong product gốc)

                    # Thêm metadata
                    product_with_detail["detail_crawled_at"] = detail_result.get("crawled_at")
                    product_with_detail["detail_status"] = status

                    products_with_detail.append(product_with_detail)
                elif status == "cached":
                    # Không lưu products từ cache (chỉ lưu products đã crawl mới)
                    products_cached += 1
                else:
                    # Không lưu products bị fail
                    products_failed += 1
            else:
                # Không lưu products không có detail
                products_without_detail += 1

        logger.info("=" * 70)
        logger.info("📊 THỐNG KÊ MERGE DETAIL")
        logger.info("=" * 70)
        logger.info(f"📦 Tổng products ban đầu: {stats['total_products']}")
        logger.info(f"🔄 Products được crawl detail: {stats['crawled_count']}")
        logger.info(f"✅ Có detail (success): {stats['with_detail']}")
        logger.info(f"📦 Có detail (cached): {stats['cached']}")
        logger.info(f"⚠️  Degraded: {stats['degraded']}")
        logger.info(f"⚡ Circuit breaker open: {stats['circuit_breaker_open']}")
        logger.info(f"❌ Failed: {stats['failed']}")
        logger.info(f"⏱️  Timeout: {stats['timeout']}")

        # Tính tổng có detail (success + cached)
        total_with_detail = stats["with_detail"] + stats["cached"]
        
        # Tỷ lệ thành công dựa trên số lượng được crawl (quan trọng hơn)
        if stats["crawled_count"] > 0:
            success_rate = (stats["with_detail"] / stats["crawled_count"]) * 100
            logger.info(
                f"📈 Tỷ lệ thành công (dựa trên crawled): {stats['with_detail']}/{stats['crawled_count']} ({success_rate:.1f}%)"
            )
        
        # Tỷ lệ có detail trong tổng products (để tham khảo)
        if stats["total_products"] > 0:
            detail_coverage = total_with_detail / stats["total_products"] * 100
            logger.info(
                f"📊 Tỷ lệ có detail (trong tổng products): {total_with_detail}/{stats['total_products']} ({detail_coverage:.1f}%)"
            )

        logger.info("=" * 70)
        logger.info(
            f"💾 Products được lưu vào file: {len(products_with_detail)} (chỉ lưu products có status='success')"
        )
        logger.info(f"📦 Products từ cache (đã bỏ qua): {products_cached}")
        logger.info(f"❌ Products bị fail (đã bỏ qua): {products_failed}")
        logger.info(f"🚫 Products không có detail (đã bỏ qua): {products_without_detail}")
        logger.info("=" * 70)

        # Cập nhật stats để phản ánh số lượng products thực tế được lưu
        stats["products_saved"] = len(products_with_detail)
        stats["products_skipped"] = products_without_detail
        stats["products_cached_skipped"] = products_cached
        stats["products_failed_skipped"] = products_failed

        result = {
            "products": products_with_detail,
            "stats": stats,
            "merged_at": datetime.now().isoformat(),
            "note": f"Chỉ lưu {len(products_with_detail)} products có status='success' (đã bỏ qua {products_cached} cached, {products_failed} failed, {products_without_detail} không có detail)",
        }

        return result

    except ValueError as e:
        logger.error(f"❌ Validation error khi merge details: {e}", exc_info=True)
        # Nếu là validation error (thiếu products), return empty result thay vì raise
        return {
            "products": [],
            "stats": {
                "total_products": 0,
                "crawled_count": 0,  # Số lượng products được crawl detail
                "with_detail": 0,
                "cached": 0,
                "failed": 0,
                "timeout": 0,
            },
            "merged_at": datetime.now().isoformat(),
            "error": str(e),
        }
    except Exception as e:
        logger.error(f"❌ Lỗi khi merge details: {e}", exc_info=True)
        # Log chi tiết context để debug
        logger.error(f"   Context keys: {list(context.keys()) if context else 'None'}")
        try:
            ti = context.get("ti")
            if ti:
                logger.error(f"   Task ID: {ti.task_id}, DAG ID: {ti.dag_id}, Run ID: {ti.run_id}")
        except Exception:
            pass
        raise

def prepare_detail_kwargs(**context):
    """Helper function để prepare op_kwargs cho Dynamic Task Mapping detail"""
    import logging

    logger = logging.getLogger("airflow.task")
    ti = context["ti"]

    # Lấy products từ prepare_products_for_detail
    # Task này nằm trong TaskGroup 'crawl_product_details', nên task_id đầy đủ là 'crawl_product_details.prepare_products_for_detail'
    products_to_crawl = None

    # Lấy từ upstream task (prepare_products_for_detail) - cách đáng tin cậy nhất
    # Thử lấy upstream_task_ids từ nhiều nguồn khác nhau (tương thích với các phiên bản Airflow)
    upstream_task_ids = []
    try:
        task_instance = context.get("task_instance")
        if task_instance:
            # Thử với RuntimeTaskInstance (Airflow SDK mới)
            if hasattr(task_instance, "upstream_task_ids"):
                upstream_task_ids = list(task_instance.upstream_task_ids)
            # Thử với ti.task (cách khác)
            elif hasattr(ti, "task") and hasattr(ti.task, "upstream_task_ids"):
                upstream_task_ids = list(ti.task.upstream_task_ids)
    except (AttributeError, TypeError) as e:
        logger.debug(f"   Không thể lấy upstream_task_ids: {e}")

    if upstream_task_ids:
        logger.info(f"🔍 Upstream tasks: {upstream_task_ids}")
        # Thử lấy từ tất cả upstream tasks
        for task_id in upstream_task_ids:
            try:
                products_to_crawl = ti.xcom_pull(task_ids=task_id)
                if products_to_crawl:
                    logger.info(f"✅ Lấy XCom từ upstream task: {task_id}")
                    break
            except Exception as e:
                logger.debug(f"   Không lấy được từ {task_id}: {e}")
                continue

    # Nếu vẫn không lấy được, thử các cách khác
    if not products_to_crawl:
        try:
            # Thử với task_id đầy đủ (có TaskGroup prefix)
            products_to_crawl = ti.xcom_pull(
                task_ids="crawl_product_details.prepare_products_for_detail"
            )
            logger.info(
                "✅ Lấy XCom từ task_id: crawl_product_details.prepare_products_for_detail"
            )
        except Exception as e1:
            logger.warning(f"⚠️  Không lấy được với task_id đầy đủ: {e1}")
            try:
                # Thử với task_id không có prefix (fallback)
                products_to_crawl = ti.xcom_pull(task_ids="prepare_products_for_detail")
                logger.info("✅ Lấy XCom từ task_id: prepare_products_for_detail")
            except Exception as e2:
                logger.error(f"❌ Không thể lấy XCom với cả 2 cách: {e1}, {e2}")

    if not products_to_crawl:
        logger.error("❌ Không thể lấy products từ XCom!")
        try:
            task_instance = context.get("task_instance")
            upstream_info = []
            if task_instance:
                if hasattr(task_instance, "upstream_task_ids"):
                    upstream_info = list(task_instance.upstream_task_ids)
                elif hasattr(ti, "task") and hasattr(ti.task, "upstream_task_ids"):
                    upstream_info = list(ti.task.upstream_task_ids)
            logger.error(f"   Upstream tasks: {upstream_info}")
        except Exception as e:
            logger.error(f"   Không thể lấy thông tin upstream tasks: {e}")
        return []

    if not isinstance(products_to_crawl, list):
        logger.error(f"❌ Products không phải list: {type(products_to_crawl)}")
        logger.error(f"   Value: {products_to_crawl}")
        return []

    logger.info(f"✅ Đã lấy {len(products_to_crawl)} products từ XCom")

    # Trả về list các dict để expand
    op_kwargs_list = [{"product_info": product} for product in products_to_crawl]

    logger.info(f"🔢 Tạo {len(op_kwargs_list)} op_kwargs cho Dynamic Task Mapping")
    if op_kwargs_list:
        logger.info("📋 Sample op_kwargs (first 2):")
        for i, kwargs in enumerate(op_kwargs_list[:2]):
            product_info = kwargs.get("product_info", {})
            logger.info(
                f"  {i+1}. Product ID: {product_info.get('product_id')}, URL: {product_info.get('url', '')[:60]}..."
            )

    return op_kwargs_list


def save_products_with_detail(**context) -> str:
    """
    Task: Lưu products với detail vào file

    Returns:
        str: Đường dẫn file đã lưu
    """
    logger = get_logger(context)
    logger.info("=" * 70)
    logger.info("💾 TASK: Save Products with Detail")
    logger.info("=" * 70)

    try:
        ti = context["ti"]

        # Lấy kết quả merge
        merge_result = None
        try:
            merge_result = ti.xcom_pull(task_ids="crawl_product_details.merge_product_details")
        except Exception:
            try:
                merge_result = ti.xcom_pull(task_ids="merge_product_details")
            except Exception:
                pass

        if not merge_result:
            raise ValueError("Không tìm thấy merge result từ XCom")

        products = merge_result.get("products", [])
        stats = merge_result.get("stats", {})
        note = merge_result.get("note", "Crawl từ Airflow DAG với product details")

        logger.info(f"💾 Đang lưu {len(products)} products với detail...")
        
        # Log thông tin về crawl detail
        crawled_count = stats.get("crawled_count", 0)
        if crawled_count > 0:
            logger.info(f"🔄 Products được crawl detail: {crawled_count}")
            logger.info(f"✅ Products có detail (success): {stats.get('with_detail', 0)}")
            if stats.get("timeout", 0) > 0:
                logger.info(f"⏱️  Products timeout: {stats.get('timeout', 0)}")
            if stats.get("failed", 0) > 0:
                logger.info(f"❌ Products failed: {stats.get('failed', 0)}")
        
        if stats.get("products_skipped"):
            logger.info(f"🚫 Đã bỏ qua {stats.get('products_skipped')} products không có detail")

        # Chuẩn bị dữ liệu
        output_data = {
            "total_products": len(products),
            "stats": stats,
            "crawled_at": datetime.now().isoformat(),
            "note": note,
            "products": products,
        }

        # Atomic write
        output_file = str(OUTPUT_FILE_WITH_DETAIL)
        atomic_write_file(output_file, output_data, **context)

        logger.info(f"✅ Đã lưu {len(products)} products với detail vào: {output_file}")

        return output_file

    except Exception as e:
        logger.error(f"❌ Lỗi khi save products with detail: {e}", exc_info=True)
        raise