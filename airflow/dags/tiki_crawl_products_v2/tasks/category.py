from __future__ import annotations

# Import all bootstrap globals (paths, config, dynamic imports, singletons).
# This preserves legacy behavior without renaming any globals referenced by task callables.
from tiki_crawl_products_v2.bootstrap import (
    CACHE_DIR,
    CATEGORIES_FILE,
    DEBUG_LOAD_CATEGORIES,
    Any,
    CircuitBreakerOpenError,
    classify_error,
    crawl_category_products,
    datetime,
    ensure_output_dirs,
    get_int_variable,
    get_tiki_circuit_breaker,
    get_tiki_degradation,
    get_tiki_dlq,
    get_variable,
    json,
    os,
    sys,
    time,
)

from .common import _fix_sys_path_for_pipelines_import  # noqa: F401
from .common import get_logger  # noqa: F401


def load_categories(**context) -> list[dict[str, Any]]:
    """
    Task 1: Load danh sách danh mục từ file

    Returns:
        List[Dict]: Danh sách danh mục
    """
    logger = get_logger(context)
    if DEBUG_LOAD_CATEGORIES:
        logger.debug("DEBUG: Task load_categories starting...")

    logger.info("=" * 70)
    logger.info("📖 TASK: Load Categories")

    if DEBUG_LOAD_CATEGORIES:
        logger.debug(f"DEBUG: CWD: {os.getcwd()}")
        logger.debug(f"DEBUG: PYTHONPATH: {sys.path}")

    try:
        categories_file = str(CATEGORIES_FILE)
        if DEBUG_LOAD_CATEGORIES:
            logger.debug(f"DEBUG: Reading file {categories_file}")

        if not os.path.exists(categories_file):
            raise FileNotFoundError(f"Không tìm thấy file: {categories_file}")

        with open(categories_file, encoding="utf-8") as f:
            categories = json.load(f)

        logger.info(f"✅ Đã load {len(categories)} danh mục")

        # Lọc level
        try:
            min_level = get_int_variable("TIKI_MIN_CATEGORY_LEVEL", default=2)
            max_level = get_int_variable("TIKI_MAX_CATEGORY_LEVEL", default=4)
        except Exception:
            min_level, max_level = 2, 4
        categories = [cat for cat in categories if min_level <= cat.get("level", 0) <= max_level]

        try:
            max_categories = get_int_variable("TIKI_MAX_CATEGORIES", default=0)
        except Exception:
            max_categories = 0

        if max_categories > 0:
            categories = categories[:max_categories]
            logger.info(
                f"📊 Xử lý {len(categories)} danh mục (Level {min_level}-{max_level} | Giới hạn: {max_categories})"
            )
        else:
            logger.info(f"📊 Xử lý {len(categories)} danh mục (Level {min_level}-{max_level})")

        # Push categories lên XCom để các task khác dùng
        return categories

    except Exception as e:
        logger.error(f"❌ Lỗi khi load categories: {e}", exc_info=True)
        raise


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

    # Clean log for workers
    logger.info(f"▶️  START: Category [{category_id}] {category_name}")

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
        # Lazily initialize expensive singletons only when the task runs
        ensure_output_dirs()
        tiki_degradation = get_tiki_degradation()
        tiki_circuit_breaker = get_tiki_circuit_breaker()
        tiki_dlq = get_tiki_dlq()

        # Kiểm tra graceful degradation
        if tiki_degradation.should_skip():
            result["error"] = "Service đang ở trạng thái FAILED, skip crawl"
            result["status"] = "degraded"
            logger.warning(f"⚠️  Service degraded, skip category {category_name}")
            return result

        # Lấy cấu hình từ Airflow Variables
        max_pages = int(
            get_variable("TIKI_MAX_PAGES_PER_CATEGORY", default="20")
        )  # Mặc định 20 trang để tránh timeout
        use_selenium = get_variable("TIKI_USE_SELENIUM", default="false").lower() == "true"
        timeout = int(get_variable("TIKI_CRAWL_TIMEOUT", default="300"))  # 5 phút mặc định

        # Rate limiting: được xử lý bên trong crawl_category_products (smart rate limit / cache check)
        # Không sleep ở đây để tránh chậm khi hit cache

        # Crawl với timeout và circuit breaker
        start_time = time.time()

        def _crawl_with_params():
            """
            Wrapper function để gọi với circuit breaker.
            """
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


def crawl_category_batch(
    category_batch: list[dict[str, Any]] = None, batch_index: int = -1, **context
) -> list[dict[str, Any]]:
    """
    Task 3: Crawl sản phẩm từ một batch danh mục (Giảm overhead của Airflow)

    Args:
        category_batch: List các danh mục trong batch
        batch_index: Index của batch
        context: Airflow context

    Returns:
        List[Dict]: Kết quả crawl cho toàn bộ batch
    """
    logger = get_logger(context)

    # Lấy category_batch từ op_kwargs nếu chưa có
    if not category_batch:
        ti = context.get("ti")
        if ti:
            op_kwargs = getattr(ti, "op_kwargs", {})
            if op_kwargs:
                category_batch = op_kwargs.get("category_batch")
                batch_index = op_kwargs.get("batch_index", -1)

    if not category_batch:
        logger.error(f"❌ MISSING CATEGORY_BATCH! Batch index: {batch_index}")
        return []

    logger.info(f"🚀 BATCH CATEGORY {batch_index}: Đang xử lý {len(category_batch)} danh mục")

    results = []
    for _i, category in enumerate(category_batch):
        try:
            # Re-use crawl_single_category logic for each category in batch
            cat_result = crawl_single_category(category=category, **context)
            results.append(cat_result)
        except Exception as e:
            logger.error(f"❌ Lỗi khi xử lý category trong batch: {e}")
            results.append(
                {"category_id": category.get("id", "unknown"), "status": "failed", "error": str(e)}
            )

    logger.info(f"✅ Hoàn thành BATCH CATEGORY {batch_index}: {len(results)} kết quả")
    return results
