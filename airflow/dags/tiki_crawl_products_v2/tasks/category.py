from __future__ import annotations

# Import all bootstrap globals (paths, config, dynamic imports, singletons).
# This preserves legacy behavior without renaming any globals referenced by task callables.
from concurrent.futures import ThreadPoolExecutor

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
from tiki_crawl_products_v2.tasks.common import (
    _fix_sys_path_for_pipelines_import,  # noqa: F401
    get_logger,  # noqa: F401
)


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

        # 1. Xác định is_leaf cho từng danh mục
        # Một danh mục là leaf nếu URL của nó không được dùng làm parent_url của bất kỳ danh mục nào khác
        parent_urls = {cat.get("parent_url") for cat in categories if cat.get("parent_url")}

        for cat in categories:
            cat["is_leaf"] = cat.get("url") not in parent_urls

        # 2. Lọc danh mục: Chỉ lấy leaf categories theo đúng định hướng
        # Cho phép override bằng variable nếu cần (mặc định crawl_only_leaf=True)
        crawl_only_leaf = get_variable("TIKI_CRAWL_ONLY_LEAF", default="true").lower() == "true"

        if crawl_only_leaf:
            original_count = len(categories)
            categories = [cat for cat in categories if cat.get("is_leaf")]
            logger.info(
                f"🍃 Đã lọc lấy {len(categories)} leaf categories (từ {original_count} ban đầu)"
            )
        else:
            # Fallback về lọc theo level nếu không chỉ lọc leaf
            try:
                min_level = get_int_variable("TIKI_MIN_CATEGORY_LEVEL", default=2)
                max_level = get_int_variable("TIKI_MAX_CATEGORY_LEVEL", default=4)
            except Exception:
                min_level, max_level = 2, 4
            categories = [
                cat for cat in categories if min_level <= cat.get("level", 0) <= max_level
            ]
            logger.info(
                f"📊 Lọc theo Level: {len(categories)} danh mục (Level {min_level}-{max_level})"
            )

        # Lưu categories vào file thay vì XCom
        from pathlib import Path

        output_file = Path("/opt/airflow/data/processed") / "loaded_categories.json"
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(categories, f, ensure_ascii=False)

        logger.info(f"💾 Đã lưu {len(categories)} categories vào {output_file}")

        # Return chỉ file path và metadata
        return {
            "categories_file": str(output_file),
            "count": len(categories),
            "crawl_only_leaf": crawl_only_leaf,
        }

    except Exception as e:
        logger.error(f"❌ Lỗi khi load categories: {e}", exc_info=True)
        raise


def crawl_single_category(
    category: dict[str, Any] = None,
    max_pages_param: int | None = None,
    use_selenium_param: bool | None = None,
    **context,
) -> dict[str, Any]:
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
    category_id = category.get("id", "") or category.get("category_id", "")

    # Bổ sung category_id nếu thiếu
    if not category_id and category_url:
        from tiki_crawl_products_v2.bootstrap import extract_category_id_from_url

        category_id = extract_category_id_from_url(category_url)

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

        # Lấy cấu hình từ Airflow Variables hoặc tham số truyền vào (để giảm DB hit trong batch)
        max_pages = (
            max_pages_param
            if max_pages_param is not None
            else int(get_variable("TIKI_MAX_PAGES_PER_CATEGORY", default="20"))
        )
        use_selenium = (
            use_selenium_param
            if use_selenium_param is not None
            else get_variable("TIKI_USE_SELENIUM", default="false").lower() == "true"
        )
        timeout = int(get_variable("TIKI_CRAWL_TIMEOUT", default="300"))

        # Thêm cấu hình cache
        force_refresh = get_variable("TIKI_FORCE_REFRESH", default="false").lower() == "true"
        cache_ttl = int(get_variable("TIKI_CACHE_TTL", default="86400"))

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
                allow_selenium_fallback=use_selenium,  # Nếu use_selenium=False thì cũng tắt fallback cho nhanh
                use_rate_limiting=True,  # Sử dụng rate limiting
                logger=logger,
                force_refresh=force_refresh,
                cache_ttl=cache_ttl,
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

        # Trả về products qua XCom thay vì lưu trực tiếp vào DB
        # Việc load vào DB sẽ được thực hiện bởi task transform_and_load sau này
        result["products"] = products
        result["status"] = "success"
        result["products_count"] = len(products)
        result["elapsed_time"] = elapsed

        logger.info(f"✅ Crawl thành công: {len(products)} sản phẩm qua XCom trong {elapsed:.1f}s")

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

    # Lấy cấu hình cho cả batch
    max_pages = int(get_variable("TIKI_MAX_PAGES_PER_CATEGORY", default="20"))
    use_selenium = get_variable("TIKI_USE_SELENIUM", default="false").lower() == "true"
    concurrency = int(get_variable("TIKI_CATEGORY_CONCURRENCY", default="6"))

    def process_category(cat):
        try:
            # Re-use crawl_single_category logic
            return crawl_single_category(
                category=cat,
                max_pages_param=max_pages,
                use_selenium_param=use_selenium,
                **context,
            )
        except Exception as e:
            logger.error(f"❌ Batch error for category {cat.get('name')}: {e}")
            return {
                "category_id": cat.get("id", "unknown"),
                "status": "failed",
                "error": str(e),
            }

    # Chạy song song dùng ThreadPoolExecutor
    logger.info(
        f"🚀 Chạy batch {batch_index} với {len(category_batch)} categories, concurrency={concurrency}"
    )
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        results = list(executor.map(process_category, category_batch))

    logger.info(f"✅ Hoàn thành BATCH CATEGORY {batch_index}: {len(results)} kết quả")
    return results
