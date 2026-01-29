from __future__ import annotations

# Import all bootstrap globals (paths, config, dynamic imports, singletons).
# This preserves legacy behavior without renaming any globals referenced by task callables.
from tiki_crawl_products_v2.bootstrap import (
    DETAIL_CACHE_DIR,
    OUTPUT_FILE,
    OUTPUT_FILE_WITH_DETAIL,
    PROGRESS_FILE,
    Any,
    CircuitBreakerOpenError,
    Path,
    atomic_write_file,
    classify_error,
    crawl_product_detail_async,
    crawl_product_detail_with_driver,
    crawl_product_detail_with_selenium,
    datetime,
    ensure_output_dirs,
    extract_product_detail,
    get_hierarchy_map,
    get_int_variable,
    get_logger,
    get_tiki_circuit_breaker,
    get_tiki_degradation,
    get_tiki_dlq,
    get_variable,
    json,
    os,
    re,
    redis_cache,
    shutil,
    sys,
    time,
)

from .common import (  # noqa: F401
    _fix_sys_path_for_pipelines_import,
    get_selenium_driver_pool_class,
)
from .loader import _import_postgres_storage  # noqa: F401


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
        db_hits = 0  # Products đã có trong DB

        products_per_day = get_int_variable("TIKI_PRODUCTS_PER_DAY", default=1000)
        # Mặc định giới hạn số products/ngày để tránh quá tải server
        max_products = int(
            get_variable("TIKI_MAX_PRODUCTS_FOR_DETAIL", default="0")
        )  # 0 = không giới hạn

        logger.info(
            f"⚙️  Cấu hình: {products_per_day} products/ngày, max: {max_products if max_products > 0 else 'không giới hạn'}"
        )

        # Kiểm tra products đã có trong database với detail đầy đủ (để tránh crawl lại)
        # Chỉ skip products có price và sales_count (detail đầy đủ)
        existing_product_ids_in_db = set()
        try:
            PostgresStorage = _import_postgres_storage()
            if PostgresStorage is None:
                logger.warning("⚠️  Không thể import PostgresStorage, bỏ qua kiểm tra database")
            else:
                # Lấy database config
                db_host = get_variable(
                    "POSTGRES_HOST", default=os.getenv("POSTGRES_HOST", "postgres")
                )
                db_port = int(
                    get_variable("POSTGRES_PORT", default=os.getenv("POSTGRES_PORT", "5432"))
                )
                db_name = get_variable(
                    "POSTGRES_DB", default=os.getenv("POSTGRES_DB", "crawl_data")
                )
                db_user = get_variable(
                    "POSTGRES_USER", default=os.getenv("POSTGRES_USER", "postgres")
                )
                # trufflehog:ignore - Fallback for development, production uses Airflow Variables
                db_password = get_variable(
                    "POSTGRES_PASSWORD", default=os.getenv("POSTGRES_PASSWORD", "postgres")
                )

                storage = PostgresStorage(
                    host=db_host,
                    port=db_port,
                    database=db_name,
                    user=db_user,
                    password=db_password,
                )

                # Lấy danh sách product_ids từ products list
                product_ids_to_check = [
                    p.get("product_id") for p in products if p.get("product_id")
                ]

                if product_ids_to_check:
                    logger.info(
                        f"🔍 Đang kiểm tra {len(product_ids_to_check)} products trong database..."
                    )

                    # Cấu hình thời gian relaxation (mặc định 7 ngày)
                    cache_relax_days = get_int_variable("TIKI_CACHE_RELAX_DAYS", default=7)

                    # Skip check info reduced to avoid spam
                    # logger.info("Checking skip conditions...")

                    with storage.get_connection() as conn:
                        with conn.cursor() as cur:
                            # Chia nhỏ query nếu có quá nhiều product_ids
                            for i in range(0, len(product_ids_to_check), 1000):
                                batch_ids = product_ids_to_check[i : i + 1000]
                                placeholders = ",".join(["%s"] * len(batch_ids))

                                # Logic check:
                                # Normal: Skip if (Has Full Detail) OR (Is Recent)
                                # Strict Recency (Force Update Old): Skip if (Is Recent) ONLY.

                                check_recency_only = (
                                    get_variable("TIKI_CHECK_RECENCY_ONLY", default="false").lower()
                                    == "true"
                                )

                                if check_recency_only:
                                    logger.info("🕒 RECENCY MODE: Chỉ check updated_at")
                                    filter_condition = (
                                        f"updated_at > NOW() - INTERVAL '{cache_relax_days} days'"
                                    )
                                else:
                                    filter_condition = f"""
                                        (brand IS NOT NULL AND brand != '' AND seller_name IS NOT NULL AND seller_name != '')
                                        OR (updated_at > NOW() - INTERVAL '{cache_relax_days} days')
                                    """

                                cur.execute(
                                    f"""
                                    SELECT product_id
                                    FROM products
                                    WHERE product_id IN ({placeholders})
                                      AND price IS NOT NULL
                                      AND sales_count IS NOT NULL
                                      AND ({filter_condition})
                                    """,
                                    batch_ids,
                                )
                                existing_product_ids_in_db.update(row[0] for row in cur.fetchall())

                    if len(existing_product_ids_in_db) > 0:
                        logger.info(
                            f"✅ DB Check: Found {len(existing_product_ids_in_db)} valid/recent products to skip"
                        )
                    storage.close()
        except Exception as e:
            logger.warning(f"⚠️  Không thể kiểm tra database: {e}")
            logger.info("   Sẽ tiếp tục với cache và progress file")

        # Bắt đầu iteration từ đầu (Stateless iteration)
        # Thay vì dựa vào index, chúng ta dựa vào crawled_product_ids

        # Check Force Crawl flag
        force_crawl = get_variable("TIKI_FORCE_CRAWL", default="false").lower() == "true"
        # Check Ignore Progress flag (Soft Force)
        ignore_progress = get_variable("TIKI_IGNORE_PROGRESS", default="false").lower() == "true"

        if force_crawl:
            logger.warning("🔥 FORCE CRAWL: ON")
        elif ignore_progress:
            logger.warning("🔄 IGNORE PROGRESS: ON (Re-scan list)")

        logger.info(
            f"🔄 Bắt đầu kiểm tra {len(products)} products (đã crawl {progress['total_crawled']} products)"
        )

        skipped_count = 0
        products_to_crawl = []

        # Reset counters
        db_hits = 0
        cache_hits = 0
        already_crawled = 0

        # Tối ưu: Duyệt tất cả products để tìm products chưa có trong DB
        for idx, product in enumerate(products):
            product_id = product.get("product_id")
            product_url = product.get("url")

            if not product_id or not product_url:
                continue

            # 1. Kiểm tra xem đã crawl chưa (từ progress) - Trừ khi force crawl hoặc ignore progress
            if (
                not force_crawl
                and not ignore_progress
                and product_id in progress["crawled_product_ids"]
            ):
                already_crawled += 1
                skipped_count += 1
                continue

            # 2. Kiểm tra xem đã có trong database chưa (với detail đầy đủ) - Trừ khi force crawl
            if not force_crawl and product_id in existing_product_ids_in_db:
                # Đã có trong DB với detail đầy đủ (có price và sales_count)
                # → Skip crawl lại
                db_hits += 1
                progress["crawled_product_ids"].add(product_id)
                already_crawled += 1
                skipped_count += 1
                continue

            # 3. Kiểm tra cache với Redis (thay vì file cache)
            cache_hit = False

            if not force_crawl and redis_cache:
                # Chuẩn hóa URL trước khi check cache (CRITICAL)
                product_id_for_cache = product_id

                # Thử lấy từ Redis cache với flexible validation
                cached_detail, is_valid = redis_cache.get_product_detail_with_validation(
                    product_id_for_cache
                )

                if is_valid:
                    cache_hits += 1
                    cache_hit = True
                    progress["crawled_product_ids"].add(product_id)
                    already_crawled += 1
                    skipped_count += 1

            # Nếu chưa có valid cache (hoặc force crawl), thêm vào danh sách crawl
            if cache_hit:
                continue

            products_to_crawl.append(
                {
                    "product_id": product_id,
                    "url": product_url,
                    "name": product.get("name", ""),
                    "product": product,  # Giữ nguyên product data
                    "index": idx,  # Lưu index để track progress (nếu cần debug)
                }
            )
            skipped_count = 0  # Reset counter khi tìm thấy product mới

            # Giới hạn số lượng products crawl trong ngày này
            if len(products_to_crawl) >= products_per_day:
                logger.info(f"✓ Đã đạt giới hạn {products_per_day} products cho ngày hôm nay")
                break

            # Giới hạn tổng số (nếu có)
            if max_products > 0 and len(products_to_crawl) >= max_products:
                logger.info(f"✓ Đã đạt giới hạn tổng {max_products} products")
                break

        # Stats Summary
        total_checked = idx + 1
        cache_hit_rate = (cache_hits / total_checked * 100) if total_checked > 0 else 0.0
        total_skipped = already_crawled

        progress["total_crawled"] = len(progress["crawled_product_ids"])

        logger.info("-" * 30)
        logger.info(
            f"📊 SUMMARY: Input={len(products)} | ToCrawl={len(products_to_crawl)} | Skipped={total_skipped}"
        )
        logger.info(f"   Hits: DB={db_hits}, Cache={cache_hits} ({cache_hit_rate:.1f}%)")
        logger.info(f"   Total Unique Crawled: {progress['total_crawled']}")
        logger.info("-" * 30)

        if len(products_to_crawl) == 0:
            logger.warning(
                f"⚠️  NO PRODUCTS TO CRAWL! (Check: Progress={not ignore_progress}, DB/Cache={not force_crawl})"
            )
            logger.info(
                "   Hint: Set TIKI_FORCE_CRAWL=true to recrawl all, or TIKI_IGNORE_PROGRESS=true to rescan."
            )

        # Lưu progress (list of IDs)
        if products_to_crawl or skipped_count > 0:  # Lưu nếu có thay đổi hoặc skip
            # Update last_crawled_index is mostly meaningless now, set to last checked
            progress["last_crawled_index"] = idx
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
            sample_names = [p.get("product_id", "N/A") for p in products_to_crawl[:3]]
            logger.info(f"📋 Sample products: {', '.join(sample_names)}...")
        else:
            logger.warning("⚠️  Không có products nào cần crawl detail hôm nay!")
            logger.info("💡 Tất cả products đã được crawl hoặc có cache hợp lệ")

        logger.info(f"🔢 Trả về {len(products_to_crawl)} products cho Dynamic Task Mapping")

        return products_to_crawl

    except Exception as e:
        logger.error(f"❌ Lỗi khi prepare products: {e}", exc_info=True)
        raise


def crawl_product_batch(
    product_batch: list[dict[str, Any]] = None, batch_index: int = -1, **context
) -> list[dict[str, Any]]:
    """
    Task: Crawl detail cho một batch products (Batch Processing với Driver Pooling và Async)

    Tối ưu:
    - Batch processing: 10 products/batch
    - Driver pooling: Reuse Selenium drivers trong batch
    - Async/aiohttp: Crawl parallel trong batch
    - Fallback Selenium: Nếu aiohttp thiếu sales_count

    Args:
        product_batch: List products trong batch (từ expand_kwargs)
        batch_index: Index của batch
        context: Airflow context

    Returns:
        List[Dict]: List kết quả crawl cho batch
    """
    try:
        logger = get_logger(context)
    except Exception:
        import logging

        logger = logging.getLogger("airflow.task")

    # Lấy product_batch từ op_kwargs nếu chưa có
    if not product_batch:
        ti = context.get("ti")
        if ti:
            op_kwargs = getattr(ti, "op_kwargs", {})
            if op_kwargs:
                product_batch = op_kwargs.get("product_batch")
                batch_index = op_kwargs.get("batch_index", -1)

        if not product_batch:
            product_batch = context.get("product_batch") or context.get("op_kwargs", {}).get(
                "product_batch"
            )
            batch_index = context.get("batch_index", -1)

    if not product_batch:
        logger.error(f"❌ MISSING PRODUCT_BATCH! Context keys: {list(context.keys())}")
        return []

    # Validate product_batch
    if not isinstance(product_batch, list):
        logger.error(f"❌ INVALID BATCH TYPE: {type(product_batch)} (Value: {product_batch})")
        return []

    if len(product_batch) == 0:
        logger.warning(f"⚠️  BATCH {batch_index} EMPTY")
        return []

    ids_preview = ", ".join([str(p.get("product_id", "unknown")) for p in product_batch[:3]])
    logger.info(
        f"🚀 BATCH {batch_index}: Đang xử lý {len(product_batch)} sản phẩm (Mẫu: {ids_preview}...)"
    )

    results = []

    # Initialize Driver Pool for fallback Selenium
    driver_pool = None
    try:
        from .common import get_selenium_driver_pool_class

        DriverPool = get_selenium_driver_pool_class(logger)
        pool_size = int(get_variable("TIKI_DETAIL_POOL_SIZE", default="2"))
        driver_pool = DriverPool(pool_size=pool_size, headless=True)
        logger.info(f"✅ Driver pool initialized with size {pool_size}")
    except Exception as e:
        logger.warning(f"⚠️  Không thể khởi tạo Driver Pool: {e}")

    try:
        import asyncio

        # Tạo event loop trước
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        # Session sẽ được tạo bên trong async function (cần async context)
        session = None

        async def crawl_single_async(product_info: dict) -> dict[str, Any]:
            """
            Crawl một product với async.
            """
            product_id = product_info.get("product_id", "unknown")
            product_url = product_info.get("url", "")

            result = {
                "product_id": product_id,
                "url": product_url,
                "status": "failed",
                "error": None,
                "detail": None,
                "crawled_at": datetime.now().isoformat(),
            }

            try:
                # Thử async crawl trước
                if session:
                    # FAST DAG: Disable Selenium fallback
                    detail = await crawl_product_detail_async(
                        product_url,
                        session=session,
                        use_selenium_fallback=False,  # Disable fallback for Fast DAG
                        verbose=False,
                    )

                    # Kiểm tra nếu crawl_product_detail_async trả về HTML string (do fallback về Selenium)
                    if isinstance(detail, str) and detail.strip().startswith("<"):
                        # Phân tích HTML để xác định loại
                        html_preview = detail[:500] if len(detail) > 500 else detail
                        html_lower = detail.lower()

                        # Kiểm tra các trường hợp đặc biệt
                        # Kiểm tra error page - cần kiểm tra kỹ hơn để tránh false positive
                        # Error page thường có title hoặc heading chứa "404", "not found", etc.
                        is_error_page = False
                        error_keywords = [
                            "404",
                            "not found",
                            "page not found",
                            "500",
                            "internal server error",
                            "403",
                            "forbidden",
                            "access denied",
                        ]
                        # Chỉ coi là error page nếu có keyword trong title hoặc heading, không phải trong toàn bộ HTML
                        # Vì một số product có thể có "404" trong tên hoặc mô tả
                        if any(keyword in html_lower for keyword in error_keywords):
                            # Kiểm tra trong title tag hoặc h1 tag (nơi thường có error message)
                            title_match = re.search(
                                r"<title[^>]*>(.*?)</title>", html_lower, re.IGNORECASE | re.DOTALL
                            )
                            h1_match = re.search(
                                r"<h1[^>]*>(.*?)</h1>", html_lower, re.IGNORECASE | re.DOTALL
                            )

                            title_text = title_match.group(1) if title_match else ""
                            h1_text = h1_match.group(1) if h1_match else ""

                            # Chỉ coi là error nếu keyword xuất hiện trong title hoặc h1
                            is_error_page = any(
                                keyword in title_text or keyword in h1_text
                                for keyword in error_keywords
                            )

                        is_captcha = any(
                            keyword in html_lower
                            for keyword in [
                                "captcha",
                                "recaptcha",
                                "cloudflare",
                                "checking your browser",
                            ]
                        )
                        has_next_data = (
                            "__next_data__" in html_lower or 'id="__NEXT_DATA__"' in html_lower
                        )

                        # Kiểm tra xem có phải là HTML bình thường của Tiki không
                        is_tiki_page = any(
                            indicator in html_lower
                            for indicator in [
                                "tiki.vn",
                                "tiki",
                                "pdp_product_name",
                                "product-detail",
                                "data-view-id",
                                "pdp-product",
                            ]
                        )

                        if is_error_page:
                            logger.warning(
                                f"⚠️  HTML là error page cho product {product_id}: {html_preview[:200]}..."
                            )
                            detail = None
                        elif is_captcha:
                            logger.warning(
                                f"⚠️  HTML là captcha/block page cho product {product_id}"
                            )
                            detail = None
                        elif not is_tiki_page and not has_next_data:
                            # Nếu không phải Tiki page và không có __NEXT_DATA__, có thể là page lạ
                            logger.warning(
                                f"⚠️  HTML không giống Tiki product page cho product {product_id}"
                            )
                            logger.warning(f"   - Có __NEXT_DATA__: {has_next_data}")
                            logger.warning(f"   - HTML preview: {html_preview[:300]}...")
                            # Vẫn thử parse, có thể vẫn extract được một số thông tin
                        else:
                            logger.info(
                                f"ℹ️  crawl_product_detail_async trả về HTML (fallback Selenium) cho product {product_id}"
                            )
                            logger.info(f"   - HTML length: {len(detail)} chars")
                            logger.info(f"   - Có __NEXT_DATA__: {has_next_data}")

                            # Parse HTML thành dict
                            try:
                                hierarchy_map = get_hierarchy_map()
                                detail = extract_product_detail(
                                    detail, product_url, verbose=False, hierarchy_map=hierarchy_map
                                )
                                if detail and isinstance(detail, dict):
                                    # Kiểm tra xem có đầy đủ thông tin không
                                    has_name = bool(detail.get("name"))
                                    has_price = bool(detail.get("price", {}).get("current_price"))
                                    has_sales = detail.get("sales_count") is not None
                                    logger.info(
                                        f"✅ Đã parse HTML thành công cho product {product_id}"
                                    )
                                    logger.info(
                                        f"   - Có name: {has_name}, có price: {has_price}, có sales_count: {has_sales}"
                                    )
                                else:
                                    logger.warning(
                                        f"⚠️  extract_product_detail trả về None hoặc không phải dict cho product {product_id}"
                                    )
                                    detail = None
                            except Exception as parse_error:
                                logger.warning(
                                    f"⚠️  Lỗi khi parse HTML từ crawl_product_detail_async: {parse_error}"
                                )
                                logger.debug(f"   HTML preview: {html_preview}")
                                detail = None

                    # Đảm bảo detail là dict
                    if detail and not isinstance(detail, dict):
                        logger.warning(
                            f"⚠️  crawl_product_detail_async trả về {type(detail)} thay vì dict cho product {product_id}"
                        )
                        detail = None
                else:
                    # Fallback về Selenium nếu không có aiohttp
                    # Ưu tiên dùng driver pool nếu có
                    html = None
                    try:
                        if "crawl_product_detail_with_driver" in globals() and callable(
                            crawl_product_detail_with_driver
                        ):
                            drv = driver_pool.get_driver()
                            if drv is not None:
                                try:
                                    html = crawl_product_detail_with_driver(
                                        drv,
                                        product_url,
                                        save_html=False,
                                        verbose=False,
                                        timeout=120,  # Tăng từ 60 -> 120s (2 phút) để trang load đầy đủ
                                        use_redis_cache=True,
                                        use_rate_limiting=True,
                                    )
                                finally:
                                    driver_pool.return_driver(drv)
                    except Exception as pooled_err:
                        logger.warning(f"⚠️  Lỗi khi dùng pooled driver: {pooled_err}")
                        html = None

                    # Fallback cuối cùng: tạo driver riêng qua hàm sẵn có
                    if html is None:
                        html = crawl_product_detail_with_selenium(
                            product_url,
                            verbose=False,
                            max_retries=2,
                            timeout=120,  # Tăng từ 60 -> 120s (2 phút) để trang load đầy đủ
                            use_redis_cache=True,
                            use_rate_limiting=True,
                        )
                    if html:
                        # Sử dụng hàm đã được import ở đầu file
                        hierarchy_map = get_hierarchy_map()
                        detail = extract_product_detail(
                            html, product_url, verbose=False, hierarchy_map=hierarchy_map
                        )

                        # Kiểm tra nếu extract_product_detail trả về HTML thay vì dict
                        if isinstance(detail, str) and detail.strip().startswith("<"):
                            logger.warning(
                                f"⚠️  extract_product_detail trả về HTML thay vì dict cho product {product_id}, thử parse lại"
                            )
                            # Thử parse lại HTML
                            try:
                                detail = extract_product_detail(
                                    html, product_url, verbose=False, hierarchy_map=hierarchy_map
                                )
                            except Exception as parse_error:
                                logger.warning(f"⚠️  Lỗi khi parse lại HTML: {parse_error}")
                                detail = None

                        # Đảm bảo detail là dict, không phải HTML string
                        if not isinstance(detail, dict):
                            logger.warning(
                                f"⚠️  extract_product_detail trả về {type(detail)} thay vì dict cho product {product_id}"
                            )
                            detail = None
                    else:
                        detail = None

                if detail and isinstance(detail, dict):
                    result["detail"] = detail
                    result["status"] = "success"
                else:
                    result["error"] = "Không thể crawl detail hoặc extract detail không hợp lệ"
                    result["status"] = "failed"

            except Exception as e:
                result["error"] = str(e)
                result["status"] = "failed"
                logger.warning(f"⚠️  Lỗi khi crawl product {product_id}: {e}")

            return result

        # Crawl tất cả products trong batch song song với async
        # (Event loop đã được tạo ở trên)
        # Sử dụng asyncio.gather() để crawl parallel
        rate_limit_delay = float(get_variable("TIKI_DETAIL_RATE_LIMIT_DELAY", default="0.05"))

        # Tạo semaphore để limit concurrent tasks (tối ưu throughput)
        max_concurrent = int(get_variable("TIKI_DETAIL_MAX_CONCURRENT_TASKS", default="30"))
        semaphore = asyncio.Semaphore(max_concurrent)

        async def bounded_task(task_coro):
            """
            Wrap task để respect semaphore limit.
            """
            async with semaphore:
                return await task_coro

        # Tạo tasks với rate limiting: stagger start times
        async def crawl_batch_parallel():
            """
            Crawl batch với parallel processing và rate limiting.
            """
            # Tạo session ngay lập tức trong async context (trước khi tạo tasks)
            # Đảm bảo session được tạo trong async context có event loop
            nonlocal session
            if session is None:
                try:
                    import aiohttp

                    timeout = aiohttp.ClientTimeout(total=30)
                    # Tạo connector với optimized pooling (sử dụng config)
                    # Đảm bảo sys.path được cấu hình trước khi import
                    src_path = Path("/opt/airflow/src")
                    if src_path.exists() and str(src_path) not in sys.path:
                        sys.path.insert(0, str(src_path))

                    from pipelines.crawl.config import (
                        HTTP_CONNECTOR_LIMIT,
                        HTTP_CONNECTOR_LIMIT_PER_HOST,
                        HTTP_DNS_CACHE_TTL,
                    )

                    connector = aiohttp.TCPConnector(
                        limit=HTTP_CONNECTOR_LIMIT,  # Sử dụng config (150)
                        limit_per_host=HTTP_CONNECTOR_LIMIT_PER_HOST,  # Sử dụng config (15)
                        ttl_dns_cache=HTTP_DNS_CACHE_TTL,  # Sử dụng config (1800s = 30 min)
                        force_close=False,  # Keep connections alive for reuse
                        enable_cleanup_closed=True,
                    )
                    # Tạo session trong async context (có event loop đang chạy)
                    # Đây là async function nên event loop đã có sẵn
                    session = aiohttp.ClientSession(
                        timeout=timeout,
                        connector=connector,
                        headers={
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                        },
                    )
                    logger.info("✅ Đã tạo aiohttp session trong async context")
                except RuntimeError as e:
                    # Lỗi "no running event loop" - fallback về Selenium
                    logger.warning(
                        f"⚠️  Không thể tạo aiohttp session (no event loop): {e}, sẽ dùng Selenium"
                    )
                    session = None
                except Exception as e:
                    logger.warning(f"⚠️  Không thể tạo aiohttp session: {e}, sẽ dùng Selenium")
                    session = None

            # Factory function để tránh closure issue
            def create_crawl_task(product_info, delay_value):
                async def crawl_with_delay():
                    if delay_value > 0:
                        await asyncio.sleep(delay_value)
                    return await crawl_single_async(product_info)

                return crawl_with_delay()

            tasks = []
            for i, product in enumerate(product_batch):
                delay = i * rate_limit_delay / len(product_batch)  # Phân tán delay
                task = create_crawl_task(product, delay)
                # Wrap với bounded_task để respect semaphore
                bounded = bounded_task(task)
                tasks.append(bounded)

            # Chạy tất cả tasks song song (limited bởi semaphore)
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Xử lý exceptions
            processed_results = []
            for i, result in enumerate(batch_results):
                if isinstance(result, Exception):
                    product_info = product_batch[i]
                    processed_results.append(
                        {
                            "product_id": product_info.get("product_id", "unknown"),
                            "url": product_info.get("url", ""),
                            "status": "failed",
                            "error": str(result),
                            "detail": None,
                            "crawled_at": datetime.now().isoformat(),
                        }
                    )
                else:
                    processed_results.append(result)

            return processed_results

        results = loop.run_until_complete(crawl_batch_parallel())

        # Đóng session
        if session:
            loop.run_until_complete(session.close())

        # Cleanup driver pool
        if driver_pool:
            try:
                driver_pool.close_all()
                logger.info("✅ Driver pool closed")
            except Exception as e:
                logger.warning(f"⚠️  Lỗi khi đóng driver pool: {e}")

        # Thống kê
        success_count = sum(1 for r in results if r.get("status") == "success")
        failed_count = len(results) - success_count

        logger.info(f"✅ Batch {batch_index} hoàn thành:")
        logger.info(f"   - Success: {success_count}/{len(product_batch)}")
        logger.info(f"   - Failed: {failed_count}/{len(product_batch)}")

    except Exception as e:
        logger.error(f"❌ Lỗi khi crawl batch {batch_index}: {e}", exc_info=True)
        # Trả về results với status failed cho tất cả
        if product_batch and isinstance(product_batch, list):
            for product_info in product_batch:
                results.append(
                    {
                        "product_id": product_info.get("product_id", "unknown"),
                        "url": product_info.get("url", ""),
                        "status": "failed",
                        "error": f"Batch error: {str(e)}",
                        "detail": None,
                        "crawled_at": datetime.now().isoformat(),
                    }
                )
        else:
            logger.error("⚠️  Không thể tạo failed results vì product_batch không hợp lệ")

    return results


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

    logger.info(f"🔍 TASK: Crawl Product '{product_name}' | ID: {product_id} | URL: {product_url}")

    result = {
        "product_id": product_id,
        "url": product_url,
        "status": "failed",
        "error": None,
        "detail": None,
        "crawled_at": datetime.now().isoformat(),
    }

    # Kiểm tra cache trước - ưu tiên Redis, fallback về file
    # Kiểm tra xem có force refresh không (từ Airflow Variable)
    force_refresh = get_variable("TIKI_FORCE_REFRESH_CACHE", default="false").lower() == "true"

    if force_refresh:
        logger.info(f"🔄 FORCE REFRESH MODE: Bỏ qua cache cho product {product_id}")
    else:
        # Thử Redis cache trước (nhanh hơn, distributed)
        logger.info(f"🔍 Đang kiểm tra cache cho product {product_id}...")
        redis_cache = None
        try:
            from pipelines.crawl.storage.redis_cache import get_redis_cache

            redis_cache = get_redis_cache("redis://redis:6379/1")
            if redis_cache:
                cached_detail = redis_cache.get_cached_product_detail(product_id)
                if cached_detail:
                    has_price = cached_detail.get("price", {}).get("current_price")
                    has_sales_count = cached_detail.get("sales_count") is not None
                    brand_value = cached_detail.get("brand")
                    has_brand = bool(
                        brand_value and (not isinstance(brand_value, str) or brand_value.strip())
                    )
                    seller_info = cached_detail.get("seller", {})
                    seller_name = seller_info.get("name") if isinstance(seller_info, dict) else None
                    has_seller = bool(
                        seller_name and (not isinstance(seller_name, str) or seller_name.strip())
                    )

                    if has_price and has_sales_count and has_brand and has_seller:
                        logger.info("=" * 70)
                        logger.info(f"✅ SKIP CRAWL - Redis Cache Hit cho product {product_id}")
                        logger.info(f"   - Có price: {has_price}")
                        logger.info(f"   - Có sales_count: {has_sales_count}")
                        logger.info(f"   - Có brand: {has_brand}")
                        logger.info(f"   - Có seller: {has_seller}")
                        logger.info("   - Sử dụng cache, không cần crawl lại")
                        logger.info("=" * 70)
                        result["detail"] = cached_detail
                        result["status"] = "cached"
                        return result
                    if has_price or has_sales_count or has_brand or has_seller:
                        logger.info(
                            f"[Redis Cache] ⚠️  Cache thiếu dữ liệu cho product {product_id}, sẽ crawl lại"
                        )
                else:
                    logger.info(
                        f"[Redis Cache] ⚠️  Cache không đầy đủ cho product {product_id}, sẽ crawl lại"
                    )
        except Exception:
            # Redis không available, fallback về file cache
            pass

        # Fallback: Kiểm tra file cache nếu Redis không available hoặc không có cache
        if not force_refresh:
            cache_file = DETAIL_CACHE_DIR / f"{product_id}.json"
            if cache_file.exists():
                try:
                    with open(cache_file, encoding="utf-8") as f:
                        cached_detail = json.load(f)
                        has_price = cached_detail.get("price", {}).get("current_price")
                        has_sales_count = cached_detail.get("sales_count") is not None
                        brand_value = cached_detail.get("brand")
                        has_brand = bool(
                            brand_value
                            and (not isinstance(brand_value, str) or brand_value.strip())
                        )
                        seller_info = cached_detail.get("seller", {})
                        seller_name = (
                            seller_info.get("name") if isinstance(seller_info, dict) else None
                        )
                        has_seller = bool(
                            seller_name
                            and (not isinstance(seller_name, str) or seller_name.strip())
                        )

                        if has_price and has_sales_count and has_brand and has_seller:
                            logger.info("=" * 70)
                            logger.info(f"✅ SKIP CRAWL - File Cache Hit cho product {product_id}")
                            logger.info(f"   - Có price: {has_price}")
                            logger.info(f"   - Có sales_count: {has_sales_count}")
                            logger.info(f"   - Có brand: {has_brand}")
                            logger.info(f"   - Có seller: {has_seller}")
                            logger.info("   - Sử dụng cache, không cần crawl lại")
                            logger.info("=" * 70)
                            result["detail"] = cached_detail
                            result["status"] = "cached"
                            return result
                except Exception:
                    # File cache lỗi, tiếp tục crawl
                    pass

    # Tiếp tục crawl nếu không có cache hoặc force refresh
    # (File cache check đã được xử lý ở trên trong else block)

    # Bắt đầu crawl product detail
    try:
        # Lazily initialize expensive singletons only when the task runs
        ensure_output_dirs()
        tiki_degradation = get_tiki_degradation()
        tiki_circuit_breaker = get_tiki_circuit_breaker()
        tiki_dlq = get_tiki_dlq()

        # Kiểm tra graceful degradation
        if tiki_degradation.should_skip():
            logger.warning("=" * 70)
            logger.warning(f"⚠️  SKIP CRAWL - Service Degraded cho product {product_id}")
            logger.warning("   - Service đang ở trạng thái FAILED")
            logger.warning("   - Graceful degradation: skip crawl để tránh làm tệ hơn")
            logger.warning("=" * 70)
            result["error"] = "Service đang ở trạng thái FAILED, skip crawl"
            result["status"] = "degraded"
            return result

        # Validate URL
        if not product_url or not product_url.startswith("http"):
            raise ValueError(f"URL không hợp lệ: {product_url}")

        # Lấy cấu hình
        rate_limit_delay = float(
            get_variable("TIKI_DETAIL_RATE_LIMIT_DELAY", default="0.1")
        )  # Delay 1.5s cho detail (tối ưu từ 2.0s)
        timeout = int(
            get_variable("TIKI_DETAIL_CRAWL_TIMEOUT", default="180")
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
                """
                Wrapper function để gọi với circuit breaker.
                """
                return crawl_product_detail_with_selenium(
                    product_url,
                    save_html=False,
                    verbose=False,  # Không verbose trong Airflow
                    max_retries=3,  # Retry 3 lần (tăng từ 2)
                    timeout=120,  # Tăng từ 60 -> 120s (2 phút) để đủ thời gian cho trang load đầy đủ
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
            hierarchy_map = get_hierarchy_map()
            detail = extract_product_detail(
                html_content, product_url, verbose=False, hierarchy_map=hierarchy_map
            )

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
        # Redis cache (nhanh, distributed) - CRITICAL: Chuẩn hóa URL trước khi cache
        if redis_cache:
            try:
                # IMPORTANT: Sử dụng product_id (không phụ thuộc vào URL) để cache
                # Điều này đảm bảo rằng cùng 1 product từ category khác nhau sẽ hit cache
                redis_cache.cache_product_detail(product_id, detail, ttl=604800)  # 7 ngày
                logger.info(
                    f"[Redis Cache] ✅ Đã cache detail cho product {product_id} (TTL: 7 days)"
                )
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
    # Kiểm tra result có hợp lệ không trước khi return
    if not result or not isinstance(result, dict):
        logger.warning("⚠️  Result không hợp lệ, sử dụng default_result")
        result = default_result.copy()
        result["error"] = "Result không hợp lệ"
        result["status"] = "failed"

    # Đảm bảo result có đầy đủ các field cần thiết
    if "product_id" not in result:
        result["product_id"] = product_id if "product_id" in locals() else "unknown"
    if "url" not in result:
        result["url"] = product_url if "product_url" in locals() else ""
    if "status" not in result:
        result["status"] = "failed"
    if "crawled_at" not in result:
        result["crawled_at"] = datetime.now().isoformat()

    try:
        return result
    except Exception as e:
        # Nếu có lỗi khi return (không thể xảy ra nhưng để an toàn)
        logger.error(f"❌ Lỗi khi return result: {e}", exc_info=True)
        default_result["error"] = f"Lỗi khi return result: {str(e)}"
        default_result["product_id"] = product_id if "product_id" in locals() else "unknown"
        default_result["url"] = product_url if "product_url" in locals() else ""
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
        ensure_output_dirs()
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
                import json as json_module  # noqa: F401

                with open(OUTPUT_FILE, encoding="utf-8") as f:
                    data = json_module.load(f)
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

        # Số lượng products thực tế được crawl
        expected_products_count = len(products_to_crawl) if products_to_crawl else 0
        # Với batch processing, số map_index = số batches, không phải số products
        # Lấy batch size từ config
        try:
            from pipelines.crawl.config import PRODUCT_BATCH_SIZE

            batch_size = PRODUCT_BATCH_SIZE
        except Exception:
            batch_size = 12  # Default fallback
        expected_crawl_count = (
            (expected_products_count + batch_size - 1) // batch_size
            if expected_products_count > 0
            else 0
        )
        logger.info(
            f"📊 Số products: {expected_products_count}, Số batches dự kiến: {expected_crawl_count}"
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
            logger.warning("=" * 70)
            logger.warning("⚠️  KHÔNG CÓ PRODUCTS NÀO ĐƯỢC CRAWL DETAIL!")
            logger.warning("=" * 70)
            logger.warning("💡 Nguyên nhân có thể:")
            logger.warning("   - Tất cả products đã có trong database với detail đầy đủ")
            logger.warning("   - Tất cả products đã có trong cache với detail đầy đủ")
            logger.warning("   - Tất cả products đã được crawl trước đó (từ progress file)")
            logger.warning("   - Không có products nào được prepare để crawl")
            logger.warning("=" * 70)
            logger.warning("💡 Để force crawl lại, kiểm tra task 'prepare_products_for_detail' log")
            logger.warning("=" * 70)
            # Trả về products gốc không có detail
            return {
                "products": products,
                "stats": {
                    "total_products": len(products),
                    "with_detail": 0,
                    "cached": 0,
                    "failed": 0,
                    "timeout": 0,
                    "crawled_count": 0,
                },
                "merged_at": datetime.now().isoformat(),
            }

        # Lấy detail results từ Dynamic Task Mapping
        task_id = "crawl_product_details.crawl_product_detail"
        all_detail_results = []

        # Lấy tất cả results bằng cách lấy từng map_index để tránh giới hạn XCom
        # CHỈ lấy từ map_index 0 đến actual_crawl_count - 1 (không phải len(products))
        # Fetch detail results from crawled products

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
                    f"💓 [Heartbeat] Đang xử lý batch {batch_num}/{total_batches} (index {start_idx}-{end_idx - 1})..."
                )

            try:
                batch_results = ti.xcom_pull(
                    task_ids=task_id, key="return_value", map_indexes=batch_map_indexes
                )

                if batch_results:
                    if isinstance(batch_results, list):
                        # List results theo thứ tự map_indexes
                        # Mỗi result có thể là list (từ batch) hoặc dict (từ single)
                        for result in batch_results:
                            if result:
                                if isinstance(result, list):
                                    # Batch result: flatten list of results
                                    all_detail_results.extend([r for r in result if r])
                                elif isinstance(result, dict):
                                    # Single result
                                    all_detail_results.append(result)
                    elif isinstance(batch_results, dict):
                        # Dict với key là map_index hoặc string
                        # Lấy tất cả values, sắp xếp theo map_index nếu có thể
                        for value in batch_results.values():
                            if value:
                                if isinstance(value, list):
                                    # Batch result: flatten
                                    all_detail_results.extend([r for r in value if r])
                                elif isinstance(value, dict):
                                    # Single result
                                    all_detail_results.append(value)
                    else:
                        # Single result
                        if isinstance(batch_results, list):
                            # Batch result: flatten
                            all_detail_results.extend([r for r in batch_results if r])
                        else:
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
        # Reset valid_results
        valid_results = 0
        error_details = {}  # Thống kê chi tiết các loại lỗi
        failed_products = []  # Danh sách products bị fail để phân tích

        for detail_result in all_detail_results:
            if detail_result and isinstance(detail_result, dict):
                product_id = detail_result.get("product_id")
                if product_id:
                    detail_dict[product_id] = detail_result
                    status = detail_result.get("status", "failed")
                    error = detail_result.get("error")

                    # Đếm số lượng products được crawl (tất cả các status trừ "not_crawled")
                    if status in [
                        "success",
                        "failed",
                        "timeout",
                        "degraded",
                        "circuit_breaker_open",
                        "selenium_error",
                        "network_error",
                        "extract_error",
                        "validation_error",
                        "memory_error",
                    ]:
                        stats["crawled_count"] += 1

                    if status == "success":
                        stats["with_detail"] += 1
                        valid_results += 1
                    elif status == "cached":
                        stats["cached"] += 1
                        valid_results += 1
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
        products_no_brand = 0  # Đếm số products bị loại bỏ vì brand null

        for product in products:
            product_id = product.get("product_id")
            detail_result = detail_dict.get(product_id)

            if detail_result and detail_result.get("detail"):
                status = detail_result.get("status", "failed")

                # CHỈ lưu products có status == "success" (đã crawl thành công, không phải từ cache)
                if status == "success":
                    # Merge detail vào product
                    detail = detail_result["detail"]

                    # Kiểm tra nếu detail là None hoặc rỗng
                    if detail is None:
                        logger.warning(f"⚠️  Detail là None cho product {product_id}")
                        products_failed += 1
                        continue

                    # Kiểm tra nếu detail là string (JSON), parse nó
                    if isinstance(detail, str):
                        # Bỏ qua string rỗng
                        if not detail.strip():
                            logger.warning(f"⚠️  Detail là string rỗng cho product {product_id}")
                            products_failed += 1
                            continue

                        try:
                            import json

                            detail = json.loads(detail)
                        except (json.JSONDecodeError, TypeError) as e:
                            logger.warning(
                                f"⚠️  Không thể parse detail JSON cho product {product_id}: {e}, detail type: {type(detail)}, detail value: {str(detail)[:100]}"
                            )
                            products_failed += 1
                            continue

                    # Kiểm tra nếu detail không phải là dict
                    if not isinstance(detail, dict):
                        logger.warning(
                            f"⚠️  Detail không phải là dict cho product {product_id}: {type(detail)}, value: {str(detail)[:100]}"
                        )
                        products_failed += 1
                        continue

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
                    detail_metadata = detail.get("_metadata")
                    if detail_metadata:
                        product_with_detail["_metadata"] = detail_metadata
                        product_with_detail["_metadata"]["crawl_status"] = status
                        if detail_result.get("crawled_at"):
                            product_with_detail["_metadata"]["completed_at"] = detail_result.get(
                                "crawled_at"
                            )
                    elif status or detail_result.get("crawled_at"):
                        product_with_detail["_metadata"] = {
                            "crawl_status": status,
                            "completed_at": detail_result.get("crawled_at"),
                        }

                    # CRITICAL: Lọc bỏ products có brand null/empty
                    # Brand thiếu thường dẫn đến nhiều trường khác cũng thiếu
                    # Seller có thể là "Unknown" - vẫn lưu lại
                    # Những products này sẽ được crawl lại trong lần chạy tiếp theo
                    brand = product_with_detail.get("brand")

                    # Only skip if BRAND is missing/empty (seller can be "Unknown")
                    # Only skip if BRAND is missing/empty (seller can be "Unknown")
                    if not brand or (isinstance(brand, str) and not brand.strip()):
                        # 2 DAGs Strategy: Mark for enrichment
                        logger.warning(
                            f"ℹ️  Product {product_id} missing brand. Marked for enrichment."
                        )
                        product_with_detail["needs_enrichment"] = True
                        product_with_detail["crawl_quality"] = "basic"
                        # Keep brand as None/Empty (will be filled by Enrichment DAG)
                    else:
                        product_with_detail["needs_enrichment"] = False
                        product_with_detail["crawl_quality"] = "full"
                        # Don't increment products_no_brand since we are fixing it
                        # Don't increment products_failed since we are keeping it

                        # logger.warning(
                        #     f"⚠️  Product {product_id} ({product_with_detail.get('name', 'Unknown')[:50]}) "
                        #     f"có brand null/empty, sẽ bỏ qua để crawl lại lần sau"
                        # )
                        # products_no_brand += 1
                        # products_failed += 1
                        # continue

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
        logger.info(f"🚫 Products không có brand (đã bỏ qua để crawl lại): {products_no_brand}")
        logger.info(f"🚫 Products không có detail (đã bỏ qua): {products_without_detail}")
        logger.info("=" * 70)

        # Cảnh báo nếu có nhiều products không có brand (>10% total products)
        if products_no_brand > 0 and stats["total_products"] > 0:
            no_brand_rate = (products_no_brand / stats["total_products"]) * 100
            if no_brand_rate > 10:
                logger.warning("=" * 70)
                logger.warning(
                    f"⚠️  CẢNH BÁO: Có {products_no_brand} products ({no_brand_rate:.1f}%) không có brand!"
                )
                logger.warning("   Những products này sẽ được crawl lại trong lần chạy tiếp theo.")
                logger.warning("   Nguyên nhân có thể:")
                logger.warning("   - Trang detail không load đầy đủ (network issue, timeout)")
                logger.warning("   - HTML structure thay đổi (cần update selector)")
                logger.warning("   - Rate limit quá cao (cần giảm TIKI_DETAIL_RATE_LIMIT_DELAY)")
                logger.warning("=" * 70)
            elif no_brand_rate > 0:
                logger.info(
                    f"💡 Có {products_no_brand} products ({no_brand_rate:.1f}%) không có brand, sẽ crawl lại lần sau"
                )

        # Cập nhật stats để phản ánh số lượng products thực tế được lưu
        stats["products_saved"] = len(products_with_detail)
        stats["products_skipped"] = products_without_detail
        stats["products_cached_skipped"] = products_cached
        stats["products_failed_skipped"] = products_failed
        stats["products_no_brand_skipped"] = products_no_brand

        result = {
            "products": products_with_detail,
            "stats": stats,
            "merged_at": datetime.now().isoformat(),
            "note": f"Chỉ lưu {len(products_with_detail)} products có status='success' và brand/seller không null (đã bỏ qua {products_cached} cached, {products_failed} failed, {products_no_brand} không có brand, {products_without_detail} không có detail)",
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
        ensure_output_dirs()
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
