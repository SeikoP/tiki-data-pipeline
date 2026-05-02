from __future__ import annotations

import os

from airflow.providers.standard.operators.python import ShortCircuitOperator
from tiki_crawl_products_v2.bootstrap import (
    DAG,
    DAG_CONFIG,
    PythonOperator,
    TaskGroup,
    get_int_variable,
    timedelta,
)
from tiki_crawl_products_v2.tasks import (
    aggregate_and_notify,
    backup_database,
    cleanup_categories_wrapper,
    cleanup_incomplete_products_wrapper,
    cleanup_old_history_wrapper,
    cleanup_redis_cache,
    crawl_category_batch,
    crawl_product_batch_fast,
    crawl_single_category,
    load_categories,
    load_categories_to_db_wrapper,
    load_products,
    merge_product_details,
    merge_products,
    prepare_products_for_detail,
    reconcile_categories_wrapper,
    transform_products,
)

# Safe Mode: airflow DAG
# This file is an Airflow DAG for Tiki product crawling (Speed Layer).


with DAG(
    dag_id="tiki_crawl_fast",
    description="Speed Layer: API-based Fast Crawling",
    **{k: v for k, v in DAG_CONFIG.items() if k not in ["dag_id", "description"]},
) as dag:
    # ===== STEP 1: PRE-CRAWL CLEANUP & LOAD =====
    with TaskGroup("pre_crawl") as pre_crawl_group:
        task_cleanup_products = PythonOperator(
            task_id="cleanup_incomplete_products",
            python_callable=cleanup_incomplete_products_wrapper,
            execution_timeout=timedelta(minutes=5),
            pool="fast_pool",
            doc_md="""
            ### Cleanup Incomplete Products
            **Nhiệm vụ:** Xóa products thiếu thông tin quan trọng (seller, brand) để crawl lại

            **Chi tiết:**
            - Xóa products không có seller hoặc brand
            - Cho phép re-crawl products không đầy đủ
            - Chạy TRƯỚC khi crawl để đảm bảo data quality

            **Output:** Số lượng products đã xóa
            """,
        )

        task_load_categories = PythonOperator(
            task_id="load_categories",
            python_callable=load_categories,
            execution_timeout=timedelta(minutes=5),
            pool="fast_pool",
            doc_md="""
            ### Load Categories
            **Nhiệm vụ:** Load danh sách categories từ file JSON để crawl

            **Chi tiết:**
            - Đọc categories từ `data/raw/categories.json`
            - Lọc theo `crawl_only_leaf` flag (chỉ crawl leaf categories)
            - Lưu vào file và trả về metadata qua XCom

            **Output:** File path + metadata (count, crawl_only_leaf)
            """,
        )

        # Tối ưu: Cho phép chạy song song vì task_cleanup_products và task_load_categories độc lập
        # Không còn sử dụng task_cleanup_products >> task_load_categories

    # TaskGroup: Crawl Categories (Dynamic Task Mapping)
    with TaskGroup("crawl_categories") as crawl_group:
        # Sử dụng expand để Dynamic Task Mapping
        # Cần một task helper để lấy categories và tạo list op_kwargs
        def prepare_crawl_kwargs(**context):
            """
            Helper function để prepare op_kwargs cho Dynamic Task Mapping.
            """
            import json
            import logging

            logger = logging.getLogger("airflow.task")
            ti = context["ti"]
            result = ti.xcom_pull(task_ids="pre_crawl.load_categories")

            # Đọc categories từ file
            categories_file = result.get("categories_file") if result else None
            if not categories_file:
                logger.error("❌ Không thể lấy categories file path từ XCom!")
                return []

            with open(categories_file, encoding="utf-8") as f:
                categories = json.load(f)

            logger.info(f"📂 Đọc {len(categories)} categories từ {categories_file}")
            return [{"category": cat} for cat in categories]

        def prepare_category_batch_kwargs(**context):
            """
            Helper function để prepare op_kwargs cho Dynamic Task Mapping với batch processing.
            """
            import json
            import logging
            import math

            logger = logging.getLogger("airflow.task")
            ti = context["ti"]
            result = ti.xcom_pull(task_ids="pre_crawl.load_categories")

            # Đọc categories từ file
            categories_file = result.get("categories_file") if result else None
            if not categories_file:
                logger.error("❌ Không thể lấy categories file path từ XCom!")
                return []

            with open(categories_file, encoding="utf-8") as f:
                categories = json.load(f)

            logger.info(f"📂 Đọc {len(categories)} categories từ {categories_file}")

            # Tối ưu: Chia thành n batch task để tận dụng tối đa pool (mặc định 3)
            num_batches = get_int_variable("TIKI_CATEGORY_NUM_BATCHES", default=3)
            total_categories = len(categories)

            if total_categories <= num_batches:
                # Nếu ít category hơn số batch mong muốn, mỗi category 1 task
                batch_size = 1
            else:
                batch_size = math.ceil(total_categories / num_batches)

            batches = [
                categories[i : i + batch_size] for i in range(0, total_categories, batch_size)
            ]

            logger.info(
                f"📦 Chia {total_categories} categories thành {len(batches)} batches (size ~{batch_size}) để tối ưu pool"
            )

            return [
                {"category_batch": batch, "batch_index": idx} for idx, batch in enumerate(batches)
            ]

        if crawl_category_batch is not None:
            task_prepare = PythonOperator(
                task_id="prepare_crawl_kwargs",
                python_callable=prepare_category_batch_kwargs,
                execution_timeout=timedelta(minutes=1),
                doc_md="""
                ### Prepare Category Crawl Batches
                **Nhiệm vụ:** Chia categories thành batches để crawl song song

                **Chi tiết:**
                - Chia categories thành 3 batches (tối ưu cho pool size)
                - Mỗi batch chứa ~N/3 categories
                - Tạo op_kwargs cho Dynamic Task Mapping

                **Output:** List of batch configs cho crawl_category
                """,
            )
            task_crawl_category = PythonOperator.partial(
                task_id="crawl_category",
                python_callable=crawl_category_batch,
                execution_timeout=timedelta(minutes=45),
                pool="fast_pool",
                retries=2,
                retry_delay=timedelta(seconds=30),
                priority_weight=10,
                doc_md="""
                ### Crawl Category Batch (Mapped)
                **Nhiệm vụ:** Crawl products từ một batch categories qua Tiki API

                **Chi tiết:**
                - Crawl parallel nhiều categories trong 1 batch
                - Sử dụng aiohttp async để tăng tốc
                - Lưu products vào memory, trả về qua XCom
                - Retry 2 lần nếu fail

                **Output:** List products từ batch này
                """,
            ).expand(op_kwargs=task_prepare.output)
        else:
            task_prepare = PythonOperator(
                task_id="prepare_crawl_kwargs",
                python_callable=prepare_crawl_kwargs,
                execution_timeout=timedelta(minutes=1),
                doc_md="""
                ### Prepare Category Crawl (Single)
                **Nhiệm vụ:** Chuẩn bị kwargs cho crawl từng category riêng lẻ

                **Output:** List of category configs
                """,
            )
            task_crawl_category = PythonOperator.partial(
                task_id="crawl_category",
                python_callable=crawl_single_category,
                execution_timeout=timedelta(minutes=45),  # Giảm từ 2h xuống 45 phút
                pool="fast_pool",
                retries=2,  # Thêm retries
                retry_delay=timedelta(seconds=30),
                priority_weight=10,
                doc_md="""
                ### Crawl Single Category (Mapped)
                **Nhiệm vụ:** Crawl products từ 1 category qua Tiki API

                **Output:** Products từ category này
                """,
            ).expand(op_kwargs=task_prepare.output)

    # TaskGroup: Process và Save
    with TaskGroup("process_and_save") as process_group:
        task_merge_products = PythonOperator(
            task_id="merge_products",
            python_callable=merge_products,
            execution_timeout=timedelta(minutes=30),
            pool="fast_pool",
            trigger_rule="all_done",
            priority_weight=8,
            doc_md="""
            ### Merge & Save Products
            **Nhiệm vụ:** Merge products từ tất cả category batches và lưu vào file

            **Chi tiết:**
            - Lấy products từ tất cả mapped tasks (crawl_category)
            - Loại bỏ duplicates theo product_id
            - Tính toán stats (success/failed categories)
            - Lưu vào file `merged_products.json` và `products.json`

            **Output:** File path + stats + product count
            """,
        )

    # TaskGroup: Crawl Product Details (Dynamic Task Mapping)
    with TaskGroup("crawl_product_details") as detail_group:

        def prepare_detail_kwargs(**context):
            """
            Helper function để prepare op_kwargs cho Dynamic Task Mapping detail.
            Chia batch thông minh dựa trên số lượng products và max_active_tis_per_dagrun.
            """
            import logging
            import math

            logger = logging.getLogger("airflow.task")
            ti = context["ti"]
            products_to_crawl = ti.xcom_pull(
                task_ids="crawl_product_details.prepare_products_for_detail"
            )
            if not products_to_crawl:
                return []

            total_products = len(products_to_crawl)

            # Số batch tasks chạy đồng thời (từ DAG config)
            max_concurrent_batches = 3

            # Tính batch size tối ưu: chia đều cho các batch tasks
            # Với 1000 products, muốn có 3-6 batches thay vì 12
            if total_products <= max_concurrent_batches * 10:
                # Ít products: chia nhỏ để tận dụng 3 tasks song song
                batch_size = max(1, math.ceil(total_products / max_concurrent_batches))
            else:
                # Nhiều products: dùng batch size lớn (300-350) để có ít batches hơn
                base_batch_size = get_int_variable("TIKI_DETAIL_BATCH_SIZE", default=333)
                # Điều chỉnh để số batches là bội số của max_concurrent_batches
                num_batches = max(
                    max_concurrent_batches, math.ceil(total_products / base_batch_size)
                )
                # Làm tròn num_batches lên bội số của max_concurrent_batches
                num_batches = (
                    math.ceil(num_batches / max_concurrent_batches) * max_concurrent_batches
                )
                batch_size = math.ceil(total_products / num_batches)

            batches = [
                products_to_crawl[i : i + batch_size]
                for i in range(0, len(products_to_crawl), batch_size)
            ]

            logger.info(
                f"📦 Chia {total_products} products thành {len(batches)} batches "
                f"(size ~{batch_size}) để tối ưu {max_concurrent_batches} tasks song song"
            )

            return [
                {"product_batch": batch, "batch_index": idx} for idx, batch in enumerate(batches)
            ]

        task_prepare_detail = PythonOperator(
            task_id="prepare_products_for_detail",
            python_callable=prepare_products_for_detail,
            execution_timeout=timedelta(minutes=5),
            doc_md="""
            ### Prepare Products for Detail Crawl
            **Nhiệm vụ:** Lọc products cần crawl detail (chưa có hoặc cũ)

            **Chi tiết:**
            - Check DB: Skip products đã có detail đầy đủ + updated gần đây
            - Giới hạn theo TIKI_PRODUCTS_PER_DAY (default: 1000)
            - Ưu tiên products chưa có brand/seller

            **Output:** List products cần crawl detail
            """,
        )

        task_prepare_detail_kwargs = PythonOperator(
            task_id="prepare_detail_kwargs",
            python_callable=prepare_detail_kwargs,
            execution_timeout=timedelta(minutes=1),
            doc_md="""
            ### Prepare Detail Crawl Batches
            **Nhiệm vụ:** Chia products thành batches để crawl detail song song

            **Chi tiết:**
            - Chia products thành batches (size ~333)
            - Tối ưu để có 3-6 batches chạy song song
            - Giảm số mapped tasks để UI không lag

            **Output:** List of batch configs
            """,
        )

        task_crawl_product_detail = PythonOperator.partial(
            task_id="crawl_product_detail",
            python_callable=crawl_product_batch_fast,
            execution_timeout=timedelta(minutes=15),
            pool="fast_pool",
            retries=2,
            retry_delay=timedelta(seconds=30),
            priority_weight=9,
            doc_md="""
            ### Crawl Product Detail Batch (Mapped)
            **Nhiệm vụ:** Crawl chi tiết products (price, sales, brand, seller) qua API

            **Chi tiết:**
            - Crawl async với aiohttp (nhanh hơn Selenium)
            - Lấy thông tin: price, sales_count, brand, seller, rating
            - Lưu vào crawl_history để track price changes
            - Retry 2 lần nếu fail

            **Output:** List product details từ batch này
            """,
        ).expand(op_kwargs=task_prepare_detail_kwargs.output)

        task_merge_product_details = PythonOperator(
            task_id="merge_product_details",
            python_callable=merge_product_details,
            execution_timeout=timedelta(minutes=15),
            pool="fast_pool",
            trigger_rule="all_done",
            priority_weight=8,
            doc_md="""
            ### Merge & Save Product Details
            **Nhiệm vụ:** Merge product details từ tất cả batches và lưu vào file

            **Chi tiết:**
            - Lấy details từ tất cả mapped tasks (crawl_product_detail)
            - Merge với products gốc theo product_id
            - Lưu vào file `products_with_detail.json`
            - Tính stats (crawled, success, failed)

            **Output:** File path + stats + crawled count
            """,
        )

        (
            task_prepare_detail
            >> task_prepare_detail_kwargs
            >> task_crawl_product_detail
            >> task_merge_product_details
        )

    # TaskGroup: Transform and Load
    with TaskGroup("transform_and_load") as transform_load_group:
        # task_enrich_category_path removed as it was undefined and redundant

        task_load_categories_db = PythonOperator(
            task_id="load_categories_to_db",
            python_callable=load_categories_to_db_wrapper,
            execution_timeout=timedelta(minutes=5),
            pool="fast_pool",
            priority_weight=7,  # Higher priority - must run before load_products
            doc_md="""
            ### Load Categories to Database
            **Nhiệm vụ:** Load/update categories vào PostgreSQL (TRƯỚC khi load products)

            **Chi tiết:**
            - Load categories từ file JSON vào bảng `categories`
            - Update category metadata (name, path, is_leaf)
            - UPSERT để tránh duplicates
            - **Phải chạy trước load_products** do FK constraint

            **Output:** Load stats
            """,
        )

        task_transform_products = PythonOperator(
            task_id="transform_products",
            python_callable=transform_products,
            execution_timeout=timedelta(minutes=30),
            pool="fast_pool",
            priority_weight=6,  # Priority trung bình
            doc_md="""
            ### Transform Products
            **Nhiệm vụ:** Chuẩn hóa và validate dữ liệu products trước khi load vào DB

            **Chi tiết:**
            - Normalize fields (price, name, category_path)
            - Validate data quality (required fields)
            - Enrich category_path từ categories file
            - Loại bỏ invalid products
            - Hỗ trợ vectorized transform (nhanh hơn)

            **Output:** File `products_transformed.json` + transform stats
            """,
        )

        task_load_products = PythonOperator(
            task_id="load_products",
            python_callable=load_products,
            execution_timeout=timedelta(minutes=30),
            pool="fast_pool",
            priority_weight=5,
            doc_md="""
            ### Load Products to Database
            **Nhiệm vụ:** Load transformed products vào PostgreSQL database

            **Chi tiết:**
            - Bulk insert/update products vào bảng `products`
            - Lưu price history vào bảng `crawl_history`
            - Sử dụng UPSERT (ON CONFLICT UPDATE)
            - Track inserted/updated counts

            **Output:** Load stats (inserted, updated, failed)
            """,
        )
        # Categories MUST be loaded before products (FK constraint)
        # Transform can run in parallel with load_categories
        [task_load_categories_db, task_transform_products] >> task_load_products

    # TaskGroup: Maintenance and Reports
    with TaskGroup("maintenance_and_reports") as maintenance_group:
        task_reconcile = PythonOperator(
            task_id="reconcile_categories",
            python_callable=reconcile_categories_wrapper,
            execution_timeout=timedelta(minutes=10),
            pool="fast_pool",
            priority_weight=3,
            doc_md="""
            ### Reconcile Categories
            **Nhiệm vụ:** Đồng bộ category names và product counts từ JSON

            **Chi tiết:**
            - Update category names từ JSON file
            - Update product_count cho mỗi category
            - Đảm bảo data consistency

            **Output:** Updated counts
            """,
        )

        task_cleanup_categories = PythonOperator(
            task_id="cleanup_categories",
            python_callable=cleanup_categories_wrapper,
            execution_timeout=timedelta(minutes=10),
            pool="fast_pool",
            priority_weight=2,
            doc_md="""
            ### Cleanup Categories
            **Nhiệm vụ:** Xóa orphan và redundant categories

            **Chi tiết:**
            - Xóa categories không có products (orphan)
            - Xóa non-leaf categories (redundant)
            - Giải phóng database space

            **Output:** Deleted counts
            """,
        )

        task_cleanup_history = PythonOperator(
            task_id="cleanup_old_history",
            python_callable=cleanup_old_history_wrapper,
            execution_timeout=timedelta(minutes=5),
            pool="fast_pool",
            priority_weight=1,  # Priority thấp nhất
            doc_md="""
            ### Cleanup Old History
            **Nhiệm vụ:** Archive và xóa crawl history cũ

            **Chi tiết:**
            - Archive history > 6 tháng
            - Xóa history > 12 tháng
            - Giảm database size

            **Output:** Archived/deleted counts
            """,
        )

        # Check if Discord is enabled before running aggregate_and_notify
        def check_discord_enabled(**context):
            """Skip aggregate_and_notify if Discord is disabled."""
            discord_enabled = os.getenv("DISCORD_ENABLED", "false").lower() == "true"
            if not discord_enabled:
                context["ti"].log.info("⏭️  Discord disabled, skipping aggregate_and_notify")
            return discord_enabled

        task_check_discord = ShortCircuitOperator(
            task_id="check_discord_enabled",
            python_callable=check_discord_enabled,
            doc_md="""
            ### Check Discord Enabled
            **Nhiệm vụ:** Kiểm tra Discord notification có enabled không

            **Chi tiết:**
            - Check env var DISCORD_ENABLED
            - Skip aggregate_and_notify nếu disabled

            **Output:** Boolean (continue or skip)
            """,
        )

        task_aggregate_and_notify = PythonOperator(
            task_id="aggregate_and_notify",
            python_callable=aggregate_and_notify,
            execution_timeout=timedelta(minutes=15),
            pool="fast_pool",
            trigger_rule="all_done",
            priority_weight=2,
            doc_md="""
            ### Aggregate & Notify Discord
            **Nhiệm vụ:** Tổng hợp stats và gửi notification qua Discord

            **Chi tiết:**
            - Aggregate stats từ tất cả tasks
            - Tạo summary report (products, categories, errors)
            - Gửi notification qua Discord webhook
            - Chỉ chạy nếu Discord enabled

            **Output:** Notification status
            """,
        )

        task_cleanup_cache = PythonOperator(
            task_id="cleanup_redis_cache",
            python_callable=cleanup_redis_cache,
            execution_timeout=timedelta(minutes=5),
            pool="fast_pool",
            trigger_rule="all_done",
            priority_weight=1,
            doc_md="""
            ### Cleanup Redis Cache
            **Nhiệm vụ:** Cleanup Redis cache (nếu cần)

            **Chi tiết:**
            - Check Redis memory usage
            - Redis tự động expire keys theo TTL
            - Task này chỉ log stats, không flush cache

            **Output:** Cache stats
            """,
        )

        task_backup_database = PythonOperator(
            task_id="backup_database",
            python_callable=backup_database,
            execution_timeout=timedelta(minutes=15),
            pool="fast_pool",
            trigger_rule="all_done",
            priority_weight=1,
            doc_md="""
            ### Backup Database
            **Nhiệm vụ:** Backup PostgreSQL database

            **Chi tiết:**
            - Backup database `tiki` bằng pg_dump
            - Lưu vào `/opt/airflow/data/backups/postgres/`
            - Format: SQL plain text (dễ restore)
            - Chạy cuối cùng sau tất cả tasks

            **Output:** Backup file path + size
            """,
        )

        task_reconcile >> task_cleanup_categories

        # Aggregate and notify - only if Discord enabled
        task_cleanup_categories >> task_check_discord >> task_aggregate_and_notify

    # ===== FINAL DAG DEPENDENCIES =====
    # Loại bỏ labels và empty operators để tasks gần nhau hơn

    # Main flow: pre_crawl → crawl → process
    pre_crawl_group >> crawl_group >> process_group

    # detail_group bắt đầu ngay sau merge_products (parallel với process_group)
    task_merge_products >> detail_group

    # transform_load_group chờ cả process và detail hoàn thành
    [process_group, detail_group] >> transform_load_group >> maintenance_group
