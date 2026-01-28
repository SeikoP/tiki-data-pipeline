from __future__ import annotations

from .bootstrap import DAG, DAG_CONFIG, PythonOperator, logging, timedelta
from .tasks import (
    aggregate_and_notify,
    backup_database,
    cleanup_incomplete_products_wrapper,
    cleanup_old_history_wrapper,
    cleanup_orphan_categories_wrapper,
    cleanup_redis_cache,
    cleanup_redundant_categories_wrapper,
    crawl_product_batch,
    crawl_single_category,
    load_categories,
    load_categories_to_db_wrapper,
    load_products,
    merge_product_details,
    merge_products,
    prepare_products_for_detail,
    reconcile_categories_wrapper,
    save_products,
    save_products_with_detail,
    transform_products,
    validate_data,
)

with DAG(**DAG_CONFIG) as dag:
    # ===== STEP 1: PRE-CRAWL CLEANUP & LOAD =====
    with TaskGroup("pre_crawl") as pre_crawl_group:
        task_cleanup_products = PythonOperator(
            task_id="cleanup_incomplete_products",
            python_callable=cleanup_incomplete_products_wrapper,
            execution_timeout=timedelta(minutes=5),
            pool="crawl_pool",
        )

        task_load_categories = PythonOperator(
            task_id="load_categories",
            python_callable=load_categories,
            execution_timeout=timedelta(minutes=5),
            pool="crawl_pool",
        )

        # Tối ưu: Cho phép chạy song song vì task_cleanup_products và task_load_categories độc lập
        # Không còn sử dụng task_cleanup_products >> task_load_categories
        pass

    # TaskGroup: Crawl Categories (Dynamic Task Mapping)
    with TaskGroup("crawl_categories") as crawl_group:
        # Sử dụng expand để Dynamic Task Mapping
        # Cần một task helper để lấy categories và tạo list op_kwargs
        def prepare_crawl_kwargs(**context):
            """Helper function để prepare op_kwargs cho Dynamic Task Mapping"""
            import logging

            logger = logging.getLogger("airflow.task")
            ti = context["ti"]
            categories = ti.xcom_pull(task_ids="pre_crawl.load_categories")
            if not categories:
                logger.error("❌ Không thể lấy categories từ XCom!")
                return []
            return [{"category": cat} for cat in categories]

        def prepare_category_batch_kwargs(**context):
            """Helper function để prepare op_kwargs cho Dynamic Task Mapping với batch processing"""
            import logging

            logger = logging.getLogger("airflow.task")
            ti = context["ti"]
            categories = ti.xcom_pull(task_ids="pre_crawl.load_categories")
            if not categories:
                logger.error("❌ Không thể lấy categories từ XCom!")
                return []
            batch_size = int(get_variable("TIKI_CATEGORY_BATCH_SIZE", default="10"))
            batches = [
                categories[i : i + batch_size] for i in range(0, len(categories), batch_size)
            ]
            return [
                {"category_batch": batch, "batch_index": idx} for idx, batch in enumerate(batches)
            ]

        if crawl_category_batch is not None:
            task_prepare = PythonOperator(
                task_id="prepare_batch_kwargs",
                python_callable=prepare_category_batch_kwargs,
                execution_timeout=timedelta(minutes=1),
            )
            task_crawl_category = PythonOperator.partial(
                task_id="crawl_category",
                python_callable=crawl_category_batch,
                execution_timeout=timedelta(minutes=12),
                pool="crawl_pool",
                retries=1,
                retry_delay=timedelta(seconds=15),
            ).expand(op_kwargs=task_prepare.output)
        else:
            task_prepare = PythonOperator(
                task_id="prepare_crawl_kwargs",
                python_callable=prepare_crawl_kwargs,
                execution_timeout=timedelta(minutes=1),
            )
            task_crawl_category = PythonOperator.partial(
                task_id="crawl_category",
                python_callable=crawl_single_category,
                execution_timeout=timedelta(minutes=10),
                pool="crawl_pool",
                retries=1,
            ).expand(op_kwargs=task_prepare.output)

    # TaskGroup: Process và Save
    with TaskGroup("process_and_save") as process_group:
        task_merge_products = PythonOperator(
            task_id="merge_products",
            python_callable=merge_products,
            execution_timeout=timedelta(minutes=30),
            pool="crawl_pool",
            trigger_rule="all_done",
        )

        task_save_products = PythonOperator(
            task_id="save_products",
            python_callable=save_products,
            execution_timeout=timedelta(minutes=10),
            pool="crawl_pool",
        )
        task_merge_products >> task_save_products

    # TaskGroup: Crawl Product Details (Dynamic Task Mapping)
    with TaskGroup("crawl_product_details") as detail_group:

        def prepare_detail_kwargs(**context):
            """Helper function để prepare op_kwargs cho Dynamic Task Mapping detail"""
            import logging

            logging.getLogger("airflow.task")
            ti = context["ti"]
            products_to_crawl = ti.xcom_pull(
                task_ids="crawl_product_details.prepare_products_for_detail"
            )
            if not products_to_crawl:
                return []
            batch_size = 15  # Optimized batch size
            batches = [
                products_to_crawl[i : i + batch_size]
                for i in range(0, len(products_to_crawl), batch_size)
            ]
            return [
                {"product_batch": batch, "batch_index": idx} for idx, batch in enumerate(batches)
            ]

        task_prepare_detail = PythonOperator(
            task_id="prepare_products_for_detail",
            python_callable=prepare_products_for_detail,
            execution_timeout=timedelta(minutes=5),
        )

        task_prepare_detail_kwargs = PythonOperator(
            task_id="prepare_detail_kwargs",
            python_callable=prepare_detail_kwargs,
            execution_timeout=timedelta(minutes=1),
        )

        task_crawl_product_detail = PythonOperator.partial(
            task_id="crawl_product_detail",
            python_callable=crawl_product_batch,
            execution_timeout=timedelta(minutes=15),
            pool="crawl_pool",
            retries=1,
            retry_delay=timedelta(seconds=30),
        ).expand(op_kwargs=task_prepare_detail_kwargs.output)

        task_merge_product_details = PythonOperator(
            task_id="merge_product_details",
            python_callable=merge_product_details,
            execution_timeout=timedelta(minutes=30),
            pool="crawl_pool",
            trigger_rule="all_done",
        )

        task_save_products_with_detail = PythonOperator(
            task_id="save_products_with_detail",
            python_callable=save_products_with_detail,
            execution_timeout=timedelta(minutes=10),
            pool="crawl_pool",
        )

        (
            task_prepare_detail
            >> task_prepare_detail_kwargs
            >> task_crawl_product_detail
            >> task_merge_product_details
            >> task_save_products_with_detail
        )

    # TaskGroup: Transform and Load
    with TaskGroup("transform_and_load") as transform_load_group:
        # task_enrich_category_path removed as it was undefined and redundant

        task_transform_products = PythonOperator(
            task_id="transform_products",
            python_callable=transform_products,
            execution_timeout=timedelta(minutes=30),
            pool="crawl_pool",
        )

        task_load_products = PythonOperator(
            task_id="load_products",
            python_callable=load_products,
            execution_timeout=timedelta(minutes=30),
            pool="crawl_pool",
        )
        task_transform_products >> task_load_products

    # TaskGroup: Maintenance and Reports
    with TaskGroup("maintenance_and_reports") as maintenance_group:
        task_load_categories_db = PythonOperator(
            task_id="load_categories_to_db",
            python_callable=load_categories_to_db_wrapper,
            execution_timeout=timedelta(minutes=5),
            pool="crawl_pool",
        )

        task_reconcile = PythonOperator(
            task_id="reconcile_categories",
            python_callable=reconcile_categories_wrapper,
            execution_timeout=timedelta(minutes=10),
            pool="crawl_pool",
        )

        task_cleanup_orphans = PythonOperator(
            task_id="cleanup_orphan_categories",
            python_callable=cleanup_orphan_categories_wrapper,
            execution_timeout=timedelta(minutes=5),
            pool="crawl_pool",
        )

        task_cleanup_redundant = PythonOperator(
            task_id="cleanup_redundant_categories",
            python_callable=cleanup_redundant_categories_wrapper,
            execution_timeout=timedelta(minutes=5),
            pool="crawl_pool",
        )

        task_cleanup_history = PythonOperator(
            task_id="cleanup_old_history",
            python_callable=cleanup_old_history_wrapper,
            execution_timeout=timedelta(minutes=5),
            pool="crawl_pool",
        )

        task_validate_data = PythonOperator(
            task_id="validate_data",
            python_callable=validate_data,
            execution_timeout=timedelta(minutes=5),
            pool="crawl_pool",
        )

        task_aggregate_and_notify = PythonOperator(
            task_id="aggregate_and_notify",
            python_callable=aggregate_and_notify,
            execution_timeout=timedelta(minutes=10),
            pool="crawl_pool",
            trigger_rule="all_done",
        )

        task_cleanup_cache = PythonOperator(
            task_id="cleanup_redis_cache",
            python_callable=cleanup_redis_cache,
            execution_timeout=timedelta(minutes=5),
            pool="crawl_pool",
            trigger_rule="all_done",
        )

        task_backup_database = PythonOperator(
            task_id="backup_database",
            python_callable=backup_database,
            execution_timeout=timedelta(minutes=15),
            pool="crawl_pool",
            trigger_rule="all_done",
        )

        # Maintenance dependencies
        # Tối ưu: Cho phép chạy các tác vụ bảo trì song song sau khi dữ liệu đã ổn định
        task_load_categories_db >> task_reconcile >> [task_cleanup_orphans, task_cleanup_redundant]
        (
            [task_cleanup_orphans, task_cleanup_redundant]
            >> task_validate_data
            >> task_aggregate_and_notify
        )

        # Tối ưu: Các task maintenance không cần đợi group thông báo hoàn tất nếu không phụ thuộc dữ liệu summary
        # Chúng có thể chạy song song ngay sau khi validate_data xong
        task_validate_data >> [
            task_cleanup_cache,
            task_backup_database,
            task_cleanup_history,
        ]

    # ===== FINAL DAG DEPENDENCIES =====
    (
        pre_crawl_group
        >> crawl_group
        >> process_group
        >> detail_group
        >> transform_load_group
        >> maintenance_group
    )
