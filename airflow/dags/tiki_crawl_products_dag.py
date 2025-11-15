"""
DAG Airflow để crawl sản phẩm Tiki với tối ưu hóa cho dữ liệu lớn

Tính năng:
- Dynamic Task Mapping: crawl song song nhiều danh mục
- Chia nhỏ tasks: mỗi task một chức năng riêng
- XCom: chia sẻ dữ liệu giữa các tasks
- Retry: tự động retry khi lỗi
- Timeout: giới hạn thời gian thực thi
- Logging: ghi log rõ ràng cho từng task
- Error handling: xử lý lỗi và tiếp tục với danh mục khác
- Atomic writes: ghi file an toàn, tránh corrupt
- TaskGroup: nhóm các tasks liên quan
- Tối ưu: batch processing, rate limiting, caching
- Asset-aware scheduling: hỗ trợ data lineage tracking

Dependencies được quản lý bằng >> operator giữa các tasks.
"""

import os
from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Import TaskGroup
try:
    from airflow.sdk import TaskGroup
except ImportError:
    try:
        from airflow.utils.task_group import TaskGroup
    except ImportError:
        class TaskGroup:
            def __init__(self, *args, **kwargs):
                pass
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass

# Import config và helpers
from dag_helpers.config import DAG_CONFIG, set_dag_file_dir
from dag_helpers.imports import import_all_modules
from dag_helpers.resilience import setup_resilience_patterns
from dag_helpers.shared_state import set_shared_state

# Set DAG file dir for config
dag_file_dir = os.path.dirname(os.path.abspath(__file__))
set_dag_file_dir(dag_file_dir)

# Import các modules và setup shared state
imported_modules = import_all_modules(dag_file_dir)

# Setup resilience patterns
resilience_patterns = setup_resilience_patterns(dag_file_dir)

# Setup shared state với các dependencies
set_shared_state(
    tiki_circuit_breaker_val=resilience_patterns["tiki_circuit_breaker"],
    tiki_dlq_val=resilience_patterns["tiki_dlq"],
    tiki_degradation_val=resilience_patterns["tiki_degradation"],
    crawl_category_products_val=imported_modules.get("crawl_category_products"),
    get_page_with_requests_val=imported_modules.get("get_page_with_requests"),
    parse_products_from_html_val=imported_modules.get("parse_products_from_html"),
    get_total_pages_val=imported_modules.get("get_total_pages"),
    crawl_product_detail_with_selenium_val=imported_modules.get("crawl_product_detail_with_selenium"),
    extract_product_detail_val=imported_modules.get("extract_product_detail"),
    CircuitBreakerOpenError_val=resilience_patterns.get("CircuitBreakerOpenError"),
    classify_error_val=resilience_patterns.get("classify_error"),
    DataAggregator_val=imported_modules.get("DataAggregator"),
    AISummarizer_val=imported_modules.get("AISummarizer"),
    DiscordNotifier_val=imported_modules.get("DiscordNotifier"),
)

# Import task functions
from dag_tasks.extract import extract_and_load_categories_to_db, load_categories
from dag_tasks.crawl import crawl_single_category, merge_products, save_products, prepare_crawl_kwargs
from dag_tasks.detail import (
    prepare_products_for_detail,
    crawl_single_product_detail,
    merge_product_details,
    save_products_with_detail,
    prepare_detail_kwargs,
)
from dag_tasks.transform import transform_products, load_products
from dag_tasks.validate import validate_data, aggregate_and_notify

# Import Asset helpers
from dag_assets import get_outlets_for_task

# Tạo DAG
with DAG(**DAG_CONFIG) as dag:

    # TaskGroup: Load và Prepare
    with TaskGroup("load_and_prepare") as load_group:
        task_extract_and_load_categories = PythonOperator(
            task_id="extract_and_load_categories_to_db",
            python_callable=extract_and_load_categories_to_db,
            execution_timeout=timedelta(minutes=10),
            pool="default_pool",
        )

        task_load_categories = PythonOperator(
            task_id="load_categories",
            python_callable=load_categories,
            execution_timeout=timedelta(minutes=5),
            pool="default_pool",
        )

        task_extract_and_load_categories >> task_load_categories

    # TaskGroup: Crawl Categories (Dynamic Task Mapping)
    with TaskGroup("crawl_categories") as crawl_group:
        task_prepare_crawl = PythonOperator(
            task_id="prepare_crawl_kwargs",
            python_callable=prepare_crawl_kwargs,
            execution_timeout=timedelta(minutes=1),
        )

        task_crawl_category = PythonOperator.partial(
            task_id="crawl_category",
            python_callable=crawl_single_category,
            execution_timeout=timedelta(minutes=10),
            pool="default_pool",
            retries=1,
        ).expand(op_kwargs=task_prepare_crawl.output)

    # TaskGroup: Process và Save
    with TaskGroup("process_and_save") as process_group:
        task_merge_products = PythonOperator(
            task_id="merge_products",
            python_callable=merge_products,
            execution_timeout=timedelta(minutes=30),
            pool="default_pool",
            trigger_rule="all_done",
        )

        task_save_products = PythonOperator(
            task_id="save_products",
            python_callable=save_products,
            execution_timeout=timedelta(minutes=10),
            pool="default_pool",
            outlets=get_outlets_for_task("save_products"),
        )

        task_merge_products >> task_save_products

    # TaskGroup: Crawl Product Details (Dynamic Task Mapping)
    with TaskGroup("crawl_product_details") as detail_group:
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
            python_callable=crawl_single_product_detail,
            execution_timeout=timedelta(minutes=15),
            pool="default_pool",
            retries=3,
            retry_delay=timedelta(minutes=1),
        ).expand(op_kwargs=task_prepare_detail_kwargs.output)

        task_merge_product_details = PythonOperator(
            task_id="merge_product_details",
            python_callable=merge_product_details,
            execution_timeout=timedelta(minutes=60),
            pool="default_pool",
            trigger_rule="all_done",
        )

        task_save_products_with_detail = PythonOperator(
            task_id="save_products_with_detail",
            python_callable=save_products_with_detail,
            execution_timeout=timedelta(minutes=10),
            pool="default_pool",
            outlets=get_outlets_for_task("save_products_with_detail"),
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
        task_transform_products = PythonOperator(
            task_id="transform_products",
            python_callable=transform_products,
            execution_timeout=timedelta(minutes=30),
            pool="default_pool",
            outlets=get_outlets_for_task("transform_products"),
        )

        task_load_products = PythonOperator(
            task_id="load_products",
            python_callable=load_products,
            execution_timeout=timedelta(minutes=30),
            pool="default_pool",
            outlets=get_outlets_for_task("load_products"),
        )

        task_transform_products >> task_load_products

    # TaskGroup: Validate
    with TaskGroup("validate") as validate_group:
        task_validate_data = PythonOperator(
            task_id="validate_data",
            python_callable=validate_data,
            execution_timeout=timedelta(minutes=5),
            pool="default_pool",
        )

    # TaskGroup: Aggregate and Notify
    with TaskGroup("aggregate_and_notify") as aggregate_group:
        task_aggregate_and_notify = PythonOperator(
            task_id="aggregate_and_notify",
            python_callable=aggregate_and_notify,
            execution_timeout=timedelta(minutes=10),
            pool="default_pool",
            trigger_rule="all_done",
        )

    # Định nghĩa dependencies
    task_load_categories >> task_prepare_crawl
    task_prepare_crawl >> task_crawl_category
    task_crawl_category >> task_merge_products
    task_merge_products >> task_save_products
    task_save_products >> task_prepare_detail
    (
        task_save_products_with_detail
        >> task_transform_products
        >> task_load_products
        >> task_validate_data
        >> task_aggregate_and_notify
    )

