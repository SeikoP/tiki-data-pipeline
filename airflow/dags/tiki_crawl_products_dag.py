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
"""

import json
import os
import shutil
import sys
import time
import warnings
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator

from airflow import DAG

# Import Variable và TaskGroup với suppress warning
try:
    # Thử import từ airflow.sdk (Airflow 3.x)
    from airflow.sdk import TaskGroup, Variable

    _Variable = Variable  # Alias để dùng wrapper
except ImportError:
    # Fallback: dùng airflow.models và airflow.utils.task_group (Airflow 2.x)
    try:
        from airflow.utils.task_group import TaskGroup
    except ImportError:
        # Nếu không có TaskGroup, tạo dummy class
        class TaskGroup:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

    from airflow.models import Variable as _Variable


# Wrapper function để suppress deprecation warning khi gọi Variable.get()
def get_variable(key, default_var=None):
    """Wrapper cho Variable.get() để suppress deprecation warning"""
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", category=DeprecationWarning, module="airflow.models.variable"
        )
        return _Variable.get(key, default=default_var)


# Alias Variable để code cũ vẫn hoạt động, nhưng dùng wrapper
class VariableWrapper:
    """Wrapper cho Variable để suppress warnings"""

    @staticmethod
    def get(key, default_var=None):
        return get_variable(key, default_var)

    @staticmethod
    def set(key, value):
        return _Variable.set(key, value)


Variable = VariableWrapper

# Thêm đường dẫn src vào sys.path
# Lấy đường dẫn tuyệt đối của DAG file
dag_file_dir = os.path.dirname(os.path.abspath(__file__))

# Thử nhiều đường dẫn có thể
# Trong Docker, src được mount vào /opt/airflow/src
possible_paths = [
    # Từ /opt/airflow (Docker default - ưu tiên)
    "/opt/airflow/src/pipelines/crawl",
    # Từ airflow/dags/ lên 2 cấp đến root (local development)
    os.path.abspath(os.path.join(dag_file_dir, "..", "..", "src", "pipelines", "crawl")),
    # Từ airflow/dags/ lên 1 cấp (nếu airflow/ là root)
    os.path.abspath(os.path.join(dag_file_dir, "..", "src", "pipelines", "crawl")),
    # Từ workspace root (nếu mount vào /workspace)
    "/workspace/src/pipelines/crawl",
    # Từ current working directory
    os.path.join(os.getcwd(), "src", "pipelines", "crawl"),
]

# Tìm đường dẫn hợp lệ
crawl_module_path = None
crawl_products_path = None

for path in possible_paths:
    test_path = os.path.join(path, "crawl_products.py")
    if os.path.exists(test_path):
        crawl_module_path = path
        crawl_products_path = test_path
        break

if not crawl_module_path:
    # Nếu không tìm thấy, thử đường dẫn tương đối từ DAG file
    relative_path = os.path.abspath(
        os.path.join(dag_file_dir, "..", "..", "src", "pipelines", "crawl")
    )
    test_path = os.path.join(relative_path, "crawl_products.py")
    if os.path.exists(test_path):
        crawl_module_path = relative_path
        crawl_products_path = test_path

# Import module utils TRƯỚC (cần thiết cho crawl_products và crawl_products_detail)
utils_path = None
if crawl_module_path:
    utils_path = os.path.join(crawl_module_path, "utils.py")
    if not os.path.exists(utils_path):
        utils_path = None

if not utils_path:
    # Thử tìm trong các possible paths
    for path in possible_paths:
        test_path = os.path.join(path, "utils.py")
        if os.path.exists(test_path):
            utils_path = test_path
            break

if utils_path and os.path.exists(utils_path):
    try:
        import importlib.util

        spec = importlib.util.spec_from_file_location("crawl_utils", utils_path)
        if spec and spec.loader:
            utils_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(utils_module)
            # Lưu vào sys.modules để các module khác có thể import
            sys.modules["crawl_utils"] = utils_module
            # Tạo fake package structure để relative import hoạt động
            if "pipelines.crawl.utils" not in sys.modules:
                sys.modules["pipelines"] = type(sys)("pipelines")
                sys.modules["pipelines.crawl"] = type(sys)("pipelines.crawl")
                sys.modules["pipelines.crawl.utils"] = utils_module
    except Exception as e:
        # Nếu import lỗi, log và tiếp tục (sẽ fail khi chạy task)
        import warnings

        warnings.warn(f"Không thể import utils module: {e}", stacklevel=2)

# Import module crawl_products
if crawl_products_path and os.path.exists(crawl_products_path):
    try:
        # Sử dụng importlib để import trực tiếp từ file (cách đáng tin cậy nhất)
        import importlib.util

        spec = importlib.util.spec_from_file_location("crawl_products", crawl_products_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Không thể load spec từ {crawl_products_path}")
        crawl_products_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(crawl_products_module)

        # Extract các functions cần thiết
        crawl_category_products = crawl_products_module.crawl_category_products
        get_page_with_requests = crawl_products_module.get_page_with_requests
        parse_products_from_html = crawl_products_module.parse_products_from_html
        get_total_pages = crawl_products_module.get_total_pages
    except Exception as e:
        # Nếu import lỗi, log và tiếp tục (sẽ fail khi chạy task)
        import warnings

        warnings.warn(f"Không thể import crawl_products module: {e}", stacklevel=2)

        # Tạo dummy functions để tránh NameError
        error_msg = str(e)

        def crawl_category_products(*args, **kwargs):
            raise ImportError(f"Module crawl_products chưa được import: {error_msg}")

        get_page_with_requests = crawl_category_products
        parse_products_from_html = crawl_category_products
        get_total_pages = crawl_category_products
else:
    # Fallback: thử import thông thường nếu đã thêm vào sys.path
    if crawl_module_path and crawl_module_path not in sys.path:
        sys.path.insert(0, crawl_module_path)

    try:
        from crawl_products import crawl_category_products
    except ImportError as e:
        # Debug: kiểm tra xem thư mục có tồn tại không
        debug_info = {
            "dag_file_dir": dag_file_dir,
            "cwd": os.getcwd(),
            "possible_paths": possible_paths,
            "crawl_module_path": crawl_module_path,
            "crawl_products_path": crawl_products_path,
            "sys_path": sys.path[:5],  # Chỉ lấy 5 đầu tiên
        }

        # Kiểm tra xem /opt/airflow/src có tồn tại không
        if os.path.exists("/opt/airflow/src"):
            try:
                debug_info["opt_airflow_src_contents"] = os.listdir("/opt/airflow/src")
            except Exception:
                pass

        raise ImportError(
            f"Không tìm thấy module crawl_products.\n" f"Debug info: {debug_info}\n" f"Lỗi gốc: {e}"
        ) from e

# Import module crawl_products_detail
crawl_products_detail_path = None
for path in possible_paths:
    test_path = os.path.join(path, "crawl_products_detail.py")
    if os.path.exists(test_path):
        crawl_products_detail_path = test_path
        break

if crawl_products_detail_path and os.path.exists(crawl_products_detail_path):
    try:
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "crawl_products_detail", crawl_products_detail_path
        )
        if spec is None or spec.loader is None:
            raise ImportError(f"Không thể load spec từ {crawl_products_detail_path}")
        crawl_products_detail_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(crawl_products_detail_module)

        # Extract các functions cần thiết
        crawl_product_detail_with_selenium = (
            crawl_products_detail_module.crawl_product_detail_with_selenium
        )
        extract_product_detail = crawl_products_detail_module.extract_product_detail
    except Exception as e:
        # Nếu import lỗi, log và tiếp tục (sẽ fail khi chạy task)
        import warnings

        warnings.warn(f"Không thể import crawl_products_detail module: {e}", stacklevel=2)

        # Tạo dummy functions để tránh NameError
        error_msg = str(e)

        def crawl_product_detail_with_selenium(*args, **kwargs):
            raise ImportError(f"Module crawl_products_detail chưa được import: {error_msg}")

        extract_product_detail = crawl_product_detail_with_selenium
else:
    # Fallback: thử import thông thường
    try:
        from crawl_products_detail import crawl_product_detail_with_selenium, extract_product_detail
    except ImportError as e:
        raise ImportError(
            f"Không tìm thấy module crawl_products_detail.\n"
            f"Path: {crawl_products_detail_path}\n"
            f"Lỗi gốc: {e}"
        ) from e

# Cấu hình mặc định
DEFAULT_ARGS = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,  # Retry 3 lần
    "retry_delay": timedelta(minutes=2),  # Delay 2 phút giữa các retry
    "retry_exponential_backoff": True,  # Exponential backoff
    "max_retry_delay": timedelta(minutes=10),
}

# Cấu hình DAG - Có thể chuyển đổi giữa tự động và thủ công qua Variable
# Đọc schedule mode từ Airflow Variable (mặc định: 'manual' để test)
# Có thể set Variable 'TIKI_DAG_SCHEDULE_MODE' = 'scheduled' để chạy tự động
try:
    schedule_mode = Variable.get("TIKI_DAG_SCHEDULE_MODE", default_var="manual")
except Exception:
    schedule_mode = "manual"  # Mặc định là manual để test

# Xác định schedule dựa trên mode
if schedule_mode == "scheduled":
    dag_schedule = timedelta(days=1)  # Chạy tự động hàng ngày
    dag_description = (
        "Crawl sản phẩm Tiki với Dynamic Task Mapping và tối ưu hóa (Tự động chạy hàng ngày)"
    )
    dag_tags = ["tiki", "crawl", "products", "data-pipeline", "scheduled"]
else:
    dag_schedule = None  # Chỉ chạy khi trigger thủ công
    dag_description = (
        "Crawl sản phẩm Tiki với Dynamic Task Mapping và tối ưu hóa (Chạy thủ công - Test mode)"
    )
    dag_tags = ["tiki", "crawl", "products", "data-pipeline", "manual"]

DAG_CONFIG = {
    "dag_id": "tiki_crawl_products",
    "description": dag_description,
    "default_args": DEFAULT_ARGS,
    "schedule": dag_schedule,
    "start_date": datetime(2025, 11, 1),  # Ngày cố định trong quá khứ
    "catchup": False,  # Không chạy lại các task đã bỏ lỡ
    "tags": dag_tags,
    "max_active_runs": 1,  # Chỉ chạy 1 DAG instance tại một thời điểm
    "max_active_tasks": 20,  # Tối đa 20 tasks song song
}

# Thư mục dữ liệu
# Trong Docker, data được mount vào /opt/airflow/data
# Thử nhiều đường dẫn
possible_data_dirs = [
    Path("/opt/airflow/data"),  # Docker mount
    Path(__file__).parent.parent.parent / "data",  # Local development
    Path(os.getcwd()) / "data",  # Current working directory
]

DATA_DIR = None
for data_dir in possible_data_dirs:
    if data_dir.exists():
        DATA_DIR = data_dir
        break

if not DATA_DIR:
    # Fallback: dùng đường dẫn tương đối
    DATA_DIR = Path(__file__).parent.parent.parent / "data"

CATEGORIES_FILE = DATA_DIR / "raw" / "categories_recursive_optimized.json"
OUTPUT_DIR = DATA_DIR / "raw" / "products"
CACHE_DIR = OUTPUT_DIR / "cache"
DETAIL_CACHE_DIR = OUTPUT_DIR / "detail" / "cache"
OUTPUT_FILE = OUTPUT_DIR / "products.json"
OUTPUT_FILE_WITH_DETAIL = OUTPUT_DIR / "products_with_detail.json"
# Progress tracking cho multi-day crawling
PROGRESS_FILE = OUTPUT_DIR / "crawl_progress.json"

# Tạo thư mục nếu chưa có
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)
DETAIL_CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Thread-safe lock cho atomic writes
write_lock = Lock()


def get_logger(context):
    """Lấy logger từ context (Airflow 3.x compatible)"""
    try:
        # Airflow 3.x: sử dụng logging module
        import logging

        ti = context.get("task_instance")
        if ti:
            # Tạo logger với task_id và dag_id
            logger_name = f"airflow.task.{ti.dag_id}.{ti.task_id}"
            return logging.getLogger(logger_name)
        else:
            # Fallback: dùng root logger
            return logging.getLogger("airflow.task")
    except Exception:
        # Fallback: dùng root logger
        import logging

        return logging.getLogger("airflow.task")


def load_categories(**context) -> list[dict[str, Any]]:
    """
    Task 1: Load danh sách danh mục từ file

    Returns:
        List[Dict]: Danh sách danh mục
    """
    logger = get_logger(context)
    logger.info("=" * 70)
    logger.info("📖 TASK: Load Categories")
    logger.info("=" * 70)

    try:
        categories_file = str(CATEGORIES_FILE)
        logger.info(f"Đang đọc file: {categories_file}")

        if not os.path.exists(categories_file):
            raise FileNotFoundError(f"Không tìm thấy file: {categories_file}")

        with open(categories_file, encoding="utf-8") as f:
            categories = json.load(f)

        logger.info(f"✅ Đã load {len(categories)} danh mục")

        # Lọc danh mục nếu cần (ví dụ: chỉ lấy level 2-4)
        # Có thể cấu hình qua Airflow Variable
        try:
            min_level = int(Variable.get("TIKI_MIN_CATEGORY_LEVEL", default_var="2"))
            max_level = int(Variable.get("TIKI_MAX_CATEGORY_LEVEL", default_var="4"))
            categories = [
                cat for cat in categories if min_level <= cat.get("level", 0) <= max_level
            ]
            logger.info(f"✓ Sau khi lọc level {min_level}-{max_level}: {len(categories)} danh mục")
        except Exception as e:
            logger.warning(f"Không thể lọc theo level: {e}")

        # Giới hạn số danh mục nếu cần (để test)
        try:
            max_categories = int(Variable.get("TIKI_MAX_CATEGORIES", default_var="0"))
            if max_categories > 0:
                categories = categories[:max_categories]
                logger.info(f"✓ Giới hạn: {max_categories} danh mục")
        except Exception:
            pass

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

        # Crawl với timeout
        start_time = time.time()

        products = crawl_category_products(
            category_url,
            max_pages=max_pages if max_pages > 0 else None,
            use_selenium=use_selenium,
            cache_dir=str(CACHE_DIR),
            use_redis_cache=True,  # Sử dụng Redis cache
            use_rate_limiting=True,  # Sử dụng rate limiting
        )

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
        logger.error(f"⏱️  Timeout: {e}")
        # Không raise để tiếp tục với danh mục khác

    except Exception as e:
        result["error"] = str(e)
        result["status"] = "failed"
        logger.error(f"❌ Lỗi khi crawl category {category_name}: {e}", exc_info=True)
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


def atomic_write_file(filepath: str, data: Any, **context):
    """
    Ghi file an toàn (atomic write) để tránh corrupt

    Sử dụng temporary file và rename để đảm bảo atomicity
    """
    logger = get_logger(context)

    filepath = Path(filepath)
    temp_file = filepath.with_suffix(".tmp")

    try:
        # Ghi vào temporary file
        with open(temp_file, "w", encoding="utf-8") as f:
            if isinstance(data, dict):
                json.dump(data, f, ensure_ascii=False, indent=2)
            else:
                f.write(str(data))

        # Atomic rename (trên Unix) hoặc move (trên Windows)
        if os.name == "nt":  # Windows
            # Trên Windows, cần xóa file cũ trước
            if filepath.exists():
                filepath.unlink()
            shutil.move(str(temp_file), str(filepath))
        else:  # Unix/Linux
            os.rename(str(temp_file), str(filepath))

        logger.info(f"✅ Đã ghi file atomic: {filepath}")

    except Exception as e:
        # Xóa temp file nếu có lỗi
        if temp_file.exists():
            temp_file.unlink()
        logger.error(f"❌ Lỗi khi ghi file: {e}", exc_info=True)
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
            Variable.get("TIKI_PRODUCTS_PER_DAY", default_var="280")
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
        # Validate URL
        if not product_url or not product_url.startswith("http"):
            raise ValueError(f"URL không hợp lệ: {product_url}")

        # Lấy cấu hình
        rate_limit_delay = float(
            Variable.get("TIKI_DETAIL_RATE_LIMIT_DELAY", default_var="2.0")
        )  # Delay 2s cho detail
        timeout = int(
            Variable.get("TIKI_DETAIL_CRAWL_TIMEOUT", default_var="120")
        )  # 2 phút mỗi product (tăng từ 60s)

        # Rate limiting
        if rate_limit_delay > 0:
            time.sleep(rate_limit_delay)

        # Crawl với timeout
        start_time = time.time()

        # Sử dụng Selenium để crawl detail (cần thiết cho dynamic content)
        html_content = None
        try:
            # Thử crawl với retry và timeout ngắn hơn
            html_content = crawl_product_detail_with_selenium(
                product_url,
                save_html=False,
                verbose=False,  # Không verbose trong Airflow
                max_retries=2,  # Retry 2 lần
                timeout=25,  # Timeout 25s (ngắn hơn để fail nhanh hơn)
                use_redis_cache=True,  # Sử dụng Redis cache
                use_rate_limiting=True,  # Sử dụng rate limiting
            )

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
        logger.error(f"⏱️  Timeout: {e}")

    except ValueError as e:
        result["error"] = str(e)
        result["status"] = "validation_error"
        logger.error(f"❌ Validation error: {e}")

    except Exception as e:
        result["error"] = str(e)
        result["status"] = "failed"
        error_type = type(e).__name__
        logger.error(f"❌ Lỗi khi crawl detail ({error_type}): {e}", exc_info=True)
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
            # Nhưng để đơn giản, thử từ cuối về đầu với step size lớn
            test_indices = []
            if expected_crawl_count > 1000:
                # Với số lượng lớn, test một số điểm
                step = max(100, expected_crawl_count // 20)
                test_indices = list(range(0, expected_crawl_count, step))
                test_indices.append(expected_crawl_count - 1)
            else:
                # Với số lượng nhỏ, test tất cả
                test_indices = list(range(expected_crawl_count))

            for test_idx in reversed(test_indices):
                try:
                    result = ti.xcom_pull(
                        task_ids=task_id, key="return_value", map_indexes=[test_idx]
                    )
                    if result:
                        max_found_index = test_idx
                        break
                except Exception:
                    pass

            if max_found_index >= 0:
                # Tìm chính xác map_index cao nhất bằng cách tìm từ max_found_index
                # Thử từ max_found_index đến expected_crawl_count
                for idx in range(max_found_index, min(max_found_index + 200, expected_crawl_count)):
                    try:
                        result = ti.xcom_pull(
                            task_ids=task_id, key="return_value", map_indexes=[idx]
                        )
                        if result:
                            max_found_index = idx
                    except Exception:
                        break

                actual_crawl_count = max_found_index + 1
                logger.info(
                    f"✅ Phát hiện {actual_crawl_count} map_index thực tế có XCom (dự kiến: {expected_crawl_count})"
                )
            else:
                logger.warning(
                    f"⚠️  Không tìm thấy XCom nào, sử dụng expected_crawl_count: {expected_crawl_count}"
                )

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
        for start_idx in range(0, actual_crawl_count, batch_size):
            end_idx = min(start_idx + batch_size, actual_crawl_count)
            batch_map_indexes = list(range(start_idx, end_idx))

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

                if (start_idx // batch_size + 1) % 10 == 0:
                    logger.info(f"Đã lấy {len(all_detail_results)}/{actual_crawl_count} results...")
            except Exception as e:
                logger.warning(f"Lỗi khi lấy batch {start_idx}-{end_idx}: {e}")
                # Thử lấy từng map_index riêng lẻ
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
                        logger.debug(f"Không lấy được map_index {map_index}: {e2}")
                        pass

        logger.info(
            f"Lấy được {len(all_detail_results)} detail results (mong đợi {actual_crawl_count})"
        )

        # Nếu không lấy đủ, thử lấy từng map_index một (chỉ trong phạm vi actual_crawl_count)
        if len(all_detail_results) < actual_crawl_count * 0.8:  # Nếu thiếu hơn 20%
            logger.warning(
                f"Chỉ lấy được {len(all_detail_results)}/{actual_crawl_count} results, thử lấy từng map_index..."
            )
            all_detail_results = []  # Reset và lấy lại
            for map_index in range(actual_crawl_count):  # CHỈ lấy từ 0 đến actual_crawl_count - 1
                try:
                    result = ti.xcom_pull(
                        task_ids=task_id, key="return_value", map_indexes=[map_index]
                    )
                    if result:
                        if isinstance(result, list):
                            all_detail_results.extend([r for r in result if r])
                        elif isinstance(result, dict):
                            # Nếu là dict, có thể là dict chứa result
                            all_detail_results.append(result)
                        else:
                            all_detail_results.append(result)

                    if (map_index + 1) % 500 == 0:
                        logger.info(
                            f"Đã lấy {len(all_detail_results)}/{actual_crawl_count} results (từng map_index)..."
                        )
                except Exception as e:
                    # Bỏ qua nếu không lấy được (có thể task chưa chạy xong hoặc failed)
                    logger.debug(f"Không lấy được map_index {map_index}: {e}")
                    pass

            logger.info(f"Sau khi lấy từng map_index: {len(all_detail_results)} detail results")

        # Tạo dict để lookup nhanh
        detail_dict = {}
        stats = {
            "total_products": len(products),
            "with_detail": 0,
            "cached": 0,
            "failed": 0,
            "timeout": 0,
        }

        for detail_result in all_detail_results:
            if detail_result and isinstance(detail_result, dict):
                product_id = detail_result.get("product_id")
                if product_id:
                    detail_dict[product_id] = detail_result
                    status = detail_result.get("status", "failed")
                    if status == "success":
                        stats["with_detail"] += 1
                    elif status == "cached":
                        stats["cached"] += 1
                    elif status == "timeout":
                        stats["timeout"] += 1
                    else:
                        stats["failed"] += 1

        # Merge detail vào products
        products_with_detail = []
        for product in products:
            product_id = product.get("product_id")
            detail_result = detail_dict.get(product_id)

            if detail_result and detail_result.get("detail"):
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
                product_with_detail["detail_status"] = detail_result.get("status")

                products_with_detail.append(product_with_detail)
            else:
                # Giữ nguyên product nếu không có detail
                # Đảm bảo sales_count có trong product (kể cả None)
                if "sales_count" not in product:
                    product["sales_count"] = None
                products_with_detail.append(product)

        logger.info("=" * 70)
        logger.info("📊 THỐNG KÊ MERGE DETAIL")
        logger.info("=" * 70)
        logger.info(f"📦 Tổng products: {stats['total_products']}")
        logger.info(f"✅ Có detail: {stats['with_detail']}")
        logger.info(f"📦 Cache: {stats['cached']}")
        logger.info(f"❌ Failed: {stats['failed']}")
        logger.info(f"⏱️  Timeout: {stats['timeout']}")
        logger.info("=" * 70)

        result = {
            "products": products_with_detail,
            "stats": stats,
            "merged_at": datetime.now().isoformat(),
        }

        return result

    except Exception as e:
        logger.error(f"❌ Lỗi khi merge details: {e}", exc_info=True)
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

        logger.info(f"Đang lưu {len(products)} products với detail...")

        # Chuẩn bị dữ liệu
        output_data = {
            "total_products": len(products),
            "stats": stats,
            "crawled_at": datetime.now().isoformat(),
            "note": "Crawl từ Airflow DAG với product details",
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


def validate_data(**context) -> dict[str, Any]:
    """
    Task 5: Validate dữ liệu đã crawl

    Returns:
        Dict: Kết quả validation
    """
    logger = get_logger(context)
    logger.info("=" * 70)
    logger.info("✅ TASK: Validate Data")
    logger.info("=" * 70)

    try:
        ti = context["ti"]
        output_file = None

        # Cách 1: Lấy từ task_id với TaskGroup prefix
        try:
            output_file = ti.xcom_pull(task_ids="process_and_save.save_products")
            logger.info(f"Lấy output_file từ 'process_and_save.save_products': {output_file}")
        except Exception as e:
            logger.warning(f"Không lấy được từ 'process_and_save.save_products': {e}")

        # Cách 2: Thử không có prefix
        if not output_file:
            try:
                output_file = ti.xcom_pull(task_ids="save_products")
                logger.info(f"Lấy output_file từ 'save_products': {output_file}")
            except Exception as e:
                logger.warning(f"Không lấy được từ 'save_products': {e}")

        if not output_file or not os.path.exists(output_file):
            raise FileNotFoundError(f"Không tìm thấy file output: {output_file}")

        logger.info(f"Đang validate file: {output_file}")

        with open(output_file, encoding="utf-8") as f:
            data = json.load(f)

        products = data.get("products", [])

        # Validation
        validation_result = {
            "file_exists": True,
            "total_products": len(products),
            "valid_products": 0,
            "invalid_products": 0,
            "errors": [],
        }

        required_fields = ["product_id", "name", "url"]

        for i, product in enumerate(products):
            is_valid = True
            missing_fields = []

            for field in required_fields:
                if not product.get(field):
                    is_valid = False
                    missing_fields.append(field)

            if is_valid:
                validation_result["valid_products"] += 1
            else:
                validation_result["invalid_products"] += 1
                validation_result["errors"].append(
                    {
                        "index": i,
                        "product_id": product.get("product_id"),
                        "missing_fields": missing_fields,
                    }
                )

        logger.info("=" * 70)
        logger.info("📊 VALIDATION RESULTS")
        logger.info("=" * 70)
        logger.info(f"✅ Valid products: {validation_result['valid_products']}")
        logger.info(f"❌ Invalid products: {validation_result['invalid_products']}")
        logger.info("=" * 70)

        if validation_result["invalid_products"] > 0:
            logger.warning(f"Có {validation_result['invalid_products']} sản phẩm không hợp lệ")
            # Không fail task, chỉ warning

        return validation_result

    except Exception as e:
        logger.error(f"❌ Lỗi khi validate data: {e}", exc_info=True)
        raise


# Tạo DAG duy nhất với schedule có thể config qua Variable
with DAG(**DAG_CONFIG) as dag:

    # TaskGroup: Load và Prepare
    with TaskGroup("load_and_prepare", tooltip="Load categories và chuẩn bị") as load_group:
        task_load_categories = PythonOperator(
            task_id="load_categories",
            python_callable=load_categories,
            execution_timeout=timedelta(minutes=5),  # Timeout 5 phút
            pool="default_pool",
        )

    # TaskGroup: Crawl Categories (Dynamic Task Mapping)
    with TaskGroup("crawl_categories", tooltip="Crawl sản phẩm từ các danh mục") as crawl_group:
        # Sử dụng expand để Dynamic Task Mapping
        # Cần một task helper để lấy categories và tạo list op_kwargs
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

        task_prepare_crawl = PythonOperator(
            task_id="prepare_crawl_kwargs",
            python_callable=prepare_crawl_kwargs,
            execution_timeout=timedelta(minutes=1),
        )

        # Dynamic Task Mapping với expand
        # Sử dụng expand với op_kwargs để tránh lỗi với PythonOperator constructor
        task_crawl_category = PythonOperator.partial(
            task_id="crawl_category",
            python_callable=crawl_single_category,
            execution_timeout=timedelta(minutes=10),  # Timeout 10 phút mỗi category
            pool="default_pool",  # Có thể tạo pool riêng nếu cần
            retries=1,  # Retry 1 lần (tổng 2 lần thử: 1 lần đầu + 1 retry)
        ).expand(op_kwargs=task_prepare_crawl.output)

    # TaskGroup: Process và Save
    with TaskGroup("process_and_save", tooltip="Merge và lưu sản phẩm") as process_group:
        task_merge_products = PythonOperator(
            task_id="merge_products",
            python_callable=merge_products,
            execution_timeout=timedelta(minutes=30),  # Timeout 30 phút
            pool="default_pool",
            trigger_rule="all_done",  # QUAN TRỌNG: Chạy khi tất cả upstream tasks done (success hoặc failed)
        )

        task_save_products = PythonOperator(
            task_id="save_products",
            python_callable=save_products,
            execution_timeout=timedelta(minutes=10),  # Timeout 10 phút
            pool="default_pool",
        )

    # TaskGroup: Crawl Product Details (Dynamic Task Mapping)
    with TaskGroup("crawl_product_details", tooltip="Crawl chi tiết sản phẩm") as detail_group:

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

        # Dynamic Task Mapping cho crawl detail
        task_crawl_product_detail = PythonOperator.partial(
            task_id="crawl_product_detail",
            python_callable=crawl_single_product_detail,
            execution_timeout=timedelta(
                minutes=7
            ),  # Tăng timeout lên 7 phút để đủ thời gian cho Selenium driver khởi động
            pool="default_pool",
            retries=2,  # Tăng retry lên 2 lần để giảm failed tasks
            retry_delay=timedelta(seconds=30),  # Delay 30s giữa các retry
        ).expand(op_kwargs=task_prepare_detail_kwargs.output)

        task_merge_product_details = PythonOperator(
            task_id="merge_product_details",
            python_callable=merge_product_details,
            execution_timeout=timedelta(minutes=30),  # Timeout 30 phút
            pool="default_pool",
            trigger_rule="all_done",  # Chạy khi tất cả upstream tasks done
        )

        task_save_products_with_detail = PythonOperator(
            task_id="save_products_with_detail",
            python_callable=save_products_with_detail,
            execution_timeout=timedelta(minutes=10),  # Timeout 10 phút
            pool="default_pool",
        )

        # Dependencies trong detail group
        (
            task_prepare_detail
            >> task_prepare_detail_kwargs
            >> task_crawl_product_detail
            >> task_merge_product_details
            >> task_save_products_with_detail
        )

    # TaskGroup: Validate
    with TaskGroup("validate", tooltip="Validate dữ liệu") as validate_group:
        task_validate_data = PythonOperator(
            task_id="validate_data",
            python_callable=validate_data,
            execution_timeout=timedelta(minutes=5),  # Timeout 5 phút
            pool="default_pool",
        )

    # Định nghĩa dependencies
    # Flow: Load -> Crawl Categories -> Merge & Save -> Prepare Detail -> Crawl Detail -> Merge & Save Detail -> Validate

    # Dependencies giữa các TaskGroup
    # Load categories trước, sau đó prepare crawl kwargs
    task_load_categories >> task_prepare_crawl

    # Prepare crawl kwargs -> crawl category (dynamic mapping)
    task_prepare_crawl >> task_crawl_category

    # Crawl category -> merge products (merge chạy khi tất cả crawl tasks done)
    task_crawl_category >> task_merge_products

    # Merge -> save products
    task_merge_products >> task_save_products

    # Save products -> prepare detail -> crawl detail -> merge detail -> save detail -> validate
    task_save_products >> task_prepare_detail
    # Dependencies trong detail group đã được định nghĩa ở dòng 1800
    # Chỉ cần thêm dependency từ save_products -> prepare_detail (đã có ở trên)
    # và từ save_products_with_detail -> validate
    task_save_products_with_detail >> task_validate_data
