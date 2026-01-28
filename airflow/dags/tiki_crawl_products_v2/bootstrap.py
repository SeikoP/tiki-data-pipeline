"""DAG Airflow để crawl sản phẩm Tiki với tối ưu hóa cho dữ liệu lớn.

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

Dependencies được quản lý bằng >> operator giữa các tasks.
"""

import json
import logging
import os
import re  # noqa: F401
import shutil  # noqa: F401
import sys
import time  # noqa: F401
import warnings
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path
from threading import Lock
from typing import Any

try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import psycopg2
except ImportError:
    psycopg2 = None

try:
    import aiohttp
except ImportError:
    aiohttp = None

try:
    from airflow import DAG
    from airflow.models import Pool

    # Airflow 3.0+ compatible imports
    try:
        from airflow.sdk import TaskGroup
    except ImportError:
        from airflow.utils.task_group import TaskGroup

    try:
        from airflow.providers.standard.operators.python import PythonOperator
    except ImportError:
        from airflow.operators.python import PythonOperator
except ImportError:
    # Mock for local development without airflow installed
    class DAG:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    class Pool:
        def __init__(self, *args, **kwargs):
            pass

    class PythonOperator:
        def __init__(self, *args, **kwargs):
            self.output = []

        def expand(self, **kwargs):
            return self

        @classmethod
        def partial(cls, *args, **kwargs):
            return cls()

        def __rshift__(self, other):
            return other

        def __lshift__(self, other):
            return self

        def __rrshift__(self, other):
            return self

        def __rlshift__(self, other):
            return self

    class TaskGroup:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def __rshift__(self, other):
            return other

        def __lshift__(self, other):
            return self

        def __rrshift__(self, other):
            return self

        def __rlshift__(self, other):
            return self


try:
    from airflow.exceptions import AirflowSkipException
except ImportError:

    class AirflowSkipException(Exception):
        pass


def load_env_file():
    """
    Dummy load_env_file for bootstrap.
    """


# Setup imports path
src_path = Path("/opt/airflow/src")
if src_path.exists() and str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

# Fallback for local development
if not src_path.exists():
    local_src = Path(__file__).resolve().parent.parent.parent.parent / "src"
    if local_src.exists() and str(local_src) not in sys.path:
        sys.path.insert(0, str(local_src))

# Import Custom Utilities
try:
    from common.airflow_utils import (
        TaskGroup,
        get_int_variable,
        get_variable,
        load_env_file,
        safe_import_attr,
    )
    from pipelines.crawl.hierarchy import get_hierarchy_map
except ImportError as e:
    logging.warning(f"⚠️ Could not import common utilities: {e}")

    # Minimal fallback to allow DAG parse (though it will likely fail run)
    def get_variable(key, default=None):
        return os.getenv(key, default)

    def get_int_variable(key, default=0):
        return int(os.getenv(key, default))

    def load_env_file():
        pass

    class TaskGroup:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def __rshift__(self, other):
            return other

        def __lshift__(self, other):
            return self

        def __rrshift__(self, other):
            return self

        def __rlshift__(self, other):
            return self

    def safe_import_attr(*args, **kwargs):
        return None

    def get_hierarchy_map(force_reload=False):
        return {}


# NOTE: Avoid heavy work at DAG parse time.
# We defer loading .env and other expensive initialization until first use in tasks.
_ENV_LOADED = False


def ensure_env_loaded() -> None:
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    try:
        load_env_file()
    finally:
        _ENV_LOADED = True


# Try to import redis_cache for caching
redis_cache = None
try:
    # try:
    #     from pipelines.crawl.storage.redis_cache import get_redis_cache  # type: ignore
    #     # redis_cache = get_redis_cache("redis://redis:6379/1")  # type: ignore
    # except Exception as import_err:
    #     pass
    pass
except Exception as e:
    warnings.warn(f"Redis cache initialization skipped: {e}", stacklevel=2)
    redis_cache = None


# Đường dẫn cơ sở của file DAG
dag_file_dir = os.path.dirname(__file__)

# Các đường dẫn có thể chứa module crawl
possible_paths = [
    os.path.abspath(os.path.join(dag_file_dir, "..", "..", "..", "src", "pipelines", "crawl")),
    os.path.abspath(os.path.join(dag_file_dir, "..", "..", "src", "pipelines", "crawl")),
    "/opt/airflow/src/pipelines/crawl",
]


def _fix_sys_path_for_pipelines_import(logger=None):
    """Sửa sys.path và sys.modules để đảm bảo import được pipelines.

    Xử lý trường hợp pipelines bị nhận nhầm là namespace package hoặc module không tồn tại.
    """
    if logger:
        logger.info("🔧 Fixing sys.path for pipelines import...")

    # 1. Thêm src vào sys.path nếu chưa có
    src_path_str = str(src_path)
    if src_path_str not in sys.path:
        sys.path.insert(0, src_path_str)
        if logger:
            logger.info(f"   Added {src_path_str} to sys.path")

    # 2. Xóa các entries giả trong sys.modules (quan trọng cho reload)
    modules_to_remove = [
        m for m in sys.modules.keys() if m.startswith("pipelines") or m.startswith("common")
    ]
    # Chỉ xóa nếu thực sự cần thiết (optional strategy) - ở đây ta giữ nguyên logic cũ là xóa
    # However, aggressive cleaning might break things if imports are partial.
    # Let's trust the adding of src_path to sys.path is enough for now,
    # OR replicate the full logic if we are sure.

    # Full logic from previous viewing:
    # Xóa các fake modules khỏi sys.modules
    count = 0
    for module_name in modules_to_remove:
        # Check if attribute is None (namespace package without init)
        if sys.modules[module_name] is None:
            del sys.modules[module_name]
            count += 1

    if logger and count > 0:
        logger.info(f"   Cleaned {count} None modules starting with pipelines/common")


def get_logger(context=None):
    """
    Get Airflow task logger or standard logger.
    """
    # In Airflow 3.0+, context["ti"] is a RuntimeTaskInstance which may not have .log
    # Standard way to get the task logger across versions:
    return logging.getLogger("airflow.task")


def atomic_write_file(filepath: str, data: Any, **context):
    """Ghi file an toàn (atomic write) để tránh corrupt.

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

        # Rename atomic
        # os.replace is atomic on POSIX and Windows (Python 3.3+)
        os.replace(temp_file, filepath)
        # logger.info(f"✅ Đã ghi file: {filepath}")
    except Exception as e:
        logger.error(f"❌ Lỗi khi ghi file {filepath}: {e}")
        if temp_file.exists():
            try:
                os.remove(temp_file)
            except OSError:
                pass
        raise


# Import pipelines modules
try:
    from pipelines.transform.transformer import DataTransformer
except ImportError:
    DataTransformer = None

try:
    from pipelines.load.optimized_loader import OptimizedDataLoader
except ImportError:
    OptimizedDataLoader = None

try:
    from pipelines.transform.merger import combine_products
except ImportError:
    combine_products = None


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
        crawl_product_detail_async = crawl_products_detail_module.crawl_product_detail_async
        # Optional pooled-driver variant (present in optimized module)
        crawl_product_detail_with_driver = getattr(
            crawl_products_detail_module, "crawl_product_detail_with_driver", None
        )
    except Exception as e:
        # Nếu import lỗi, log và tiếp tục (sẽ fail khi chạy task)
        import warnings

        warnings.warn(f"Không thể import crawl_products_detail module: {e}", stacklevel=2)

        # Tạo dummy functions để tránh NameError
        error_msg = str(e)

        def crawl_product_detail_with_selenium(*args, **kwargs):
            raise ImportError(f"Module crawl_products_detail chưa được import: {error_msg}")

        def crawl_product_detail_with_driver(*args, **kwargs):
            raise ImportError(f"Module crawl_products_detail chưa được import: {error_msg}")

        extract_product_detail = crawl_product_detail_with_selenium

        async def crawl_product_detail_async(*args, **kwargs):
            raise ImportError(f"Module crawl_products_detail chưa được import: {error_msg}")

        # Fallback SeleniumDriverPool
        SeleniumDriverPool = None

else:
    # Fallback: thử import thông thường
    try:
        from crawl_products_detail import (
            crawl_product_detail_with_selenium,
        )

        SeleniumDriverPool = None  # Không có trong crawl_products_detail, sẽ import từ utils
    except ImportError as e:
        raise ImportError(
            f"Không tìm thấy module crawl_products_detail.\n"
            f"Path: {crawl_products_detail_path}\n"
            f"Lỗi gốc: {e}"
        ) from e

# Import crawl_category_products từ crawl_products
crawl_products_path = None
for path in possible_paths:
    test_path = os.path.join(path, "crawl_products.py")
    if os.path.exists(test_path):
        crawl_products_path = test_path
        break

if crawl_products_path and os.path.exists(crawl_products_path):
    try:
        import importlib.util

        spec = importlib.util.spec_from_file_location("crawl_products", crawl_products_path)
        if spec and spec.loader:
            crawl_products_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(crawl_products_module)
            crawl_category_products = getattr(
                crawl_products_module, "crawl_category_products", None
            )
        else:
            crawl_category_products = None
    except Exception as e:
        err_msg = str(e)
        warnings.warn(f"Không thể import crawl_products module: {err_msg}", stacklevel=2)

        def crawl_category_products(*args, **kwargs):
            raise ImportError(f"Module crawl_products chưa được import: {err_msg}")
else:
    try:
        from pipelines.crawl.crawl_products import crawl_category_products
    except ImportError:

        def crawl_category_products(*args, **kwargs):
            raise ImportError("Không tìm thấy module crawl_products")


# Import SeleniumDriverPool từ utils ở module level
try:
    # utils là file (.py), không phải package, nên import trực tiếp
    import importlib.util
    import sys

    src_path = Path("/opt/airflow/src")
    if src_path.exists() and str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    # Thử import từ pipelines.crawl.utils
    try:
        from pipelines.crawl.utils import SeleniumDriverPool
    except Exception:
        # Fallback: direct import từ file
        utils_path = src_path / "pipelines" / "crawl" / "utils.py"
        if utils_path.exists():
            spec = importlib.util.spec_from_file_location("crawl_utils", str(utils_path))
            if spec and spec.loader:
                crawl_utils_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(crawl_utils_module)
                SeleniumDriverPool = getattr(crawl_utils_module, "SeleniumDriverPool", None)
            else:
                SeleniumDriverPool = None
        else:
            SeleniumDriverPool = None
except Exception:
    SeleniumDriverPool = None  # Fallback, sẽ import lại trong task nếu cần

# Import module crawl_categories_batch (for category batch processing)
crawl_categories_batch_path = None
for path in possible_paths:
    test_path = os.path.join(path, "crawl_categories_batch.py")
    if os.path.exists(test_path):
        crawl_categories_batch_path = test_path
        break

if crawl_categories_batch_path and os.path.exists(crawl_categories_batch_path):
    try:
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "crawl_categories_batch", crawl_categories_batch_path
        )
        if spec is None or spec.loader is None:
            raise ImportError(f"Không thể load spec từ {crawl_categories_batch_path}")
        crawl_categories_batch_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(crawl_categories_batch_module)

        # Extract các functions cần thiết
        crawl_category_batch = crawl_categories_batch_module.crawl_category_batch
    except Exception as e:
        # Nếu import lỗi, log và tạo fallback (sẽ sử dụng crawl_single_category)
        import warnings

        warnings.warn(f"Không thể import crawl_categories_batch module: {e}", stacklevel=2)

        # Tạo dummy function để tránh NameError
        crawl_category_batch = None
else:
    # Module chưa tồn tại, sẽ fallback về crawl_single_category
    crawl_category_batch = None

# Import resilience patterns
# Import trực tiếp từng module con để tránh vấn đề relative imports
resilience_module_path = None
for path in possible_paths:
    test_path = os.path.join(path, "resilience", "__init__.py")
    if os.path.exists(test_path):
        resilience_module_path = os.path.join(path, "resilience")
        break

if resilience_module_path and os.path.exists(resilience_module_path):
    try:
        import importlib.util
        import sys

        # Thêm parent path (pipelines/crawl) vào sys.path
        parent_path = os.path.dirname(resilience_module_path)  # .../crawl
        if parent_path not in sys.path:
            sys.path.insert(0, parent_path)

        # Thêm grandparent path (pipelines) vào sys.path
        grandparent_path = os.path.dirname(parent_path)  # .../pipelines
        if grandparent_path not in sys.path:
            sys.path.insert(0, grandparent_path)

        # Import trực tiếp từng module con với tên module đầy đủ
        # Điều này đảm bảo các module có thể import lẫn nhau nếu cần

        # Tạo package structure trong sys.modules
        import types

        if "pipelines" not in sys.modules:
            sys.modules["pipelines"] = types.ModuleType("pipelines")
        if "pipelines.crawl" not in sys.modules:
            sys.modules["pipelines.crawl"] = types.ModuleType("pipelines.crawl")
        if "pipelines.crawl.resilience" not in sys.modules:
            sys.modules["pipelines.crawl.resilience"] = types.ModuleType(
                "pipelines.crawl.resilience"
            )

        # Đảm bảo utils module đã được import (cần thiết cho dead_letter_queue)
        # utils module đã được import ở trên (dòng 137-156)
        # Nếu chưa có, tạo fake module
        if "pipelines.crawl.utils" not in sys.modules and "crawl_utils" in sys.modules:
            sys.modules["pipelines.crawl.utils"] = sys.modules["crawl_utils"]

        # 1. Import exceptions trước (không có dependency)
        exceptions_path = os.path.join(resilience_module_path, "exceptions.py")
        if os.path.exists(exceptions_path):
            spec = importlib.util.spec_from_file_location(
                "pipelines.crawl.resilience.exceptions", exceptions_path
            )
            if spec and spec.loader:
                exceptions_module = importlib.util.module_from_spec(spec)
                sys.modules["pipelines.crawl.resilience.exceptions"] = exceptions_module
                spec.loader.exec_module(exceptions_module)
                CrawlError = exceptions_module.CrawlError
                classify_error = exceptions_module.classify_error
            else:
                raise ImportError(f"Không thể load exceptions module từ {exceptions_path}")
        else:
            raise ImportError(f"Không tìm thấy exceptions.py tại {exceptions_path}")

        # 2. Import circuit_breaker (không có dependency)
        circuit_breaker_path = os.path.join(resilience_module_path, "circuit_breaker.py")
        if os.path.exists(circuit_breaker_path):
            spec = importlib.util.spec_from_file_location(
                "pipelines.crawl.resilience.circuit_breaker", circuit_breaker_path
            )
            if spec and spec.loader:
                circuit_breaker_module = importlib.util.module_from_spec(spec)
                sys.modules["pipelines.crawl.resilience.circuit_breaker"] = circuit_breaker_module
                spec.loader.exec_module(circuit_breaker_module)
                CircuitBreaker = circuit_breaker_module.CircuitBreaker
                CircuitBreakerOpenError = circuit_breaker_module.CircuitBreakerOpenError
            else:
                raise ImportError("Không thể load circuit_breaker module")
        else:
            raise ImportError("Không tìm thấy circuit_breaker.py")

        # 3. Import dead_letter_queue (có thể import từ utils)
        dlq_path = os.path.join(resilience_module_path, "dead_letter_queue.py")
        if os.path.exists(dlq_path):
            spec = importlib.util.spec_from_file_location(
                "pipelines.crawl.resilience.dead_letter_queue", dlq_path
            )
            if spec and spec.loader:
                dlq_module = importlib.util.module_from_spec(spec)
                sys.modules["pipelines.crawl.resilience.dead_letter_queue"] = dlq_module
                spec.loader.exec_module(dlq_module)
                DeadLetterQueue = dlq_module.DeadLetterQueue
                get_dlq = dlq_module.get_dlq
            else:
                raise ImportError("Không thể load dead_letter_queue module")
        else:
            raise ImportError("Không tìm thấy dead_letter_queue.py")

        # 4. Import graceful_degradation (không có dependency)
        degradation_path = os.path.join(resilience_module_path, "graceful_degradation.py")
        if os.path.exists(degradation_path):
            spec = importlib.util.spec_from_file_location(
                "pipelines.crawl.resilience.graceful_degradation", degradation_path
            )
            if spec and spec.loader:
                degradation_module = importlib.util.module_from_spec(spec)
                sys.modules["pipelines.crawl.resilience.graceful_degradation"] = degradation_module
                spec.loader.exec_module(degradation_module)
                GracefulDegradation = degradation_module.GracefulDegradation
                DegradationLevel = degradation_module.DegradationLevel
                get_service_health = degradation_module.get_service_health
            else:
                raise ImportError("Không thể load graceful_degradation module")
        else:
            raise ImportError("Không tìm thấy graceful_degradation.py")

    except Exception as e:
        # Nếu import lỗi, tạo dummy classes để tránh NameError
        import warnings

        warnings.warn(f"Không thể import resilience module: {e}", stacklevel=2)

        # Tạo dummy classes
        class CircuitBreaker:
            def __init__(self, *args, **kwargs):
                pass

            def call(self, func, *args, **kwargs):
                return func(*args, **kwargs)

        class CircuitBreakerOpenError(Exception):
            pass

        class DeadLetterQueue:
            def add(self, *args, **kwargs):
                pass

        def get_dlq(*args, **kwargs):
            return DeadLetterQueue()

        class GracefulDegradation:
            def should_skip(self):
                return False

            def record_success(self):
                pass

            def record_failure(self):
                pass

        class DegradationLevel:
            FULL = "full"
            REDUCED = "reduced"
            MINIMAL = "minimal"
            FAILED = "failed"

        def get_service_health():
            return type(
                "ServiceHealth",
                (),
                {"register_service": lambda *args, **kwargs: GracefulDegradation()},
            )()

        def classify_error(error, **kwargs):
            return error

        class CrawlError(Exception):
            pass

else:
    # Fallback: tạo dummy classes
    class CircuitBreaker:
        def __init__(self, *args, **kwargs):
            pass

        def call(self, func, *args, **kwargs):
            return func(*args, **kwargs)

    class CircuitBreakerOpenError(Exception):
        pass

    class DeadLetterQueue:
        def add(self, *args, **kwargs):
            pass

    def get_dlq(*args, **kwargs):
        return DeadLetterQueue()

    class GracefulDegradation:
        def should_skip(self):
            return False

        def record_success(self):
            pass

        def record_failure(self):
            pass

    class DegradationLevel:
        FULL = "full"
        REDUCED = "reduced"
        MINIMAL = "minimal"
        FAILED = "failed"

    def get_service_health():
        return type(
            "ServiceHealth", (), {"register_service": lambda *args, **kwargs: GracefulDegradation()}
        )()

    def classify_error(error, **kwargs):
        return error

    class CrawlError(Exception):
        pass


# Cấu hình mặc định
DEFAULT_ARGS = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,  # Disable retries (was 3) - SDK API issues with retries
    "retry_delay": timedelta(minutes=2),  # Delay 2 phút giữa các retry
    "retry_exponential_backoff": True,  # Exponential backoff
    "max_retry_delay": timedelta(minutes=10),
}

# Cấu hình DAG - Có thể chuyển đổi giữa tự động và thủ công qua Variable
# Đọc cấu hình từ Airflow Variable/Env
schedule_mode = get_variable("TIKI_DAG_SCHEDULE_MODE", default="manual")
schedule_hours = get_int_variable("TIKI_DAG_SCHEDULE_HOURS", default=1)

# Xác định schedule dựa trên mode
if schedule_mode == "scheduled":
    # Sử dụng timedelta để đảm bảo khoảng cách giữa các lần chạy
    dag_schedule = timedelta(hours=schedule_hours)
    dag_description = (
        f"Crawl sản phẩm Tiki với Dynamic Task Mapping (Tự động chạy: mỗi {schedule_hours} giờ)"
    )
    dag_tags = ["tiki", "crawl", "products", "data-pipeline", "scheduled"]
else:
    dag_schedule = None  # Chỉ chạy khi trigger thủ công
    dag_description = "Crawl sản phẩm Tiki với Dynamic Task Mapping (Chạy thủ công - Test mode)"
    dag_tags = ["tiki", "crawl", "products", "data-pipeline", "manual"]

# Cấu hình DAG schedule
dag_schedule_config = dag_schedule

# Documentation đơn giản cho DAG
dag_doc_md = "Crawl sản phẩm từ Tiki.vn với Dynamic Task Mapping và Selenium"

DAG_CONFIG = {
    "dag_id": "tiki_crawl_products_v2",
    "description": dag_description,
    "doc_md": dag_doc_md,
    "default_args": DEFAULT_ARGS,
    "schedule": dag_schedule_config,
    "start_date": datetime(2025, 11, 1),  # Ngày cố định trong quá khứ
    "catchup": False,  # Không chạy lại các task đã bỏ lỡ
    "tags": dag_tags,
    "max_active_runs": 1,  # Chỉ chạy 1 DAG instance tại một thời điểm
    "max_active_tasks": 4,  # Tối ưu cho Local: 4 tasks * 4 drivers = 16 drivers (vừa đủ 16GB RAM)
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
    # Fallback: dùng đường dẫn tương đối hoặc AIRFLOW_HOME
    if os.getenv("AIRFLOW_HOME"):
        DATA_DIR = Path(os.getenv("AIRFLOW_HOME")) / "data"
    else:
        DATA_DIR = Path(__file__).parent.parent.parent / "data"

CATEGORIES_FILE = DATA_DIR / "raw" / "categories_recursive_optimized.json"
CATEGORIES_TREE_FILE = DATA_DIR / "raw" / "categories_tree.json"
OUTPUT_DIR = DATA_DIR / "raw" / "products"
CACHE_DIR = OUTPUT_DIR / "cache"
DETAIL_CACHE_DIR = OUTPUT_DIR / "detail" / "cache"
OUTPUT_FILE = OUTPUT_DIR / "products.json"
OUTPUT_FILE_WITH_DETAIL = OUTPUT_DIR / "products_with_detail.json"
# Progress tracking cho multi-day crawling
PROGRESS_FILE = OUTPUT_DIR / "crawl_progress.json"

# Asset/Dataset đã được xóa - dependencies được quản lý bằng >> operator


@lru_cache(maxsize=1)
def ensure_output_dirs() -> None:
    """Ensure output/cache directories exist.

    Previously executed at import time; now lazily executed on first write-related task call.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    DETAIL_CACHE_DIR.mkdir(parents=True, exist_ok=True)


# Thread-safe lock cho atomic writes
write_lock = Lock()


@lru_cache(maxsize=1)
def get_tiki_circuit_breaker():
    """
    Lazy circuit breaker init to speed up DAG parse.
    """
    ensure_env_loaded()
    return CircuitBreaker(
        failure_threshold=get_int_variable("TIKI_CIRCUIT_BREAKER_FAILURE_THRESHOLD", default=5),
        recovery_timeout=get_int_variable("TIKI_CIRCUIT_BREAKER_RECOVERY_TIMEOUT", default=60),
        expected_exception=Exception,
        name="tiki_api",
    )


@lru_cache(maxsize=1)
def get_tiki_dlq():
    """
    Lazy DLQ init (redis preferred, file fallback).
    """
    ensure_env_loaded()
    try:
        redis_url = get_variable("REDIS_URL", default="redis://redis:6379/3")
        return get_dlq(storage_type="redis", redis_url=redis_url)
    except Exception:
        try:
            dlq_path = DATA_DIR / "dlq"
            return get_dlq(storage_type="file", storage_path=str(dlq_path))
        except Exception:
            return get_dlq()


@lru_cache(maxsize=1)
def get_tiki_degradation():
    """
    Lazy graceful degradation registration.
    """
    ensure_env_loaded()
    service_health = get_service_health()
    return service_health.register_service(
        name="tiki",
        failure_threshold=get_int_variable("TIKI_DEGRADATION_FAILURE_THRESHOLD", default=3),
        recovery_threshold=get_int_variable("TIKI_DEGRADATION_RECOVERY_THRESHOLD", default=5),
    )


# Import modules cho AI summarization và Discord notification
# Tìm các thư mục: analytics/, ai/, notifications/ ở common/ (sau src/)
analytics_path = None
ai_path = None
notifications_path = None
config_path = None

# Thử nhiều đường dẫn có thể cho các modules ở common/
common_base_paths = [
    # Từ /opt/airflow (Docker default - ưu tiên)
    "/opt/airflow/src/common",
    # Từ airflow/dags/ lên 2 cấp đến root (local development)
    os.path.abspath(os.path.join(dag_file_dir, "..", "..", "src", "common")),
    # Từ airflow/dags/ lên 1 cấp (nếu airflow/ là root)
    os.path.abspath(os.path.join(dag_file_dir, "..", "src", "common")),
    # Từ workspace root (nếu mount vào /workspace)
    "/workspace/src/common",
    # Từ current working directory
    os.path.join(os.getcwd(), "src", "common"),
]

for common_base in common_base_paths:
    test_analytics = os.path.join(common_base, "analytics", "aggregator.py")
    test_ai = os.path.join(common_base, "ai", "summarizer.py")
    test_notifications = os.path.join(common_base, "notifications", "discord.py")
    test_config = os.path.join(common_base, "config.py")

    if os.path.exists(test_analytics):
        analytics_path = test_analytics
    if os.path.exists(test_ai):
        ai_path = test_ai
    if os.path.exists(test_notifications):
        notifications_path = test_notifications
    if os.path.exists(test_config):
        config_path = test_config

    if analytics_path and ai_path and notifications_path and config_path:
        break

# Fallback for common_base_paths in local dev
if not (analytics_path and ai_path and notifications_path and config_path):
    project_root = Path(__file__).resolve().parent.parent.parent.parent
    common_base = project_root / "src" / "common"
    if common_base.exists():
        test_analytics = common_base / "analytics" / "aggregator.py"
        test_ai = common_base / "ai" / "summarizer.py"
        test_notifications = common_base / "notifications" / "discord.py"
        test_config = common_base / "config.py"

        if test_analytics.exists():
            analytics_path = str(test_analytics)
        if test_ai.exists():
            ai_path = str(test_ai)
        if test_notifications.exists():
            notifications_path = str(test_notifications)
        if test_config.exists():
            config_path = str(test_config)


def _lazy_import_from_file(module_name: str, file_path: str, attr: str):
    """
    Import attribute from a .py file path lazily.
    """
    import importlib.util

    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        return None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return getattr(mod, attr, None)


@lru_cache(maxsize=1)
def get_DataAggregator():
    ensure_env_loaded()
    if analytics_path and os.path.exists(analytics_path):
        try:
            return _lazy_import_from_file(
                "common.analytics.aggregator", analytics_path, "DataAggregator"
            )
        except Exception as e:
            warnings.warn(f"Không thể import common.analytics.aggregator module: {e}", stacklevel=2)
    return None


@lru_cache(maxsize=1)
def get_load_categories_db_func():
    ensure_env_loaded()
    func = safe_import_attr(
        "pipelines.load.load_categories_to_db",
        "load_categories",
        fallback_paths=[Path(p).parent.parent for p in possible_paths],
    )
    if not func:
        warnings.warn("load_categories_to_db not available; DB load will be skipped", stacklevel=2)
    return func


# Global debug flags controlled via Airflow Variables
DEBUG_LOAD_CATEGORIES = (
    get_variable("TIKI_DEBUG_LOAD_CATEGORIES", default="false").lower() == "true"
)
DEBUG_ENRICH_CATEGORY_PATH = (
    get_variable("TIKI_DEBUG_ENRICH_CATEGORY_PATH", default="false").lower() == "true"
)


@lru_cache(maxsize=1)
def get_AISummarizer():
    ensure_env_loaded()
    if ai_path and os.path.exists(ai_path):
        try:
            return _lazy_import_from_file("common.ai.summarizer", ai_path, "AISummarizer")
        except Exception as e:
            warnings.warn(f"Không thể import common.ai.summarizer module: {e}", stacklevel=2)
    return None


@lru_cache(maxsize=1)
def get_DiscordNotifier():
    ensure_env_loaded()
    if notifications_path and os.path.exists(notifications_path):
        try:
            return _lazy_import_from_file(
                "common.notifications.discord", notifications_path, "DiscordNotifier"
            )
        except Exception as e:
            warnings.warn(
                f"Không thể import common.notifications.discord module: {e}", stacklevel=2
            )
    return None
