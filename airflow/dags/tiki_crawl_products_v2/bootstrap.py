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
    import psycopg2
except ImportError:
    psycopg2 = None

try:
    import aiohttp
except ImportError:
    aiohttp = None

try:
    from airflow import DAG

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

        def __exit__(self, exc_type, *args):
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


def load_env_file():
    """
    Dummy load_env_file for bootstrap.
    """


# Setup imports path
src_path = Path("/opt/airflow/src")
if src_path.exists() and str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))
else:
    # Fallback for local development
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

    def get_variable(key, default=None):
        return os.getenv(key, default)

    def get_int_variable(key, default=0):
        return int(os.getenv(key, default))

    def load_env_file():
        pass

    def safe_import_attr(module_path, attr, **kwargs):
        try:
            mod = __import__(module_path, fromlist=[attr])
            return getattr(mod, attr, None)
        except ImportError:
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


# Global debug flags controlled via Airflow Variables
DEBUG_LOAD_CATEGORIES = (
    get_variable("TIKI_DEBUG_LOAD_CATEGORIES", default="false").lower() == "true"
)
DEBUG_ENRICH_CATEGORY_PATH = (
    get_variable("TIKI_DEBUG_ENRICH_CATEGORY_PATH", default="false").lower() == "true"
)



# Import resilience patterns
try:
    from pipelines.crawl.resilience.exceptions import CrawlError, classify_error
    from pipelines.crawl.resilience.circuit_breaker import CircuitBreaker, CircuitBreakerOpenError
    from pipelines.crawl.resilience.dead_letter_queue import DeadLetterQueue, get_dlq
    from pipelines.crawl.resilience.graceful_degradation import (
        GracefulDegradation,
        DegradationLevel,
        get_service_health,
    )
except ImportError:
    class CircuitBreaker:
        def __init__(self, *args, **kwargs): pass
        def call(self, func, *args, **kwargs): return func(*args, **kwargs)
    class CircuitBreakerOpenError(Exception): pass
    class DeadLetterQueue:
        def add(self, *args, **kwargs): pass
    def get_dlq(*args, **kwargs): return DeadLetterQueue()
    class GracefulDegradation:
        def should_skip(self): return False
        def record_success(self): pass
        def record_failure(self): pass
    class DegradationLevel:
        FULL, REDUCED, MINIMAL, FAILED = "full", "reduced", "minimal", "failed"
    def get_service_health():
        return type("ServiceHealth", (), {"register_service": lambda *args, **kwargs: GracefulDegradation()})()
    def classify_error(error, **kwargs): return error
    class CrawlError(Exception): pass


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


@lru_cache(maxsize=1)
def ensure_output_dirs() -> None:
    """Ensure output/cache directories exist."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    DETAIL_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _fix_sys_path_for_pipelines_import(logger=None):
    """Sửa sys.path và sys.modules để đảm bảo import được pipelines."""
    if logger:
        logger.info("🔧 Fixing sys.path for pipelines import...")

    # 1. Thêm src vào sys.path nếu chưa có
    src_path_str = str(src_path)
    if src_path_str not in sys.path:
        sys.path.insert(0, src_path_str)
        if logger:
            logger.info(f"   Added {src_path_str} to sys.path")

    # 2. Xóa các entries giả trong sys.modules
    modules_to_remove = [m for m in sys.modules.keys() if m.startswith("pipelines") or m.startswith("common")]
    count = 0
    for module_name in modules_to_remove:
        if sys.modules[module_name] is None:
            del sys.modules[module_name]
            count += 1
    if logger and count > 0:
        logger.info(f"   Cleaned {count} None modules starting with pipelines/common")


def get_logger(context=None):
    ti = (context or {}).get("ti")
    logger = getattr(ti, "log", None)
    if logger is not None:
        return logger
    return logging.getLogger("airflow.task")


@lru_cache(maxsize=1)
def get_tiki_circuit_breaker():
    ensure_env_loaded()
    return CircuitBreaker(
        failure_threshold=get_int_variable("TIKI_CIRCUIT_BREAKER_FAILURE_THRESHOLD", default=5),
        recovery_timeout=get_int_variable("TIKI_CIRCUIT_BREAKER_RECOVERY_TIMEOUT", default=60),
        expected_exception=Exception,
        name="tiki_api",
    )


@lru_cache(maxsize=1)
def get_tiki_dlq():
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
    ensure_env_loaded()
    service_health = get_service_health()
    return service_health.register_service(
        name="tiki",
        failure_threshold=get_int_variable("TIKI_DEGRADATION_FAILURE_THRESHOLD", default=3),
        recovery_threshold=get_int_variable("TIKI_DEGRADATION_RECOVERY_THRESHOLD", default=5),
    )


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


# Import crawl_products_detail
try:
    from pipelines.crawl.crawl_products_detail import (
        crawl_product_detail_async,
        crawl_product_detail_with_driver,
        crawl_product_detail_with_selenium,
        extract_product_detail,
    )
except ImportError:
    def crawl_product_detail_with_selenium(*args, **kwargs):
        raise ImportError("Module pipelines.crawl.crawl_products_detail not found")
    def crawl_product_detail_with_driver(*args, **kwargs):
        raise ImportError("Module pipelines.crawl.crawl_products_detail not found")
    def extract_product_detail(*args, **kwargs):
        raise ImportError("Module pipelines.crawl.crawl_products_detail not found")
    async def crawl_product_detail_async(*args, **kwargs):
        raise ImportError("Module pipelines.crawl.crawl_products_detail not found")

# Import crawl_category_products
try:
    from pipelines.crawl.crawl_products import crawl_category_products
except ImportError:
    def crawl_category_products(*args, **kwargs):
        raise ImportError("Module pipelines.crawl.crawl_products not found")


# Import crawl utilities from renamed crawl_utils.py (to avoid collision with utils/ dir)
try:
    from pipelines.crawl.crawl_utils import (
        SeleniumDriverPool,
        extract_category_id_from_url,
        extract_product_id_from_url,
    )
except ImportError:
    SeleniumDriverPool = None
    extract_category_id_from_url = None
    extract_product_id_from_url = None




# Define paths for lazy imports
analytics_path = os.path.join(src_path, "common", "analytics", "aggregator.py")
ai_path = os.path.join(src_path, "common", "ai", "summarizer.py")
notifications_path = os.path.join(src_path, "common", "notifications", "discord.py")
config_path = os.path.join(src_path, "common", "config.py")


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
