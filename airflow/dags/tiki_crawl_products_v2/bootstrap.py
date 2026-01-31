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

OPTIMIZATION: This module uses lazy loading patterns to minimize import overhead
during DAG parsing. Heavy imports are deferred until actually needed.
"""

import json
import logging
import os
import re
import shutil
import sys
import time
import warnings
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

# ============================================================================
# IMPORT TIME MEASUREMENT
# ============================================================================
_bootstrap_import_start_time = time.time()
_import_times = {}


def _measure_import(module_name: str, import_func):
    """Measure and log import time for a module."""
    if module_name in _import_times:
        return _import_times[module_name]
    
    start = time.time()
    try:
        result = import_func()
        duration = time.time() - start
        _import_times[module_name] = duration
        logging.info(f"⏱️  Import {module_name}: {duration:.3f}s")
        return result
    except Exception as e:
        duration = time.time() - start
        logging.warning(f"⚠️  Import {module_name} failed after {duration:.3f}s: {e}")
        raise


# ============================================================================
# LAZY IMPORT CACHES
# ============================================================================
_psycopg2_cache = None
_aiohttp_cache = None
_airflow_dag_cache = None
_airflow_taskgroup_cache = None
_airflow_pythonoperator_cache = None


def get_psycopg2():
    """Lazy import psycopg2 with caching."""
    global _psycopg2_cache
    if _psycopg2_cache is None:
        def _import():
            global _psycopg2_cache
            try:
                import psycopg2
                _psycopg2_cache = psycopg2
            except ImportError:
                _psycopg2_cache = None
            return _psycopg2_cache
        return _measure_import("psycopg2", _import)
    return _psycopg2_cache


def get_aiohttp():
    """Lazy import aiohttp with caching."""
    global _aiohttp_cache
    if _aiohttp_cache is None:
        def _import():
            global _aiohttp_cache
            try:
                import aiohttp
                _aiohttp_cache = aiohttp
            except ImportError:
                _aiohttp_cache = None
            return _aiohttp_cache
        return _measure_import("aiohttp", _import)
    return _aiohttp_cache


def get_airflow_dag():
    """Lazy import Airflow DAG with caching."""
    global _airflow_dag_cache
    if _airflow_dag_cache is None:
        def _import():
            global _airflow_dag_cache
            try:
                from airflow import DAG
                _airflow_dag_cache = DAG
            except ImportError:
                # Mock for local development without airflow installed
                class DAG:
                    def __init__(self, *args, **kwargs):
                        pass

                    def __enter__(self):
                        return self

                    def __exit__(self, exc_type, *args):
                        pass
                _airflow_dag_cache = DAG
            return _airflow_dag_cache
        return _measure_import("airflow.DAG", _import)
    return _airflow_dag_cache


def get_airflow_taskgroup():
    """Lazy import Airflow TaskGroup with caching."""
    global _airflow_taskgroup_cache
    if _airflow_taskgroup_cache is None:
        def _import():
            global _airflow_taskgroup_cache
            try:
                # Airflow 3.0+ compatible imports
                try:
                    from airflow.sdk import TaskGroup
                except ImportError:
                    from airflow.utils.task_group import TaskGroup
                _airflow_taskgroup_cache = TaskGroup
            except ImportError:
                # Mock for local development
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
                _airflow_taskgroup_cache = TaskGroup
            return _airflow_taskgroup_cache
        return _measure_import("airflow.TaskGroup", _import)
    return _airflow_taskgroup_cache


def get_airflow_pythonoperator():
    """Lazy import Airflow PythonOperator with caching."""
    global _airflow_pythonoperator_cache
    if _airflow_pythonoperator_cache is None:
        def _import():
            global _airflow_pythonoperator_cache
            try:
                try:
                    from airflow.providers.standard.operators.python import PythonOperator
                except ImportError:
                    from airflow.operators.python import PythonOperator
                _airflow_pythonoperator_cache = PythonOperator
            except ImportError:
                # Mock for local development
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
                _airflow_pythonoperator_cache = PythonOperator
            return _airflow_pythonoperator_cache
        return _measure_import("airflow.PythonOperator", _import)
    return _airflow_pythonoperator_cache


# Expose lazy-loaded classes at module level for backward compatibility
# These will trigger lazy loading when accessed
_airflow_classes_initialized = False
DAG = None
TaskGroup = None
PythonOperator = None


def _ensure_airflow_classes():
    """Ensure Airflow classes are loaded."""
    global DAG, TaskGroup, PythonOperator, _airflow_classes_initialized
    
    if _airflow_classes_initialized:
        return DAG, TaskGroup, PythonOperator
    
    if DAG is None:
        DAG = get_airflow_dag()
    if TaskGroup is None:
        TaskGroup = get_airflow_taskgroup()
    if PythonOperator is None:
        PythonOperator = get_airflow_pythonoperator()
    
    _airflow_classes_initialized = True
    return DAG, TaskGroup, PythonOperator


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

# ============================================================================
# LAZY IMPORT: Custom Utilities
# ============================================================================
_common_utils_cache = None


def get_common_utils():
    """Lazy import common utilities with caching."""
    global _common_utils_cache
    if _common_utils_cache is None:
        def _import():
            global _common_utils_cache
            try:
                from common.airflow_utils import (
                    get_int_variable,
                    get_variable,
                    load_env_file,
                    safe_import_attr,
                )
                from pipelines.crawl.hierarchy import get_hierarchy_map
                
                _common_utils_cache = {
                    'get_variable': get_variable,
                    'get_int_variable': get_int_variable,
                    'load_env_file': load_env_file,
                    'safe_import_attr': safe_import_attr,
                    'get_hierarchy_map': get_hierarchy_map,
                }
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
                
                _common_utils_cache = {
                    'get_variable': get_variable,
                    'get_int_variable': get_int_variable,
                    'load_env_file': load_env_file,
                    'safe_import_attr': safe_import_attr,
                    'get_hierarchy_map': get_hierarchy_map,
                }
            return _common_utils_cache
        return _measure_import("common.airflow_utils", _import)
    return _common_utils_cache


# Expose utility functions at module level for backward compatibility
def get_variable(key, default=None):
    """Get Airflow variable or environment variable."""
    return get_common_utils()['get_variable'](key, default)


def get_int_variable(key, default=0):
    """Get integer Airflow variable or environment variable."""
    return get_common_utils()['get_int_variable'](key, default)


def load_env_file():
    """Load environment file."""
    return get_common_utils()['load_env_file']()


def safe_import_attr(module_path, attr, **kwargs):
    """Safely import attribute from module."""
    return get_common_utils()['safe_import_attr'](module_path, attr, **kwargs)


def get_hierarchy_map(force_reload=False):
    """Get hierarchy map."""
    return get_common_utils()['get_hierarchy_map'](force_reload)


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


# ============================================================================
# LAZY EVALUATION: Global debug flags and configuration
# ============================================================================
# These are evaluated lazily to avoid triggering imports at module load time

_debug_load_categories = None
_debug_enrich_category_path = None
_schedule_mode = None
_schedule_hours = None


def get_debug_load_categories():
    """Get DEBUG_LOAD_CATEGORIES flag with lazy evaluation."""
    global _debug_load_categories
    if _debug_load_categories is None:
        _debug_load_categories = (
            get_variable("TIKI_DEBUG_LOAD_CATEGORIES", default="false").lower() == "true"
        )
    return _debug_load_categories


def get_debug_enrich_category_path():
    """Get DEBUG_ENRICH_CATEGORY_PATH flag with lazy evaluation."""
    global _debug_enrich_category_path
    if _debug_enrich_category_path is None:
        _debug_enrich_category_path = (
            get_variable("TIKI_DEBUG_ENRICH_CATEGORY_PATH", default="false").lower() == "true"
        )
    return _debug_enrich_category_path


def get_schedule_mode():
    """Get schedule mode with lazy evaluation."""
    global _schedule_mode
    if _schedule_mode is None:
        _schedule_mode = get_variable("TIKI_DAG_SCHEDULE_MODE", default="manual")
    return _schedule_mode


def get_schedule_hours():
    """Get schedule hours with lazy evaluation."""
    global _schedule_hours
    if _schedule_hours is None:
        _schedule_hours = get_int_variable("TIKI_DAG_SCHEDULE_HOURS", default=1)
    return _schedule_hours


# Backward compatibility: expose as module-level variables that trigger lazy evaluation
# Note: These will be None initially and should be accessed via getter functions
# or will be populated when first accessed by DAG code
DEBUG_LOAD_CATEGORIES = None
DEBUG_ENRICH_CATEGORY_PATH = None


# ============================================================================
# LAZY IMPORT: Resilience Patterns
# ============================================================================
_resilience_cache = None


def get_resilience_patterns():
    """Lazy import resilience patterns with caching."""
    global _resilience_cache
    if _resilience_cache is None:
        def _import():
            global _resilience_cache
            try:
                from pipelines.crawl.resilience.circuit_breaker import CircuitBreaker, CircuitBreakerOpenError
                from pipelines.crawl.resilience.dead_letter_queue import DeadLetterQueue, get_dlq
                from pipelines.crawl.resilience.exceptions import CrawlError, classify_error
                from pipelines.crawl.resilience.graceful_degradation import (
                    DegradationLevel,
                    GracefulDegradation,
                    get_service_health,
                )
                
                _resilience_cache = {
                    'CircuitBreaker': CircuitBreaker,
                    'CircuitBreakerOpenError': CircuitBreakerOpenError,
                    'DeadLetterQueue': DeadLetterQueue,
                    'get_dlq': get_dlq,
                    'CrawlError': CrawlError,
                    'classify_error': classify_error,
                    'DegradationLevel': DegradationLevel,
                    'GracefulDegradation': GracefulDegradation,
                    'get_service_health': get_service_health,
                }
            except ImportError:
                # Fallback implementations
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
                    FULL, REDUCED, MINIMAL, FAILED = "full", "reduced", "minimal", "failed"

                def get_service_health():
                    return type(
                        "ServiceHealth", (), {"register_service": lambda *args, **kwargs: GracefulDegradation()}
                    )()

                def classify_error(error, **kwargs):
                    return error

                class CrawlError(Exception):
                    pass
                
                _resilience_cache = {
                    'CircuitBreaker': CircuitBreaker,
                    'CircuitBreakerOpenError': CircuitBreakerOpenError,
                    'DeadLetterQueue': DeadLetterQueue,
                    'get_dlq': get_dlq,
                    'CrawlError': CrawlError,
                    'classify_error': classify_error,
                    'DegradationLevel': DegradationLevel,
                    'GracefulDegradation': GracefulDegradation,
                    'get_service_health': get_service_health,
                }
            return _resilience_cache
        return _measure_import("pipelines.crawl.resilience", _import)
    return _resilience_cache


# Expose resilience classes at module level for backward compatibility
def get_CircuitBreaker():
    """Get CircuitBreaker class."""
    return get_resilience_patterns()['CircuitBreaker']


def get_CircuitBreakerOpenError():
    """Get CircuitBreakerOpenError class."""
    return get_resilience_patterns()['CircuitBreakerOpenError']


def get_DeadLetterQueue():
    """Get DeadLetterQueue class."""
    return get_resilience_patterns()['DeadLetterQueue']


def get_dlq(*args, **kwargs):
    """Get dead letter queue instance."""
    return get_resilience_patterns()['get_dlq'](*args, **kwargs)


def get_CrawlError():
    """Get CrawlError class."""
    return get_resilience_patterns()['CrawlError']


def classify_error(error, **kwargs):
    """Classify error."""
    return get_resilience_patterns()['classify_error'](error, **kwargs)


def get_DegradationLevel():
    """Get DegradationLevel class."""
    return get_resilience_patterns()['DegradationLevel']


def get_GracefulDegradation():
    """Get GracefulDegradation class."""
    return get_resilience_patterns()['GracefulDegradation']


def get_service_health():
    """Get service health instance."""
    return get_resilience_patterns()['get_service_health']()


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
# Đọc cấu hình từ Airflow Variable/Env (lazy evaluation)
# Note: These are now functions to avoid triggering imports at module load time

def _get_dag_schedule_config():
    """Get DAG schedule configuration with lazy evaluation."""
    schedule_mode = get_schedule_mode()
    schedule_hours = get_schedule_hours()
    
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
    
    return dag_schedule, dag_description, dag_tags


# Cấu hình DAG schedule (lazy evaluation)
def get_dag_schedule():
    """Get DAG schedule."""
    return _get_dag_schedule_config()[0]


def get_dag_description():
    """Get DAG description."""
    return _get_dag_schedule_config()[1]


def get_dag_tags():
    """Get DAG tags."""
    return _get_dag_schedule_config()[2]

# Documentation đơn giản cho DAG
dag_doc_md = "Crawl sản phẩm từ Tiki.vn với Dynamic Task Mapping và Selenium"

# DAG_CONFIG with lazy evaluation
def get_dag_config():
    """Get DAG configuration with lazy evaluation."""
    dag_schedule, dag_description, dag_tags = _get_dag_schedule_config()
    
    return {
        "dag_id": "tiki_crawl_products_v2",
        "description": dag_description,
        "doc_md": dag_doc_md,
        "default_args": DEFAULT_ARGS,
        "schedule": dag_schedule,
        "start_date": datetime(2025, 11, 1),  # Ngày cố định trong quá khứ
        "catchup": False,  # Không chạy lại các task đã bỏ lỡ
        "tags": dag_tags,
        "max_active_runs": 1,  # Chỉ chạy 1 DAG instance tại một thời điểm
        "max_active_tasks": 4,  # Tối ưu cho Local: 4 tasks * 4 drivers = 16 drivers (vừa đủ 16GB RAM)
    }


# Backward compatibility: DAG_CONFIG as a property-like access
# This will be evaluated when first accessed
_dag_config_cache = None


def _get_dag_config_cached():
    """Get cached DAG config."""
    global _dag_config_cache
    if _dag_config_cache is None:
        _dag_config_cache = get_dag_config()
    return _dag_config_cache


# For backward compatibility, expose DAG_CONFIG at module level
# This will be None initially and populated when get_dag_config() is called
DAG_CONFIG = None


def _ensure_dag_config():
    """Ensure DAG_CONFIG is populated."""
    global DAG_CONFIG
    if DAG_CONFIG is None:
        DAG_CONFIG = get_dag_config()
    return DAG_CONFIG


@lru_cache(maxsize=1)
def ensure_output_dirs() -> None:
    """
    Ensure output/cache directories exist.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    DETAIL_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _fix_sys_path_for_pipelines_import(logger=None):
    """
    Sửa sys.path và sys.modules để đảm bảo import được pipelines.
    """
    if logger:
        logger.info("🔧 Fixing sys.path for pipelines import...")

    # 1. Thêm src vào sys.path nếu chưa có
    src_path_str = str(src_path)
    if src_path_str not in sys.path:
        sys.path.insert(0, src_path_str)
        if logger:
            logger.info(f"   Added {src_path_str} to sys.path")

    # 2. Xóa các entries giả trong sys.modules
    modules_to_remove = [
        m for m in sys.modules.keys() if m.startswith("pipelines") or m.startswith("common")
    ]
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
    """Get Tiki circuit breaker instance with lazy loading."""
    ensure_env_loaded()
    CircuitBreaker = get_CircuitBreaker()
    return CircuitBreaker(
        failure_threshold=get_int_variable("TIKI_CIRCUIT_BREAKER_FAILURE_THRESHOLD", default=5),
        recovery_timeout=get_int_variable("TIKI_CIRCUIT_BREAKER_RECOVERY_TIMEOUT", default=60),
        expected_exception=Exception,
        name="tiki_api",
    )


@lru_cache(maxsize=1)
def get_tiki_dlq():
    """Get Tiki dead letter queue instance with lazy loading."""
    ensure_env_loaded()
    try:
        redis_url = get_variable("REDIS_URL", default="redis://redis:6379/3")
        return get_dlq(redis_url=redis_url, storage_type="redis")
    except Exception:
        try:
            dlq_path = DATA_DIR / "dlq"
            return get_dlq(storage_path=str(dlq_path), storage_type="file")
        except Exception:
            return get_dlq()


@lru_cache(maxsize=1)
def get_tiki_degradation():
    """Get Tiki graceful degradation instance with lazy loading."""
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


# ============================================================================
# LAZY IMPORT: Pipeline Modules
# ============================================================================
_data_transformer_cache = None
_optimized_loader_cache = None
_crawl_detail_cache = None
_crawl_products_cache = None
_crawl_utils_cache = None


def get_DataTransformer():
    """Lazy import DataTransformer with caching."""
    global _data_transformer_cache
    if _data_transformer_cache is None:
        def _import():
            global _data_transformer_cache
            try:
                from pipelines.transform.transformer import DataTransformer
                _data_transformer_cache = DataTransformer
            except ImportError:
                _data_transformer_cache = None
            return _data_transformer_cache
        return _measure_import("pipelines.transform.transformer", _import)
    return _data_transformer_cache


def get_OptimizedDataLoader():
    """Lazy import OptimizedDataLoader with caching."""
    global _optimized_loader_cache
    if _optimized_loader_cache is None:
        def _import():
            global _optimized_loader_cache
            try:
                from pipelines.load.optimized_loader import OptimizedDataLoader
                _optimized_loader_cache = OptimizedDataLoader
            except ImportError:
                _optimized_loader_cache = None
            return _optimized_loader_cache
        return _measure_import("pipelines.load.optimized_loader", _import)
    return _optimized_loader_cache


def get_crawl_detail_functions():
    """Lazy import crawl detail functions with caching."""
    global _crawl_detail_cache
    if _crawl_detail_cache is None:
        def _import():
            global _crawl_detail_cache
            try:
                from pipelines.crawl.crawl_products_detail import (
                    crawl_product_detail_async,
                    crawl_product_detail_with_driver,
                    crawl_product_detail_with_selenium,
                    extract_product_detail,
                )
                _crawl_detail_cache = {
                    'crawl_product_detail_async': crawl_product_detail_async,
                    'crawl_product_detail_with_driver': crawl_product_detail_with_driver,
                    'crawl_product_detail_with_selenium': crawl_product_detail_with_selenium,
                    'extract_product_detail': extract_product_detail,
                }
            except ImportError:
                def crawl_product_detail_with_selenium(*args, **kwargs):
                    raise ImportError("Module pipelines.crawl.crawl_products_detail not found")

                def crawl_product_detail_with_driver(*args, **kwargs):
                    raise ImportError("Module pipelines.crawl.crawl_products_detail not found")

                def extract_product_detail(*args, **kwargs):
                    raise ImportError("Module pipelines.crawl.crawl_products_detail not found")

                async def crawl_product_detail_async(*args, **kwargs):
                    raise ImportError("Module pipelines.crawl.crawl_products_detail not found")
                
                _crawl_detail_cache = {
                    'crawl_product_detail_async': crawl_product_detail_async,
                    'crawl_product_detail_with_driver': crawl_product_detail_with_driver,
                    'crawl_product_detail_with_selenium': crawl_product_detail_with_selenium,
                    'extract_product_detail': extract_product_detail,
                }
            return _crawl_detail_cache
        return _measure_import("pipelines.crawl.crawl_products_detail", _import)
    return _crawl_detail_cache


def crawl_product_detail_with_selenium(*args, **kwargs):
    """Crawl product detail with Selenium."""
    return get_crawl_detail_functions()['crawl_product_detail_with_selenium'](*args, **kwargs)


def crawl_product_detail_with_driver(*args, **kwargs):
    """Crawl product detail with driver."""
    return get_crawl_detail_functions()['crawl_product_detail_with_driver'](*args, **kwargs)


def extract_product_detail(*args, **kwargs):
    """Extract product detail."""
    return get_crawl_detail_functions()['extract_product_detail'](*args, **kwargs)


async def crawl_product_detail_async(*args, **kwargs):
    """Crawl product detail async."""
    return await get_crawl_detail_functions()['crawl_product_detail_async'](*args, **kwargs)


def get_crawl_category_products():
    """Lazy import crawl_category_products with caching."""
    global _crawl_products_cache
    if _crawl_products_cache is None:
        def _import():
            global _crawl_products_cache
            try:
                from pipelines.crawl.crawl_products import crawl_category_products
                _crawl_products_cache = crawl_category_products
            except ImportError:
                def crawl_category_products(*args, **kwargs):
                    raise ImportError("Module pipelines.crawl.crawl_products not found")
                _crawl_products_cache = crawl_category_products
            return _crawl_products_cache
        return _measure_import("pipelines.crawl.crawl_products", _import)
    return _crawl_products_cache


def crawl_category_products(*args, **kwargs):
    """Crawl category products."""
    return get_crawl_category_products()(*args, **kwargs)


def get_crawl_utils():
    """Lazy import crawl utilities with caching."""
    global _crawl_utils_cache
    if _crawl_utils_cache is None:
        def _import():
            global _crawl_utils_cache
            try:
                from pipelines.crawl.crawl_utils import (
                    SeleniumDriverPool,
                    extract_category_id_from_url,
                    extract_product_id_from_url,
                )
                _crawl_utils_cache = {
                    'SeleniumDriverPool': SeleniumDriverPool,
                    'extract_category_id_from_url': extract_category_id_from_url,
                    'extract_product_id_from_url': extract_product_id_from_url,
                }
            except ImportError:
                _crawl_utils_cache = {
                    'SeleniumDriverPool': None,
                    'extract_category_id_from_url': None,
                    'extract_product_id_from_url': None,
                }
            return _crawl_utils_cache
        return _measure_import("pipelines.crawl.crawl_utils", _import)
    return _crawl_utils_cache


# Expose crawl utils at module level for backward compatibility
def SeleniumDriverPool():
    """Get SeleniumDriverPool class."""
    return get_crawl_utils()['SeleniumDriverPool']


def extract_category_id_from_url(url):
    """Get extract_category_id_from_url function."""
    func = get_crawl_utils()['extract_category_id_from_url']
    if func:
        return func(url)
    return None


def extract_product_id_from_url(url):
    """Get extract_product_id_from_url function."""
    func = get_crawl_utils()['extract_product_id_from_url']
    if func:
        return func(url)
    return None


# ============================================================================
# LAZY IMPORT: Analytics, AI, and Notifications
# ============================================================================
# Define paths for lazy imports
analytics_path = os.path.join(src_path, "common", "analytics", "aggregator.py")
ai_path = os.path.join(src_path, "common", "ai", "summarizer.py")
notifications_path = os.path.join(src_path, "common", "notifications", "discord.py")
config_path = os.path.join(src_path, "common", "config.py")

_data_aggregator_cache = None
_ai_summarizer_cache = None
_discord_notifier_cache = None
_load_categories_db_cache = None


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
    """Lazy import DataAggregator with caching."""
    global _data_aggregator_cache
    if _data_aggregator_cache is None:
        def _import():
            global _data_aggregator_cache
            ensure_env_loaded()
            if analytics_path and os.path.exists(analytics_path):
                try:
                    _data_aggregator_cache = _lazy_import_from_file(
                        "common.analytics.aggregator", analytics_path, "DataAggregator"
                    )
                except Exception as e:
                    warnings.warn(f"Không thể import common.analytics.aggregator module: {e}", stacklevel=2)
                    _data_aggregator_cache = None
            else:
                _data_aggregator_cache = None
            return _data_aggregator_cache
        return _measure_import("common.analytics.aggregator", _import)
    return _data_aggregator_cache


@lru_cache(maxsize=1)
def get_load_categories_db_func():
    """Lazy import load_categories function with caching."""
    global _load_categories_db_cache
    if _load_categories_db_cache is None:
        def _import():
            global _load_categories_db_cache
            ensure_env_loaded()
            func = safe_import_attr(
                "pipelines.load.load_categories_to_db",
                "load_categories",
                fallback_paths=[Path(p).parent.parent for p in possible_paths],
            )
            if not func:
                warnings.warn("load_categories_to_db not available; DB load will be skipped", stacklevel=2)
            _load_categories_db_cache = func
            return _load_categories_db_cache
        return _measure_import("pipelines.load.load_categories_to_db", _import)
    return _load_categories_db_cache


@lru_cache(maxsize=1)
def get_AISummarizer():
    """Lazy import AISummarizer with caching."""
    global _ai_summarizer_cache
    if _ai_summarizer_cache is None:
        def _import():
            global _ai_summarizer_cache
            ensure_env_loaded()
            if ai_path and os.path.exists(ai_path):
                try:
                    _ai_summarizer_cache = _lazy_import_from_file("common.ai.summarizer", ai_path, "AISummarizer")
                except Exception as e:
                    warnings.warn(f"Không thể import common.ai.summarizer module: {e}", stacklevel=2)
                    _ai_summarizer_cache = None
            else:
                _ai_summarizer_cache = None
            return _ai_summarizer_cache
        return _measure_import("common.ai.summarizer", _import)
    return _ai_summarizer_cache


@lru_cache(maxsize=1)
def get_DiscordNotifier():
    """Lazy import DiscordNotifier with caching."""
    global _discord_notifier_cache
    if _discord_notifier_cache is None:
        def _import():
            global _discord_notifier_cache
            ensure_env_loaded()
            if notifications_path and os.path.exists(notifications_path):
                try:
                    _discord_notifier_cache = _lazy_import_from_file(
                        "common.notifications.discord", notifications_path, "DiscordNotifier"
                    )
                except Exception as e:
                    warnings.warn(
                        f"Không thể import common.notifications.discord module: {e}", stacklevel=2
                    )
                    _discord_notifier_cache = None
            else:
                _discord_notifier_cache = None
            return _discord_notifier_cache
        return _measure_import("common.notifications.discord", _import)
    return _discord_notifier_cache


# ============================================================================
# BOOTSTRAP IMPORT TIME LOGGING
# ============================================================================
def log_bootstrap_import_time():
    """Log total bootstrap module import time."""
    total_time = time.time() - _bootstrap_import_start_time
    logging.info(f"⏱️  Bootstrap module total import time: {total_time:.3f}s")
    
    # Log individual import times
    if _import_times:
        logging.info("📊 Individual import times:")
        for module, duration in sorted(_import_times.items(), key=lambda x: x[1], reverse=True):
            logging.info(f"   - {module}: {duration:.3f}s")
    
    return total_time


# Log bootstrap import time when module is first imported
log_bootstrap_import_time()


# ============================================================================
# MODULE INITIALIZATION - Populate module-level variables
# ============================================================================
# Initialize Airflow classes at module load time so they're available for DAG definitions
try:
    _ensure_airflow_classes()
except Exception as e:
    logging.warning(f"⚠️ Could not initialize Airflow classes: {e}")
    # Set fallback values
    DAG = None
    TaskGroup = None
    PythonOperator = None

# Initialize DAG_CONFIG at module load time
try:
    DAG_CONFIG = get_dag_config()
except Exception as e:
    logging.warning(f"⚠️ Could not initialize DAG_CONFIG: {e}")
    DAG_CONFIG = None

# Initialize resilience patterns at module load time to make them available for import
# This ensures CircuitBreakerOpenError and other classes are available
# We wrap this in a try-except to handle cases where the modules aren't available
try:
    _resilience_initialized = False
    
    def _ensure_resilience_initialized():
        """Ensure resilience patterns are initialized."""
        global _resilience_initialized, CircuitBreaker, CircuitBreakerOpenError
        global DeadLetterQueue, CrawlError, DegradationLevel, GracefulDegradation
        
        if _resilience_initialized:
            return
        
        try:
            CircuitBreaker = get_CircuitBreaker()
            CircuitBreakerOpenError = get_CircuitBreakerOpenError()
            DeadLetterQueue = get_DeadLetterQueue()
            CrawlError = get_CrawlError()
            DegradationLevel = get_DegradationLevel()
            GracefulDegradation = get_GracefulDegradation()
            _resilience_initialized = True
        except Exception as e:
            logging.warning(f"⚠️ Could not initialize resilience patterns: {e}")
            # Fallback to None - will be initialized on first use
            CircuitBreaker = None
            CircuitBreakerOpenError = None
            DeadLetterQueue = None
            CrawlError = None
            DegradationLevel = None
            GracefulDegradation = None
    
    # Initialize immediately for import availability
    _ensure_resilience_initialized()
    
except Exception as e:
    logging.warning(f"⚠️ Could not initialize resilience patterns: {e}")
    # Fallback to None - will be initialized on first use
    CircuitBreaker = None
    CircuitBreakerOpenError = None
    DeadLetterQueue = None
    CrawlError = None
    DegradationLevel = None
    GracefulDegradation = None

# Log bootstrap completion
log_bootstrap_import_time()
