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

Dependencies được quản lý bằng >> operator giữa các tasks.
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
from pathlib import Path
from threading import Lock
from typing import Any

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

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

# Try to import redis_cache for caching
redis_cache = None
try:
    # Ensure src path in sys.path for package-style imports
    from pathlib import Path

    src_path = Path("/opt/airflow/src")
    if src_path.exists() and str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    try:
        from pipelines.crawl.storage.redis_cache import get_redis_cache  # type: ignore

        redis_cache = get_redis_cache("redis://redis:6379/1")  # type: ignore
    except Exception as import_err:
        warnings.warn(
            f"⚠️  Import get_redis_cache failed: {import_err} -> trying dynamic import", stacklevel=2
        )
        # Dynamic import fallback
        import importlib.util

        rc_path = src_path / "pipelines" / "crawl" / "storage" / "redis_cache.py"
        get_redis_cache = None  # type: ignore
        if rc_path.exists():
            spec = importlib.util.spec_from_file_location("redis_cache_dyn", rc_path)
            if spec and spec.loader:
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)  # type: ignore
                get_redis_cache = getattr(mod, "get_redis_cache", None)  # type: ignore
            if get_redis_cache:
                redis_cache = get_redis_cache("redis://redis:6379/1")  # type: ignore
except Exception as e:
    warnings.warn(f"Redis cache initialization failed: {e}", stacklevel=2)
    redis_cache = None


# Wrapper function để suppress deprecation warning khi gọi Variable.get()
def get_variable(key: str, default: Any = None) -> Any:
    try:
        return _Variable.get(key, default=default)
    except Exception:
        return default


# ========== LOAD CATEGORY HIERARCHY MAP FOR AUTO-PARENT-DETECTION ==========
# Cache hierarchy map globally to avoid reloading in every task
_hierarchy_map_cache = None


def get_hierarchy_map(force_reload=False):
    """Load category hierarchy map for auto-parent-detection

    This map contains all categories with their parent chains,
    allowing extract_product_detail to auto-detect missing Level 0 (parent category)
    """
    global _hierarchy_map_cache

    if _hierarchy_map_cache is not None and not force_reload:
        return _hierarchy_map_cache

    try:
        hierarchy_file = "/opt/airflow/data/raw/category_hierarchy_map.json"
        if not os.path.exists(hierarchy_file):
            # Fallback to relative path
            hierarchy_file = os.path.join(os.getcwd(), "data", "raw", "category_hierarchy_map.json")

        if os.path.exists(hierarchy_file):
            with open(hierarchy_file, encoding="utf-8") as f:
                _hierarchy_map_cache = json.load(f)
                logging.info(
                    f"✅ Loaded category hierarchy map: {len(_hierarchy_map_cache)} categories"
                )
                return _hierarchy_map_cache
        else:
            logging.warning(f"⚠️  Hierarchy map not found at {hierarchy_file}")
            return {}
    except Exception as e:
        logging.error(f"❌ Error loading hierarchy map: {e}")
        return {}


# Đường dẫn cơ sở của file DAG
dag_file_dir = os.path.dirname(__file__)

# Các đường dẫn có thể chứa module crawl
possible_paths = [
    os.path.abspath(os.path.join(dag_file_dir, "..", "..", "src", "pipelines", "crawl")),
    os.path.abspath(os.path.join(dag_file_dir, "..", "src", "pipelines", "crawl")),
    "/opt/airflow/src/pipelines/crawl",
]

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
            crawl_product_detail_async,
            crawl_product_detail_with_driver,
            crawl_product_detail_with_selenium,
            extract_product_detail,
        )

        SeleniumDriverPool = None  # Không có trong crawl_products_detail, sẽ import từ utils
    except ImportError as e:
        raise ImportError(
            f"Không tìm thấy module crawl_products_detail.\n"
            f"Path: {crawl_products_detail_path}\n"
            f"Lỗi gốc: {e}"
        ) from e

    # Import crawl_category_products từ crawl_products
    try:
        from crawl_products import crawl_category_products
    except ImportError as e:
        raise ImportError(
            f"Không tìm thấy hàm crawl_category_products trong crawl_products.\n" f"Lỗi gốc: {e}"
        ) from e

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
    "retries": 3,  # Retry 3 lần
    "retry_delay": timedelta(minutes=2),  # Delay 2 phút giữa các retry
    "retry_exponential_backoff": True,  # Exponential backoff
    "max_retry_delay": timedelta(minutes=10),
}

# Cấu hình DAG - Có thể chuyển đổi giữa tự động và thủ công qua Variable
# Đọc schedule mode từ Airflow Variable (mặc định: 'manual' để test)
# Có thể set Variable 'TIKI_DAG_SCHEDULE_MODE' = 'scheduled' để chạy tự động
try:
    schedule_mode = Variable.get("TIKI_DAG_SCHEDULE_MODE", default="manual")
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

# Cấu hình DAG schedule
dag_schedule_config = dag_schedule

# Documentation đơn giản cho DAG
dag_doc_md = "Crawl sản phẩm từ Tiki.vn với Dynamic Task Mapping và Selenium"

DAG_CONFIG = {
    "dag_id": "tiki_crawl_products",
    "description": dag_description,
    "doc_md": dag_doc_md,
    "default_args": DEFAULT_ARGS,
    "schedule": dag_schedule_config,
    "start_date": datetime(2025, 11, 1),  # Ngày cố định trong quá khứ
    "catchup": False,  # Không chạy lại các task đã bỏ lỡ
    "tags": dag_tags,
    "max_active_runs": 1,  # Chỉ chạy 1 DAG instance tại một thời điểm
    "max_active_tasks": 15,  # Giảm xuống 10 tasks song song để tránh quá tải khi tạo Selenium driver
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
CATEGORIES_TREE_FILE = DATA_DIR / "raw" / "categories_tree.json"
OUTPUT_DIR = DATA_DIR / "raw" / "products"
CACHE_DIR = OUTPUT_DIR / "cache"
DETAIL_CACHE_DIR = OUTPUT_DIR / "detail" / "cache"
OUTPUT_FILE = OUTPUT_DIR / "products.json"
OUTPUT_FILE_WITH_DETAIL = OUTPUT_DIR / "products_with_detail.json"
# Progress tracking cho multi-day crawling
PROGRESS_FILE = OUTPUT_DIR / "crawl_progress.json"

# Asset/Dataset đã được xóa - dependencies được quản lý bằng >> operator

# Tạo thư mục nếu chưa có
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)
DETAIL_CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Thread-safe lock cho atomic writes
write_lock = Lock()

# Khởi tạo resilience patterns
# Circuit breaker cho Tiki API
tiki_circuit_breaker = CircuitBreaker(
    failure_threshold=int(Variable.get("TIKI_CIRCUIT_BREAKER_FAILURE_THRESHOLD", default="5")),
    recovery_timeout=int(Variable.get("TIKI_CIRCUIT_BREAKER_RECOVERY_TIMEOUT", default="60")),
    expected_exception=Exception,
    name="tiki_api",
)

# Dead Letter Queue
try:
    # Thử dùng Redis nếu có
    redis_url = Variable.get("REDIS_URL", default="redis://redis:6379/3")
    tiki_dlq = get_dlq(storage_type="redis", redis_url=redis_url)
except Exception:
    # Fallback về file-based
    try:
        dlq_path = DATA_DIR / "dlq"
        tiki_dlq = get_dlq(storage_type="file", storage_path=str(dlq_path))
    except Exception:
        # Nếu không tạo được, dùng default
        tiki_dlq = get_dlq()

# Graceful Degradation cho Tiki service
service_health = get_service_health()
tiki_degradation = service_health.register_service(
    name="tiki",
    failure_threshold=int(Variable.get("TIKI_DEGRADATION_FAILURE_THRESHOLD", default="3")),
    recovery_threshold=int(Variable.get("TIKI_DEGRADATION_RECOVERY_THRESHOLD", default="5")),
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

# IMPORTANT: Load config.py TRƯỚC để đảm bảo .env được load
# Điều này đảm bảo các biến môi trường từ .env được set trước khi các module khác import
if config_path and os.path.exists(config_path):
    try:
        import importlib.util

        spec = importlib.util.spec_from_file_location("common.config", config_path)
        if spec is not None and spec.loader is not None:
            config_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(config_module)
            # Config module sẽ tự động load .env khi được import
            import warnings

            warnings.warn(
                f"✅ Đã load common.config từ {config_path}, .env sẽ được load tự động",
                stacklevel=2,
            )
    except Exception as e:
        import warnings

        warnings.warn(f"⚠️  Không thể load common.config: {e}", stacklevel=2)

# Import DataAggregator từ common/analytics/
if analytics_path and os.path.exists(analytics_path):
    try:
        import importlib.util

        spec = importlib.util.spec_from_file_location("common.analytics.aggregator", analytics_path)
        if spec is not None and spec.loader is not None:
            aggregator_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(aggregator_module)
            DataAggregator = aggregator_module.DataAggregator
        else:
            DataAggregator = None
    except Exception as e:
        import warnings

        warnings.warn(f"Không thể import common.analytics.aggregator module: {e}", stacklevel=2)
        DataAggregator = None
else:
    DataAggregator = None

# Import AISummarizer từ common/ai/
if ai_path and os.path.exists(ai_path):
    try:
        import importlib.util

        spec = importlib.util.spec_from_file_location("common.ai.summarizer", ai_path)
        if spec is not None and spec.loader is not None:
            ai_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(ai_module)
            AISummarizer = ai_module.AISummarizer
        else:
            AISummarizer = None
    except Exception as e:
        import warnings

        warnings.warn(f"Không thể import common.ai.summarizer module: {e}", stacklevel=2)
        AISummarizer = None
else:
    AISummarizer = None

# Import DiscordNotifier từ common/notifications/
if notifications_path and os.path.exists(notifications_path):
    try:
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "common.notifications.discord", notifications_path
        )
        if spec is not None and spec.loader is not None:
            notifications_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(notifications_module)
            DiscordNotifier = notifications_module.DiscordNotifier
        else:
            DiscordNotifier = None
    except Exception as e:
        import warnings

        warnings.warn(f"Không thể import common.notifications.discord module: {e}", stacklevel=2)
        DiscordNotifier = None
else:
    DiscordNotifier = None


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


def _fix_sys_path_for_pipelines_import(logger=None):
    """
    Sửa sys.path và sys.modules để đảm bảo pipelines có thể được import đúng cách.
    Xóa các đường dẫn con như /opt/airflow/src/pipelines khỏi sys.path,
    xóa các fake modules khỏi sys.modules, và chỉ giữ lại /opt/airflow/src.
    """

    if logger is None:
        logger = logging.getLogger("airflow.task")

    # Xóa các fake modules khỏi sys.modules (quan trọng!)
    # Các fake modules này được tạo ở đầu file và gây lỗi 'pipelines' is not a package
    modules_to_remove = [
        module_name
        for module_name in list(sys.modules.keys())
        if module_name.startswith("pipelines")
    ]

    for module_name in modules_to_remove:
        del sys.modules[module_name]
        if logger:
            logger.info(f"🗑️  Đã xóa fake module khỏi sys.modules: {module_name}")

    # Xóa các đường dẫn con khỏi sys.path (gây lỗi 'pipelines' is not a package)
    paths_to_remove = []
    for path in sys.path:
        # Xóa các đường dẫn như /opt/airflow/src/pipelines hoặc /opt/airflow/src/pipelines/crawl
        normalized_path = path.replace("\\", "/")
        if normalized_path.endswith("/pipelines") or normalized_path.endswith("/pipelines/crawl"):
            paths_to_remove.append(path)

    for path in paths_to_remove:
        if path in sys.path:
            sys.path.remove(path)
            if logger:
                logger.info(f"🗑️  Đã xóa đường dẫn sai khỏi sys.path: {path}")

    # Đảm bảo /opt/airflow/src có trong sys.path
    possible_src_paths = [
        "/opt/airflow/src",  # Docker default path
        os.path.abspath(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src")
        ),  # Local dev
    ]

    for src_path in possible_src_paths:
        if os.path.exists(src_path) and os.path.isdir(src_path):
            if src_path not in sys.path:
                sys.path.insert(0, src_path)
                if logger:
                    logger.info(f"✅ Đã thêm vào sys.path: {src_path}")
            return src_path

    return None


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
            min_level = int(Variable.get("TIKI_MIN_CATEGORY_LEVEL", default="2"))
            max_level = int(Variable.get("TIKI_MAX_CATEGORY_LEVEL", default="4"))
            categories = [
                cat for cat in categories if min_level <= cat.get("level", 0) <= max_level
            ]
            logger.info(f"✓ Sau khi lọc level {min_level}-{max_level}: {len(categories)} danh mục")
        except Exception as e:
            logger.warning(f"Không thể lọc theo level: {e}")

        # Giới hạn số danh mục nếu cần (để test)
        try:
            max_categories = int(Variable.get("TIKI_MAX_CATEGORIES", default="0"))
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
        # Kiểm tra graceful degradation
        if tiki_degradation.should_skip():
            result["error"] = "Service đang ở trạng thái FAILED, skip crawl"
            result["status"] = "degraded"
            logger.warning(f"⚠️  Service degraded, skip category {category_name}")
            return result

        # Lấy cấu hình từ Airflow Variables
        max_pages = int(
            Variable.get("TIKI_MAX_PAGES_PER_CATEGORY", default="20")
        )  # Mặc định 20 trang để tránh timeout
        use_selenium = Variable.get("TIKI_USE_SELENIUM", default="false").lower() == "true"
        timeout = int(Variable.get("TIKI_CRAWL_TIMEOUT", default="300"))  # 5 phút mặc định
        rate_limit_delay = float(
            Variable.get("TIKI_RATE_LIMIT_DELAY", default="1.0")
        )  # Delay 1s giữa các request

        # Rate limiting: delay trước khi crawl
        if rate_limit_delay > 0:
            time.sleep(rate_limit_delay)

        # Crawl với timeout và circuit breaker
        start_time = time.time()

        def _crawl_with_params():
            """Wrapper function để gọi với circuit breaker"""
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
                # Với batch processing, mỗi phần tử có thể là list of results (từ 1 batch)
                for result in all_results:
                    # Check if result is a list (batch result) hoặc dict (single category result)
                    if isinstance(result, list):
                        # Batch result - flatten it
                        for single_result in result:
                            if single_result and isinstance(single_result, dict):
                                if single_result.get("status") == "success":
                                    stats["success_categories"] += 1
                                    products = single_result.get("products", [])
                                    all_products.extend(products)
                                    stats["total_products"] += len(products)
                                elif single_result.get("status") == "timeout":
                                    stats["timeout_categories"] += 1
                                    logger.warning(
                                        f"⏱️  Category {single_result.get('category_name')} timeout"
                                    )
                                else:
                                    stats["failed_categories"] += 1
                                    logger.warning(
                                        f"❌ Category {single_result.get('category_name')} failed: {single_result.get('error')}"
                                    )
                    elif result and isinstance(result, dict):
                        # Single category result
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
                # Try fetching individual map_index results
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
            # Get merge result from upstream task
        except Exception as e:
            logger.warning(f"Không lấy được từ 'process_and_save.merge_products': {e}")

        # Cách 2: Thử không có prefix
        if not merge_result:
            try:
                merge_result = ti.xcom_pull(task_ids="merge_products")
                # Fallback to merge_products without prefix
            except Exception as e:
                logger.warning(f"Không lấy được từ 'merge_products': {e}")

        if not merge_result:
            raise ValueError("Không tìm thấy kết quả merge từ XCom")

        products = merge_result.get("products", [])
        stats = merge_result.get("stats", {})

        logger.info(f"Đang lưu {len(products)} sản phẩm...")

        # Batch processing cho dữ liệu lớn
        batch_size = int(Variable.get("TIKI_SAVE_BATCH_SIZE", default="10000"))

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
        db_hits = 0  # Products đã có trong DB

        # Lấy cấu hình cho multi-day crawling
        # Tính toán: 500 products ~ 52.75 phút -> 280 products ~ 30 phút
        products_per_day = int(
            Variable.get("TIKI_PRODUCTS_PER_DAY", default="500")
        )  # Mặc định 280 products/ngày (~30 phút)
        max_products = int(
            Variable.get("TIKI_MAX_PRODUCTS_FOR_DETAIL", default="0")
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
                db_host = Variable.get(
                    "POSTGRES_HOST", default=os.getenv("POSTGRES_HOST", "postgres")
                )
                db_port = int(
                    Variable.get("POSTGRES_PORT", default=os.getenv("POSTGRES_PORT", "5432"))
                )
                db_name = Variable.get(
                    "POSTGRES_DB", default=os.getenv("POSTGRES_DB", "crawl_data")
                )
                db_user = Variable.get(
                    "POSTGRES_USER", default=os.getenv("POSTGRES_USER", "postgres")
                )
                # trufflehog:ignore - Fallback for development, production uses Airflow Variables
                db_password = Variable.get(
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
                    logger.info(
                        "   (chỉ skip products có price, sales_count VÀ brand - detail đầy đủ)"
                    )
                    with storage.get_connection() as conn:
                        with conn.cursor() as cur:
                            # Chia nhỏ query nếu có quá nhiều product_ids
                            # Chỉ lấy products có price, sales_count VÀ brand (detail đầy đủ)
                            # Products không có brand sẽ được crawl lại
                            for i in range(0, len(product_ids_to_check), 1000):
                                batch_ids = product_ids_to_check[i : i + 1000]
                                placeholders = ",".join(["%s"] * len(batch_ids))
                                cur.execute(
                                    f"""
                                    SELECT product_id
                                    FROM products
                                    WHERE product_id IN ({placeholders})
                                      AND price IS NOT NULL
                                      AND sales_count IS NOT NULL
                                      AND brand IS NOT NULL
                                      AND brand != ''
                                      AND seller_name IS NOT NULL
                                      AND seller_name != ''
                                    """,
                                    batch_ids,
                                )
                                existing_product_ids_in_db.update(row[0] for row in cur.fetchall())

                    logger.info(
                        f"✅ Tìm thấy {len(existing_product_ids_in_db)} products đã có detail đầy đủ trong database"
                    )
                    logger.info("   (có price, sales_count VÀ brand - sẽ skip crawl lại)")
                    logger.info(
                        "   💡 Products không có brand sẽ được crawl lại để lấy đầy đủ thông tin"
                    )
                    storage.close()
        except Exception as e:
            logger.warning(f"⚠️  Không thể kiểm tra database: {e}")
            logger.info("   Sẽ tiếp tục với cache và progress file")

        # Bắt đầu từ index đã crawl
        start_index = progress["last_crawled_index"]

        # Kiểm tra nếu start_index vượt quá số lượng products hiện tại
        # (có thể do test mode giới hạn số lượng products)
        if start_index >= len(products):
            logger.warning("=" * 70)
            logger.warning("⚠️  RESET PROGRESS INDEX!")
            logger.warning(f"   - Progress index: {start_index}")
            logger.warning(f"   - Số products hiện tại: {len(products)}")
            logger.warning("   - Index vượt quá số lượng products")
            logger.warning("   - Có thể do test mode giới hạn số lượng products")
            logger.warning("   - Reset về index 0 để crawl lại từ đầu")
            logger.warning("=" * 70)
            start_index = 0
            # Reset progress để tránh nhầm lẫn
            progress["last_crawled_index"] = 0
            progress["total_crawled"] = 0
            # Giữ lại crawled_product_ids để tránh crawl lại products đã có

        products_to_check = products[start_index:]

        logger.info(
            f"🔄 Bắt đầu từ index {start_index} (đã crawl {progress['total_crawled']} products, tổng products: {len(products)})"
        )

        # Tối ưu: Duyệt tất cả products để tìm products chưa có trong DB
        # Thay vì dừng khi đạt max_products nhưng toàn bộ là skip
        skipped_count = 0
        max_skipped_before_stop = 100  # Dừng nếu skip liên tiếp 100 products

        for idx, product in enumerate(products_to_check):
            product_id = product.get("product_id")
            product_url = product.get("url")

            if not product_id or not product_url:
                continue

            # Kiểm tra xem đã crawl chưa (từ progress)
            if product_id in progress["crawled_product_ids"]:
                already_crawled += 1
                skipped_count += 1
                continue

            # Kiểm tra xem đã có trong database chưa (với detail đầy đủ)
            # existing_product_ids_in_db chỉ chứa products có price và sales_count
            if product_id in existing_product_ids_in_db:
                # Đã có trong DB với detail đầy đủ (có price và sales_count)
                # → Skip crawl lại
                db_hits += 1
                progress["crawled_product_ids"].add(product_id)
                already_crawled += 1
                skipped_count += 1
                continue

            # Kiểm tra cache với Redis (thay vì file cache)
            cache_hit = False

            if redis_cache:
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

            # Nếu chưa có valid cache, thêm vào danh sách crawl
            if not cache_hit:
                products_to_crawl.append(
                    {
                        "product_id": product_id,
                        "url": product_url,
                        "name": product.get("name", ""),
                        "product": product,  # Giữ nguyên product data
                        "index": start_index + idx,  # Lưu index để track progress
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

            # Dừng nếu skip quá nhiều products liên tiếp (có thể đã hết products mới)
            if skipped_count >= max_skipped_before_stop:
                logger.info(
                    f"⚠️  Đã skip {skipped_count} products liên tiếp, có thể đã hết products mới"
                )
                logger.info(f"   - Đã tìm được {len(products_to_crawl)} products để crawl")
                break

        logger.info("=" * 70)
        logger.info("📊 THỐNG KÊ PREPARE PRODUCTS FOR DETAIL")
        logger.info("=" * 70)
        logger.info(f"📦 Tổng products đầu vào: {len(products)}")
        logger.info(f"✅ Products cần crawl hôm nay: {len(products_to_crawl)}")

        # Cache hit rate analytics
        total_checked = cache_hits + db_hits + (already_crawled - db_hits - cache_hits)
        if total_checked > 0:
            cache_hit_rate = (cache_hits / total_checked) * 100
        else:
            cache_hit_rate = 0.0

        logger.info(f"🔥 Cache hits (Redis - có data hợp lệ): {cache_hits}")
        logger.info(f"💾 DB hits (đã có trong DB): {db_hits}")
        logger.info(f"✓ Đã crawl trước đó (từ progress): {already_crawled - db_hits - cache_hits}")
        logger.info(f"📈 Tổng đã kiểm tra: {total_checked}")
        logger.info(f"📊 **CACHE HIT RATE: {cache_hit_rate:.1f}%** ← TARGET: 60-80%")
        logger.info(f"📈 Tổng đã crawl toàn bộ: {progress['total_crawled'] + already_crawled}")
        logger.info(
            f"📉 Còn lại chưa crawl: {len(products) - (progress['total_crawled'] + already_crawled + len(products_to_crawl))}"
        )
        logger.info("=" * 70)

        if len(products_to_crawl) == 0:
            logger.warning("=" * 70)
            logger.warning("⚠️  KHÔNG CÓ PRODUCTS NÀO CẦN CRAWL DETAIL!")
            logger.warning("=" * 70)
            logger.warning("💡 Lý do:")
            if already_crawled > 0:
                logger.warning(
                    f"   - Đã có trong progress: {already_crawled - db_hits - cache_hits} products"
                )
            if cache_hits > 0:
                logger.warning(
                    f"   - Đã có trong cache (có price và sales_count): {cache_hits} products"
                )
            if db_hits > 0:
                logger.warning(
                    f"   - Đã có trong database (có price và sales_count): {db_hits} products"
                )
            logger.warning("=" * 70)
            logger.warning("💡 Để force crawl lại, bạn có thể:")
            logger.warning("   1. Xóa progress file: data/processed/detail_crawl_progress.json")
            logger.warning("   2. Xóa cache files trong: data/raw/products/detail/cache/")
            logger.warning("   3. Xóa products trong database (nếu muốn crawl lại)")
            logger.warning("=" * 70)

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
        logger.error("=" * 70)
        logger.error("❌ KHÔNG TÌM THẤY PRODUCT_BATCH TRONG CONTEXT!")
        logger.error("=" * 70)
        logger.error("💡 Debug info:")
        logger.error(f"   - Context keys: {list(context.keys())}")
        if ti:
            logger.error(f"   - ti.op_kwargs: {getattr(ti, 'op_kwargs', 'N/A')}")
        logger.error("=" * 70)
        return []

    # Validate product_batch
    if not isinstance(product_batch, list):
        logger.error("=" * 70)
        logger.error(f"❌ PRODUCT_BATCH KHÔNG PHẢI LIST: {type(product_batch)}")
        logger.error(f"   - Value: {product_batch}")
        logger.error("=" * 70)
        return []

    if len(product_batch) == 0:
        logger.warning("=" * 70)
        logger.warning(f"⚠️  BATCH {batch_index} RỖNG - Không có products nào")
        logger.warning("=" * 70)
        return []

    logger.info("=" * 70)
    logger.info(f"📦 BATCH {batch_index}: Crawl {len(product_batch)} products")
    logger.info(f"   - Product IDs: {[p.get('product_id', 'unknown') for p in product_batch[:5]]}")
    if len(product_batch) > 5:
        logger.info(f"   - ... và {len(product_batch) - 5} products nữa")
    logger.info("=" * 70)

    results = []

    try:
        import asyncio

        # Import SeleniumDriverPool từ utils nếu chưa có (cho task scope)
        global SeleniumDriverPool
        _SeleniumDriverPool = SeleniumDriverPool
        if _SeleniumDriverPool is None:
            # Fallback: thử import từ utils trực tiếp nếu không thành công
            try:
                _fix_sys_path_for_pipelines_import(logger)
                # utils là file (.py), không phải package
                import importlib.util

                src_path = Path("/opt/airflow/src")
                utils_path = src_path / "pipelines" / "crawl" / "utils.py"
                if utils_path.exists():
                    spec = importlib.util.spec_from_file_location(
                        "crawl_utils_fallback", str(utils_path)
                    )
                    if spec and spec.loader:
                        crawl_utils = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(crawl_utils)
                        _SeleniumDriverPool = getattr(crawl_utils, "SeleniumDriverPool", None)
                if _SeleniumDriverPool:
                    logger.info("✅ Imported SeleniumDriverPool from utils.py file")
                else:
                    raise ImportError("Không tìm thấy SeleniumDriverPool trong utils.py")
            except Exception as e:
                logger.error(f"⚠️  Không thể import SeleniumDriverPool từ pipelines: {e}")
                raise ImportError("SeleniumDriverPool chưa được import từ utils module") from e

        # Sử dụng hàm đã được import ở đầu file
        # crawl_product_detail_async và SeleniumDriverPool đã được import ở đầu file
        pool_size = int(
            Variable.get("TIKI_DETAIL_POOL_SIZE", default="15")
        )  # Tối ưu: tăng từ 5 -> 15
        driver_pool = _SeleniumDriverPool(
            pool_size=pool_size, headless=True, timeout=120
        )  # Tối ưu: tăng từ 60 -> 120s để trang load đầy đủ

        # Tạo event loop trước
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        # Session sẽ được tạo bên trong async function (cần async context)
        session = None

        async def crawl_single_async(product_info: dict) -> dict[str, Any]:
            """Crawl một product với async"""
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
                    detail = await crawl_product_detail_async(
                        product_url, session=session, use_selenium_fallback=True, verbose=False
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
        rate_limit_delay = float(Variable.get("TIKI_DETAIL_RATE_LIMIT_DELAY", default="1.5"))

        # Tạo semaphore để limit concurrent tasks (tối ưu throughput)
        max_concurrent = int(Variable.get("TIKI_DETAIL_MAX_CONCURRENT_TASKS", default="12"))
        semaphore = asyncio.Semaphore(max_concurrent)

        async def bounded_task(task_coro):
            """Wrap task để respect semaphore limit"""
            async with semaphore:
                return await task_coro

        # Tạo tasks với rate limiting: stagger start times
        async def crawl_batch_parallel():
            """Crawl batch với parallel processing và rate limiting"""
            # Tạo session ngay lập tức trong async context (trước khi tạo tasks)
            # Đảm bảo session được tạo trong async context có event loop
            nonlocal session
            if session is None:
                try:
                    import aiohttp

                    timeout = aiohttp.ClientTimeout(total=30)
                    # Tạo connector với optimized pooling
                    connector = aiohttp.TCPConnector(
                        limit=50,  # Tổng connection limit
                        limit_per_host=10,  # Connection limit per host
                        ttl_dns_cache=300,  # Cache DNS query 5 phút
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
        driver_pool.cleanup()

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

    logger.info("=" * 70)
    logger.info(f"🔍 TASK: Crawl Product Detail - {product_name}")
    logger.info(f"🆔 Product ID: {product_id}")
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
    # Kiểm tra xem có force refresh không (từ Airflow Variable)
    force_refresh = Variable.get("TIKI_FORCE_REFRESH_CACHE", default="false").lower() == "true"

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
                    # Kiểm tra cache có đầy đủ không: cần có price và sales_count
                    has_price = cached_detail.get("price", {}).get("current_price")
                    has_sales_count = cached_detail.get("sales_count") is not None

                    # Nếu đã có detail đầy đủ (có price và sales_count), dùng cache
                    if has_price and has_sales_count:
                        logger.info("=" * 70)
                        logger.info(f"✅ SKIP CRAWL - Redis Cache Hit cho product {product_id}")
                        logger.info(f"   - Có price: {has_price}")
                        logger.info(f"   - Có sales_count: {has_sales_count}")
                        logger.info("   - Sử dụng cache, không cần crawl lại")
                        logger.info("=" * 70)
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

        # Fallback: Kiểm tra file cache nếu Redis không available hoặc không có cache
        if not force_refresh:
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
                            logger.info("=" * 70)
                            logger.info(f"✅ SKIP CRAWL - File Cache Hit cho product {product_id}")
                            logger.info(f"   - Có price: {has_price}")
                            logger.info(f"   - Có sales_count: {has_sales_count}")
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
            Variable.get("TIKI_DETAIL_RATE_LIMIT_DELAY", default="1.5")
        )  # Delay 1.5s cho detail (tối ưu từ 2.0s)
        timeout = int(
            Variable.get("TIKI_DETAIL_CRAWL_TIMEOUT", default="180")
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
                """Wrapper function để gọi với circuit breaker"""
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
        batch_size = 10
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
                    pass

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
                    f"💓 [Heartbeat] Đang xử lý batch {batch_num}/{total_batches} (index {start_idx}-{end_idx-1})..."
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
                        pass

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
                    pass

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
        valid_results = 0
        error_details = {}  # Thống kê chi tiết các loại lỗi
        failed_products = []  # Danh sách products bị fail để phân tích

        for detail_result in all_detail_results:
            if detail_result and isinstance(detail_result, dict):
                product_id = detail_result.get("product_id")
                if product_id:
                    detail_dict[product_id] = detail_result
                    valid_results += 1
                    status = detail_result.get("status", "failed")
                    error = detail_result.get("error")

                    # Đếm số lượng products được crawl (tất cả các status trừ "not_crawled")
                    if status in [
                        "success",
                        "cached",
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
                    elif status == "cached":
                        stats["cached"] += 1
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

                    # CRITICAL: Lọc bỏ products có brand null/empty
                    # Brand thiếu thường dẫn đến nhiều trường khác cũng thiếu
                    # Seller có thể là "Unknown" - vẫn lưu lại
                    # Những products này sẽ được crawl lại trong lần chạy tiếp theo
                    brand = product_with_detail.get("brand")

                    # Only skip if BRAND is missing/empty (seller can be "Unknown")
                    if not brand or (isinstance(brand, str) and not brand.strip()):
                        logger.warning(
                            f"⚠️  Product {product_id} ({product_with_detail.get('name', 'Unknown')[:50]}) "
                            f"có brand null/empty, sẽ bỏ qua để crawl lại lần sau"
                        )
                        products_no_brand += 1
                        products_failed += 1
                        continue

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


def transform_products(**context) -> dict[str, Any]:
    """
    Task: Transform dữ liệu sản phẩm (normalize, validate, compute fields)

    Returns:
        Dict: Kết quả transform với transformed products và stats
    """
    logger = get_logger(context)
    logger.info("=" * 70)
    logger.info("🔄 TASK: Transform Products")
    logger.info("=" * 70)

    try:
        ti = context["ti"]

        # Lấy file từ save_products_with_detail
        output_file = None
        try:
            output_file = ti.xcom_pull(task_ids="crawl_product_details.save_products_with_detail")
        except Exception:
            try:
                output_file = ti.xcom_pull(task_ids="save_products_with_detail")
            except Exception:
                pass

        if not output_file:
            output_file = str(OUTPUT_FILE_WITH_DETAIL)

        if not os.path.exists(output_file):
            raise FileNotFoundError(f"Không tìm thấy file: {output_file}")

        logger.info(f"📂 Đang đọc file: {output_file}")

        # Đọc products từ file
        with open(output_file, encoding="utf-8") as f:
            data = json.load(f)

        products = data.get("products", [])
        stats = data.get("stats", {})
        logger.info(f"📊 Tổng số products trong file: {len(products)}")

        # Log thông tin về crawl detail nếu có
        crawled_count = stats.get("crawled_count", 0)
        if crawled_count > 0:
            logger.info(f"🔄 Products được crawl detail: {crawled_count}")
            logger.info(f"✅ Products có detail (success): {stats.get('with_detail', 0)}")

        # Bổ sung category_url và category_id trước khi transform
        logger.info("🔗 Đang bổ sung category_url và category_id...")

        # Bước 1: Load category_url mapping từ products.json (nếu có)
        category_url_mapping = {}  # product_id -> category_url
        products_file = OUTPUT_DIR / "products.json"
        if products_file.exists():
            try:
                logger.info(f"📖 Đang đọc category_url mapping từ: {products_file}")
                with open(products_file, encoding="utf-8") as f:
                    products_data = json.load(f)

                products_list = []
                if isinstance(products_data, list):
                    products_list = products_data
                elif isinstance(products_data, dict):
                    if "products" in products_data:
                        products_list = products_data["products"]
                    elif "data" in products_data and isinstance(products_data["data"], dict):
                        products_list = products_data["data"].get("products", [])

                for product in products_list:
                    product_id = product.get("product_id")
                    category_url = product.get("category_url")
                    if product_id and category_url:
                        category_url_mapping[product_id] = category_url

                logger.info(
                    f"✅ Đã load {len(category_url_mapping)} category_url mappings từ products.json"
                )
            except Exception as e:
                logger.warning(f"⚠️  Lỗi khi đọc products.json: {e}")

        # Bước 2: Import utility để extract category_id
        try:
            # Tìm đường dẫn utils module
            utils_paths = [
                "/opt/airflow/src/pipelines/crawl/utils.py",
                os.path.abspath(
                    os.path.join(dag_file_dir, "..", "..", "src", "pipelines", "crawl", "utils.py")
                ),
                os.path.join(os.getcwd(), "src", "pipelines", "crawl", "utils.py"),
            ]

            utils_path = None
            for path in utils_paths:
                if os.path.exists(path):
                    utils_path = path
                    break

            if utils_path:
                import importlib.util

                spec = importlib.util.spec_from_file_location("crawl_utils", utils_path)
                utils_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(utils_module)
                extract_category_id_from_url = utils_module.extract_category_id_from_url
            else:
                # Fallback: định nghĩa hàm đơn giản
                import re

                def extract_category_id_from_url(url: str) -> str | None:
                    if not url:
                        return None
                    match = re.search(r"/c(\d+)", url)
                    if match:
                        return f"c{match.group(1)}"
                    return None

        except Exception as e:
            logger.warning(f"⚠️  Không thể import extract_category_id_from_url: {e}")
            import re

            def extract_category_id_from_url(url: str) -> str | None:
                if not url:
                    return None
                match = re.search(r"/c(\d+)", url)
                if match:
                    return f"c{match.group(1)}"
                return None

        # Bước 3: Bổ sung category_url, category_id và ENRICH category_path cho products
        updated_count = 0
        category_id_added = 0
        category_path_count = 0
        category_path_enriched = 0

        # Bước 3a: Build category_path lookup từ categories file
        category_path_lookup: dict[str, list] = {}  # category_id -> category_path

        if CATEGORIES_FILE.exists():
            try:
                logger.info(f"📖 Đang load categories từ: {CATEGORIES_FILE}")
                with open(CATEGORIES_FILE, encoding="utf-8") as cf:
                    raw_categories = json.load(cf)

                for cat in raw_categories:
                    cat_id = cat.get("category_id")
                    cat_path = cat.get("category_path")

                    # Chỉ thêm vào lookup nếu có category_id và category_path
                    if cat_id and cat_path:
                        category_path_lookup[cat_id] = cat_path

                logger.info(f"✅ Loaded {len(category_path_lookup)} category_path từ file")
            except Exception as e:
                logger.warning(f"⚠️ Lỗi đọc categories file: {e}")
        else:
            logger.warning(f"⚠️ Categories file không tồn tại: {CATEGORIES_FILE}")

        for product in products:
            product_id = product.get("product_id")

            # Bổ sung category_url nếu chưa có
            if not product.get("category_url") and product_id in category_url_mapping:
                product["category_url"] = category_url_mapping[product_id]
                updated_count += 1

            # Extract category_id từ category_url nếu có
            category_url = product.get("category_url")
            if category_url and not product.get("category_id"):
                category_id = extract_category_id_from_url(category_url)
                if category_id:
                    product["category_id"] = category_id
                    category_id_added += 1

            # Enrich category_path từ lookup map (nếu chưa có)
            if product.get("category_id") and not product.get("category_path"):
                cat_id = product["category_id"]
                if cat_id in category_path_lookup:
                    product["category_path"] = category_path_lookup[cat_id]
                    category_path_enriched += 1

            # Đảm bảo category_path được giữ lại
            if product.get("category_path"):
                category_path_count += 1

        if updated_count > 0:
            logger.info(f"✅ Đã bổ sung category_url cho {updated_count} products")
        if category_id_added > 0:
            logger.info(f"✅ Đã bổ sung category_id cho {category_id_added} products")
        if category_path_enriched > 0:
            logger.info(f"✅ Đã enrich category_path cho {category_path_enriched} products")
        if category_path_count > 0:
            logger.info(f"✅ Tổng products có category_path: {category_path_count}/{len(products)}")

        # Import DataTransformer
        try:
            # Tìm đường dẫn transform module
            transform_paths = [
                "/opt/airflow/src/pipelines/transform/transformer.py",
                os.path.abspath(
                    os.path.join(
                        dag_file_dir, "..", "..", "src", "pipelines", "transform", "transformer.py"
                    )
                ),
                os.path.join(os.getcwd(), "src", "pipelines", "transform", "transformer.py"),
            ]

            transformer_path = None
            for path in transform_paths:
                if os.path.exists(path):
                    transformer_path = path
                    break

            if not transformer_path:
                raise ImportError("Không tìm thấy transformer.py")

            import importlib.util

            spec = importlib.util.spec_from_file_location("transformer", transformer_path)
            transformer_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(transformer_module)
            DataTransformer = transformer_module.DataTransformer

            # Transform products
            transformer = DataTransformer(
                strict_validation=False, remove_invalid=True, normalize_fields=True
            )

            transformed_products, transform_stats = transformer.transform_products(
                products, validate=True
            )

            logger.info("=" * 70)
            logger.info("📊 TRANSFORM RESULTS")
            logger.info("=" * 70)
            logger.info(f"✅ Valid products: {transform_stats['valid_products']}")
            logger.info(f"❌ Invalid products: {transform_stats['invalid_products']}")
            logger.info(f"🔄 Duplicates removed: {transform_stats['duplicates_removed']}")
            logger.info("=" * 70)

            # Lưu transformed products vào file
            processed_dir = DATA_DIR / "processed"
            processed_dir.mkdir(parents=True, exist_ok=True)
            transformed_file = processed_dir / "products_transformed.json"

            output_data = {
                "transformed_at": datetime.now().isoformat(),
                "source_file": output_file,
                "total_products": len(products),
                "transform_stats": transform_stats,
                "products": transformed_products,
            }

            atomic_write_file(str(transformed_file), output_data, **context)
            logger.info(
                f"✅ Đã lưu {len(transformed_products)} transformed products vào: {transformed_file}"
            )

            return {
                "transformed_file": str(transformed_file),
                "transformed_count": len(transformed_products),
                "transform_stats": transform_stats,
            }

        except ImportError as e:
            logger.error(f"❌ Không thể import DataTransformer: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"❌ Lỗi khi transform products: {e}", exc_info=True)
            raise

    except Exception as e:
        logger.error(f"❌ Lỗi trong transform_products task: {e}", exc_info=True)
        raise


def _import_postgres_storage():
    """
    Helper function để import PostgresStorage với fallback logic
    Hỗ trợ cả môi trường Airflow (importlib) và môi trường bình thường

    Returns:
        PostgresStorage class hoặc None nếu không thể import
    """
    try:
        # Thử import từ __init__.py của storage module
        from pipelines.crawl.storage import PostgresStorage

        return PostgresStorage
    except ImportError:
        try:
            # Thử import trực tiếp từ file
            from pipelines.crawl.storage.postgres_storage import PostgresStorage

            return PostgresStorage
        except ImportError:
            try:
                import importlib.util
                from pathlib import Path

                # Tìm đường dẫn đến postgres_storage.py
                possible_paths = [
                    # Từ /opt/airflow/src (Docker default - ưu tiên)
                    Path("/opt/airflow/src/pipelines/crawl/storage/postgres_storage.py"),
                    # Từ dag_file_dir
                    Path(dag_file_dir).parent.parent
                    / "src"
                    / "pipelines"
                    / "crawl"
                    / "storage"
                    / "postgres_storage.py",
                    # Từ current working directory
                    Path(os.getcwd())
                    / "src"
                    / "pipelines"
                    / "crawl"
                    / "storage"
                    / "postgres_storage.py",
                    # Từ workspace root
                    Path("/workspace/src/pipelines/crawl/storage/postgres_storage.py"),
                ]

                postgres_storage_path = None
                for path in possible_paths:
                    if path.exists() and path.is_file():
                        postgres_storage_path = path
                        break

                if postgres_storage_path:
                    # Sử dụng importlib để load trực tiếp từ file
                    spec = importlib.util.spec_from_file_location(
                        "postgres_storage", postgres_storage_path
                    )
                    if spec and spec.loader:
                        postgres_storage_module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(postgres_storage_module)
                        return postgres_storage_module.PostgresStorage

                # Nếu không tìm thấy file, thử thêm src vào path và import absolute
                src_paths = [
                    Path("/opt/airflow/src"),
                    Path(dag_file_dir).parent.parent / "src",
                    Path(os.getcwd()) / "src",
                ]

                for src_path in src_paths:
                    if src_path.exists() and str(src_path) not in sys.path:
                        sys.path.insert(0, str(src_path))
                        try:
                            from pipelines.crawl.storage import PostgresStorage

                            return PostgresStorage
                        except ImportError:
                            try:
                                from pipelines.crawl.storage.postgres_storage import PostgresStorage

                                return PostgresStorage
                            except ImportError:
                                continue

                return None
            except Exception:
                return None


def load_products(**context) -> dict[str, Any]:
    """
    Task: Load dữ liệu đã transform vào database

    Returns:
        Dict: Kết quả load với stats
    """
    logger = get_logger(context)
    logger.info("=" * 70)
    logger.info("💾 TASK: Load Products to Database")
    logger.info("=" * 70)

    try:
        ti = context["ti"]

        # Lấy transformed file từ transform_products task
        transform_result = None
        try:
            transform_result = ti.xcom_pull(task_ids="transform_and_load.transform_products")
        except Exception:
            try:
                transform_result = ti.xcom_pull(task_ids="transform_products")
            except Exception:
                pass

        if not transform_result:
            # Fallback: tìm file transformed
            processed_dir = DATA_DIR / "processed"
            transformed_file = processed_dir / "products_transformed.json"
            if transformed_file.exists():
                transform_result = {"transformed_file": str(transformed_file)}
            else:
                raise ValueError("Không tìm thấy transform result từ XCom hoặc file")

        transformed_file = transform_result.get("transformed_file")
        if not transformed_file or not os.path.exists(transformed_file):
            raise FileNotFoundError(f"Không tìm thấy file transformed: {transformed_file}")

        logger.info(f"📂 Đang đọc transformed file: {transformed_file}")

        # Đọc transformed products
        with open(transformed_file, encoding="utf-8") as f:
            data = json.load(f)

        products = data.get("products", [])
        logger.info(f"📊 Tổng số products để load: {len(products)}")

        # Import DataLoader
        try:
            # Tìm đường dẫn load module
            load_paths = [
                "/opt/airflow/src/pipelines/load/loader.py",
                os.path.abspath(
                    os.path.join(dag_file_dir, "..", "..", "src", "pipelines", "load", "loader.py")
                ),
                os.path.join(os.getcwd(), "src", "pipelines", "load", "loader.py"),
            ]

            loader_path = None
            for path in load_paths:
                if os.path.exists(path):
                    loader_path = path
                    break

            if not loader_path:
                raise ImportError("Không tìm thấy loader.py")

            import importlib.util

            spec = importlib.util.spec_from_file_location("loader", loader_path)
            loader_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(loader_module)
            DataLoader = loader_module.DataLoader

            # Lấy database config từ Airflow Variables hoặc environment variables
            # Ưu tiên: Airflow Variables > Environment Variables > Default
            db_host = Variable.get("POSTGRES_HOST", default=os.getenv("POSTGRES_HOST", "postgres"))
            db_port = int(Variable.get("POSTGRES_PORT", default=os.getenv("POSTGRES_PORT", "5432")))
            db_name = Variable.get("POSTGRES_DB", default=os.getenv("POSTGRES_DB", "crawl_data"))
            db_user = Variable.get("POSTGRES_USER", default=os.getenv("POSTGRES_USER", "postgres"))
            # trufflehog:ignore - Fallback for development, production uses Airflow Variables
            db_password = Variable.get(
                "POSTGRES_PASSWORD", default=os.getenv("POSTGRES_PASSWORD", "postgres")
            )

            # Load vào database
            loader = DataLoader(
                host=db_host,
                port=db_port,
                database=db_name,
                user=db_user,
                password=db_password,
                batch_size=100,
                enable_db=True,
            )

            try:
                # Lưu vào processed directory
                processed_dir = DATA_DIR / "processed"
                processed_dir.mkdir(parents=True, exist_ok=True)
                final_file = processed_dir / "products_final.json"

                # Khởi tạo biến để lưu số lượng products
                count_before = None
                count_after = None
                deleted_no_brand_count = 0

                # CRITICAL: Xóa products có brand hoặc seller null từ database trước khi load
                # Products có brand/seller null thường thiếu nhiều trường khác và sẽ được crawl lại
                try:
                    PostgresStorage = _import_postgres_storage()
                    if PostgresStorage is None:
                        raise ImportError("Không thể import PostgresStorage")
                    storage = PostgresStorage(
                        host=db_host,
                        port=db_port,
                        database=db_name,
                        user=db_user,
                        password=db_password,
                    )

                    logger.info("=" * 70)
                    logger.info("🗑️  XÓA PRODUCTS CÓ BRAND HOẶC SELLER NULL KHỎI DATABASE")
                    logger.info("=" * 70)

                    with storage.get_connection() as conn:
                        with conn.cursor() as cur:
                            # Đếm số lượng trước khi xóa
                            cur.execute(
                                "SELECT COUNT(*) FROM products WHERE brand IS NULL OR brand = '' OR seller_name IS NULL OR seller_name = '';"
                            )
                            count_to_delete = cur.fetchone()[0]

                            if count_to_delete > 0:
                                logger.info(
                                    f"🔍 Tìm thấy {count_to_delete} products có brand/seller null/empty"
                                )

                                # Xóa products có brand hoặc seller null/empty
                                cur.execute(
                                    "DELETE FROM products WHERE brand IS NULL OR brand = '' OR seller_name IS NULL OR seller_name = '';"
                                )
                                deleted_no_brand_count = cur.rowcount
                                conn.commit()

                                logger.info(
                                    f"✅ Đã xóa {deleted_no_brand_count} products có brand/seller null/empty"
                                )
                                logger.info(
                                    "💡 Những products này sẽ được crawl lại trong lần chạy tiếp theo"
                                )
                            else:
                                logger.info(
                                    "✓ Không có products nào có brand/seller null/empty cần xóa"
                                )

                    logger.info("=" * 70)

                    # Kiểm tra số lượng products trong DB sau khi xóa
                    with storage.get_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute("SELECT COUNT(*) FROM products;")
                            count_before = cur.fetchone()[0]
                    storage.close()
                    logger.info(f"📊 Số products trong DB sau khi xóa: {count_before}")
                except Exception as e:
                    logger.warning(f"⚠️  Không thể xóa/kiểm tra products trong DB: {e}")
                    count_before = None

                load_stats = loader.load_products(
                    products,
                    save_to_file=str(final_file),
                    upsert=True,  # UPDATE nếu đã tồn tại, INSERT nếu mới
                    validate_before_load=True,
                )

                # Kiểm tra số lượng products trong DB sau khi load
                try:
                    PostgresStorage = _import_postgres_storage()
                    if PostgresStorage is None:
                        raise ImportError("Không thể import PostgresStorage")
                    storage = PostgresStorage(
                        host=db_host,
                        port=db_port,
                        database=db_name,
                        user=db_user,
                        password=db_password,
                    )
                    with storage.get_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute("SELECT COUNT(*) FROM products;")
                            count_after = cur.fetchone()[0]
                    storage.close()
                    logger.info(f"📊 Số products trong DB sau khi load: {count_after}")
                    if count_before is not None:
                        diff = count_after - count_before
                        if diff > 0:
                            logger.info(f"✅ Đã thêm {diff} products mới vào DB")
                        elif diff == 0:
                            logger.info("ℹ️  Không có products mới (chỉ UPDATE các products đã có)")
                        else:
                            logger.warning(
                                f"⚠️  Số lượng products giảm {abs(diff)} (có thể do xóa hoặc lỗi)"
                            )
                except Exception as e:
                    logger.warning(f"⚠️  Không thể kiểm tra số lượng products sau khi load: {e}")
                    count_after = None

                logger.info("=" * 70)
                logger.info("📊 LOAD RESULTS")
                logger.info("=" * 70)
                if deleted_no_brand_count > 0:
                    logger.info(f"🗑️  Deleted (brand null): {deleted_no_brand_count} products")
                logger.info(f"✅ DB loaded: {load_stats['db_loaded']} products")
                if load_stats.get("inserted_count") is not None:
                    logger.info(
                        f"   - INSERT (products mới): {load_stats.get('inserted_count', 0)}"
                    )
                    logger.info(
                        f"   - UPDATE (products đã có): {load_stats.get('updated_count', 0)}"
                    )
                logger.info(f"✅ File loaded: {load_stats['file_loaded']}")
                logger.info(f"❌ Failed: {load_stats['failed_count']}")
                if count_before is not None and count_after is not None:
                    diff = count_after - count_before
                    logger.info(
                        f"📈 DB count: {count_before} → {count_after} (thay đổi: {diff:+d})"
                    )
                    if diff == 0 and load_stats.get("inserted_count", 0) == 0:
                        logger.info("ℹ️  Không có products mới - chỉ UPDATE các products đã có")
                    elif diff > 0:
                        logger.info(f"✅ Đã thêm {diff} products mới vào DB")
                logger.info("=" * 70)
                logger.info(
                    "ℹ️  Lưu ý: Với upsert=True, products đã có sẽ được UPDATE (không tăng số lượng)"
                )
                logger.info(
                    "ℹ️  Chỉ products mới (product_id chưa có) mới được INSERT và tăng số lượng"
                )
                logger.info("=" * 70)

                return {
                    "final_file": str(final_file),
                    "load_stats": load_stats,
                }

            finally:
                loader.close()

        except ImportError as e:
            logger.error(f"❌ Không thể import DataLoader: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"❌ Lỗi khi load products: {e}", exc_info=True)
            raise

    except Exception as e:
        logger.error(f"❌ Lỗi trong load_products task: {e}", exc_info=True)
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

        # Ưu tiên: Lấy từ save_products_with_detail (có detail)
        # Cách 1: Lấy từ task_id với TaskGroup prefix
        try:
            output_file = ti.xcom_pull(task_ids="crawl_product_details.save_products_with_detail")
            logger.info(
                f"Lấy output_file từ 'crawl_product_details.save_products_with_detail': {output_file}"
            )
        except Exception as e:
            logger.warning(
                f"Không lấy được từ 'crawl_product_details.save_products_with_detail': {e}"
            )

        # Cách 2: Thử không có prefix
        if not output_file:
            try:
                output_file = ti.xcom_pull(task_ids="save_products_with_detail")
                logger.debug(f"Output from save_products_with_detail: {output_file}")
            except Exception as e:
                logger.warning(f"Không lấy được từ 'save_products_with_detail': {e}")

        # Fallback: Lấy từ save_products (không có detail) nếu không có file với detail
        if not output_file:
            try:
                output_file = ti.xcom_pull(task_ids="process_and_save.save_products")
                logger.info(
                    f"Lấy output_file từ 'process_and_save.save_products' (fallback): {output_file}"
                )
            except Exception as e:
                logger.warning(f"Không lấy được từ 'process_and_save.save_products': {e}")

        # Cách 3: Thử không có prefix
        if not output_file:
            try:
                output_file = ti.xcom_pull(task_ids="save_products")
                logger.debug(f"Output from save_products (fallback): {output_file}")
            except Exception as e:
                logger.warning(f"Không lấy được từ 'save_products': {e}")

        if not output_file or not os.path.exists(output_file):
            raise FileNotFoundError(f"Không tìm thấy file output: {output_file}")

        logger.info(f"Đang validate file: {output_file}")

        with open(output_file, encoding="utf-8") as f:
            data = json.load(f)

        products = data.get("products", [])
        stats = data.get("stats", {})

        # Validation
        validation_result = {
            "file_exists": True,
            "total_products": len(products),
            "crawled_count": stats.get("crawled_count", 0),  # Số lượng products được crawl detail
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
        logger.info(f"📦 Tổng số products trong file: {validation_result['total_products']}")

        # Log thông tin về crawl detail nếu có
        crawled_count = stats.get("crawled_count", 0)
        if crawled_count > 0:
            logger.info(f"🔄 Products được crawl detail: {crawled_count}")
            logger.info(f"✅ Products có detail (success): {stats.get('with_detail', 0)}")
            if stats.get("timeout", 0) > 0:
                logger.info(f"⏱️  Products timeout: {stats.get('timeout', 0)}")
            if stats.get("failed", 0) > 0:
                logger.info(f"❌ Products failed: {stats.get('failed', 0)}")

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


def aggregate_and_notify(**context) -> dict[str, Any]:
    """
    Task: Tổng hợp dữ liệu với AI và gửi thông báo qua Discord

    Returns:
        Dict: Kết quả tổng hợp và gửi thông báo
    """
    logger = get_logger(context)
    logger.info("=" * 70)
    logger.info("🤖 TASK: Aggregate Data and Send Discord Notification")
    logger.info("=" * 70)

    result = {
        "aggregation_success": False,
        "ai_summary_success": False,
        "discord_notification_success": False,
        "summary": None,
        "ai_summary": None,
    }

    try:
        # Lấy đường dẫn file products_with_detail.json
        output_file = str(OUTPUT_FILE_WITH_DETAIL)

        if not os.path.exists(output_file):
            logger.warning(f"⚠️  File không tồn tại: {output_file}")
            logger.info("   Thử lấy từ XCom...")

            ti = context["ti"]
            try:
                output_file = ti.xcom_pull(
                    task_ids="crawl_product_details.save_products_with_detail"
                )
                logger.info(f"   Lấy từ XCom: {output_file}")
            except Exception:
                try:
                    output_file = ti.xcom_pull(task_ids="save_products_with_detail")
                    logger.info(f"   Lấy từ XCom (không có prefix): {output_file}")
                except Exception as e:
                    logger.warning(f"   Không lấy được từ XCom: {e}")

        if not output_file or not os.path.exists(output_file):
            raise FileNotFoundError(f"Không tìm thấy file output: {output_file}")

        logger.info(f"📊 Đang tổng hợp dữ liệu từ: {output_file}")

        # 1. Tổng hợp dữ liệu
        if DataAggregator is None:
            logger.warning("⚠️  DataAggregator module chưa được import, bỏ qua tổng hợp")
        else:
            try:
                aggregator = DataAggregator(output_file)
                if aggregator.load_data():
                    summary = aggregator.aggregate()
                    result["summary"] = summary
                    result["aggregation_success"] = True
                    logger.info("✅ Tổng hợp dữ liệu thành công")

                    # Log thống kê
                    stats = summary.get("statistics", {})
                    total_products = stats.get("total_products", 0)
                    crawled_count = stats.get("crawled_count", 0)
                    with_detail = stats.get("with_detail", 0)
                    failed = stats.get("failed", 0)
                    timeout = stats.get("timeout", 0)

                    logger.info(f"   📦 Tổng sản phẩm: {total_products}")
                    logger.info(f"   🔄 Products được crawl detail: {crawled_count}")
                    logger.info(f"   ✅ Có chi tiết (success): {with_detail}")
                    logger.info(f"   ❌ Thất bại: {failed}")
                    logger.info(f"   ⏱️  Timeout: {timeout}")

                    # Tính và hiển thị tỷ lệ thành công
                    if crawled_count > 0:
                        success_rate = (with_detail / crawled_count) * 100
                        logger.info(
                            f"   📈 Tỷ lệ thành công: {with_detail}/{crawled_count} ({success_rate:.1f}%)"
                        )
                    else:
                        logger.warning("   ⚠️  Không có products nào được crawl detail")
                else:
                    logger.error("❌ Không thể load dữ liệu để tổng hợp")
            except Exception as e:
                logger.error(f"❌ Lỗi khi tổng hợp dữ liệu: {e}", exc_info=True)

        # 2. Tổng hợp với AI
        if AISummarizer is None:
            logger.warning("⚠️  AISummarizer module chưa được import, bỏ qua tổng hợp AI")
        elif result.get("summary"):
            try:
                summarizer = AISummarizer()
                ai_summary = summarizer.summarize_data(result["summary"])
                if ai_summary:
                    result["ai_summary"] = ai_summary
                    result["ai_summary_success"] = True
                    logger.info("✅ Tổng hợp với AI thành công")
                    logger.info(f"   Độ dài summary: {len(ai_summary)} ký tự")
                else:
                    logger.warning("⚠️  Không nhận được summary từ AI")
            except Exception as e:
                logger.error(f"❌ Lỗi khi tổng hợp với AI: {e}", exc_info=True)

        # 3. Gửi thông báo qua Discord (rút gọn nội dung nhưng giữ lại lỗi chi tiết)
        if DiscordNotifier is None:
            logger.warning("DiscordNotifier module chưa được import, bỏ qua gửi thông báo")
        else:
            try:
                notifier = DiscordNotifier()

                if result.get("summary"):
                    # Lấy stats
                    stats = result["summary"].get("statistics", {})
                    total_products = stats.get("total_products", 0)
                    crawled_count = stats.get("crawled_count", 0)
                    with_detail = stats.get("with_detail", 0)
                    failed = stats.get("failed", 0)
                    timeout = stats.get("timeout", 0)
                    products_saved = stats.get("products_saved", 0)
                    crawled_at = result["summary"].get("metadata", {}).get("crawled_at", "N/A")

                    # Tính màu theo success rate
                    if crawled_count > 0:
                        success_rate = (with_detail / crawled_count) * 100
                        color = (
                            0x00B894
                            if success_rate >= 80
                            else (0xF39C12 if success_rate >= 50 else 0xE74C3C)
                        )
                    else:
                        success_rate = 0
                        color = 0x95A5A6

                    # Fields với error analysis đầy đủ
                    fields = []
                    fields.append({"name": "Total", "value": f"{total_products:,}", "inline": True})
                    fields.append(
                        {"name": "Crawled", "value": f"{crawled_count:,}", "inline": True}
                    )
                    fields.append(
                        {
                            "name": "Success",
                            "value": f"{with_detail:,} ({success_rate:.1f}%)",
                            "inline": True,
                        }
                    )

                    # Thêm error analysis chi tiết
                    if failed > 0 or timeout > 0:
                        total_errors = failed + timeout
                        error_rate = (
                            (total_errors / crawled_count * 100) if crawled_count > 0 else 0
                        )
                        err_info = f"**Total Errors: {total_errors}** ({error_rate:.1f}%)\n"
                        if failed > 0:
                            failed_rate = (failed / crawled_count * 100) if crawled_count > 0 else 0
                            err_info += f"• Failed: {failed} ({failed_rate:.1f}%)\n"
                        if timeout > 0:
                            timeout_rate = (
                                (timeout / crawled_count * 100) if crawled_count > 0 else 0
                            )
                            err_info += f"• Timeout: {timeout} ({timeout_rate:.1f}%)"
                        fields.append(
                            {"name": "Error Analysis", "value": err_info.strip(), "inline": False}
                        )

                    if products_saved:
                        fields.append(
                            {"name": "Saved to DB", "value": f"{products_saved:,}", "inline": True}
                        )

                    # Nội dung rõ ràng
                    content = "Tổng hợp dữ liệu crawl Tiki.vn\n"
                    if crawled_count > 0:
                        content += f"```\nThành công: {success_rate:.1f}% ({with_detail}/{crawled_count})\n```"
                    else:
                        content += "Chưa có sản phẩm được crawl detail."

                    success = notifier.send_message(
                        content=content,
                        title="Tổng hợp dữ liệu Tiki",
                        color=color,
                        fields=fields,
                        footer=f"Crawl lúc: {crawled_at}",
                    )
                    if success:
                        result["discord_notification_success"] = True
                        logger.info("Đã gửi thông báo Discord")
                    else:
                        logger.warning("Không thể gửi thông báo qua Discord")
                else:
                    logger.warning("Không có dữ liệu để gửi thông báo")
            except Exception as e:
                logger.error(f"Lỗi khi gửi thông báo Discord: {e}", exc_info=True)

        logger.info("=" * 70)
        logger.info("📊 KẾT QUẢ TỔNG HỢP VÀ THÔNG BÁO")
        logger.info("=" * 70)
        logger.info(
            f"✅ Tổng hợp dữ liệu: {'Thành công' if result['aggregation_success'] else 'Thất bại'}"
        )
        logger.info(
            f"✅ Tổng hợp AI: {'Thành công' if result['ai_summary_success'] else 'Thất bại'}"
        )
        logger.info(
            f"✅ Gửi Discord: {'Thành công' if result['discord_notification_success'] else 'Thất bại'}"
        )
        logger.info("=" * 70)

        # Performance Summary
        try:
            dag_run = context.get("dag_run")
            if dag_run and dag_run.start_date:
                start_time = dag_run.start_date
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                total_products = result.get("with_detail", 0)  # Use crawled products count

                # Calculate throughput
                throughput = total_products / duration if duration > 0 else 0
                avg_time = duration / total_products if total_products > 0 else 0

                logger.info("=" * 70)
                logger.info("⚡ PERFORMANCE SUMMARY")
                logger.info(f"⏱️  Duration: {duration/60:.1f} min | Products: {total_products}")
                if throughput > 0:
                    logger.info(
                        f"📈 Throughput: {throughput:.2f} products/s | Avg: {avg_time:.1f}s/product"
                    )
                logger.info("=" * 70)

                result["performance"] = {
                    "duration_minutes": round(duration / 60, 2),
                    "total_products": total_products,
                    "throughput": round(throughput, 2),
                    "avg_time_per_product": round(avg_time, 2),
                }
        except Exception as perf_error:
            logger.warning(f"⚠️  Performance summary error: {perf_error}")

        return result

    except Exception as e:
        logger.error(f"❌ Lỗi khi tổng hợp và gửi thông báo: {e}", exc_info=True)
        # Không fail task, chỉ log lỗi
        return result


def health_check_monitoring(**context) -> dict[str, Any]:
    """
    Task: Health check and monitoring

    Thu thập metrics từ CircuitBreaker, PostgreSQL connection pool và các services khác
    để monitoring health của hệ thống.

    Returns:
        Dict: Health check results
    """
    logger = get_logger(context)
    logger.info("=" * 70)
    logger.info("🏥 TASK: Health Check & Monitoring")
    logger.info("=" * 70)

    result = {
        "status": "success",
        "circuit_breaker_state": {},
        "postgres_pool_stats": {},
        "redis_stats": {},
        "timestamp": datetime.now().isoformat(),
    }

    try:
        # 1. Circuit Breaker State
        try:
            circuit_breaker_state = tiki_circuit_breaker.get_state()
            result["circuit_breaker_state"] = circuit_breaker_state
            logger.info("🔌 Circuit Breaker State:")
            logger.info(f"   - State: {circuit_breaker_state.get('state', 'unknown')}")
            logger.info(f"   - Failure count: {circuit_breaker_state.get('failure_count', 0)}")
            logger.info(
                f"   - Last failure: {circuit_breaker_state.get('last_failure_time', 'N/A')}"
            )
        except Exception as e:
            logger.warning(f"⚠️  Không thể lấy circuit breaker state: {e}")
            result["circuit_breaker_state"] = {"error": str(e)}

        # 2. PostgreSQL Connection Pool Stats
        try:
            # Fix import bằng importlib để tránh conflict với pipelines.crawl package
            import importlib.util
            import sys
            from pathlib import Path

            # Add src/ to sys.path nếu chưa có
            src_path = Path("/opt/airflow/src")
            if src_path.exists() and str(src_path) not in sys.path:
                sys.path.insert(0, str(src_path))

            # Import bằng importlib
            postgres_storage_path = (
                src_path / "pipelines" / "crawl" / "storage" / "postgres_storage.py"
            )
            if postgres_storage_path.exists():
                spec = importlib.util.spec_from_file_location(
                    "postgres_storage_module", postgres_storage_path
                )
                if spec and spec.loader:
                    postgres_module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(postgres_module)
                    PostgresStorage = postgres_module.PostgresStorage

                    # Khởi tạo PostgresStorage để lấy pool stats
                    postgres = PostgresStorage()
                    pool_stats = postgres.get_pool_stats()
                    result["postgres_pool_stats"] = pool_stats
                    logger.info("🐘 PostgreSQL Pool Stats:")
                    logger.info(
                        f"   - Min connections: {pool_stats.get('minconn', pool_stats.get('min_conn', 0))}"
                    )
                    logger.info(
                        f"   - Max connections: {pool_stats.get('maxconn', pool_stats.get('pool_size', 0))}"
                    )
                    logger.info(
                        f"   - Active connections: {pool_stats.get('active_connections', 0)}"
                    )
                    logger.info(f"   - Total queries: {pool_stats.get('total_queries', 0)}")

                    # Close connection
                    try:
                        postgres.close()
                    except Exception:
                        pass
                else:
                    raise ImportError("Cannot load spec for postgres_storage")
            else:
                raise FileNotFoundError(f"postgres_storage.py not found at {postgres_storage_path}")

        except Exception as e:
            logger.warning(f"⚠️  Không thể lấy postgres pool stats: {e}")
            result["postgres_pool_stats"] = {"error": str(e)}

        # 3. Redis Stats
        try:
            # Fix import bằng importlib
            import importlib.util
            import sys
            from pathlib import Path

            # Add src/ to sys.path nếu chưa có
            src_path = Path("/opt/airflow/src")
            if src_path.exists() and str(src_path) not in sys.path:
                sys.path.insert(0, str(src_path))

            # Import bằng importlib
            redis_cache_path = src_path / "pipelines" / "crawl" / "storage" / "redis_cache.py"
            if redis_cache_path.exists():
                spec = importlib.util.spec_from_file_location(
                    "redis_cache_module", redis_cache_path
                )
                if spec and spec.loader:
                    redis_module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(redis_module)
                    get_redis_cache = redis_module.get_redis_cache

                    redis_cache = get_redis_cache("redis://redis:6379/1")
                    if redis_cache:
                        # Redis client info để lấy stats
                        redis_info = redis_cache.client.info()

                        # Tính hit rate từ keyspace_hits và keyspace_misses
                        hits = int(redis_info.get("keyspace_hits", 0))
                        misses = int(redis_info.get("keyspace_misses", 0))
                        total = hits + misses
                        hit_rate = (hits / total * 100) if total > 0 else 0

                        # Count keys với prefix
                        keys_count = redis_cache.client.dbsize()

                        redis_stats = {
                            "keys": keys_count,
                            "used_memory_human": redis_info.get("used_memory_human", "N/A"),
                            "hit_rate": hit_rate,
                            "keyspace_hits": hits,
                            "keyspace_misses": misses,
                            "evicted_keys": int(redis_info.get("evicted_keys", 0)),
                            "connected_clients": int(redis_info.get("connected_clients", 0)),
                        }

                        result["redis_stats"] = redis_stats
                        logger.info("📦 Redis Stats:")
                        logger.info(f"   - Keys: {redis_stats.get('keys', 0)}")
                        logger.info(f"   - Memory: {redis_stats.get('used_memory_human', 'N/A')}")
                        logger.info(f"   - Hit rate: {redis_stats.get('hit_rate', 0):.1f}%")
                        logger.info(f"   - Evicted keys: {redis_stats.get('evicted_keys', 0)}")
                        logger.info(
                            f"   - Connected clients: {redis_stats.get('connected_clients', 0)}"
                        )
                else:
                    raise ImportError("Cannot load spec for redis_cache")
            else:
                raise FileNotFoundError(f"redis_cache.py not found at {redis_cache_path}")

        except Exception as e:
            logger.warning(f"⚠️  Không thể lấy redis stats: {e}")
            result["redis_stats"] = {"error": str(e)}

        # 4. Graceful Degradation Stats
        try:
            # GracefulDegradation có các attributes: current_level, failure_count, success_count
            degradation_state = {
                "level": tiki_degradation.current_level,  # DegradationLevel enum
                "failure_count": tiki_degradation.failure_count,
                "success_count": tiki_degradation.success_count,
                "failure_threshold": tiki_degradation.failure_threshold,
                "recovery_threshold": tiki_degradation.recovery_threshold,
            }

            # Tính success rate
            total = degradation_state["failure_count"] + degradation_state["success_count"]
            success_rate = (degradation_state["success_count"] / total * 100) if total > 0 else 100
            degradation_state["success_rate"] = success_rate

            result["degradation_stats"] = {"tiki_service": degradation_state}
            logger.info("📊 Graceful Degradation Stats:")
            logger.info(f"   - Level: {degradation_state['level']}")
            logger.info(f"   - Success rate: {success_rate:.1f}%")
            logger.info(
                f"   - Failures: {degradation_state['failure_count']}/{degradation_state['failure_threshold']}"
            )
        except Exception as e:
            logger.warning(f"⚠️  Không thể lấy degradation stats: {e}")
            result["degradation_stats"] = {"error": str(e)}

        logger.info("✅ Health check hoàn tất")

        # 5. Gửi alert qua Discord nếu có vấn đề
        try:
            alerts = []

            # Check circuit breaker
            cb_state = result["circuit_breaker_state"]
            if cb_state.get("state") == "open":
                alerts.append(
                    "🔴 **Circuit Breaker OPEN** - Service đang bị block do quá nhiều lỗi!"
                )
            elif cb_state.get("state") == "half_open":
                alerts.append("🟡 **Circuit Breaker HALF-OPEN** - Đang thử khôi phục service...")

            # Check postgres pool
            pg_stats = result["postgres_pool_stats"]
            if not pg_stats.get("error"):
                active = pg_stats.get("active_connections", 0)
                max_conn = pg_stats.get("pool_size", pg_stats.get("maxconn", 100))
                usage_percent = (active / max_conn * 100) if max_conn > 0 else 0

                if usage_percent > 90:
                    alerts.append(
                        f"🔴 **PostgreSQL Pool gần đầy**: {active}/{max_conn} ({usage_percent:.1f}%)"
                    )
                elif usage_percent > 80:
                    alerts.append(
                        f"🟡 **PostgreSQL Pool cao**: {active}/{max_conn} ({usage_percent:.1f}%)"
                    )

            # Check redis hit rate
            redis_stats = result["redis_stats"]
            if not redis_stats.get("error"):
                hit_rate = redis_stats.get("hit_rate", 0)
                if hit_rate < 50 and redis_stats.get("keys", 0) > 100:
                    alerts.append(
                        f"🟡 **Redis hit rate thấp**: {hit_rate:.1f}% - Cache không hiệu quả"
                    )

            logger.info(f"📋 Total alerts to send: {len(alerts)}")

            # Gửi alert nếu có (hoặc trong test mode)
            if alerts:
                try:
                    monitoring_webhook = os.getenv("DISCORD_MONITORING_WEBHOOK_URL")
                    logger.info(f"🔍 Discord webhook configured: {bool(monitoring_webhook)}")

                    if not monitoring_webhook:
                        logger.warning(
                            "⚠️  DISCORD_MONITORING_WEBHOOK_URL chưa được set, skip gửi alert"
                        )
                    else:
                        logger.info("📨 Đang gửi alerts qua Discord...")

                        # Fix import path
                        import sys
                        from pathlib import Path

                        # Add src/ to sys.path nếu chưa có
                        src_path = Path("/opt/airflow/src")
                        if src_path.exists() and str(src_path) not in sys.path:
                            sys.path.insert(0, str(src_path))

                        # Import DiscordNotifier with fallback dynamic import to avoid path issues
                        try:
                            from common.discord_notifier import DiscordNotifier  # type: ignore
                        except Exception as import_err:
                            logger.warning(
                                f"⚠️  Import direct thất bại: {import_err} -> thử dynamic import"
                            )
                            import importlib.util

                            dn_path = Path("/opt/airflow/src/common/discord_notifier.py")
                            if dn_path.exists():
                                spec = importlib.util.spec_from_file_location(
                                    "discord_notifier_dyn", dn_path
                                )
                                if spec and spec.loader:
                                    mod = importlib.util.module_from_spec(spec)
                                    spec.loader.exec_module(mod)  # type: ignore
                                    DiscordNotifier = getattr(mod, "DiscordNotifier", None)  # type: ignore
                                else:
                                    DiscordNotifier = None  # type: ignore
                            else:
                                DiscordNotifier = None  # type: ignore
                        if not DiscordNotifier:
                            raise RuntimeError(
                                "DiscordNotifier không khả dụng sau khi cố dynamic import"
                            )

                        notifier = DiscordNotifier(monitoring_webhook)

                        # Dedupe: build hash of alerts list
                        import hashlib

                        alerts_hash_source = "\n".join(sorted(alerts)) + result["timestamp"][:10]
                        alerts_hash = hashlib.sha256(alerts_hash_source.encode("utf-8")).hexdigest()
                        last_hash = None
                        try:
                            last_hash = Variable.get("HEALTH_CHECK_LAST_ALERT_HASH")
                        except Exception:
                            last_hash = None
                        is_critical = any("🔴" in a for a in alerts)
                        if alerts_hash == last_hash and not is_critical:
                            logger.info(
                                "🛑 Dedupe: Alert hash unchanged and no critical alerts -> skip send"
                            )
                        else:
                            # Batch embeds if many alerts
                            MAX_ALERTS_PER_EMBED = 10
                            embed_chunks = []
                            for i in range(0, len(alerts), MAX_ALERTS_PER_EMBED):
                                chunk = alerts[i : i + MAX_ALERTS_PER_EMBED]
                                description = "\n".join(f"• {a}" for a in chunk)
                                embed_chunks.append(
                                    {
                                        "title": f"Segment {i//MAX_ALERTS_PER_EMBED + 1}",
                                        "description": description,
                                        "color": (
                                            0xFF0000
                                            if any("🔴" in a for a in chunk)
                                            else (
                                                0xFFA500
                                                if any("🟡" in a for a in chunk)
                                                else 0x3498DB
                                            )
                                        ),
                                    }
                                )

                            # Summary embed
                            summary_color = (
                                0xFF0000
                                if any("🔴" in a for a in alerts)
                                else (0xFFA500 if any("🟡" in a for a in alerts) else 0x2ECC71)
                            )
                            summary_embed = {
                                "title": "🏥 System Health Alert",
                                "description": f"Tổng cộng {len(alerts)} alerts\nTimestamp: {result['timestamp']}",
                                "color": summary_color,
                            }
                            embeds = [summary_embed] + embed_chunks

                            notifier.send_alert(
                                title="🏥 System Health Alert",
                                message="Chi tiết xem các embeds.",
                                color=summary_color,
                                embeds=embeds,
                            )
                            logger.info(
                                f"📨 Đã gửi {len(alerts)} alerts qua Discord monitoring webhook"
                            )
                            try:
                                Variable.set("HEALTH_CHECK_LAST_ALERT_HASH", alerts_hash)
                            except Exception as ve:
                                logger.warning(
                                    f"⚠️  Không set được HEALTH_CHECK_LAST_ALERT_HASH: {ve}"
                                )

                except Exception as discord_error:
                    logger.warning(f"⚠️  Không thể gửi Discord alert: {discord_error}")

        except Exception as alert_error:
            logger.warning(f"⚠️  Lỗi khi xử lý alerts: {alert_error}")

    except Exception as e:
        logger.error(f"❌ Lỗi khi health check: {e}", exc_info=True)
        result["status"] = "failed"
        result["error"] = str(e)

    logger.info("=" * 70)
    return result


def cleanup_redis_cache(**context) -> dict[str, Any]:
    """
    Task: Cleanup Redis cache

    Cleanup Redis cache để giải phóng bộ nhớ và đảm bảo cache không quá cũ.
    Task này chạy với trigger_rule="all_done" để chạy ngay cả khi upstream tasks fail.

    Returns:
        Dict: Kết quả cleanup
    """
    logger = get_logger(context)
    logger.info("=" * 70)
    logger.info("🧹 TASK: Cleanup Redis Cache")
    logger.info("=" * 70)

    result = {
        "status": "failed",
        "redis_reset": False,
        "stats_before": {},
        "stats_after": {},
    }

    try:
        # Ensure src path in sys.path for package-style imports
        import sys
        from pathlib import Path

        src_path = Path("/opt/airflow/src")
        if src_path.exists() and str(src_path) not in sys.path:
            sys.path.insert(0, str(src_path))

        try:
            from pipelines.crawl.storage.redis_cache import get_redis_cache  # type: ignore
        except Exception as import_err:
            logger.warning(
                f"⚠️  Import get_redis_cache failed: {import_err} -> trying dynamic import"
            )
            # Dynamic import fallback
            import importlib.util

            rc_path = src_path / "pipelines" / "crawl" / "storage" / "redis_cache.py"
            get_redis_cache = None  # type: ignore
            if rc_path.exists():
                spec = importlib.util.spec_from_file_location("redis_cache_dyn", rc_path)
                if spec and spec.loader:
                    mod = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(mod)  # type: ignore
                    get_redis_cache = getattr(mod, "get_redis_cache", None)  # type: ignore
            if not get_redis_cache:
                raise RuntimeError(
                    "Không thể import get_redis_cache (dynamic import cũng thất bại)"
                ) from import_err

        # Kết nối Redis
        redis_cache = get_redis_cache("redis://redis:6379/1")  # type: ignore
        if not redis_cache:
            logger.warning("⚠️  Không thể kết nối Redis, skip cleanup")
            result["status"] = "skipped"
            result["reason"] = "Redis not available"
            return result

        # Lấy stats trước khi cleanup (dùng client.info())
        try:
            info_before = redis_cache.client.info()
            db_key = f"db{redis_cache.client.connection_pool.connection_kwargs.get('db', 1)}"
            keys_before = info_before.get(db_key, {}).get("keys", 0)
            hits = info_before.get("keyspace_hits", 0)
            misses = info_before.get("keyspace_misses", 0)
            hit_rate = (hits / (hits + misses) * 100) if (hits + misses) > 0 else 0.0
            stats_before = {
                "keys": keys_before,
                "used_memory_human": info_before.get("used_memory_human"),
                "hit_rate": hit_rate,
                "keyspace_hits": hits,
                "keyspace_misses": misses,
            }
            result["stats_before"] = stats_before
            logger.info("📊 Redis stats trước cleanup:")
            logger.info(f"   - Keys: {keys_before}")
            logger.info(f"   - Memory used: {stats_before.get('used_memory_human', 'N/A')}")
            logger.info(f"   - Hit rate: {hit_rate:.1f}%")
        except Exception as e:
            logger.warning(f"⚠️  Không thể lấy stats trước cleanup: {e}")

        # Reset cache
        logger.info("🧹 Đang cleanup Redis cache...")
        try:
            redis_cache.client.flushdb()
            result["redis_reset"] = True
            logger.info("✅ Đã flush DB Redis cache thành công")
        except Exception as e:
            logger.error(f"❌ Flush DB thất bại: {e}")

        # Lấy stats sau khi cleanup
        try:
            import time as _t

            _t.sleep(1)
            info_after = redis_cache.client.info()
            db_key = f"db{redis_cache.client.connection_pool.connection_kwargs.get('db', 1)}"
            keys_after = info_after.get(db_key, {}).get("keys", 0)
            hits_a = info_after.get("keyspace_hits", 0)
            misses_a = info_after.get("keyspace_misses", 0)
            hit_rate_a = (hits_a / (hits_a + misses_a) * 100) if (hits_a + misses_a) > 0 else 0.0
            stats_after = {
                "keys": keys_after,
                "used_memory_human": info_after.get("used_memory_human"),
                "hit_rate": hit_rate_a,
                "keyspace_hits": hits_a,
                "keyspace_misses": misses_a,
            }
            result["stats_after"] = stats_after
            logger.info("📊 Redis stats sau cleanup:")
            logger.info(f"   - Keys: {keys_after}")
            logger.info(f"   - Memory used: {stats_after.get('used_memory_human', 'N/A')}")
        except Exception as e:
            logger.warning(f"⚠️  Không thể lấy stats sau cleanup: {e}")

        result["status"] = "success"
        logger.info("✅ Cleanup Redis cache hoàn tất")

    except Exception as e:
        logger.error(f"❌ Lỗi khi cleanup Redis cache: {e}", exc_info=True)
        result["error"] = str(e)

    logger.info("=" * 70)
    return result


def cleanup_old_backups(retention_count: int = 5, **context) -> dict[str, Any]:
    """
    Cleanup old backup files, keep only latest N backups

    Args:
        retention_count: Số lượng backups cần giữ lại (mặc định 5)
        **context: Airflow context for logging

    Returns:
        Dict: Số file đã xóa
    """
    from pathlib import Path

    logger = get_logger(context)

    backup_dir = Path("/opt/airflow/backups/postgres")
    if not backup_dir.exists():
        # Fallback paths
        for bd in [
            Path("/backups"),
            Path("/opt/airflow/data/backups/postgres"),
            Path("/tmp/backups"),
        ]:
            if bd.exists():
                backup_dir = bd
                break
        else:
            logger.warning("No backup directory found, skipping cleanup")
            return {"status": "skipped", "reason": "No backup directory found"}

    # Find all backup files sorted by modification time
    backup_files = sorted(
        backup_dir.glob("crawl_data_*.sql"), key=lambda x: x.stat().st_mtime, reverse=True
    )

    deleted_count = 0
    if len(backup_files) > retention_count:
        for backup_file in backup_files[retention_count:]:
            try:
                file_size = backup_file.stat().st_size / (1024 * 1024)
                backup_file.unlink()
                deleted_count += 1
                logger.info(f"🗑️  Xóa backup cũ: {backup_file.name} ({file_size:.2f}MB)")
            except Exception as e:
                logger.warning(f"⚠️  Không xóa được {backup_file.name}: {e}")

    return {
        "status": "success",
        "deleted": deleted_count,
        "remaining": len(backup_files[:retention_count]),
    }


def backup_database(**context) -> dict[str, Any]:
    """
    Task: Backup PostgreSQL database

    Backup database crawl_data vào thư mục backups/postgres sau khi các tasks khác hoàn thành.

    Returns:
        Dict: Kết quả backup
    """
    logger = get_logger(context)
    logger.info("=" * 70)
    logger.info("💾 TASK: Backup Database")
    logger.info("=" * 70)

    try:
        import subprocess
        from pathlib import Path

        # Đường dẫn script backup
        script_path = Path("/opt/airflow/scripts/helper/backup_postgres.py")
        if not script_path.exists():
            # Fallback: thử đường dẫn tương đối
            script_path = (
                Path(__file__).parent.parent.parent / "scripts" / "helper" / "backup_postgres.py"
            )

        if not script_path.exists():
            logger.warning(f"⚠️  Không tìm thấy script backup tại: {script_path}")
            logger.info("💡 Sử dụng pg_dump trực tiếp...")

            # Fallback: sử dụng pg_dump trực tiếp
            container_name = "tiki-data-pipeline-postgres-1"
            # Thử nhiều đường dẫn backup
            backup_dirs = [
                Path("/opt/airflow/backups/postgres"),  # Trong container Airflow
                Path("/backups"),  # Mount từ postgres container
                Path("/opt/airflow/data/backups/postgres"),  # Fallback
            ]
            backup_dir = None
            for bd in backup_dirs:
                try:
                    bd.mkdir(parents=True, exist_ok=True)
                    # Test write
                    test_file = bd / ".test_write"
                    test_file.write_text("test")
                    test_file.unlink()
                    backup_dir = bd
                    break
                except Exception:
                    continue

            if not backup_dir:
                logger.warning("⚠️  Không tìm thấy thư mục backup có thể ghi, sử dụng /tmp")
                backup_dir = Path("/tmp/backups")
                backup_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = backup_dir / f"crawl_data_{timestamp}.sql"  # Đổi .dump -> .sql

            # Lấy thông tin từ environment variables
            postgres_user = os.getenv("POSTGRES_USER", "airflow_user")
            postgres_password = os.getenv("POSTGRES_PASSWORD", "")

            if not postgres_password:
                logger.warning("⚠️  Không tìm thấy POSTGRES_PASSWORD trong environment")
                return {"status": "skipped", "reason": "No password"}

            logger.info("📦 Đang backup database: crawl_data...")
            logger.info(f"   File: {backup_file}")

            # Chạy pg_dump trong container - dùng plain SQL format
            cmd = [
                "docker",
                "exec",
                "-e",
                f"PGPASSWORD={postgres_password}",
                container_name,
                "pg_dump",
                "-U",
                postgres_user,
                "--format=plain",  # Plain SQL format - dễ restore, tương thích
                "--no-owner",  # Không dump owner info
                "--no-acl",  # Không dump access privileges
                "crawl_data",
            ]

            try:
                with open(backup_file, "wb") as f:
                    result = subprocess.run(
                        cmd,
                        stdout=f,
                        stderr=subprocess.PIPE,
                        check=False,
                        timeout=600,  # 10 phút timeout
                    )

                if result.returncode == 0:
                    file_size = backup_file.stat().st_size
                    size_mb = file_size / (1024 * 1024)
                    logger.info(f"✅ Đã backup thành công: {backup_file.name}")
                    logger.info(f"   Size: {size_mb:.2f} MB")
                    return {
                        "status": "success",
                        "backup_file": str(backup_file),
                        "size_mb": round(size_mb, 2),
                    }
                else:
                    error_msg = result.stderr.decode("utf-8", errors="ignore")
                    logger.error(f"❌ Lỗi khi backup: {error_msg}")
                    if backup_file.exists():
                        backup_file.unlink()
                    return {"status": "failed", "error": error_msg}

            except subprocess.TimeoutExpired:
                logger.error("❌ Timeout khi backup database")
                if backup_file.exists():
                    backup_file.unlink()
                return {"status": "failed", "error": "Timeout"}
            except Exception as e:
                logger.error(f"❌ Exception khi backup: {e}")
                if backup_file.exists():
                    backup_file.unlink()
                return {"status": "failed", "error": str(e)}
        else:
            # Sử dụng script backup (dùng format sql để tránh vấn đề version dump)
            logger.info(f"📦 Đang backup database bằng script: {script_path}")

            cmd = ["python", str(script_path), "--database", "crawl_data", "--format", "sql"]

            try:
                result = subprocess.run(
                    cmd, capture_output=True, text=True, check=False, timeout=600  # 10 phút timeout
                )

                if result.returncode == 0:
                    logger.info("✅ Backup thành công!")
                    logger.info(result.stdout)
                    return {
                        "status": "success",
                        "output": result.stdout,
                    }
                else:
                    logger.warning(f"⚠️  Backup có lỗi (exit code: {result.returncode})")
                    logger.warning(result.stderr)
                    # Không fail task, chỉ log warning
                    return {
                        "status": "warning",
                        "error": result.stderr,
                    }
            except subprocess.TimeoutExpired:
                logger.error("❌ Timeout khi backup database")
                return {"status": "failed", "error": "Timeout"}
            except Exception as e:
                logger.error(f"❌ Exception khi backup: {e}")
                return {"status": "failed", "error": str(e)}

    except Exception as e:
        logger.error(f"❌ Lỗi trong backup_database task: {e}", exc_info=True)
        # Không fail task, chỉ log lỗi
        return {"status": "failed", "error": str(e)}


def send_quality_report_discord():
    """
    Gửi báo cáo chất lượng dữ liệu lên Discord (không trùng tiêu đề, hạn chế icon).
    """
    import psycopg2
    from psycopg2.extras import RealDictCursor

    # Ensure logger is defined for this task context
    logger = logging.getLogger("airflow.task")
    logger.info("Bắt đầu gửi báo cáo chất lượng lên Discord...")

    # Helper: làm sạch nội dung (bỏ emoji, bỏ tiêu đề trùng, giữ định dạng markdown)
    def _clean_report_text(text: str) -> str:
        # Bỏ dòng tiêu đề đầu tiên nếu có
        lines = text.splitlines()
        cleaned_lines = []
        for idx, line in enumerate(lines):
            if idx == 0 and ("BÁO CÁO" in line or "Báo cáo" in line):
                continue
            # Bỏ dòng phân cách dạng toàn ký tự ━
            if set(line.strip()) <= {"━", "—", "-", "_"} and len(line.strip()) >= 5:
                continue
            cleaned_lines.append(line)

        text = "\n".join(cleaned_lines).strip()

        # Loại bỏ emoji phổ biến (giữ tiếng Việt)
        emoji_pattern = (
            "[\U0001f600-\U0001f64f]"  # emoticons
            "|[\U0001f300-\U0001f5ff]"  # symbols & pictographs
            "|[\U0001f680-\U0001f6ff]"  # transport & map
            "|[\U0001f1e0-\U0001f1ff]"  # flags
            "|[\u2600-\u26ff]"  # misc symbols
            "|[\u2700-\u27bf]"  # dingbats
        )
        try:
            text = re.sub(emoji_pattern, "", text)
        except re.error:
            # Nếu môi trường không hỗ trợ UCS-4, bỏ qua bước này
            pass

        # Thay thế một số icon còn lại bằng highlight chữ
        replacements = {
            "📊": "",
            "✅": "",
            "⚠️": "",
            "💰": "",
            "📌": "",
            "💡": "",
            "🔗": "",
        }
        for k, v in replacements.items():
            text = text.replace(k, v)

        return text.strip()

    try:
        # Import modules từ src
        sys.path.insert(0, "/opt/airflow/src")
        from common.ai.summarizer import AISummarizer
        from common.notifications.discord import DiscordNotifier

        # Kết nối DB
        db_config = {
            "host": os.getenv("POSTGRES_HOST", "postgres"),
            "database": "crawl_data",
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
        }
        conn = psycopg2.connect(**db_config)

        # Tạo báo cáo
        summarizer = AISummarizer()
        summarizer.generate_data_quality_report(conn)
        conn.close()

        # Làm sạch nội dung để tránh trùng tiêu đề và bớt icon

        # Gửi Discord
        notifier = DiscordNotifier()

        # Lấy top 5 sản phẩm giảm giá để tạo fields riêng (kèm link)
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT
                name,
                url,
                discount_percent,
                price,
                sales_count
            FROM products
            WHERE discount_percent IS NOT NULL
                AND discount_percent > 20
                AND name IS NOT NULL
            ORDER BY discount_percent DESC
            LIMIT 5
            """
        )
        discount_products = cur.fetchall()
        cur.close()
        conn.close()

        # Lấy snapshot số liệu chi tiết
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT
                COUNT(*) AS total_products,
                COUNT(CASE WHEN sales_count IS NOT NULL AND sales_count > 0 THEN 1 END) AS with_sales,
                COUNT(CASE WHEN discount_percent > 0 THEN 1 END) AS with_discount,
                AVG(discount_percent) AS avg_discount,
                MAX(discount_percent) AS max_discount,
                MIN(price) AS min_price,
                MAX(price) AS max_price,
                AVG(price) AS avg_price,
                SUM(CAST(sales_count AS BIGINT)) AS total_sales
            FROM products
            """
        )
        snap = cur.fetchone() or {}
        cur.close()
        conn.close()

        total = snap.get("total_products") or 0
        with_sales = snap.get("with_sales") or 0
        with_discount = snap.get("with_discount") or 0
        coverage = (with_sales * 100 / total) if total > 0 else 0.0
        avg_disc = snap.get("avg_discount") or 0.0
        max_disc = snap.get("max_discount") or 0.0
        min_price = snap.get("min_price") or 0
        max_price = snap.get("max_price") or 0
        avg_price = snap.get("avg_price") or 0
        total_sales = snap.get("total_sales") or 0

        # Mô tả chi tiết với code block
        description = (
            "**Dataset Quality Report**\n"
            + "```\n"
            + f"Total Products: {total:,}\n"
            + f"With Sales: {with_sales:,} ({coverage:.1f}%)\n"
            + f"On Discount: {with_discount:,} ({(with_discount*100/total if total>0 else 0):.1f}%)\n"
            + f"Total Sales: {total_sales:,} units\n"
            + f"Avg Discount: {avg_disc:.1f}% (Max: {max_disc:.1f}%)\n"
            + f"Price Range: {min_price:,.0f}đ - {max_price:,.0f}đ (Avg: {avg_price:,.0f}đ)\n"
            + "```"
        )

        # Tạo fields cho layout rộng
        fields = []

        # Row 1: Status
        fields.append({"name": "Status", "value": "✅ Success", "inline": True})
        fields.append(
            {"name": "Time", "value": datetime.now().strftime("%H:%M:%S"), "inline": True}
        )
        fields.append({"name": "DB", "value": "crawl_data", "inline": True})

        # Row 2: Coverage metrics
        fields.append({"name": "Coverage", "value": f"{coverage:.1f}% with sales", "inline": True})
        fields.append(
            {
                "name": "Discounts",
                "value": f"{(with_discount*100/total if total>0 else 0):.1f}% on sale",
                "inline": True,
            }
        )
        fields.append({"name": "Total Sales", "value": f"{total_sales:,} units", "inline": True})

        # Separator
        fields.append(
            {"name": "═══════ TOP DEEP DISCOUNTS (>20%) ═══════", "value": "​", "inline": False}
        )

        # Thêm top 5 sản phẩm giảm giá theo 2 cột để rộng ngang
        if discount_products:
            for i in range(0, len(discount_products), 2):
                # Column 1
                p1 = discount_products[i]
                name1 = (p1["name"] or "N/A")[:30]
                disc1 = p1["discount_percent"] or 0
                price1 = p1["price"] or 0
                sales1 = p1.get("sales_count") or 0
                url1 = p1.get("url") or ""

                info1 = f"**#{i+1}. {name1}**\n"
                info1 += f"Discount: {disc1:.0f}% | Price: {price1:,.0f}đ\n"
                info1 += f"Sales: {sales1:,} units"
                if url1:
                    info1 += f"\n[→ View on Tiki]({url1})"

                if i + 1 < len(discount_products):
                    # Column 2
                    p2 = discount_products[i + 1]
                    name2 = (p2["name"] or "N/A")[:30]
                    disc2 = p2["discount_percent"] or 0
                    price2 = p2["price"] or 0
                    sales2 = p2.get("sales_count") or 0
                    url2 = p2.get("url") or ""

                    info2 = f"**#{i+2}. {name2}**\n"
                    info2 += f"Discount: {disc2:.0f}% | Price: {price2:,.0f}đ\n"
                    info2 += f"Sales: {sales2:,} units"
                    if url2:
                        info2 += f"\n[→ View on Tiki]({url2})"

                    fields.append({"name": "Product", "value": info1, "inline": True})
                    fields.append({"name": "Product", "value": info2, "inline": True})
                else:
                    fields.append({"name": "Product", "value": info1, "inline": False})
        else:
            fields.append(
                {"name": "Notice", "value": "No products with >20% discount found", "inline": False}
            )

        success = notifier.send_message(
            content=description,
            title="📊 Tiki Data Quality Report",
            color=0x3498DB,
            fields=fields,
            footer="Tiki Pipeline - Airflow DAG | Data Quality Analysis",
        )

        if success:
            logger.info("Đã gửi báo cáo lên Discord thành công!")
            return {"status": "success", "message": "Discord report sent successfully"}
        else:
            logger.warning("⚠️ Lỗi gửi Discord nhưng pipeline hoàn tất")
            return {"status": "warning", "message": "Failed to send Discord report"}

    except ImportError as e:
        logger.warning(f"⚠️ Import error: {e} - Discord report skipped")
        return {"status": "skipped", "reason": "Import error"}
    except Exception as e:
        logger.warning(f"⚠️ Lỗi gửi báo cáo Discord: {e}")
        # Không fail task, chỉ log warning
        return {"status": "failed", "error": str(e)}


# Tạo DAG duy nhất với schedule có thể config qua Variable
with DAG(**DAG_CONFIG) as dag:

    # TaskGroup: Load và Prepare
    with TaskGroup("load_and_prepare") as load_group:
        # (ĐÃ BỎ) extract_and_load_categories_to_db để giảm thời gian pipeline.
        # Task: Load danh sách categories từ file để crawl
        task_load_categories = PythonOperator(
            task_id="load_categories",
            python_callable=load_categories,
            execution_timeout=timedelta(minutes=5),  # Timeout 5 phút
            pool="crawl_pool",
        )

    # TaskGroup: Crawl Categories (Dynamic Task Mapping)
    with TaskGroup("crawl_categories") as crawl_group:
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

            # Cách 3: Thử lấy từ upstream task (Airflow 3.x compatible)
            if not categories:
                try:
                    # Airflow 3.x: Dùng dag_run.get_task_instance() thay vì TaskInstance constructor
                    dag_run = context["dag_run"]
                    # Thử lấy từ task trong TaskGroup
                    upstream_ti = dag_run.get_task_instance(
                        task_id="load_and_prepare.load_categories"
                    )
                    if upstream_ti:
                        categories = upstream_ti.xcom_pull(key="return_value")
                        logger.info(
                            f"Lấy categories từ dag_run.get_task_instance(): {len(categories) if categories else 0} items"
                        )
                except Exception as e:
                    logger.warning(f"Không lấy được từ dag_run.get_task_instance(): {e}")

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

        def prepare_category_batch_kwargs(**context):
            """
            Helper function để prepare op_kwargs cho Dynamic Task Mapping với batch processing

            Chia categories thành batches để giảm số lượng Airflow tasks và tận dụng driver pooling
            """
            import logging

            logger = logging.getLogger("airflow.task")

            ti = context["ti"]

            # Lấy categories từ XCom (same logic as prepare_crawl_kwargs)
            categories = None

            try:
                categories = ti.xcom_pull(task_ids="load_and_prepare.load_categories")
                logger.info(
                    f"Lấy categories từ 'load_and_prepare.load_categories': {len(categories) if categories else 0} items"
                )
            except Exception as e:
                logger.warning(f"Không lấy được từ 'load_and_prepare.load_categories': {e}")

            if not categories:
                try:
                    categories = ti.xcom_pull(task_ids="load_categories")
                    logger.info(
                        f"Lấy categories từ 'load_categories': {len(categories) if categories else 0} items"
                    )
                except Exception as e:
                    logger.warning(f"Không lấy được từ 'load_categories': {e}")

            if not categories:
                try:
                    # Airflow 3.x: Dùng dag_run.get_task_instance() thay vì TaskInstance constructor
                    dag_run = context["dag_run"]
                    # Thử lấy từ task trong TaskGroup
                    upstream_ti = dag_run.get_task_instance(
                        task_id="load_and_prepare.load_categories"
                    )
                    if upstream_ti:
                        categories = upstream_ti.xcom_pull(key="return_value")
                        logger.info(
                            f"Lấy categories từ dag_run.get_task_instance(): {len(categories) if categories else 0} items"
                        )
                except Exception as e:
                    logger.warning(f"Không lấy được từ dag_run.get_task_instance(): {e}")

            if not categories:
                logger.error("❌ Không thể lấy categories từ XCom!")
                return []

            if not isinstance(categories, list):
                logger.error(f"❌ Categories không phải list: {type(categories)}")
                return []

            logger.info(f"✅ Đã lấy {len(categories)} categories")

            # Batch Processing: Chia categories thành batches
            batch_size = int(Variable.get("TIKI_CATEGORY_BATCH_SIZE", default="10"))
            batches = []
            for i in range(0, len(categories), batch_size):
                batch = categories[i : i + batch_size]
                batches.append(batch)

            logger.info(
                f"📦 Đã chia thành {len(batches)} batches (mỗi batch {batch_size} categories)"
            )
            logger.info(f"   - Batch đầu tiên: {len(batches[0]) if batches else 0} categories")
            logger.info(f"   - Batch cuối cùng: {len(batches[-1]) if batches else 0} categories")

            # Trả về list các dict để expand (mỗi dict là 1 batch)
            op_kwargs_list = [
                {"category_batch": batch, "batch_index": idx} for idx, batch in enumerate(batches)
            ]

            logger.info(
                f"🔢 Tạo {len(op_kwargs_list)} op_kwargs cho Dynamic Task Mapping (batches)"
            )
            if op_kwargs_list:
                logger.info("📋 Sample batches (first 2):")
                for _i, kwargs in enumerate(op_kwargs_list[:2]):
                    batch = kwargs.get("category_batch", [])
                    batch_idx = kwargs.get("batch_index", -1)
                    category_names = [c.get("name", "Unknown") for c in batch[:3]]
                    logger.info(
                        f"  Batch {batch_idx}: {len(batch)} categories - {category_names}..."
                    )

            return op_kwargs_list

        # Chỉ tạo task prepare phù hợp với mode đang dùng (batch hoặc single-category)
        # Để tránh task bị skip vì chỉ 1 trong 2 task prepare được dùng
        if crawl_category_batch is not None:
            # Batch processing mode (PRODUCTION: Khuyến nghị dùng mode này)
            task_prepare = PythonOperator(
                task_id="prepare_batch_kwargs",
                python_callable=prepare_category_batch_kwargs,
                execution_timeout=timedelta(minutes=1),
            )

            task_crawl_category = PythonOperator.partial(
                task_id="crawl_category",
                python_callable=crawl_category_batch,
                execution_timeout=timedelta(minutes=12),  # Tối ưu: giảm từ 15 -> 12 phút
                pool="crawl_pool",
                retries=1,  # Retry 1 lần
                retry_delay=timedelta(seconds=15),  # Tối ưu: thêm delay ngắn
            ).expand(op_kwargs=task_prepare.output)
        else:
            # Fallback: single-category processing
            task_prepare = PythonOperator(
                task_id="prepare_crawl_kwargs",
                python_callable=prepare_crawl_kwargs,
                execution_timeout=timedelta(minutes=1),
            )

            task_crawl_category = PythonOperator.partial(
                task_id="crawl_category",
                python_callable=crawl_single_category,
                execution_timeout=timedelta(minutes=10),  # Timeout 10 phút mỗi category
                pool="crawl_pool",
                retries=1,
            ).expand(op_kwargs=task_prepare.output)

    # TaskGroup: Process và Save
    with TaskGroup("process_and_save") as process_group:
        task_merge_products = PythonOperator(
            task_id="merge_products",
            python_callable=merge_products,
            execution_timeout=timedelta(minutes=30),  # Timeout 30 phút
            pool="crawl_pool",
            trigger_rule="all_done",  # QUAN TRỌNG: Chạy khi tất cả upstream tasks done (success hoặc failed)
        )

        task_save_products = PythonOperator(
            task_id="save_products",
            python_callable=save_products,
            execution_timeout=timedelta(minutes=10),  # Timeout 10 phút
            pool="crawl_pool",
        )

    # TaskGroup: Crawl Product Details (Dynamic Task Mapping)
    with TaskGroup("crawl_product_details") as detail_group:

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
                            logger.debug(f"XCom from upstream: {task_id}")
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
                        logger.debug("XCom from prepare_products_for_detail")
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

            logger.info(f"✅ Retrieved {len(products_to_crawl)} products for detail crawl")

            # Batch Processing: Chia products thành batches 15 products/batch (tối ưu: từ 15 -> 12 để parallel hơn)
            batch_size = 15
            batches = []
            for i in range(0, len(products_to_crawl), batch_size):
                batch = products_to_crawl[i : i + batch_size]
                batches.append(batch)

            logger.info(
                f"📦 Đã chia thành {len(batches)} batches (mỗi batch {batch_size} products)"
            )
            logger.info(f"   - Batch đầu tiên: {len(batches[0]) if batches else 0} products")
            logger.info(f"   - Batch cuối cùng: {len(batches[-1]) if batches else 0} products")

            # Trả về list các dict để expand (mỗi dict là 1 batch)
            op_kwargs_list = [
                {"product_batch": batch, "batch_index": idx} for idx, batch in enumerate(batches)
            ]

            logger.info(f"🔢 Created {len(op_kwargs_list)} batches for Dynamic Task Mapping")

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

        # Dynamic Task Mapping cho crawl detail (Batch Processing)
        task_crawl_product_detail = PythonOperator.partial(
            task_id="crawl_product_detail",
            python_callable=crawl_product_batch,  # Dùng batch function thay vì single
            execution_timeout=timedelta(minutes=15),  # Tối ưu: giảm từ 20 -> 15 phút (nhanh hơn)
            pool="crawl_pool",
            retries=1,  # Tối ưu: giảm từ 2 -> 1 (nhanh hơn)
            retry_delay=timedelta(seconds=30),  # Tối ưu: giảm từ 2 phút -> 30 giây
        ).expand(op_kwargs=task_prepare_detail_kwargs.output)

        task_merge_product_details = PythonOperator(
            task_id="merge_product_details",
            python_callable=merge_product_details,
            execution_timeout=timedelta(minutes=30),  # Tối ưu: giảm từ 60 -> 30 phút
            pool="crawl_pool",
            trigger_rule="all_done",  # Chạy khi tất cả upstream tasks done
            # Tăng heartbeat interval để tránh timeout khi xử lý nhiều dữ liệu
        )

        task_save_products_with_detail = PythonOperator(
            task_id="save_products_with_detail",
            python_callable=save_products_with_detail,
            execution_timeout=timedelta(minutes=10),  # Timeout 10 phút
            pool="crawl_pool",
        )

        # Dependencies trong detail group
        (
            task_prepare_detail
            >> task_prepare_detail_kwargs
            >> task_crawl_product_detail
            >> task_merge_product_details
            >> task_save_products_with_detail
        )

    # TaskGroup: Enrich Category Path (thêm category_path cho products thiếu, dựa vào categories file)
    with TaskGroup("enrich_category_path") as enrich_group:

        def enrich_category_path_task(**context):
            """Bổ sung category_path cho products có category_id nhưng chưa có breadcrumb.

            Luồng:
            1. Đọc file sản phẩm chi tiết (products_with_detail.json)
            2. Đọc categories từ file (categories_recursive_optimized.json) - đã có category_id và category_path
            3. Xây dựng lookup map: category_id -> category_path
            4. Với mỗi product: nếu có category_id nhưng chưa có category_path, tìm từ lookup map
            5. Ghi lại file
            """
            logger = get_logger(context)
            logger.info("=" * 70)
            logger.info("🧩 TASK: Enrich Category Path")
            logger.info("=" * 70)

            ti = context["ti"]

            # Bước 1: Lấy file sản phẩm chi tiết
            output_file = None
            try:
                output_file = ti.xcom_pull(
                    task_ids="crawl_product_details.save_products_with_detail"
                )
            except Exception:
                pass
            if not output_file:
                output_file = str(OUTPUT_FILE_WITH_DETAIL)

            if not os.path.exists(output_file):
                raise FileNotFoundError(f"Không tìm thấy file detail: {output_file}")

            logger.info(f"📂 Đang đọc file detail: {output_file}")
            with open(output_file, encoding="utf-8") as f:
                data = json.load(f)
            products = data.get("products", [])
            logger.info(f"📊 Số products trước enrich: {len(products)}")

            # Bước 2: Đọc categories từ file
            category_path_lookup: dict[str, list] = {}  # category_id -> category_path

            if CATEGORIES_FILE.exists():
                try:
                    logger.info(f"📖 Đọc categories từ file: {CATEGORIES_FILE}")
                    with open(CATEGORIES_FILE, encoding="utf-8") as cf:
                        raw_categories = json.load(cf)

                    for cat in raw_categories:
                        cat_id = cat.get("category_id")
                        cat_path = cat.get("category_path")

                        # Chỉ thêm vào lookup nếu có category_id và category_path
                        if cat_id and cat_path:
                            category_path_lookup[cat_id] = cat_path

                    logger.info(f"✅ Loaded {len(category_path_lookup)} category_path từ file")
                except Exception as fe:
                    logger.warning(f"⚠️ Lỗi đọc file categories: {fe}")
            else:
                logger.warning(f"⚠️ File categories không tồn tại: {CATEGORIES_FILE}")

            # Bước 3: Enrich category_path cho products
            enriched = 0
            without_category_id = 0
            for p in products:
                # Nếu product có category_id nhưng chưa có category_path
                if p.get("category_id") and not p.get("category_path"):
                    cat_id = p["category_id"]

                    # Tìm category_path từ lookup map
                    if cat_id in category_path_lookup:
                        p["category_path"] = category_path_lookup[cat_id]
                        enriched += 1
                    else:
                        # Log: category_id không tìm thấy trong file categories
                        without_category_id += 1

            # Bước 4: Report
            if enriched > 0:
                logger.info(f"✅ Enriched category_path cho {enriched} products")
            if without_category_id > 0:
                logger.warning(
                    f"⚠️ {without_category_id} products có category_id nhưng không tìm thấy trong categories"
                )

            # Check: Số products có category_path
            products_with_path = sum(1 for p in products if p.get("category_path"))
            logger.info(f"📊 Tổng products có category_path: {products_with_path}/{len(products)}")

            # Bước 5: Ghi lại file (in-place update)
            data["products"] = products
            atomic_write_file(output_file, data, **context)
            logger.info(f"💾 Đã cập nhật file với category_path enrich: {output_file}")

            return {
                "file": output_file,
                "enriched_count": enriched,
                "products_with_path": products_with_path,
            }

        task_enrich_category_path = PythonOperator(
            task_id="enrich_products_category_path",
            python_callable=enrich_category_path_task,
            execution_timeout=timedelta(minutes=10),
            pool="crawl_pool",
        )

    # TaskGroup: Transform and Load
    with TaskGroup("transform_and_load") as transform_load_group:
        task_transform_products = PythonOperator(
            task_id="transform_products",
            python_callable=transform_products,
            execution_timeout=timedelta(minutes=30),  # Timeout 30 phút
            pool="crawl_pool",
        )

        task_load_products = PythonOperator(
            task_id="load_products",
            python_callable=load_products,
            execution_timeout=timedelta(minutes=30),  # Timeout 30 phút
            pool="crawl_pool",
        )

        # Dependencies trong transform_load group
        task_transform_products >> task_load_products

    # TaskGroup: Validate
    with TaskGroup("validate") as validate_group:
        task_validate_data = PythonOperator(
            task_id="validate_data",
            python_callable=validate_data,
            execution_timeout=timedelta(minutes=5),  # Timeout 5 phút
            pool="crawl_pool",
        )

    # TaskGroup: Aggregate and Notify
    with TaskGroup("aggregate_and_notify") as aggregate_group:
        task_aggregate_and_notify = PythonOperator(
            task_id="aggregate_and_notify",
            python_callable=aggregate_and_notify,
            execution_timeout=timedelta(minutes=10),  # Timeout 10 phút
            pool="crawl_pool",
            trigger_rule="all_done",  # Chạy ngay cả khi có task upstream fail
        )

        # Health Check task (no TaskGroup to allow direct reference)
        task_health_check = PythonOperator(
            task_id="health_check_monitoring",
            python_callable=health_check_monitoring,
            execution_timeout=timedelta(minutes=5),  # Timeout 5 phút
            pool="crawl_pool",
            trigger_rule="all_done",  # Chạy ngay cả khi có task upstream fail
        )

        # Cleanup Cache task (no TaskGroup to allow direct reference)
        task_cleanup_cache = PythonOperator(
            task_id="cleanup_redis_cache",
            python_callable=cleanup_redis_cache,
            execution_timeout=timedelta(minutes=5),  # Timeout 5 phút
            pool="crawl_pool",
            trigger_rule="all_done",  # Chạy ngay cả khi có task upstream fail
        )

        # Backup Database task (no TaskGroup to allow direct reference)
        task_backup_database = PythonOperator(
            task_id="backup_database",
            python_callable=backup_database,
            execution_timeout=timedelta(minutes=15),  # Timeout 15 phút
            pool="crawl_pool",
            trigger_rule="all_done",  # Chạy ngay cả khi có task upstream fail
        )

        # Cleanup old backups task (keep only latest 5)
        task_cleanup_backups = PythonOperator(
            task_id="cleanup_old_backups",
            python_callable=cleanup_old_backups,
            op_kwargs={"retention_count": 5},
            execution_timeout=timedelta(minutes=5),
            pool="crawl_pool",
            trigger_rule="all_done",
        )

    # Định nghĩa dependencies
    # Flow: Load -> Crawl Categories -> Merge & Save -> Prepare Detail -> Crawl Detail -> Merge & Save Detail -> Transform -> Load -> Validate -> Aggregate

    # Dependencies giữa các TaskGroup
    # Load categories trước, sau đó prepare crawl kwargs
    task_load_categories >> task_prepare

    # Prepare crawl kwargs -> crawl category (dynamic mapping)
    task_prepare >> task_crawl_category

    # Crawl category -> merge products (merge chạy khi tất cả crawl tasks done)
    task_crawl_category >> task_merge_products

    # Merge -> save products
    task_merge_products >> task_save_products

    # Save products -> prepare detail -> crawl detail -> merge detail -> save detail -> transform -> load -> validate -> aggregate and notify
    task_save_products >> task_prepare_detail
    # Dependencies trong detail group đã được định nghĩa ở dòng 1800
    # Flow: save_products_with_detail -> transform -> load -> validate -> aggregate_and_notify -> health_check -> cleanup_cache -> backup_database -> discord_report

    # Create Discord report task
    task_send_discord_report = PythonOperator(
        task_id="send_quality_report_discord",
        python_callable=send_quality_report_discord,
        execution_timeout=timedelta(minutes=5),
        pool="crawl_pool",
        trigger_rule="all_done",  # Chạy bất kể task trước có lỗi không
    )

    (
        task_save_products_with_detail
        >> task_enrich_category_path
        >> task_transform_products
        >> task_load_products
        >> task_validate_data
        >> task_aggregate_and_notify
        >> task_health_check
        >> task_cleanup_cache
        >> task_backup_database
        >> task_cleanup_backups
        >> task_send_discord_report  # Discord report là task cuối cùng
    )
