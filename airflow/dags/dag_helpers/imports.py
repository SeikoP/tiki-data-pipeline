"""
Import logic for external modules used in Tiki crawl products DAG
"""
import os
import sys
import types
import warnings
import importlib.util


def get_possible_paths(dag_file_dir):
    """Get possible paths for crawl modules"""
    return [
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


def import_crawl_utils(dag_file_dir):
    """Import crawl utils module"""
    possible_paths = get_possible_paths(dag_file_dir)
    
    # Tìm crawl module path
    crawl_module_path = None
    for path in possible_paths:
        test_path = os.path.join(path, "crawl_products.py")
        if os.path.exists(test_path):
            crawl_module_path = path
            break
    
    if not crawl_module_path:
        relative_path = os.path.abspath(
            os.path.join(dag_file_dir, "..", "..", "src", "pipelines", "crawl")
        )
        test_path = os.path.join(relative_path, "crawl_products.py")
        if os.path.exists(test_path):
            crawl_module_path = relative_path
    
    # Tìm utils path
    utils_path = None
    if crawl_module_path:
        utils_path = os.path.join(crawl_module_path, "utils.py")
        if not os.path.exists(utils_path):
            utils_path = None
    
    if not utils_path:
        for path in possible_paths:
            test_path = os.path.join(path, "utils.py")
            if os.path.exists(test_path):
                utils_path = test_path
                break
    
    if utils_path and os.path.exists(utils_path):
        try:
            spec = importlib.util.spec_from_file_location("crawl_utils", utils_path)
            if spec and spec.loader:
                utils_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(utils_module)
                sys.modules["crawl_utils"] = utils_module
                if "pipelines.crawl.utils" not in sys.modules:
                    sys.modules["pipelines"] = type(sys)("pipelines")
                    sys.modules["pipelines.crawl"] = type(sys)("pipelines.crawl")
                    sys.modules["pipelines.crawl.utils"] = utils_module
        except Exception as e:
            warnings.warn(f"Không thể import utils module: {e}", stacklevel=2)


def import_crawl_products(dag_file_dir):
    """Import crawl_products module and return functions"""
    possible_paths = get_possible_paths(dag_file_dir)
    
    crawl_module_path = None
    crawl_products_path = None
    
    for path in possible_paths:
        test_path = os.path.join(path, "crawl_products.py")
        if os.path.exists(test_path):
            crawl_module_path = path
            crawl_products_path = test_path
            break
    
    if not crawl_module_path:
        relative_path = os.path.abspath(
            os.path.join(dag_file_dir, "..", "..", "src", "pipelines", "crawl")
        )
        test_path = os.path.join(relative_path, "crawl_products.py")
        if os.path.exists(test_path):
            crawl_module_path = relative_path
            crawl_products_path = test_path
    
    if crawl_products_path and os.path.exists(crawl_products_path):
        try:
            spec = importlib.util.spec_from_file_location("crawl_products", crawl_products_path)
            if spec is None or spec.loader is None:
                raise ImportError(f"Không thể load spec từ {crawl_products_path}")
            crawl_products_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(crawl_products_module)
            
            return {
                "crawl_category_products": crawl_products_module.crawl_category_products,
                "get_page_with_requests": crawl_products_module.get_page_with_requests,
                "parse_products_from_html": crawl_products_module.parse_products_from_html,
                "get_total_pages": crawl_products_module.get_total_pages,
            }
        except Exception as e:
            warnings.warn(f"Không thể import crawl_products module: {e}", stacklevel=2)
            error_msg = str(e)
            
            def dummy_func(*args, **kwargs):
                raise ImportError(f"Module crawl_products chưa được import: {error_msg}")
            
            return {
                "crawl_category_products": dummy_func,
                "get_page_with_requests": dummy_func,
                "parse_products_from_html": dummy_func,
                "get_total_pages": dummy_func,
            }
    else:
        if crawl_module_path and crawl_module_path not in sys.path:
            sys.path.insert(0, crawl_module_path)
        
        try:
            from crawl_products import crawl_category_products
            return {
                "crawl_category_products": crawl_category_products,
                "get_page_with_requests": crawl_category_products,
                "parse_products_from_html": crawl_category_products,
                "get_total_pages": crawl_category_products,
            }
        except ImportError as e:
            debug_info = {
                "dag_file_dir": dag_file_dir,
                "cwd": os.getcwd(),
                "possible_paths": possible_paths,
                "crawl_module_path": crawl_module_path,
                "crawl_products_path": crawl_products_path,
                "sys_path": sys.path[:5],
            }
            
            if os.path.exists("/opt/airflow/src"):
                try:
                    debug_info["opt_airflow_src_contents"] = os.listdir("/opt/airflow/src")
                except Exception:
                    pass
            
            raise ImportError(
                f"Không tìm thấy module crawl_products.\n"
                f"Debug info: {debug_info}\n"
                f"Lỗi gốc: {e}"
            ) from e


def import_crawl_products_detail(dag_file_dir):
    """Import crawl_products_detail module and return functions"""
    possible_paths = get_possible_paths(dag_file_dir)
    
    crawl_products_detail_path = None
    for path in possible_paths:
        test_path = os.path.join(path, "crawl_products_detail.py")
        if os.path.exists(test_path):
            crawl_products_detail_path = test_path
            break
    
    if crawl_products_detail_path and os.path.exists(crawl_products_detail_path):
        try:
            spec = importlib.util.spec_from_file_location(
                "crawl_products_detail", crawl_products_detail_path
            )
            if spec is None or spec.loader is None:
                raise ImportError(f"Không thể load spec từ {crawl_products_detail_path}")
            crawl_products_detail_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(crawl_products_detail_module)
            
            return {
                "crawl_product_detail_with_selenium": (
                    crawl_products_detail_module.crawl_product_detail_with_selenium
                ),
                "extract_product_detail": crawl_products_detail_module.extract_product_detail,
            }
        except Exception as e:
            warnings.warn(f"Không thể import crawl_products_detail module: {e}", stacklevel=2)
            error_msg = str(e)
            
            def dummy_func(*args, **kwargs):
                raise ImportError(f"Module crawl_products_detail chưa được import: {error_msg}")
            
            return {
                "crawl_product_detail_with_selenium": dummy_func,
                "extract_product_detail": dummy_func,
            }
    else:
        try:
            from crawl_products_detail import (
                crawl_product_detail_with_selenium,
                extract_product_detail,
            )
            return {
                "crawl_product_detail_with_selenium": crawl_product_detail_with_selenium,
                "extract_product_detail": extract_product_detail,
            }
        except ImportError as e:
            raise ImportError(
                f"Không tìm thấy module crawl_products_detail.\n"
                f"Path: {crawl_products_detail_path}\n"
                f"Lỗi gốc: {e}"
            ) from e


def import_resilience_patterns(dag_file_dir):
    """Import resilience patterns and return classes/functions"""
    possible_paths = get_possible_paths(dag_file_dir)
    
    resilience_module_path = None
    for path in possible_paths:
        test_path = os.path.join(path, "resilience", "__init__.py")
        if os.path.exists(test_path):
            resilience_module_path = os.path.join(path, "resilience")
            break
    
    if resilience_module_path and os.path.exists(resilience_module_path):
        try:
            parent_path = os.path.dirname(resilience_module_path)
            if parent_path not in sys.path:
                sys.path.insert(0, parent_path)
            
            grandparent_path = os.path.dirname(parent_path)
            if grandparent_path not in sys.path:
                sys.path.insert(0, grandparent_path)
            
            if "pipelines" not in sys.modules:
                sys.modules["pipelines"] = types.ModuleType("pipelines")
            if "pipelines.crawl" not in sys.modules:
                sys.modules["pipelines.crawl"] = types.ModuleType("pipelines.crawl")
            if "pipelines.crawl.resilience" not in sys.modules:
                sys.modules["pipelines.crawl.resilience"] = types.ModuleType(
                    "pipelines.crawl.resilience"
                )
            
            if "pipelines.crawl.utils" not in sys.modules and "crawl_utils" in sys.modules:
                sys.modules["pipelines.crawl.utils"] = sys.modules["crawl_utils"]
            
            # Import exceptions
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
            
            # Import circuit_breaker
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
            
            # Import dead_letter_queue
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
            
            # Import graceful_degradation
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
            
            return {
                "CircuitBreaker": CircuitBreaker,
                "CircuitBreakerOpenError": CircuitBreakerOpenError,
                "DeadLetterQueue": DeadLetterQueue,
                "get_dlq": get_dlq,
                "GracefulDegradation": GracefulDegradation,
                "DegradationLevel": DegradationLevel,
                "get_service_health": get_service_health,
                "classify_error": classify_error,
                "CrawlError": CrawlError,
            }
        except Exception as e:
            warnings.warn(f"Không thể import resilience module: {e}", stacklevel=2)
            return create_dummy_resilience()
    else:
        return create_dummy_resilience()


def create_dummy_resilience():
    """Create dummy resilience classes when module not available"""
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

    return {
        "CircuitBreaker": CircuitBreaker,
        "CircuitBreakerOpenError": CircuitBreakerOpenError,
        "DeadLetterQueue": DeadLetterQueue,
        "get_dlq": get_dlq,
        "GracefulDegradation": GracefulDegradation,
        "DegradationLevel": DegradationLevel,
        "get_service_health": get_service_health,
        "classify_error": classify_error,
        "CrawlError": CrawlError,
    }


def import_common_modules(dag_file_dir):
    """Import common modules (analytics, ai, notifications)"""
    common_base_paths = [
        "/opt/airflow/src/common",
        os.path.abspath(os.path.join(dag_file_dir, "..", "..", "src", "common")),
        os.path.abspath(os.path.join(dag_file_dir, "..", "src", "common")),
        "/workspace/src/common",
        os.path.join(os.getcwd(), "src", "common"),
    ]
    
    analytics_path = None
    ai_path = None
    notifications_path = None
    config_path = None
    
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
    
    # Load config first
    if config_path and os.path.exists(config_path):
        try:
            spec = importlib.util.spec_from_file_location("common.config", config_path)
            if spec is not None and spec.loader is not None:
                config_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(config_module)
                warnings.warn(
                    f"✅ Đã load common.config từ {config_path}, .env sẽ được load tự động",
                    stacklevel=2,
                )
        except Exception as e:
            warnings.warn(f"⚠️  Không thể load common.config: {e}", stacklevel=2)
    
    # Import DataAggregator
    DataAggregator = None
    if analytics_path and os.path.exists(analytics_path):
        try:
            spec = importlib.util.spec_from_file_location("common.analytics.aggregator", analytics_path)
            if spec is not None and spec.loader is not None:
                aggregator_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(aggregator_module)
                DataAggregator = aggregator_module.DataAggregator
        except Exception as e:
            warnings.warn(f"Không thể import common.analytics.aggregator module: {e}", stacklevel=2)
    
    # Import AISummarizer
    AISummarizer = None
    if ai_path and os.path.exists(ai_path):
        try:
            spec = importlib.util.spec_from_file_location("common.ai.summarizer", ai_path)
            if spec is not None and spec.loader is not None:
                ai_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(ai_module)
                AISummarizer = ai_module.AISummarizer
        except Exception as e:
            warnings.warn(f"Không thể import common.ai.summarizer module: {e}", stacklevel=2)
    
    # Import DiscordNotifier
    DiscordNotifier = None
    if notifications_path and os.path.exists(notifications_path):
        try:
            spec = importlib.util.spec_from_file_location(
                "common.notifications.discord", notifications_path
            )
            if spec is not None and spec.loader is not None:
                notifications_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(notifications_module)
                DiscordNotifier = notifications_module.DiscordNotifier
        except Exception as e:
            warnings.warn(f"Không thể import common.notifications.discord module: {e}", stacklevel=2)
    
    return {
        "DataAggregator": DataAggregator,
        "AISummarizer": AISummarizer,
        "DiscordNotifier": DiscordNotifier,
    }


def import_all_modules(dag_file_dir):
    """Import all external modules and return a dictionary with all imports"""
    # Import utils first (needed by other modules)
    import_crawl_utils(dag_file_dir)
    
    # Import crawl modules
    crawl_products_funcs = import_crawl_products(dag_file_dir)
    crawl_products_detail_funcs = import_crawl_products_detail(dag_file_dir)
    resilience_classes = import_resilience_patterns(dag_file_dir)
    common_modules = import_common_modules(dag_file_dir)
    
    # Combine all imports
    return {
        **crawl_products_funcs,
        **crawl_products_detail_funcs,
        **resilience_classes,
        **common_modules,
    }

