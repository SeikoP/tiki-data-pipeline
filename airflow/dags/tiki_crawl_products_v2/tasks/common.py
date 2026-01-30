from __future__ import annotations

# Import all bootstrap globals (paths, config, dynamic imports, singletons).
# This preserves legacy behavior without renaming any globals referenced by task callables.
from tiki_crawl_products_v2.bootstrap import Any, Path, json, logging, os, shutil, sys


def get_logger(context):
    """
    Lấy logger từ context (Airflow 3.x compatible)
    """
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
    """Sửa sys.path và sys.modules để đảm bảo pipelines có thể được import đúng cách.

    Xóa các đường dẫn con như /opt/airflow/src/pipelines khỏi sys.path, xóa các fake modules khỏi
    sys.modules, và chỉ giữ lại /opt/airflow/src.
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
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "src")
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


def get_selenium_driver_pool_class(logger):
    """
    Import SeleniumDriverPool helper to reduce complexity in tasks.
    """
    _SeleniumDriverPool = None

    try:
        # 1. Try standard package import first (preferred)
        _fix_sys_path_for_pipelines_import(logger)
        from pipelines.crawl.crawl_utils import SeleniumDriverPool

        _SeleniumDriverPool = SeleniumDriverPool
        logger.info("✅ Imported SeleniumDriverPool from pipelines.crawl.crawl_utils")
        return _SeleniumDriverPool
    except ImportError:
        logger.warning("⚠️ Standard import failed, trying file-based import for SeleniumDriverPool")

    try:
        import importlib.util

        src_path = Path("/opt/airflow/src")
        if not src_path.exists():
            # Try local dev path
            src_path = Path(__file__).parent.parent.parent.parent.parent / "src"

        utils_path = src_path / "pipelines" / "crawl" / "crawl_utils.py"
        if utils_path.exists():
            spec = importlib.util.spec_from_file_location("crawl_utils_task", str(utils_path))
            if spec and spec.loader:
                utils_mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(utils_mod)
                _SeleniumDriverPool = getattr(utils_mod, "SeleniumDriverPool", None)
    except Exception as e:
        logger.error(f"❌ Failed to import SeleniumDriverPool: {e}")

    if _SeleniumDriverPool is None:
        raise ImportError("Could not import SeleniumDriverPool from any source")

    return _SeleniumDriverPool


def get_postgres_storage_class(logger):
    """
    Import PostgresStorage helper to reduce complexity in tasks.
    """
    _PostgresStorage = None

    try:
        # 1. Try standard package import first (preferred)
        _fix_sys_path_for_pipelines_import(logger)
        from pipelines.crawl.storage.postgres_storage import PostgresStorage

        _PostgresStorage = PostgresStorage
        logger.info("✅ Imported PostgresStorage from pipelines.crawl.storage.postgres_storage")
        return _PostgresStorage
    except ImportError:
        logger.warning("⚠️ Standard import failed, trying file-based import for PostgresStorage")

    try:
        import importlib.util

        src_path = Path("/opt/airflow/src")
        if not src_path.exists():
            # Try local dev path
            src_path = Path(__file__).parent.parent.parent.parent.parent / "src"

        storage_path = src_path / "pipelines" / "crawl" / "storage" / "postgres_storage.py"
        if storage_path.exists():
            spec = importlib.util.spec_from_file_location(
                "postgres_storage_task", str(storage_path)
            )
            if spec and spec.loader:
                storage_mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(storage_mod)
                _PostgresStorage = getattr(storage_mod, "PostgresStorage", None)
    except Exception as e:
        logger.error(f"❌ Failed to import PostgresStorage: {e}")

    if _PostgresStorage is None:
        raise ImportError("Could not import PostgresStorage from any source")

    return _PostgresStorage
