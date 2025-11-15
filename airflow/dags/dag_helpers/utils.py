"""
Utility functions for Tiki crawl products DAG
"""
import warnings
from typing import Any

# Import Variable và TaskGroup với suppress warning
try:
    # Thử import từ airflow.sdk (Airflow 3.x)
    from airflow.sdk import Variable as _Variable
except ImportError:
    # Fallback: dùng airflow.models (Airflow 2.x)
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


def get_dag_file_dir():
    """Lấy đường dẫn thư mục chứa DAG file - placeholder, sẽ được set từ config"""
    import os
    return os.path.dirname(os.path.abspath(__file__))


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


def atomic_write_file(filepath: str, data: Any, **context):
    """
    Ghi file an toàn (atomic write) để tránh corrupt

    Sử dụng temporary file và rename để đảm bảo atomicity
    """
    import json
    import os
    import shutil
    from pathlib import Path

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

