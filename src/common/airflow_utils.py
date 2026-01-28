import os
import sys
import logging
import warnings
from pathlib import Path
from typing import Any, Optional
try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

# Try to import Airflow components, handle missing imports gracefull for non-airflow environments
try:
    from airflow.models import Variable
except ImportError:
    Variable = None

try:
    from airflow.utils.task_group import TaskGroup
except ImportError:
    # Fallback for older Airflow versions or if running outside Airflow
    try:
        from airflow.sdk import TaskGroup
    except ImportError:
        class TaskGroup:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

def safe_import_attr(module_path: str, attr_name: str, fallback_paths: list[Any] | None = None):
    """
    Import an attribute from a module with optional fallback file paths.

    Args:
        module_path: Dot-separated path to the module (e.g., 'pipelines.load.module')
        attr_name: Name of the attribute to import (e.g., 'load_categories')
        fallback_paths: List of directory paths to check for the .py file if standard import fails

    Returns:
        The imported attribute or None if not found
    """
    try:
        # Standard import
        module = __import__(module_path, fromlist=[attr_name])
        return getattr(module, attr_name, None)
    except ImportError:
        if not fallback_paths:
            return None

        import importlib.util

        # Try fallback file paths
        for base in fallback_paths:
            # Convert module path to file path
            rel_file_path = module_path.replace(".", "/") + ".py"
            p = Path(base) / rel_file_path

            if p.exists():
                spec = importlib.util.spec_from_file_location(module_path, str(p))
                if spec and spec.loader:
                    try:
                        mod = importlib.util.module_from_spec(spec)
                        # Pre-populate sys.path if needed for internal imports in the module
                        if str(Path(base)) not in sys.path:
                            sys.path.insert(0, str(Path(base)))
                        spec.loader.exec_module(mod)
                        return getattr(mod, attr_name, None)
                    except Exception as e:
                        warnings.warn(f"Failed to load module from {p}: {e}", stacklevel=2)
                        continue
        return None

def get_variable(key: str, default: Any = None) -> Any:
    """
    Get generic variable from Airflow Variable or Environment Variable.
    """
    # 1. Try Airflow Variable first
    if Variable:
        try:
            return Variable.get(key, default_var=default)
        except Exception:
            pass
    
    # 2. Try Environment Variable
    return os.getenv(key, default)

def get_int_variable(key: str, default: int) -> int:
    """Safely parse Airflow variable as integer with fallback and warning"""
    raw_value = get_variable(key, default=str(default))
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        logging.warning(
            "Invalid integer value for Airflow variable %s=%r, falling back to default %r",
            key,
            raw_value,
            default,
        )
        return default

def load_env_file(search_paths: list[Path] | None = None) -> bool:
    """
    Load .env file for the DAG/Process.
    """
    if load_dotenv is None:
        logging.warning("⚠️ python-dotenv not installed, skipping .env load")
        return False

    if not search_paths:
        search_paths = [
            Path("/opt/airflow/.env"),                      # Docker root
            Path(os.getcwd()) / ".env"                      # CWD
        ]
        # Add local dev path relative to this file if possible, 
        # but since this file is in src/common, we can try to look up to project root
        # src/common/airflow_utils.py -> src/common -> src -> project_root
        project_root = Path(__file__).parent.parent.parent
        search_paths.append(project_root / ".env")

    for env_p in search_paths:
        if env_p.exists():
            load_dotenv(env_p, override=True)
            logging.info(f"✅ Loaded .env from {env_p}")
            return True
            
    logging.warning("⚠️ Could not find .env file")
    return False
