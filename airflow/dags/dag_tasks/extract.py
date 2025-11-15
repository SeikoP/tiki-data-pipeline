"""
Extract and load tasks for Tiki crawl products DAG
"""
import json
import os
import sys
import importlib.util
from pathlib import Path
from typing import Any

from ..dag_helpers.config import CATEGORIES_FILE, CATEGORIES_TREE_FILE
from ..dag_helpers.utils import get_logger, Variable


def extract_and_load_categories_to_db(**context) -> dict[str, Any]:
    """
    Task 0: Extract categories từ categories_tree.json và load vào database

    Returns:
        Dict: Stats về việc load categories
    """
    logger = get_logger(context)
    logger.info("=" * 70)
    logger.info("📁 TASK: Extract & Load Categories to Database")
    logger.info("=" * 70)

    try:
        # Import extract và load modules
        try:
            # Thử import từ đường dẫn trong Docker/Airflow
            # Tìm đường dẫn đến extract_categories.py
            possible_paths = [
                "/opt/airflow/src/pipelines/extract/extract_categories.py",
                os.path.join(
                    os.path.dirname(__file__), "..", "..", "..", "src", "pipelines", "extract", "extract_categories.py"
                ),
                os.path.join(os.getcwd(), "src", "pipelines", "extract", "extract_categories.py"),
            ]

            extract_module_path = None
            for path in possible_paths:
                test_path = Path(path)
                if test_path.exists():
                    extract_module_path = test_path
                    break

            if extract_module_path:
                spec = importlib.util.spec_from_file_location("extract_categories", extract_module_path)
                if spec and spec.loader:
                    extract_module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(extract_module)
                    extract_categories_from_tree_file = extract_module.extract_categories_from_tree_file
                else:
                    raise ImportError("Không thể load extract_categories module")
            else:
                raise ImportError("Không tìm thấy extract_categories.py")
        except Exception as e:
            logger.warning(f"⚠️  Không thể import extract module: {e}")
            logger.info("Thử import trực tiếp...")
            # Fallback: thử import trực tiếp
            try:
                from pipelines.extract.extract_categories import extract_categories_from_tree_file
            except ImportError:
                # Thêm src vào path
                dag_file_dir = os.path.dirname(os.path.abspath(__file__))
                src_path = os.path.abspath(os.path.join(dag_file_dir, "..", "..", "..", "src"))
                if src_path not in sys.path:
                    sys.path.insert(0, src_path)
                from pipelines.extract.extract_categories import extract_categories_from_tree_file

        # Import DataLoader
        try:
            from pipelines.load.loader import DataLoader
        except ImportError:
            # Thêm src vào path nếu chưa có
            dag_file_dir = os.path.dirname(os.path.abspath(__file__))
            src_path = os.path.abspath(os.path.join(dag_file_dir, "..", "..", "..", "src"))
            if src_path not in sys.path:
                sys.path.insert(0, src_path)
            from pipelines.load.loader import DataLoader

        # 1. Extract categories từ tree file
        tree_file = str(CATEGORIES_TREE_FILE)
        logger.info(f"📖 Đang extract categories từ: {tree_file}")

        if not os.path.exists(tree_file):
            logger.warning(f"⚠️  Không tìm thấy file: {tree_file}")
            logger.info("Bỏ qua task này, categories có thể đã được load trước đó")
            return {
                "total_loaded": 0,
                "db_loaded": 0,
                "success_count": 0,
                "failed_count": 0,
                "skipped": True,
            }

        categories = extract_categories_from_tree_file(tree_file)
        logger.info(f"✅ Đã extract {len(categories)} categories")

        # 2. Load vào database
        logger.info("💾 Đang load categories vào database...")

        # Lấy credentials từ environment variables
        loader = DataLoader(
            database=os.getenv("POSTGRES_DB", "crawl_data"),
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            user=os.getenv("POSTGRES_USER", "airflow_user"),
            password=os.getenv("POSTGRES_PASSWORD", ""),
            batch_size=100,
            enable_db=True,
        )

        try:
            stats = loader.load_categories(
                categories,
                save_to_file=None,  # Không lưu file, chỉ load vào DB
                upsert=True,
                validate_before_load=True,
            )

            logger.info(f"✅ Đã load {stats['db_loaded']} categories vào database")
            logger.info(f"   - Tổng số: {stats['total_loaded']}")
            logger.info(f"   - Thành công: {stats['success_count']}")
            logger.info(f"   - Thất bại: {stats['failed_count']}")

            if stats.get("errors"):
                logger.warning(f"⚠️  Có {len(stats['errors'])} lỗi (hiển thị 5 đầu tiên):")
                for error in stats["errors"][:5]:
                    logger.warning(f"   - {error}")

            loader.close()
            return stats

        except Exception as e:
            logger.error(f"❌ Lỗi khi load vào database: {e}", exc_info=True)
            loader.close()
            raise

    except Exception as e:
        logger.error(f"❌ Lỗi trong extract_and_load_categories_to_db: {e}", exc_info=True)
        raise


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

