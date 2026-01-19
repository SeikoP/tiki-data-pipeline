"""
Load pipeline để load dữ liệu đã transform vào database
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

logger = logging.getLogger(__name__)

# Import PostgresStorage với TYPE_CHECKING để tránh mypy errors
if TYPE_CHECKING:
    pass  # PostgresStorageType không cần thiết
else:
    pass  # type: ignore[misc, assignment]

PostgresStorageClass: type[Any] | None = None


class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder để serialize datetime objects"""

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def serialize_for_json(obj: Any) -> Any:
    """Recursively convert datetime objects to ISO format strings"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: serialize_for_json(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [serialize_for_json(item) for item in obj]
    else:
        return obj


# Thêm /opt/airflow/src vào sys.path nếu chưa có (cho Docker environment)
src_paths = [
    Path("/opt/airflow/src"),  # Docker default path
    Path(__file__).parent.parent.parent,  # Từ loader.py lên src
]

for src_path in src_paths:
    if src_path.exists() and str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
        break

PostgresStorageClass = None
try:
    # Ưu tiên 1: Absolute import (sau khi đã thêm src vào path)
    from pipelines.crawl.storage import (
        PostgresStorage as _PostgresStorage,  # type: ignore[attr-defined]
    )

    PostgresStorageClass = _PostgresStorage  # type: ignore[assignment]
except ImportError:
    try:
        # Ưu tiên 2: Absolute import từ file trực tiếp
        from pipelines.crawl.storage.postgres_storage import (
            PostgresStorage as _PostgresStorage2,  # type: ignore[attr-defined]
        )

        PostgresStorageClass = _PostgresStorage2  # type: ignore[assignment]
    except ImportError:
        try:
            # Ưu tiên 3: Relative import (nếu chạy như package)
            from ...crawl.storage import (
                PostgresStorage as _PostgresStorage3,  # type: ignore[attr-defined]
            )

            PostgresStorageClass = _PostgresStorage3  # type: ignore[assignment]
        except ImportError:
            try:
                import importlib.util
                import os

                # Tìm đường dẫn đến postgres_storage.py
                # Lấy đường dẫn tuyệt đối của file hiện tại
                current_file = Path(__file__).resolve()

                # Tìm đường dẫn src (có thể là parent hoặc grandparent)
                # loader.py ở: src/pipelines/load/loader.py
                # postgres_storage.py ở: src/pipelines/crawl/storage/postgres_storage.py
                possible_paths = [
                    # Từ /opt/airflow/src (Docker default - ưu tiên)
                    Path("/opt/airflow/src/pipelines/crawl/storage/postgres_storage.py"),
                    # Từ current file: loader.py -> pipelines -> crawl/storage/postgres_storage.py
                    current_file.parent.parent / "crawl" / "storage" / "postgres_storage.py",
                    # Từ current file lên 1 cấp nữa (trong trường hợp đặc biệt)
                    current_file.parent.parent.parent
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
                    test_path = Path(path) if isinstance(path, str) else path
                    if test_path.exists() and test_path.is_file():
                        postgres_storage_path = test_path
                        break

                if postgres_storage_path:
                    # Sử dụng importlib để load trực tiếp từ file
                    spec = importlib.util.spec_from_file_location(
                        "postgres_storage", postgres_storage_path
                    )
                    if spec and spec.loader:
                        postgres_storage_module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(postgres_storage_module)
                        PostgresStorageClass = postgres_storage_module.PostgresStorage  # type: ignore[assignment]
                else:
                    # Nếu không tìm thấy file, thử thêm src vào path và import absolute
                    # loader.py ở: src/pipelines/load/loader.py
                    # src ở: current_file.parent.parent.parent
                    src_path = current_file.parent.parent.parent
                    if src_path.exists() and str(src_path) not in sys.path:
                        sys.path.insert(0, str(src_path))

                        try:
                            from pipelines.crawl.storage import (
                                PostgresStorage as _PostgresStorage4,  # type: ignore[attr-defined]
                            )

                            PostgresStorageClass = _PostgresStorage4  # type: ignore[assignment]
                        except ImportError:
                            try:
                                from pipelines.crawl.storage.postgres_storage import (
                                    PostgresStorage as _PostgresStorage5,  # type: ignore[attr-defined]
                                )

                                PostgresStorageClass = _PostgresStorage5  # type: ignore[assignment]
                            except ImportError:
                                # Không thể import, sẽ dùng file-based loading
                                PostgresStorageClass = None
            except Exception:
                # Nếu importlib fail, set None
                PostgresStorageClass = None

# Nếu vẫn không import được, log warning
if PostgresStorageClass is None:
    logger.warning(
        "⚠️  Không thể import PostgresStorage. Chỉ hỗ trợ load từ file (không có database)."
    )


class DataLoader:
    """Class để load dữ liệu đã transform vào database hoặc file"""

    def __init__(
        self,
        database: str | None = None,
        host: str | None = None,
        port: int = 5432,
        user: str | None = None,
        password: str | None = None,
        batch_size: int = 100,
        enable_db: bool = True,
    ):
        """
        Khởi tạo DataLoader

        Args:
            database: Database name (mặc định: crawl_data)
            host: PostgreSQL host
            port: PostgreSQL port
            user: PostgreSQL user
            password: PostgreSQL password
            batch_size: Kích thước batch khi insert
            enable_db: Có enable database loading không
        """
        self.batch_size = batch_size
        self.enable_db = enable_db and PostgresStorageClass is not None
        self.stats: dict[str, Any] = {
            "total_loaded": 0,
            "success_count": 0,
            "failed_count": 0,
            "db_loaded": 0,
            "file_loaded": 0,
            "inserted_count": 0,  # Số products mới được INSERT
            "updated_count": 0,  # Số products đã có được UPDATE
            "errors": [],
        }

        # Khởi tạo PostgresStorage nếu enable_db
        self.db_storage: Any = None
        if self.enable_db:
            try:
                if PostgresStorageClass is None:
                    raise ImportError("PostgresStorageClass not available")
                self.db_storage = PostgresStorageClass(
                    host=host,
                    port=port,
                    database=database or "crawl_data",
                    user=user,
                    password=password,
                )
                logger.info("✅ Đã kết nối với PostgreSQL database")
            except Exception as e:
                logger.warning(f"⚠️  Không thể kết nối database: {e}")
                self.enable_db = False
                self.db_storage = None

    def load_products(
        self,
        products: list[dict[str, Any]],
        save_to_file: str | None = None,
        upsert: bool = True,
        validate_before_load: bool = True,
    ) -> dict[str, Any]:
        """
        Load danh sách products vào database và/hoặc file

        Args:
            products: Danh sách products đã transform
            save_to_file: Đường dẫn file để lưu (nếu cần)
            upsert: Nếu True, update nếu đã tồn tại
            validate_before_load: Có validate trước khi load không

        Returns:
            Stats dictionary
        """
        self.stats = {
            "total_loaded": len(products),
            "success_count": 0,
            "failed_count": 0,
            "db_loaded": 0,
            "file_loaded": 0,
            "errors": [],
        }

        if not products:
            logger.warning("⚠️  Danh sách products rỗng")
            return self.stats

        # Validate trước nếu cần
        if validate_before_load:
            valid_products = []
            for product in products:
                # Kiểm tra required fields
                if not product.get("product_id") or not product.get("name"):
                    self.stats["failed_count"] += 1
                    self.stats["errors"].append(
                        f"Missing required fields: product_id={product.get('product_id')}"
                    )
                    continue
                valid_products.append(product)
            products = valid_products

        # Load vào database
        if self.enable_db and self.db_storage:
            try:
                result: dict[str, Any] = self.db_storage.save_products(
                    products, upsert=upsert, batch_size=self.batch_size
                )

                # Xử lý kết quả (always dict with consistent structure)
                saved_count: int = result.get("saved_count", 0)
                inserted_count: int = result.get("inserted_count", 0)
                updated_count: int = result.get("updated_count", 0)
                self.stats["db_loaded"] = saved_count
                self.stats["success_count"] = saved_count
                self.stats["inserted_count"] = inserted_count
                self.stats["updated_count"] = updated_count
                logger.info(f"✅ Đã load {saved_count} products vào database")
                if upsert:
                    logger.info(f"   - INSERT (mới): {inserted_count}")
                    logger.info(f"   - UPDATE (đã có): {updated_count}")
            except Exception as e:
                error_msg = f"Lỗi khi load vào database: {str(e)}"
                self.stats["errors"].append(error_msg)
                self.stats["failed_count"] += len(products)
                logger.error(f"❌ {error_msg}")

        # Load vào file nếu cần
        if save_to_file:
            try:
                file_path = Path(save_to_file)
                file_path.parent.mkdir(parents=True, exist_ok=True)

                # Serialize datetime objects trong products trước khi save
                serialized_products = serialize_for_json(products)

                # Chuẩn bị dữ liệu để lưu
                output_data = {
                    "loaded_at": datetime.now().isoformat(),
                    "total_products": len(products),
                    "stats": {
                        "db_loaded": self.stats.get("db_loaded", 0),
                        "file_loaded": len(products),
                    },
                    "products": serialized_products,
                }

                with open(file_path, "w", encoding="utf-8", newline="\n") as f:
                    json.dump(output_data, f, ensure_ascii=False, indent=2, cls=DateTimeEncoder)

                self.stats["file_loaded"] = len(products)
                logger.info(f"✅ Đã lưu {len(products)} products vào file: {save_to_file}")
            except Exception as e:
                error_msg = f"Lỗi khi lưu vào file: {str(e)}"
                self.stats["errors"].append(error_msg)
                logger.error(f"❌ {error_msg}")
                import traceback

                logger.debug(traceback.format_exc())

        # Update success count nếu chưa có
        if self.stats["success_count"] == 0 and self.stats["file_loaded"] > 0:
            self.stats["success_count"] = self.stats["file_loaded"]

        return self.stats

    def load_from_file(
        self,
        input_file: str,
        save_to_file: str | None = None,
        upsert: bool = True,
    ) -> dict[str, Any]:
        """
        Load products từ file JSON

        Args:
            input_file: Đường dẫn file JSON input
            save_to_db: Có lưu vào database không
            save_to_file: Đường dẫn file output (nếu cần)
            upsert: Nếu True, update nếu đã tồn tại

        Returns:
            Stats dictionary
        """
        file_path = Path(input_file)
        if not file_path.exists():
            error_msg = f"File không tồn tại: {input_file}"
            self.stats["errors"].append(error_msg)
            logger.error(f"❌ {error_msg}")
            return self.stats

        try:
            with open(file_path, encoding="utf-8") as f:
                data = json.load(f)

            # Extract products từ data
            # Hỗ trợ nhiều format:
            # - {"products": [...]}
            # - {"data": {"products": [...]}}
            # - [...] (trực tiếp là list)
            products = []
            if isinstance(data, list):
                products = data
            elif isinstance(data, dict):
                if "products" in data:
                    products = data["products"]
                elif "data" in data and isinstance(data["data"], dict):
                    products = data["data"].get("products", [])

            if not products:
                logger.warning("⚠️  Không tìm thấy products trong file")
                return self.stats

            logger.info(f"📂 Đã load {len(products)} products từ file: {input_file}")

            # Load vào database và/hoặc file
            return self.load_products(
                products,
                save_to_file=save_to_file,
                upsert=upsert,
                validate_before_load=True,
            )

        except json.JSONDecodeError as e:
            error_msg = f"Lỗi parse JSON: {str(e)}"
            self.stats["errors"].append(error_msg)
            logger.error(f"❌ {error_msg}")
            return self.stats
        except Exception as e:
            error_msg = f"Lỗi khi load từ file: {str(e)}"
            self.stats["errors"].append(error_msg)
            logger.error(f"❌ {error_msg}")
            return self.stats

    def load_categories(
        self,
        categories: list[dict[str, Any]],
        save_to_file: str | None = None,
        upsert: bool = True,
        validate_before_load: bool = True,
    ) -> dict[str, Any]:
        """
        Load danh sách categories vào database và/hoặc file

        Args:
            categories: Danh sách categories đã transform
            save_to_file: Đường dẫn file để lưu (nếu cần)
            upsert: Nếu True, update nếu đã tồn tại
            validate_before_load: Có validate trước khi load không

        Returns:
            Stats dictionary
        """
        self.stats = {
            "total_loaded": len(categories),
            "success_count": 0,
            "failed_count": 0,
            "db_loaded": 0,
            "file_loaded": 0,
            "errors": [],
        }

        if not categories:
            logger.warning("⚠️  Danh sách categories rỗng")
            return self.stats

        # Validate trước nếu cần
        if validate_before_load:
            valid_categories = []
            for cat in categories:
                # Kiểm tra required fields
                if not cat.get("url") or not cat.get("name"):
                    self.stats["failed_count"] += 1
                    self.stats["errors"].append(
                        f"Missing required fields: url={cat.get('url')}, name={cat.get('name')}"
                    )
                    continue
                valid_categories.append(cat)
            categories = valid_categories

        # Load vào database
        if self.enable_db and self.db_storage:
            logger.info(
                "ℹ️ Skip saving categories to DB (Table 'categories' removed). Only saving to file."
            )
            # Database load logic removed as requested

        # Load vào file nếu cần
        if save_to_file:
            try:
                file_path = Path(save_to_file)
                file_path.parent.mkdir(parents=True, exist_ok=True)

                # Serialize datetime objects trong categories trước khi save
                serialized_categories = serialize_for_json(categories)

                # Chuẩn bị dữ liệu để lưu
                output_data = {
                    "loaded_at": datetime.now().isoformat(),
                    "total_categories": len(categories),
                    "stats": {
                        "db_loaded": self.stats.get("db_loaded", 0),
                        "file_loaded": len(categories),
                    },
                    "categories": serialized_categories,
                }

                with open(file_path, "w", encoding="utf-8", newline="\n") as f:
                    json.dump(output_data, f, ensure_ascii=False, indent=2, cls=DateTimeEncoder)

                self.stats["file_loaded"] = len(categories)
                logger.info(f"✅ Đã lưu {len(categories)} categories vào file: {save_to_file}")
            except Exception as e:
                error_msg = f"Lỗi khi lưu vào file: {str(e)}"
                self.stats["errors"].append(error_msg)
                logger.error(f"❌ {error_msg}")
                import traceback

                logger.debug(traceback.format_exc())

        # Update success count nếu chưa có
        if self.stats["success_count"] == 0 and self.stats["file_loaded"] > 0:
            self.stats["success_count"] = self.stats["file_loaded"]

        return self.stats

    def get_stats(self) -> dict[str, Any]:
        """Lấy thống kê load"""
        return self.stats.copy()

    def close(self):
        """Đóng kết nối database"""
        if self.db_storage:
            self.db_storage.close()
            logger.info("✅ Đã đóng kết nối database")
