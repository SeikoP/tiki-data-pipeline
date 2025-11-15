"""
Load pipeline để load dữ liệu đã transform vào database
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


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


# Import PostgresStorage từ crawl storage
try:
    from ...crawl.storage.postgres_storage import PostgresStorage
except ImportError:
    try:
        import sys
        from pathlib import Path

        # Thêm src vào path
        src_path = Path(__file__).parent.parent.parent.parent
        sys.path.insert(0, str(src_path))
        from pipelines.crawl.storage.postgres_storage import PostgresStorage
    except ImportError:
        logger.warning("⚠️  Không thể import PostgresStorage, chỉ hỗ trợ load từ file")
        PostgresStorage = None


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
        self.enable_db = enable_db and PostgresStorage is not None
        self.stats = {
            "total_loaded": 0,
            "success_count": 0,
            "failed_count": 0,
            "db_loaded": 0,
            "file_loaded": 0,
            "errors": [],
        }

        # Khởi tạo PostgresStorage nếu enable_db
        self.db_storage: PostgresStorage | None = None
        if self.enable_db:
            try:
                self.db_storage = PostgresStorage(
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
                saved_count = self.db_storage.save_products(
                    products, upsert=upsert, batch_size=self.batch_size
                )
                self.stats["db_loaded"] = saved_count
                self.stats["success_count"] = saved_count
                logger.info(f"✅ Đã load {saved_count} products vào database")
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
        save_to_db: bool = True,
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

    def get_stats(self) -> dict[str, Any]:
        """Lấy thống kê load"""
        return self.stats.copy()

    def close(self):
        """Đóng kết nối database"""
        if self.db_storage:
            self.db_storage.close()
            logger.info("✅ Đã đóng kết nối database")
