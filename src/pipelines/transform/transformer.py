"""
Transform pipeline để làm sạch, validate và chuẩn hóa dữ liệu sản phẩm
"""

import logging
import re
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


class DataTransformer:
    """Class để transform dữ liệu sản phẩm trước khi load vào database"""

    def __init__(
        self,
        strict_validation: bool = False,
        remove_invalid: bool = True,
        normalize_fields: bool = True,
    ):
        """
        Khởi tạo DataTransformer

        Args:
            strict_validation: Nếu True, bỏ qua products không pass validation
            remove_invalid: Nếu True, loại bỏ products không hợp lệ
            normalize_fields: Nếu True, chuẩn hóa các trường dữ liệu
        """
        self.strict_validation = strict_validation
        self.remove_invalid = remove_invalid
        self.normalize_fields = normalize_fields
        self.stats = {
            "total_processed": 0,
            "valid_products": 0,
            "invalid_products": 0,
            "duplicates_removed": 0,
            "errors": [],
        }

    def transform_products(
        self, products: list[dict[str, Any]], validate: bool = True
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """
        Transform danh sách products

        Args:
            products: Danh sách products cần transform
            validate: Có validate dữ liệu không

        Returns:
            Tuple (transformed_products, stats)
        """
        self.stats = {
            "total_processed": len(products),
            "valid_products": 0,
            "invalid_products": 0,
            "duplicates_removed": 0,
            "errors": [],
        }

        if not products:
            logger.warning("⚠️  Danh sách products rỗng")
            return [], self.stats

        transformed = []
        seen_product_ids = set()

        for idx, product in enumerate(products):
            try:
                # Transform product
                transformed_product = self.transform_product(product)

                if not transformed_product:
                    self.stats["invalid_products"] += 1
                    continue

                # Kiểm tra duplicate
                product_id = transformed_product.get("product_id")
                if product_id in seen_product_ids:
                    self.stats["duplicates_removed"] += 1
                    logger.debug(f"⚠️  Duplicate product_id: {product_id}")
                    continue

                seen_product_ids.add(product_id)

                # Validate nếu cần
                if validate:
                    is_valid, error = self.validate_product(transformed_product)
                    if not is_valid:
                        if self.remove_invalid:
                            self.stats["invalid_products"] += 1
                            self.stats["errors"].append(f"Product {product_id}: {error}")
                            continue
                        elif self.strict_validation:
                            raise ValueError(f"Invalid product {product_id}: {error}")

                transformed.append(transformed_product)
                self.stats["valid_products"] += 1

            except Exception as e:
                self.stats["invalid_products"] += 1
                error_msg = f"Error transforming product at index {idx}: {str(e)}"
                self.stats["errors"].append(error_msg)
                logger.error(f"❌ {error_msg}")

                if self.strict_validation:
                    raise

        logger.info(
            f"✅ Transform hoàn tất: {self.stats['valid_products']}/{self.stats['total_processed']} products hợp lệ"
        )

        return transformed, self.stats

    def transform_product(self, product: dict[str, Any]) -> dict[str, Any] | None:
        """
        Transform một product

        Args:
            product: Product dictionary cần transform

        Returns:
            Transformed product dictionary hoặc None nếu không hợp lệ
        """
        if not product:
            return None

        try:
            # Copy product để không modify original
            transformed = product.copy()

            # Normalize fields nếu cần
            if self.normalize_fields:
                transformed = self._normalize_product(transformed)

            # Transform format để phù hợp với database schema
            transformed = self._transform_to_db_format(transformed)

            return transformed

        except Exception as e:
            logger.error(f"❌ Lỗi khi transform product: {e}")
            return None

    def _normalize_product(self, product: dict[str, Any]) -> dict[str, Any]:
        """Chuẩn hóa các trường dữ liệu"""
        # Normalize product_id
        if "product_id" in product:
            product["product_id"] = str(product["product_id"]).strip()

        # Normalize name
        if "name" in product and product["name"]:
            product["name"] = self._normalize_text(product["name"])

        # Normalize URL
        if "url" in product and product["url"]:
            product["url"] = self._normalize_url(product["url"])

        # Normalize brand
        if "brand" in product:
            if isinstance(product["brand"], str):
                # Loại bỏ prefix "Thương hiệu: "
                brand = product["brand"].replace("Thương hiệu: ", "").strip()
                product["brand"] = brand if brand else None
            elif isinstance(product["brand"], dict):
                product["brand"] = product["brand"].get("name") or None

        # Normalize seller
        if "seller" in product and isinstance(product["seller"], dict):
            seller = product["seller"]
            # Đảm bảo seller_is_official là boolean
            if "is_official" in seller:
                seller["is_official"] = bool(seller["is_official"])
            # Normalize seller name
            if "name" in seller and seller["name"]:
                seller["name"] = self._normalize_text(seller["name"])
            # Normalize seller_id
            if "seller_id" in seller and seller["seller_id"]:
                seller["seller_id"] = str(seller["seller_id"]).strip()

        # Normalize price
        if "price" in product and isinstance(product["price"], dict):
            price = product["price"]
            # Đảm bảo các trường price là số
            if "current_price" in price:
                price["current_price"] = self._parse_float(price["current_price"])
            if "original_price" in price:
                price["original_price"] = self._parse_float(price["original_price"])
            # Tính lại discount_percent nếu cần
            if price.get("current_price") and price.get("original_price"):
                if price["original_price"] > 0:
                    discount = (
                        (price["original_price"] - price["current_price"])
                        / price["original_price"]
                        * 100
                    )
                    price["discount_percent"] = round(discount, 2)
            elif "discount_percent" in price:
                price["discount_percent"] = self._parse_float(price["discount_percent"])

        # Normalize rating
        if "rating" in product and isinstance(product["rating"], dict):
            rating = product["rating"]
            if "average" in rating:
                rating["average"] = self._parse_float(rating["average"])
            if "total_reviews" in rating:
                rating["total_reviews"] = self._parse_int(rating["total_reviews"])

        # Normalize sales_count
        if "sales_count" in product:
            product["sales_count"] = self._parse_int(product["sales_count"])

        # Normalize category_url
        if "category_url" in product and product["category_url"]:
            product["category_url"] = self._normalize_url(product["category_url"])

        return product

    def _transform_to_db_format(self, product: dict[str, Any]) -> dict[str, Any]:
        """Transform format để phù hợp với database schema"""
        db_product = {
            "product_id": product.get("product_id"),
            "name": product.get("name"),
            "url": product.get("url"),
            "image_url": product.get("image_url"),
            "category_url": product.get("category_url"),
            "category_id": product.get("category_id"),  # Extract từ category_url nếu chưa có
            "category_path": product.get("category_path")
            or [],  # Default [] nếu không có breadcrumb
            "sales_count": product.get("sales_count"),
        }

        # Extract category_id từ category_url nếu chưa có
        if not db_product.get("category_id") and db_product.get("category_url"):
            try:
                from ..crawl.utils import extract_category_id_from_url

                category_id = extract_category_id_from_url(db_product["category_url"])
                if category_id:
                    db_product["category_id"] = category_id
            except Exception:
                pass  # Nếu không import được, bỏ qua

        # Price fields (flatten từ dict)
        price = product.get("price", {})
        if isinstance(price, dict):
            db_product["price"] = price.get("current_price")
            db_product["original_price"] = price.get("original_price")
            db_product["discount_percent"] = (
                int(round(price["discount_percent"]))
                if price.get("discount_percent") is not None
                else None
            )
        else:
            db_product["price"] = None
            db_product["original_price"] = None
            db_product["discount_percent"] = None

        # Rating fields (flatten từ dict)
        rating = product.get("rating", {})
        if isinstance(rating, dict):
            db_product["rating_average"] = rating.get("average")
            db_product["review_count"] = rating.get("total_reviews") or rating.get("review_count")
        else:
            db_product["rating_average"] = None
            db_product["review_count"] = None

        # Description
        db_product["description"] = product.get("description")

        # Seller fields (flatten từ dict) - với validation để loại bỏ giá trị rác
        seller = product.get("seller", {})
        if isinstance(seller, dict):
            raw_seller_name = seller.get("name")
            db_product["seller_name"] = self._validate_seller_name(raw_seller_name)
            db_product["seller_id"] = seller.get("seller_id")
            db_product["seller_is_official"] = seller.get("is_official", False)
        else:
            db_product["seller_name"] = None
            db_product["seller_id"] = None
            db_product["seller_is_official"] = False

        # Brand
        db_product["brand"] = product.get("brand")

        # Stock fields - chỉ giữ stock_available (stock_quantity, stock_status không sử dụng)
        stock = product.get("stock", {})
        if isinstance(stock, dict):
            db_product["stock_available"] = stock.get("available")
        else:
            db_product["stock_available"] = None

        # Shipping fields (có thể flatten hoặc giữ JSONB - giữ JSONB để dễ mở rộng)
        shipping = product.get("shipping", {})
        if shipping:
            db_product["shipping"] = shipping if isinstance(shipping, dict) else None
        else:
            db_product["shipping"] = None

        # JSONB fields
        db_product["specifications"] = product.get("specifications")
        db_product["images"] = product.get("images")

        # Metadata (không lưu vào DB nhưng giữ lại cho reference)
        if "_metadata" in product:
            db_product["_metadata"] = product["_metadata"]

        # crawled_at từ product hoặc metadata (giữ nguyên string để serialize dễ hơn)
        crawled_at = product.get("crawled_at") or product.get("detail_crawled_at")
        if crawled_at:
            # Nếu là datetime object, convert sang ISO format string
            if isinstance(crawled_at, datetime):
                db_product["crawled_at"] = crawled_at.isoformat()
            else:
                # Giữ nguyên string hoặc convert sang ISO nếu cần
                parsed_dt = self._parse_datetime(crawled_at)
                db_product["crawled_at"] = parsed_dt.isoformat() if parsed_dt else str(crawled_at)

        return db_product

    def validate_product(self, product: dict[str, Any]) -> tuple[bool, str | None]:
        """
        Validate một product

        Returns:
            Tuple (is_valid, error_message)
        """
        # Kiểm tra required fields
        if not product.get("product_id"):
            return False, "Missing product_id"

        if not product.get("name"):
            return False, "Missing name"

        if not product.get("url"):
            return False, "Missing url"

        # Validate product_id format (chỉ số)
        product_id = str(product["product_id"]).strip()
        if not product_id.isdigit():
            return False, f"Invalid product_id format: {product_id}"

        # Validate URL format
        url = product.get("url", "")
        if not url.startswith("http://") and not url.startswith("https://"):
            return False, f"Invalid URL format: {url}"

        # Validate price nếu có
        price = product.get("price")
        original_price = product.get("original_price")
        if price is not None and original_price is not None:
            if price < 0 or original_price < 0:
                return False, "Price cannot be negative"
            if price > original_price:
                return False, "Current price cannot be greater than original price"

        # Validate rating nếu có
        rating_average = product.get("rating_average")
        if rating_average is not None:
            if rating_average < 0 or rating_average > 5:
                return False, f"Rating must be between 0 and 5: {rating_average}"

        # Validate sales_count nếu có
        sales_count = product.get("sales_count")
        if sales_count is not None and sales_count < 0:
            return False, "Sales count cannot be negative"

        return True, None

    def _normalize_text(self, text: str) -> str:
        """Chuẩn hóa text"""
        if not text:
            return ""
        # Loại bỏ whitespace thừa
        text = " ".join(text.split())
        return text.strip()

    def _normalize_url(self, url: str) -> str:
        """Chuẩn hóa URL"""
        if not url:
            return ""
        url = url.strip()
        # Loại bỏ query parameters không cần thiết nếu cần
        # Hiện tại giữ nguyên URL
        return url

    def _validate_seller_name(self, seller_name: str | None) -> str | None:
        """
        Validate và clean seller_name, trả về None nếu không hợp lệ.
        
        Loại bỏ các giá trị rác dựa trên config (có thể mở rộng qua .env):
        - Patterns trong INVALID_SELLER_PATTERNS (default + extra từ env)
        - Chỉ số hoặc ký tự đặc biệt
        - Text quá ngắn hoặc quá dài
        
        Config env variables:
        - INVALID_SELLER_PATTERNS_EXTRA: thêm patterns (comma-separated)
        - SELLER_NAME_MIN_LENGTH: độ dài tối thiểu (default: 2)
        - SELLER_NAME_MAX_LENGTH: độ dài tối đa (default: 100)
        """
        if not seller_name:
            return None
        
        seller_name = str(seller_name).strip()
        
        # Import config với fallback values
        try:
            from pipelines.crawl.config import (
                INVALID_SELLER_PATTERNS,
                SELLER_NAME_MIN_LENGTH,
                SELLER_NAME_MAX_LENGTH,
            )
        except ImportError:
            # Fallback nếu không import được config
            INVALID_SELLER_PATTERNS = [
                "đã mua", "đã bán", "sold", "bought", "mua", "bán", "xem thêm", "more info", "chi tiết", "loading", "đang tải","Đã mua hàng"
            ]
            SELLER_NAME_MIN_LENGTH = 2
            SELLER_NAME_MAX_LENGTH = 100
        
        # Kiểm tra độ dài
        if len(seller_name) < SELLER_NAME_MIN_LENGTH:
            return None
        
        if len(seller_name) > SELLER_NAME_MAX_LENGTH:
            return None
        
        seller_lower = seller_name.lower()
        
        # Kiểm tra invalid patterns từ config
        for pattern in INVALID_SELLER_PATTERNS:
            if pattern in seller_lower:
                return None
        
        # Chỉ là số (không phải tên seller)
        if seller_name.isdigit():
            return None
        
        # Chỉ là ký tự đặc biệt
        if re.match(r'^[^\w]+$', seller_name):
            return None
        
        # Bắt đầu bằng số + text (e.g., "1234 đã mua")
        if re.match(r'^\d+\s+', seller_name):
            return None
        
        return seller_name

    def _parse_int(self, value: Any) -> int | None:
        """Parse value thành int"""
        if value is None:
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            # Loại bỏ ký tự không phải số
            cleaned = re.sub(r"[^\d]", "", value)
            return int(cleaned) if cleaned else None
        return None

    def _parse_float(self, value: Any) -> float | None:
        """Parse value thành float"""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            # Loại bỏ ký tự không phải số và dấu chấm
            cleaned = re.sub(r"[^\d.]", "", value)
            return float(cleaned) if cleaned else None
        return None

    def _parse_datetime(self, value: Any) -> datetime | None:
        """Parse datetime từ nhiều format"""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            # Thử các format phổ biến
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%d",
                "%Y-%m-%d %H:%M:%S.%f",
            ]
            for fmt in formats:
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue
        return None

    def get_stats(self) -> dict[str, Any]:
        """Lấy thống kê transform"""
        return self.stats.copy()
