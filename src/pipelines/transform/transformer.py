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

            # Tạo computed fields
            transformed = self._add_computed_fields(transformed)

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
            "sales_count": product.get("sales_count"),
        }

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

        # Seller fields (flatten từ dict)
        seller = product.get("seller", {})
        if isinstance(seller, dict):
            db_product["seller_name"] = seller.get("name")
            db_product["seller_id"] = seller.get("seller_id")
            db_product["seller_is_official"] = seller.get("is_official", False)
        else:
            db_product["seller_name"] = None
            db_product["seller_id"] = None
            db_product["seller_is_official"] = False

        # Brand
        db_product["brand"] = product.get("brand")

        # Stock fields (flatten từ dict hoặc giữ JSONB)
        stock = product.get("stock", {})
        if isinstance(stock, dict):
            db_product["stock_available"] = stock.get("available")
            db_product["stock_quantity"] = stock.get("quantity")
            db_product["stock_status"] = stock.get("stock_status")
        else:
            db_product["stock_available"] = None
            db_product["stock_quantity"] = None
            db_product["stock_status"] = None

        # Shipping fields (có thể flatten hoặc giữ JSONB - giữ JSONB để dễ mở rộng)
        shipping = product.get("shipping", {})
        if shipping:
            db_product["shipping"] = shipping if isinstance(shipping, dict) else None
        else:
            db_product["shipping"] = None

        # JSONB fields
        db_product["specifications"] = product.get("specifications")
        db_product["images"] = product.get("images")

        # Computed fields (doanh thu, metrics)
        db_product["estimated_revenue"] = product.get("estimated_revenue")
        db_product["price_savings"] = product.get("price_savings")
        db_product["price_category"] = product.get("price_category")
        db_product["popularity_score"] = product.get("popularity_score")
        db_product["value_score"] = product.get("value_score")
        db_product["discount_amount"] = product.get("discount_amount")
        db_product["sales_velocity"] = product.get("sales_velocity")

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

    def _add_computed_fields(self, product: dict[str, Any]) -> dict[str, Any]:
        """Thêm các trường tính toán"""
        # 1. Doanh thu ước tính (Estimated Revenue)
        # Doanh thu = số lượng đã bán * giá hiện tại
        sales_count = product.get("sales_count")
        price = (
            product.get("price", {}).get("current_price")
            if isinstance(product.get("price"), dict)
            else product.get("price")
        )

        if sales_count and price:
            estimated_revenue = sales_count * price
            product["estimated_revenue"] = round(estimated_revenue, 2)
        else:
            product["estimated_revenue"] = None

        # 2. Tiết kiệm (Price Savings)
        # Tiết kiệm = giá gốc - giá hiện tại
        original_price = (
            product.get("price", {}).get("original_price")
            if isinstance(product.get("price"), dict)
            else product.get("original_price")
        )

        if price and original_price and original_price > price:
            price_savings = original_price - price
            product["price_savings"] = round(price_savings, 2)
        else:
            product["price_savings"] = None

        # 3. Price Category (Phân loại giá)
        if price:
            if price < 500000:  # < 500k
                product["price_category"] = "budget"
            elif price < 2000000:  # 500k - 2M
                product["price_category"] = "mid-range"
            elif price < 10000000:  # 2M - 10M
                product["price_category"] = "premium"
            else:  # > 10M
                product["price_category"] = "luxury"
        else:
            product["price_category"] = None

        # 4. Popularity Score (Điểm độ phổ biến)
        # Kết hợp sales_count, rating, review_count
        rating_avg = (
            product.get("rating", {}).get("average")
            if isinstance(product.get("rating"), dict)
            else product.get("rating_average")
        )
        review_count = (
            product.get("rating", {}).get("total_reviews")
            or product.get("rating", {}).get("review_count")
            if isinstance(product.get("rating"), dict)
            else product.get("review_count")
        )

        popularity_score = 0
        if sales_count:
            # Sales count chiếm 50% (normalize về scale 0-100)
            # Giả sử max sales_count là 100k để normalize
            max_sales = 100000
            sales_score = min((sales_count / max_sales) * 50, 50)
            popularity_score += sales_score

        if rating_avg:
            # Rating chiếm 30% (0-5 scale -> 0-30)
            rating_score = (rating_avg / 5) * 30
            popularity_score += rating_score

        if review_count:
            # Review count chiếm 20% (normalize về scale 0-100)
            # Giả sử max review_count là 10k để normalize
            max_reviews = 10000
            review_score = min((review_count / max_reviews) * 20, 20)
            popularity_score += review_score

        product["popularity_score"] = round(popularity_score, 2) if popularity_score > 0 else None

        # 5. Value Score (Điểm giá trị)
        # Giá trị = rating / (price / 1000000) - rating càng cao, giá càng thấp thì điểm càng cao
        if rating_avg and price and price > 0:
            # Normalize price về triệu đồng
            price_million = price / 1000000
            # Tính value score: rating / price_million
            # Nếu rating cao và giá thấp -> điểm cao
            value_score = rating_avg / price_million if price_million > 0 else None
            product["value_score"] = round(value_score, 2) if value_score else None
        else:
            product["value_score"] = None

        # 6. Discount Amount (Số tiền giảm)
        # Số tiền giảm = original_price - price (đã có trong price_savings nhưng đổi tên cho rõ ràng)
        if not product.get("price_savings") and price and original_price and original_price > price:
            product["discount_amount"] = round(original_price - price, 2)
        else:
            product["discount_amount"] = product.get("price_savings")

        # 7. Sales Velocity (Tốc độ bán)
        # Giả định: sales_velocity = sales_count (có thể tính chi tiết hơn nếu có dữ liệu theo thời gian)
        product["sales_velocity"] = sales_count

        # 8. Price per Unit (Giá trên đơn vị) - nếu có specifications về số lượng/khối lượng
        # Ví dụ: giá trên m², giá trên kg, etc.
        # Có thể implement nếu cần extract từ specifications

        # Normalized brand name (loại bỏ "Thương hiệu: " prefix nếu có trong name)
        if not product.get("brand") and product.get("name"):
            # Thử extract brand từ name
            # Một số pattern phổ biến: "Brand Name Product Name"
            # Có thể implement logic extract brand ở đây nếu cần
            pass

        return product

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
