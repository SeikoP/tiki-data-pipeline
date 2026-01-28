"""
Transform pipeline để làm sạch, validate và chuẩn hóa dữ liệu sản phẩm.
"""

import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from threading import Lock
from typing import Any

logger = logging.getLogger(__name__)


class DataTransformer:
    """
    Class để transform dữ liệu sản phẩm trước khi load vào database.
    """

    def __init__(
        self,
        strict_validation: bool = False,
        remove_invalid: bool = True,
        normalize_fields: bool = True,
    ):
        """Khởi tạo DataTransformer.

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

        # Initialize AI Summarizer for name shortening
        # Initialize AI Summarizer for name shortening
        try:
            from src.common.ai.summarizer import AISummarizer

            # Try to import config to check if AI short name is enabled
            try:
                from src.common.config import SHORT_NAME_CONFIG

                use_ai_short_name = SHORT_NAME_CONFIG.get("use_ai", True)
            except ImportError:
                import os

                # Fallback to env var directly
                use_ai_short_name = os.getenv("SHORT_NAME_USE_AI", "true").lower() == "true"

            if use_ai_short_name:
                self.ai_summarizer = AISummarizer()
                logger.info("✅ AI Short Name enabled")
            else:
                self.ai_summarizer = None
                logger.info("⚠️  AI Short Name DISABLED by config")

        except ImportError:
            # Fallback for relative import if src is not in path
            try:
                import os

                from ..common.ai.summarizer import AISummarizer

                use_ai_short_name = os.getenv("SHORT_NAME_USE_AI", "true").lower() == "true"

                if use_ai_short_name:
                    self.ai_summarizer = AISummarizer()
                else:
                    self.ai_summarizer = None
            except ImportError:
                # Try absolute path from project root
                try:
                    import os
                    import sys
                    from pathlib import Path

                    root_path = Path(__file__).resolve().parent.parent.parent.parent
                    if str(root_path) not in sys.path:
                        sys.path.append(str(root_path))
                    from src.common.ai.summarizer import AISummarizer

                    use_ai_short_name = os.getenv("SHORT_NAME_USE_AI", "true").lower() == "true"
                    if use_ai_short_name:
                        self.ai_summarizer = AISummarizer()
                    else:
                        self.ai_summarizer = None
                except Exception as e:
                    logger.warning(f"Could not import AISummarizer: {e}")
                    self.ai_summarizer = None

    def transform_products(
        self, products: list[dict[str, Any]], validate: bool = True
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Transform danh sách products.

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
        seen_lock = Lock()

        # Determine max_workers based on list size, but cap at reasonable limit (e.g. 20)
        # Higher if IO bound (AI calls), lower if CPU bound.
        max_workers = min(20, len(products)) if len(products) > 0 else 1

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Map futures to original index to maintain error context (log only)
            future_to_product = {
                executor.submit(self.transform_product, p): idx for idx, p in enumerate(products)
            }

            for future in as_completed(future_to_product):
                idx = future_to_product[future]
                try:
                    transformed_product = future.result()

                    if not transformed_product:
                        self.stats["invalid_products"] += 1
                        continue

                    # Thread-safe duplicate check
                    product_id = transformed_product.get("product_id")
                    with seen_lock:
                        if product_id in seen_product_ids:
                            self.stats["duplicates_removed"] += 1
                            logger.debug(f"⚠️  Duplicate product_id: {product_id}")
                            continue
                        seen_product_ids.add(product_id)

                    # Validate if needed
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

        # Sort results slightly to ensure some deterministic output order if needed (optional)
        # transformed.sort(key=lambda x: x.get('product_id'))

        return transformed, self.stats

    def transform_product(self, product: dict[str, Any]) -> dict[str, Any] | None:
        """Transform một product.

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
        """
        Chuẩn hóa các trường dữ liệu.
        """
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
        """
        Transform format để phù hợp với database schema.
        """
        db_product = {
            "product_id": product.get("product_id"),
            "name": product.get("name"),
            "short_name": self._get_short_name(product.get("name")),
            "url": product.get("url"),
            "image_url": product.get("image_url"),
            "category_url": product.get("category_url"),
            "category_id": product.get("category_id"),  # Extract từ category_url nếu chưa có
            "category_path": product.get("category_path")
            or [],  # Default [] nếu không có breadcrumb
            "sales_count": product.get("sales_count")
            if product.get("sales_count") is not None
            else 0,
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
        """Validate một product.

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

        # CRITICAL: Validate seller_name (Task: Xóa seller_name null trước khi load db)
        if not product.get("seller_name"):
            return False, "Missing or invalid seller_name"

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
        """
        Chuẩn hóa text.
        """
        if not text:
            return ""
        # Loại bỏ whitespace thừa
        text = " ".join(text.split())
        return text.strip()

    def _normalize_url(self, url: str) -> str:
        """
        Chuẩn hóa URL.
        """
        if not url:
            return ""
        url = url.strip()
        # Loại bỏ query parameters không cần thiết nếu cần
        # Hiện tại giữ nguyên URL
        return url

    def _validate_seller_name(self, seller_name: str | None) -> str | None:
        """Validate và clean seller_name, trả về None nếu không hợp lệ.

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
                SELLER_NAME_MAX_LENGTH,
                SELLER_NAME_MIN_LENGTH,
            )
        except ImportError:
            # Fallback nếu không import được config
            INVALID_SELLER_PATTERNS = [
                "đã mua",
                "đã bán",
                "sold",
                "bought",
                "mua",
                "bán",
                "xem thêm",
                "more info",
                "chi tiết",
                "loading",
                "đang tải",
                "Đã mua hàng",
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
        if re.match(r"^[^\w]+$", seller_name):
            return None

        # Bắt đầu bằng số + text (e.g., "1234 đã mua")
        if re.match(r"^\d+\s+", seller_name):
            return None

        return seller_name

    def _parse_int(self, value: Any) -> int | None:
        """
        Parse value thành int.
        """
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
        """
        Parse value thành float.
        """
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
        """
        Parse datetime từ nhiều format.
        """
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

    def _clean_name_heuristics(self, name: str) -> str:
        """
        Apply heuristic rules to clean product names:
        - Remove SKU codes
        - Remove marketing fluff and subjective adjectives
        - Remove special characters
        - Normalize phrases
        """
        if not name:
            return ""

        # 0. Strip leading/trailing brackets/special chars that often surround fluff
        name = re.sub(r"^[\[\(\-\s]+", "", name)
        name = re.sub(r"[\]\)\-\s]+$", "", name)

        # 1. Remove hashtags (e.g., #jean) - do this early
        cleaned = re.sub(r"#\w+\b", "", name)

        # 2. Regex cleaning for SKUs and codes (e.g., CV0016, CV123, SKU-99, MS123)
        sku_patterns = [
            r"\b[A-Za-z]{2,}\d{3,}\b",  # CV0016, SP1234
            r"\b[A-Za-z]+\-\d+\b",  # SKU-123, MS-001
            r"\bMS\s*\d+\b",  # MS 123
            r"\bMã\s*(?:số)?\s*\d+\b",  # Mã số 123
        ]

        for pattern in sku_patterns:
            cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE)

        # 3. Marketing fluff and subjective adjectives
        fluff_keywords = [
            "sang chảnh",
            "siêu xinh",
            "trẻ trung",
            "thoáng mát",
            "cực đẹp",
            "chất lượng",
            "cao cấp",
            "gợi cảm",
            "quyến rũ",
            "sexy",
            "hot hot",
            "mẫu mới nhất",
            "new design",
            "hot trend",
            "giá rẻ",
            "siêu rẻ",
            "vải mềm",
            "co giãn",
            "thiết kế",
            "chất mềm",
            "mới nhất",
            "siêu đẹp",
            "hot",
            "giá sốc",
            "giá tốt",
            "flash sale",
            "siêu sale",
            "sale sốc",
            "khuyến mãi",
            "quà tặng",
            "combo",
            "set",
            "uy tín",
            "nhập khẩu",
            "xuất khẩu",
            "hàng hiệu",
            "bền đẹp",
            "siêu bền",
            "chống nước",
            "xịn",
            "vip",
            "luxury",
            "limited",
            "bản giới hạn",
            "đa năng",
            "tiện lợi",
            "tiện dụng",
            "thông minh",
            "tự động",
            "chính hãng",
            "hàng công ty",
            "fullbox",
            "nguyên seal",
            "giá sỉ",
            "giá tận gốc",
            "mẫu mới",
            "mẫu hot",
            "hàng nội địa",
            "nhập khẩu",
            "chất lượng cao",
            "siêu cấp",
            "siêu sạch",
            "siêu gọn",
            "nhỏ gọn",
            "cầm tay",
            "mini",
            "loại 1",
            "bản cao cấp",
            "chống trầy",
            "chống xước",
            "kháng khuẩn",
            "khử mùi",
            "tiết kiệm",
            "hiệu quả",
            "bảo hành",
            "đổi trả",
            "tặng kèm",
            "quà tặng",
            "freeship",
            "miễn phí",
            "chăm sóc",
            "vệ sinh",
            "làm sạch",
            "giúp",
            "giảm",
            "tăng",
            "hỗ trợ",
            "công nghệ",
            "phong cách",
            "thiết kế mới",
            "mẫu mã đẹp",
        ]

        # Build regex for fluff (word boundaries)
        fluff_pattern = r"\b(" + "|".join(re.escape(k) for k in fluff_keywords) + r")\b"
        cleaned = re.sub(fluff_pattern, "", cleaned, flags=re.IGNORECASE)

        # 4. Remove dimensions and technical specs (e.g., 76x30.5cm, 10m, 5 chế độ)
        spec_patterns = [
            r"\b[A-Za-z]?\d+[\.,]?\d*\s*[xX]\s*[A-Za-z]?\d+[\.,]?\d*(?:\s*[xX]\s*[A-Za-z]?\d+[\.,]?\d*)?[^ ]*\b",  # W76xD30.5xH11.5Cm
            r"\b\d+[\.,]?\d*\s*(?:m|cm|mm|kg|g|l|ml|w|v|kw|ah|ma|mah)\b",  # 5m, 10kg, 100w, 2000mah
            r"\b\d+\s*(?:chế độ|đầu|món|chi tiết|cái|nấc|vị|mùi|lít|hũ|gói|viên)\b",  # 5 chế độ, 4 đầu
            r"\b\d+[-/]\d+\b",  # 2/3, 2-1
        ]
        for pattern in spec_patterns:
            cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE)

        # 5. Remove years (e.g., 2024, 2023) if they appear to be part of title fluff
        cleaned = re.sub(r"\b202\d\b", "", cleaned)

        # 6. General special characters and repetitive symbols (Preserve delimiters for splitting later)
        # We only remove very noisy symbols here
        cleaned = re.sub(r"[\!\*\+\=~…\.]", " ", cleaned)

        # 7. Normalize spacing and capitalization
        cleaned = " ".join(cleaned.split())

        if cleaned:
            # Convert to sentence case as per user examples
            cleaned = cleaned.lower()
            cleaned = cleaned[0].upper() + cleaned[1:] if len(cleaned) > 1 else cleaned.upper()

        return cleaned

    def _extract_short_name_heuristics(self, name: str) -> str:
        """Advanced heuristic extraction to find the core product name.

        Strategies:
        1. Clean noise/fluff.
        2. Structural split (delimiters).
        3. Semantic cutoff (stop words).
        """
        # 1. Base cleaning
        cleaned = self._clean_name_heuristics(name)
        if not cleaned:
            return ""

        # 2. Structural Split (Priority Delimiters)
        # Split by common separators and take the first meaningful chunk
        # Delimiters: | , - – ( [ /
        separators = [
            r"\|",  # "|"
            r"\s-\s",  # " - "
            r"\s–\s",  # " – "
            r"\(",  # "("
            r"\[",  # "["
            r",",  # ","
            r"\s\/\s",  # " / "
        ]

        for sep in separators:
            parts = re.split(sep, cleaned)
            # Find the first part that has a decent length and doesn't look like fluff
            for part in parts:
                candidate = part.strip()
                if len(candidate) > 5:
                    cleaned = candidate
                    break

        # 3. Semantic Cutoff (Stop Words/Phrases that start the 'details')
        # These words often signal the start of attributes, not the name itself
        stop_phrases = [
            "chính hãng",
            "cao cấp",
            "nhập khẩu",
            "giá rẻ",
            "uy tín",
            "chất lượng",
            "bảo hành",
            "xuất xứ",
            "thương hiệu",
            "dành cho",
            "phù hợp",
            "kích thước",
            "size",
            "màu sắc",
            "màu",
            "bộ nhớ",
            "ram",
            "dung lượng",
            "phiên bản",
            "model",
            "tặng kèm",
            "miễn phí",
            "freeship",
            "fullbox",
            "nguyên seal",
            "hàng mới",
            "new",
            "hot",
            "xả kho",
            "thanh lý",
            "chăm sóc",
            "công nghệ",
            "tự động",
            "đa năng",
            "tiện lợi",
            "tiện dụng",
            "hỗ trợ",
            "giải pháp",
            "giúp",
            "hiệu quả",
            "an toàn",
            "chống",
            "tặng",
            "kèm",
            "bộ",
            "set",
            "combo",
            "mẫu mã",
            "mẫu mới",
            "thiết kế",
            "phong cách",
            "không dây",
            "không dùng",
            "dùng cho",
        ]

        name_lower = cleaned.lower()
        earliest_idx = len(cleaned)

        for phrase in stop_phrases:
            # Use regex word boundary to avoid partial matches
            pattern = re.compile(r"\b" + re.escape(phrase) + r"\b")
            match = pattern.search(name_lower)
            if match:
                idx = match.start()
                # Only cut if the phrase is not at the very beginning (keep "Bộ nhớ..." if that's the product)
                # But allow cutting if it's "Dien thoai ABC chinh hang" (idx > 10)
                if idx > 5 and idx < earliest_idx:
                    earliest_idx = idx

        if earliest_idx < len(cleaned):
            cleaned = cleaned[:earliest_idx].strip()

        # 4. Cleanup trailing noise
        cleaned = re.sub(r"[\s\-\+\&\,\/\|\(\)\[\]]+$", "", cleaned)
        cleaned = re.sub(r"^[\s\-\+\&\,\/\|\(\)\[\]]+", "", cleaned)

        return cleaned

    def _get_short_name(self, name: str) -> str:
        """
        Get shortened name using:
        1. Advanced Heuristics (Extract core name).
        2. Validation (Is heuristic result good enough?).
        3. AI Fallback (Only if heuristics fail/result too long).
        4. Hard Fallback (Truncate).
        """
        if not name:
            return ""

        # 1. Advanced Heuristics
        # This handles cleaning, splitting, and stop-word removal locally
        heuristic_name = self._extract_short_name_heuristics(name)

        # 2. Heuristic Validation
        # Explicitly accept if result looks "good enough" to avoid AI costs
        # Criteria:
        # - Reasonable length (e.g., < 75 chars)
        # - Reasonable word count (e.g., < 12 words)
        # - Not empty
        if heuristic_name:
            word_count = len(heuristic_name.split())
            char_count = len(heuristic_name)

            # Acceptance Thresholds (Stricter for higher quality)
            # - Short enough to be a "name", not a description
            # - But long enough to be descriptive
            if 2 <= word_count <= 7 and char_count <= 45:
                # logger.debug(f"✅ Heuristic accepted: '{name}' -> '{heuristic_name}'")
                return heuristic_name

        # 3. AI Fallback (Smart Summarization)
        # Use AI only if heuristic result is still too long or complex
        if self.ai_summarizer:
            try:
                # Pass the *heuristic* result to AI (it's already cleaner than raw name)
                # If heuristic failed (empty), use original name
                input_name = heuristic_name if heuristic_name and len(heuristic_name) > 3 else name

                short_name = self.ai_summarizer.shorten_product_name(input_name)

                if short_name and short_name != input_name:
                    return short_name

            except Exception as e:
                logger.warning(f"⚠️  AI shortening failed, using fallback: {e}")

        # 4. Hard Fallback
        # If Heuristic result exists but wasn't perfect, and AI failed, use Heuristic result.
        # Otherwise use truncation.
        final_candidate = heuristic_name if heuristic_name else name

        if len(final_candidate) > 80:
            truncated = final_candidate[:77]
            last_space = truncated.rfind(" ")
            if last_space > 40:
                truncated = final_candidate[:last_space]
            return truncated + "..."

        return final_candidate

    def get_stats(self) -> dict[str, Any]:
        """
        Lấy thống kê transform.
        """
        return self.stats.copy()
