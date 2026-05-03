"""
Transform pipeline để làm sạch, validate và chuẩn hóa dữ liệu sản phẩm.
"""

import logging
import re
from datetime import datetime
from html import unescape
from typing import Any

# Import profiling utilities
from common.profiling import profile_transformer

logger = logging.getLogger(__name__)

REQUIRED_QUALITY_FIELDS = (
    "product_id",
    "name",
    "url",
    "price",
    "brand",
    "seller_name",
    "category_id",
)
ENRICHMENT_QUALITY_FIELDS = (
    "review_count",
)


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
        self.ai_summarizer = self._init_ai_summarizer()

    @staticmethod
    def _init_ai_summarizer():
        """Try to load AISummarizer with multiple import strategies."""
        try:
            from common.settings import get_bool_setting

            use_ai = get_bool_setting("SHORT_NAME_USE_AI", use_airflow=False)
        except Exception:
            use_ai = False

        if not use_ai:
            logger.info("AI Short Name disabled; using local heuristic rules")
            return None

        # Try import strategies in order
        for import_fn in [
            lambda: __import__("src.common.ai.summarizer", fromlist=["AISummarizer"]).AISummarizer,
            lambda: __import__("common.ai.summarizer", fromlist=["AISummarizer"]).AISummarizer,
        ]:
            try:
                cls = import_fn()
                logger.info("AI Short Name enabled")
                return cls()
            except ImportError:
                continue

        logger.warning("Could not import AISummarizer")
        return None

    @profile_transformer
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
        max_errors = 100  # Cap error list to prevent unbounded memory growth

        for idx, product in enumerate(products):
            try:
                transformed_product = self.transform_product(product)

                if not transformed_product:
                    self.stats["invalid_products"] += 1
                    continue

                # Duplicate check
                product_id = transformed_product.get("product_id")
                if product_id in seen_product_ids:
                    self.stats["duplicates_removed"] += 1
                    logger.debug(f"Duplicate product_id: {product_id}")
                    continue
                seen_product_ids.add(product_id)

                # Validate if needed
                if validate:
                    is_valid, error = self.validate_product(transformed_product)
                    if not is_valid:
                        if self.remove_invalid:
                            self.stats["invalid_products"] += 1
                            if len(self.stats["errors"]) < max_errors:
                                self.stats["errors"].append(f"Product {product_id}: {error}")
                            continue
                        elif self.strict_validation:
                            raise ValueError(f"Invalid product {product_id}: {error}")

                transformed.append(transformed_product)
                self.stats["valid_products"] += 1

            except Exception as e:
                self.stats["invalid_products"] += 1
                error_msg = f"Error transforming product at index {idx}: {e}"
                if len(self.stats["errors"]) < max_errors:
                    self.stats["errors"].append(error_msg)
                logger.error(error_msg)

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
            "category_url": product.get("category_url"),
            "category_id": product.get("category_id"),
            "category_path": product.get("category_path") or [],
            "sales_count": self._parse_int(product.get("sales_count")),
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
            db_product["original_price"] = price.get("original_price") or db_product["price"]
            if price.get("discount_percent") is not None:
                db_product["discount_percent"] = round(float(price["discount_percent"]), 2)
            elif db_product["price"] is not None and db_product["original_price"] is not None:
                db_product["discount_percent"] = (
                    0.0 if db_product["price"] == db_product["original_price"] else None
                )
            else:
                db_product["discount_percent"] = None
        else:
            db_product["price"] = None
            db_product["original_price"] = None
            db_product["discount_percent"] = None

        # Rating fields (flatten từ dict)
        rating = product.get("rating", {})
        if isinstance(rating, dict):
            db_product["rating_average"] = rating.get("average")
            db_product["review_count"] = self._first_present(
                rating.get("total_reviews"), rating.get("review_count")
            )
        else:
            db_product["rating_average"] = None
            db_product["review_count"] = None

        # Description
        db_product["description"] = product.get("description")
        db_product["description_text"] = self._html_to_text(product.get("description"))

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

        # JSONB fields
        db_product["images"] = self._normalize_json_payload(product.get("images"))
        db_product["image_url"] = product.get("image_url") or product.get("thumbnail_url")
        if not db_product["image_url"]:
            db_product["image_url"] = self._first_image_url(db_product["images"])
        db_product["shipping"] = self._normalize_shipping(product.get("shipping"))
        stock_info = self._stock_info(product)
        db_product["stock_available"] = stock_info["available"]
        db_product["stock_status"] = stock_info["status"]
        db_product["stock_source"] = stock_info["source"]
        db_product["detail_status"] = product.get("detail_status")

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

        self._apply_quality_contract(db_product)
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

        # seller_name is optional — listing API products may not have it
        # Database cleanup handles missing sellers separately

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

    def _apply_quality_contract(self, product: dict[str, Any]) -> None:
        """Set quality fields from the flattened DB payload."""
        missing_required = [
            field for field in REQUIRED_QUALITY_FIELDS if self._is_missing(product.get(field))
        ]
        missing_enrichment = [
            field for field in ENRICHMENT_QUALITY_FIELDS if self._is_missing(product.get(field))
        ]
        review_count = product.get("review_count")
        if (
            review_count is not None
            and self._parse_int(review_count) is not None
            and self._parse_int(review_count) > 0
            and self._is_missing(product.get("rating_average"))
        ):
            missing_enrichment.append("rating_average")
        missing_fields = missing_required + missing_enrichment

        if missing_required:
            crawl_quality = "basic"
        elif missing_enrichment:
            crawl_quality = "partial"
        else:
            crawl_quality = "full"

        total_fields = len(REQUIRED_QUALITY_FIELDS) + len(ENRICHMENT_QUALITY_FIELDS)
        present_fields = total_fields - len(missing_fields)
        quality_score = round(present_fields / total_fields, 2) if total_fields else 0.0

        product["missing_fields"] = missing_fields
        product["needs_enrichment"] = bool(missing_fields)
        product["crawl_quality"] = crawl_quality
        product["data_quality_score"] = quality_score

        if crawl_quality == "full":
            product["detail_status"] = "full"
        elif crawl_quality == "partial":
            product["detail_status"] = "partial_needs_enrichment"
        else:
            product["detail_status"] = "basic_needs_enrichment"

        metadata = product.setdefault("_metadata", {})
        if isinstance(metadata, dict):
            metadata["missing_fields"] = missing_fields
            metadata["data_completeness_score"] = quality_score
            metadata["crawl_quality"] = crawl_quality

    @staticmethod
    def _is_missing(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, str):
            return value.strip() == ""
        if isinstance(value, (list, dict, tuple, set)):
            return len(value) == 0
        return False

    @staticmethod
    def _first_present(*values: Any) -> Any:
        for value in values:
            if value is not None:
                return value
        return None

    @staticmethod
    def _normalize_json_payload(value: Any) -> Any:
        if value in ("", [], {}):
            return None
        return value

    @classmethod
    def _normalize_shipping(cls, value: Any) -> dict[str, Any] | None:
        normalized = {
            "free_shipping": None,
            "fast_delivery": None,
            "delivery_time": None,
            "badges": [],
        }
        if not value:
            return None
        if isinstance(value, dict):
            normalized["free_shipping"] = value.get("free_shipping")
            normalized["fast_delivery"] = value.get("fast_delivery")
            normalized["delivery_time"] = value.get("delivery_time")
            extras = {
                key: val
                for key, val in value.items()
                if key not in {"free_shipping", "fast_delivery", "delivery_time"}
                and not cls._is_missing(val)
            }
            if extras:
                normalized["extra"] = extras
        elif isinstance(value, list):
            normalized["badges"] = value
        else:
            normalized["raw"] = value

        return normalized if any(not cls._is_missing(v) for v in normalized.values()) else None

    def _stock_info(self, product: dict[str, Any]) -> dict[str, Any]:
        status = self._normalize_stock_status(product.get("stock_status"))
        source = self._stock_source(product)
        inventory_status = self._normalize_stock_status(product.get("inventory_status"))

        if inventory_status:
            status = inventory_status
            return {
                "available": self._available_from_status(inventory_status),
                "status": inventory_status,
                "source": source,
            }

        if product.get("stock_available") is not None:
            available = bool(product.get("stock_available"))
            status = status or ("available" if available else "out_of_stock")
            return {"available": available, "status": status, "source": source or "explicit"}

        stock = product.get("stock")
        if not isinstance(stock, dict):
            return {"available": None, "status": status or "unknown", "source": source}

        stock_status = self._normalize_stock_status(
            stock.get("stock_status") or stock.get("status") or stock.get("inventory_status")
        )
        if stock_status:
            status = stock_status
            return {
                "available": self._available_from_status(stock_status),
                "status": stock_status,
                "source": source,
            }

        quantity = self._parse_int(stock.get("quantity", stock.get("qty")))
        if quantity is not None:
            return {
                "available": quantity > 0,
                "status": "available" if quantity > 0 else "out_of_stock",
                "source": source,
            }

        if stock.get("available") is True:
            return {"available": True, "status": "available", "source": source}

        return {"available": None, "status": status or "unknown", "source": source}

    @staticmethod
    def _normalize_stock_status(value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip().lower()
        if not normalized:
            return None
        aliases = {
            "in_stock": "available",
            "instock": "available",
            "available": "available",
            "out_of_stock": "out_of_stock",
            "outofstock": "out_of_stock",
            "unavailable": "out_of_stock",
            "sold_out": "out_of_stock",
            "hethang": "out_of_stock",
            "het_hang": "out_of_stock",
        }
        return aliases.get(normalized, normalized)

    @staticmethod
    def _available_from_status(status: str) -> bool | None:
        if status == "available":
            return True
        if status in {"out_of_stock", "discontinued", "inactive"}:
            return False
        return None

    @staticmethod
    def _stock_source(product: dict[str, Any]) -> str | None:
        if product.get("stock_source"):
            return str(product["stock_source"])

        metadata = product.get("_metadata")
        crawl_status = metadata.get("crawl_status") if isinstance(metadata, dict) else None
        extraction_method = metadata.get("extraction_method") if isinstance(metadata, dict) else None

        if crawl_status == "success_api" or extraction_method == "api":
            return "detail_api"
        if crawl_status == "success_html" or extraction_method in {"aiohttp", "html"}:
            return "detail_html"
        if product.get("extraction_method") == "listing_api":
            return "listing_api"
        if product.get("detail_status"):
            return str(product["detail_status"])
        return None

    @staticmethod
    def _first_image_url(images: Any) -> str | None:
        if not isinstance(images, list):
            return None
        for image in images:
            if isinstance(image, str) and image.strip():
                return image.strip()
            if isinstance(image, dict):
                url = image.get("url") or image.get("base_url") or image.get("large_url")
                if url:
                    return str(url).strip()
        return None

    @staticmethod
    def _html_to_text(value: Any) -> str | None:
        if not value:
            return None
        text = str(value)
        text = re.sub(r"<script\b[^<]*(?:(?!</script>)<[^<]*)*</script>", " ", text, flags=re.I)
        text = re.sub(r"<style\b[^<]*(?:(?!</style>)<[^<]*)*</style>", " ", text, flags=re.I)
        text = re.sub(r"<[^>]+>", " ", text)
        text = unescape(text)
        text = re.sub(r"\s+", " ", text).strip()
        return text or None

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
            "tiện lợi",
            "tiện dụng",
            "thông minh",
            "chính hãng",
            "hàng công ty",
            "fullbox",
            "nguyên seal",
            "giá sỉ",
            "giá tận gốc",
            "mẫu mới",
            "mẫu hot",
            "hàng nội địa",
            "chất lượng cao",
            "siêu cấp",
            "siêu sạch",
            "siêu gọn",
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
            r"\b\d+\s*(?:tháng|năm)\s*(?:bảo hành|bh)\b",
            r"\b(?:bảo hành|bh)\s*\d+\s*(?:tháng|năm)\b",
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
            "mẫu mã",
            "mẫu mới",
            "thiết kế",
            "phong cách",
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

        return self._compact_short_name_candidate(cleaned)

    def _compact_short_name_candidate(self, name: str) -> str:
        """Keep a local short name readable without calling AI."""
        if not name:
            return ""

        cleaned = " ".join(name.split())
        cleaned = re.sub(r"\b(?:cho|với|và|của|dành|kèm|tặng)\s*$", "", cleaned, flags=re.IGNORECASE)
        cleaned = cleaned.strip(" -,+/&|()[]")

        words = cleaned.split()
        if len(words) > 8:
            cleaned = " ".join(words[:8])
            cleaned = re.sub(
                r"\b(?:cho|với|và|của|dành|kèm|tặng)\s*$",
                "",
                cleaned,
                flags=re.IGNORECASE,
            ).strip()

        if len(cleaned) > 60:
            truncated = cleaned[:60].rstrip()
            last_space = truncated.rfind(" ")
            if last_space > 30:
                cleaned = truncated[:last_space]

        if cleaned:
            cleaned = cleaned.lower()
            cleaned = cleaned[0].upper() + cleaned[1:] if len(cleaned) > 1 else cleaned.upper()

        return cleaned

    def _get_short_name(self, name: str) -> str:
        """
        Get shortened name using:
        1. Advanced Heuristics (Extract core name).
        2. Local compaction.
        3. Optional AI Fallback (Only if local rules fail and feature flag is enabled).
        4. Hard Fallback (Truncate).
        """
        if not name:
            return ""

        # 1. Advanced Heuristics
        # This handles cleaning, splitting, and stop-word removal locally
        heuristic_name = self._extract_short_name_heuristics(name)

        if heuristic_name:
            return heuristic_name

        # 3. AI Fallback (Smart Summarization)
        # Use AI only if local extraction failed completely.
        if self.ai_summarizer:
            try:
                short_name = self.ai_summarizer.shorten_product_name(name)

                if short_name:
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
