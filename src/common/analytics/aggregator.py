"""
Module để tổng hợp và phân tích dữ liệu sản phẩm
"""

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class DataAggregator:
    """Class để tổng hợp và phân tích dữ liệu sản phẩm"""

    def __init__(self, data_file_path: str):
        """
        Khởi tạo DataAggregator

        Args:
            data_file_path: Đường dẫn đến file JSON chứa dữ liệu sản phẩm
        """
        self.data_file_path = Path(data_file_path)
        self.data: dict[str, Any] = {}

    def load_data(self) -> bool:
        """
        Load dữ liệu từ file JSON

        Returns:
            True nếu load thành công, False nếu có lỗi
        """
        try:
            if not self.data_file_path.exists():
                logger.error(f"❌ File không tồn tại: {self.data_file_path}")
                return False

            with open(self.data_file_path, encoding="utf-8") as f:
                self.data = json.load(f)

            logger.info(f"✅ Đã load dữ liệu từ {self.data_file_path}")
            return True

        except json.JSONDecodeError as e:
            logger.error(f"❌ Lỗi parse JSON: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Lỗi khi load dữ liệu: {e}")
            return False

    def aggregate(self) -> dict[str, Any]:
        """
        Tổng hợp và phân tích dữ liệu

        Returns:
            Dictionary chứa thông tin tổng hợp
        """
        if not self.data:
            logger.warning("⚠️  Chưa load dữ liệu, đang load...")
            if not self.load_data():
                return {}

        summary: dict[str, Any] = {
            "metadata": {},
            "statistics": {},
            "price_analysis": {},
            "rating_analysis": {},
            "category_analysis": {},
            "seller_analysis": {},
            "issues": [],
        }

        # Metadata
        summary["metadata"] = {
            "crawled_at": self.data.get("crawled_at", ""),
            "total_products": self.data.get("total_products", 0),
            "file_path": str(self.data_file_path),
        }

        # Statistics từ stats
        stats = self.data.get("stats", {})
        summary["statistics"] = {
            "total_products": stats.get("total_products", 0),
            "with_detail": stats.get("with_detail", 0),
            "failed": stats.get("failed", 0),
            "timeout": stats.get("timeout", 0),
            "cached": stats.get("cached", 0),
            "products_saved": stats.get("products_saved", 0),
            "products_skipped": stats.get("products_skipped", 0),
            "failed_products_count": stats.get("failed_products_count", 0),
        }

        # Phân tích products
        products = self.data.get("products", [])
        if products:
            summary = self._analyze_products(products, summary)

        # Issues
        if stats.get("failed", 0) > 0:
            summary["issues"].append(f"Có {stats.get('failed')} sản phẩm crawl thất bại")

        if stats.get("timeout", 0) > 0:
            summary["issues"].append(f"Có {stats.get('timeout')} sản phẩm bị timeout")

        if stats.get("products_skipped", 0) > 0:
            summary["issues"].append(
                f"Có {stats.get('products_skipped')} sản phẩm bị bỏ qua (không có detail)"
            )

        return summary

    def _analyze_products(
        self, products: list[dict[str, Any]], summary: dict[str, Any]
    ) -> dict[str, Any]:
        """Phân tích chi tiết các sản phẩm"""
        prices = []
        ratings = []
        categories = {}
        sellers = {}
        discounts = []

        for product in products:
            # Price analysis
            price_info = product.get("price", {})
            if price_info:
                current_price = price_info.get("current_price")
                discount = price_info.get("discount_percent", 0)

                if current_price:
                    prices.append(current_price)
                if discount:
                    discounts.append(discount)

            # Rating analysis
            rating_info = product.get("rating", {})
            if rating_info:
                avg_rating = rating_info.get("average")
                if avg_rating:
                    ratings.append(avg_rating)

            # Category analysis
            category_url = product.get("category_url", "")
            if category_url:
                # Extract category name from URL
                category_name = category_url.split("/")[-1] if "/" in category_url else category_url
                categories[category_name] = categories.get(category_name, 0) + 1

            # Seller analysis
            seller_info = product.get("seller", {})
            if seller_info:
                seller_name = seller_info.get("name", "Unknown")
                sellers[seller_name] = sellers.get(seller_name, 0) + 1

        # Price analysis
        if prices:
            summary["price_analysis"] = {
                "min_price": min(prices),
                "max_price": max(prices),
                "avg_price": sum(prices) / len(prices),
                "total_products_with_price": len(prices),
            }

        if discounts:
            summary["price_analysis"]["avg_discount"] = sum(discounts) / len(discounts)
            summary["price_analysis"]["max_discount"] = max(discounts)
            summary["price_analysis"]["min_discount"] = min(discounts)

        # Rating analysis
        if ratings:
            summary["rating_analysis"] = {
                "min_rating": min(ratings),
                "max_rating": max(ratings),
                "avg_rating": sum(ratings) / len(ratings),
                "total_products_with_rating": len(ratings),
            }

        # Category analysis
        if categories:
            sorted_categories = sorted(categories.items(), key=lambda x: x[1], reverse=True)
            summary["category_analysis"] = {
                "total_categories": len(categories),
                "top_categories": sorted_categories[:10],  # Top 10 categories
            }

        # Seller analysis
        if sellers:
            sorted_sellers = sorted(sellers.items(), key=lambda x: x[1], reverse=True)
            summary["seller_analysis"] = {
                "total_sellers": len(sellers),
                "top_sellers": sorted_sellers[:10],  # Top 10 sellers
            }

        return summary
