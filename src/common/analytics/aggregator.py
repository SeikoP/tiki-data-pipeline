"""
Module để tổng hợp và phân tích dữ liệu sản phẩm.
"""

import hashlib
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Simple in-memory cache
_CACHE: dict[str, tuple[dict[str, Any], datetime]] = {}
_CACHE_TTL = timedelta(minutes=5)


class DataAggregator:
    """
    Class để tổng hợp và phân tích dữ liệu sản phẩm.
    """

    def __init__(self, data_file_path: str, use_db: bool = False, db_conn=None):
        """Khởi tạo DataAggregator.

        Args:
            data_file_path: Đường dẫn đến file JSON chứa dữ liệu sản phẩm
            use_db: Nếu True, query trực tiếp từ DB thay vì file
            db_conn: Database connection (required if use_db=True)
        """
        self.data_file_path = Path(data_file_path)
        self.data: dict[str, Any] = {}
        self.use_db = use_db
        self.db_conn = db_conn

        if use_db and not db_conn:
            logger.warning("⚠️  use_db=True but no db_conn provided, falling back to file")
            self.use_db = False

    def load_data(self) -> bool:
        """Load dữ liệu từ file JSON.

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
        """Tổng hợp và phân tích dữ liệu (with caching).

        Returns:
            Dictionary chứa thông tin tổng hợp
        """
        # Check cache first
        cache_key = self._get_cache_key()
        cached_result = self._get_from_cache(cache_key)
        if cached_result:
            logger.info("✅ Using cached aggregation result")
            return cached_result

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
            "trends": {},  # NEW: Trend analysis
        }

        # Metadata
        summary["metadata"] = {
            "crawled_at": self.data.get("crawled_at", ""),
            "total_products": self.data.get("total_products", 0),
            "file_path": str(self.data_file_path),
            "aggregated_at": datetime.now().isoformat(),
        }

        # Statistics từ stats
        stats = self.data.get("stats", {})
        summary["statistics"] = {
            "total_products": stats.get("total_products", 0),
            "crawled_count": stats.get("crawled_count", 0),  # Số lượng products được crawl detail
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

        # NEW: Add trend analysis (compare with previous run)
        summary["trends"] = self._analyze_trends(summary)

        # Cache result
        self._save_to_cache(cache_key, summary)

        return summary

    def _get_cache_key(self) -> str:
        """Generate cache key from file path and mtime."""
        try:
            if self.data_file_path.exists():
                mtime = self.data_file_path.stat().st_mtime
                key_str = f"{self.data_file_path}:{mtime}"
            else:
                key_str = f"{self.data_file_path}:nofile"
            return hashlib.md5(key_str.encode(), usedforsecurity=False).hexdigest()
        except Exception:
            return "default"

    def _get_from_cache(self, key: str) -> dict[str, Any] | None:
        """Get result from cache if not expired."""
        if key in _CACHE:
            result, timestamp = _CACHE[key]
            if datetime.now() - timestamp < _CACHE_TTL:
                return result
            else:
                # Expired, remove
                del _CACHE[key]
        return None

    def _save_to_cache(self, key: str, result: dict[str, Any]) -> None:
        """Save result to cache."""
        _CACHE[key] = (result, datetime.now())
        # Cleanup old entries (keep max 10)
        if len(_CACHE) > 10:
            oldest_key = min(_CACHE.keys(), key=lambda k: _CACHE[k][1])
            del _CACHE[oldest_key]

    def _analyze_trends(self, current_summary: dict[str, Any]) -> dict[str, Any]:
        """Analyze trends by comparing with previous run."""
        trends = {
            "has_previous_data": False,
            "changes": {},
        }

        try:
            # Try to load previous summary from history file
            history_file = self.data_file_path.parent / "aggregation_history.json"
            if not history_file.exists():
                return trends

            with open(history_file, encoding="utf-8") as f:
                history = json.load(f)
                if not history or len(history) < 2:
                    return trends

            # Get previous run (second to last)
            previous = history[-2] if len(history) >= 2 else None
            if not previous:
                return trends

            trends["has_previous_data"] = True
            current_stats = current_summary.get("statistics", {})
            prev_stats = previous.get("statistics", {})

            # Compare key metrics
            for metric in ["crawled_count", "with_detail", "failed", "timeout"]:
                current_val = current_stats.get(metric, 0)
                prev_val = prev_stats.get(metric, 0)
                diff = current_val - prev_val
                pct_change = (diff / prev_val * 100) if prev_val > 0 else 0

                trends["changes"][metric] = {
                    "current": current_val,
                    "previous": prev_val,
                    "diff": diff,
                    "pct_change": round(pct_change, 1),
                }

            # Success rate trend
            current_success = (
                current_stats.get("with_detail", 0) / current_stats.get("crawled_count", 1) * 100
            )
            prev_success = (
                prev_stats.get("with_detail", 0) / prev_stats.get("crawled_count", 1) * 100
            )
            trends["success_rate_change"] = round(current_success - prev_success, 1)

        except Exception as e:
            logger.warning(f"⚠️  Could not analyze trends: {e}")

        return trends

    def save_to_history(self, summary: dict[str, Any]) -> None:
        """Save current summary to history file for trend analysis."""
        try:
            history_file = self.data_file_path.parent / "aggregation_history.json"
            history = []

            # Load existing history
            if history_file.exists():
                with open(history_file, encoding="utf-8") as f:
                    history = json.load(f)

            # Add current summary
            history.append(summary)

            # Keep only last 30 runs
            if len(history) > 30:
                history = history[-30:]

            # Save
            with open(history_file, "w", encoding="utf-8") as f:
                json.dump(history, f, ensure_ascii=False, indent=2)

            logger.info(f"✅ Saved aggregation to history ({len(history)} runs)")

        except Exception as e:
            logger.warning(f"⚠️  Could not save to history: {e}")

    def _analyze_products(
        self, products: list[dict[str, Any]], summary: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Phân tích chi tiết các sản phẩm.
        """
        prices = []
        ratings = []
        categories: dict[str, int] = {}
        sellers: dict[str, int] = {}
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
