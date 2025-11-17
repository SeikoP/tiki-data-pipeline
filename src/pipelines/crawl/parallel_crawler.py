"""
Parallel crawler wrapper for concurrent crawling

Features:
- ThreadPoolExecutor for I/O-bound operations
- Progress tracking
- Error handling per thread
- Rate limiting per thread
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List

logger = logging.getLogger(__name__)


class ParallelCrawler:
    """Execute crawling operations in parallel with rate limiting"""

    def __init__(
        self,
        max_workers: int = 5,
        rate_limit_per_worker: float = 0.5,
        show_progress: bool = True,
        continue_on_error: bool = True,
    ):
        """
        Args:
            max_workers: Maximum number of concurrent workers
            rate_limit_per_worker: Minimum seconds between requests per worker
            show_progress: Show progress logs
            continue_on_error: Continue if a task fails
        """
        self.max_workers = max_workers
        self.rate_limit = rate_limit_per_worker
        self.show_progress = show_progress
        self.continue_on_error = continue_on_error

        self.stats = {
            "total": 0,
            "completed": 0,
            "failed": 0,
            "errors": [],
            "start_time": 0,
            "end_time": 0,
        }

    def crawl_parallel(
        self,
        items: List[Any],
        crawler_func: Callable[[Any], Any],
        total_count: int = None,
    ) -> Dict[str, Any]:
        """
        Crawl items in parallel

        Args:
            items: List of items to crawl (e.g., URLs, product IDs)
            crawler_func: Function to crawl each item (must be thread-safe)
            total_count: Total count for progress display

        Returns:
            Dictionary with statistics and results
        """
        self.stats["total"] = total_count or len(items)
        self.stats["start_time"] = time.time()

        results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_item = {
                executor.submit(self._crawl_with_rate_limit, crawler_func, item): item
                for item in items
            }

            # Process completed tasks
            for future in as_completed(future_to_item):
                item = future_to_item[future]

                try:
                    result = future.result()
                    results.append(result)
                    self.stats["completed"] += 1

                    if self.show_progress:
                        pct = (self.stats["completed"] / self.stats["total"]) * 100
                        elapsed = time.time() - self.stats["start_time"]
                        rate = self.stats["completed"] / elapsed if elapsed > 0 else 0
                        print(
                            f"⏱️  Progress: {self.stats['completed']}/{self.stats['total']} ({pct:.1f}%) - {rate:.1f} items/s"
                        )

                except Exception as e:
                    self.stats["failed"] += 1
                    error_msg = f"Failed to crawl {item}: {str(e)}"
                    self.stats["errors"].append(error_msg)

                    if self.show_progress:
                        logger.warning(f"⚠️  {error_msg}")

                    if not self.continue_on_error:
                        raise

        self.stats["end_time"] = time.time()
        elapsed = self.stats["end_time"] - self.stats["start_time"]

        return {
            "results": results,
            "stats": {
                "total": self.stats["total"],
                "completed": self.stats["completed"],
                "failed": self.stats["failed"],
                "elapsed": elapsed,
                "rate": self.stats["completed"] / elapsed if elapsed > 0 else 0,
                "errors": self.stats["errors"][:10],  # First 10 errors
            },
        }

    def _crawl_with_rate_limit(self, crawler_func: Callable, item: Any) -> Any:
        """Crawl with rate limiting"""
        start_time = time.time()

        try:
            result = crawler_func(item)

            # Rate limiting
            elapsed = time.time() - start_time
            if elapsed < self.rate_limit:
                time.sleep(self.rate_limit - elapsed)

            return result

        except Exception as e:
            logger.error(f"Crawl error: {e}")
            raise


__all__ = ["ParallelCrawler"]
