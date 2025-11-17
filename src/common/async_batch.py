"""
Async batch processor for concurrent I/O operations

Features:
- asyncio for async operations
- Batch processing with concurrency control
- Memory-efficient streaming
"""

import asyncio
import logging
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)


class AsyncBatchProcessor:
    """Process batches asynchronously with concurrency control"""

    def __init__(
        self,
        batch_size: int = 100,
        max_concurrent: int = 5,
        show_progress: bool = True,
    ):
        """
        Args:
            batch_size: Items per batch
            max_concurrent: Maximum concurrent tasks
            show_progress: Show progress logs
        """
        self.batch_size = batch_size
        self.max_concurrent = max_concurrent
        self.show_progress = show_progress

        self.semaphore = None
        self.processed = 0
        self.failed = 0

    async def process_async(
        self,
        items: list[Any],
        async_processor: Callable,
        total_count: int | None = None,
    ):
        """
        Process items asynchronously

        Args:
            items: List of items to process
            async_processor: Async function to process each item
            total_count: Total count for progress

        Returns:
            List of results
        """
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
        total = total_count or len(items)

        tasks = [self._process_with_semaphore(item, async_processor) for item in items]

        results = []
        for coro in asyncio.as_completed(tasks):
            try:
                result = await coro
                results.append(result)
                self.processed += 1

                if self.show_progress:
                    pct = (self.processed / total) * 100
                    print(f"⚡ Async progress: {self.processed}/{total} ({pct:.1f}%)")

            except Exception as e:
                self.failed += 1
                logger.warning(f"⚠️  Async task failed: {e}")

        return results

    async def _process_with_semaphore(self, item: Any, processor: Callable):
        """Process with semaphore for concurrency control"""
        async with self.semaphore:
            return await processor(item)


__all__ = ["AsyncBatchProcessor"]
