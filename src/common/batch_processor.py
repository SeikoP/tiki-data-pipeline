"""Optimized batch processing for large datasets.

Features:
- Configurable batch sizes
- Progress tracking
- Error handling per batch
- Memory-efficient iteration
"""

import time
from collections.abc import Callable, Generator, Iterable
from typing import Any


def create_batches(items: Iterable[Any], batch_size: int) -> Generator[list[Any], None, None]:
    """Create batches from an iterable.

    Args:
        items: Iterable of items
        batch_size: Size of each batch

    Yields:
        Lists of items in batches
    """
    batch = []
    for item in items:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []

    if batch:  # Yield remaining items
        yield batch


class BatchProcessor:
    """
    Process items in batches with progress tracking and error handling.
    """

    def __init__(
        self, batch_size: int = 100, show_progress: bool = True, continue_on_error: bool = True
    ):
        """
        Args:
            batch_size: Number of items per batch
            show_progress: Show progress logs
            continue_on_error: Continue processing if a batch fails
        """
        self.batch_size = batch_size
        self.show_progress = show_progress
        self.continue_on_error = continue_on_error

        self.total_processed = 0
        self.total_failed = 0
        self.total_time = 0.0

    def process(
        self,
        items: Iterable[Any],
        processor: Callable[[list[Any]], Any],
        total_count: int | None = None,
    ) -> dict:
        """Process items in batches.

        Args:
            items: Iterable of items to process
            processor: Function to process each batch
            total_count: Total number of items (for progress display)

        Returns:
            dict with processing statistics
        """
        start_time = time.time()
        batch_num = 0

        for batch in create_batches(items, self.batch_size):
            batch_num += 1
            batch_start = time.time()

            try:
                # Process batch
                processor(batch)

                batch_time = time.time() - batch_start
                self.total_processed += len(batch)
                self.total_time += batch_time

                if self.show_progress:
                    progress = ""
                    if total_count:
                        pct = (self.total_processed / total_count) * 100
                        progress = f" ({pct:.1f}%)"

                    rate = len(batch) / batch_time if batch_time > 0 else 0
                    print(
                        f"⏱️  Batch {batch_num}: {len(batch)} items in {batch_time:.2f}s ({rate:.1f} items/s){progress}"
                    )

            except Exception as e:
                self.total_failed += len(batch)

                if self.show_progress:
                    print(f"❌ Batch {batch_num} failed: {e}")

                if not self.continue_on_error:
                    raise

        total_time = time.time() - start_time

        return {
            "total_processed": self.total_processed,
            "total_failed": self.total_failed,
            "total_batches": batch_num,
            "total_time": total_time,
            "avg_rate": self.total_processed / total_time if total_time > 0 else 0,
        }


__all__ = [
    "create_batches",
    "BatchProcessor",
]
