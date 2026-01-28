"""
Batch Processing Utilities với parallel processing support Tối ưu cho xử lý dữ liệu lớn.
"""

from collections.abc import Callable, Iterator
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from typing import TypeVar

T = TypeVar("T")
R = TypeVar("R")


def chunk_list(items: list[T], chunk_size: int) -> Iterator[list[T]]:
    """Chia list thành các chunks.

    Args:
        items: List cần chia
        chunk_size: Kích thước mỗi chunk

    Yields:
        Các chunks
    """
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


def process_batch_sequential(
    items: list[T],
    processor: Callable[[T], R],
    batch_size: int = 100,
    on_error: Callable[[T, Exception], None] | None = None,
) -> list[R]:
    """Xử lý batch tuần tự.

    Args:
        items: List items cần xử lý
        processor: Function xử lý từng item
        batch_size: Kích thước batch
        on_error: Callback khi có lỗi (item, exception)

    Returns:
        List kết quả
    """
    results = []

    for batch in chunk_list(items, batch_size):
        for item in batch:
            try:
                result = processor(item)
                if result is not None:
                    results.append(result)
            except Exception as e:
                if on_error:
                    on_error(item, e)
                continue

    return results


def process_batch_parallel_threads(
    items: list[T],
    processor: Callable[[T], R],
    max_workers: int = 5,
    batch_size: int | None = None,
    on_error: Callable[[T, Exception], None] | None = None,
    timeout: float | None = None,
) -> list[R]:
    """Xử lý batch song song với threads (I/O-bound tasks)

    Args:
        items: List items cần xử lý
        processor: Function xử lý từng item
        max_workers: Số threads tối đa
        batch_size: Kích thước batch (None = xử lý tất cả cùng lúc)
        on_error: Callback khi có lỗi
        timeout: Timeout cho mỗi item (giây)

    Returns:
        List kết quả
    """
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        if batch_size:
            # Process theo batches
            futures = {}
            for batch in chunk_list(items, batch_size):
                for item in batch:
                    future = executor.submit(processor, item)
                    futures[future] = item

                # Collect results từ batch này
                for future in as_completed(futures, timeout=timeout):
                    item = futures.pop(future)
                    try:
                        result = future.result()
                        if result is not None:
                            results.append(result)
                    except Exception as e:
                        if on_error:
                            on_error(item, e)
        else:
            # Process tất cả cùng lúc
            futures = {executor.submit(processor, item): item for item in items}

            for future in as_completed(futures, timeout=timeout):
                item = futures[future]
                try:
                    result = future.result()
                    if result is not None:
                        results.append(result)
                except Exception as e:
                    if on_error:
                        on_error(item, e)

    return results


def process_batch_parallel_processes(
    items: list[T],
    processor: Callable[[T], R],
    max_workers: int = 4,
    batch_size: int | None = None,
    on_error: Callable[[T, Exception], None] | None = None,
) -> list[R]:
    """Xử lý batch song song với processes (CPU-bound tasks)

    Args:
        items: List items cần xử lý
        processor: Function xử lý từng item (phải pickle-able)
        max_workers: Số processes tối đa
        batch_size: Kích thước batch
        on_error: Callback khi có lỗi

    Returns:
        List kết quả
    """
    results = []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        if batch_size:
            # Process theo batches
            for batch in chunk_list(items, batch_size):
                futures = {executor.submit(processor, item): item for item in batch}

                for future in as_completed(futures):
                    item = futures[future]
                    try:
                        result = future.result()
                        if result is not None:
                            results.append(result)
                    except Exception as e:
                        if on_error:
                            on_error(item, e)
        else:
            # Process tất cả cùng lúc
            futures = {executor.submit(processor, item): item for item in items}

            for future in as_completed(futures):
                item = futures[future]
                try:
                    result = future.result()
                    if result is not None:
                        results.append(result)
                except Exception as e:
                    if on_error:
                        on_error(item, e)

    return results


def process_batch_with_progress(
    items: list[T],
    processor: Callable[[T], R],
    mode: str = "sequential",
    max_workers: int = 5,
    batch_size: int = 100,
    on_error: Callable[[T, Exception], None] | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
) -> list[R]:
    """Xử lý batch với progress tracking.

    Args:
        items: List items cần xử lý
        processor: Function xử lý từng item
        mode: "sequential", "threads", hoặc "processes"
        max_workers: Số workers (cho parallel modes)
        batch_size: Kích thước batch
        on_error: Callback khi có lỗi
        progress_callback: Callback để update progress (processed, total)

    Returns:
        List kết quả
    """
    total = len(items)
    processed = 0

    def processor_with_progress(item: T) -> R:
        nonlocal processed
        result = processor(item)
        processed += 1
        if progress_callback:
            progress_callback(processed, total)
        return result

    if mode == "sequential":
        return process_batch_sequential(
            items, processor_with_progress, batch_size=batch_size, on_error=on_error
        )
    elif mode == "threads":
        return process_batch_parallel_threads(
            items, processor_with_progress, max_workers=max_workers, on_error=on_error
        )
    elif mode == "processes":
        return process_batch_parallel_processes(
            items, processor_with_progress, max_workers=max_workers, on_error=on_error
        )
    else:
        raise ValueError(f"Unknown mode: {mode}")


class BatchProcessor:
    """
    Batch processor với configurable options.
    """

    def __init__(
        self,
        mode: str = "threads",
        max_workers: int = 5,
        batch_size: int = 100,
        on_error: Callable[[T, Exception], None] | None = None,
    ):
        """
        Args:
            mode: "sequential", "threads", hoặc "processes"
            max_workers: Số workers (cho parallel modes)
            batch_size: Kích thước batch
            on_error: Callback khi có lỗi
        """
        self.mode = mode
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.on_error = on_error

    def process(self, items: list[T], processor: Callable[[T], R]) -> list[R]:
        """
        Xử lý items.
        """
        return process_batch_with_progress(
            items,
            processor,
            mode=self.mode,
            max_workers=self.max_workers,
            batch_size=self.batch_size,
            on_error=self.on_error,
        )
