"""Pipeline profiler wrapper for integrating component profiling into pipeline stages.

This module provides wrapper functions that add component-level profiling
to the Crawler, Transformer, and Loader stages of the pipeline.
"""

from collections.abc import Callable

from .component_profiler import get_global_component_profiler


def profile_crawler(func: Callable) -> Callable:
    """Decorator to add component profiling to crawler functions.

    Args:
        func: Crawler function to profile

    Returns:
        Wrapped function with profiling
    """

    def wrapper(*args, **kwargs):
        profiler = get_global_component_profiler()
        profiler.start_component("Crawler")

        try:
            result = func(*args, **kwargs)

            # Count items processed
            items_processed = 0
            if isinstance(result, list):
                items_processed = len(result)
            elif isinstance(result, dict) and "products" in result:
                items_processed = len(result["products"])

            profiler.end_component("Crawler", items_processed=items_processed)
            return result
        except Exception as e:
            # End profiling even on error
            try:
                profiler.end_component("Crawler", items_processed=0)
            except Exception:
                pass
            raise e

    wrapper.__name__ = func.__name__
    wrapper.__doc__ = func.__doc__
    return wrapper


def profile_transformer(func: Callable) -> Callable:
    """Decorator to add component profiling to transformer functions.

    Args:
        func: Transformer function to profile

    Returns:
        Wrapped function with profiling
    """

    def wrapper(*args, **kwargs):
        profiler = get_global_component_profiler()
        profiler.start_component("Transformer")

        try:
            result = func(*args, **kwargs)

            # Count items processed
            items_processed = 0
            if isinstance(result, tuple) and len(result) >= 1:
                # Transform returns (transformed_products, stats)
                transformed = result[0]
                if isinstance(transformed, list):
                    items_processed = len(transformed)
            elif isinstance(result, list):
                items_processed = len(result)
            elif isinstance(result, dict):
                if "products" in result:
                    items_processed = len(result["products"])
                elif "transformed" in result:
                    items_processed = len(result["transformed"])

            profiler.end_component("Transformer", items_processed=items_processed)
            return result
        except Exception as e:
            # End profiling even on error
            try:
                profiler.end_component("Transformer", items_processed=0)
            except Exception:
                pass
            raise e

    wrapper.__name__ = func.__name__
    wrapper.__doc__ = func.__doc__
    return wrapper


def profile_loader(func: Callable) -> Callable:
    """Decorator to add component profiling to loader functions.

    Args:
        func: Loader function to profile

    Returns:
        Wrapped function with profiling
    """

    def wrapper(*args, **kwargs):
        profiler = get_global_component_profiler()
        profiler.start_component("Loader")

        try:
            result = func(*args, **kwargs)

            # Count items processed
            items_processed = 0
            if isinstance(result, dict):
                # Loader typically returns stats dict
                items_processed = (
                    result.get("loaded_count", 0)
                    or result.get("inserted", 0)
                    or result.get("updated", 0)
                )
            elif isinstance(result, int):
                items_processed = result

            profiler.end_component("Loader", items_processed=items_processed)
            return result
        except Exception as e:
            # End profiling even on error
            try:
                profiler.end_component("Loader", items_processed=0)
            except Exception:
                pass
            raise e

    wrapper.__name__ = func.__name__
    wrapper.__doc__ = func.__doc__
    return wrapper


class PipelineProfilerContext:
    """Context manager for profiling pipeline components.

    Example:
        >>> with PipelineProfilerContext("Crawler") as profiler:
        >>>     products = crawl_products()
        >>>     profiler.set_items_processed(len(products))
    """

    def __init__(self, component_name: str):
        """Initialize the context manager.

        Args:
            component_name: Name of the component (Crawler, Transformer, Loader)
        """
        self.component_name = component_name
        self.profiler = get_global_component_profiler()
        self.items_processed = 0

    def __enter__(self):
        """Start profiling the component."""
        self.profiler.start_component(self.component_name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """End profiling the component."""
        try:
            self.profiler.end_component(self.component_name, items_processed=self.items_processed)
        except Exception:
            pass
        return False  # Don't suppress exceptions

    def set_items_processed(self, count: int):
        """Set the number of items processed.

        Args:
            count: Number of items processed
        """
        self.items_processed = count

    def sample_memory(self):
        """Sample current memory usage."""
        self.profiler.sample_memory(self.component_name)
