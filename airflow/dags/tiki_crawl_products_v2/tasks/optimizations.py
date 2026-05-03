"""
Optimization Components Integration

This module provides wrappers and utilities to integrate performance optimizations
into the DAG tasks with feature flags for easy enable/disable.
"""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import Callable
from functools import wraps

# Add src to path for imports
src_paths = [
    "/opt/airflow/src",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "src")),
    os.path.join(os.getcwd(), "src"),
]

for src_path in src_paths:
    if os.path.exists(src_path) and src_path not in sys.path:
        sys.path.insert(0, src_path)


# Feature flags - can be controlled via Airflow Variables
def get_feature_flag(flag_name: str, default: bool = False) -> bool:
    """Get feature flag value from environment or Airflow Variable"""
    try:
        try:
            from airflow.sdk import Variable
        except ImportError:
            from airflow.models import Variable

        try:
            value = Variable.get(flag_name, str(default))
        except TypeError:
            value = Variable.get(flag_name, default_var=str(default))
        return str(value).lower() in ("true", "1", "yes")
    except Exception:
        # Fallback to environment variable
        return os.getenv(flag_name, str(default)).lower() in ("true", "1", "yes")


# Profiling decorator
def profile_task(component_name: str = None):
    """Decorator to add profiling to DAG tasks

    Usage:
        @profile_task("crawler")
        def my_task(**context):
            ...
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(**context):
            # Check if profiling is enabled
            if not get_feature_flag("ENABLE_PROFILING", default=False):
                return func(**context)

            try:
                from common.profiling.component_profiler import ComponentProfiler
                from common.profiling.resource_monitor import ResourceMonitor

                logger = context.get("ti", {}).log if "ti" in context else logging.getLogger()

                # Start profiling
                comp_name = component_name or func.__name__
                profiler = ComponentProfiler()
                monitor = ResourceMonitor()

                logger.info(f"🔍 Starting profiling for: {comp_name}")
                profiler.start_component(comp_name)
                monitor.start_monitoring(interval_seconds=2.0)

                try:
                    # Execute task
                    result = func(**context)

                    # End profiling
                    stats = profiler.end_component(comp_name)
                    monitor.stop_monitoring()
                    resource_stats = monitor.get_stats()

                    # Log profiling results
                    logger.info(f"📊 Profiling results for {comp_name}:")
                    logger.info(f"  - Execution time: {stats.execution_time_ms:.2f}ms")
                    logger.info(f"  - CPU time: {stats.cpu_time_ms:.2f}ms")
                    logger.info(f"  - Memory peak: {stats.memory_peak_mb:.2f}MB")
                    logger.info(f"  - Items processed: {stats.items_processed}")
                    logger.info(f"  - Throughput: {stats.throughput_items_per_sec:.2f} items/sec")

                    # Store profiling data in XCom
                    if "ti" in context:
                        context["ti"].xcom_push(
                            key=f"profiling_{comp_name}",
                            value={
                                "execution_time_ms": stats.execution_time_ms,
                                "cpu_time_ms": stats.cpu_time_ms,
                                "memory_peak_mb": stats.memory_peak_mb,
                                "items_processed": stats.items_processed,
                                "throughput": stats.throughput_items_per_sec,
                                "resource_stats": resource_stats,
                            },
                        )

                    return result

                except Exception as e:
                    # Ensure profiling is stopped even on error
                    try:
                        profiler.end_component(comp_name)
                        monitor.stop_monitoring()
                    except Exception:
                        pass
                    raise e

            except ImportError as e:
                logger = context.get("ti", {}).log if "ti" in context else logging.getLogger()
                logger.warning(f"⚠️  Profiling not available: {e}. Running without profiling.")
                return func(**context)

        return wrapper

    return decorator


# Async crawler wrapper
class AsyncCrawlerWrapper:
    """Wrapper for async crawler with feature flag"""

    @staticmethod
    def is_enabled() -> bool:
        return get_feature_flag("USE_ASYNC_CRAWLER", default=False)

    @staticmethod
    def get_crawler():
        """Get async crawler if enabled, otherwise return None"""
        if not AsyncCrawlerWrapper.is_enabled():
            return None

        try:
            from common.async_io import AsyncCrawlerAdapter

            return AsyncCrawlerAdapter
        except ImportError:
            return None


# Vectorized transformer wrapper
class VectorizedTransformerWrapper:
    """Wrapper for vectorized transformer with feature flag"""

    @staticmethod
    def is_enabled() -> bool:
        return get_feature_flag("USE_VECTORIZED_TRANSFORM", default=False)

    @staticmethod
    def get_transformer():
        """Get vectorized transformer if enabled, otherwise return None"""
        if not VectorizedTransformerWrapper.is_enabled():
            return None

        try:
            from common.vectorized_transformer import VectorizedTransformer

            return VectorizedTransformer
        except ImportError:
            return None


# Metrics collector wrapper
class MetricsCollectorWrapper:
    """Wrapper for metrics collection with feature flag"""

    @staticmethod
    def is_enabled() -> bool:
        return get_feature_flag("ENABLE_METRICS_COLLECTION", default=False)

    @staticmethod
    def collect_metrics(task_name: str, metrics: dict, **context):
        """Collect and store metrics if enabled"""
        if not MetricsCollectorWrapper.is_enabled():
            return

        try:
            from common.metrics_collector import MetricsCollector

            collector = MetricsCollector()
            collector.record_task_metrics(task_name, metrics)

            # Store in XCom
            if "ti" in context:
                context["ti"].xcom_push(key=f"metrics_{task_name}", value=metrics)

        except ImportError:
            pass


# Configuration optimizer wrapper
class ConfigurationOptimizerWrapper:
    """Wrapper for configuration optimizer"""

    @staticmethod
    def get_optimal_config():
        """Get optimal configuration recommendations"""
        try:
            from common.configuration_analyzer import ConfigurationAnalyzer

            analyzer = ConfigurationAnalyzer()
            system_caps = analyzer.analyze_system()
            config = analyzer.recommend_config(system_caps, profile_data=None)
            return config
        except ImportError:
            return None


# Helper function to apply optimizations
def apply_optimizations_to_task(func: Callable, component_name: str = None) -> Callable:
    """Apply all enabled optimizations to a task

    Usage:
        @apply_optimizations_to_task
        def my_task(**context):
            ...
    """
    # Apply profiling
    func = profile_task(component_name)(func)

    # Add metrics collection wrapper
    @wraps(func)
    def metrics_wrapper(**context):
        import time

        start_time = time.time()

        try:
            result = func(**context)
            execution_time = time.time() - start_time

            # Collect metrics
            metrics = {
                "execution_time_seconds": execution_time,
                "status": "success",
                "timestamp": time.time(),
            }

            MetricsCollectorWrapper.collect_metrics(
                component_name or func.__name__, metrics, **context
            )

            return result

        except Exception as e:
            execution_time = time.time() - start_time

            # Collect error metrics
            metrics = {
                "execution_time_seconds": execution_time,
                "status": "error",
                "error": str(e),
                "timestamp": time.time(),
            }

            MetricsCollectorWrapper.collect_metrics(
                component_name or func.__name__, metrics, **context
            )

            raise

    return metrics_wrapper


# Export all wrappers
__all__ = [
    "profile_task",
    "apply_optimizations_to_task",
    "AsyncCrawlerWrapper",
    "VectorizedTransformerWrapper",
    "MetricsCollectorWrapper",
    "ConfigurationOptimizerWrapper",
    "get_feature_flag",
]
