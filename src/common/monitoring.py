"""
Performance monitoring utilities for Tiki Data Pipeline

Provides decorators and utilities for measuring and logging performance metrics
"""

import functools
import logging
import time
from datetime import datetime
from typing import Any, Callable

logger = logging.getLogger(__name__)


def measure_time(operation_name: str | None = None, log_args: bool = False):
    """
    Decorator to measure and log function execution time
    
    Args:
        operation_name: Custom name for the operation (defaults to function name)
        log_args: Whether to log function arguments (default: False)
    
    Example:
        @measure_time("crawl_category")
        def crawl_products(url):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            name = operation_name or func.__name__
            start_time = time.time()
            
            # Log start
            if log_args:
                logger.info(f"⏱️  Starting {name} with args={args[:2] if args else []}, kwargs={list(kwargs.keys())}")
            else:
                logger.info(f"⏱️  Starting {name}")
            
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                
                # Log success with duration
                logger.info(f"✅ {name} completed in {duration:.2f}s")
                
                return result
                
            except Exception as e:
                duration = time.time() - start_time
                logger.error(f"❌ {name} failed after {duration:.2f}s: {type(e).__name__}: {e}")
                raise
                
        return wrapper
    return decorator


def measure_time_simple(func: Callable) -> Callable:
    """
    Simple decorator to measure execution time (no parameters)
    
    Example:
        @measure_time_simple
        def my_function():
            ...
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            logger.info(f"⏱️  {func.__name__} took {duration:.2f}s")
            return result
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"❌ {func.__name__} failed after {duration:.2f}s: {e}")
            raise
    return wrapper


class PerformanceTimer:
    """Context manager for timing code blocks"""
    
    def __init__(self, operation_name: str, verbose: bool = True):
        """
        Args:
            operation_name: Name of the operation being timed
            verbose: Whether to log timing info
        """
        self.operation_name = operation_name
        self.verbose = verbose
        self.start_time = None
        self.end_time = None
        self.duration = None
    
    def __enter__(self):
        self.start_time = time.time()
        if self.verbose:
            logger.info(f"⏱️  Starting {self.operation_name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.time()
        self.duration = self.end_time - self.start_time
        
        if exc_type is None:
            if self.verbose:
                logger.info(f"✅ {self.operation_name} completed in {self.duration:.2f}s")
        else:
            if self.verbose:
                logger.error(f"❌ {self.operation_name} failed after {self.duration:.2f}s")
        
        return False  # Don't suppress exceptions


class PerformanceMetrics:
    """
    Collect and track performance metrics
    
    Example:
        metrics = PerformanceMetrics()
        
        with metrics.timer("crawl"):
            # ... do work ...
        
        with metrics.timer("transform"):
            # ... do work ...
        
        metrics.print_summary()
    """
    
    def __init__(self):
        self.timings = {}
        self.counts = {}
    
    def timer(self, operation_name: str):
        """Get a timer for an operation"""
        return _MetricsTimer(self, operation_name)
    
    def record(self, operation_name: str, duration: float):
        """Record a timing"""
        if operation_name not in self.timings:
            self.timings[operation_name] = []
            self.counts[operation_name] = 0
        
        self.timings[operation_name].append(duration)
        self.counts[operation_name] += 1
    
    def get_stats(self, operation_name: str) -> dict[str, float]:
        """Get statistics for an operation"""
        if operation_name not in self.timings:
            return {}
        
        timings = self.timings[operation_name]
        return {
            "count": len(timings),
            "total": sum(timings),
            "mean": sum(timings) / len(timings),
            "min": min(timings),
            "max": max(timings),
        }
    
    def print_summary(self):
        """Print summary of all metrics"""
        logger.info("=" * 60)
        logger.info("PERFORMANCE SUMMARY")
        logger.info("=" * 60)
        
        for operation in sorted(self.timings.keys()):
            stats = self.get_stats(operation)
            logger.info(
                f"{operation:30s} | "
                f"Count: {stats['count']:4d} | "
                f"Total: {stats['total']:7.2f}s | "
                f"Avg: {stats['mean']:6.2f}s | "
                f"Min: {stats['min']:6.2f}s | "
                f"Max: {stats['max']:6.2f}s"
            )
        
        logger.info("=" * 60)


class _MetricsTimer:
    """Internal timer class for PerformanceMetrics"""
    
    def __init__(self, metrics: PerformanceMetrics, operation_name: str):
        self.metrics = metrics
        self.operation_name = operation_name
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        self.metrics.record(self.operation_name, duration)
        return False


def log_slow_operations(threshold_seconds: float = 1.0):
    """
    Decorator to log only slow operations
    
    Args:
        threshold_seconds: Only log if duration exceeds this threshold
    
    Example:
        @log_slow_operations(threshold_seconds=2.0)
        def crawl_product(url):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            start_time = time.time()
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            
            if duration >= threshold_seconds:
                logger.warning(f"🐌 Slow operation: {func.__name__} took {duration:.2f}s (threshold: {threshold_seconds}s)")
            
            return result
        return wrapper
    return decorator
