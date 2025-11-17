"""
Memory optimization utilities

Features:
- Memory profiling
- Garbage collection management
- Large object detection
- Memory-efficient data structures
"""

import gc
import sys
from typing import Any, Dict, List


def get_object_size(obj: Any) -> int:
    """Get approximate size of object in bytes"""
    return sys.getsizeof(obj)


def profile_memory() -> Dict[str, Any]:
    """
    Get current memory usage statistics

    Returns:
        dict with memory stats
    """
    import os

    import psutil

    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()

    return {
        "rss_mb": mem_info.rss / 1024 / 1024,  # Resident Set Size
        "vms_mb": mem_info.vms / 1024 / 1024,  # Virtual Memory Size
        "percent": process.memory_percent(),
    }


def force_gc():
    """Force garbage collection"""
    collected = gc.collect()
    return collected


def get_large_objects(limit: int = 10) -> List[tuple]:
    """
    Get largest objects in memory

    Args:
        limit: Number of objects to return

    Returns:
        List of (size, type, repr) tuples
    """
    objects = []

    for obj in gc.get_objects():
        try:
            size = sys.getsizeof(obj)
            objects.append((size, type(obj).__name__, str(obj)[:100]))
        except:
            continue

    objects.sort(reverse=True, key=lambda x: x[0])
    return objects[:limit]


def optimize_dataframe_memory(df):
    """
    Optimize pandas DataFrame memory usage

    Args:
        df: pandas DataFrame

    Returns:
        Optimized DataFrame
    """
    try:
        import pandas as pd
    except ImportError:
        return df

    for col in df.columns:
        col_type = df[col].dtype

        if col_type != object:
            c_min = df[col].min()
            c_max = df[col].max()

            if str(col_type)[:3] == "int":
                if c_min > -128 and c_max < 127:
                    df[col] = df[col].astype("int8")
                elif c_min > -32768 and c_max < 32767:
                    df[col] = df[col].astype("int16")
                elif c_min > -2147483648 and c_max < 2147483647:
                    df[col] = df[col].astype("int32")

            elif str(col_type)[:5] == "float":
                if c_min > -3.4e38 and c_max < 3.4e38:
                    df[col] = df[col].astype("float32")

    return df


__all__ = [
    "get_object_size",
    "profile_memory",
    "force_gc",
    "get_large_objects",
    "optimize_dataframe_memory",
]
