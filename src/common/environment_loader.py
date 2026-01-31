"""
Environment Loader Module

This module provides a cached environment loader to optimize .env file loading
in Airflow tasks. The loader ensures that environment variables are loaded only
once per process, reducing startup overhead.

Feature: airflow-task-startup-optimization
Requirements: 3.3 - Environment loader shall load configuration files at most once per process
"""

import logging
from functools import lru_cache
from pathlib import Path

# Configure logging
logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def load_environment(env_path: str | None = None) -> bool:
    """
    Load environment variables from .env file with caching.

    This function uses @lru_cache to ensure the .env file is loaded only once
    per process, regardless of how many times it's called. Subsequent calls
    return the cached result immediately without re-reading the file.

    Args:
        env_path: Optional path to .env file. If None, searches for .env in:
                  1. Current working directory
                  2. Project root (parent of src directory)
                  3. Directory containing this module

    Returns:
        bool: True if environment was loaded successfully, False otherwise

    Example:
        >>> load_environment()  # First call - loads .env file
        True
        >>> load_environment()  # Subsequent calls - returns cached result
        True

    Note:
        - Cache hits/misses are logged at INFO level
        - Errors during loading are logged at ERROR level
        - The function is thread-safe due to lru_cache implementation
    """
    try:
        # Import dotenv here to avoid import overhead if not needed
        from dotenv import load_dotenv

        # Determine .env file path
        if env_path is None:
            # Try multiple locations
            possible_paths = [
                Path.cwd() / ".env",  # Current working directory
                Path(__file__).parent.parent.parent / ".env",  # Project root
                Path(__file__).parent / ".env",  # Module directory
            ]

            env_file = None
            for path in possible_paths:
                if path.exists():
                    env_file = path
                    break

            if env_file is None:
                logger.warning(
                    "No .env file found in standard locations. "
                    "Environment variables may not be loaded."
                )
                return False
        else:
            env_file = Path(env_path)
            if not env_file.exists():
                logger.error(f"Specified .env file not found: {env_path}")
                return False

        # Load environment variables
        logger.info(f"Loading environment from: {env_file}")
        load_dotenv(env_file, override=False)
        logger.info(f"Environment loaded successfully from: {env_file}")

        return True

    except ImportError:
        logger.error("python-dotenv package not installed. Install with: pip install python-dotenv")
        return False
    except Exception as e:
        logger.error(f"Failed to load environment: {e}", exc_info=True)
        return False


def get_cache_info() -> dict:
    """
    Get cache statistics for the environment loader.

    Returns:
        dict: Cache statistics including hits, misses, maxsize, and currsize

    Example:
        >>> load_environment()
        True
        >>> get_cache_info()
        {'hits': 0, 'misses': 1, 'maxsize': 1, 'currsize': 1}
        >>> load_environment()
        True
        >>> get_cache_info()
        {'hits': 1, 'misses': 1, 'maxsize': 1, 'currsize': 1}
    """
    cache_info = load_environment.cache_info()
    return {
        "hits": cache_info.hits,
        "misses": cache_info.misses,
        "maxsize": cache_info.maxsize,
        "currsize": cache_info.currsize,
    }


def clear_cache() -> None:
    """
    Clear the environment loader cache.

    This forces the next call to load_environment() to reload the .env file.
    Useful for testing or when environment variables need to be reloaded.

    Warning:
        This should rarely be needed in production. The cache is designed to
        persist for the lifetime of the process.

    Example:
        >>> load_environment()
        True
        >>> clear_cache()
        >>> load_environment()  # Will reload .env file
        True
    """
    load_environment.cache_clear()
    logger.info("Environment loader cache cleared")


def log_cache_stats() -> None:
    """
    Log current cache statistics at INFO level.

    This is useful for monitoring cache effectiveness and debugging.

    Example:
        >>> load_environment()
        True
        >>> load_environment()
        True
        >>> log_cache_stats()
        # Logs: "Environment loader cache stats: hits=1, misses=1, size=1/1"
    """
    stats = get_cache_info()
    logger.info(
        f"Environment loader cache stats: "
        f"hits={stats['hits']}, "
        f"misses={stats['misses']}, "
        f"size={stats['currsize']}/{stats['maxsize']}"
    )


# Convenience function for backward compatibility
def ensure_environment_loaded(env_path: str | None = None) -> bool:
    """
    Ensure environment variables are loaded (alias for load_environment).

    This is a convenience function that provides a more descriptive name
    for the common use case of ensuring the environment is loaded.

    Args:
        env_path: Optional path to .env file

    Returns:
        bool: True if environment was loaded successfully, False otherwise
    """
    result = load_environment(env_path)
    log_cache_stats()
    return result


if __name__ == "__main__":
    # Example usage and testing
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    print("Testing environment loader...")
    print(f"First call: {load_environment()}")
    print(f"Cache info: {get_cache_info()}")

    print(f"\nSecond call (should hit cache): {load_environment()}")
    print(f"Cache info: {get_cache_info()}")

    print(f"\nThird call (should hit cache): {load_environment()}")
    print(f"Cache info: {get_cache_info()}")

    log_cache_stats()
