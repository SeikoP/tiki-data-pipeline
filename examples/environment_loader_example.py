"""
Example usage of the cached environment loader

This example demonstrates how to use the environment_loader module
to efficiently load environment variables in Airflow tasks.

Feature: airflow-task-startup-optimization
Requirements: 3.3 - Environment loader shall load configuration files at most once per process
"""

import logging
import time
from src.common.environment_loader import (
    load_environment,
    get_cache_info,
    log_cache_stats,
    ensure_environment_loaded,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def simulate_task_startup():
    """Simulate an Airflow task startup that loads environment"""
    logger.info("=" * 60)
    logger.info("Simulating Airflow Task Startup")
    logger.info("=" * 60)
    
    # First call - loads .env file
    logger.info("\n1. First call to load_environment():")
    start = time.time()
    result = load_environment()
    duration = time.time() - start
    logger.info(f"   Result: {result}")
    logger.info(f"   Duration: {duration*1000:.2f}ms")
    logger.info(f"   Cache info: {get_cache_info()}")
    
    # Second call - uses cache (much faster)
    logger.info("\n2. Second call to load_environment() (should use cache):")
    start = time.time()
    result = load_environment()
    duration = time.time() - start
    logger.info(f"   Result: {result}")
    logger.info(f"   Duration: {duration*1000:.2f}ms (cached!)")
    logger.info(f"   Cache info: {get_cache_info()}")
    
    # Third call - still uses cache
    logger.info("\n3. Third call to load_environment() (should use cache):")
    start = time.time()
    result = load_environment()
    duration = time.time() - start
    logger.info(f"   Result: {result}")
    logger.info(f"   Duration: {duration*1000:.2f}ms (cached!)")
    logger.info(f"   Cache info: {get_cache_info()}")
    
    # Log final cache statistics
    logger.info("\n4. Final cache statistics:")
    log_cache_stats()


def demonstrate_convenience_function():
    """Demonstrate the convenience function"""
    logger.info("\n" + "=" * 60)
    logger.info("Using Convenience Function")
    logger.info("=" * 60)
    
    # Using ensure_environment_loaded (logs cache stats automatically)
    logger.info("\nCalling ensure_environment_loaded():")
    result = ensure_environment_loaded()
    logger.info(f"Result: {result}")


def demonstrate_multiple_imports():
    """Demonstrate that multiple imports in different modules would share cache"""
    logger.info("\n" + "=" * 60)
    logger.info("Simulating Multiple Module Imports")
    logger.info("=" * 60)
    
    # In a real scenario, different modules would import and call load_environment
    # They would all share the same cache within the same process
    
    logger.info("\nModule A calls load_environment():")
    load_environment()
    logger.info(f"Cache info: {get_cache_info()}")
    
    logger.info("\nModule B calls load_environment():")
    load_environment()
    logger.info(f"Cache info: {get_cache_info()}")
    
    logger.info("\nModule C calls load_environment():")
    load_environment()
    logger.info(f"Cache info: {get_cache_info()}")
    
    logger.info("\nAll modules share the same cache - only one .env file load!")


def main():
    """Run all examples"""
    logger.info("\n" + "=" * 60)
    logger.info("ENVIRONMENT LOADER EXAMPLE")
    logger.info("=" * 60)
    
    # Example 1: Basic task startup simulation
    simulate_task_startup()
    
    # Example 2: Convenience function
    demonstrate_convenience_function()
    
    # Example 3: Multiple imports
    demonstrate_multiple_imports()
    
    logger.info("\n" + "=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    logger.info("""
Key Benefits:
1. .env file is loaded only ONCE per process (first call)
2. Subsequent calls return cached result instantly (microseconds)
3. Reduces task startup overhead significantly
4. Thread-safe due to lru_cache implementation
5. Cache statistics available for monitoring

Usage in Airflow DAGs:
    from src.common.environment_loader import load_environment
    
    # At the top of your DAG file or task function
    load_environment()  # Fast on subsequent calls!
    
    # Now use environment variables
    import os
    db_host = os.getenv('POSTGRES_HOST')
    """)


if __name__ == "__main__":
    main()
