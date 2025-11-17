"""
DAG Integration Helper

Provides drop-in replacements for DAG tasks to use optimized modules
"""

from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)


def crawl_products_optimized(
    product_urls: List[str],
    **context
) -> Dict[str, Any]:
    """
    Optimized product crawling task for Airflow DAG
    
    Drop-in replacement for sequential crawling.
    Uses ParallelCrawler with 5 workers.
    
    Args:
        product_urls: List of product URLs to crawl
        **context: Airflow context
        
    Returns:
        Dictionary with results
    """
    from pipelines.crawl.crawl_products_parallel import crawl_products_parallel
    
    ti = context['ti']
    
    logger.info(f"🚀 Starting parallel crawl of {len(product_urls)} products")
    
    result = crawl_products_parallel(
        product_urls,
        max_workers=5,
        rate_limit=0.5,
        show_progress=True,
    )
    
    # Push to XCom
    ti.xcom_push(key='crawled_products', value=result['products'])
    ti.xcom_push(key='crawl_stats', value=result['stats'])
    
    logger.info(f"✅ Crawled {result['stats']['succeeded']}/{result['stats']['total']} products")
    logger.info(f"⏱️  Time: {result['stats']['elapsed']:.2f}s")
    logger.info(f"📈 Rate: {result['stats']['rate']:.1f} products/s")
    
    return result['stats']


def load_products_optimized(
    products: List[Dict[str, Any]] = None,
    **context
) -> Dict[str, Any]:
    """
    Optimized product loading task for Airflow DAG
    
    Drop-in replacement for DataLoader.
    Uses OptimizedDataLoader with connection pooling.
    
    Args:
        products: List of products to load (optional, pulls from XCom if not provided)
        **context: Airflow context
        
    Returns:
        Dictionary with load statistics
    """
    from pipelines.load.loader_optimized import OptimizedDataLoader
    
    ti = context['ti']
    
    # Get products from XCom if not provided
    if products is None:
        products = ti.xcom_pull(key='transformed_products', task_ids='transform_task')
    
    if not products:
        logger.warning("⚠️  No products to load")
        return {"error": "No products"}
    
    logger.info(f"💾 Starting optimized load of {len(products)} products")
    
    # Initialize loader with connection pooling
    loader = OptimizedDataLoader(
        batch_size=100,
        enable_db=True,
        show_progress=True,
        continue_on_error=True,
    )
    
    # Load products
    stats = loader.load_products(
        products,
        upsert=True,
        validate_before_load=True,
    )
    
    # Push stats to XCom with specific key
    ti.xcom_push(key='load_stats', value=stats)
    
    logger.info(f"✅ Loaded {stats['db_loaded']}/{stats['total_products']} products")
    logger.info(f"⏱️  Time: {stats.get('processing_time', 0):.2f}s")
    
    if stats.get('failed_count', 0) > 0:
        logger.warning(f"⚠️  {stats['failed_count']} products failed")
    
    return stats


def get_cache_stats_task(**context) -> Dict[str, Any]:
    """
    Get cache statistics for monitoring
    
    Usage in DAG:
        get_stats = PythonOperator(
            task_id='get_cache_stats',
            python_callable=get_cache_stats_task,
        )
    """
    from common.cache_utils import get_cache_stats
    
    ti = context['ti']
    stats = get_cache_stats()
    
    logger.info(f"📊 Cache Stats:")
    logger.info(f"   Hits: {stats['hits']}")
    logger.info(f"   Misses: {stats['misses']}")
    logger.info(f"   Hit rate: {stats['hit_rate']:.1f}%")
    logger.info(f"   Memory size: {stats['memory_size']} items")
    
    ti.xcom_push(key='cache_stats', value=stats)
    
    return stats


def monitor_infrastructure_task(**context) -> Dict[str, Any]:
    """
    Monitor infrastructure health
    
    Usage in DAG:
        monitor = PythonOperator(
            task_id='monitor_infrastructure',
            python_callable=monitor_infrastructure_task,
        )
    """
    from common.infrastructure_monitor import (
        get_docker_stats,
        get_postgres_stats,
        get_redis_stats,
        get_system_resources,
    )
    
    ti = context['ti']
    
    stats = {
        'system': get_system_resources(),
        'docker': get_docker_stats(),
        'postgres': get_postgres_stats("postgresql://postgres:postgres@postgres:5432/crawl_data"),
        'redis': get_redis_stats("redis", 6379),
    }
    
    # Log summary
    logger.info("📊 Infrastructure Health:")
    if stats['system']:
        logger.info(f"   CPU: {stats['system']['cpu_percent']:.1f}%")
        logger.info(f"   Memory: {stats['system']['memory_percent']:.1f}%")
    
    if stats['postgres']:
        logger.info(f"   PostgreSQL:")
        logger.info(f"      DB Size: {stats['postgres']['db_size_mb']:.1f} MB")
        logger.info(f"      Connections: {stats['postgres']['active_connections']}")
        logger.info(f"      Cache Hit: {stats['postgres']['cache_hit_ratio']:.2f}%")
    
    if stats['redis']:
        logger.info(f"   Redis:")
        logger.info(f"      Memory: {stats['redis']['used_memory_mb']:.1f} MB")
        logger.info(f"      Hit Rate: {stats['redis']['hit_rate']:.2f}%")
    
    ti.xcom_push(key='infrastructure_stats', value=stats)
    
    return stats


# Example DAG task definitions
DAG_TASK_EXAMPLES = '''
# Example: Replace existing tasks with optimized versions

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dag_integration import (
    crawl_products_optimized,
    load_products_optimized,
    get_cache_stats_task,
    monitor_infrastructure_task,
)

with DAG(
    'tiki_crawl_optimized',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
) as dag:
    
    # Optimized crawling (parallel with 5 workers)
    crawl_task = PythonOperator(
        task_id='crawl_products_parallel',
        python_callable=crawl_products_optimized,
        op_kwargs={'product_urls': ['url1', 'url2', 'url3']},
    )
    
    # Transform task (unchanged)
    transform_task = PythonOperator(
        task_id='transform_products',
        python_callable=transform_products,  # Your existing function
    )
    
    # Optimized loading (connection pooling + batch processing)
    load_task = PythonOperator(
        task_id='load_products_optimized',
        python_callable=load_products_optimized,
    )
    
    # Monitoring tasks
    cache_stats = PythonOperator(
        task_id='get_cache_stats',
        python_callable=get_cache_stats_task,
    )
    
    infrastructure_monitor = PythonOperator(
        task_id='monitor_infrastructure',
        python_callable=monitor_infrastructure_task,
    )
    
    # Task dependencies
    crawl_task >> transform_task >> load_task
    load_task >> [cache_stats, infrastructure_monitor]
'''


__all__ = [
    "crawl_products_optimized",
    "load_products_optimized",
    "get_cache_stats_task",
    "monitor_infrastructure_task",
]
