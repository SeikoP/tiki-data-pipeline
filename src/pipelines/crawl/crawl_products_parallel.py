"""
Parallel product detail crawler wrapper

Integrates ParallelCrawler with existing crawl_products_detail.py
"""

import json
import logging
from pathlib import Path
from typing import List, Dict, Any

# Import existing crawler
from .crawl_products_detail import (
    crawl_product_detail_with_selenium,
    extract_product_detail,
)

# Import parallel crawler
from .parallel_crawler import ParallelCrawler

# Import monitoring
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from common.monitoring import PerformanceTimer
from common.cache_utils import cache_in_memory

logger = logging.getLogger(__name__)


@cache_in_memory(ttl=3600)  # Cache for 1 hour
def crawl_single_product_cached(url: str) -> Dict[str, Any]:
    """
    Crawl single product with caching
    
    Args:
        url: Product URL
        
    Returns:
        Product data dictionary
    """
    try:
        # Crawl HTML
        html_content = crawl_product_detail_with_selenium(
            url,
            save_html=False,
            verbose=False,
            use_redis_cache=True,
            use_rate_limiting=True,
        )
        
        if not html_content:
            return None
        
        # Extract data
        product_data = extract_product_detail(html_content, url, verbose=False)
        return product_data
        
    except Exception as e:
        logger.error(f"Failed to crawl {url}: {e}")
        return None


def crawl_products_parallel(
    urls: List[str],
    max_workers: int = 5,
    rate_limit: float = 0.5,
    show_progress: bool = True,
) -> Dict[str, Any]:
    """
    Crawl multiple products in parallel
    
    Args:
        urls: List of product URLs
        max_workers: Number of concurrent workers
        rate_limit: Minimum seconds between requests per worker
        show_progress: Show progress logs
        
    Returns:
        Dictionary with results and statistics
    """
    with PerformanceTimer("crawl_products_parallel", verbose=show_progress):
        crawler = ParallelCrawler(
            max_workers=max_workers,
            rate_limit_per_worker=rate_limit,
            show_progress=show_progress,
            continue_on_error=True,
        )
        
        result = crawler.crawl_parallel(
            urls,
            crawl_single_product_cached,
            total_count=len(urls),
        )
        
        # Filter out None results
        valid_results = [r for r in result["results"] if r is not None]
        
        return {
            "products": valid_results,
            "stats": {
                "total": len(urls),
                "succeeded": len(valid_results),
                "failed": len(urls) - len(valid_results),
                "elapsed": result["stats"]["elapsed"],
                "rate": result["stats"]["rate"],
            }
        }


def crawl_products_from_file(
    input_file: str,
    output_file: str,
    max_workers: int = 5,
    rate_limit: float = 0.5,
) -> Dict[str, Any]:
    """
    Crawl products from input file and save to output file
    
    Args:
        input_file: JSON file with product URLs
        output_file: Output JSON file
        max_workers: Number of concurrent workers
        rate_limit: Rate limit per worker
        
    Returns:
        Statistics dictionary
    """
    # Load URLs from file
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    urls = data.get("urls", [])
    
    if not urls:
        logger.warning("No URLs found in input file")
        return {"error": "No URLs found"}
    
    # Crawl in parallel
    result = crawl_products_parallel(
        urls,
        max_workers=max_workers,
        rate_limit=rate_limit,
        show_progress=True,
    )
    
    # Save results
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    logger.info(f"✅ Saved {len(result['products'])} products to {output_file}")
    
    return result["stats"]


__all__ = [
    "crawl_single_product_cached",
    "crawl_products_parallel",
    "crawl_products_from_file",
]
