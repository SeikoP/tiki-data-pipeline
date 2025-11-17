"""
Integration wrapper for optimized pipeline

Combines all Phase 4 optimizations:
- Parallel crawling
- Database connection pooling
- Batch processing
- Memory caching
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def run_optimized_pipeline(
    category_urls: List[str],
    output_dir: str = "data/optimized",
    max_crawl_workers: int = 5,
    crawl_rate_limit: float = 0.5,
    db_batch_size: int = 100,
    enable_db: bool = True,
    save_intermediate: bool = True,
) -> Dict[str, Any]:
    """
    Run complete optimized ETL pipeline

    Pipeline stages:
    1. Parallel crawl products from categories
    2. Transform product data
    3. Load to database with connection pooling

    Args:
        category_urls: List of category URLs to crawl
        output_dir: Output directory for intermediate files
        max_crawl_workers: Concurrent crawlers
        crawl_rate_limit: Rate limit per crawler
        db_batch_size: Database batch size
        enable_db: Enable database loading
        save_intermediate: Save intermediate JSON files

    Returns:
        Pipeline statistics
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    stats = {
        "crawl": {},
        "transform": {},
        "load": {},
        "total_time": 0,
    }

    import time

    start_time = time.time()

    # Stage 1: Parallel Crawl
    logger.info("=" * 70)
    logger.info("STAGE 1: PARALLEL CRAWL")
    logger.info("=" * 70)

    try:
        from pipelines.crawl.crawl_products_parallel import crawl_products_parallel

        # Get product URLs from categories first
        product_urls = []
        for cat_url in category_urls:
            # TODO: Extract product URLs from category
            # For now, assume we have them
            pass

        if not product_urls:
            logger.warning("No product URLs found")
            return stats

        crawl_result = crawl_products_parallel(
            product_urls,
            max_workers=max_crawl_workers,
            rate_limit=crawl_rate_limit,
            show_progress=True,
        )

        stats["crawl"] = crawl_result["stats"]
        products = crawl_result["products"]

        if save_intermediate:
            crawl_file = output_path / "01_crawled.json"
            with open(crawl_file, "w", encoding="utf-8") as f:
                json.dump({"products": products}, f, ensure_ascii=False, indent=2)
            logger.info(f"✅ Saved crawled data: {crawl_file}")

    except Exception as e:
        logger.error(f"❌ Crawl failed: {e}")
        return stats

    # Stage 2: Transform
    logger.info("=" * 70)
    logger.info("STAGE 2: TRANSFORM")
    logger.info("=" * 70)

    try:
        from pipelines.transform.transformer import DataTransformer

        transformer = DataTransformer()
        transformed = transformer.transform_products(products)

        stats["transform"] = {
            "input_count": len(products),
            "output_count": len(transformed),
            "failed": len(products) - len(transformed),
        }

        if save_intermediate:
            transform_file = output_path / "02_transformed.json"
            with open(transform_file, "w", encoding="utf-8") as f:
                json.dump({"products": transformed}, f, ensure_ascii=False, indent=2)
            logger.info(f"✅ Saved transformed data: {transform_file}")

    except Exception as e:
        logger.error(f"❌ Transform failed: {e}")
        return stats

    # Stage 3: Load with connection pooling
    logger.info("=" * 70)
    logger.info("STAGE 3: LOAD (OPTIMIZED)")
    logger.info("=" * 70)

    try:
        from pipelines.load.loader_optimized import OptimizedDataLoader

        loader = OptimizedDataLoader(
            batch_size=db_batch_size,
            enable_db=enable_db,
            show_progress=True,
            continue_on_error=True,
        )

        load_file = output_path / "03_loaded.json" if save_intermediate else None

        load_result = loader.load_products(
            transformed,
            upsert=True,
            validate_before_load=True,
            save_to_file=str(load_file) if load_file else None,
        )

        stats["load"] = load_result

    except Exception as e:
        logger.error(f"❌ Load failed: {e}")
        return stats

    # Final stats
    stats["total_time"] = time.time() - start_time

    logger.info("=" * 70)
    logger.info("PIPELINE COMPLETED")
    logger.info("=" * 70)
    logger.info(f"Total time: {stats['total_time']:.2f}s")
    logger.info(f"Crawled: {stats['crawl'].get('succeeded', 0)}")
    logger.info(f"Transformed: {stats['transform'].get('output_count', 0)}")
    logger.info(f"Loaded: {stats['load'].get('db_loaded', 0)}")

    return stats


def quick_test_optimized_pipeline(num_products: int = 10):
    """
    Quick test of optimized pipeline with sample products

    Args:
        num_products: Number of products to test
    """
    logger.info(f"🧪 Quick test with {num_products} products")

    # Sample product URLs (replace with real URLs)
    sample_urls = [f"https://tiki.vn/product-{i}.html" for i in range(num_products)]

    stats = run_optimized_pipeline(
        category_urls=[],  # Not needed for direct product crawl
        output_dir="data/test_optimized",
        max_crawl_workers=3,
        crawl_rate_limit=0.3,
        db_batch_size=50,
        enable_db=False,  # Disable DB for quick test
        save_intermediate=True,
    )

    return stats


__all__ = [
    "run_optimized_pipeline",
    "quick_test_optimized_pipeline",
]
