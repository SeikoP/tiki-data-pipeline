"""
Example demonstrating vectorized transformer integration.

This example shows how to use the VectorizedTransformerIntegration to transform
products with vectorized operations for improved performance.
"""

import logging
import sys
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipelines.transform.vectorized_transformer_integration import (
    get_vectorized_transformer_integration
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def create_sample_products(count: int = 100):
    """Create sample products for testing."""
    products = []
    
    for i in range(count):
        product = {
            "product_id": str(1000000 + i),
            "name": f"  Sample Product {i}  ",  # Extra whitespace to test cleaning
            "url": f"https://tiki.vn/product-{i}.html",
            "category_url": "https://tiki.vn/category-1.html",
            "price": {
                "current_price": 100000 + (i * 1000),
                "original_price": 150000 + (i * 1000),
            },
            "brand": "Thương hiệu: Sample Brand",  # Test prefix removal
            "seller": {
                "name": f"Seller {i}",
                "seller_id": str(5000 + i),
                "is_official": i % 2 == 0
            },
            "rating": {
                "average": 4.5,
                "total_reviews": 100 + i
            },
            "sales_count": 500 + (i * 10),
        }
        products.append(product)
    
    return products


def example_vectorized_transformer():
    """Example: Transform products using vectorized operations."""
    
    logger.info("=" * 60)
    logger.info("Vectorized Transformer Integration Example")
    logger.info("=" * 60)
    
    # Create sample products
    products = create_sample_products(100)
    logger.info(f"\nCreated {len(products)} sample products")
    
    # Example 1: Transform with vectorized mode enabled
    logger.info("\n1. Transforming with vectorized mode enabled...")
    
    transformer = get_vectorized_transformer_integration(use_vectorized=True)
    transformed, stats = transformer.transform_products(products, validate=True)
    
    logger.info(f"✅ Transformed {len(transformed)} products (vectorized mode)")
    logger.info(f"   Stats: {stats}")
    
    # Display first transformed product
    if transformed:
        logger.info(f"\n   Sample transformed product:")
        product = transformed[0]
        logger.info(f"   - ID: {product.get('product_id')}")
        logger.info(f"   - Name: {product.get('name')}")
        logger.info(f"   - Price: {product.get('current_price')}")
        logger.info(f"   - Original Price: {product.get('original_price')}")
        logger.info(f"   - Discount: {product.get('discount_percent')}%")
        logger.info(f"   - Brand: {product.get('brand')}")
        logger.info(f"   - Seller: {product.get('seller_name')}")
    
    # Example 2: Transform with vectorized mode disabled (fallback)
    logger.info("\n2. Transforming with vectorized mode disabled...")
    
    transformer = get_vectorized_transformer_integration(use_vectorized=False)
    transformed, stats = transformer.transform_products(products, validate=True)
    
    logger.info(f"✅ Transformed {len(transformed)} products (traditional mode)")
    logger.info(f"   Stats: {stats}")
    
    logger.info("\n" + "=" * 60)
    logger.info("Example completed successfully!")
    logger.info("=" * 60)


def example_performance_comparison():
    """Example: Compare vectorized vs traditional performance."""
    
    logger.info("=" * 60)
    logger.info("Performance Comparison: Vectorized vs Traditional")
    logger.info("=" * 60)
    
    # Test with different dataset sizes
    sizes = [100, 500, 1000, 5000]
    
    for size in sizes:
        logger.info(f"\nTesting with {size} products...")
        products = create_sample_products(size)
        
        # Test vectorized mode
        start_time = time.time()
        transformer = get_vectorized_transformer_integration(use_vectorized=True)
        transformed_vec, _ = transformer.transform_products(products, validate=True)
        vectorized_time = time.time() - start_time
        
        logger.info(f"  Vectorized: {len(transformed_vec)} products in {vectorized_time:.3f}s")
        
        # Test traditional mode
        start_time = time.time()
        transformer = get_vectorized_transformer_integration(use_vectorized=False)
        transformed_trad, _ = transformer.transform_products(products, validate=True)
        traditional_time = time.time() - start_time
        
        logger.info(f"  Traditional: {len(transformed_trad)} products in {traditional_time:.3f}s")
        
        # Compare
        if vectorized_time > 0 and traditional_time > 0:
            speedup = traditional_time / vectorized_time
            logger.info(f"  📊 Speedup: {speedup:.2f}x faster with vectorized mode")
    
    logger.info("\n" + "=" * 60)


def example_validation():
    """Example: Test validation with invalid products."""
    
    logger.info("=" * 60)
    logger.info("Validation Example")
    logger.info("=" * 60)
    
    # Create products with some invalid ones
    products = [
        # Valid product
        {
            "product_id": "123456",
            "name": "Valid Product",
            "url": "https://tiki.vn/product.html",
            "seller": {"name": "Valid Seller"},
            "price": {"current_price": 100000, "original_price": 150000}
        },
        # Invalid: missing product_id
        {
            "name": "Invalid Product 1",
            "url": "https://tiki.vn/product.html",
            "seller": {"name": "Seller"},
        },
        # Invalid: missing seller_name
        {
            "product_id": "123457",
            "name": "Invalid Product 2",
            "url": "https://tiki.vn/product.html",
        },
        # Invalid: bad URL format
        {
            "product_id": "123458",
            "name": "Invalid Product 3",
            "url": "not-a-url",
            "seller": {"name": "Seller"},
        },
    ]
    
    logger.info(f"\nCreated {len(products)} products (1 valid, 3 invalid)")
    
    # Transform with validation and removal of invalid products
    transformer = get_vectorized_transformer_integration(
        use_vectorized=True,
        remove_invalid=True
    )
    
    transformed, stats = transformer.transform_products(products, validate=True)
    
    logger.info(f"\n✅ Transformation complete:")
    logger.info(f"   - Total processed: {stats['total_processed']}")
    logger.info(f"   - Valid products: {stats['valid_products']}")
    logger.info(f"   - Invalid products: {stats['invalid_products']}")
    logger.info(f"   - Duplicates removed: {stats['duplicates_removed']}")
    
    logger.info("\n" + "=" * 60)


if __name__ == "__main__":
    # Run basic example
    example_vectorized_transformer()
    
    # Uncomment to run performance comparison
    # example_performance_comparison()
    
    # Uncomment to run validation example
    # example_validation()
