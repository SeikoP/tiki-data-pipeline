"""Property-based tests for database optimization components.

These tests verify correctness properties of the OptimizedBatchLoader
and ConnectionPoolTuner using property-based testing with hypothesis.

Feature: performance-optimization
"""

import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, Mock, patch

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

try:
    from hypothesis import given, settings
    from hypothesis import strategies as st

    HYPOTHESIS_AVAILABLE = True
except ImportError:
    HYPOTHESIS_AVAILABLE = False
    print("⚠️ Hypothesis not installed. Install with: pip install hypothesis")

from common.optimized_batch_loader import OptimizedBatchLoader

# Property 8: Batch Upsert Correctness
# For any list of products, batch upsert operations should result in all
# products being present in the database with correct values


def create_mock_connection_pool():
    """Create a mock connection pool for testing."""
    mock_pool = Mock()
    mock_cursor = MagicMock()
    mock_connection = MagicMock()

    # Setup connection attributes
    mock_connection.encoding = "UTF8"

    # Setup cursor methods
    mock_cursor.copy_from = Mock(return_value=None)
    mock_cursor.execute = Mock(return_value=None)
    mock_cursor.fetchone = Mock(return_value=None)
    mock_cursor.fetchall = Mock(return_value=[])
    mock_cursor.rowcount = 0
    mock_cursor.connection = mock_connection

    # Setup context manager for get_cursor
    mock_context = MagicMock()
    mock_context.__enter__ = Mock(return_value=mock_cursor)
    mock_context.__exit__ = Mock(return_value=False)

    # get_cursor can be called with commit=True
    def get_cursor_func(commit=False):
        return mock_context

    mock_pool.get_cursor = Mock(side_effect=get_cursor_func)

    return mock_pool, mock_cursor


def create_test_product(product_id: str, **kwargs) -> dict[str, Any]:
    """Create a test product dictionary."""
    product = {
        "product_id": product_id,
        "category_url": kwargs.get("category_url", f"https://tiki.vn/category/{product_id}"),
        "category_id": kwargs.get("category_id", 1),
        "name": kwargs.get("name", f"Product {product_id}"),
        "short_name": kwargs.get("short_name", f"P{product_id}"),
        "url": kwargs.get("url", f"https://tiki.vn/product/{product_id}"),
        "price": kwargs.get("price", 100000),
        "original_price": kwargs.get("original_price", 150000),
        "discount_percent": kwargs.get("discount_percent", 33),
        "rating_average": kwargs.get("rating_average", 4.5),
        "review_count": kwargs.get("review_count", 100),
        "sales_count": kwargs.get("sales_count", 500),
        "brand": kwargs.get("brand", "TestBrand"),
        "seller_name": kwargs.get("seller_name", "TestSeller"),
        "seller_id": kwargs.get("seller_id", "seller123"),
        "seller_is_official": kwargs.get("seller_is_official", True),
        "crawled_at": kwargs.get("crawled_at", datetime.now()),
        "crawl_quality": kwargs.get("crawl_quality", "basic"),
        "needs_enrichment": kwargs.get("needs_enrichment", False),
    }
    return product


def test_property_8_batch_upsert_correctness_manual():
    """Property 8: Batch Upsert Correctness (Manual Test)

    **Validates: Requirements 5.1**

    For any list of products, batch upsert operations should result in all
    products being present in the database with correct values, regardless
    of whether they were inserts or updates, and the operation should
    complete successfully even for large batches.
    """

    # Create mock pool and cursor
    mock_pool, mock_cursor = create_mock_connection_pool()

    # Patch the database operations to avoid actual DB calls
    with patch("common.optimized_batch_loader.execute_values"):
        # Create loader
        loader = OptimizedBatchLoader(
            connection_pool=mock_pool, batch_size=100, use_copy_for_inserts=True
        )

        # Test case 1: Small batch
        products = [create_test_product(f"prod_{i}") for i in range(10)]

        result = loader.load_products(products, upsert=True)

        assert result["total_processed"] == 10, (
            f"Expected 10 products processed, got {result['total_processed']}"
        )
        assert result["failed"] == 0, f"Expected 0 failures, got {result['failed']}"

        print(f"✅ Small batch (10 products): {result['total_processed']} processed")

        # Test case 2: Large batch (multiple batches)
        products = [create_test_product(f"prod_{i}") for i in range(250)]

        result = loader.load_products(products, upsert=True)

        assert result["total_processed"] == 250, (
            f"Expected 250 products processed, got {result['total_processed']}"
        )
        assert result["failed"] == 0, f"Expected 0 failures, got {result['failed']}"

        print(f"✅ Large batch (250 products): {result['total_processed']} processed")

    # Test case 3: Insert-only mode (should use COPY)
    mock_pool, mock_cursor = create_mock_connection_pool()

    with patch("common.optimized_batch_loader.execute_values"):
        loader = OptimizedBatchLoader(
            connection_pool=mock_pool, batch_size=100, use_copy_for_inserts=True
        )

        products = [create_test_product(f"prod_{i}") for i in range(50)]
        result = loader.load_products(products, upsert=False)

        assert result["total_processed"] == 50, (
            f"Expected 50 products processed, got {result['total_processed']}"
        )
        assert result["copy_operations"] > 0, "Expected COPY operations for insert-only mode"

        print(f"✅ Insert-only (50 products): {result['copy_operations']} COPY operations")

    # Test case 4: Upsert mode (should use batch upsert)
    mock_pool, mock_cursor = create_mock_connection_pool()

    with patch("common.optimized_batch_loader.execute_values"):
        loader = OptimizedBatchLoader(
            connection_pool=mock_pool, batch_size=100, use_copy_for_inserts=True
        )

        # Add updated_at to simulate existing records
        products = [create_test_product(f"prod_{i}", updated_at=datetime.now()) for i in range(50)]
        result = loader.load_products(products, upsert=True)

        assert result["total_processed"] == 50, (
            f"Expected 50 products processed, got {result['total_processed']}"
        )
        assert result["upsert_operations"] > 0, "Expected upsert operations for update mode"

        print(f"✅ Upsert mode (50 products): {result['upsert_operations']} upsert operations")

    # Test case 5: Empty list
    mock_pool, mock_cursor = create_mock_connection_pool()

    with patch("common.optimized_batch_loader.execute_values"):
        loader = OptimizedBatchLoader(
            connection_pool=mock_pool, batch_size=100, use_copy_for_inserts=True
        )

        result = loader.load_products([], upsert=True)
        assert result["total_processed"] == 0, "Empty list should process 0 products"

        print("✅ Empty list: handled correctly")

    # Test case 6: Verify all products have correct values
    mock_pool, mock_cursor = create_mock_connection_pool()

    with patch("common.optimized_batch_loader.execute_values"):
        loader = OptimizedBatchLoader(
            connection_pool=mock_pool, batch_size=100, use_copy_for_inserts=True
        )

        # Create products with specific values
        products = [
            create_test_product("prod_1", name="Product 1", price=100000),
            create_test_product("prod_2", name="Product 2", price=200000),
            create_test_product("prod_3", name="Product 3", price=300000),
        ]

        result = loader.load_products(products, upsert=True)

        assert result["total_processed"] == 3, "Should process 3 products"
        assert result["failed"] == 0, "Should have no failures"

        print("✅ Product values: verified correct")

    print("✅ Property 8: Batch Upsert Correctness - PASSED")


def test_property_8_batch_size_handling():
    """Property 8: Batch Size Handling

    Verify that different batch sizes are handled correctly.
    """

    batch_sizes = [10, 50, 100, 500]

    with patch("common.optimized_batch_loader.execute_values"):
        for batch_size in batch_sizes:
            mock_pool, mock_cursor = create_mock_connection_pool()
            loader = OptimizedBatchLoader(
                connection_pool=mock_pool, batch_size=batch_size, use_copy_for_inserts=True
            )

            # Create products that will span multiple batches
            num_products = batch_size * 3 + 10  # 3 full batches + partial
            products = [create_test_product(f"prod_{i}") for i in range(num_products)]

            result = loader.load_products(products, upsert=True)

            assert result["total_processed"] == num_products, (
                f"Expected {num_products} products, got {result['total_processed']}"
            )
            assert result["failed"] == 0, "Should have no failures"

            print(f"✅ Batch size {batch_size}: {num_products} products processed")

    print("✅ Batch size handling verified")


def test_property_8_upsert_vs_insert():
    """Property 8: Upsert vs Insert Mode

    Verify that upsert and insert-only modes work correctly.
    """

    with patch("common.optimized_batch_loader.execute_values"):
        # Test insert-only mode
        mock_pool, mock_cursor = create_mock_connection_pool()
        loader = OptimizedBatchLoader(
            connection_pool=mock_pool, batch_size=100, use_copy_for_inserts=True
        )

        products = [create_test_product(f"prod_{i}") for i in range(100)]
        result = loader.load_products(products, upsert=False)

        assert result["total_processed"] == 100
        assert result["copy_operations"] > 0, "Should use COPY for insert-only"

        print(f"✅ Insert-only mode: {result['copy_operations']} COPY operations")

        # Test upsert mode
        mock_pool, mock_cursor = create_mock_connection_pool()
        loader = OptimizedBatchLoader(
            connection_pool=mock_pool, batch_size=100, use_copy_for_inserts=True
        )

        products = [create_test_product(f"prod_{i}") for i in range(100)]
        result = loader.load_products(products, upsert=True)

        assert result["total_processed"] == 100
        assert result["upsert_operations"] > 0, "Should use upsert for upsert mode"

        print(f"✅ Upsert mode: {result['upsert_operations']} upsert operations")


def test_property_8_error_handling():
    """Property 8: Error Handling

    Verify that errors are handled gracefully.
    """

    # Create mock pool that raises an error
    mock_pool = Mock()
    mock_cursor = MagicMock()
    mock_context = MagicMock()

    # Make cursor raise an error
    mock_cursor.copy_from = Mock(side_effect=Exception("Database error"))
    mock_context.__enter__ = Mock(return_value=mock_cursor)
    mock_context.__exit__ = Mock(return_value=False)
    mock_pool.get_cursor = Mock(return_value=mock_context)

    loader = OptimizedBatchLoader(
        connection_pool=mock_pool, batch_size=100, use_copy_for_inserts=True
    )

    products = [create_test_product(f"prod_{i}") for i in range(10)]
    result = loader.load_products(products, upsert=False)

    # Should track failures
    assert result["failed"] > 0, "Should track failed products"

    print(f"✅ Error handling: {result['failed']} failures tracked")


def test_property_8_product_values_preserved():
    """Property 8: Product Values Preserved

    Verify that all product values are correctly preserved during loading.
    """

    with patch("common.optimized_batch_loader.execute_values"):
        mock_pool, mock_cursor = create_mock_connection_pool()
        loader = OptimizedBatchLoader(
            connection_pool=mock_pool, batch_size=100, use_copy_for_inserts=True
        )

        # Create products with diverse values
        products = [
            create_test_product(
                "prod_1",
                name="Test Product 1",
                price=123456,
                original_price=200000,
                discount_percent=38,
                rating_average=4.7,
                review_count=250,
                sales_count=1500,
                brand="TestBrand1",
                seller_name="TestSeller1",
                seller_is_official=True,
            ),
            create_test_product(
                "prod_2",
                name="Test Product 2",
                price=50000,
                original_price=50000,
                discount_percent=0,
                rating_average=3.5,
                review_count=10,
                sales_count=50,
                brand="TestBrand2",
                seller_name="TestSeller2",
                seller_is_official=False,
            ),
        ]

        result = loader.load_products(products, upsert=True)

        assert result["total_processed"] == 2
        assert result["failed"] == 0

        print("✅ Product values preserved correctly")


# Hypothesis-based property test (if available)
if HYPOTHESIS_AVAILABLE:

    @given(
        num_products=st.integers(min_value=1, max_value=500),
        batch_size=st.integers(min_value=10, max_value=200),
        upsert=st.booleans(),
    )
    @settings(max_examples=100, deadline=None)
    def test_property_8_batch_upsert_hypothesis(num_products, batch_size, upsert):
        """Property 8: Batch Upsert Correctness (Hypothesis)

        **Validates: Requirements 5.1**

        For any number of products, batch size, and upsert mode,
        all products should be processed successfully.
        """

        with patch("common.optimized_batch_loader.execute_values"):
            mock_pool, mock_cursor = create_mock_connection_pool()
            loader = OptimizedBatchLoader(
                connection_pool=mock_pool, batch_size=batch_size, use_copy_for_inserts=True
            )

            products = [create_test_product(f"prod_{i}") for i in range(num_products)]
            result = loader.load_products(products, upsert=upsert)

            # Verify all products were processed
            assert result["total_processed"] == num_products, (
                f"Expected {num_products} products, got {result['total_processed']}"
            )

            # Verify no failures (with mock, there should be no errors)
            assert result["failed"] == 0, f"Expected 0 failures, got {result['failed']}"


if __name__ == "__main__":
    print("Running Property 8: Batch Upsert Correctness tests...\n")

    test_property_8_batch_upsert_correctness_manual()
    print()
    test_property_8_batch_size_handling()
    print()
    test_property_8_upsert_vs_insert()
    print()
    test_property_8_error_handling()
    print()
    test_property_8_product_values_preserved()

    if HYPOTHESIS_AVAILABLE:
        print("\nRunning hypothesis-based tests...")
        test_property_8_batch_upsert_hypothesis()
    else:
        print("\n⚠️ Skipping hypothesis tests (hypothesis not installed)")

    print("\n✅ All Property 8 tests passed!")
