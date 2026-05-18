"""Property-based tests for CPU optimization components.

These tests verify correctness properties of the vectorized and multiprocessing
transformers using property-based testing with hypothesis.

Feature: performance-optimization
"""

import sys
from pathlib import Path
from typing import Any

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

try:
    from hypothesis import given, settings
    from hypothesis import strategies as st

    HYPOTHESIS_AVAILABLE = True
except ImportError:
    HYPOTHESIS_AVAILABLE = False
    print("⚠️ Hypothesis not installed. Install with: pip install hypothesis")

from common.multiprocessing_transformer import MultiprocessingTransformer
from common.vectorized_transformer import VectorizedTransformer


# Helper strategies for generating test data
def product_strategy():
    """Strategy for generating product dictionaries."""
    return st.fixed_dictionaries(
        {
            "product_id": st.integers(min_value=1, max_value=999999).map(str),
            "name": st.text(
                min_size=5,
                max_size=100,
                alphabet=st.characters(
                    whitelist_categories=("Lu", "Ll", "Nd", "Zs"), blacklist_characters="\x00\n\r\t"
                ),
            ),
            "price": st.one_of(
                st.floats(min_value=0, max_value=100000000, allow_nan=False, allow_infinity=False),
                st.fixed_dictionaries(
                    {
                        "current_price": st.floats(
                            min_value=0, max_value=100000000, allow_nan=False, allow_infinity=False
                        ),
                        "original_price": st.floats(
                            min_value=0, max_value=100000000, allow_nan=False, allow_infinity=False
                        ),
                    }
                ),
            ),
            "brand": st.one_of(st.none(), st.text(min_size=1, max_size=50)),
            "seller": st.one_of(
                st.none(),
                st.fixed_dictionaries(
                    {
                        "name": st.text(min_size=1, max_size=50),
                        "seller_id": st.integers(min_value=1, max_value=999999).map(str),
                    }
                ),
            ),
            "url": st.text(min_size=10, max_size=100).map(lambda x: f"https://example.com/{x}"),
        }
    )


# Property 18: Optimization Result Equivalence (Critical)
# For any input dataset, the optimized implementation should produce
# identical results to the original implementation


def _normalize_product_original(product: dict[str, Any]) -> dict[str, Any]:
    """
    Original (loop-based) product normalization for comparison.

    This simulates the original implementation that the optimized
    version should match.
    """
    result = product.copy()

    # Normalize prices
    if "price" in result:
        if isinstance(result["price"], dict):
            result["current_price"] = result["price"].get("current_price")
            result["original_price"] = result["price"].get("original_price")
        else:
            result["current_price"] = result["price"]
            result["original_price"] = None

    # Convert to numeric
    if "current_price" in result and result["current_price"] is not None:
        try:
            result["current_price"] = float(result["current_price"])
        except (ValueError, TypeError):
            result["current_price"] = None

    if "original_price" in result and result["original_price"] is not None:
        try:
            result["original_price"] = float(result["original_price"])
        except (ValueError, TypeError):
            result["original_price"] = None

    # Fill original_price with current_price if missing
    if result.get("original_price") is None and result.get("current_price") is not None:
        result["original_price"] = result["current_price"]

    # Calculate discount
    if (
        result.get("current_price") is not None
        and result.get("original_price") is not None
        and result["original_price"] > 0
    ):
        discount = (
            (result["original_price"] - result["current_price"]) / result["original_price"] * 100
        )
        result["discount_percent"] = round(discount, 2)
        if result["discount_percent"] < 0:
            result["discount_percent"] = 0
    else:
        result["discount_percent"] = None

    # Clean strings
    if "name" in result and result["name"]:
        result["name"] = " ".join(str(result["name"]).split()).strip()
        result["short_name"] = result["name"]

    if "brand" in result and result["brand"]:
        result["brand"] = str(result["brand"]).strip().replace("Thương hiệu: ", "")
        if result["brand"] == "":
            result["brand"] = None

    if "seller" in result and isinstance(result["seller"], dict):
        result["seller_name"] = result["seller"].get("name")
        if result["seller_name"]:
            result["seller_name"] = str(result["seller_name"]).strip()
    elif "seller" in result:
        result["seller_name"] = None

    if "product_id" in result:
        result["product_id"] = str(result["product_id"]).strip()

    if "url" in result:
        result["url"] = str(result["url"]).strip()

    return result


def _compare_products(original: dict[str, Any], optimized: dict[str, Any]) -> bool:
    """
    Compare two product dictionaries for equivalence.

    Handles floating point comparison with tolerance.
    """
    # Check same keys (allowing for NaN/None differences)
    keys_to_check = set(original.keys()) | set(optimized.keys())

    for key in keys_to_check:
        orig_val = original.get(key)
        opt_val = optimized.get(key)

        # Handle None/NaN equivalence
        if orig_val is None and opt_val is None:
            continue

        # Handle float comparison with tolerance
        if isinstance(orig_val, float) and isinstance(opt_val, float):
            if abs(orig_val - opt_val) > 0.01:  # 0.01 tolerance for rounding
                return False
        elif orig_val != opt_val:
            return False

    return True


def test_property_18_vectorized_equivalence_manual():
    """Property 18: Optimization Result Equivalence - Vectorized (Manual Test)

    **Validates: Requirements 10.2, 12.2**

    For any input dataset, the vectorized implementation should produce
    identical results to the original loop-based implementation.
    """
    # Test with various product configurations
    test_cases = [
        # Simple product with flat price
        {
            "product_id": "123",
            "name": "  Test Product  ",
            "price": 100.0,
            "brand": "Thương hiệu: TestBrand",
            "seller": {"name": "TestSeller", "seller_id": "456"},
            "url": "https://example.com/product",
        },
        # Product with nested price dict
        {
            "product_id": "456",
            "name": "Another Product",
            "price": {"current_price": 80.0, "original_price": 100.0},
            "brand": "Brand2",
            "seller": None,
            "url": "https://example.com/product2",
        },
        # Product with missing fields
        {
            "product_id": "789",
            "name": "Minimal Product",
            "price": None,
            "brand": None,
            "seller": None,
            "url": "https://example.com/product3",
        },
    ]

    vectorized = VectorizedTransformer()

    for i, product in enumerate(test_cases):
        # Original transformation
        original_result = _normalize_product_original(product)

        # Vectorized transformation
        df = vectorized.transform_batch([product])
        optimized_results = vectorized.transform_to_dict_list(df)

        assert len(optimized_results) == 1, f"Should return 1 result for test case {i}"
        optimized_result = optimized_results[0]

        # Compare results
        assert _compare_products(original_result, optimized_result), (
            f"Test case {i}: Results don't match.\nOriginal: {original_result}\nOptimized: {optimized_result}"
        )

        print(f"✅ Test case {i} passed")

    print("✅ Property 18 (Vectorized): Manual tests PASSED")


def test_property_18_batch_equivalence():
    """Property 18: Batch Equivalence

    Verify that processing products in batch produces same results
    as processing individually.
    """
    products = [
        {
            "product_id": str(i),
            "name": f"Product {i}",
            "price": {"current_price": float(i * 10), "original_price": float(i * 15)},
            "brand": f"Brand{i}",
            "seller": {"name": f"Seller{i}", "seller_id": str(i * 100)},
            "url": f"https://example.com/product{i}",
        }
        for i in range(1, 11)
    ]

    vectorized = VectorizedTransformer()

    # Process all at once
    df_batch = vectorized.transform_batch(products)
    batch_results = vectorized.transform_to_dict_list(df_batch)

    # Process individually
    individual_results = []
    for product in products:
        df = vectorized.transform_batch([product])
        result = vectorized.transform_to_dict_list(df)[0]
        individual_results.append(result)

    # Compare
    assert len(batch_results) == len(individual_results), (
        "Batch and individual processing should return same number of results"
    )

    for i, (batch_result, individual_result) in enumerate(
        zip(batch_results, individual_results, strict=False)
    ):
        assert _compare_products(batch_result, individual_result), (
            f"Product {i}: Batch and individual results don't match"
        )

    print("✅ Property 18: Batch equivalence test PASSED")


def test_property_18_multiprocessing_equivalence():
    """Property 18: Multiprocessing Equivalence

    Verify that multiprocessing transformation produces same results
    as sequential processing.
    """
    products = [
        {
            "product_id": str(i),
            "name": f"  Product {i}  ",
            "price": float(i * 10),
            "brand": f"Brand{i}",
            "seller": {"name": f"Seller{i}", "seller_id": str(i * 100)},
            "url": f"https://example.com/product{i}",
        }
        for i in range(1, 21)
    ]

    # Sequential processing
    sequential_results = [_normalize_product_original(p) for p in products]

    # Multiprocessing
    with MultiprocessingTransformer(num_processes=2) as mp_transformer:
        mp_results = mp_transformer.transform_parallel(products, _normalize_product_original)

    # Compare (order might differ, so sort by product_id)
    sequential_sorted = sorted(sequential_results, key=lambda x: x["product_id"])
    mp_sorted = sorted(mp_results, key=lambda x: x["product_id"])

    assert len(sequential_sorted) == len(mp_sorted), (
        "Sequential and multiprocessing should return same number of results"
    )

    for i, (seq_result, mp_result) in enumerate(zip(sequential_sorted, mp_sorted, strict=False)):
        assert _compare_products(seq_result, mp_result), (
            f"Product {i}: Sequential and multiprocessing results don't match"
        )

    print("✅ Property 18: Multiprocessing equivalence test PASSED")


# Hypothesis-based property tests (if available)
if HYPOTHESIS_AVAILABLE:

    @given(products=st.lists(product_strategy(), min_size=1, max_size=20))
    @settings(max_examples=20, deadline=None)
    def test_property_18_vectorized_hypothesis(products):
        """Property 18: Vectorized Equivalence (Hypothesis)

        **Validates: Requirements 10.2, 12.2**

        For any list of products, vectorized transformation should produce
        identical results to original implementation.
        """
        vectorized = VectorizedTransformer()

        # Original transformation
        original_results = [_normalize_product_original(p) for p in products]

        # Vectorized transformation
        df = vectorized.transform_batch(products)
        optimized_results = vectorized.transform_to_dict_list(df)

        # Should have same number of results
        assert len(original_results) == len(optimized_results)

        # Compare each result
        for orig, opt in zip(original_results, optimized_results, strict=False):
            assert _compare_products(orig, opt), (
                f"Results don't match.\nOriginal: {orig}\nOptimized: {opt}"
            )

    @given(products=st.lists(product_strategy(), min_size=1, max_size=10))
    @settings(max_examples=10, deadline=None)
    def test_property_18_multiprocessing_hypothesis(products):
        """Property 18: Multiprocessing Equivalence (Hypothesis)

        **Validates: Requirements 10.2, 12.2**

        For any list of products, multiprocessing transformation should
        produce identical results to sequential processing.
        """
        # Sequential processing
        sequential_results = [_normalize_product_original(p) for p in products]

        # Multiprocessing
        with MultiprocessingTransformer(num_processes=2) as mp_transformer:
            mp_results = mp_transformer.transform_parallel(products, _normalize_product_original)

        # Sort both by product_id for comparison
        sequential_sorted = sorted(sequential_results, key=lambda x: x["product_id"])
        mp_sorted = sorted(mp_results, key=lambda x: x["product_id"])

        assert len(sequential_sorted) == len(mp_sorted)

        for seq, mp in zip(sequential_sorted, mp_sorted, strict=False):
            assert _compare_products(seq, mp)


if __name__ == "__main__":
    print("Running Property 18: Optimization Result Equivalence tests...\n")

    test_property_18_vectorized_equivalence_manual()
    test_property_18_batch_equivalence()
    test_property_18_multiprocessing_equivalence()

    if HYPOTHESIS_AVAILABLE:
        print("\nRunning hypothesis-based tests...")
        test_property_18_vectorized_hypothesis()
        test_property_18_multiprocessing_hypothesis()
    else:
        print("\n⚠️ Skipping hypothesis tests (hypothesis not installed)")

    print("\n✅ All Property 18 tests passed!")
