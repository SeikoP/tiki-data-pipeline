"""Unit tests for vectorized transformer operations.

These tests verify specific functionality of the VectorizedTransformer
including price normalization, string cleaning, and discount calculations.

Feature: performance-optimization
"""

import sys
from pathlib import Path

import pandas as pd
import numpy as np

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from common.vectorized_transformer import VectorizedTransformer


def test_price_normalization_flat_price():
    """Test price normalization with flat price values."""
    transformer = VectorizedTransformer()
    
    products = [
        {'product_id': '1', 'price': 100.0},
        {'product_id': '2', 'price': 200.5},
        {'product_id': '3', 'price': 0},
    ]
    
    df = transformer.transform_batch(products)
    
    assert df.loc[0, 'current_price'] == 100.0
    assert df.loc[0, 'original_price'] == 100.0  # Should fill with current_price
    
    assert df.loc[1, 'current_price'] == 200.5
    assert df.loc[1, 'original_price'] == 200.5
    
    assert df.loc[2, 'current_price'] == 0
    assert df.loc[2, 'original_price'] == 0
    
    print("✅ Flat price normalization test passed")


def test_price_normalization_nested_dict():
    """Test price normalization with nested price dictionaries."""
    transformer = VectorizedTransformer()
    
    products = [
        {
            'product_id': '1',
            'price': {'current_price': 80.0, 'original_price': 100.0}
        },
        {
            'product_id': '2',
            'price': {'current_price': 150.0, 'original_price': 200.0}
        },
    ]
    
    df = transformer.transform_batch(products)
    
    assert df.loc[0, 'current_price'] == 80.0
    assert df.loc[0, 'original_price'] == 100.0
    
    assert df.loc[1, 'current_price'] == 150.0
    assert df.loc[1, 'original_price'] == 200.0
    
    print("✅ Nested price normalization test passed")


def test_price_normalization_negative_prices():
    """Test that negative prices are handled correctly."""
    transformer = VectorizedTransformer()
    
    products = [
        {'product_id': '1', 'price': -100.0},
        {'product_id': '2', 'price': {'current_price': -50.0, 'original_price': 100.0}},
    ]
    
    df = transformer.transform_batch(products)
    
    # Negative prices should be converted to NaN
    assert pd.isna(df.loc[0, 'current_price'])
    assert pd.isna(df.loc[1, 'current_price'])
    
    print("✅ Negative price handling test passed")


def test_price_normalization_missing_original():
    """Test that missing original_price is filled with current_price."""
    transformer = VectorizedTransformer()
    
    products = [
        {'product_id': '1', 'price': {'current_price': 100.0}},
    ]
    
    df = transformer.transform_batch(products)
    
    assert df.loc[0, 'current_price'] == 100.0
    assert df.loc[0, 'original_price'] == 100.0  # Should be filled
    
    print("✅ Missing original_price test passed")


def test_string_cleaning_name():
    """Test string cleaning for product names."""
    transformer = VectorizedTransformer()
    
    products = [
        {'product_id': '1', 'name': '  Product   with   spaces  '},
        {'product_id': '2', 'name': 'Normal Product'},
        {'product_id': '3', 'name': '   '},
    ]
    
    df = transformer.transform_batch(products)
    
    assert df.loc[0, 'name'] == 'Product with spaces'
    assert df.loc[1, 'name'] == 'Normal Product'
    assert df.loc[2, 'name'] == ''  # Empty after stripping
    
    print("✅ Name string cleaning test passed")


def test_string_cleaning_brand():
    """Test string cleaning for brand field."""
    transformer = VectorizedTransformer()
    
    products = [
        {'product_id': '1', 'brand': 'Thương hiệu: Samsung'},
        {'product_id': '2', 'brand': 'Apple'},
        {'product_id': '3', 'brand': ''},
        {'product_id': '4', 'brand': None},
    ]
    
    df = transformer.transform_batch(products)
    
    assert df.loc[0, 'brand'] == 'Samsung'  # Prefix removed
    assert df.loc[1, 'brand'] == 'Apple'
    assert pd.isna(df.loc[2, 'brand'])  # Empty string -> None
    assert pd.isna(df.loc[3, 'brand'])  # None stays None
    
    print("✅ Brand string cleaning test passed")


def test_string_cleaning_seller():
    """Test string cleaning for seller field."""
    transformer = VectorizedTransformer()
    
    products = [
        {'product_id': '1', 'seller': {'name': '  Seller A  ', 'seller_id': '123'}},
        {'product_id': '2', 'seller': {'name': 'Seller B', 'seller_id': '456'}},
        {'product_id': '3', 'seller': None},
    ]
    
    df = transformer.transform_batch(products)
    
    assert df.loc[0, 'seller_name'] == 'Seller A'  # Trimmed
    assert df.loc[1, 'seller_name'] == 'Seller B'
    assert pd.isna(df.loc[2, 'seller_name'])  # None seller -> None seller_name
    
    print("✅ Seller string cleaning test passed")


def test_discount_calculation_valid():
    """Test discount calculation with valid prices."""
    transformer = VectorizedTransformer()
    
    products = [
        {'product_id': '1', 'price': {'current_price': 80.0, 'original_price': 100.0}},
        {'product_id': '2', 'price': {'current_price': 50.0, 'original_price': 100.0}},
        {'product_id': '3', 'price': {'current_price': 100.0, 'original_price': 100.0}},
    ]
    
    df = transformer.transform_batch(products)
    
    assert df.loc[0, 'discount_percent'] == 20.0  # (100-80)/100 * 100
    assert df.loc[1, 'discount_percent'] == 50.0  # (100-50)/100 * 100
    assert df.loc[2, 'discount_percent'] == 0.0   # No discount
    
    print("✅ Discount calculation test passed")


def test_discount_calculation_edge_cases():
    """Test discount calculation edge cases."""
    transformer = VectorizedTransformer()
    
    products = [
        # Missing prices
        {'product_id': '1', 'price': None},
        # Zero original price
        {'product_id': '2', 'price': {'current_price': 50.0, 'original_price': 0}},
        # Current > original (should result in 0 discount)
        {'product_id': '3', 'price': {'current_price': 150.0, 'original_price': 100.0}},
    ]
    
    df = transformer.transform_batch(products)
    
    assert pd.isna(df.loc[0, 'discount_percent'])  # No prices
    assert pd.isna(df.loc[1, 'discount_percent'])  # Zero original
    assert df.loc[2, 'discount_percent'] == 0.0    # Negative discount clamped to 0
    
    print("✅ Discount edge cases test passed")


def test_discount_calculation_accuracy():
    """Test discount calculation accuracy with various values."""
    transformer = VectorizedTransformer()
    
    products = [
        {'product_id': '1', 'price': {'current_price': 75.5, 'original_price': 100.0}},
        {'product_id': '2', 'price': {'current_price': 33.33, 'original_price': 99.99}},
    ]
    
    df = transformer.transform_batch(products)
    
    # Should be rounded to 2 decimal places
    assert df.loc[0, 'discount_percent'] == 24.5
    assert abs(df.loc[1, 'discount_percent'] - 66.67) < 0.01  # Allow small rounding difference
    
    print("✅ Discount accuracy test passed")


def test_transform_to_dict_list():
    """Test conversion from DataFrame back to dict list."""
    transformer = VectorizedTransformer()
    
    products = [
        {
            'product_id': '1',
            'name': 'Product 1',
            'price': {'current_price': 80.0, 'original_price': 100.0},
        },
        {
            'product_id': '2',
            'name': 'Product 2',
            'price': 50.0,
        },
    ]
    
    df = transformer.transform_batch(products)
    result = transformer.transform_to_dict_list(df)
    
    assert isinstance(result, list)
    assert len(result) == 2
    assert isinstance(result[0], dict)
    
    # Check that NaN is converted to None
    assert result[0]['product_id'] == '1'
    assert result[1]['product_id'] == '2'
    
    # Check discount was calculated
    assert result[0]['discount_percent'] == 20.0
    
    print("✅ Dict list conversion test passed")


def test_empty_product_list():
    """Test handling of empty product list."""
    transformer = VectorizedTransformer()
    
    df = transformer.transform_batch([])
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0
    
    print("✅ Empty product list test passed")


def test_product_id_cleaning():
    """Test product_id string cleaning."""
    transformer = VectorizedTransformer()
    
    products = [
        {'product_id': '  123  '},
        {'product_id': 456},  # Integer
        {'product_id': '789'},
    ]
    
    df = transformer.transform_batch(products)
    
    assert df.loc[0, 'product_id'] == '123'  # Trimmed
    assert df.loc[1, 'product_id'] == '456'  # Converted to string
    assert df.loc[2, 'product_id'] == '789'
    
    print("✅ Product ID cleaning test passed")


if __name__ == "__main__":
    print("Running unit tests for vectorized operations...\n")
    
    # Price normalization tests
    test_price_normalization_flat_price()
    test_price_normalization_nested_dict()
    test_price_normalization_negative_prices()
    test_price_normalization_missing_original()
    
    # String cleaning tests
    test_string_cleaning_name()
    test_string_cleaning_brand()
    test_string_cleaning_seller()
    test_product_id_cleaning()
    
    # Discount calculation tests
    test_discount_calculation_valid()
    test_discount_calculation_edge_cases()
    test_discount_calculation_accuracy()
    
    # Utility tests
    test_transform_to_dict_list()
    test_empty_product_list()
    
    print("\n✅ All unit tests passed!")
