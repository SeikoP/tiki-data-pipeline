"""
Unit tests for data_validator module.

Tests validation logic, completeness scoring, and field checking.
"""

import sys
from pathlib import Path

import pytest

# Add src to path
src_path = Path(__file__).parent.parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from pipelines.crawl.data_validator import (
    calculate_completeness_score,
    enrich_product_metadata,
    get_missing_fields,
    should_retry_for_field,
    validate_product_data,
)


def test_get_missing_fields_complete():
    """Test get_missing_fields with complete data."""
    product = {
        "name": "Test Product",
        "price": 100.0,
        "product_id": "12345",
        "seller_name": "Test Seller",
        "brand": "Test Brand",
        "category_id": "c123",
    }

    missing = get_missing_fields(product)
    assert missing == [], "Should have no missing fields"


def test_get_missing_fields_partial():
    """Test get_missing_fields with partial data."""
    product = {
        "name": "Test Product",
        "price": 100.0,
        "product_id": "12345",
        "seller_name": "",  # Empty string counts as missing
        "brand": None,  # None counts as missing
        # category_id missing entirely
    }

    missing = get_missing_fields(product)
    assert "seller_name" in missing
    assert "brand" in missing
    assert "category_id" in missing


def test_calculate_completeness_score_perfect():
    """Test completeness score with perfect data."""
    product = {
        "name": "Test Product",
        "price": 100.0,
        "product_id": "12345",
        "seller_name": "Test Seller",
        "brand": "Test Brand",
        "category_id": "c123",
        "rating_average": 4.5,
        "sales_count": 100,
        "stock_quantity": 50,
    }

    score = calculate_completeness_score(product)
    assert score == 1.0, "Perfect data should score 1.0"


def test_calculate_completeness_score_missing_critical():
    """Test completeness score with missing critical fields."""
    product = {
        # Missing name, price, product_id
        "seller_name": "Test Seller",
        "brand": "Test Brand",
    }

    score = calculate_completeness_score(product)
    assert score == 0.0, "Missing critical fields should score 0.0"


def test_calculate_completeness_score_missing_important():
    """Test completeness score with missing important fields."""
    product = {
        "name": "Test Product",
        "price": 100.0,
        "product_id": "12345",
        # Missing seller_name, brand, category_id
        "rating_average": 4.5,
        "sales_count": 100,
    }

    score = calculate_completeness_score(product)
    # Critical: 50% * 1.0 = 0.5
    # Important: 35% * 0.0 = 0.0
    # Optional: 15% * 0.67 = 0.1
    # Total: ~0.6
    assert 0.5 <= score <= 0.7, f"Score should be around 0.6, got {score}"


def test_validate_product_data_skip_missing_critical():
    """Test validation skips products with missing critical fields."""
    product = {
        "seller_name": "Test Seller",
        "brand": "Test Brand",
        # Missing name, price, product_id
    }

    action = validate_product_data(product, retry_count=0)
    assert action == "skip", "Should skip products with missing critical fields"


def test_validate_product_data_accept_complete():
    """Test validation accepts complete products."""
    product = {
        "name": "Test Product",
        "price": 100.0,
        "product_id": "12345",
        "seller_name": "Test Seller",
        "brand": "Test Brand",
        "category_id": "c123",
    }

    action = validate_product_data(product, retry_count=0)
    assert action == "accept", "Should accept complete products"


def test_validate_product_data_retry_missing_important():
    """Test validation retries for missing important fields."""
    product = {
        "name": "Test Product",
        "price": 100.0,
        "product_id": "12345",
        # Missing seller_name, brand
        "category_id": "c123",
    }

    action = validate_product_data(product, retry_count=0)
    assert action == "retry", "Should retry for missing important fields"


def test_validate_product_data_accept_after_max_retries():
    """Test validation accepts partial data after max retries if score >= threshold."""
    product = {
        "name": "Test Product",
        "price": 100.0,
        "product_id": "12345",
        # Missing seller_name, brand (important fields)
        # But has category_id (1 of 3 important fields)
        "category_id": "c123",
        # Add optional fields to boost score above threshold
        "rating_average": 4.5,
        "sales_count": 100,
        "stock_quantity": 50,
    }

    # After 2 retries (max), should accept if score >= 0.7
    # Score calculation:
    # Critical: 50% * 1.0 = 0.5 (all present)
    # Important: 35% * 0.33 = 0.12 (1 of 3 present)
    # Optional: 15% * 1.0 = 0.15 (all present)
    # Total: 0.5 + 0.12 + 0.15 = 0.77 >= 0.7 → accept
    action = validate_product_data(product, retry_count=2)
    assert action == "accept", "Should accept partial data after max retries if score >= 0.7"


def test_enrich_product_metadata():
    """Test product metadata enrichment."""
    product = {
        "name": "Test Product",
        "price": 100.0,
        "product_id": "12345",
        # Missing seller_name, brand
        "category_id": "c123",
    }

    enriched = enrich_product_metadata(product, retry_count=1, crawl_status="partial")

    assert "_metadata" in enriched
    assert "missing_fields" in enriched["_metadata"]
    assert "seller_name" in enriched["_metadata"]["missing_fields"]
    assert "brand" in enriched["_metadata"]["missing_fields"]
    assert enriched["_metadata"]["retry_count"] == 1
    assert enriched["_metadata"]["crawl_status"] == "partial"
    assert "data_completeness_score" in enriched["_metadata"]
    assert "data_quality" in enriched["_metadata"]


def test_should_retry_for_field():
    """Test field retry check."""
    assert should_retry_for_field("seller_name") is True
    assert should_retry_for_field("brand") is True
    assert should_retry_for_field("category_id") is True
    assert should_retry_for_field("rating_average") is False
    assert should_retry_for_field("sales_count") is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
