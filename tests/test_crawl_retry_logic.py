"""Integration tests for retry logic.

Tests end-to-end retry flow with validation and metadata tracking.
"""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Add src to path
src_path = Path(__file__).parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))


@pytest.fixture
def mock_html_complete():
    """
    Mock HTML with complete product data.
    """
    return """
    <html>
        <h1 data-view-id="pdp_product_name">Test Product</h1>
        <div data-view-id="pdp_product_price">100,000₫</div>
        <div class="product-price__list-price">150,000₫</div>
        <div data-view-id="pdp_seller_name">Test Seller Official</div>
        <div data-view-id="pdp_product_brand">Test Brand</div>
    </html>
    """


@pytest.fixture
def mock_html_missing_seller():
    """
    Mock HTML missing seller information.
    """
    return """
    <html>
        <h1 data-view-id="pdp_product_name">Test Product</h1>
        <div data-view-id="pdp_product_price">100,000₫</div>
        <div data-view-id="pdp_product_brand">Test Brand</div>
    </html>
    """


def test_retry_logic_complete_data_first_attempt(mock_html_complete):
    """
    Test that complete data is accepted on first attempt without retry.
    """
    from pipelines.crawl.crawl_products_detail import crawl_product_with_retry

    with patch(
        "pipelines.crawl.crawl_products_detail.crawl_product_detail_with_selenium"
    ) as mock_crawl:
        # Mock successful crawl with complete data
        mock_crawl.return_value = mock_html_complete

        # Mock extract_product_detail to return complete product
        with patch("pipelines.crawl.crawl_products_detail.extract_product_detail") as mock_extract:
            mock_extract.return_value = {
                "name": "Test Product",
                "price": {"current_price": 100000},
                "product_id": "12345",
                "seller": {"name": "Test Seller Official"},
                "seller_name": "Test Seller Official",
                "brand": "Test Brand",
                "category_id": "c123",
                "category_path": ["Electronics"],
            }

            # Call retry wrapper
            result = crawl_product_with_retry(url="https://tiki.vn/test-product", verbose=False)

            # Should accept on first attempt
            assert result is not None
            assert result["_metadata"]["retry_count"] == 0
            assert result["_metadata"]["crawl_status"] == "success"
            assert result["_metadata"]["data_quality"] == "complete"
            assert mock_crawl.call_count == 1  # Only called once


def test_retry_logic_missing_seller_triggers_retry(mock_html_missing_seller, mock_html_complete):
    """
    Test that missing seller triggers retry.
    """
    from pipelines.crawl.crawl_products_detail import crawl_product_with_retry

    call_count = 0

    def mock_crawl_side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        # First call: missing seller, second call: complete
        if call_count == 1:
            return mock_html_missing_seller
        else:
            return mock_html_complete

    with patch(
        "pipelines.crawl.crawl_products_detail.crawl_product_detail_with_selenium"
    ) as mock_crawl:
        mock_crawl.side_effect = mock_crawl_side_effect

        # Mock extract to return incomplete then complete data
        extract_call_count = 0

        def mock_extract_side_effect(*args, **kwargs):
            nonlocal extract_call_count
            extract_call_count += 1
            if extract_call_count == 1:
                # First: missing seller
                return {
                    "name": "Test Product",
                    "price": {"current_price": 100000},
                    "product_id": "12345",
                    "seller": {"name": ""},  # Missing
                    "seller_name": "",
                    "brand": "Test Brand",
                    "category_id": "c123",
                }
            else:
                # Second: complete
                return {
                    "name": "Test Product",
                    "price": {"current_price": 100000},
                    "product_id": "12345",
                    "seller": {"name": "Test Seller"},
                    "seller_name": "Test Seller",
                    "brand": "Test Brand",
                    "category_id": "c123",
                }

        with patch("pipelines.crawl.crawl_products_detail.extract_product_detail") as mock_extract:
            mock_extract.side_effect = mock_extract_side_effect

            result = crawl_product_with_retry(
                url="https://tiki.vn/test-product", max_retries=2, verbose=False
            )

            # Should retry and succeed
            assert result is not None
            assert result["_metadata"]["retry_count"] == 1
            assert mock_crawl.call_count == 2  # Called twice


def test_retry_logic_accepts_partial_after_max_retries(mock_html_missing_seller):
    """
    Test that partial data is accepted after max retries.
    """
    from pipelines.crawl.crawl_products_detail import crawl_product_with_retry

    with patch(
        "pipelines.crawl.crawl_products_detail.crawl_product_detail_with_selenium"
    ) as mock_crawl:
        # Always return incomplete data
        mock_crawl.return_value = mock_html_missing_seller

        with patch("pipelines.crawl.crawl_products_detail.extract_product_detail") as mock_extract:
            # Always return incomplete product
            mock_extract.return_value = {
                "name": "Test Product",
                "price": {"current_price": 100000},
                "product_id": "12345",
                "seller": {"name": "Test Seller"},
                "seller_name": "Test Seller",
                "brand": "Test Brand",
                "category_id": "",  # Missing category_id -> retry -> partial
            }

            result = crawl_product_with_retry(
                url="https://tiki.vn/test-product", max_retries=2, verbose=False
            )

            # Should accept with partial data after max retries
            assert result is not None
            assert result["_metadata"]["retry_count"] == 2
            assert result["_metadata"]["crawl_status"] == "partial"
            assert "category_id" in result["_metadata"]["missing_fields"]

            assert mock_crawl.call_count == 3  # Initial + 2 retries


def test_retry_logic_handles_permanent_failures():
    """
    Test that permanent failures return last_product with failed status.
    """
    from pipelines.crawl.crawl_products_detail import crawl_product_with_retry

    with (
        patch(
            "pipelines.crawl.crawl_products_detail.crawl_product_detail_with_selenium"
        ) as mock_crawl,
        patch("pipelines.crawl.crawl_products_detail.extract_product_detail") as mock_extract,
    ):
        # First attempt: crawl succeeds; subsequent attempts: crawl fails
        mock_crawl.side_effect = [
            "<html><body>Valid</body></html>",
            Exception("crawl failed"),
            Exception("crawl failed"),
        ]

        # Extraction is performed only once; subsequent calls should never happen
        extracted_product = {
            "product_id": "123",
            "name": "Test Product",
            "price": {"current_price": 50000},
            "seller_name": "Test",
            "brand": "Test",
            "category_id": "",  # Missing category_id -> retry
            "_metadata": {},
        }
        mock_extract.side_effect = [
            extracted_product,
            AssertionError("extract_product_detail should not be called after the first attempt"),
        ]

        result = crawl_product_with_retry("https://tiki.vn/test-product", max_retries=2)

        # After exhausting retries, we should return the last extracted product
        assert result is not None
        assert result["product_id"] == "123"
        assert result["_metadata"]["retry_count"] == 2
        assert result["_metadata"]["crawl_status"] == "failed"

        # Ensure an error_message is set and is informative
        error_message = result["_metadata"].get("error_message")
        assert error_message is not None
        assert "crawl failed" in error_message

        # Verify call counts: initial success + 2 failing retries
        assert mock_crawl.call_count == 3
        assert mock_extract.call_count == 1


def test_retry_logic_skips_missing_critical_fields():
    """
    Test that products with missing critical fields are skipped.
    """
    from pipelines.crawl.crawl_products_detail import crawl_product_with_retry

    with patch(
        "pipelines.crawl.crawl_products_detail.crawl_product_detail_with_selenium"
    ) as mock_crawl:
        mock_crawl.return_value = "<html><body>Invalid</body></html>"

        with patch("pipelines.crawl.crawl_products_detail.extract_product_detail") as mock_extract:
            # Return product missing critical fields
            mock_extract.return_value = {
                "seller": {"name": "Test Seller"},
                "seller_name": "Test Seller",
                "brand": "Test Brand",
                "category_id": "c123",
                # Missing name, price, product_id
            }

            result = crawl_product_with_retry(url="https://tiki.vn/test-product", verbose=False)

            # Should skip
            assert result is None


def test_retry_logic_metadata_tracking():
    """
    Test that metadata is properly tracked through retries.
    """
    from pipelines.crawl.crawl_products_detail import crawl_product_with_retry

    with patch(
        "pipelines.crawl.crawl_products_detail.crawl_product_detail_with_selenium"
    ) as mock_crawl:
        mock_crawl.return_value = "<html>test</html>"

        with patch("pipelines.crawl.crawl_products_detail.extract_product_detail") as mock_extract:
            mock_extract.return_value = {
                "name": "Test Product",
                "price": {"current_price": 100000},
                "product_id": "12345",
                "seller": {"name": "Test Seller"},
                "seller_name": "Test Seller",
                "brand": "Test Brand",
                "category_id": "c123",
            }

            result = crawl_product_with_retry(url="https://tiki.vn/test-product", verbose=False)

            # Check metadata structure
            assert "_metadata" in result
            assert "started_at" in result["_metadata"]
            assert "completed_at" in result["_metadata"]
            assert "crawl_duration_ms" in result["_metadata"]
            assert "data_completeness_score" in result["_metadata"]
            assert "missing_fields" in result["_metadata"]
            assert "retry_count" in result["_metadata"]
            assert "crawl_status" in result["_metadata"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
