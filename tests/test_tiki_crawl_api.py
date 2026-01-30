from unittest.mock import AsyncMock, MagicMock

import pytest

from pipelines.crawl.crawl_products_detail import crawl_product_detail_via_api


@pytest.mark.asyncio
async def test_crawl_product_detail_via_api_success():
    """
    Verify that valid product data is extracted when the API returns success.
    """
    url = "https://tiki.vn/some-product-p12345.html"

    # Mock data returned by Tiki API
    mock_api_data = {
        "id": 12345,
        "name": "Test Product",
        "price": 50000,
        "list_price": 60000,
        "discount_rate": 17,
        "rating_average": 4.5,
        "review_count": 100,
        "current_seller": {"name": "Test Seller", "is_best_store": True, "id": 888},
        "description": "<p>Description</p>",
        "specifications": [{"name": "Color", "value": "Blue"}],
        "images": [{"base_url": "image.jpg"}],
        "brand": {"name": "Test Brand"},
        "all_time_quantity_sold": 99,
        "stock_item": {"qty": 10},
    }

    # Mock aiohttp session
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json.return_value = mock_api_data

    # Setup context manager for session.get()
    mock_session = MagicMock()
    mock_session.get.return_value.__aenter__.return_value = mock_response

    result = await crawl_product_detail_via_api(url, mock_session)

    assert result is not None
    assert result["product_id"] == "12345"
    assert result["name"] == "Test Product"
    assert result["price"]["current_price"] == 50000
    assert result["price"]["original_price"] == 60000
    assert result["rating"]["average"] == 4.5
    assert result["seller"]["name"] == "Test Seller"
    assert result["brand"] == "Test Brand"
    assert result["sales_count"] == 99
    assert result["specifications"]["Color"] == "Blue"


@pytest.mark.asyncio
async def test_crawl_product_detail_via_api_404():
    """
    Verify that None is returned on API failure.
    """
    url = "https://tiki.vn/missing-product-p99999.html"

    mock_response = AsyncMock()
    mock_response.status = 404

    mock_session = MagicMock()
    mock_session.get.return_value.__aenter__.return_value = mock_response

    result = await crawl_product_detail_via_api(url, mock_session, verbose=True)
    assert result is None


@pytest.mark.asyncio
async def test_crawl_product_detail_via_api_exception():
    """
    Verify that None is returned on connection exception.
    """
    url = "https://tiki.vn/error-product.html"

    mock_session = MagicMock()
    # Simulate an exception during the get call
    mock_session.get.side_effect = Exception("Connection Error")

    result = await crawl_product_detail_via_api(url, mock_session, verbose=True)
    assert result is None
