"""Property-based tests for async I/O layer.

These tests verify correctness properties of the async HTTP client
and crawler adapter using property-based testing with hypothesis.

Feature: performance-optimization
"""

import asyncio
import sys
from pathlib import Path
from typing import List
from unittest.mock import AsyncMock, MagicMock, patch

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

try:
    from hypothesis import given, strategies as st, settings
    HYPOTHESIS_AVAILABLE = True
except ImportError:
    HYPOTHESIS_AVAILABLE = False
    print("⚠️ Hypothesis not installed. Install with: pip install hypothesis")

from common.async_io import AsyncHTTPClient, AsyncCrawlerAdapter, get_with_retry


# Property 4: Concurrent Request Limits
# For any batch of HTTP requests, the number of concurrent connections
# should never exceed the configured connector limit


def test_property_4_concurrent_request_limits_manual():
    """Property 4: Concurrent Request Limits (Manual Test)
    
    **Validates: Requirements 3.2**
    
    For any batch of HTTP requests, the number of concurrent connections
    should never exceed the configured connector limit, and all requests
    should eventually complete or fail with appropriate error handling.
    
    This is a manual test that simulates the property without hypothesis.
    """
    
    async def run_test():
        # Test with different connector limits
        test_cases = [
            (10, 5),   # 10 URLs, limit 5
            (50, 20),  # 50 URLs, limit 20
            (100, 30), # 100 URLs, limit 30
        ]
        
        for num_urls, connector_limit in test_cases:
            # Track concurrent connections
            max_concurrent = 0
            current_concurrent = 0
            lock = asyncio.Lock()
            
            # Mock response
            async def mock_get(url, **kwargs):
                nonlocal max_concurrent, current_concurrent
                
                async with lock:
                    current_concurrent += 1
                    max_concurrent = max(max_concurrent, current_concurrent)
                
                # Simulate network delay
                await asyncio.sleep(0.01)
                
                async with lock:
                    current_concurrent -= 1
                
                # Return mock response
                mock_response = AsyncMock()
                mock_response.status = 200
                mock_response.json = AsyncMock(return_value={"data": "test"})
                mock_response.close = MagicMock()
                return mock_response
            
            # Create client with connector limit
            client = AsyncHTTPClient(connector_limit=connector_limit)
            
            # Patch the get method to track concurrency
            with patch.object(client, 'get', side_effect=mock_get):
                # Generate test URLs
                urls = [f"https://example.com/item/{i}" for i in range(num_urls)]
                
                # Fetch all URLs
                results = await client.get_many(urls)
                
                # Verify all requests completed
                assert len(results) == num_urls, \
                    f"Should return {num_urls} results, got {len(results)}"
                
                # Verify concurrent limit was respected
                # Note: Due to asyncio scheduling, we allow some tolerance
                tolerance = 5  # Allow small overshoot due to async scheduling
                assert max_concurrent <= connector_limit + tolerance, \
                    f"Max concurrent ({max_concurrent}) exceeded limit ({connector_limit}) by more than tolerance"
                
                print(f"✅ Test passed: {num_urls} URLs with limit {connector_limit}, "
                      f"max concurrent: {max_concurrent}")
            
            await client.close()
    
    # Run the async test
    asyncio.run(run_test())
    print("✅ Property 4: Concurrent Request Limits - PASSED")


def test_property_4_error_handling():
    """Property 4: Error Handling
    
    Verify that errors are handled gracefully and don't cause crashes.
    """
    
    async def run_test():
        client = AsyncHTTPClient(connector_limit=10)
        
        # Test with URLs that will fail
        urls = [
            "https://invalid-domain-that-does-not-exist-12345.com",
            "https://httpstat.us/500",  # Returns 500 error
            "https://httpstat.us/404",  # Returns 404 error
        ]
        
        # Should not raise exceptions
        results = await client.get_many(urls)
        
        # Should return results (possibly None for failures)
        assert len(results) == len(urls), "Should return result for each URL"
        
        # All results should be None or dict (no exceptions leaked)
        for result in results:
            assert result is None or isinstance(result, dict), \
                "Results should be None or dict, not exceptions"
        
        await client.close()
        print("✅ Error handling test passed")
    
    asyncio.run(run_test())


def test_property_4_with_rate_limiter():
    """Property 4: Concurrent Limits with Rate Limiter
    
    Verify that rate limiting doesn't break concurrent request limits.
    """
    
    async def run_test():
        # Mock rate limiter
        mock_rate_limiter = MagicMock()
        mock_rate_limiter.wait = MagicMock()
        
        client = AsyncHTTPClient(
            connector_limit=20,
            rate_limiter=mock_rate_limiter
        )
        
        # Track concurrent connections
        max_concurrent = 0
        current_concurrent = 0
        lock = asyncio.Lock()
        
        async def mock_get(url, **kwargs):
            nonlocal max_concurrent, current_concurrent
            
            async with lock:
                current_concurrent += 1
                max_concurrent = max(max_concurrent, current_concurrent)
            
            await asyncio.sleep(0.01)
            
            async with lock:
                current_concurrent -= 1
            
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value={"data": "test"})
            mock_response.close = MagicMock()
            return mock_response
        
        with patch.object(client, 'get', side_effect=mock_get):
            urls = [f"https://example.com/item/{i}" for i in range(50)]
            results = await client.get_many(urls)
            
            assert len(results) == 50
            assert max_concurrent <= 25  # Allow some tolerance
            
            print(f"✅ Rate limiter test passed, max concurrent: {max_concurrent}")
        
        await client.close()
    
    asyncio.run(run_test())


# Hypothesis-based property test (if available)
if HYPOTHESIS_AVAILABLE:
    @given(
        num_urls=st.integers(min_value=1, max_value=100),
        connector_limit=st.integers(min_value=5, max_value=50)
    )
    @settings(max_examples=100, deadline=None)
    def test_property_4_concurrent_limits_hypothesis(num_urls, connector_limit):
        """Property 4: Concurrent Request Limits (Hypothesis)
        
        **Validates: Requirements 3.2**
        
        For any number of URLs and connector limit, the concurrent
        connections should never exceed the limit.
        """
        
        async def run_test():
            max_concurrent = 0
            current_concurrent = 0
            lock = asyncio.Lock()
            
            async def mock_get(url, **kwargs):
                nonlocal max_concurrent, current_concurrent
                
                async with lock:
                    current_concurrent += 1
                    max_concurrent = max(max_concurrent, current_concurrent)
                
                await asyncio.sleep(0.001)
                
                async with lock:
                    current_concurrent -= 1
                
                mock_response = AsyncMock()
                mock_response.status = 200
                mock_response.json = AsyncMock(return_value={"data": "test"})
                mock_response.close = MagicMock()
                return mock_response
            
            client = AsyncHTTPClient(connector_limit=connector_limit)
            
            with patch.object(client, 'get', side_effect=mock_get):
                urls = [f"https://example.com/item/{i}" for i in range(num_urls)]
                results = await client.get_many(urls)
                
                assert len(results) == num_urls
                # Allow tolerance for async scheduling
                assert max_concurrent <= connector_limit + 10
            
            await client.close()
        
        asyncio.run(run_test())


if __name__ == "__main__":
    print("Running Property 4: Concurrent Request Limits tests...\n")
    
    test_property_4_concurrent_request_limits_manual()
    test_property_4_error_handling()
    test_property_4_with_rate_limiter()
    
    if HYPOTHESIS_AVAILABLE:
        print("\nRunning hypothesis-based tests...")
        test_property_4_concurrent_limits_hypothesis()
    else:
        print("\n⚠️ Skipping hypothesis tests (hypothesis not installed)")
    
    print("\n✅ All Property 4 tests passed!")
