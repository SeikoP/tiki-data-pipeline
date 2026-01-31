"""Unit tests for async I/O error handling.

These tests verify error handling behavior of the async HTTP client
including connection failures, timeouts, and rate limiting integration.

Feature: performance-optimization
Requirements: 3.2, 3.7
"""

import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import aiohttp

from common.async_io import AsyncHTTPClient, get_with_retry


def test_connection_failure_with_retry():
    """Test connection failures are retried with exponential backoff.

    Requirements: 3.7
    """

    async def run_test():
        client = AsyncHTTPClient(connector_limit=10)

        # Mock get to simulate connection failures
        call_count = 0

        async def mock_get_failing(url, **kwargs):
            nonlocal call_count
            call_count += 1

            if call_count < 3:
                # Fail first 2 attempts
                raise aiohttp.ClientError("Connection failed")
            else:
                # Succeed on 3rd attempt
                mock_response = AsyncMock()
                mock_response.status = 200
                mock_response.json = AsyncMock(return_value={"success": True})
                mock_response.close = MagicMock()
                return mock_response

        with patch.object(client, "get", side_effect=mock_get_failing):
            result = await get_with_retry(client, "https://example.com/test", max_retries=3)

            # Should succeed after retries
            assert result is not None, "Should succeed after retries"
            assert result["success"] is True
            assert call_count == 3, f"Should have called get 3 times, got {call_count}"

        await client.close()
        print("✅ Connection failure with retry test passed")

    asyncio.run(run_test())


def test_connection_failure_exhausted_retries():
    """Test that failures after max retries return None.

    Requirements: 3.7
    """

    async def run_test():
        client = AsyncHTTPClient(connector_limit=10)

        # Mock get to always fail
        async def mock_get_always_failing(url, **kwargs):
            raise aiohttp.ClientError("Connection failed")

        with patch.object(client, "get", side_effect=mock_get_always_failing):
            result = await get_with_retry(client, "https://example.com/test", max_retries=3)

            # Should return None after exhausting retries
            assert result is None, "Should return None after max retries"

        await client.close()
        print("✅ Exhausted retries test passed")

    asyncio.run(run_test())


def test_timeout_handling():
    """Test timeout handling in async requests.

    Requirements: 3.2
    """

    async def run_test():
        # Create client with short timeout
        client = AsyncHTTPClient(connector_limit=10, timeout_total=1, timeout_connect=1)

        # Mock get to simulate timeout
        async def mock_get_timeout(url, **kwargs):
            raise TimeoutError("Request timed out")

        with patch.object(client, "get", side_effect=mock_get_timeout):
            result = await get_with_retry(client, "https://example.com/slow", max_retries=2)

            # Should return None after timeouts
            assert result is None, "Should return None on timeout"

        await client.close()
        print("✅ Timeout handling test passed")

    asyncio.run(run_test())


def test_rate_limit_429_handling():
    """Test handling of 429 (rate limit) responses.

    Requirements: 3.2, 3.7
    """

    async def run_test():
        client = AsyncHTTPClient(connector_limit=10)

        call_count = 0

        async def mock_get_rate_limited(url, **kwargs):
            nonlocal call_count
            call_count += 1

            mock_response = AsyncMock()

            if call_count < 2:
                # Return 429 on first attempt
                mock_response.status = 429
            else:
                # Return 200 on retry
                mock_response.status = 200

            mock_response.json = AsyncMock(return_value={"data": "test"})
            mock_response.close = MagicMock()
            return mock_response

        with patch.object(client, "get", side_effect=mock_get_rate_limited):
            result = await get_with_retry(client, "https://example.com/test", max_retries=3)

            # Should succeed after retry
            assert result is not None, "Should succeed after rate limit retry"
            assert call_count == 2, f"Should have retried once, got {call_count} calls"

        await client.close()
        print("✅ Rate limit 429 handling test passed")

    asyncio.run(run_test())


def test_rate_limiter_integration():
    """Test integration with rate limiter.

    Requirements: 3.2
    """

    async def run_test():
        # Mock rate limiter
        mock_rate_limiter = MagicMock()
        wait_calls = []

        def mock_wait(domain):
            wait_calls.append(domain)

        mock_rate_limiter.wait = mock_wait

        client = AsyncHTTPClient(connector_limit=10, rate_limiter=mock_rate_limiter)

        # Don't mock get() - let it run naturally so rate limiter is called
        # Instead, mock the actual HTTP session
        mock_session = AsyncMock()

        async def mock_session_get(url, **kwargs):
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value={"data": "test"})
            mock_response.close = MagicMock()
            return mock_response

        mock_session.get = mock_session_get
        mock_session.closed = False

        # Patch _ensure_session to return our mock
        async def mock_ensure_session():
            return mock_session

        with patch.object(client, "_ensure_session", side_effect=mock_ensure_session):
            # Make multiple requests
            urls = ["https://example.com/1", "https://example.com/2", "https://other.com/3"]

            results = await client.get_many(urls)

            # All should succeed
            assert len(results) == 3
            assert all(r is not None for r in results)

            # Rate limiter should have been called for each request
            assert len(wait_calls) == 3, (
                f"Rate limiter should be called 3 times, got {len(wait_calls)}"
            )

            # Check domains
            assert "example.com" in wait_calls
            assert "other.com" in wait_calls

        await client.close()
        print("✅ Rate limiter integration test passed")

    asyncio.run(run_test())


def test_exponential_backoff_timing():
    """Test that exponential backoff uses correct wait times.

    Requirements: 3.7
    """

    async def run_test():
        client = AsyncHTTPClient(connector_limit=10)

        call_times = []

        async def mock_get_failing(url, **kwargs):
            call_times.append(asyncio.get_event_loop().time())
            raise aiohttp.ClientError("Connection failed")

        with patch.object(client, "get", side_effect=mock_get_failing):
            result = await get_with_retry(client, "https://example.com/test", max_retries=3)

            assert result is None
            assert len(call_times) == 3, "Should have made 3 attempts"

            # Check backoff timing (2^0=1s, 2^1=2s)
            # Allow some tolerance for execution time
            if len(call_times) >= 2:
                delay1 = call_times[1] - call_times[0]
                assert 0.9 <= delay1 <= 1.5, f"First retry should wait ~1s, got {delay1:.2f}s"

            if len(call_times) >= 3:
                delay2 = call_times[2] - call_times[1]
                assert 1.9 <= delay2 <= 2.5, f"Second retry should wait ~2s, got {delay2:.2f}s"

        await client.close()
        print("✅ Exponential backoff timing test passed")

    asyncio.run(run_test())


def test_mixed_success_and_failure():
    """Test handling of mixed successful and failed requests.

    Requirements: 3.2
    """

    async def run_test():
        client = AsyncHTTPClient(connector_limit=10)

        # Mock get to return different results based on URL
        async def mock_get_mixed(url, **kwargs):
            if "fail" in url:
                raise aiohttp.ClientError("Failed")
            elif "timeout" in url:
                raise TimeoutError("Timeout")
            else:
                mock_response = AsyncMock()
                mock_response.status = 200
                mock_response.json = AsyncMock(return_value={"url": url})
                mock_response.close = MagicMock()
                return mock_response

        with patch.object(client, "get", side_effect=mock_get_mixed):
            urls = [
                "https://example.com/success1",
                "https://example.com/fail",
                "https://example.com/success2",
                "https://example.com/timeout",
                "https://example.com/success3",
            ]

            results = await client.get_many(urls)

            # Should have 5 results
            assert len(results) == 5

            # Check which succeeded and which failed
            assert results[0] is not None and results[0]["url"] == urls[0]
            assert results[1] is None  # Failed
            assert results[2] is not None and results[2]["url"] == urls[2]
            assert results[3] is None  # Timeout
            assert results[4] is not None and results[4]["url"] == urls[4]

        await client.close()
        print("✅ Mixed success and failure test passed")

    asyncio.run(run_test())


if __name__ == "__main__":
    print("Running async I/O unit tests...\n")

    test_connection_failure_with_retry()
    test_connection_failure_exhausted_retries()
    test_timeout_handling()
    test_rate_limit_429_handling()
    test_rate_limiter_integration()
    test_exponential_backoff_timing()
    test_mixed_success_and_failure()

    print("\n✅ All async I/O unit tests passed!")
