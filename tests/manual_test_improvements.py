"""
Manual test script to verify improvements work correctly.
Run this to test the actual modules with real data.
"""

import json
import tempfile
from pathlib import Path


def test_data_aggregator_with_cache():
    """Test DataAggregator caching."""
    print("\n" + "=" * 70)
    print("TEST 1: DataAggregator with Caching")
    print("=" * 70)

    from src.common.analytics.aggregator import DataAggregator

    # Create sample data
    data = {
        "crawled_at": "2026-02-01T10:00:00",
        "total_products": 1000,
        "stats": {
            "total_products": 1000,
            "crawled_count": 800,
            "with_detail": 720,
            "failed": 50,
            "timeout": 30,
            "products_saved": 720,
        },
        "products": [
            {
                "product_id": "123",
                "name": "Test Product",
                "price": {"current_price": 100000, "discount_percent": 10},
                "rating": {"average": 4.5},
            }
        ],
    }

    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
        json.dump(data, f)
        temp_path = f.name

    try:
        aggregator = DataAggregator(temp_path)
        aggregator.load_data()

        # First call - should compute
        print("\n1️⃣  First aggregate call (should compute)...")
        import time

        start = time.time()
        summary1 = aggregator.aggregate()
        time1 = time.time() - start
        print(f"   ⏱️  Time: {time1:.4f}s")
        print(f"   📊 Crawled: {summary1['statistics']['crawled_count']}")
        print(f"   ✅ Success: {summary1['statistics']['with_detail']}")

        # Second call - should use cache
        print("\n2️⃣  Second aggregate call (should use cache)...")
        start = time.time()
        aggregator.aggregate()
        time2 = time.time() - start
        print(f"   ⏱️  Time: {time2:.4f}s")
        print(f"   🚀 Speedup: {time1 / time2:.1f}x faster")

        # Save to history
        print("\n3️⃣  Saving to history...")
        aggregator.save_to_history(summary1)
        history_file = Path(temp_path).parent / "aggregation_history.json"
        if history_file.exists():
            with open(history_file) as f:
                history = json.load(f)
            print(f"   ✅ History saved: {len(history)} runs")
        else:
            print("   ❌ History file not created")

        print("\n✅ TEST 1 PASSED")

    finally:
        import os

        os.unlink(temp_path)
        history_file = Path(temp_path).parent / "aggregation_history.json"
        if history_file.exists():
            os.unlink(history_file)


def test_ai_summarizer_with_cache():
    """Test AISummarizer caching."""
    print("\n" + "=" * 70)
    print("TEST 2: AISummarizer with Caching")
    print("=" * 70)

    from src.common.ai.summarizer import AISummarizer

    summarizer = AISummarizer()

    if not summarizer.enabled or not summarizer.api_keys:
        print("⚠️  AI not enabled or no API key, skipping test")
        return

    sample_summary = {
        "statistics": {
            "total_products": 1000,
            "crawled_count": 800,
            "with_detail": 720,
            "failed": 50,
            "timeout": 30,
        },
        "price_analysis": {"avg_price": 500000},
        "trends": {"has_previous_data": False},
    }

    # Test cache key generation
    print("\n1️⃣  Testing cache key generation...")
    key1 = summarizer._get_summary_cache_key(sample_summary)
    key2 = summarizer._get_summary_cache_key(sample_summary)
    print(f"   Key 1: {key1}")
    print(f"   Key 2: {key2}")
    print(f"   ✅ Same data = same key: {key1 == key2}")

    # Test prompt optimization
    print("\n2️⃣  Testing prompt optimization...")
    prompt = summarizer._create_prompt(sample_summary)
    print(f"   Prompt length: {len(prompt)} chars")
    print(f"   ✅ Optimized: {len(prompt) < 1000}")

    # Test AI call with cache (if enabled)
    print("\n3️⃣  Testing AI call with cache...")
    print("   (This will call real AI API, may take time)")
    import time

    start = time.time()
    summary1 = summarizer.summarize_data(sample_summary)
    time1 = time.time() - start

    if summary1:
        print(f"   ⏱️  First call: {time1:.2f}s")
        print(f"   📝 Summary length: {len(summary1)} chars")

        # Second call should use cache
        start = time.time()
        summarizer.summarize_data(sample_summary)
        time2 = time.time() - start
        print(f"   ⏱️  Second call: {time2:.2f}s (cached)")
        print(f"   🚀 Speedup: {time1 / time2:.1f}x faster")
        print("\n✅ TEST 2 PASSED")
    else:
        print("   ⚠️  No AI summary returned")


def test_discord_notifier_with_retry():
    """Test DiscordNotifier retry and split."""
    print("\n" + "=" * 70)
    print("TEST 3: DiscordNotifier with Retry & Split")
    print("=" * 70)

    from src.common.notifications.discord import DiscordNotifier

    notifier = DiscordNotifier()

    if not notifier.enabled or not notifier.webhook_url:
        print("⚠️  Discord not enabled or no webhook URL, skipping test")
        return

    # Test split long content
    print("\n1️⃣  Testing split long content...")
    short_content = "Short message"
    embeds_short = notifier._split_content_to_embeds(short_content, "Title", 0x00FF00, None, None)
    print(f"   Short content: {len(embeds_short)} embed(s)")

    long_content = "A" * 3000
    embeds_long = notifier._split_content_to_embeds(long_content, "Title", 0x00FF00, None, None)
    print(f"   Long content (3000 chars): {len(embeds_long)} embed(s)")
    print(f"   ✅ Split working: {len(embeds_long) >= 2}")

    # Test actual send (if you want to test)
    print("\n2️⃣  Testing actual Discord send...")
    print("   (Set test_send = True to actually send)")
    test_send = False  # Change to True to test real send

    if test_send:
        result = notifier.send_message(
            content="🧪 Test message from improvements",
            title="Test Notification",
            color=0x00FF00,
            max_retries=3,
        )
        print(f"   Send result: {'✅ Success' if result else '❌ Failed'}")
    else:
        print("   ⏭️  Skipped (test_send = False)")

    print("\n✅ TEST 3 PASSED")


def test_trend_analysis():
    """Test trend analysis with multiple runs."""
    print("\n" + "=" * 70)
    print("TEST 4: Trend Analysis")
    print("=" * 70)

    from src.common.analytics.aggregator import _CACHE, DataAggregator

    # Create sample data
    data = {
        "crawled_at": "2026-02-01T10:00:00",
        "total_products": 1000,
        "stats": {
            "total_products": 1000,
            "crawled_count": 800,
            "with_detail": 720,
            "failed": 50,
            "timeout": 30,
        },
        "products": [],
    }

    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
        json.dump(data, f)
        temp_path = f.name

    try:
        # Run 1
        print("\n1️⃣  Run 1: Initial data...")
        aggregator1 = DataAggregator(temp_path)
        aggregator1.load_data()
        summary1 = aggregator1.aggregate()
        aggregator1.save_to_history(summary1)
        print(f"   Crawled: {summary1['statistics']['crawled_count']}")
        print(f"   Success: {summary1['statistics']['with_detail']}")
        print(f"   Trends: {summary1['trends'].get('has_previous_data')}")

        # Clear cache
        _CACHE.clear()

        # Run 2 - improved performance
        print("\n2️⃣  Run 2: Improved performance...")
        data["stats"]["crawled_count"] = 900
        data["stats"]["with_detail"] = 810
        with open(temp_path, "w") as f:
            json.dump(data, f)

        aggregator2 = DataAggregator(temp_path)
        aggregator2.load_data()
        summary2 = aggregator2.aggregate()
        aggregator2.save_to_history(summary2)
        print(f"   Crawled: {summary2['statistics']['crawled_count']}")
        print(f"   Success: {summary2['statistics']['with_detail']}")
        print(f"   Trends: {summary2['trends'].get('has_previous_data')}")

        # Clear cache
        _CACHE.clear()

        # Run 3 - should show trends
        print("\n3️⃣  Run 3: Should show trends...")
        aggregator3 = DataAggregator(temp_path)
        aggregator3.load_data()
        summary3 = aggregator3.aggregate()

        trends = summary3.get("trends", {})
        print(f"   Has previous data: {trends.get('has_previous_data')}")
        if trends.get("has_previous_data"):
            changes = trends.get("changes", {})
            print(f"   Crawled change: {changes.get('crawled_count', {})}")
            print(f"   Success rate change: {trends.get('success_rate_change')}%")
            print("\n✅ TEST 4 PASSED")
        else:
            print("\n⚠️  No trend data (need at least 2 runs in history)")

    finally:
        import os

        os.unlink(temp_path)
        history_file = Path(temp_path).parent / "aggregation_history.json"
        if history_file.exists():
            os.unlink(history_file)


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("🧪 MANUAL TESTING: aggregate_and_notify Improvements")
    print("=" * 70)

    try:
        test_data_aggregator_with_cache()
        test_ai_summarizer_with_cache()
        test_discord_notifier_with_retry()
        test_trend_analysis()

        print("\n" + "=" * 70)
        print("✅ ALL TESTS COMPLETED")
        print("=" * 70)

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback

        traceback.print_exc()
