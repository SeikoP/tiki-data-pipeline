"""
Tests for aggregate_and_notify task and supporting modules.
"""

import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pytest


class TestDataAggregator:
    """Test DataAggregator with caching and trend analysis."""

    @pytest.fixture
    def sample_data_file(self):
        """Create a temporary data file for testing."""
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

        yield temp_path

        # Cleanup
        os.unlink(temp_path)
        # Cleanup history file if exists
        history_file = Path(temp_path).parent / "aggregation_history.json"
        if history_file.exists():
            os.unlink(history_file)

    def test_load_data(self, sample_data_file):
        """Test loading data from file."""
        from src.common.analytics.aggregator import DataAggregator

        aggregator = DataAggregator(sample_data_file)
        assert aggregator.load_data() is True
        assert aggregator.data["total_products"] == 1000

    def test_aggregate_basic(self, sample_data_file):
        """Test basic aggregation."""
        from src.common.analytics.aggregator import DataAggregator

        aggregator = DataAggregator(sample_data_file)
        aggregator.load_data()
        summary = aggregator.aggregate()

        assert summary["statistics"]["crawled_count"] == 800
        assert summary["statistics"]["with_detail"] == 720
        assert summary["statistics"]["failed"] == 50
        assert summary["statistics"]["timeout"] == 30

    def test_aggregate_caching(self, sample_data_file):
        """Test that aggregation results are cached."""
        from src.common.analytics.aggregator import DataAggregator

        aggregator = DataAggregator(sample_data_file)
        aggregator.load_data()

        # First call
        summary1 = aggregator.aggregate()

        # Second call should use cache
        with patch.object(aggregator, "_analyze_products") as mock_analyze:
            summary2 = aggregator.aggregate()
            # Should not call _analyze_products again (cached)
            mock_analyze.assert_not_called()

        assert summary1 == summary2

    def test_save_to_history(self, sample_data_file):
        """Test saving aggregation to history."""
        from src.common.analytics.aggregator import DataAggregator

        aggregator = DataAggregator(sample_data_file)
        aggregator.load_data()
        summary = aggregator.aggregate()

        aggregator.save_to_history(summary)

        history_file = Path(sample_data_file).parent / "aggregation_history.json"
        assert history_file.exists()

        with open(history_file) as f:
            history = json.load(f)
            assert len(history) == 1
            assert history[0]["statistics"]["crawled_count"] == 800

    def test_trend_analysis(self, sample_data_file):
        """Test trend analysis with previous data."""
        from src.common.analytics.aggregator import DataAggregator

        aggregator = DataAggregator(sample_data_file)
        aggregator.load_data()

        # First run
        summary1 = aggregator.aggregate()
        aggregator.save_to_history(summary1)

        # Clear cache to force re-aggregate
        from src.common.analytics.aggregator import _CACHE

        _CACHE.clear()

        # Modify data for second run
        with open(sample_data_file) as f:
            data = json.load(f)
        data["stats"]["crawled_count"] = 900
        data["stats"]["with_detail"] = 810
        with open(sample_data_file, "w") as f:
            json.dump(data, f)

        # Second run
        aggregator2 = DataAggregator(sample_data_file)
        aggregator2.load_data()
        summary2 = aggregator2.aggregate()
        aggregator2.save_to_history(summary2)

        # Clear cache again
        _CACHE.clear()

        # Third run should have trend data (comparing run 2 vs run 1)
        aggregator3 = DataAggregator(sample_data_file)
        aggregator3.load_data()
        summary3 = aggregator3.aggregate()

        trends = summary3.get("trends", {})
        # Should have previous data now (2 runs in history)
        assert trends.get("has_previous_data") is True
        assert "changes" in trends
        assert "crawled_count" in trends["changes"]


class TestAISummarizer:
    """Test AISummarizer with caching."""

    @pytest.fixture
    def sample_summary(self):
        """Sample data summary for testing."""
        return {
            "statistics": {
                "total_products": 1000,
                "crawled_count": 800,
                "with_detail": 720,
                "failed": 50,
                "timeout": 30,
            },
            "price_analysis": {"avg_price": 500000, "min_price": 10000, "max_price": 5000000},
            "trends": {"has_previous_data": True, "success_rate_change": 2.5},
        }

    def test_cache_key_generation(self, sample_summary):
        """Test cache key generation."""
        from src.common.ai.summarizer import AISummarizer

        summarizer = AISummarizer()
        key1 = summarizer._get_summary_cache_key(sample_summary)
        key2 = summarizer._get_summary_cache_key(sample_summary)

        assert key1 == key2  # Same data = same key

        # Different data = different key
        sample_summary["statistics"]["crawled_count"] = 900
        key3 = summarizer._get_summary_cache_key(sample_summary)
        assert key1 != key3

    @patch("src.common.ai.summarizer.requests.post")
    def test_summarize_with_cache(self, mock_post, sample_summary):
        """Test that AI summary is cached."""
        from src.common.ai.summarizer import AISummarizer

        # Mock AI response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"choices": [{"message": {"content": "Test AI summary"}}]}
        mock_post.return_value = mock_response

        summarizer = AISummarizer()
        summarizer.enabled = True
        summarizer.api_keys = ["test_key"]

        # First call - should call API
        summary1 = summarizer.summarize_data(sample_summary)
        assert summary1 == "Test AI summary"
        assert mock_post.call_count == 1

        # Second call - should use cache
        summary2 = summarizer.summarize_data(sample_summary)
        assert summary2 == "Test AI summary"
        assert mock_post.call_count == 1  # Not called again

    def test_prompt_optimization(self, sample_summary):
        """Test that prompt is shorter than before."""
        from src.common.ai.summarizer import AISummarizer

        summarizer = AISummarizer()
        prompt = summarizer._create_prompt(sample_summary)

        # Optimized prompt should be < 1000 chars
        assert len(prompt) < 1000
        # Should contain key info
        assert "800" in prompt  # crawled_count
        assert "720" in prompt  # with_detail


class TestDiscordNotifier:
    """Test DiscordNotifier with retry and fallback."""

    @patch("src.common.notifications.discord.requests.post")
    def test_send_message_success(self, mock_post):
        """Test successful message send."""
        from src.common.notifications.discord import DiscordNotifier

        mock_response = Mock()
        mock_response.status_code = 204
        mock_post.return_value = mock_response

        notifier = DiscordNotifier()
        notifier.enabled = True
        notifier.webhook_url = "https://discord.com/api/webhooks/test"

        result = notifier.send_message("Test message")
        assert result is True
        assert mock_post.call_count == 1

    @patch("src.common.notifications.discord.requests.post")
    @patch("src.common.notifications.discord.time.sleep")
    def test_send_message_retry(self, mock_sleep, mock_post):
        """Test retry on failure."""
        from src.common.notifications.discord import DiscordNotifier

        # First 2 calls fail, 3rd succeeds
        mock_response_fail = Mock()
        mock_response_fail.status_code = 500
        mock_response_fail.raise_for_status.side_effect = Exception("Server error")

        mock_response_success = Mock()
        mock_response_success.status_code = 204

        mock_post.side_effect = [
            mock_response_fail,
            mock_response_fail,
            mock_response_success,
        ]

        notifier = DiscordNotifier()
        notifier.enabled = True
        notifier.webhook_url = "https://discord.com/api/webhooks/test"

        result = notifier.send_message("Test message", max_retries=3)
        assert result is True
        assert mock_post.call_count == 3
        assert mock_sleep.call_count == 2  # Sleep between retries

    @patch("src.common.notifications.discord.requests.post")
    def test_send_message_fallback(self, mock_post):
        """Test fallback webhook on primary failure."""
        from src.common.notifications.discord import DiscordNotifier

        # Primary fails, fallback succeeds
        mock_response_fail = Mock()
        mock_response_fail.status_code = 500
        mock_response_fail.raise_for_status.side_effect = Exception("Server error")

        mock_response_success = Mock()
        mock_response_success.status_code = 204

        mock_post.side_effect = [
            mock_response_fail,
            mock_response_fail,
            mock_response_fail,  # Primary fails 3 times
            mock_response_success,  # Fallback succeeds
        ]

        notifier = DiscordNotifier()
        notifier.enabled = True
        notifier.webhook_url = "https://discord.com/api/webhooks/primary"
        notifier.fallback_webhook_url = "https://discord.com/api/webhooks/fallback"

        result = notifier.send_message("Test message", max_retries=3)
        assert result is True
        assert mock_post.call_count == 4  # 3 primary + 1 fallback

    def test_split_long_content(self):
        """Test splitting long content into multiple embeds."""
        from src.common.notifications.discord import DiscordNotifier

        notifier = DiscordNotifier()

        # Short content - single embed
        short_content = "Short message"
        embeds = notifier._split_content_to_embeds(short_content, "Title", 0x00FF00, None, None)
        assert len(embeds) == 1

        # Long content - multiple embeds
        long_content = "A" * 3000  # 3000 chars
        embeds = notifier._split_content_to_embeds(long_content, "Title", 0x00FF00, None, None)
        assert len(embeds) >= 2


class TestDiscordReportFormatting:
    """Test deterministic Discord report formatting."""

    @pytest.fixture
    def sample_summary(self):
        return {
            "statistics": {
                "total_products": 200,
                "crawled_count": 200,
                "with_detail": 190,
                "failed": 6,
                "timeout": 4,
                "cached": 30,
                "products_saved": 190,
                "products_skipped": 10,
            },
            "price_analysis": {
                "avg_price": 150000,
                "min_price": 10000,
                "max_price": 990000,
                "avg_discount": 12.5,
                "max_discount": 40,
                "total_products_with_price": 190,
            },
            "rating_analysis": {
                "avg_rating": 4.6,
                "total_products_with_rating": 120,
            },
            "category_analysis": {"total_categories": 5},
            "seller_analysis": {"total_sellers": 80},
            "issues": ["Có 6 sản phẩm crawl thất bại"],
            "trends": {
                "has_previous_data": True,
                "success_rate_change": 1.5,
                "changes": {
                    "crawled_count": {
                        "current": 200,
                        "previous": 180,
                        "diff": 20,
                        "pct_change": 11.1,
                    }
                },
            },
        }

    def test_notification_fields_are_grouped_for_readability(self, sample_summary):
        from airflow.dags.tiki_crawl_products_v2.tasks.monitoring import (
            _build_notification_fields,
        )

        fields = _build_notification_fields(sample_summary)

        field_names = [field["name"] for field in fields]
        assert "Pipeline" in field_names
        assert "Detail result" in field_names
        assert "Coverage" in field_names
        assert "Operational notes" in field_names
        assert "Trend vs previous run" in field_names
        assert "Saved DB: **190**" in fields[0]["value"]

    def test_detail_report_includes_data_quality_sections(self, sample_summary):
        from airflow.dags.tiki_crawl_products_v2.tasks.monitoring import _build_detail_report

        report = _build_detail_report(sample_summary, ai_summary="AI note")

        assert "**Tổng quan phiên crawl**" in report
        assert "**Giá và giảm giá**" in report
        assert "**Độ phủ dữ liệu**" in report
        assert "**Vấn đề cần chú ý**" in report
        assert "**Nhận định AI**" in report
        assert "AI note" in report


class TestMonitoringTask:
    """Test aggregate_and_notify task integration."""

    @pytest.fixture
    def mock_context(self):
        """Mock Airflow context."""
        ti = Mock()
        ti.xcom_pull.return_value = None

        dag_run = Mock()
        dag_run.start_date = datetime(2026, 2, 1, 10, 0, 0)

        return {"ti": ti, "dag_run": dag_run}

    @pytest.fixture
    def sample_output_file(self):
        """Create sample output file."""
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
            "products": [],
        }

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
            json.dump(data, f)
            temp_path = f.name

        yield temp_path
        os.unlink(temp_path)

    @pytest.mark.skipif(
        os.name == "nt", reason="Airflow not fully supported on Windows, skip integration test"
    )
    @patch("airflow.dags.tiki_crawl_products_v2.tasks.monitoring.OUTPUT_FILE_WITH_DETAIL")
    @patch("airflow.dags.tiki_crawl_products_v2.tasks.monitoring.get_DataAggregator")
    @patch("airflow.dags.tiki_crawl_products_v2.tasks.monitoring.get_AISummarizer")
    @patch("airflow.dags.tiki_crawl_products_v2.tasks.monitoring.get_DiscordNotifier")
    def test_aggregate_and_notify_success(
        self,
        mock_discord,
        mock_ai,
        mock_aggregator,
        mock_output_file,
        mock_context,
        sample_output_file,
    ):
        """Test successful aggregate_and_notify execution."""
        from airflow.dags.tiki_crawl_products_v2.tasks.monitoring import aggregate_and_notify

        # Mock output file path
        mock_output_file.__str__ = Mock(return_value=sample_output_file)

        # Mock DataAggregator
        mock_agg_instance = Mock()
        mock_agg_instance.load_data.return_value = True
        mock_agg_instance.aggregate.return_value = {
            "statistics": {
                "crawled_count": 800,
                "with_detail": 720,
                "failed": 50,
                "timeout": 30,
            },
            "metadata": {"crawled_at": "2026-02-01T10:00:00"},
            "trends": {"has_previous_data": False},
        }
        mock_aggregator.return_value = lambda x: mock_agg_instance

        # Mock AISummarizer
        mock_ai_instance = Mock()
        mock_ai_instance.summarize_data.return_value = "AI summary test"
        mock_ai.return_value = lambda: mock_ai_instance

        # Mock DiscordNotifier
        mock_discord_instance = Mock()
        mock_discord_instance.send_message.return_value = True
        mock_discord.return_value = lambda: mock_discord_instance

        # Execute
        result = aggregate_and_notify(**mock_context)

        # Verify
        assert result["aggregation_success"] is True
        assert result["ai_summary_success"] is True
        assert result["discord_notification_success"] is True
        assert "performance" in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
