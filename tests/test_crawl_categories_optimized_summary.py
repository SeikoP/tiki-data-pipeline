"""Tests for the optimized category crawler run summary."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipelines.crawl import crawl_categories_optimized as crawler


def test_main_returns_run_summary_without_category_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fake_recursive_crawl(*_args, **_kwargs) -> list[dict[str, object]]:
        crawler.stats["total_crawled"] = 2
        crawler.stats["total_success"] = 1
        crawler.stats["total_failed"] = 1
        crawler.stats["total_categories"] = 1
        return [
            {
                "name": "Category",
                "url": "https://tiki.vn/category/c1",
                "level": 0,
            }
        ]

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(crawler, "crawl_category_recursive_optimized", fake_recursive_crawl)
    output = tmp_path / "data" / "raw" / "categories_recursive_optimized.json"
    output.parent.mkdir(parents=True)

    summary = crawler.main()

    assert summary == {
        "total_success": 1,
        "total_failed": 1,
        "total_categories": 1,
        "output_file": "data/raw/categories_recursive_optimized.json",
    }
    assert json.loads(output.read_text(encoding="utf-8")) == [
        {
            "name": "Category",
            "url": "https://tiki.vn/category/c1",
            "level": 0,
        }
    ]
