"""Shared Airflow assets for cross-DAG data-aware scheduling."""

from __future__ import annotations

from dataclasses import dataclass

TIKI_CORE_TABLES_ASSET_URI = "postgres://postgres:5432/tiki/public/core_product_tables"

try:
    from airflow.sdk import Asset
except Exception:
    try:
        from airflow.datasets import Dataset as Asset
    except Exception:

        @dataclass(frozen=True)
        class Asset:
            uri: str


TIKI_CORE_TABLES_ASSET = Asset(TIKI_CORE_TABLES_ASSET_URI)

__all__ = ["TIKI_CORE_TABLES_ASSET", "TIKI_CORE_TABLES_ASSET_URI"]
