"""
Airflow entrypoint for the refactored Tiki crawl products DAG.
"""

from __future__ import annotations

# Importing dag_definition constructs the DAG at import time (same as legacy file).
from .dag_definition import dag  # noqa: F401
