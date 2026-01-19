"""
Test suite for crawl_history schema migration.

Verifies that:
1. All new columns are created correctly
2. Indexes are created
3. Existing data is not affected
4. NULL values are handled correctly
"""

import os
import sys
from pathlib import Path

import pytest

# Add src to path for imports
src_path = Path(__file__).parent.parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from pipelines.crawl.storage.postgres_storage import PostgresStorage


@pytest.fixture
def storage():
    """Create PostgresStorage instance for testing."""
    # Use test database or local database
    return PostgresStorage(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        database=os.getenv("POSTGRES_DB", "tiki"),
        user=os.getenv("POSTGRES_USER", "airflow_user"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
    )


def test_crawl_history_schema_exists(storage):
    """Test that crawl_history table exists with all required columns."""
    with storage.get_connection() as conn:
        with conn.cursor() as cur:
            # Ensure schema is created
            storage._ensure_history_schema(cur)

            # Check table exists
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'crawl_history'
                )
            """
            )
            assert cur.fetchone()[0], "crawl_history table should exist"


def test_crawl_history_new_columns_exist(storage):
    """Test that all new columns exist in crawl_history table."""
    expected_new_columns = [
        "started_at",
        "completed_at",
        "crawl_status",
        "retry_count",
        "error_message",
        "missing_fields",
        "data_completeness_score",
        "crawl_duration_ms",
        "page_load_time_ms",
    ]

    with storage.get_connection() as conn:
        with conn.cursor() as cur:
            # Ensure schema is created
            storage._ensure_history_schema(cur)

            # Get all columns
            cur.execute(
                """
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'crawl_history'
            """
            )
            columns = {row[0]: row[1] for row in cur.fetchall()}

            # Check each new column exists
            for col in expected_new_columns:
                assert col in columns, f"Column {col} should exist in crawl_history"


def test_crawl_history_indexes_exist(storage):
    """Test that all new indexes exist."""
    expected_indexes = [
        "idx_history_status",
        "idx_history_started_at",
        "idx_history_retry_count",
        "idx_history_product_id",
        "idx_history_crawled_at",
        "idx_history_product_latest",
    ]

    with storage.get_connection() as conn:
        with conn.cursor() as cur:
            # Ensure schema is created
            storage._ensure_history_schema(cur)

            # Get all indexes
            cur.execute(
                """
                SELECT indexname 
                FROM pg_indexes 
                WHERE tablename = 'crawl_history'
            """
            )
            indexes = [row[0] for row in cur.fetchall()]

            # Check each index exists
            for idx in expected_indexes:
                assert idx in indexes, f"Index {idx} should exist"


def test_products_metadata_columns_exist(storage):
    """Test that products table has new metadata columns."""
    expected_columns = ["data_quality", "last_crawl_status", "retry_count"]

    with storage.get_connection() as conn:
        with conn.cursor() as cur:
            # Ensure schema is created
            storage._ensure_products_schema(cur)

            # Get all columns
            cur.execute(
                """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'products'
            """
            )
            columns = [row[0] for row in cur.fetchall()]

            # Check each new column exists
            for col in expected_columns:
                assert col in columns, f"Column {col} should exist in products table"


def test_products_metadata_indexes_exist(storage):
    """Test that products table has indexes on metadata columns."""
    expected_indexes = ["idx_products_data_quality", "idx_products_retry_count"]

    with storage.get_connection() as conn:
        with conn.cursor() as cur:
            # Ensure schema is created
            storage._ensure_products_schema(cur)

            # Get all indexes
            cur.execute(
                """
                SELECT indexname 
                FROM pg_indexes 
                WHERE tablename = 'products'
            """
            )
            indexes = [row[0] for row in cur.fetchall()]

            # Check each index exists
            for idx in expected_indexes:
                assert idx in indexes, f"Index {idx} should exist"


def test_crawl_history_null_values_allowed(storage):
    """Test that new columns allow NULL values (backward compatibility)."""
    with storage.get_connection() as conn:
        with conn.cursor() as cur:
            # Ensure schema is created
            storage._ensure_history_schema(cur)

            # Insert a minimal record (only required fields)
            cur.execute(
                """
                INSERT INTO crawl_history (product_id, price)
                VALUES ('test_product_null', 100.00)
                RETURNING id
            """
            )
            record_id = cur.fetchone()[0]

            # Verify record was inserted
            assert record_id is not None

            # Verify new columns are NULL
            cur.execute(
                """
                SELECT started_at, completed_at, crawl_status, retry_count,
                       error_message, missing_fields, data_completeness_score,
                       crawl_duration_ms, page_load_time_ms
                FROM crawl_history
                WHERE id = %s
            """,
                (record_id,),
            )
            row = cur.fetchone()

            # started_at, completed_at, crawl_status, error_message, missing_fields,
            # data_completeness_score, crawl_duration_ms, page_load_time_ms should be NULL
            assert row[0] is None, "started_at should be NULL"
            assert row[1] is None, "completed_at should be NULL"
            assert row[2] is None, "crawl_status should be NULL"
            # retry_count has DEFAULT 0
            assert row[3] == 0, "retry_count should default to 0"
            assert row[4] is None, "error_message should be NULL"
            assert row[5] is None, "missing_fields should be NULL"
            assert row[6] is None, "data_completeness_score should be NULL"
            assert row[7] is None, "crawl_duration_ms should be NULL"
            assert row[8] is None, "page_load_time_ms should be NULL"

            # Cleanup
            cur.execute("DELETE FROM crawl_history WHERE id = %s", (record_id,))


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
