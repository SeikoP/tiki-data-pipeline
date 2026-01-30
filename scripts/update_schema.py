import os
import sys

# Add src to path to import PostgresStorage
sys.path.append(os.path.join(os.getcwd(), "src"))

try:
    from pipelines.crawl.storage.postgres_storage import PostgresStorage
except ImportError:
    # Fallback if running from root
    sys.path.append(os.path.join(os.getcwd(), "src"))
    from pipelines.crawl.storage.postgres_storage import PostgresStorage


def update_schema():
    print("Connecting to database...")
    storage = PostgresStorage()

    with storage.get_connection() as conn:
        with conn.cursor() as cur:
            print("Checking/Updating schema for 'products' table...")

            # Check crawl_quality column
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name='products' AND column_name='crawl_quality';
            """)
            if not cur.fetchone():
                print("Adding 'crawl_quality' column...")
                cur.execute(
                    "ALTER TABLE products ADD COLUMN crawl_quality VARCHAR(50) DEFAULT 'basic';"
                )
            else:
                print("'crawl_quality' column already exists.")

            # Check review_count column
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name='products' AND column_name='review_count';
            """)
            if not cur.fetchone():
                print("Adding 'review_count' column...")
                cur.execute("ALTER TABLE products ADD COLUMN review_count INTEGER DEFAULT 0;")
            else:
                print("'review_count' column already exists.")

            # Check needs_enrichment column
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name='products' AND column_name='needs_enrichment';
            """)
            if not cur.fetchone():
                print("Adding 'needs_enrichment' column...")
                cur.execute(
                    "ALTER TABLE products ADD COLUMN needs_enrichment BOOLEAN DEFAULT TRUE;"
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_products_needs_enrichment ON products(needs_enrichment);"
                )
            else:
                print("'needs_enrichment' column already exists.")

    print("Schema update complete.")


if __name__ == "__main__":
    update_schema()
