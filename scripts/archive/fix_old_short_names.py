
import logging
import sys
from pathlib import Path

# Add project root to sys.path
# Script is in scripts/ -> parent is project root
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / ".env")

from src.pipelines.crawl.storage.postgres_storage import PostgresStorage
from src.pipelines.transform.transformer import DataTransformer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("fix_short_names")

def fix_old_short_names(batch_size=500, use_ai=False, db_host="localhost"):
    """
    Update short_name for all existing products in the database using the new logic.
    """
    # Use specified host (default localhost for local run) to avoid resolving 'postgres' container name
    storage = PostgresStorage(host=db_host)
    transformer = DataTransformer()
    
    # Disable AI if not explicitly requested to avoid cost/rate limits during mass fix
    if not use_ai:
        transformer.ai_summarizer = None
        logger.info("🤖 AI shortening disabled for this run (using heuristics only)")
    else:
        logger.info("🤖 AI shortening enabled (may be slow due to rate limits)")

    with storage.get_connection() as conn:
        with conn.cursor() as cur:
            # 1. Get total count
            cur.execute("SELECT COUNT(*) FROM products")
            total = cur.fetchone()[0]
            logger.info(f"📊 Total products to process: {total}")
            
            if total == 0:
                logger.info("✅ No products found in database.")
                return

            # 2. Fetch products in batches
            offset = 0
            updated_total = 0
            
            while offset < total:
                cur.execute(
                    "SELECT product_id, name FROM products ORDER BY product_id LIMIT %s OFFSET %s",
                    (batch_size, offset)
                )
                rows = cur.fetchall()
                if not rows:
                    break
                
                updates = []
                for pid, name in rows:
                    new_short_name = transformer._get_short_name(name)
                    if new_short_name:
                        updates.append((new_short_name, pid))
                
                # 3. Batch Update
                if updates:
                    cur.executemany(
                        "UPDATE products SET short_name = %s WHERE product_id = %s",
                        updates
                    )
                    conn.commit()
                    updated_total += len(updates)
                    logger.info(f"✅ Updated {updated_total}/{total} products...")
                
                offset += batch_size

    logger.info(f"✨ Successfully updated {updated_total} products with new short_name logic.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fix old short_names in project database")
    parser.add_argument("--batch", type=int, default=500, help="Batch size for updates")
    parser.add_argument("--ai", action="store_true", help="Enable AI shortening (slow)")
    parser.add_argument("--host", default="localhost", help="Database host (default: localhost, use 'postgres' if inside docker)")
    
    args = parser.parse_args()
    
    try:
        fix_old_short_names(batch_size=args.batch, use_ai=args.ai, db_host=args.host)
    except Exception as e:
        logger.error(f"❌ Error during fix: {e}", exc_info=True)
        sys.exit(1)
