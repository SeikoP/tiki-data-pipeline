
import os
import sys
import logging

# Add src to path relative to this file so it is independent of the current working directory
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(PROJECT_ROOT, "src"))

from pipelines.crawl.storage.postgres_storage import PostgresStorage

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_category_cleanup():
    logger.info("Starting verification of category cleanup...")
    
    storage = PostgresStorage()
    
    # 1. Check current state
    with storage.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM categories WHERE is_leaf = FALSE")
            count_before = cur.fetchone()[0]
            logger.info(f"Non-leaf categories before cleanup: {count_before}")
            
            # 2. Run cleanup
            logger.info("Running cleanup_redundant_categories()...")
            removed = storage.cleanup_redundant_categories(cur)
            logger.info(f"Removed {removed} categories.")
            
            # 3. Check after state
            cur.execute("SELECT count(*) FROM categories WHERE is_leaf = FALSE")
            count_after = cur.fetchone()[0]
            logger.info(f"Non-leaf categories after cleanup: {count_after}")
            
            if count_after == 0:
                logger.info("SUCCESS: All non-leaf categories were removed.")
            else:
                logger.error(f"FAIL: {count_after} non-leaf categories still remain.")

if __name__ == "__main__":
    verify_category_cleanup()
