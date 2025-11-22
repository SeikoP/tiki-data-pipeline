"""Quick test for 3NF pipeline"""
import sys
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

sys.path.insert(0, 'src')

try:
    from pipelines.crawl.storage.postgres_storage import PostgresStorage
    
    logger.info("🔄 Connecting to DB...")
    storage = PostgresStorage(
        host='localhost', port=5432,
        database='crawl_data', user='postgres', password='postgres',
        connect_timeout=10
    )
    
    with storage.get_connection() as conn:
        logger.info("✓ Connected to DB")
        
        with conn.cursor() as cur:
            # Check products table
            cur.execute("SELECT COUNT(*) FROM products")
            total = cur.fetchone()[0]
            logger.info(f"📊 Total products: {total}")
            
            if total == 0:
                logger.warning("⚠️ No products in DB. Run crawl first!")
                sys.exit(1)
            
            # Check brand coverage
            cur.execute("""
                SELECT COUNT(*) FROM products 
                WHERE brand IS NOT NULL AND brand != ''
            """)
            with_brand = cur.fetchone()[0]
            logger.info(f"✓ Products with brand: {with_brand}/{total} ({100*with_brand/total:.1f}%)")
            
            if with_brand < 100:
                logger.warning(f"⚠️ {total - with_brand} products missing brand!")
            
            # Sample data
            cur.execute("""
                SELECT product_id, name, brand 
                FROM products 
                WHERE brand IS NOT NULL 
                LIMIT 3
            """)
            for row in cur.fetchall():
                logger.info(f"  - Product {row[0]}: {row[1]} ({row[2]})")

except Exception as e:
    logger.error(f"❌ Error: {e}", exc_info=True)
    sys.exit(1)

logger.info("\n✅ Test passed! Ready to run cleanup pipeline")
