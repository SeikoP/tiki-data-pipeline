#!/usr/bin/env python3
"""
Xây dựng warehouse từ raw data
"""
import argparse
import hashlib
import json
import logging
from datetime import datetime

import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class StarSchemaBuilderV2:
    """Xây dựng Star Schema từ crawl_data với error handling tốt"""

    def __init__(
        self,
        db_host: str = "localhost",
        db_port: int = 5432,
        source_db: str = "crawl_data",
        target_db: str = "tiki_warehouse",
        db_user: str = "postgres",
        db_password: str = "postgres",
    ):
        self.db_host = db_host
        self.db_port = db_port
        self.source_db = source_db
        self.target_db = target_db
        self.db_user = db_user
        self.db_password = db_password

        self.source_conn = None
        self.target_conn = None
        self.source_cur = None
        self.target_cur = None

    def connect(self):
        """Connect to databases"""
        try:
            self.source_conn = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                database=self.source_db,
                user=self.db_user,
                password=self.db_password,
            )
            self.source_cur = self.source_conn.cursor()
            logger.info(f"✓ Connected to source: {self.source_db}")

            try:
                self.target_conn = psycopg2.connect(
                    host=self.db_host,
                    port=self.db_port,
                    database=self.target_db,
                    user=self.db_user,
                    password=self.db_password,
                )
                self.target_cur = self.target_conn.cursor()
            except psycopg2.OperationalError:
                logger.info(f"  Creating database: {self.target_db}...")
                temp_conn = psycopg2.connect(
                    host=self.db_host,
                    port=self.db_port,
                    database="postgres",
                    user=self.db_user,
                    password=self.db_password,
                )
                temp_conn.autocommit = True
                temp_cur = temp_conn.cursor()
                temp_cur.execute(f"DROP DATABASE IF EXISTS {self.target_db}")
                temp_cur.execute(f"CREATE DATABASE {self.target_db}")
                temp_cur.close()
                temp_conn.close()

                self.target_conn = psycopg2.connect(
                    host=self.db_host,
                    port=self.db_port,
                    database=self.target_db,
                    user=self.db_user,
                    password=self.db_password,
                )
                self.target_cur = self.target_conn.cursor()

            logger.info(f"✓ Connected to target: {self.target_db}")

        except Exception as e:
            logger.error(f"Connection failed: {e}")
            raise

    def close(self):
        """Close connections"""
        for cur in [self.source_cur, self.target_cur]:
            if cur:
                cur.close()
        for conn in [self.source_conn, self.target_conn]:
            if conn:
                conn.close()
        logger.info("✓ Closed connections")

    def create_schema(self):
        """Create warehouse schema"""
        logger.info("\n" + "=" * 70)
        logger.info("CREATE WAREHOUSE SCHEMA")
        logger.info("=" * 70)

        tables = [
            "fact_product_sales",
            "dim_price_segment",
            "dim_date",
            "dim_brand",
            "dim_seller",
            "dim_category",
            "dim_product",
        ]

        for table in tables:
            try:
                self.target_cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
            except Exception as e:
                logger.warning(f"  Warning dropping {table}: {e}")

        self.target_conn.commit()

        # Create tables
        self.target_cur.execute(
            """
            CREATE TABLE dim_product (
                product_sk SERIAL PRIMARY KEY,
                product_id VARCHAR(50) UNIQUE,
                product_name VARCHAR(500),
                brand VARCHAR(255),
                url VARCHAR(500),
                created_at TIMESTAMP
            )
        """
        )
        logger.info("✓ Created dim_product")

        self.target_cur.execute(
            """
            CREATE TABLE dim_category (
                category_sk SERIAL PRIMARY KEY,
                category_id VARCHAR(50) UNIQUE,
                level_1 VARCHAR(255),
                level_2 VARCHAR(255),
                level_3 VARCHAR(255),
                level_4 VARCHAR(255),
                level_5 VARCHAR(255),
                full_path VARCHAR(1000)
            )
        """
        )
        logger.info("✓ Created dim_category")

        self.target_cur.execute(
            """
            CREATE TABLE dim_seller (
                seller_sk SERIAL PRIMARY KEY,
                seller_id VARCHAR(50) UNIQUE,
                seller_name VARCHAR(500)
            )
        """
        )
        logger.info("✓ Created dim_seller")

        self.target_cur.execute(
            """
            CREATE TABLE dim_brand (
                brand_sk SERIAL PRIMARY KEY,
                brand_name VARCHAR(255) UNIQUE
            )
        """
        )
        logger.info("✓ Created dim_brand")

        self.target_cur.execute(
            """
            CREATE TABLE dim_date (
                date_sk SERIAL PRIMARY KEY,
                date_value DATE UNIQUE,
                year INT,
                month INT,
                day INT
            )
        """
        )
        logger.info("✓ Created dim_date")

        self.target_cur.execute(
            """
            CREATE TABLE dim_price_segment (
                price_segment_sk SERIAL PRIMARY KEY,
                segment_name VARCHAR(100) UNIQUE,
                min_price NUMERIC,
                max_price NUMERIC
            )
        """
        )
        logger.info("✓ Created dim_price_segment")

        self.target_cur.execute(
            """
            CREATE TABLE fact_product_sales (
                fact_id SERIAL PRIMARY KEY,
                product_sk INT REFERENCES dim_product(product_sk),
                category_sk INT REFERENCES dim_category(category_sk),
                seller_sk INT REFERENCES dim_seller(seller_sk),
                brand_sk INT REFERENCES dim_brand(brand_sk),
                date_sk INT REFERENCES dim_date(date_sk),
                price_segment_sk INT REFERENCES dim_price_segment(price_segment_sk),
                price NUMERIC(12, 2),
                original_price NUMERIC(12, 2),
                discount_percent NUMERIC(5, 2),
                quantity_sold INT,
                estimated_revenue NUMERIC(15, 2),
                estimated_profit NUMERIC(15, 2),
                average_rating NUMERIC(3, 1),
                rating_count INT,
                review_count INT
            )
        """
        )
        logger.info("✓ Created fact_product_sales")

        # Create indexes
        self.target_cur.execute(
            "CREATE INDEX idx_fact_product_sk ON fact_product_sales(product_sk)"
        )
        self.target_cur.execute(
            "CREATE INDEX idx_fact_category_sk ON fact_product_sales(category_sk)"
        )
        self.target_cur.execute("CREATE INDEX idx_fact_seller_sk ON fact_product_sales(seller_sk)")
        logger.info("✓ Created indexes")

        self.target_conn.commit()

    def load_price_segments(self):
        """Load price segments"""
        logger.info("\n" + "=" * 70)
        logger.info("LOAD PRICE SEGMENTS")
        logger.info("=" * 70)

        segments = [
            ("Chưa cập nhật", None, None),
            ("Rẻ (< 100K)", 0, 100000),
            ("Bình dân (100K-500K)", 100000, 500000),
            ("Trung bình (500K-1M)", 500000, 1000000),
            ("Cao (1M-5M)", 1000000, 5000000),
            ("Cao cấp (> 5M)", 5000000, None),
        ]

        for name, min_p, max_p in segments:
            self.target_cur.execute(
                """
                INSERT INTO dim_price_segment (segment_name, min_price, max_price)
                VALUES (%s, %s, %s)
            """,
                (name, min_p, max_p),
            )

        self.target_conn.commit()
        logger.info(f"✓ Loaded {len(segments)} price segments")

    def extract_products(self) -> list[dict]:
        """Extract products from crawl_data"""
        logger.info("\n" + "=" * 70)
        logger.info("EXTRACT PRODUCTS FROM CRAWL_DATA")
        logger.info("=" * 70)

        self.source_cur.execute(
            """
            SELECT product_id, name, brand, price, original_price, discount_percent,
                   url, category_path, rating_average, review_count, sales_count,
                   seller_name, seller_id, crawled_at
            FROM products ORDER BY product_id
        """
        )

        products = []
        for row in self.source_cur.fetchall():
            try:
                product = {
                    "product_id": row[0],
                    "name": row[1],
                    "brand": row[2],
                    "price": row[3],
                    "original_price": row[4],
                    "discount_percent": row[5],
                    "url": row[6],
                    "category_path": row[7] if isinstance(row[7], list) else [],
                    "rating": row[8] or 0,
                    "review_count": row[9] or 0,
                    "sales_count": row[10] or 0,
                    "seller_name": row[11] or "Unknown",
                    "seller_id": row[12],
                    "crawled_at": row[13],
                }
                products.append(product)
            except Exception as e:
                logger.warning(f"  Error parsing product row: {e}")
                continue

        logger.info(f"✓ Extracted {len(products)} products")
        return products

    def load_data(self, products: list[dict]):
        """Load all dimensions and facts"""
        logger.info("\n" + "=" * 70)
        logger.info("LOAD DIMENSIONS AND FACTS")
        logger.info("=" * 70)

        # Filter: Remove products with NULL brand or seller
        filtered_products = [p for p in products if p.get("brand") and p.get("seller_name")]
        logger.info(
            f"  Filtering: {len(products)} → {len(filtered_products)} products (removed {len(products) - len(filtered_products)} with NULL brand/seller)"
        )

        # Caches
        prod_cache = {}
        cat_cache = {}
        seller_cache = {}
        brand_cache = {}
        date_cache = {}

        # Get price segments
        self.target_cur.execute("SELECT price_segment_sk, segment_name FROM dim_price_segment")
        price_segments = {row[1]: row[0] for row in self.target_cur.fetchall()}

        fact_count = 0

        for idx, prod in enumerate(filtered_products, 1):
            try:
                pid = prod["product_id"]

                # 1. Product
                if pid not in prod_cache:
                    self.target_cur.execute(
                        """
                        INSERT INTO dim_product (product_id, product_name, brand, url)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (product_id) DO UPDATE
                        SET product_name = EXCLUDED.product_name
                        RETURNING product_sk
                    """,
                        (
                            pid,
                            prod["name"][:500],
                            (prod.get("brand") or "Unknown")[:255],
                            prod.get("url", "")[:500],
                        ),
                    )

                    result = self.target_cur.fetchone()
                    prod_cache[pid] = result[0]

                prod_sk = prod_cache[pid]

                # 2. Category
                cat_path = prod.get("category_path") or []
                valid_cats = [c for c in (cat_path[:4] if isinstance(cat_path, list) else []) if c]
                cat_key = json.dumps(valid_cats, ensure_ascii=False)

                if cat_key not in cat_cache:
                    cat_id = hashlib.md5(cat_key.encode()).hexdigest()[:16]
                    levels = valid_cats + [None] * (5 - len(valid_cats))

                    self.target_cur.execute(
                        """
                        INSERT INTO dim_category (category_id, level_1, level_2, level_3, level_4, level_5, full_path)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (category_id) DO UPDATE
                        SET level_1 = EXCLUDED.level_1
                        RETURNING category_sk
                    """,
                        (
                            cat_id,
                            levels[0],
                            levels[1],
                            levels[2],
                            levels[3],
                            levels[4],
                            " > ".join(valid_cats)[:1000],
                        ),
                    )

                    result = self.target_cur.fetchone()
                    cat_cache[cat_key] = result[0]

                cat_sk = cat_cache[cat_key]

                # 3. Seller
                seller_name = prod.get("seller_name", "Unknown")[:500]
                seller_id = hashlib.md5(seller_name.encode()).hexdigest()[:16]

                if seller_id not in seller_cache:
                    self.target_cur.execute(
                        """
                        INSERT INTO dim_seller (seller_id, seller_name)
                        VALUES (%s, %s)
                        ON CONFLICT (seller_id) DO UPDATE
                        SET seller_name = EXCLUDED.seller_name
                        RETURNING seller_sk
                    """,
                        (seller_id, seller_name),
                    )

                    result = self.target_cur.fetchone()
                    seller_cache[seller_id] = result[0]

                seller_sk = seller_cache[seller_id]

                # 4. Brand
                brand = (prod.get("brand") or "Unknown")[:255]
                if brand not in brand_cache:
                    self.target_cur.execute(
                        """
                        INSERT INTO dim_brand (brand_name)
                        VALUES (%s)
                        ON CONFLICT (brand_name) DO UPDATE
                        SET brand_name = EXCLUDED.brand_name
                        RETURNING brand_sk
                    """,
                        (brand,),
                    )

                    result = self.target_cur.fetchone()
                    brand_cache[brand] = result[0]

                brand_sk = brand_cache[brand]

                # 5. Date
                try:
                    date_val = prod["crawled_at"]
                    if isinstance(date_val, str):
                        date_val = datetime.fromisoformat(date_val.split()[0]).date()
                except (ValueError, AttributeError, TypeError):
                    date_val = datetime.now().date()

                date_key = str(date_val)
                if date_key not in date_cache:
                    self.target_cur.execute(
                        """
                        INSERT INTO dim_date (date_value, year, month, day)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (date_value) DO UPDATE
                        SET year = EXCLUDED.year
                        RETURNING date_sk
                    """,
                        (date_val, date_val.year, date_val.month, date_val.day),
                    )

                    result = self.target_cur.fetchone()
                    date_cache[date_key] = result[0]

                date_sk = date_cache[date_key]

                # 6. Price segment
                price = prod.get("price")
                if price:
                    try:
                        price = float(price)
                    except (ValueError, TypeError):
                        price = None

                if not price:
                    price_seg_sk = price_segments["Chưa cập nhật"]
                elif price < 100000:
                    price_seg_sk = price_segments["Rẻ (< 100K)"]
                elif price < 500000:
                    price_seg_sk = price_segments["Bình dân (100K-500K)"]
                elif price < 1000000:
                    price_seg_sk = price_segments["Trung bình (500K-1M)"]
                elif price < 5000000:
                    price_seg_sk = price_segments["Cao (1M-5M)"]
                else:
                    price_seg_sk = price_segments["Cao cấp (> 5M)"]

                # 7. Fact
                orig_price = prod.get("original_price")
                if orig_price:
                    try:
                        orig_price = float(orig_price)
                    except (ValueError, TypeError):
                        orig_price = None

                discount = prod.get("discount_percent")
                if not discount and price and orig_price and orig_price > 0:
                    discount = round(((orig_price - price) / orig_price) * 100, 2)

                revenue = price if price else 0
                profit = (price * 0.15) if price else 0

                self.target_cur.execute(
                    """
                    INSERT INTO fact_product_sales (
                        product_sk, category_sk, seller_sk, brand_sk, date_sk, price_segment_sk,
                        price, original_price, discount_percent, quantity_sold,
                        estimated_revenue, estimated_profit, average_rating, rating_count, review_count
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        prod_sk,
                        cat_sk,
                        seller_sk,
                        brand_sk,
                        date_sk,
                        price_seg_sk,
                        price,
                        orig_price,
                        discount,
                        prod.get("sales_count", 0),
                        revenue,
                        profit,
                        prod.get("rating", 0),
                        0,
                        prod.get("review_count", 0),
                    ),
                )

                fact_count += 1

                if idx % 200 == 0:
                    self.target_conn.commit()
                    logger.info(f"  Processed {idx}/{len(filtered_products)} (facts: {fact_count})")

            except Exception as e:
                logger.warning(f"  Error product {idx}: {e}")
                try:
                    self.target_conn.rollback()
                except Exception as rollback_exc:
                    logger.error(f"    Rollback failed for product {idx}: {rollback_exc}")
                continue

        self.target_conn.commit()
        logger.info(f"✓ Loaded {fact_count} facts")

    def verify(self):
        """Verify warehouse"""
        logger.info("\n" + "=" * 70)
        logger.info("VERIFY WAREHOUSE")
        logger.info("=" * 70)

        for table in [
            "dim_product",
            "dim_category",
            "dim_seller",
            "dim_brand",
            "dim_date",
            "dim_price_segment",
            "fact_product_sales",
        ]:
            self.target_cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = self.target_cur.fetchone()[0]
            logger.info(f"  {table:30} → {count:6,} rows")

    def run(self):
        """Run full pipeline"""
        try:
            logger.info("=" * 70)
            logger.info("STAR SCHEMA BUILDER V2")
            logger.info("=" * 70)

            self.connect()
            self.create_schema()
            self.load_price_segments()

            products = self.extract_products()
            self.load_data(products)

            self.verify()

            logger.info("\n" + "=" * 70)
            logger.info("✓ COMPLETED")
            logger.info("=" * 70)

        except Exception as e:
            logger.error(f"Error: {e}", exc_info=True)
            raise
        finally:
            self.close()


def main():
    parser = argparse.ArgumentParser(description="Build Star Schema from crawl_data")
    parser.add_argument("--db-host", default="localhost")
    parser.add_argument("--db-port", type=int, default=5432)
    parser.add_argument("--db-user", default="postgres")
    parser.add_argument("--db-password", default="postgres")
    parser.add_argument("--source-db", default="crawl_data")
    parser.add_argument("--target-db", default="tiki_warehouse")

    args = parser.parse_args()

    builder = StarSchemaBuilderV2(
        db_host=args.db_host,
        db_port=args.db_port,
        source_db=args.source_db,
        target_db=args.target_db,
        db_user=args.db_user,
        db_password=args.db_password,
    )

    builder.run()


if __name__ == "__main__":
    main()
