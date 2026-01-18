"""
Utility để lưu dữ liệu crawl vào PostgreSQL database
Tối ưu: Connection pooling, batch processing, error handling
"""

import csv
import io
import json
import os
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Any

import psycopg2
from psycopg2.extras import Json, execute_values
from psycopg2.pool import SimpleConnectionPool

from ..validate_category_path import fix_products_category_paths


class PostgresStorage:
    """Class để lưu và truy vấn dữ liệu crawl từ PostgreSQL với connection pooling tối ưu"""

    def __init__(
        self,
        host: str | None = None,
        port: int = 5432,
        database: str | None = None,
        user: str | None = None,
        password: str | None = None,
        minconn: int = 2,
        maxconn: int = 10,
        connect_timeout: int = 10,
        keepalives_idle: int = 600,
        keepalives_interval: int = 10,
        keepalives_count: int = 5,
    ):
        """
        Khởi tạo connection pool đến PostgreSQL với tối ưu hóa

        Args:
            host: PostgreSQL host (mặc định: lấy từ env hoặc 'postgres')
            port: PostgreSQL port (mặc định: 5432)
            database: Database name (mặc định: lấy từ POSTGRES_DB env hoặc 'tiki')
            user: PostgreSQL user (mặc định: lấy từ env)
            password: PostgreSQL password (mặc định: lấy từ env)
            minconn: Số connection tối thiểu trong pool (mặc định: 2)
            maxconn: Số connection tối đa trong pool (mặc định: 10)
            connect_timeout: Timeout khi connect (giây)
            keepalives_idle: Seconds before sending keepalive
            keepalives_interval: Seconds between keepalive probes
            keepalives_count: Number of keepalive probes before considering connection dead
        """
        self.host = host or os.getenv("POSTGRES_HOST", "postgres")
        self.port = port
        self.database = database or os.getenv("POSTGRES_DB", "tiki")
        self.user = user or os.getenv("POSTGRES_USER", "airflow_user")
        self.password = password or os.getenv("POSTGRES_PASSWORD", "")
        self.minconn = minconn
        self.maxconn = maxconn
        self._pool: SimpleConnectionPool | None = None
        self._pool_stats = {
            "total_connections": 0,
            "active_connections": 0,
            "failed_connections": 0,
            "retries": 0,
        }
        # Connection parameters
        self.connect_kwargs = {
            "connect_timeout": connect_timeout,
            "keepalives_idle": keepalives_idle,
            "keepalives_interval": keepalives_interval,
            "keepalives_count": keepalives_count,
        }

    def _get_pool(self) -> SimpleConnectionPool:
        """Lấy hoặc tạo connection pool với retry logic"""
        if self._pool is None:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    self._pool = SimpleConnectionPool(
                        self.minconn,
                        self.maxconn,
                        host=self.host,
                        port=self.port,
                        database=self.database,
                        user=self.user,
                        password=self.password,
                        **self.connect_kwargs,
                    )
                    self._pool_stats["total_connections"] = self.maxconn
                    return self._pool
                except Exception:
                    self._pool_stats["failed_connections"] += 1
                    if attempt < max_retries - 1:
                        time.sleep(2**attempt)  # Exponential backoff
                        continue
                    raise
        return self._pool

    def _check_connection(self, conn) -> bool:
        """Kiểm tra connection còn sống không (pre-ping)"""
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                return True
        except Exception:
            return False

    @contextmanager
    def get_connection(self, retries: int = 3):
        """
        Context manager để lấy connection từ pool với retry và health check

        Args:
            retries: Số lần retry nếu connection lỗi
        """
        pool = self._get_pool()
        conn = None

        for attempt in range(retries):
            try:
                conn = pool.getconn()

                # Pre-ping: Check connection health
                if not self._check_connection(conn):
                    # Connection dead, close và lấy connection mới
                    try:
                        conn.close()
                    except Exception:
                        pass
                    # Remove dead connection from pool
                    pool.putconn(conn, close=True)
                    conn = pool.getconn()

                self._pool_stats["active_connections"] = max(
                    self._pool_stats["active_connections"], len([c for c in pool._used if c])
                )

                yield conn
                conn.commit()
                break

            except (psycopg2.OperationalError, psycopg2.InterfaceError):
                # Connection error, retry
                if conn:
                    try:
                        conn.rollback()
                        pool.putconn(conn, close=True)  # Close dead connection
                    except Exception:
                        pass
                    conn = None

                if attempt < retries - 1:
                    self._pool_stats["retries"] += 1
                    time.sleep(0.5 * (attempt + 1))  # Exponential backoff
                    continue
                else:
                    raise

            except Exception:
                if conn:
                    conn.rollback()
                raise

            finally:
                if conn:
                    try:
                        pool.putconn(conn)
                    except Exception:
                        pass

    def _ensure_categories_schema(self, cur) -> None:
        """Đảm bảo bảng categories và các index tồn tại."""
        cur.execute("""
            CREATE TABLE IF NOT EXISTS categories (
                id SERIAL PRIMARY KEY,
                category_id VARCHAR(255) UNIQUE,
                name VARCHAR(500) NOT NULL,
                url TEXT NOT NULL UNIQUE,
                image_url TEXT,
                parent_url TEXT,
                level INTEGER,
                category_path JSONB,
                root_category_name VARCHAR(255),
                is_leaf BOOLEAN DEFAULT FALSE,
                product_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_cat_parent ON categories(parent_url);
            CREATE INDEX IF NOT EXISTS idx_cat_level ON categories(level);
            CREATE INDEX IF NOT EXISTS idx_cat_path ON categories USING GIN (category_path);
            CREATE INDEX IF NOT EXISTS idx_cat_is_leaf ON categories(is_leaf);
        """)

    def _ensure_products_schema(self, cur) -> None:
        """Đảm bảo bảng products và các index tồn tại."""
        cur.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id SERIAL PRIMARY KEY,
                product_id VARCHAR(255) UNIQUE,
                name VARCHAR(1000),
                url TEXT,
                image_url TEXT,
                category_url TEXT,
                category_id VARCHAR(255),
                category_path JSONB,
                sales_count INTEGER,
                price DECIMAL(15, 2),
                original_price DECIMAL(15, 2),
                discount_percent INTEGER,
                rating_average DECIMAL(3, 2),
                review_count INTEGER,
                description TEXT,
                specifications JSONB,
                images JSONB,
                seller_name VARCHAR(500),
                seller_id VARCHAR(255),
                seller_is_official BOOLEAN,
                brand VARCHAR(255),
                stock_available BOOLEAN,
                stock_quantity INTEGER,
                stock_status VARCHAR(100),
                shipping JSONB,
                estimated_revenue DECIMAL(15, 2),
                price_savings DECIMAL(15, 2),
                price_category VARCHAR(100),
                popularity_score DECIMAL(5, 2),
                value_score DECIMAL(5, 2),
                discount_amount DECIMAL(15, 2),
                sales_velocity DECIMAL(10, 2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_products_category_id ON products(category_id);
            CREATE INDEX IF NOT EXISTS idx_products_category_url ON products(category_url);
            CREATE INDEX IF NOT EXISTS idx_products_price ON products(price);
            CREATE INDEX IF NOT EXISTS idx_products_sales_count ON products(sales_count);
        """)

    def _ensure_history_schema(self, cur) -> None:
        """Đảm bảo bảng crawl_history và các index tồn tại."""
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crawl_history (
                id SERIAL PRIMARY KEY,
                crawl_type VARCHAR(100) DEFAULT 'product_tracking',
                status VARCHAR(50) DEFAULT 'success',
                product_id VARCHAR(255) NOT NULL,
                price DECIMAL(12, 2),
                price_change DECIMAL(12, 2),
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_history_product_id ON crawl_history(product_id);
            CREATE INDEX IF NOT EXISTS idx_history_crawled_at ON crawl_history(crawled_at);
        """)

    def _write_dicts_to_csv_buffer(self, rows: list[dict[str, Any]], columns: list[str]) -> io.StringIO:
        """Chuyển đổi danh sách dict thành CSV buffer (tab-separated)."""
        buf = io.StringIO()
        writer = csv.writer(buf, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        for item in rows:
            row = []
            for col in columns:
                val = item.get(col)
                if val is None:
                    row.append("")
                elif isinstance(val, (dict, list)):
                    try:
                        row.append(json.dumps(val, ensure_ascii=False))
                    except Exception:
                        row.append("{}")
                else:
                    row.append(val)
            writer.writerow(row)

        buf.seek(0)
        return buf

    def _bulk_merge_via_staging(
        self,
        cur,
        target_table: str,
        staging_table: str,
        columns: list[str],
        csv_buffer: io.StringIO,
        conflict_key: str,
        update_extra_clause: str = "",
        do_nothing: bool = False,
    ) -> int:
        """Helper generic để thực hiện bulk merge (COPY + INSERT ON CONFLICT) qua staging table."""
        from psycopg2 import sql

        # 1. Tạo staging table
        cur.execute(sql.SQL("""
            CREATE TEMP TABLE IF NOT EXISTS {staging} (
                LIKE {target} INCLUDING DEFAULTS
            ) ON COMMIT DROP;
            TRUNCATE {staging};
        """).format(
            staging=sql.Identifier(staging_table),
            target=sql.Identifier(target_table)
        ))

        # 2. Bulk Copy vào staging
        col_names = sql.SQL(",").join(map(sql.Identifier, columns))
        copy_query = sql.SQL("COPY {staging} ({cols}) FROM STDIN WITH (FORMAT CSV, DELIMITER '\\t', NULL '', QUOTE '\"')").format(
            staging=sql.Identifier(staging_table),
            cols=col_names
        )
        cur.copy_expert(copy_query.as_string(cur.connection), csv_buffer)

        # 3. Merge vào target table
        if do_nothing:
            merge_query = sql.SQL("""
                INSERT INTO {target} ({cols})
                SELECT {cols} FROM {staging}
                ON CONFLICT ({conflict}) DO NOTHING;
            """).format(
                target=sql.Identifier(target_table),
                staging=sql.Identifier(staging_table),
                cols=col_names,
                conflict=sql.Identifier(conflict_key)
            )
        else:
            merge_query = sql.SQL("""
                INSERT INTO {target} ({cols})
                SELECT {cols} FROM {staging}
                ON CONFLICT ({conflict}) DO UPDATE SET
                    {update_clause};
            """).format(
                target=sql.Identifier(target_table),
                staging=sql.Identifier(staging_table),
                cols=col_names,
                conflict=sql.Identifier(conflict_key),
                update_clause=sql.SQL(update_extra_clause)
            )
        
        cur.execute(merge_query)
        return cur.rowcount

    def save_categories(self, categories: list[dict[str, Any]]) -> int:
        """
        Lưu danh sách categories vào DB sử dụng bulk processing tối ưu.
        """
        if not categories:
            return 0

        import re

        # Build URL -> category lookup for path building
        url_to_cat = {cat.get("url"): cat for cat in categories}
        
        # Find all parent URLs to determine is_leaf
        parent_urls = {cat.get("parent_url") for cat in categories if cat.get("parent_url")}
        
        def build_category_path(cat: dict) -> list[str]:
            path = []
            current = cat
            visited = set()
            while current:
                if current.get("url") in visited: break
                visited.add(current.get("url"))
                path.insert(0, current.get("name", ""))
                parent_url = current.get("parent_url")
                current = url_to_cat.get(parent_url) if parent_url else None
            return path

        prepared_categories = []
        for cat in categories:
            # Extract category_id from URL if missing
            cat_id = cat.get("category_id")
            if not cat_id and cat.get("url"):
                match = re.search(r"c(\d+)", cat.get("url", ""))
                if match: cat_id = match.group(1)
            
            category_path = build_category_path(cat)
            root_name = category_path[0] if category_path else ""
            is_leaf = cat.get("url") not in parent_urls
            
            prepared_categories.append({
                "category_id": cat_id,
                "name": cat.get("name"),
                "url": cat.get("url"),
                "image_url": cat.get("image_url"),
                "parent_url": cat.get("parent_url"),
                "level": cat.get("level", 0),
                "category_path": category_path,
                "root_category_name": root_name,
                "is_leaf": is_leaf
            })

        columns = [
            "category_id", "name", "url", "image_url", "parent_url", 
            "level", "category_path", "root_category_name", "is_leaf"
        ]
        
        csv_buffer = self._write_dicts_to_csv_buffer(prepared_categories, columns)
        
        update_clause = """
            name = EXCLUDED.name,
            category_id = EXCLUDED.category_id,
            image_url = EXCLUDED.image_url,
            parent_url = EXCLUDED.parent_url,
            level = EXCLUDED.level,
            category_path = EXCLUDED.category_path,
            root_category_name = EXCLUDED.root_category_name,
            is_leaf = EXCLUDED.is_leaf,
            updated_at = CURRENT_TIMESTAMP
        """

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                self._ensure_categories_schema(cur)
                return self._bulk_merge_via_staging(
                    cur, "categories", "categories_staging", columns, 
                    csv_buffer, "url", update_clause
                )



    def save_products(
        self, products: list[dict[str, Any]], upsert: bool = True, batch_size: int = 100
    ) -> int | dict[str, Any]:
        """
        Lưu danh sách products vào database (batch insert để tối ưu)

        Args:
            products: List các product dictionaries
            upsert: Nếu True, update nếu đã tồn tại (dựa trên product_id)
            batch_size: Số products insert mỗi lần

        Returns:
            Nếu upsert=True: dict với keys: saved_count, inserted_count, updated_count
            Nếu upsert=False: int (số products đã lưu thành công)
        """
        if not products:
            return {"saved_count": 0, "inserted_count": 0, "updated_count": 0} if upsert else 0

        # Tự động fix category_path để đảm bảo parent category ở index 0
        try:
            products = fix_products_category_paths(products, hierarchy_map=None, auto_fix=True)
        except Exception as e:
            # Log nhưng không fail nếu fix có lỗi
            print(f"⚠️  Warning: Could not auto-fix category_path: {e}")

        # Tối ưu: Sử dụng Bulk COPY cho batch lớn (>200 items)
        if len(products) >= 200:
            try:
                return self._save_products_bulk_copy(products, upsert)
            except Exception as e:
                print(f"⚠️  Bulk COPY process failed: {e}. Falling back to standard batch insert.")
                # Fallback to standard flow below

        saved_count = 0
        inserted_count = 0
        updated_count = 0

        # Lấy danh sách product_id đã có trong DB (để phân biệt INSERT vs UPDATE)
        existing_product_ids: set[str] = set()
        if upsert:
            try:
                with self.get_connection() as conn:
                    with conn.cursor() as cur:
                        product_ids = [p.get("product_id") for p in products if p.get("product_id")]
                        if product_ids:
                            # Chia nhỏ query nếu có quá nhiều product_ids
                            for i in range(0, len(product_ids), 1000):
                                batch_ids = product_ids[i : i + 1000]
                                placeholders = ",".join(["%s"] * len(batch_ids))
                                cur.execute(
                                    f"SELECT product_id FROM products WHERE product_id IN ({placeholders})",
                                    batch_ids,
                                )
                                existing_product_ids.update(row[0] for row in cur.fetchall())
            except Exception as e:
                # Nếu không lấy được, tiếp tục với upsert bình thường
                print(f"⚠️  Không thể lấy danh sách products đã có: {e}")

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Chia nhỏ thành batches
                for i in range(0, len(products), batch_size):
                    batch = products[i : i + batch_size]
                    try:
                        # Chuẩn bị data cho batch insert
                        values = [
                            (
                                product.get("product_id"),
                                product.get("name"),
                                product.get("url"),
                                product.get("image_url"),
                                product.get("category_url"),
                                product.get("category_id"),  # Thêm category_id
                                (
                                    Json(product.get("category_path"))
                                    if product.get("category_path")
                                    else None
                                ),  # Thêm category_path (JSONB)
                                product.get("sales_count"),
                                product.get("price"),
                                product.get("original_price"),
                                product.get("discount_percent"),
                                product.get("rating_average"),
                                product.get("review_count"),
                                product.get("description"),
                                (
                                    Json(product.get("specifications"))
                                    if product.get("specifications")
                                    else None
                                ),
                                Json(product.get("images")) if product.get("images") else None,
                                # Seller fields
                                product.get("seller_name"),
                                product.get("seller_id"),
                                product.get("seller_is_official"),
                                # Brand and stock
                                product.get("brand"),
                                product.get("stock_available"),
                                product.get("stock_quantity"),
                                product.get("stock_status"),
                                Json(product.get("shipping")) if product.get("shipping") else None,
                                # Computed fields
                                product.get("estimated_revenue"),
                                product.get("price_savings"),
                                product.get("price_category"),
                                product.get("popularity_score"),
                                product.get("value_score"),
                                product.get("discount_amount"),
                                product.get("sales_velocity"),
                            )
                            for product in batch
                        ]

                        if upsert:
                            # Đếm số products mới vs đã có trong batch này
                            batch_inserted = sum(
                                1
                                for p in batch
                                if p.get("product_id")
                                and p.get("product_id") not in existing_product_ids
                            )
                            batch_updated = len(batch) - batch_inserted

                            # Batch INSERT ... ON CONFLICT UPDATE
                            execute_values(
                                cur,
                                """
                                INSERT INTO products
                                    (product_id, name, url, image_url, category_url, category_id, category_path, sales_count,
                                     price, original_price, discount_percent, rating_average,
                                     review_count, description, specifications, images,
                                     seller_name, seller_id, seller_is_official, brand,
                                     stock_available, stock_quantity, stock_status, shipping,
                                     estimated_revenue, price_savings, price_category, popularity_score,
                                     value_score, discount_amount, sales_velocity)
                                VALUES %s
                                ON CONFLICT (product_id)
                                DO UPDATE SET
                                    name = EXCLUDED.name,
                                    url = EXCLUDED.url,
                                    image_url = EXCLUDED.image_url,
                                    category_url = EXCLUDED.category_url,
                                    category_id = EXCLUDED.category_id,
                                    category_path = EXCLUDED.category_path,
                                    sales_count = EXCLUDED.sales_count,
                                    price = EXCLUDED.price,
                                    original_price = EXCLUDED.original_price,
                                    discount_percent = EXCLUDED.discount_percent,
                                    rating_average = EXCLUDED.rating_average,
                                    review_count = EXCLUDED.review_count,
                                    description = EXCLUDED.description,
                                    specifications = EXCLUDED.specifications,
                                    images = EXCLUDED.images,
                                    seller_name = EXCLUDED.seller_name,
                                    seller_id = EXCLUDED.seller_id,
                                    seller_is_official = EXCLUDED.seller_is_official,
                                    brand = EXCLUDED.brand,
                                    stock_available = EXCLUDED.stock_available,
                                    stock_quantity = EXCLUDED.stock_quantity,
                                    stock_status = EXCLUDED.stock_status,
                                    shipping = EXCLUDED.shipping,
                                    estimated_revenue = EXCLUDED.estimated_revenue,
                                    price_savings = EXCLUDED.price_savings,
                                    price_category = EXCLUDED.price_category,
                                    popularity_score = EXCLUDED.popularity_score,
                                    value_score = EXCLUDED.value_score,
                                    discount_amount = EXCLUDED.discount_amount,
                                    sales_velocity = EXCLUDED.sales_velocity,
                                    updated_at = CURRENT_TIMESTAMP
                                """,
                                values,
                            )

                            # Cập nhật existing_product_ids sau khi insert
                            for product in batch:
                                product_id = product.get("product_id")
                                if product_id:
                                    existing_product_ids.add(product_id)

                            inserted_count += batch_inserted
                            updated_count += batch_updated
                        else:
                            # Batch INSERT, bỏ qua nếu đã tồn tại
                            execute_values(
                                cur,
                                """
                                INSERT INTO products
                                    (product_id, name, url, image_url, category_url, category_id, category_path, sales_count,
                                     price, original_price, discount_percent, rating_average,
                                     review_count, description, specifications, images,
                                     seller_name, seller_id, seller_is_official, brand,
                                     stock_available, stock_quantity, stock_status, shipping,
                                     estimated_revenue, price_savings, price_category, popularity_score,
                                     value_score, discount_amount, sales_velocity)
                                VALUES %s
                                ON CONFLICT (product_id) DO NOTHING
                                """,
                                values,
                            )

                        saved_count += len(batch)
                    except Exception as e:
                        print(f"⚠️  Lỗi khi lưu batch products: {e}")
                        # Thử lưu từng product một nếu batch fail
                        for product in batch:
                            try:
                                result = self.save_products([product], upsert=upsert, batch_size=1)
                                if upsert and isinstance(result, dict):
                                    saved_count += 1
                                    inserted_count += result.get("inserted_count", 0)
                                    updated_count += result.get("updated_count", 0)
                                else:
                                    saved_count += 1
                            except Exception:
                                continue

        # Log crawl history for all saved products (for price tracking)
        if saved_count > 0:
            try:
                self._log_batch_crawl_history(products[:saved_count])
            except Exception as e:
                print(f"⚠️  Failed to log crawl history: {e}")

        if upsert:
            return {
                "saved_count": saved_count,
                "inserted_count": inserted_count,
                "updated_count": updated_count,
            }
        return {
            "saved_count": saved_count,
            "inserted_count": saved_count,
            "updated_count": 0,
        }

    def _log_batch_crawl_history(self, products: list[dict[str, Any]]) -> None:
        """
        Log price history for products. Calculates price_change from previous crawl.
        Schema: (product_id, price, price_change, crawled_at)
        """
        if not products:
            return

        from datetime import datetime

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Ensure table exists with new schema
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS crawl_history (
                        id SERIAL PRIMARY KEY,
                        product_id VARCHAR(255) NOT NULL,
                        price DECIMAL(12, 2),
                        price_change DECIMAL(12, 2),
                        crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    CREATE INDEX IF NOT EXISTS idx_history_product_id ON crawl_history(product_id);
                    CREATE INDEX IF NOT EXISTS idx_history_crawled_at ON crawl_history(crawled_at);
                """)

                for p in products:
                    product_id = p.get("product_id")
                    if not product_id:
                        continue
                    
                    current_price = p.get("price")
                    if current_price is None:
                        continue
                    
                    # Get previous price for this product
                    cur.execute("""
                        SELECT price FROM crawl_history 
                        WHERE product_id = %s 
                        ORDER BY crawled_at DESC LIMIT 1
                    """, (product_id,))
                    
                    row = cur.fetchone()
                    previous_price = row[0] if row else None
                    
                    # Calculate price change
                    price_change = None
                    if previous_price is not None and current_price is not None:
                        price_change = float(current_price) - float(previous_price)
                    
                    # Insert new price record
                    cur.execute("""
                        INSERT INTO crawl_history (product_id, price, price_change)
                        VALUES (%s, %s, %s)
                    """, (product_id, current_price, price_change))

    def log_price_history(self, product_id: str, price: float) -> int:
        """
        Log single product price to history.
        
        Args:
            product_id: Product ID
            price: Current product price
            
        Returns:
            ID of the new history record
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Get previous price
                cur.execute("""
                    SELECT price FROM crawl_history 
                    WHERE product_id = %s 
                    ORDER BY crawled_at DESC LIMIT 1
                """, (product_id,))
                
                row = cur.fetchone()
                previous_price = row[0] if row else None
                
                # Calculate price change
                price_change = None
                if previous_price is not None:
                    price_change = float(price) - float(previous_price)
                
                cur.execute("""
                    INSERT INTO crawl_history (product_id, price, price_change)
                    VALUES (%s, %s, %s)
                    RETURNING id
                """, (product_id, price, price_change))
                
                return cur.fetchone()[0]

    def update_category_product_counts(self) -> int:
        """
        Update product_count for all categories based on actual products in DB.
        Call this after saving products.
        
        Returns:
            Number of categories updated
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE categories c
                    SET product_count = COALESCE((
                        SELECT COUNT(*) 
                        FROM products p 
                        WHERE p.category_url = c.url
                    ), 0),
                    updated_at = CURRENT_TIMESTAMP
                """)
                return cur.rowcount

    def get_products_by_category(self, category_url: str, limit: int = 100) -> list[dict[str, Any]]:
        """Lấy danh sách products theo category_url"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT product_id, name, url, image_url, sales_count, price,
                           rating_average, review_count, crawled_at
                    FROM products
                    WHERE category_url = %s
                    ORDER BY sales_count DESC NULLS LAST, crawled_at DESC
                    LIMIT %s
                    """,
                    (category_url, limit),
                )
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row, strict=False)) for row in cur.fetchall()]

    def get_category_stats(self) -> dict[str, Any]:
        """Lấy thống kê tổng quan về products (Category stats removed)"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        0 as total_categories,
                        (SELECT COUNT(*) FROM products) as total_products,
                        (SELECT COUNT(DISTINCT category_url) FROM products) as categories_with_products,
                        (SELECT AVG(sales_count) FROM products WHERE sales_count IS NOT NULL) as avg_sales_count,
                        (SELECT MAX(crawled_at) FROM products) as last_crawl_time
                    """
                )
                row = cur.fetchone()
                return {
                    "total_categories": 0,
                    "total_products": row[1],
                    "categories_with_products": row[2],
                    "avg_sales_count": float(row[3]) if row[3] else 0,
                    "last_crawl_time": row[4],
                }

    def get_pool_stats(self) -> dict[str, Any]:
        """Lấy thống kê connection pool"""
        stats = self._pool_stats.copy()
        if self._pool:
            try:
                stats["pool_size"] = self.maxconn
                stats["min_conn"] = self.minconn
                # Note: SimpleConnectionPool không expose số connection đang dùng
            except Exception:
                pass
        return stats

    def close(self):
        """Đóng connection pool"""
        if self._pool:
            self._pool.closeall()
            self._pool = None
            self._pool_stats["active_connections"] = 0

    def _bulk_log_product_price_history(self, cur, products: list[dict[str, Any]]) -> None:
        """Bulk log crawl history using COPY protocol."""
        if not products:
            return

        from datetime import datetime
        import io
        import csv
        import json

        history_buffer = io.StringIO()
        h_writer = csv.writer(history_buffer, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        current_ts = datetime.now().isoformat()

        for p in products:
            h_writer.writerow([
                "product_tracking", # crawl_type
                "success",          # status
                p.get("product_id"),
                p.get("price") if p.get("price") is not None else "",
                "",                 # price_change Placeholder
                current_ts,         # started_at
                current_ts,         # completed_at
                current_ts          # crawled_at
            ])

        history_buffer.seek(0)
        cur.copy_expert(
            "COPY crawl_history (crawl_type, status, product_id, price, price_change, started_at, completed_at, crawled_at) "
            "FROM STDIN WITH (FORMAT CSV, DELIMITER '\\t', NULL '', QUOTE '\"')",
            history_buffer
        )

    def _save_products_bulk_copy(
        self, products: list[dict[str, Any]], upsert: bool
    ) -> dict[str, Any]:
        """Thực hiện Bulk Load sử dụng COPY protocol qua Staging Table."""
        if not products:
            return {"saved_count": 0, "inserted_count": 0, "updated_count": 0}

        columns = [
            "product_id", "name", "url", "image_url", "category_url", "category_id", "category_path",
            "sales_count", "price", "original_price", "discount_percent", "rating_average",
            "review_count", "description", "specifications", "images",
            "seller_name", "seller_id", "seller_is_official", "brand",
            "stock_available", "stock_quantity", "stock_status", "shipping",
            "estimated_revenue", "price_savings", "price_category", "popularity_score",
            "value_score", "discount_amount", "sales_velocity"
        ]

        csv_buffer = self._write_dicts_to_csv_buffer(products, columns)

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                self._ensure_products_schema(cur)
                self._ensure_history_schema(cur)

                update_cols = [f"{c} = EXCLUDED.{c}" for c in columns if c != "product_id"]
                update_stmt = ",".join(update_cols)

                saved_count = self._bulk_merge_via_staging(
                    cur, "products", "products_staging", columns,
                    csv_buffer, "product_id", update_stmt, do_nothing=not upsert
                )

                try:
                    self._bulk_log_product_price_history(cur, products)
                except Exception as e:
                    print(f"⚠️  Failed to log bulk history: {e}")

                return {
                    "saved_count": saved_count,
                    "inserted_count": saved_count, # Approx
                    "updated_count": 0
                }
