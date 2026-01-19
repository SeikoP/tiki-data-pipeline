"""
Utility để lưu dữ liệu crawl vào PostgreSQL database
Tối ưu: Connection pooling, batch processing, error handling
"""

import csv
import io
import json
import os
import re
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Any

import psycopg2
from psycopg2 import sql
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
            
    def get_used_category_ids(self) -> set[str]:
        """Lấy danh sách unique category_id từ bảng products"""
        used_ids = set()
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Check if products table exists
                    cur.execute(
                        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'products')"
                    )
                    if not cur.fetchone()[0]:
                        return used_ids

                    cur.execute("SELECT DISTINCT category_id FROM products WHERE category_id IS NOT NULL")
                    rows = cur.fetchall()
                    for row in rows:
                        cat_id = row[0]
                        if cat_id:
                            # Normalize: ensure 'c' prefix if it looks like just numbers
                            if str(cat_id).isdigit():
                                cat_id = f"c{cat_id}"
                            used_ids.add(cat_id)
        except Exception as e:
            print(f"⚠️  Error getting used category IDs: {e}")
        return used_ids

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
        """Đảm bảo bảng categories và các index tồn tại.
        
        Lưu ý: Bảng này chỉ lưu các category có is_leaf = true.
        category_path được mở rộng với các trường level_1 đến level_5 để thể hiện rõ theo từng độ sâu.
        """
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS categories (
                id SERIAL PRIMARY KEY,
                category_id VARCHAR(255) UNIQUE,
                name VARCHAR(500) NOT NULL,
                url TEXT NOT NULL UNIQUE,
                image_url TEXT,
                parent_url TEXT,
                level INTEGER,
                category_path JSONB,
                level_1 VARCHAR(255),
                level_2 VARCHAR(255),
                level_3 VARCHAR(255),
                level_4 VARCHAR(255),
                level_5 VARCHAR(255),
                root_category_name VARCHAR(255),
                is_leaf BOOLEAN DEFAULT FALSE,
                product_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            -- Only essential index: level_2 is used for category filtering queries
            CREATE INDEX IF NOT EXISTS idx_cat_level2 ON categories(level_2);
        """
        )

    def _ensure_products_schema(self, cur) -> None:
        """Đảm bảo bảng products và các index tồn tại.
        
        Đã loại bỏ các trường: category_path, review_count, description, images,
        estimated_revenue, price_savings, price_category, value_score, sales_velocity,
        specifications, popularity_score, discount_amount
        """
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS products (
                id SERIAL PRIMARY KEY,
                product_id VARCHAR(255) UNIQUE,
                name VARCHAR(1000),
                url TEXT,
                image_url TEXT,
                category_url TEXT,
                category_id VARCHAR(255),
                sales_count INTEGER,
                price DECIMAL(15, 2),
                original_price DECIMAL(15, 2),
                discount_percent INTEGER,
                rating_average DECIMAL(3, 2),
                seller_name VARCHAR(500),
                seller_id VARCHAR(255),
                seller_is_official BOOLEAN,
                brand VARCHAR(255),
                stock_available BOOLEAN,
                stock_quantity INTEGER,
                stock_status VARCHAR(100),
                shipping JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            -- Only essential index: category_id is used to join with categories table
            CREATE INDEX IF NOT EXISTS idx_products_category_id ON products(category_id);
        """
        )

    def _ensure_history_schema(self, cur) -> None:
        """Đảm bảo bảng crawl_history và các index tồn tại.
        
        Bảng này lưu lịch sử giá CHỈ KHI CÓ THAY ĐỔI (giá, discount, etc.)
        để tiết kiệm dung lượng database.
        
        Schema đã được tối ưu:
        - Loại bỏ: crawl_type, status, started_at, completed_at (không cần thiết)
        - Giữ lại: product_id, price, original_price, discount_percent, price_change, crawled_at
        """
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS crawl_history (
                id SERIAL PRIMARY KEY,
                product_id VARCHAR(255) NOT NULL,
                price DECIMAL(12, 2) NOT NULL,
                original_price DECIMAL(12, 2),
                discount_percent INTEGER,
                price_change DECIMAL(12, 2),
                previous_price DECIMAL(12, 2),
                previous_original_price DECIMAL(12, 2),
                previous_discount_percent INTEGER,
                crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Optimized indexes for common queries
            CREATE INDEX IF NOT EXISTS idx_history_product_id ON crawl_history(product_id);
            CREATE INDEX IF NOT EXISTS idx_history_crawled_at ON crawl_history(crawled_at);
            -- Composite index for getting latest price per product (most common query)
            CREATE INDEX IF NOT EXISTS idx_history_product_latest ON crawl_history(product_id, crawled_at DESC);

            -- Ensure new columns exist (migration for existing table)
            ALTER TABLE crawl_history ADD COLUMN IF NOT EXISTS previous_price DECIMAL(12, 2);
            ALTER TABLE crawl_history ADD COLUMN IF NOT EXISTS previous_original_price DECIMAL(12, 2);
            ALTER TABLE crawl_history ADD COLUMN IF NOT EXISTS previous_discount_percent INTEGER;
        """
        )

    def _write_dicts_to_csv_buffer(
        self, rows: list[dict[str, Any]], columns: list[str]
    ) -> io.StringIO:
        """Chuyển đổi danh sách dict thành CSV buffer (tab-separated)."""
        buf = io.StringIO()
        writer = csv.writer(buf, delimiter="\t", quotechar='"', quoting=csv.QUOTE_MINIMAL)

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
        update_cols: list[str] | None = None,
        do_nothing: bool = False,
    ) -> int:
        """Helper generic để thực hiện bulk merge (COPY + INSERT ON CONFLICT) qua staging table.

        Args:
            cur: Database cursor
            target_table: Target table name
            staging_table: Staging table name
            columns: Column names to insert
            csv_buffer: CSV data buffer
            conflict_key: Primary key column for conflict detection
            update_cols: List of column names to update on conflict.
                        If None, all columns except conflict_key will be updated
            do_nothing: If True, do nothing on conflict instead of update
        """
        # 1. Tạo staging table
        cur.execute(
            sql.SQL(
                """
            CREATE TEMP TABLE IF NOT EXISTS {staging} (
                LIKE {target} INCLUDING DEFAULTS
            ) ON COMMIT DROP;
            TRUNCATE {staging};
        """
            ).format(staging=sql.Identifier(staging_table), target=sql.Identifier(target_table))
        )

        # 2. Bulk Copy vào staging
        col_names = sql.SQL(",").join(map(sql.Identifier, columns))
        copy_query = sql.SQL(
            "COPY {staging} ({cols}) FROM STDIN WITH (FORMAT CSV, DELIMITER '\t', NULL '', QUOTE '\"')"
        ).format(staging=sql.Identifier(staging_table), cols=col_names)
        cur.copy_expert(copy_query.as_string(cur.connection), csv_buffer)

        # 3. Merge vào target table
        if do_nothing:
            merge_query = sql.SQL(
                """
                INSERT INTO {target} ({cols})
                SELECT {cols} FROM {staging}
                ON CONFLICT ({conflict}) DO NOTHING;
            """
            ).format(
                target=sql.Identifier(target_table),
                staging=sql.Identifier(staging_table),
                cols=col_names,
                conflict=sql.Identifier(conflict_key),
            )
        else:
            # Build update clause safely using sql.Identifier
            if not update_cols:
                update_cols = [c for c in columns if c != conflict_key]

            update_parts = sql.SQL(",").join(
                sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col))
                for col in update_cols
            )

            merge_query = sql.SQL(
                """
                INSERT INTO {target} ({cols})
                SELECT {cols} FROM {staging}
                ON CONFLICT ({conflict}) DO UPDATE SET
                    {update_clause};
            """
            ).format(
                target=sql.Identifier(target_table),
                staging=sql.Identifier(staging_table),
                cols=col_names,
                conflict=sql.Identifier(conflict_key),
                update_clause=update_parts,
            )

        cur.execute(merge_query)
        return cur.rowcount

    def save_categories(
        self, 
        categories: list[dict[str, Any]], 
        only_leaf: bool = True,
        sync_with_products: bool = False
    ) -> int:
        """
        Lưu danh sách categories vào DB sử dụng bulk processing tối ưu.
        
        Args:
            categories: Danh sách categories cần lưu
            only_leaf: Nếu True, chỉ lưu categories có is_leaf = true (mặc định).
                      Nếu False, lưu tất cả categories.
            sync_with_products: Nếu True, chỉ lưu các categories có match với product
                      (category_id có trong bảng products).
                      
        Returns:
            Số lượng categories đã lưu
        """
        if not categories:
            return 0

        # Build URL -> category lookup for path building
        url_to_cat = {cat.get("url"): cat for cat in categories}

        # Find all parent URLs to determine is_leaf
        parent_urls = {cat.get("parent_url") for cat in categories if cat.get("parent_url")}

        def build_category_path(cat: dict) -> list[str]:
            path = []
            current = cat
            visited = set()
            while current:
                if current.get("url") in visited:
                    break
                visited.add(current.get("url"))
                path.insert(0, current.get("name", ""))
                parent_url = current.get("parent_url")
                current = url_to_cat.get(parent_url) if parent_url else None
            return path

        
        # Get used category IDs if filtering is enabled
        used_category_ids = set()
        if sync_with_products:
            used_category_ids = self.get_used_category_ids()
            print(f"🔍 Found {len(used_category_ids)} active categories in products table")

        prepared_categories = []
        for cat in categories:
            # Extract category_id from URL if missing - ALWAYS include 'c' prefix for consistency
            cat_id = cat.get("category_id")
            if not cat_id and cat.get("url"):
                match = re.search(r"(c\d+)", cat.get("url", ""))
                if match:
                    cat_id = match.group(1)  # Include 'c' prefix (e.g., 'c23570')
            # Ensure existing category_id has 'c' prefix
            elif cat_id and not cat_id.startswith("c"):
                cat_id = f"c{cat_id}"

            category_path = build_category_path(cat)
            root_name = category_path[0] if category_path else ""
            is_leaf = cat.get("url") not in parent_urls

            # Nếu only_leaf=True, chỉ lưu categories có is_leaf=True
            if only_leaf and not is_leaf:
                continue

            # Nếu sync_with_products=True, chỉ lưu categories có trong products table
            if sync_with_products and is_leaf:
                # Check normalized ID
                if cat_id not in used_category_ids:
                    # Try alternate format (without 'c' prefix) just in case
                    raw_id = cat_id[1:] if cat_id.startswith('c') else cat_id
                    if raw_id not in used_category_ids:
                        continue

            # Extract level_1 đến level_5 từ category_path để thể hiện rõ theo từng độ sâu
            level_1 = category_path[0] if len(category_path) > 0 else None
            level_2 = category_path[1] if len(category_path) > 1 else None
            level_3 = category_path[2] if len(category_path) > 2 else None
            level_4 = category_path[3] if len(category_path) > 3 else None
            level_5 = category_path[4] if len(category_path) > 4 else None

            prepared_categories.append(
                {
                    "category_id": cat_id,
                    "name": cat.get("name"),
                    "url": cat.get("url"),
                    "image_url": cat.get("image_url"),
                    "parent_url": cat.get("parent_url"),
                    "level": cat.get("level", 0),
                    "category_path": category_path,
                    "level_1": level_1,
                    "level_2": level_2,
                    "level_3": level_3,
                    "level_4": level_4,
                    "level_5": level_5,
                    "root_category_name": root_name,
                    "is_leaf": is_leaf,
                }
            )

        columns = [
            "category_id",
            "name",
            "url",
            "image_url",
            "parent_url",
            "level",
            "category_path",
            "level_1",
            "level_2",
            "level_3",
            "level_4",
            "level_5",
            "root_category_name",
            "is_leaf",
        ]

        csv_buffer = self._write_dicts_to_csv_buffer(prepared_categories, columns)

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                self._ensure_categories_schema(cur)
                # Column names to update on conflict
                update_columns = [
                    "name",
                    "category_id",
                    "image_url",
                    "parent_url",
                    "level",
                    "category_path",
                    "level_1",
                    "level_2",
                    "level_3",
                    "level_4",
                    "level_5",
                    "root_category_name",
                    "is_leaf",
                ]
                saved_count = self._bulk_merge_via_staging(
                    cur,
                    "categories",
                    "categories_staging",
                    columns,
                    csv_buffer,
                    "url",
                    update_cols=update_columns,
                )
                
                # Auto-add missing parent categories (root categories)
                # Tìm các parent_url không có trong data và tự động thêm
                missing_parents = parent_urls - set(url_to_cat.keys())
                if missing_parents:
                    for parent_url in missing_parents:
                        if not parent_url:
                            continue
                        # Extract category_id from URL
                        match = re.search(r"(c\d+)", parent_url)
                        cat_id = match.group(1) if match else None
                        
                        if cat_id:
                            cur.execute(
                                """
                                INSERT INTO categories (category_id, name, url, parent_url, level, is_leaf, product_count)
                                VALUES (%s, %s, %s, NULL, 0, false, 0)
                                ON CONFLICT (url) DO NOTHING
                                """,
                                (cat_id, cat_id, parent_url),  # Tạm dùng ID làm tên
                            )
                    print(f"📂 Auto-added {len(missing_parents)} missing parent categories")
                
                return saved_count

    def save_products(
        self, products: list[dict[str, Any]], upsert: bool = True, batch_size: int = 100
    ) -> dict[str, Any]:
        """
        Lưu danh sách products vào database (batch insert để tối ưu)

        Args:
            products: List các product dictionaries
            upsert: Nếu True, update nếu đã tồn tại (dựa trên product_id)
            batch_size: Số products insert mỗi lần

        Returns:
            dict với keys: saved_count, inserted_count, updated_count
            (inserted_count = 0 if upsert=False, since we're only inserting)
        """
        if not products:
            return {"saved_count": 0, "inserted_count": 0, "updated_count": 0}

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
                        # Chuẩn bị data cho batch insert (đã loại bỏ các trường: category_path, review_count, description, images, estimated_revenue, price_savings, price_category, value_score, sales_velocity, specifications, popularity_score, discount_amount)
                        values = [
                            (
                                product.get("product_id"),
                                product.get("name"),
                                product.get("url"),
                                product.get("image_url"),
                                product.get("category_url"),
                                product.get("category_id"),
                                product.get("sales_count"),
                                product.get("price"),
                                product.get("original_price"),
                                product.get("discount_percent"),
                                product.get("rating_average"),
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

                            # Batch INSERT ... ON CONFLICT UPDATE (đã loại bỏ các trường đã xóa: specifications, popularity_score, discount_amount)
                            execute_values(
                                cur,
                                """
                                INSERT INTO products
                                    (product_id, name, url, image_url, category_url, category_id, sales_count,
                                     price, original_price, discount_percent, rating_average,
                                     seller_name, seller_id, seller_is_official, brand,
                                     stock_available, stock_quantity, stock_status, shipping)
                                VALUES %s
                                ON CONFLICT (product_id)
                                DO UPDATE SET
                                    name = EXCLUDED.name,
                                    url = EXCLUDED.url,
                                    image_url = EXCLUDED.image_url,
                                    category_url = EXCLUDED.category_url,
                                    category_id = EXCLUDED.category_id,
                                    sales_count = EXCLUDED.sales_count,
                                    price = EXCLUDED.price,
                                    original_price = EXCLUDED.original_price,
                                    discount_percent = EXCLUDED.discount_percent,
                                    rating_average = EXCLUDED.rating_average,
                                    seller_name = EXCLUDED.seller_name,
                                    seller_id = EXCLUDED.seller_id,
                                    seller_is_official = EXCLUDED.seller_is_official,
                                    brand = EXCLUDED.brand,
                                    stock_available = EXCLUDED.stock_available,
                                    stock_quantity = EXCLUDED.stock_quantity,
                                    stock_status = EXCLUDED.stock_status,
                                    shipping = EXCLUDED.shipping,
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
                            # Batch INSERT, bỏ qua nếu đã tồn tại (đã loại bỏ các trường đã xóa: specifications, popularity_score, discount_amount)
                            execute_values(
                                cur,
                                """
                                INSERT INTO products
                                    (product_id, name, url, image_url, category_url, category_id, sales_count,
                                     price, original_price, discount_percent, rating_average,
                                     seller_name, seller_id, seller_is_official, brand,
                                     stock_available, stock_quantity, stock_status, shipping)
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
                                # result is always a dict now
                                saved_count += 1
                                inserted_count += result.get("inserted_count", 0)
                                updated_count += result.get("updated_count", 0)
                            except Exception:
                                continue

        # Log crawl history for all saved products (for price tracking)
        if saved_count > 0:
            try:
                self._log_batch_crawl_history(products[:saved_count])
            except Exception as e:
                print(f"⚠️  Failed to log crawl history: {e}")
            
            # Auto-sync: Ensure all categories from products exist in categories table
            try:
                self._ensure_categories_from_products(products[:saved_count])
            except Exception as e:
                print(f"⚠️  Failed to sync missing categories: {e}")

        return {
            "saved_count": saved_count,
            "inserted_count": inserted_count,
            "updated_count": updated_count,
        }

    def _ensure_categories_from_products(self, products: list[dict[str, Any]]) -> int:
        """
        Tự động tạo missing categories từ products.
        
        Khi crawl products, category_id và category_url có thể chưa tồn tại trong bảng categories.
        Method này sẽ tự động thêm các categories đó để đảm bảo data integrity.
        
        Returns:
            Số categories mới được thêm
        """
        if not products:
            return 0

        # Collect unique category info from products
        categories_to_add = {}
        for p in products:
            cat_id = p.get("category_id")
            cat_url = p.get("category_url")
            
            if not cat_id or not cat_url:
                continue
            
            # Ensure category_id has 'c' prefix
            if not cat_id.startswith("c"):
                cat_id = f"c{cat_id}"
            
            if cat_url not in categories_to_add:
                # Chỉ lưu category_id làm tên tạm. 
                # Tên đúng (tiếng Việt có dấu) sẽ được cập nhật khi task load categories chạy.
                categories_to_add[cat_url] = {
                    "category_id": cat_id,
                    "name": cat_id,  # Tạm dùng ID làm tên
                    "url": cat_url,
                }

        if not categories_to_add:
            return 0

        added_count = 0
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                self._ensure_categories_schema(cur)
                
                for cat_url, cat_info in categories_to_add.items():
                    # Insert only if not exists (by url)
                    cur.execute(
                        """
                        INSERT INTO categories (category_id, name, url, is_leaf, product_count)
                        VALUES (%s, %s, %s, true, 0)
                        ON CONFLICT (url) DO NOTHING
                        """,
                        (cat_info["category_id"], cat_info["name"], cat_info["url"]),
                    )
                    if cur.rowcount > 0:
                        added_count += 1

        if added_count > 0:
            print(f"📂 Auto-added {added_count} missing categories from products")
        
        return added_count

    def _log_batch_crawl_history(self, products: list[dict[str, Any]]) -> None:
        """
        Log price history for products CHỈ KHI CÓ THAY ĐỔI GIÁ.
        
        Tối ưu:
        - Sử dụng batch lookup để lấy giá cũ (thay vì query từng product)
        - Chỉ INSERT khi giá thay đổi hoặc là lần crawl đầu tiên
        - Tiết kiệm ~90% storage khi giá ổn định
        
        Schema: (product_id, price, original_price, discount_percent, price_change, previous_*, crawled_at)
        """
        if not products:
            return

        # Filter products with valid price
        valid_products = [
            p for p in products 
            if p.get("product_id") and p.get("price") is not None
        ]
        
        if not valid_products:
            return

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                self._ensure_history_schema(cur)

                # BATCH LOOKUP: Get all previous prices in one query
                product_ids = [p.get("product_id") for p in valid_products]
                
                # Use DISTINCT ON to get latest price per product efficiently
                cur.execute(
                    """
                    SELECT DISTINCT ON (product_id) 
                        product_id, price, original_price, discount_percent
                    FROM crawl_history
                    WHERE product_id = ANY(%s)
                    ORDER BY product_id, crawled_at DESC
                    """,
                    (product_ids,),
                )
                
                # Build lookup dict: product_id -> (price, original_price, discount_percent)
                previous_data = {
                    row[0]: {"price": row[1], "original_price": row[2], "discount_percent": row[3]}
                    for row in cur.fetchall()
                }

                # Collect products that need history update
                records_to_insert = []
                
                for p in valid_products:
                    product_id = p.get("product_id")
                    current_price = p.get("price")
                    current_original_price = p.get("original_price")
                    current_discount = p.get("discount_percent")
                    
                    prev = previous_data.get(product_id)
                    
                    if prev is None:
                        # First time crawl - always insert
                        records_to_insert.append({
                            "product_id": product_id,
                            "price": current_price,
                            "original_price": current_original_price,
                            "discount_percent": current_discount,
                            "price_change": None,
                            "previous_price": None,
                            "previous_original_price": None,
                            "previous_discount_percent": None,
                        })
                    else:
                        # Check if anything changed
                        prev_price = float(prev["price"]) if prev["price"] else None
                        prev_original = float(prev["original_price"]) if prev["original_price"] else None
                        prev_discount = prev["discount_percent"]
                        
                        current_price_float = float(current_price) if current_price else None
                        current_original_float = float(current_original_price) if current_original_price else None
                        
                        # Only insert if price, original_price, or discount changed
                        price_changed = prev_price != current_price_float
                        original_changed = prev_original != current_original_float
                        discount_changed = prev_discount != current_discount
                        
                        if price_changed or original_changed or discount_changed:
                            price_change = None
                            if prev_price is not None and current_price_float is not None:
                                price_change = current_price_float - prev_price
                            
                            records_to_insert.append({
                                "product_id": product_id,
                                "price": current_price,
                                "original_price": current_original_price,
                                "discount_percent": current_discount,
                                "price_change": price_change,
                                "previous_price": prev_price,
                                "previous_original_price": prev_original,
                                "previous_discount_percent": prev_discount,
                            })

                # Batch insert all changed records
                if records_to_insert:
                    from psycopg2.extras import execute_values
                    execute_values(
                        cur,
                        """
                        INSERT INTO crawl_history 
                            (product_id, price, original_price, discount_percent, price_change,
                             previous_price, previous_original_price, previous_discount_percent)
                        VALUES %s
                        """,
                        [
                            (
                                r["product_id"],
                                r["price"],
                                r["original_price"],
                                r["discount_percent"],
                                r["price_change"],
                                r["previous_price"],
                                r["previous_original_price"],
                                r["previous_discount_percent"],
                            )
                            for r in records_to_insert
                        ],
                    )
                    print(f"📊 Price history: {len(records_to_insert)}/{len(valid_products)} products had changes")

    def log_price_history(self, product_id: str, price: float, original_price: float | None = None, discount_percent: int | None = None) -> int | None:
        """
        Log single product price to history CHỈ KHI CÓ THAY ĐỔI.

        Args:
            product_id: Product ID
            price: Current product price
            original_price: Original price before discount
            discount_percent: Discount percentage

        Returns:
            ID of the new history record, or None if no change
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                self._ensure_history_schema(cur)
                # Get previous data
                cur.execute(
                    """
                    SELECT price, original_price, discount_percent FROM crawl_history
                    WHERE product_id = %s
                    ORDER BY crawled_at DESC LIMIT 1
                """,
                    (product_id,),
                )

                row = cur.fetchone()
                
                if row:
                    prev_price, prev_original, prev_discount = row
                    # Check if anything changed
                    price_changed = (float(prev_price) if prev_price else None) != (float(price) if price else None)
                    original_changed = (float(prev_original) if prev_original else None) != (float(original_price) if original_price else None)
                    discount_changed = prev_discount != discount_percent
                    
                    if not (price_changed or original_changed or discount_changed):
                        return None  # No change, skip insert
                    
                    # Calculate price change
                    price_change = None
                    if prev_price is not None:
                        price_change = float(price) - float(prev_price)
                else:
                    price_change = None  # First record
                    prev_price, prev_original, prev_discount = None, None, None

                cur.execute(
                    """
                    INSERT INTO crawl_history (
                        product_id, price, original_price, discount_percent, price_change,
                        previous_price, previous_original_price, previous_discount_percent
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """,
                    (
                        product_id, price, original_price, discount_percent, price_change,
                        prev_price, prev_original, prev_discount
                    ),
                )

                return cur.fetchone()[0]

    def update_category_product_counts(self) -> int:
        """
        Update product_count for all categories based on actual products in DB.
        Match products với categories theo category_url hoặc category_id để đảm bảo tính đúng.
        Call this after saving products.

        Returns:
            Number of categories updated
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Match products với categories theo cả category_url và category_id
                cur.execute(
                    """
                    UPDATE categories c
                    SET product_count = COALESCE((
                        SELECT COUNT(DISTINCT p.id)
                        FROM products p
                        WHERE p.category_url = c.url 
                           OR (c.category_id IS NOT NULL AND p.category_id = c.category_id)
                    ), 0),
                    updated_at = CURRENT_TIMESTAMP
                    WHERE c.is_leaf = TRUE
                """
                )
                return cur.rowcount

    def get_products_by_category(self, category_url: str, limit: int = 100) -> list[dict[str, Any]]:
        """Lấy danh sách products theo category_url"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT product_id, name, url, image_url, sales_count, price,
                           rating_average, crawled_at
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
        """Bulk log crawl history CHỈ KHI CÓ THAY ĐỔI GIÁ.
        
        Tối ưu:
        - Batch lookup previous prices
        - Chỉ insert khi có thay đổi
        - Sử dụng execute_values thay vì COPY (để có thể filter)
        """
        if not products:
            return

        # Filter products with valid data
        valid_products = [
            p for p in products 
            if p.get("product_id") and p.get("price") is not None
        ]
        
        if not valid_products:
            return

        # BATCH LOOKUP: Get all previous prices in one query
        product_ids = [p.get("product_id") for p in valid_products]
        
        cur.execute(
            """
            SELECT DISTINCT ON (product_id) 
                product_id, price, original_price, discount_percent
            FROM crawl_history
            WHERE product_id = ANY(%s)
            ORDER BY product_id, crawled_at DESC
            """,
            (product_ids,),
        )
        
        previous_data = {
            row[0]: {"price": row[1], "original_price": row[2], "discount_percent": row[3]}
            for row in cur.fetchall()
        }

        # Collect records with changes
        records_to_insert = []
        
        for p in valid_products:
            product_id = p.get("product_id")
            current_price = p.get("price")
            current_original_price = p.get("original_price")
            current_discount = p.get("discount_percent")
            
            prev = previous_data.get(product_id)
            
            if prev is None:
                # First time - always insert
                records_to_insert.append((
                    product_id,
                    current_price,
                    current_original_price,
                    current_discount,
                    None,  # price_change
                    None,  # previous_price
                    None,  # previous_original_price
                    None,  # previous_discount_percent
                ))
            else:
                prev_price = float(prev["price"]) if prev["price"] else None
                prev_original = float(prev["original_price"]) if prev["original_price"] else None
                prev_discount = prev["discount_percent"]
                
                current_price_float = float(current_price) if current_price else None
                current_original_float = float(current_original_price) if current_original_price else None
                
                price_changed = prev_price != current_price_float
                original_changed = prev_original != current_original_float
                discount_changed = prev_discount != current_discount
                
                if price_changed or original_changed or discount_changed:
                    price_change = None
                    if prev_price is not None and current_price_float is not None:
                        price_change = current_price_float - prev_price
                    
                    records_to_insert.append((
                        product_id,
                        current_price,
                        current_original_price,
                        current_discount,
                        price_change,
                        prev_price,
                        prev_original,
                        prev_discount,
                    ))

        # Batch insert changed records
        if records_to_insert:
            execute_values(
                cur,
                """
                INSERT INTO crawl_history 
                    (product_id, price, original_price, discount_percent, price_change,
                     previous_price, previous_original_price, previous_discount_percent)
                VALUES %s
                """,
                records_to_insert,
            )
            print(f"📊 Bulk price history: {len(records_to_insert)}/{len(valid_products)} products had changes")

    def _save_products_bulk_copy(
        self, products: list[dict[str, Any]], upsert: bool
    ) -> dict[str, Any]:
        """Thực hiện Bulk Load sử dụng COPY protocol qua Staging Table."""
        if not products:
            return {"saved_count": 0, "inserted_count": 0, "updated_count": 0}

        # Đã loại bỏ các trường: category_path, review_count, description, images,
        # estimated_revenue, price_savings, price_category, value_score, sales_velocity,
        # specifications, popularity_score, discount_amount
        columns = [
            "product_id",
            "name",
            "url",
            "image_url",
            "category_url",
            "category_id",
            "sales_count",
            "price",
            "original_price",
            "discount_percent",
            "rating_average",
            "seller_name",
            "seller_id",
            "seller_is_official",
            "brand",
            "stock_available",
            "stock_quantity",
            "stock_status",
            "shipping",
        ]

        csv_buffer = self._write_dicts_to_csv_buffer(products, columns)

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                self._ensure_products_schema(cur)
                self._ensure_history_schema(cur)

                # Column names to update on conflict (all except product_id)
                update_columns = [c for c in columns if c != "product_id"]

                saved_count = self._bulk_merge_via_staging(
                    cur,
                    "products",
                    "products_staging",
                    columns,
                    csv_buffer,
                    "product_id",
                    update_cols=update_columns,
                    do_nothing=not upsert,
                )

                try:
                    self._bulk_log_product_price_history(cur, products)
                except Exception as e:
                    print(f"⚠️  Failed to log bulk history: {e}")

                return {
                    "saved_count": saved_count,
                    "inserted_count": saved_count if upsert else 0,  # Approx; 0 if insert-only
                    "updated_count": 0,
                }
