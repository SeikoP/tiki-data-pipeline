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
from typing import Any

import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json, execute_values
from psycopg2.pool import SimpleConnectionPool

from ..validate_category_path import fix_products_category_paths


def normalize_category_id(cat_id: str | None) -> str | None:
    """
    Normalize category ID to consistent format: 'c{id}'

    Args:
        cat_id: Category ID in any format ('23570', 'c23570', etc.)

    Returns:
        Normalized category ID ('c23570') or None

    Examples:
        >>> normalize_category_id('23570')
        'c23570'
        >>> normalize_category_id('c23570')
        'c23570'
        >>> normalize_category_id(None)
        None
    """
    if not cat_id:
        return None

    # Convert to string and strip whitespace
    cat_id_str = str(cat_id).strip()

    # Remove all 'c' prefixes and whitespace
    clean_id = cat_id_str.replace("c", "").replace("C", "").strip()

    # Return with 'c' prefix if we have a valid ID
    if clean_id and clean_id.isdigit():
        return f"c{clean_id}"

    return None


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
        self._schemas_ensured = set()

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

                    cur.execute(
                        "SELECT DISTINCT category_id FROM products WHERE category_id IS NOT NULL"
                    )
                    rows = cur.fetchall()
                    for row in rows:
                        cat_id = row[0]
                        if cat_id:
                            # Normalize using the same function as elsewhere for consistency
                            normalized_id = normalize_category_id(cat_id)
                            if normalized_id:
                                used_ids.add(normalized_id)
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
        """Đảm bảo bảng categories và các index tồn tại."""
        if "categories" in self._schemas_ensured:
            return
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
        """)
        self._schemas_ensured.add("categories")

    def _ensure_products_schema(self, cur) -> None:
        """Đảm bảo bảng products và các index tồn tại."""
        if "products" in self._schemas_ensured:
            return
        # Step 1: Create table if not exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id SERIAL PRIMARY KEY,
                product_id VARCHAR(255) UNIQUE,
                name VARCHAR(1000),
                short_name VARCHAR(255),
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
                shipping JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Step 2: Create index
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_products_category_id ON products(category_id);
        """)

        # Step 3: Add metadata columns (for existing tables) - Run separately!
        try:
            cur.execute("""
                ALTER TABLE products ADD COLUMN IF NOT EXISTS data_quality VARCHAR(50);
                ALTER TABLE products ADD COLUMN IF NOT EXISTS last_crawl_status VARCHAR(50);
                ALTER TABLE products ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0;
                ALTER TABLE products ADD COLUMN IF NOT EXISTS last_crawled_at TIMESTAMP;
                ALTER TABLE products ADD COLUMN IF NOT EXISTS last_crawl_attempt_at TIMESTAMP;
                ALTER TABLE products ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMP;
                ALTER TABLE products ADD COLUMN IF NOT EXISTS short_name VARCHAR(255);
            """)
        except Exception as e:
            print(f"⚠️  Note: Some ALTER TABLE statements for products may have been skipped: {e}")

        # Step 4: Create indexes for metadata columns
        try:
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_products_data_quality ON products(data_quality);
                CREATE INDEX IF NOT EXISTS idx_products_retry_count ON products(retry_count);
                CREATE INDEX IF NOT EXISTS idx_products_last_crawled_at ON products(last_crawled_at);
                CREATE INDEX IF NOT EXISTS idx_products_last_crawl_attempt_at ON products(last_crawl_attempt_at);
            """)
        except Exception as e:
            print(f"⚠️  Note: Some index creation for products may have been skipped: {e}")

        # Step 5: Add foreign key constraint if not exists
        cur.execute("""
            SELECT constraint_name FROM information_schema.table_constraints
            WHERE table_name = 'products'
              AND constraint_type = 'FOREIGN KEY'
              AND constraint_name = 'fk_products_category_id'
        """)

        if not cur.fetchone():
            try:
                cur.execute("""
                    ALTER TABLE products
                    ADD CONSTRAINT fk_products_category_id
                    FOREIGN KEY (category_id)
                    REFERENCES categories(category_id)
                    ON DELETE SET NULL
                    ON UPDATE CASCADE
                """)
            except Exception:
                # This is expected and will be handled by migration script
                pass
        self._schemas_ensured.add("products")

    def _ensure_history_schema(self, cur) -> None:
        """Đảm bảo bảng crawl_history và các index tồn tại."""
        if "history" in self._schemas_ensured:
            return
        # Step 1: Create table if not exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crawl_history (
                id SERIAL PRIMARY KEY,
                product_id VARCHAR(255) NOT NULL,

                -- Price tracking
                price DECIMAL(12, 2) NOT NULL,
                original_price DECIMAL(12, 2),
                discount_percent INTEGER,
                discount_amount DECIMAL(12, 2),
                price_change DECIMAL(12, 2),
                price_change_percent DECIMAL(5, 2),
                previous_price DECIMAL(12, 2),
                previous_original_price DECIMAL(12, 2),
                previous_discount_percent INTEGER,

                -- Sales tracking
                sales_count INTEGER,
                sales_change INTEGER,

                -- Promotion tracking
                is_flash_sale BOOLEAN DEFAULT FALSE,

                -- Metadata
                crawl_type VARCHAR(20) DEFAULT 'price_change',

                -- Timestamps
                crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Step 2: Create indexes (safe to run multiple times)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_history_product_id ON crawl_history(product_id);
            CREATE INDEX IF NOT EXISTS idx_history_crawled_at ON crawl_history(crawled_at);
            CREATE INDEX IF NOT EXISTS idx_history_product_latest ON crawl_history(product_id, crawled_at DESC);
            CREATE INDEX IF NOT EXISTS idx_history_crawl_type ON crawl_history(crawl_type);
            CREATE INDEX IF NOT EXISTS idx_history_price_change ON crawl_history(price_change) WHERE price_change IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_history_flash_sale ON crawl_history(is_flash_sale) WHERE is_flash_sale = true;
            CREATE INDEX IF NOT EXISTS idx_history_discount ON crawl_history(discount_percent) WHERE discount_percent > 0;
        """)

        # Step 3: Add missing columns (for existing tables) - CRITICAL: Run separately!
        # This ensures that if the table already existed before code update, it gets the new columns
        try:
            cur.execute("""
                ALTER TABLE crawl_history ADD COLUMN IF NOT EXISTS previous_price DECIMAL(12, 2);
                ALTER TABLE crawl_history ADD COLUMN IF NOT EXISTS previous_original_price DECIMAL(12, 2);
                ALTER TABLE crawl_history ADD COLUMN IF NOT EXISTS previous_discount_percent INTEGER;
                ALTER TABLE crawl_history ADD COLUMN IF NOT EXISTS crawl_type VARCHAR(20) DEFAULT 'price_change';
                ALTER TABLE crawl_history ADD COLUMN IF NOT EXISTS sales_count INTEGER;
                ALTER TABLE crawl_history ADD COLUMN IF NOT EXISTS sales_change INTEGER;
                ALTER TABLE crawl_history ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
                ALTER TABLE crawl_history ADD COLUMN IF NOT EXISTS discount_amount DECIMAL(12, 2);
                ALTER TABLE crawl_history ADD COLUMN IF NOT EXISTS price_change_percent DECIMAL(5, 2);
                ALTER TABLE crawl_history ADD COLUMN IF NOT EXISTS is_flash_sale BOOLEAN DEFAULT FALSE;
            """)
        except Exception as e:
            # Log but don't fail - columns might already exist
            print(f"⚠️  Note: Some ALTER TABLE statements may have been skipped: {e}")

        # Step 4: Add foreign key constraint if not exists
        cur.execute("""
            SELECT constraint_name FROM information_schema.table_constraints
            WHERE table_name = 'crawl_history'
              AND constraint_type = 'FOREIGN KEY'
              AND constraint_name = 'fk_crawl_history_product_id'
        """)

        if not cur.fetchone():
            try:
                cur.execute("""
                    ALTER TABLE crawl_history
                    ADD CONSTRAINT fk_crawl_history_product_id
                    FOREIGN KEY (product_id)
                    REFERENCES products(product_id)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE
                """)
            except Exception:
                # This is expected and will be handled by migration script
                pass
        self._schemas_ensured.add("history")

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
        # 1. Tạo staging table
        cur.execute(sql.SQL("""
            CREATE TEMP TABLE IF NOT EXISTS {staging} (
                LIKE {target} INCLUDING DEFAULTS
            ) ON COMMIT DROP
        """).format(staging=sql.Identifier(staging_table), target=sql.Identifier(target_table)))

        cur.execute(sql.SQL("TRUNCATE {staging}").format(staging=sql.Identifier(staging_table)))

        # 2. Bulk Copy vào staging
        col_names = sql.SQL(",").join(map(sql.Identifier, columns))
        copy_query = sql.SQL(
            "COPY {staging} ({cols}) FROM STDIN WITH (FORMAT CSV, DELIMITER '\t', NULL '', QUOTE '\"')"
        ).format(staging=sql.Identifier(staging_table), cols=col_names)
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
                update_clause=update_parts,
            )

        cur.execute(merge_query)
        return cur.rowcount

    def save_categories(
        self,
        categories: list[dict[str, Any]],
        only_leaf: bool = True,  # Changed: Start defaulting to True to prevent redundant data
        sync_with_products: bool = False,
    ) -> int:
        """
        Lưu danh sách categories vào DB sử dụng bulk processing tối ưu.

        Args:
            categories: Danh sách categories cần lưu
            only_leaf: Nếu True, chỉ lưu categories có is_leaf = true.
                      Nếu False (DEFAULT), lưu tất cả categories including parents.
            sync_with_products: Nếu True, chỉ lưu các categories có match với product
                      (category_id có trong bảng products).

        Returns:
            Số lượng categories đã lưu
        """
        if not categories:
            return 0

        # CRITICAL: Đảm bảo tất cả parent categories được include trong danh sách
        # để có thể build path đầy đủ
        url_to_cat = {cat.get("url"): cat for cat in categories}
        all_urls = set(url_to_cat.keys())

        # Tìm tất cả parent URLs cần thiết (traverse đầy đủ lên đến root)
        needed_parent_urls = set()
        for cat in categories:
            current = cat
            visited = set()
            depth = 0
            while current and depth < 10:
                parent_url = current.get("parent_url")
                if not parent_url or parent_url in visited:
                    break
                visited.add(parent_url)
                if parent_url not in all_urls:
                    needed_parent_urls.add(parent_url)
                # Tìm parent trong url_to_cat
                if parent_url in url_to_cat:
                    current = url_to_cat[parent_url]
                else:
                    break
                depth += 1

        # Nếu có parent URLs cần thiết nhưng chưa có trong danh sách,
        # thử load từ file JSON categories (nếu có)
        if needed_parent_urls:
            print(
                f"📂 Tìm thấy {len(needed_parent_urls)} parent URLs cần thiết nhưng chưa có trong danh sách"
            )
            print(f"   Các parent URLs: {list(needed_parent_urls)[:5]}...")

            # Thử load từ file JSON categories
            json_paths = [
                "/opt/airflow/data/raw/categories_recursive_optimized.json",
                os.path.join(os.getcwd(), "data", "raw", "categories_recursive_optimized.json"),
                os.path.join(
                    os.path.dirname(__file__),
                    "..",
                    "..",
                    "..",
                    "data",
                    "raw",
                    "categories_recursive_optimized.json",
                ),
            ]

            categories_from_json = {}
            for json_path in json_paths:
                if os.path.exists(json_path):
                    try:
                        with open(json_path, encoding="utf-8") as f:
                            all_categories = json.load(f)
                        # Build URL -> category lookup
                        for cat_json in all_categories:
                            url = cat_json.get("url")
                            if url:
                                categories_from_json[url] = cat_json
                        print(
                            f"📂 Loaded {len(categories_from_json)} categories from JSON: {json_path}"
                        )
                        break
                    except Exception as e:
                        print(f"⚠️  Could not load categories JSON from {json_path}: {e}")

            # Include missing parent categories từ JSON
            if categories_from_json:
                added_count = 0
                for parent_url in needed_parent_urls:
                    if parent_url in categories_from_json:
                        parent_cat = categories_from_json[parent_url]
                        if parent_url not in url_to_cat:
                            categories.append(parent_cat)
                            url_to_cat[parent_url] = parent_cat
                            added_count += 1
                            # Traverse tiếp để include parent của parent
                            current = parent_cat
                            depth = 0
                            while current and depth < 5:
                                grandparent_url = current.get("parent_url")
                                if not grandparent_url or grandparent_url in url_to_cat:
                                    break
                                if grandparent_url in categories_from_json:
                                    grandparent_cat = categories_from_json[grandparent_url]
                                    categories.append(grandparent_cat)
                                    url_to_cat[grandparent_url] = grandparent_cat
                                    added_count += 1
                                    current = grandparent_cat
                                else:
                                    break
                                depth += 1

                if added_count > 0:
                    print(f"✅ Đã thêm {added_count} parent categories từ file JSON vào danh sách")
                    # Update all_urls sau khi thêm
                    all_urls = set(url_to_cat.keys())

        # Build URL -> category lookup for path building (sau khi đã include parents)
        url_to_cat = {cat.get("url"): cat for cat in categories}

        # Find all parent URLs to determine is_leaf
        parent_urls = {cat.get("parent_url") for cat in categories if cat.get("parent_url")}

        # Cache để lưu parent categories đã query từ DB
        # Cache này được share giữa tất cả các categories trong cùng một batch
        db_parent_cache = {}

        def build_category_path(cat: dict, cur=None) -> list[str]:
            """Build category path đầy đủ bằng cách traverse lên parent.

            Logic:
            1. Ưu tiên lấy từ url_to_cat (categories đang load trong batch hiện tại)
            2. Nếu không có, query từ DB (và cache lại)
            3. Query đệ quy lên đến root category (parent_url = NULL)
            """
            path = []
            current = cat
            visited = set()
            max_depth = 10  # Giới hạn độ sâu để tránh infinite loop

            depth = 0
            while current and depth < max_depth:
                url = current.get("url")
                if url in visited:
                    # Phát hiện circular reference, dừng lại
                    break
                visited.add(url)
                depth += 1

                # Thêm name vào đầu path
                name = current.get("name", "")
                if name:  # Chỉ thêm nếu name không rỗng
                    path.insert(0, name)

                # Tìm parent
                parent_url = current.get("parent_url")
                if not parent_url:
                    # Đã đến root category, dừng lại
                    break

                # Ưu tiên 1: Lấy từ url_to_cat (categories đang load trong batch hiện tại)
                if parent_url in url_to_cat:
                    current = url_to_cat[parent_url]
                    continue

                # Ưu tiên 2: Lấy từ cache (đã query từ DB trước đó trong cùng batch)
                if parent_url in db_parent_cache:
                    current = db_parent_cache[parent_url]
                    continue

                # Ưu tiên 3: Query từ DB nếu có cursor
                if cur is not None:
                    try:
                        cur.execute(
                            "SELECT name, url, parent_url FROM categories WHERE url = %s",
                            (parent_url,),
                        )
                        row = cur.fetchone()
                        if row:
                            parent_cat = {
                                "name": row[0],
                                "url": row[1],
                                "parent_url": row[2],
                            }
                            # Cache lại để dùng cho các categories khác
                            db_parent_cache[parent_url] = parent_cat
                            current = parent_cat
                            continue
                        else:
                            # Không tìm thấy parent trong DB
                            # Có thể parent chưa được load, nhưng vẫn tiếp tục với path hiện tại
                            # (path đã có ít nhất category hiện tại)
                            break
                    except Exception as e:
                        # Lỗi khi query, log nhưng không fail
                        # Tiếp tục với path hiện tại
                        print(f"⚠️  Warning: Error querying parent {parent_url}: {e}")
                        break
                else:
                    # Không có cursor để query, dừng lại
                    break

            return path

        # Get used category IDs if filtering is enabled
        used_category_ids = set()
        urls_to_keep = set()  # URLs of categories to keep (including parents)

        if sync_with_products:
            used_category_ids = self.get_used_category_ids()
            print(f"🔍 Found {len(used_category_ids)} active categories in products table")

            # IMPORTANT: If no products exist yet, disable filtering to load all categories
            if not used_category_ids:
                print("⚠️  No products found - loading ALL categories (will filter on next run)")
                sync_with_products = False  # Disable filtering
            else:
                # Smart filtering: Find all leaf categories with products + their parent hierarchy
                # Step 1: Find leaf categories that have products
                for cat in categories:
                    cat_id = cat.get("category_id")
                    if not cat_id and cat.get("url"):
                        match = re.search(r"c?(\d+)", cat.get("url", ""))
                        if match:
                            cat_id = match.group(1)
                    cat_id = normalize_category_id(cat_id)

                    # Check if this category has products
                    is_leaf = cat.get("url") not in parent_urls
                    if is_leaf and cat_id:
                        # FIX: Normalize both sides for comparison
                        # used_category_ids already contains normalized IDs (c{id} format)
                        if cat_id in used_category_ids:
                            # Add this leaf category
                            urls_to_keep.add(cat.get("url"))

                            # Step 2: Traverse up to collect ALL parent URLs
                            current = cat
                            visited = set()
                            while current:
                                url = current.get("url")
                                if url in visited:
                                    break
                                visited.add(url)
                                urls_to_keep.add(url)
                                parent_url = current.get("parent_url")
                                current = url_to_cat.get(parent_url) if parent_url else None

                print(f"📂 Keeping {len(urls_to_keep)} categories (leaves + parents)")

        # Build category paths với connection đến DB để query parent nếu cần
        # Trước tiên, load tất cả parent categories từ DB vào url_to_cat để đảm bảo path đầy đủ
        with self.get_connection() as conn:
            with conn.cursor() as build_cur:
                # Tìm tất cả parent URLs cần thiết (đệ quy lên đến root)
                all_parent_urls_needed = set()
                for cat in categories:
                    current = cat
                    visited = set()
                    depth = 0
                    while current and depth < 10:
                        parent_url = current.get("parent_url")
                        if not parent_url or parent_url in visited:
                            break
                        visited.add(parent_url)
                        all_parent_urls_needed.add(parent_url)
                        # Tìm parent của parent trong url_to_cat hoặc DB
                        if parent_url in url_to_cat:
                            current = url_to_cat[parent_url]
                        else:
                            # Cần query từ DB để tiếp tục traverse
                            try:
                                build_cur.execute(
                                    "SELECT name, url, parent_url, category_id, image_url, level FROM categories WHERE url = %s",
                                    (parent_url,),
                                )
                                row = build_cur.fetchone()
                                if row:
                                    current = {
                                        "name": row[0],
                                        "url": row[1],
                                        "parent_url": row[2],
                                        "category_id": row[3],
                                        "image_url": row[4],
                                        "level": row[5],
                                    }
                                    # Thêm vào url_to_cat và cache
                                    url_to_cat[row[1]] = current
                                    db_parent_cache[row[1]] = current
                                else:
                                    break
                            except Exception:
                                break
                        depth += 1

                # Load tất cả parent categories từ DB nếu chưa có trong url_to_cat (batch query)
                if all_parent_urls_needed:
                    missing_parent_urls = [
                        url for url in all_parent_urls_needed if url not in url_to_cat
                    ]

                    if missing_parent_urls:
                        # Query batch để load parent categories từ DB
                        # Query batch để load parent categories từ DB
                        query = sql.SQL("""
                            SELECT name, url, parent_url, category_id, image_url, level
                            FROM categories
                            WHERE url IN ({placeholders})
                        """).format(
                            placeholders=sql.SQL(",").join(
                                [sql.Placeholder()] * len(missing_parent_urls)
                            )
                        )
                        try:
                            build_cur.execute(query, missing_parent_urls)
                            loaded_count = 0
                            for row in build_cur.fetchall():
                                parent_cat = {
                                    "name": row[0],
                                    "url": row[1],
                                    "parent_url": row[2],
                                    "category_id": row[3],
                                    "image_url": row[4],
                                    "level": row[5],
                                }
                                # Thêm vào url_to_cat để dùng khi build path
                                url_to_cat[row[1]] = parent_cat
                                # Cache lại để dùng cho các categories khác
                                db_parent_cache[row[1]] = parent_cat
                                loaded_count += 1

                            if loaded_count > 0:
                                print(
                                    f"📂 Loaded {loaded_count} parent categories from DB for path building"
                                )
                        except Exception as e:
                            print(f"⚠️  Warning: Could not load all parent categories from DB: {e}")

                prepared_categories = []
                for cat in categories:
                    # Extract and normalize category_id
                    cat_id = cat.get("category_id")
                    if not cat_id and cat.get("url"):
                        # Extract from URL
                        match = re.search(r"c?(\d+)", cat.get("url", ""))
                        if match:
                            cat_id = match.group(1)

                    # Normalize to 'c{id}' format
                    cat_id = normalize_category_id(cat_id)

                    # Build category_path đầy đủ (có thể query parent từ DB nếu cần)
                    category_path = build_category_path(cat, cur=build_cur)
                    root_name = category_path[0] if category_path else ""
                    is_leaf = cat.get("url") not in parent_urls

                    # Nếu only_leaf=True, chỉ lưu categories có is_leaf=True
                    if only_leaf and not is_leaf:
                        continue

                    # Nếu sync_with_products=True, chỉ lưu categories trong urls_to_keep
                    if sync_with_products:
                        if cat.get("url") not in urls_to_keep:
                            continue

                    # Extract level_1 đến level_5 từ category_path để thể hiện rõ theo từng độ sâu
                    level_1 = category_path[0] if len(category_path) > 0 else None
                    level_2 = category_path[1] if len(category_path) > 1 else None
                    level_3 = category_path[2] if len(category_path) > 2 else None
                    level_4 = category_path[3] if len(category_path) > 3 else None
                    level_5 = category_path[4] if len(category_path) > 4 else None

                    # Tính level từ độ dài category_path (đảm bảo chính xác)
                    # Level bắt đầu từ 1 (level 1 = root category)
                    calculated_level = len(category_path) if category_path else 0

                    prepared_categories.append(
                        {
                            "category_id": cat_id,
                            "name": cat.get("name"),
                            "url": cat.get("url"),
                            "image_url": cat.get("image_url"),
                            "parent_url": cat.get("parent_url"),
                            "level": calculated_level,  # Dùng level tính từ path thay vì từ data gốc
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

        # Deduplicate by category_id to prevent UniqueViolation
        # Keep the entry with the shortest URL (usually the canonical one)
        unique_categories_map = {}
        for cat in prepared_categories:
            cat_id = cat.get("category_id")
            if not cat_id:
                continue

            if cat_id in unique_categories_map:
                existing = unique_categories_map[cat_id]
                # If current URL is shorter or looks "better", replace
                # Also prefer non-empty names
                curr_url_len = len(cat.get("url", ""))
                exist_url_len = len(existing.get("url", ""))

                replace = False
                if curr_url_len < exist_url_len:
                    replace = True
                elif curr_url_len == exist_url_len:
                    # Tie-breaker: prefer longer name (more descriptive)
                    if len(cat.get("name", "")) > len(existing.get("name", "")):
                        replace = True

                if replace:
                    unique_categories_map[cat_id] = cat
            else:
                unique_categories_map[cat_id] = cat

        final_categories = list(unique_categories_map.values())

        csv_buffer = self._write_dicts_to_csv_buffer(final_categories, columns)

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
                # ONLY if only_leaf is False (we want parents in that case)
                if not only_leaf:
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

                # CRITICAL: If only_leaf=True, ensure NO non-leaf categories remain
                if only_leaf:
                    self.cleanup_redundant_categories(cur)

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

        # Normalize category_id for all products
        for product in products:
            if product.get("category_id"):
                product["category_id"] = normalize_category_id(product["category_id"])

        # Tự động fix category_path để đảm bảo parent category ở index 0
        try:
            products = fix_products_category_paths(products, hierarchy_map=None, auto_fix=True)
        except Exception as e:
            # Log nhưng không fail nếu fix có lỗi
            print(f"⚠️  Warning: Could not auto-fix category_path: {e}")

        # Auto-sync: Ensure all categories from products exist in categories table BEFORE saving products
        # This prevents Foreign Key constraint violations (Referential Integrity)
        try:
            self._ensure_categories_from_products(products)
        except Exception as e:
            print(f"⚠️  Failed to pre-sync missing categories: {e}")

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
        existing_products_info: dict[str, dict[str, Any]] = {}
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
                                    f"SELECT product_id, brand, seller_name FROM products WHERE product_id IN ({placeholders})",
                                    batch_ids,
                                )
                                for row in cur.fetchall():
                                    existing_products_info[row[0]] = {
                                        "brand": row[1],
                                        "seller_name": row[2],
                                    }
            except Exception as e:
                # Nếu không lấy được, tiếp tục với upsert bình thường
                print(f"⚠️  Không thể lấy danh sách products đã có: {e}")

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Chia nhỏ thành batches
                for i in range(0, len(products), batch_size):
                    batch = products[i : i + batch_size]
                    try:
                        # Chuẩn bị data cho batch insert (core fields only)
                        # Note: metadata columns (data_quality, last_crawl_status, retry_count)
                        # are optional and may not exist in all DB setups
                        values = [
                            (
                                product.get("product_id"),
                                product.get("name"),
                                product.get("short_name"),
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
                                # Brand and stock (only stock_available)
                                product.get("brand"),
                                product.get("stock_available"),
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
                                and p.get("product_id") not in existing_products_info
                            )
                            batch_updated = len(batch) - batch_inserted

                            # Batch INSERT ... ON CONFLICT UPDATE (core fields only)
                            execute_values(
                                cur,
                                """
                                INSERT INTO products
                                    (product_id, name, short_name, url, image_url, category_url, category_id, sales_count,
                                     price, original_price, discount_percent, rating_average,
                                     seller_name, seller_id, seller_is_official, brand,
                                     stock_available, shipping)
                                VALUES %s
                                ON CONFLICT (product_id)
                                DO UPDATE SET
                                    name = COALESCE(NULLIF(EXCLUDED.name, ''), products.name),
                                    short_name = COALESCE(NULLIF(EXCLUDED.short_name, ''), products.short_name),
                                    url = COALESCE(NULLIF(EXCLUDED.url, ''), products.url),
                                    image_url = COALESCE(NULLIF(EXCLUDED.image_url, ''), products.image_url),
                                    category_url = COALESCE(NULLIF(EXCLUDED.category_url, ''), products.category_url),
                                    category_id = COALESCE(NULLIF(EXCLUDED.category_id, ''), products.category_id),
                                    sales_count = COALESCE(EXCLUDED.sales_count, products.sales_count),
                                    price = COALESCE(EXCLUDED.price, products.price),
                                    original_price = COALESCE(EXCLUDED.original_price, products.original_price),
                                    discount_percent = COALESCE(EXCLUDED.discount_percent, products.discount_percent),
                                    rating_average = COALESCE(EXCLUDED.rating_average, products.rating_average),
                                    seller_name = COALESCE(NULLIF(EXCLUDED.seller_name, ''), products.seller_name),
                                    seller_id = COALESCE(NULLIF(EXCLUDED.seller_id, ''), products.seller_id),
                                    seller_is_official = COALESCE(EXCLUDED.seller_is_official, products.seller_is_official),
                                    brand = COALESCE(NULLIF(EXCLUDED.brand, ''), products.brand),
                                    stock_available = COALESCE(EXCLUDED.stock_available, products.stock_available),
                                    shipping = COALESCE(EXCLUDED.shipping, products.shipping),
                                    updated_at = CURRENT_TIMESTAMP
                                """,
                                values,
                            )

                            # Cập nhật existing_products_info sau khi insert
                            for product in batch:
                                product_id = product.get("product_id")
                                if product_id and product_id not in existing_products_info:
                                    existing_products_info[product_id] = {
                                        "brand": product.get("brand"),
                                        "seller_name": product.get("seller_name"),
                                    }

                            inserted_count += batch_inserted
                            updated_count += batch_updated
                        else:
                            # Batch INSERT, bỏ qua nếu đã tồn tại
                            execute_values(
                                cur,
                                """
                                INSERT INTO products
                                    (product_id, name, url, image_url, category_url, category_id, sales_count,
                                     price, original_price, discount_percent, rating_average,
                                     seller_name, seller_id, seller_is_official, brand,
                                     stock_available, shipping)
                                VALUES %s
                                ON CONFLICT (product_id) DO NOTHING
                                """,
                                values,
                            )

                        saved_count += len(batch)
                    except Exception as e:
                        if batch_size > 1:
                            print(
                                f"⚠️  Lỗi khi lưu batch products ({len(batch)} items): {e}. Thử lưu từng product..."
                            )
                            # Thử lưu từng product một nếu batch fail
                            for product in batch:
                                try:
                                    # Gọi lại chính nó nhưng với batch_size=1 để kích hoạt fallback logic này
                                    # Hoặc gọi thẳng logic insert đơn lẻ bên dưới
                                    single_result = self.save_products(
                                        [product], upsert=upsert, batch_size=1
                                    )
                                    saved_count += single_result.get("saved_count", 0)
                                    inserted_count += single_result.get("inserted_count", 0)
                                    updated_count += single_result.get("updated_count", 0)
                                except Exception as inner_e:
                                    print(
                                        f"❌ Failed to save single product {product.get('product_id')}: {inner_e}"
                                    )
                                    continue
                        else:
                            raise e

        try:
            # Use the robust batch history logging method - OUTSIDE of the main products transaction
            self._log_batch_crawl_history(products, previous_state=existing_products_info)
        except Exception as e:
            print(f"⚠️  Failed to log crawl history: {e}")

        return {
            "saved_count": saved_count,
            "inserted_count": inserted_count,
            "updated_count": updated_count,
        }

    def _ensure_categories_from_products(self, products: list[dict[str, Any]]) -> int:
        """
        Tự động tạo/update missing categories từ products với đầy đủ dữ liệu từ JSON file.

        Khi crawl products, category_id và category_url có thể chưa tồn tại trong bảng categories.
        Method này sẽ:
        1. Load categories JSON file để lấy đầy đủ thông tin
        2. Tìm categories cần thêm dựa trên products
        3. Insert/Update với đầy đủ dữ liệu (name, levels, path, etc.)

        Returns:
            Số categories mới được thêm/update
        """
        if not products:
            return 0

        # Collect unique category URLs from products
        product_category_urls = {}
        for p in products:
            cat_id = p.get("category_id")
            cat_url = p.get("category_url")

            if not cat_id or not cat_url:
                continue

            # Normalize category_id using the standardized function
            cat_id = normalize_category_id(cat_id)

            if cat_url not in product_category_urls:
                product_category_urls[cat_url] = cat_id

        if not product_category_urls:
            return 0

        # Load categories JSON file to get full data
        import json
        import os

        json_paths = [
            "/opt/airflow/data/raw/categories_recursive_optimized.json",
            os.path.join(os.getcwd(), "data", "raw", "categories_recursive_optimized.json"),
        ]

        categories_from_json = {}
        for json_path in json_paths:
            if os.path.exists(json_path):
                try:
                    with open(json_path, encoding="utf-8") as f:
                        all_categories = json.load(f)
                    # Build URL -> category lookup
                    for cat in all_categories:
                        url = cat.get("url")
                        if url:
                            categories_from_json[url] = cat
                    print(
                        f"📂 Loaded {len(categories_from_json)} categories from JSON for enrichment"
                    )
                    break
                except Exception as e:
                    print(f"⚠️  Could not load categories JSON: {e}")

        # Find categories that exist in JSON and match product URLs
        categories_to_add = []
        for cat_url, cat_id in product_category_urls.items():
            if cat_url in categories_from_json:
                # Use full data from JSON
                cat_data = categories_from_json[cat_url]

                # Build category_path if not present
                category_path = cat_data.get("category_path", [])
                if not category_path:
                    # Try to build from breadcrumbs or parent chain
                    category_path = [cat_data.get("name", cat_id)]

                categories_to_add.append(
                    {
                        "category_id": cat_id,
                        "name": cat_data.get("name", cat_id),
                        "url": cat_url,
                        "image_url": cat_data.get("image_url"),
                        "parent_url": cat_data.get("parent_url"),
                        "level": cat_data.get("level", 0),
                        "category_path": category_path,
                        "level_1": category_path[0] if len(category_path) > 0 else None,
                        "level_2": category_path[1] if len(category_path) > 1 else None,
                        "level_3": category_path[2] if len(category_path) > 2 else None,
                        "level_4": category_path[3] if len(category_path) > 3 else None,
                        "level_5": category_path[4] if len(category_path) > 4 else None,
                        "root_category_name": category_path[0] if category_path else None,
                        "is_leaf": True,  # Categories from products are leaf categories
                    }
                )
            else:
                # Fallback: category not in JSON, use minimal data but with useful name
                # Extract name from URL slug
                import re

                slug_match = re.search(r"tiki\.vn/([^/]+)/c\d+", cat_url)
                name_from_slug = (
                    slug_match.group(1).replace("-", " ").title() if slug_match else cat_id
                )

                categories_to_add.append(
                    {
                        "category_id": cat_id,
                        "name": name_from_slug,
                        "url": cat_url,
                        "is_leaf": True,
                    }
                )
                print(
                    f"⚠️  Category {cat_id} not found in JSON, using name from URL: {name_from_slug}"
                )

        if not categories_to_add:
            return 0

        added_count = 0
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                self._ensure_categories_schema(cur)

                for cat_info in categories_to_add:
                    # Use INSERT ... ON CONFLICT DO UPDATE to update if exists
                    cur.execute(
                        """
                        INSERT INTO categories (
                            category_id, name, url, image_url, parent_url, level,
                            category_path, level_1, level_2, level_3, level_4, level_5,
                            root_category_name, is_leaf, product_count
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0)
                        ON CONFLICT (url) DO UPDATE SET
                            name = COALESCE(NULLIF(EXCLUDED.name, categories.category_id), categories.name),
                            image_url = COALESCE(EXCLUDED.image_url, categories.image_url),
                            parent_url = COALESCE(EXCLUDED.parent_url, categories.parent_url),
                            level = COALESCE(EXCLUDED.level, categories.level),
                            category_path = COALESCE(EXCLUDED.category_path, categories.category_path),
                            level_1 = COALESCE(EXCLUDED.level_1, categories.level_1),
                            level_2 = COALESCE(EXCLUDED.level_2, categories.level_2),
                            level_3 = COALESCE(EXCLUDED.level_3, categories.level_3),
                            level_4 = COALESCE(EXCLUDED.level_4, categories.level_4),
                            level_5 = COALESCE(EXCLUDED.level_5, categories.level_5),
                            root_category_name = COALESCE(EXCLUDED.root_category_name, categories.root_category_name),
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        (
                            cat_info.get("category_id"),
                            cat_info.get("name"),
                            cat_info.get("url"),
                            cat_info.get("image_url"),
                            cat_info.get("parent_url"),
                            cat_info.get("level"),
                            (
                                Json(cat_info.get("category_path"))
                                if cat_info.get("category_path")
                                else None
                            ),
                            cat_info.get("level_1"),
                            cat_info.get("level_2"),
                            cat_info.get("level_3"),
                            cat_info.get("level_4"),
                            cat_info.get("level_5"),
                            cat_info.get("root_category_name"),
                            cat_info.get("is_leaf", True),
                        ),
                    )
                    if cur.rowcount > 0:
                        added_count += 1

        if added_count > 0:
            print(f"📂 Auto-added/updated {added_count} categories from products with full data")

        return added_count

    def _log_batch_crawl_history(
        self, products: list[dict[str, Any]], previous_state: dict[str, dict[str, Any]] | None = None
    ) -> None:
        """
        Log crawl history for ALL products (không chỉ khi có thay đổi giá).

        Tối ưu:
        - Sử dụng batch lookup để lấy giá cũ (thay vì query từng product)
        - INSERT cho mọi lần crawl với crawl_type: 'price_change', 'no_change', 'data_improvement'
        - Populate metadata fields từ _metadata

        Args:
            products: List các product đã crawl
            previous_state: Dict chứa thông tin brand/seller cũ từ bảng products (để detect data improvement)
        """
        if not products:
            return

        # Filter products with valid price
        valid_products = [p for p in products if p.get("product_id") and p.get("price") is not None]

        if not valid_products:
            return

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # CRITICAL: Ensure schema is up-to-date BEFORE any queries
                self._ensure_history_schema(cur)
                conn.commit()  # Commit schema changes first

                # BATCH LOOKUP: Get all previous prices AND sales in one query
                product_ids = [p.get("product_id") for p in valid_products]

                # Use DISTINCT ON to get latest data per product efficiently
                cur.execute(
                    """
                    SELECT DISTINCT ON (product_id)
                        product_id, price, original_price, discount_percent, sales_count
                    FROM crawl_history
                    WHERE product_id = ANY(%s)
                    ORDER BY product_id, crawled_at DESC
                    """,
                    (product_ids,),
                )

                # Build lookup dict: product_id -> (price, original_price, discount_percent, sales_count)
                previous_data = {
                    row[0]: {
                        "price": row[1],
                        "original_price": row[2],
                        "discount_percent": row[3],
                        "sales_count": row[4],
                    }
                    for row in cur.fetchall()
                }

                # Collect ALL products for history insert (not just changed ones)
                records_to_insert = []

                for p in valid_products:
                    product_id = p.get("product_id")
                    current_price = p.get("price")
                    current_original_price = p.get("original_price")
                    current_discount = p.get("discount_percent")
                    current_sales = p.get("sales_count")  # NEW

                    prev = previous_data.get(product_id)

                    # Determine crawl_type and price_change
                    if prev is None:
                        # First time crawl
                        crawl_type = "price_change"  # First crawl counts as change
                        price_change = None
                        price_change_percent = None  # FIX: Initialize to avoid NameError
                        prev_price = None
                        prev_original = None
                        prev_discount = None
                        prev_sales = None
                        sales_change = None
                    else:
                        # Check if anything changed
                        prev_price = float(prev["price"]) if prev["price"] else None
                        prev_original = (
                            float(prev["original_price"]) if prev["original_price"] else None
                        )
                        prev_discount = prev["discount_percent"]
                        prev_sales = prev.get("sales_count")

                        current_price_float = float(current_price) if current_price else None
                        current_original_float = (
                            float(current_original_price) if current_original_price else None
                        )

                        # Determine if price changed
                        price_changed = prev_price != current_price_float
                        original_changed = prev_original != current_original_float
                        discount_changed = prev_discount != current_discount

                        if price_changed or original_changed or discount_changed:
                            crawl_type = "price_change"
                            price_change = None
                            price_change_percent = None
                            if prev_price is not None and current_price_float is not None:
                                price_change = current_price_float - prev_price
                                # Calculate percentage change
                                if prev_price > 0:
                                    price_change_percent = round(
                                        (price_change / prev_price) * 100, 2
                                    )
                        else:
                            crawl_type = "no_change"
                            price_change = None
                            price_change_percent = None

                        # Calculate sales_change
                        sales_change = None
                        if prev_sales is not None and current_sales is not None:
                            sales_change = current_sales - prev_sales

                    # Calculate discount_amount (tiền giảm giá)
                    discount_amount = None
                    if current_original_price and current_price:
                        discount_amount = float(current_original_price) - float(current_price)

                    # Determine if flash sale (giảm >= 30% hoặc giảm >= 100k)
                    is_flash_sale = False
                    if current_discount and current_discount >= 30:
                        is_flash_sale = True
                    elif discount_amount and discount_amount >= 100000:
                        is_flash_sale = True

                    # DECISION: Should we log this record?
                    # Log if:
                    # 1. First time crawl (prev is None -> crawl_type='price_change')
                    # 2. Price/Discount changed (crawl_type='price_change')
                    # 3. Sales changed (sales_change != 0)
                    # 4. Data Quality improved (brand/seller was NULL but now has value)
                    should_log = False

                    # Check for data improvement
                    data_improved = False
                    if previous_state and product_id in previous_state:
                        old_brand = previous_state[product_id].get("brand")
                        old_seller = previous_state[product_id].get("seller_name")
                        
                        new_brand = p.get("brand")
                        new_seller = p.get("seller_name")
                        
                        # Detect improvement: was NULL/empty but now has content
                        brand_found = not old_brand and new_brand
                        seller_found = not old_seller and new_seller
                        
                        if brand_found or seller_found:
                            data_improved = True

                    if crawl_type == "price_change":
                        should_log = True
                    elif sales_change and sales_change != 0:
                        should_log = True
                        crawl_type = "sales_change"  # Update type for clarity
                    elif data_improved:
                        should_log = True
                        crawl_type = "data_improvement"

                    if should_log:
                        records_to_insert.append(
                            {
                                "product_id": product_id,
                                "price": current_price,
                                "original_price": current_original_price,
                                "discount_percent": current_discount,
                                "discount_amount": discount_amount,
                                "price_change": price_change,
                                "price_change_percent": price_change_percent,
                                "previous_price": prev_price,
                                "previous_original_price": prev_original,
                                "previous_discount_percent": prev_discount,
                                "sales_count": current_sales,
                                "sales_change": sales_change,
                                "is_flash_sale": is_flash_sale,
                                "crawl_type": crawl_type,
                            }
                        )

                # Batch insert ALL records (changed and unchanged)
                if records_to_insert:
                    from psycopg2.extras import execute_values

                    try:
                        execute_values(
                            cur,
                            """
                            INSERT INTO crawl_history
                                (product_id, price, original_price, discount_percent, discount_amount,
                                 price_change, price_change_percent,
                                 previous_price, previous_original_price, previous_discount_percent,
                                 sales_count, sales_change, is_flash_sale, crawl_type)
                            VALUES %s
                            """,
                            [
                                (
                                    r["product_id"],
                                    r["price"],
                                    r["original_price"],
                                    r["discount_percent"],
                                    r["discount_amount"],  # NEW
                                    r["price_change"],
                                    r["price_change_percent"],  # NEW
                                    r["previous_price"],
                                    r["previous_original_price"],
                                    r["previous_discount_percent"],
                                    r["sales_count"],
                                    r["sales_change"],
                                    r["is_flash_sale"],  # NEW
                                    r["crawl_type"],
                                )
                                for r in records_to_insert
                            ],
                        )

                        # Simple logging with crawl_type breakdown
                        type_counts = {}
                        for r in records_to_insert:
                            ct = r.get("crawl_type", "unknown")
                            type_counts[ct] = type_counts.get(ct, 0) + 1

                        type_str = ", ".join(f"{k}: {v}" for k, v in type_counts.items())
                        print(f"📊 Crawl history: {len(records_to_insert)} records ({type_str})")
                        conn.commit()  # Commit history inserts
                    except Exception as e:
                        import traceback

                        error_trace = traceback.format_exc()
                        print(f"❌ Error inserting crawl history: {e}")
                        print(f"Error type: {type(e).__name__}")
                        print(f"Error trace: {error_trace}")
                        if "foreign key" in str(e).lower() or "does not exist" in str(e).lower():
                            print(
                                "💡 FK constraint error or missing column: ensure products exist and schema is updated"
                            )
                            # Try to get which product IDs are missing
                            try:
                                missing_ids = []
                                for r in records_to_insert[:10]:  # Check first 10
                                    cur.execute(
                                        "SELECT 1 FROM products WHERE product_id = %s",
                                        (r["product_id"],),
                                    )
                                    if not cur.fetchone():
                                        missing_ids.append(r["product_id"])
                                if missing_ids:
                                    print(f"⚠️  Missing product_ids (first 5): {missing_ids[:5]}")
                                else:
                                    print(
                                        "💡 All product_ids exist - likely a schema mismatch issue"
                                    )
                                    print("💡 Run _ensure_history_schema() to add missing columns")
                            except Exception as inner_e:
                                print(f"⚠️  Could not check missing products: {inner_e}")
                        conn.rollback()  # Rollback on error
                        # Don't re-raise - log the error but continue
                        print("⚠️  Continuing without crawl history (will retry on next crawl)")

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
                # Sử dụng UPDATE FROM để tối ưu hóa thay vì subquery cho từng dòng
                cur.execute("""
                    UPDATE categories c
                    SET product_count = counts.cnt,
                        updated_at = CURRENT_TIMESTAMP
                    FROM (
                        SELECT c.url, COUNT(DISTINCT p.id) as cnt
                        FROM categories c
                        LEFT JOIN products p ON p.category_url = c.url 
                           OR (c.category_id IS NOT NULL AND p.category_id = c.category_id)
                        WHERE c.is_leaf = TRUE
                        GROUP BY c.url
                    ) counts
                    WHERE c.url = counts.url AND c.is_leaf = TRUE;
                """)
                return cur.rowcount

    def cleanup_redundant_categories(self, cur=None, dry_run: bool = False) -> int:
        """
        Xóa các categories không phải là leaf (có categories con)
        hoặc các categories không có sản phẩm nếu cần thiết.

        Method này đảm bảo bảng categories chỉ chứa dữ liệu 'leaf' thực sự.

        Args:
            cur: Database cursor
            dry_run: Nếu True, chỉ in ra số lượng sẽ xóa mà không thực hiện.
        """
        select_query = """
            SELECT count(*) FROM categories
            WHERE url IN (
                SELECT DISTINCT parent_url
                FROM categories
                WHERE parent_url IS NOT NULL
            )
            OR is_leaf = FALSE
        """

        delete_query = """
            DELETE FROM categories
            WHERE url IN (
                SELECT DISTINCT parent_url
                FROM categories
                WHERE parent_url IS NOT NULL
            )
            OR is_leaf = FALSE
        """

        if cur:
            if dry_run:
                cur.execute(select_query)
                count = cur.fetchone()[0]
                print(f"🔍 [DRY RUN] Cleanup: Would remove {count} non-leaf categories from DB")
                return 0

            cur.execute(delete_query)
            count = cur.rowcount
            if count > 0:
                print(f"🧹 Cleanup: Removed {count} non-leaf categories from DB")
            return count
        else:
            with self.get_connection() as conn:
                with conn.cursor() as standalone_cur:
                    if dry_run:
                        standalone_cur.execute(select_query)
                        count = standalone_cur.fetchone()[0]
                        print(
                            f"🔍 [DRY RUN] Cleanup: Would remove {count} non-leaf categories from DB"
                        )
                        return 0

                    standalone_cur.execute(delete_query)
                    count = standalone_cur.rowcount
                    if count > 0:
                        print(f"🧹 Cleanup: Removed {count} non-leaf categories from DB")
                    return count

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
                cur.execute("""
                    SELECT
                        0 as total_categories,
                        (SELECT COUNT(*) FROM products) as total_products,
                        (SELECT COUNT(DISTINCT category_url) FROM products) as categories_with_products,
                        (SELECT AVG(sales_count) FROM products WHERE sales_count IS NOT NULL) as avg_sales_count,
                        (SELECT MAX(crawled_at) FROM products) as last_crawl_time
                    """)
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

    def cleanup_incomplete_products(
        self,
        require_seller: bool = True,
        require_brand: bool = True,
        require_rating: bool = False,
    ) -> dict[str, int]:
        """
        Delete products with missing required fields (seller, brand, and/or rating).
        Run this BEFORE each crawl to allow re-crawling of incomplete data.

        Args:
            require_seller: If True, delete products where seller_name IS NULL or empty
            require_brand: If True, delete products where brand IS NULL or empty
            require_rating: If True, delete products where rating_average IS NULL

        Returns:
            dict with keys: deleted_count, deleted_no_seller, deleted_no_brand, deleted_both
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Build WHERE clause based on requirements
                conditions = []
                if require_seller:
                    conditions.append("(seller_name IS NULL OR seller_name = '')")
                if require_brand:
                    conditions.append("(brand IS NULL OR brand = '')")
                if require_rating:
                    conditions.append("rating_average IS NULL")

                if not conditions:
                    return {
                        "deleted_count": 0,
                        "deleted_no_seller": 0,
                        "deleted_no_brand": 0,
                        "deleted_no_rating": 0,
                        "deleted_both": 0,
                    }

                where_clause = " OR ".join(conditions)

                # Get counts before deletion for detailed reporting
                # Only calculate stats if both requirements are enabled
                deleted_no_seller = 0
                deleted_no_brand = 0
                deleted_no_rating = 0
                deleted_both = 0

                if require_seller and require_brand:
                    # Calculate detailed stats: missing seller only, missing brand only, missing both, missing rating
                    stats_query = f"""
                        SELECT
                            COUNT(*) FILTER (WHERE (seller_name IS NULL OR seller_name = '') AND NOT (brand IS NULL OR brand = '')) as no_seller,
                            COUNT(*) FILTER (WHERE NOT (seller_name IS NULL OR seller_name = '') AND (brand IS NULL OR brand = '')) as no_brand,
                            COUNT(*) FILTER (WHERE (seller_name IS NULL OR seller_name = '') AND (brand IS NULL OR brand = '')) as both_missing,
                            COUNT(*) FILTER (WHERE rating_average IS NULL) as no_rating
                        FROM products
                        WHERE {where_clause}
                    """
                    cur.execute(stats_query)
                    stats_row = cur.fetchone()
                    deleted_no_seller = stats_row[0] if stats_row else 0
                    deleted_no_brand = stats_row[1] if stats_row else 0
                    deleted_both = stats_row[2] if stats_row else 0
                    deleted_no_rating = stats_row[3] if stats_row and require_rating else 0
                elif require_seller:
                    # Only seller required - count all as no_seller
                    cur.execute(f"SELECT COUNT(*) FROM products WHERE {conditions[0]}")
                    deleted_no_seller = cur.fetchone()[0] or 0
                elif require_brand:
                    # Only brand required - count all as no_brand
                    cur.execute(f"SELECT COUNT(*) FROM products WHERE {conditions[0]}")
                    deleted_no_brand = cur.fetchone()[0] or 0
                elif require_rating:
                    # Only rating required - count all as no_rating
                    cur.execute(f"SELECT COUNT(*) FROM products WHERE {conditions[0]}")
                    deleted_no_rating = cur.fetchone()[0] or 0

                # Delete products matching criteria
                delete_query = f"""
                    DELETE FROM products
                    WHERE {where_clause}
                """
                cur.execute(delete_query)
                deleted_count = cur.rowcount

                if deleted_count > 0:
                    reason_parts = []
                    if require_seller:
                        reason_parts.append(f"{deleted_no_seller} without seller")
                    if require_brand:
                        reason_parts.append(f"{deleted_no_brand} without brand")
                    if require_rating:
                        reason_parts.append(f"{deleted_no_rating} without rating")
                    if require_seller and require_brand:
                        reason_parts.append(f"{deleted_both} missing both/multiple")

                    reason_str = ", ".join(reason_parts)
                    print(f"🧹 Cleaned up {deleted_count} incomplete products ({reason_str})")

                return {
                    "deleted_count": deleted_count,
                    "deleted_no_seller": deleted_no_seller if require_seller else 0,
                    "deleted_no_brand": deleted_no_brand if require_brand else 0,
                    "deleted_no_rating": deleted_no_rating if require_rating else 0,
                    "deleted_both": deleted_both if (require_seller and require_brand) else 0,
                }

    def cleanup_old_history(
        self, archive_months: int = 6, delete_months: int = 12
    ) -> dict[str, Any]:
        """
        Cleanup old crawl history records:
        - Archive records older than `archive_months`
        - Delete records older than `delete_months`
        - Vacuum table to reclaim space

        Returns:
            Dict with statistics on archived and deleted records
        """
        from datetime import datetime, timedelta

        archive_date = datetime.now() - timedelta(days=archive_months * 30)
        delete_date = datetime.now() - timedelta(days=delete_months * 30)

        result = {
            "archived_count": 0,
            "deleted_count": 0,
            "status": "success",
        }

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # 1. Create archive table if not exists
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS crawl_history_archive (
                        LIKE crawl_history INCLUDING ALL
                    );
                    """)

                # 2. Archive records
                cur.execute(
                    """
                    INSERT INTO crawl_history_archive
                    SELECT * FROM crawl_history
                    WHERE crawled_at >= %s AND crawled_at < %s
                    ON CONFLICT DO NOTHING
                    """,
                    (delete_date, archive_date),
                )
                result["archived_count"] = cur.rowcount

                # 3. Delete old records
                cur.execute(
                    "DELETE FROM crawl_history WHERE crawled_at < %s",
                    (delete_date,),
                )
                result["deleted_count"] = cur.rowcount

                conn.commit()

                # 4. Vacuum (cannot be inside a transaction block in some versions,
                # but with autocommit=True it works. SimpleConnectionPool usually uses autocommit=False)
                # We'll skip VACUUM here to avoid transaction issues in Airflow tasks,
                # as VACUUM is often managed by Autovacuum anyway.

        return result

    def cleanup_products_without_seller(self) -> int:
        """
        DEPRECATED: Use cleanup_incomplete_products() instead.

        Delete products where seller_name is NULL.
        Kept for backward compatibility.

        Returns:
            Number of products deleted
        """
        result = self.cleanup_incomplete_products(require_seller=True, require_brand=False)
        return result["deleted_count"]

    def cleanup_orphan_categories(self) -> int:
        """
        Remove categories that don't have any matching products.
        This ensures categories table stays clean after product crawls.

        Only removes leaf categories (is_leaf = true) that have no products.
        Parent categories are kept to maintain hierarchy.

        Returns:
            Number of categories deleted
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM categories
                    WHERE is_leaf = true
                    AND NOT EXISTS (
                        SELECT 1 FROM products p
                        WHERE p.category_id = categories.category_id
                    )
                """)
                deleted_count = cur.rowcount

                if deleted_count > 0:
                    print(f"🧹 Cleaned up {deleted_count} orphan categories")

                return deleted_count

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
            "short_name",
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

        # Log history OUTSIDE of the connection context to avoid deadlock/nested transaction issues
        try:
            # Use the robust batch history logging method
            self._log_batch_crawl_history(products)
        except Exception as e:
            print(f"⚠️  Failed to log bulk history: {e}")

        return {
            "saved_count": saved_count,
            "inserted_count": saved_count if upsert else 0,  # Approx; 0 if insert-only
            "updated_count": 0,
        }
