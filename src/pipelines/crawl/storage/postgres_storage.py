"""
Utility để lưu dữ liệu crawl vào PostgreSQL database
Tối ưu: Connection pooling, batch processing, error handling
"""

import os
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Any

import psycopg2
from psycopg2.extras import Json, execute_values
from psycopg2.pool import SimpleConnectionPool


class PostgresStorage:
    """Class để lưu và truy vấn dữ liệu crawl từ PostgreSQL với connection pooling tối ưu"""

    def __init__(
        self,
        host: str | None = None,
        port: int = 5432,
        database: str = "crawl_data",
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
            database: Database name (mặc định: 'crawl_data')
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
        self.database = database
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

    def save_categories(
        self, categories: list[dict[str, Any]], upsert: bool = True, batch_size: int = 100
    ) -> int:
        """
        Lưu danh sách categories vào database với batch processing tối ưu

        Args:
            categories: List các category dictionaries
            upsert: Nếu True, update nếu đã tồn tại (dựa trên url)
            batch_size: Số categories insert mỗi lần (mặc định: 100)

        Returns:
            Số categories đã lưu thành công
        """
        if not categories:
            return 0

        saved_count = 0
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Batch processing
                for i in range(0, len(categories), batch_size):
                    batch = categories[i : i + batch_size]
                    try:
                        # Chuẩn bị data cho batch insert
                        values = [
                            (
                                cat.get("category_id"),
                                cat.get("name"),
                                cat.get("url"),
                                cat.get("image_url"),
                                cat.get("parent_url"),
                                cat.get("level"),
                                cat.get("product_count", 0),
                            )
                            for cat in batch
                        ]

                        if upsert:
                            # Batch INSERT ... ON CONFLICT UPDATE
                            execute_values(
                                cur,
                                """
                                INSERT INTO categories
                                    (category_id, name, url, image_url, parent_url, level, product_count)
                                VALUES %s
                                ON CONFLICT (url)
                                DO UPDATE SET
                                    name = EXCLUDED.name,
                                    image_url = EXCLUDED.image_url,
                                    parent_url = EXCLUDED.parent_url,
                                    level = EXCLUDED.level,
                                    product_count = EXCLUDED.product_count,
                                    updated_at = CURRENT_TIMESTAMP
                                """,
                                values,
                            )
                        else:
                            # Batch INSERT, bỏ qua nếu đã tồn tại
                            execute_values(
                                cur,
                                """
                                INSERT INTO categories
                                    (category_id, name, url, image_url, parent_url, level, product_count)
                                VALUES %s
                                ON CONFLICT (url) DO NOTHING
                                """,
                                values,
                            )

                        saved_count += len(batch)
                    except Exception as e:
                        # Fallback: lưu từng item một
                        print(f"⚠️  Lỗi khi lưu batch categories: {e}")
                        for cat in batch:
                            try:
                                if upsert:
                                    cur.execute(
                                        """
                                        INSERT INTO categories
                                            (category_id, name, url, image_url, parent_url, level, product_count)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                                        ON CONFLICT (url)
                                        DO UPDATE SET
                                            name = EXCLUDED.name,
                                            image_url = EXCLUDED.image_url,
                                            parent_url = EXCLUDED.parent_url,
                                            level = EXCLUDED.level,
                                            product_count = EXCLUDED.product_count,
                                            updated_at = CURRENT_TIMESTAMP
                                        """,
                                        (
                                            cat.get("category_id"),
                                            cat.get("name"),
                                            cat.get("url"),
                                            cat.get("image_url"),
                                            cat.get("parent_url"),
                                            cat.get("level"),
                                            cat.get("product_count", 0),
                                        ),
                                    )
                                else:
                                    cur.execute(
                                        """
                                        INSERT INTO categories
                                            (category_id, name, url, image_url, parent_url, level, product_count)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                                        ON CONFLICT (url) DO NOTHING
                                        """,
                                        (
                                            cat.get("category_id"),
                                            cat.get("name"),
                                            cat.get("url"),
                                            cat.get("image_url"),
                                            cat.get("parent_url"),
                                            cat.get("level"),
                                            cat.get("product_count", 0),
                                        ),
                                    )
                                saved_count += 1
                            except Exception:
                                continue

        return saved_count

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

        if upsert:
            return {
                "saved_count": saved_count,
                "inserted_count": inserted_count,
                "updated_count": updated_count,
            }
        return saved_count

    def log_crawl_history(
        self,
        crawl_type: str,
        status: str,
        items_count: int = 0,
        category_url: str | None = None,
        product_id: str | None = None,
        error_message: str | None = None,
        started_at: datetime | None = None,
    ) -> int:
        """
        Ghi log lịch sử crawl

        Args:
            crawl_type: Loại crawl ('categories', 'products', 'product_detail')
            status: Trạng thái ('success', 'failed', 'partial')
            items_count: Số items đã crawl
            category_url: URL category (nếu có)
            product_id: Product ID (nếu có)
            error_message: Thông báo lỗi (nếu có)
            started_at: Thời gian bắt đầu

        Returns:
            ID của record vừa tạo
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO crawl_history
                        (crawl_type, status, items_count, category_url, product_id,
                         error_message, started_at, completed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    RETURNING id
                    """,
                    (
                        crawl_type,
                        status,
                        items_count,
                        category_url,
                        product_id,
                        error_message,
                        started_at or datetime.now(),
                    ),
                )
                return cur.fetchone()[0]

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
        """Lấy thống kê tổng quan về categories và products"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        (SELECT COUNT(*) FROM categories) as total_categories,
                        (SELECT COUNT(*) FROM products) as total_products,
                        (SELECT COUNT(DISTINCT category_url) FROM products) as categories_with_products,
                        (SELECT AVG(sales_count) FROM products WHERE sales_count IS NOT NULL) as avg_sales_count,
                        (SELECT MAX(crawled_at) FROM products) as last_crawl_time
                    """
                )
                row = cur.fetchone()
                return {
                    "total_categories": row[0],
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
