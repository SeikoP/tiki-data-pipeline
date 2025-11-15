"""
Utility để lưu dữ liệu crawl vào PostgreSQL database
"""
import os
from typing import Any, Dict, List, Optional
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values, Json
from psycopg2.pool import SimpleConnectionPool
from contextlib import contextmanager


class PostgresStorage:
    """Class để lưu và truy vấn dữ liệu crawl từ PostgreSQL"""

    def __init__(
        self,
        host: str = None,
        port: int = 5432,
        database: str = "crawl_data",
        user: str = None,
        password: str = None,
        minconn: int = 1,
        maxconn: int = 5,
    ):
        """
        Khởi tạo connection pool đến PostgreSQL

        Args:
            host: PostgreSQL host (mặc định: lấy từ env hoặc 'postgres')
            port: PostgreSQL port (mặc định: 5432)
            database: Database name (mặc định: 'crawl_data')
            user: PostgreSQL user (mặc định: lấy từ env)
            password: PostgreSQL password (mặc định: lấy từ env)
            minconn: Số connection tối thiểu trong pool
            maxconn: Số connection tối đa trong pool
        """
        self.host = host or os.getenv("POSTGRES_HOST", "postgres")
        self.port = port
        self.database = database
        self.user = user or os.getenv("POSTGRES_USER", "airflow_user")
        self.password = password or os.getenv("POSTGRES_PASSWORD", "")
        self.minconn = minconn
        self.maxconn = maxconn
        self._pool: Optional[SimpleConnectionPool] = None

    def _get_pool(self) -> SimpleConnectionPool:
        """Lấy hoặc tạo connection pool"""
        if self._pool is None:
            self._pool = SimpleConnectionPool(
                self.minconn,
                self.maxconn,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )
        return self._pool

    @contextmanager
    def get_connection(self):
        """Context manager để lấy connection từ pool"""
        pool = self._get_pool()
        conn = pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            pool.putconn(conn)

    def save_categories(self, categories: List[Dict[str, Any]], upsert: bool = True) -> int:
        """
        Lưu danh sách categories vào database

        Args:
            categories: List các category dictionaries
            upsert: Nếu True, update nếu đã tồn tại (dựa trên url)

        Returns:
            Số categories đã lưu thành công
        """
        if not categories:
            return 0

        saved_count = 0
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                for cat in categories:
                    try:
                        if upsert:
                            # INSERT ... ON CONFLICT UPDATE
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
                            # Chỉ INSERT, bỏ qua nếu đã tồn tại
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
                    except Exception as e:
                        print(f"⚠️  Lỗi khi lưu category {cat.get('url', 'unknown')}: {e}")
                        continue

        return saved_count

    def save_products(
        self, products: List[Dict[str, Any]], upsert: bool = True, batch_size: int = 100
    ) -> int:
        """
        Lưu danh sách products vào database (batch insert để tối ưu)

        Args:
            products: List các product dictionaries
            upsert: Nếu True, update nếu đã tồn tại (dựa trên product_id)
            batch_size: Số products insert mỗi lần

        Returns:
            Số products đã lưu thành công
        """
        if not products:
            return 0

        saved_count = 0
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Chia nhỏ thành batches
                for i in range(0, len(products), batch_size):
                    batch = products[i : i + batch_size]
                    try:
                        # Chuẩn bị data cho batch insert
                        values = []
                        for product in batch:
                            values.append(
                                (
                                    product.get("product_id"),
                                    product.get("name"),
                                    product.get("url"),
                                    product.get("image_url"),
                                    product.get("category_url"),
                                    product.get("sales_count"),
                                    product.get("price"),
                                    product.get("original_price"),
                                    product.get("discount_percent"),
                                    product.get("rating_average"),
                                    product.get("review_count"),
                                    product.get("description"),
                                    Json(product.get("specifications")) if product.get("specifications") else None,
                                    Json(product.get("images")) if product.get("images") else None,
                                )
                            )

                        if upsert:
                            # Batch INSERT ... ON CONFLICT UPDATE
                            execute_values(
                                cur,
                                """
                                INSERT INTO products 
                                    (product_id, name, url, image_url, category_url, sales_count,
                                     price, original_price, discount_percent, rating_average, 
                                     review_count, description, specifications, images)
                                VALUES %s
                                ON CONFLICT (product_id) 
                                DO UPDATE SET
                                    name = EXCLUDED.name,
                                    url = EXCLUDED.url,
                                    image_url = EXCLUDED.image_url,
                                    category_url = EXCLUDED.category_url,
                                    sales_count = EXCLUDED.sales_count,
                                    price = EXCLUDED.price,
                                    original_price = EXCLUDED.original_price,
                                    discount_percent = EXCLUDED.discount_percent,
                                    rating_average = EXCLUDED.rating_average,
                                    review_count = EXCLUDED.review_count,
                                    description = EXCLUDED.description,
                                    specifications = EXCLUDED.specifications,
                                    images = EXCLUDED.images,
                                    updated_at = CURRENT_TIMESTAMP
                                """,
                                values,
                            )
                        else:
                            # Batch INSERT, bỏ qua nếu đã tồn tại
                            execute_values(
                                cur,
                                """
                                INSERT INTO products 
                                    (product_id, name, url, image_url, category_url, sales_count,
                                     price, original_price, discount_percent, rating_average, 
                                     review_count, description, specifications, images)
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
                                self.save_products([product], upsert=upsert, batch_size=1)
                                saved_count += 1
                            except Exception:
                                continue

        return saved_count

    def log_crawl_history(
        self,
        crawl_type: str,
        status: str,
        items_count: int = 0,
        category_url: str = None,
        product_id: str = None,
        error_message: str = None,
        started_at: datetime = None,
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

    def get_products_by_category(self, category_url: str, limit: int = 100) -> List[Dict[str, Any]]:
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
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def get_category_stats(self) -> Dict[str, Any]:
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

    def close(self):
        """Đóng connection pool"""
        if self._pool:
            self._pool.closeall()
            self._pool = None

