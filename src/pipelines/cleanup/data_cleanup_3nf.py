"""
Data Cleanup & Normalization Pipeline - 3NF Processing

Luồng ETL để làm sạch dữ liệu hoàn toàn:
1. Extract từ DB, kiểm tra field coverage
2. Extract & flatten dict fields (category_path, shipping)
3. Normalize 3NF: tách thành các bảng riêng
4. Load vào DB normalized

Usage:
    python src/pipelines/cleanup/data_cleanup_3nf.py --db-host localhost
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class CleanupStats:
    """Thống kê cleanup process"""
    total_products: int = 0
    dropped_null_fields: int = 0  # Field có null > 70%
    extracted_fields: int = 0      # Extracted từ dict
    normalized_records: int = 0
    errors: int = 0


class DataCleanupPipeline:
    """3 giai đoạn cleanup & 3NF normalization"""
    
    def __init__(self, db_host: str = "localhost", db_port: int = 5432,
                 source_db: str = "crawl_data",
                 target_db: str = "tiki_data_3nf", db_user: str = "postgres",
                 db_password: str = "postgres"):
        self.db_host = db_host
        self.db_port = db_port
        self.source_db = source_db  # DB để lấy data
        self.target_db = target_db  # DB mới để lưu 3NF data
        self.db_user = db_user
        self.db_password = db_password
        self.stats = CleanupStats()
        
        # Import tại runtime
        self.PostgresStorage = None
        self._import_storage()
    
    def _import_storage(self):
        """Import PostgresStorage module"""
        import importlib.util
        import os
        
        # Path: data_cleanup_3nf.py is at src/pipelines/cleanup/
        # Need to reach src/pipelines/crawl/storage/postgres_storage.py
        storage_path = os.path.join(
            os.path.dirname(__file__), "..", "crawl", "storage", "postgres_storage.py"
        )
        
        if not os.path.exists(storage_path):
            logger.error(f"Cannot find PostgresStorage at {storage_path}")
            logger.error(f"  Current file: {os.path.dirname(__file__)}")
            logger.error(f"  Calculated path: {os.path.abspath(storage_path)}")
            return
        
        try:
            spec = importlib.util.spec_from_file_location("postgres_storage", storage_path)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                self.PostgresStorage = module.PostgresStorage
                logger.debug(f"✓ Loaded PostgresStorage from {storage_path}")
        except Exception as e:
            logger.error(f"Failed to load PostgresStorage: {e}")
    
    # ========== GIAI ĐOẠN 1: Extract & Filter ==========
    
    def _create_database_if_not_exists(self):
        """Create target database nếu chưa tồn tại"""
        import psycopg2
        
        # Connect to default postgres DB to create new DB
        try:
            conn = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                database='postgres',
                user=self.db_user,
                password=self.db_password,
                connect_timeout=10
            )
            conn.autocommit = True
            
            with conn.cursor() as cur:
                # Check if database exists (case-insensitive comparison)
                cur.execute("SELECT 1 FROM pg_database WHERE LOWER(datname) = %s", (self.target_db.lower(),))
                exists = cur.fetchone()
                
                if not exists:
                    logger.info(f"📝 Creating database: {self.target_db}")
                    cur.execute(f"CREATE DATABASE {self.target_db}")
                    logger.info(f"✓ Database created: {self.target_db}")
                else:
                    logger.info(f"✓ Database already exists: {self.target_db}")
            
            conn.close()
            
            # Wait a moment for DB to be ready
            import time
            time.sleep(1)
        except Exception as e:
            logger.error(f"Failed to create database: {e}")
            raise
    
    def phase1_extract_from_db(self) -> list[dict]:
        """
        Phase 1: Lấy dữ liệu từ DB, loại bỏ fields có null > 70%
        
        Returns:
            List[dict]: Products sau khi lọc
        """
        logger.info("=" * 70)
        logger.info("PHASE 1: Extract from DB & Filter Null Fields")
        logger.info("=" * 70)
        
        if not self.PostgresStorage:
            raise ImportError("Cannot import PostgresStorage")
        
        storage = self.PostgresStorage(
            host=self.db_host, port=self.db_port,
            database=self.source_db, user=self.db_user, password=self.db_password
        )
        
        try:
            # Lấy tất cả products
            with storage.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 
                            product_id, name, url, price, brand, description,
                            images, rating_average, review_count, sales_count, 
                            specifications, seller_name, stock_quantity, shipping, 
                            category_path, discount_percent, original_price,
                            popularity_score, value_score, image_url,
                            crawled_at, updated_at
                        FROM products
                        WHERE brand IS NOT NULL AND brand != ''
                        ORDER BY product_id
                    """)
                    
                    products = []
                    for row in cur.fetchall():
                        products.append({
                            'product_id': row[0],
                            'name': row[1],
                            'url': row[2],
                            'price': row[3],
                            'brand': row[4],
                            'description': row[5],
                            'images': row[6],
                            'rating_average': row[7],
                            'review_count': row[8],
                            'sales_count': row[9],
                            'specifications': row[10],
                            'seller_name': row[11],
                            'stock_quantity': row[12],
                            'shipping': row[13],
                            'category_path': row[14],
                            'discount_percent': row[15],
                            'original_price': row[16],
                            'popularity_score': row[17],
                            'value_score': row[18],
                            'image_url': row[19],
                            'crawled_at': row[20],
                            'updated_at': row[21],
                        })
                    
                    self.stats.total_products = len(products)
                    logger.info(f"📊 Lấy được {len(products)} products từ DB")
        
        finally:
            storage.close()
        
        # Phân tích field coverage
        field_coverage = self._analyze_field_coverage(products)
        logger.info(f"\n📋 Field Coverage Analysis:")
        for field, coverage in sorted(field_coverage.items(), key=lambda x: x[1]):
            null_pct = (1 - coverage) * 100
            # sales_count luôn giữ lại
            if field == 'sales_count':
                status = "✅ KEEP (required)"
            else:
                status = "❌ DROP" if null_pct > 70 else "✅ KEEP"
            logger.info(f"  {field:25} {coverage*100:6.1f}% filled ({null_pct:.1f}% null) {status}")
        
        # Lọc bỏ fields có null > 70% (trừ sales_count)
        drop_fields = [f for f, cov in field_coverage.items() if (1 - cov) > 0.7 and f != 'sales_count']
        logger.info(f"\n🗑️  Fields sẽ loại bỏ ({len(drop_fields)}): {', '.join(drop_fields)}")
        
        # Xóa fields khỏi tất cả products
        for product in products:
            for field in drop_fields:
                if field in product:
                    del product[field]
        
        # Chỉ đếm số fields bị drop, không nhân với số products
        self.stats.dropped_null_fields = len(drop_fields)
        
        logger.info(f"\n✅ Phase 1 hoàn tất: {len(products)} products, dropped {len(drop_fields)} fields")
        return products
    
    def _analyze_field_coverage(self, products: list[dict]) -> dict:
        """Phân tích % field được filled"""
        if not products:
            return {}
        
        coverage = {}
        for field in products[0].keys():
            filled = sum(1 for p in products if p.get(field) is not None)
            coverage[field] = filled / len(products)
        
        return coverage
    
    # ========== GIAI ĐOẠN 2: Extract Dict Fields ==========
    
    def phase2_extract_dict_fields(self, products: list[dict]) -> tuple[list[dict], dict]:
        """
        Phase 2: Extract & flatten dict fields (category_path, shipping, ...)
        
        Returns:
            Tuple[normalized_products, extracted_entities]
        """
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 2: Extract & Flatten Dict Fields")
        logger.info("=" * 70)
        
        extracted = {
            'categories': {},      # category_id -> category info
            'sellers': {},         # seller_id -> seller info
            'shipping_methods': {} # shipping_id -> shipping info
        }
        
        seller_id = 1
        category_id = 1
        
        for product in products:
            # Extract category_path - tách thành cấu trúc cây (level_1, level_2, ...)
            if product.get('category_path'):
                try:
                    import json
                    category_path = product['category_path']
                    if isinstance(category_path, str):
                        category_path = json.loads(category_path)
                    
                    if isinstance(category_path, list) and len(category_path) > 0:
                        # Convert list to path string
                        path_str = ' > '.join(str(x) for x in category_path)
                        
                        # Create unique category với hierarchical levels
                        cat_hash = hash(path_str)
                        if cat_hash not in extracted['categories']:
                            category_levels = {
                                'category_id': cat_hash,
                                'category_name': category_path[-1],  # Tên category cuối cùng
                                'category_path': path_str,
                                'level_1': category_path[0] if len(category_path) > 0 else None,
                                'level_2': category_path[1] if len(category_path) > 1 else None,
                                'level_3': category_path[2] if len(category_path) > 2 else None,
                                'level_4': category_path[3] if len(category_path) > 3 else None,
                                'level_5': category_path[4] if len(category_path) > 4 else None,
                                'depth': len(category_path)
                            }
                            extracted['categories'][cat_hash] = category_levels
                        
                        product['category_id'] = cat_hash
                        del product['category_path']
                        self.stats.extracted_fields += 1
                except Exception as e:
                    logger.debug(f"Skip category extract: {e}")
                    pass
            
            # Extract seller from seller_name field (already flattened in DB)
            if product.get('seller_name') and product['seller_name'].strip():
                seller_name = product['seller_name'].strip()
                # Create unique seller ID based on name hash
                seller_hash = hash(seller_name)
                
                if seller_hash not in extracted['sellers']:
                    extracted['sellers'][seller_hash] = {
                        'seller_id': seller_hash,
                        'seller_name': seller_name,
                        'seller_type': 'official' if 'official' in seller_name.lower() else 'retail',
                        'seller_link': None,
                    }
                
                product['seller_id'] = seller_hash
                # Keep seller_name for now (don't delete)
            
            # Extract shipping (usually a dict)
            if product.get('shipping'):
                shipping_raw = product['shipping']
                try:
                    import json
                    shipping_data = shipping_raw
                    
                    if isinstance(shipping_data, str):
                        shipping_data = json.loads(shipping_data)
                    
                    if isinstance(shipping_data, dict):
                        shipping_provider = shipping_data.get('provider') or shipping_data.get('name') or 'Unknown'
                        shipping_fee = shipping_data.get('fee')
                        
                        # Create unique shipping
                        ship_hash = hash(shipping_provider)
                        if ship_hash not in extracted['shipping_methods']:
                            extracted['shipping_methods'][ship_hash] = {
                                'shipping_id': ship_hash,
                                'provider': shipping_provider,
                                'fee': shipping_fee,
                            }
                        
                        product['shipping_provider'] = shipping_provider
                        product['shipping_fee'] = shipping_fee
                        self.stats.extracted_fields += 1
                except Exception as e:
                    logger.debug(f"Skip shipping extract: {e}, type={type(shipping_raw)}")
        
        # Xóa field shipping khỏi TẤT CẢ products (sau khi loop xong)
        for product in products:
            if 'shipping' in product:
                del product['shipping']
        
        logger.info(f"📊 Extracted entities:")
        logger.info(f"  Categories: {len(extracted['categories'])}")
        logger.info(f"  Sellers: {len(extracted['sellers'])}")
        logger.info(f"  Shipping methods: {len(extracted['shipping_methods'])}")
        
        # Phân tích lại field coverage sau khi transform
        logger.info(f"\n📋 Re-analyzing field coverage after transform...")
        field_coverage_post = self._analyze_field_coverage(products)
        
        # Tìm các fields mới có null > 70% (trừ sales_count)
        drop_fields_post = [f for f, cov in field_coverage_post.items() if (1 - cov) > 0.7 and f != 'sales_count']
        if drop_fields_post:
            logger.info(f"🗑️  Additional fields to drop ({len(drop_fields_post)}): {', '.join(drop_fields_post)}")
            for product in products:
                for field in drop_fields_post:
                    if field in product:
                        del product[field]
            self.stats.dropped_null_fields += len(drop_fields_post)
        else:
            logger.info("✅ No additional fields to drop")
        
        logger.info(f"✅ Phase 2 hoàn tất: {len(products)} products normalized")
        return products, extracted
    
    def _extract_category(self, category_path: Any) -> dict:
        """Extract category từ dict/string"""
        try:
            if isinstance(category_path, str):
                # Parse JSON string
                import json
                category_path = json.loads(category_path)
            
            if isinstance(category_path, dict):
                return {
                    'category_id': category_path.get('id') or hash(category_path.get('name', '')),
                    'category_name': category_path.get('name', ''),
                    'category_path': category_path.get('path', ''),
                }
            return None
        except Exception as e:
            logger.debug(f"  Error extracting category: {e}")
            return None
    
    def _extract_seller(self, seller_data: dict) -> dict:
        """Extract seller từ dict"""
        try:
            return {
                'seller_id': seller_data.get('id'),
                'seller_name': seller_data.get('name', ''),
                'seller_type': seller_data.get('type', 'unknown'),
                'seller_link': seller_data.get('link'),
            }
        except Exception as e:
            logger.debug(f"  Error extracting seller: {e}")
            return None
    
    def _extract_shipping(self, shipping_data: Any) -> dict:
        """Extract shipping từ dict/string"""
        try:
            if isinstance(shipping_data, str):
                import json
                shipping_data = json.loads(shipping_data)
            
            if isinstance(shipping_data, dict):
                return {
                    'shipping_id': 1,  # Default ID
                    'provider': shipping_data.get('provider', ''),
                    'fee': shipping_data.get('fee'),
                    'estimated_days': shipping_data.get('estimated_days'),
                }
            return None
        except Exception as e:
            logger.debug(f"  Error extracting shipping: {e}")
            return None
    
    # ========== GIAI ĐOẠN 3: Normalize 3NF ==========
    
    def phase3_normalize_3nf(self, products: list[dict], extracted: dict) -> dict:
        """
        Phase 3: Chuẩn hóa 3NF
        
        Schema 3NF:
        - products: product_id, name, url, brand, description, ...
        - categories: category_id, name, path
        - sellers: seller_id, name, type, link
        - product_categories: product_id, category_id
        - product_sellers: product_id, seller_id
        - ratings: rating_id, product_id, average, count, ...
        - specifications: spec_id, product_id, key, value
        
        Returns:
            dict: Normalized data ready for DB insert
        """
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 3: Normalize to 3NF")
        logger.info("=" * 70)
        
        normalized = {
            'products': [],
            'categories': list(extracted['categories'].values()),
            'sellers': list(extracted['sellers'].values()),
            'ratings': [],
            'specifications': [],
            'product_categories': [],
        }
        
        spec_id = 1
        
        for product in products:
            # Main product table - CHỈ GIỮ CÁC TRƯỜNG CÓ DỮ LIỆU VÀ ĐÃ QUA FILTER
            product_record = {}
            
            # Core fields (luôn có)
            product_record['product_id'] = product.get('product_id')
            product_record['name'] = product.get('name')
            
            # Optional fields - chỉ thêm nếu có trong product (đã qua filter phase 1)
            optional_fields = [
                'url', 'brand', 'price', 'original_price', 'discount_percent',
                'sales_count', 'rating_average', 'review_count', 
                'category_id', 'seller_id', 'image_url', 
                'crawled_at', 'updated_at'
            ]
            
            for field in optional_fields:
                if field in product:
                    product_record[field] = product.get(field)
            
            # Computed fields
            if 'seller_name' in product and product.get('seller_name'):
                product_record['seller_is_official'] = 'official' in product['seller_name'].lower()
            else:
                product_record['seller_is_official'] = False
            
            normalized['products'].append(product_record)
            self.stats.normalized_records += 1
            
            # Extract rating từ rating_average và review_count
            if product.get('rating_average') is not None or product.get('review_count') is not None:
                rating_record = {
                    'product_id': product['product_id'],
                    'rating_average': product.get('rating_average'),
                    'review_count': product.get('review_count'),
                }
                normalized['ratings'].append(rating_record)
            
            # Extract specifications từ JSONB field
            if product.get('specifications'):
                specs = product['specifications']
                try:
                    # Parse JSON string nếu cần
                    if isinstance(specs, str):
                        import json
                        specs = json.loads(specs)
                    
                    # Nếu là dict, convert thành list of key-value pairs
                    if isinstance(specs, dict):
                        for key, value in specs.items():
                            spec_record = {
                                'spec_id': spec_id,
                                'product_id': product['product_id'],
                                'spec_key': str(key),
                                'spec_value': str(value) if value is not None else None,
                            }
                            normalized['specifications'].append(spec_record)
                            spec_id += 1
                    # Nếu là list
                    elif isinstance(specs, list):
                        for spec in specs:
                            if isinstance(spec, dict):
                                spec_record = {
                                    'spec_id': spec_id,
                                    'product_id': product['product_id'],
                                    'spec_key': spec.get('key') or spec.get('name', ''),
                                    'spec_value': spec.get('value') or spec.get('content', ''),
                                }
                                normalized['specifications'].append(spec_record)
                                spec_id += 1
                except Exception as e:
                    logger.debug(f"Skip specifications extract for {product['product_id']}: {e}")
            
            # Link product to category (Junction Table cho Many-to-Many relationship)
            # Ý nghĩa: 1 product có thể thuộc nhiều categories, 1 category có nhiều products
            # Ví dụ: iPhone có thể thuộc "Điện thoại" VÀ "Apple" categories
            if product.get('category_id'):
                normalized['product_categories'].append({
                    'product_id': product['product_id'],
                    'category_id': product['category_id'],
                })
        
        logger.info(f"📊 Normalized 3NF schema:")
        logger.info(f"  Products: {len(normalized['products'])} (chỉ giữ trường có dữ liệu)")
        logger.info(f"  Categories: {len(normalized['categories'])} (có cấu trúc cây level_1->level_5)")
        logger.info(f"  Sellers: {len(normalized['sellers'])}")
        logger.info(f"  Ratings: {len(normalized['ratings'])} (rating_average + review_count)")
        
        if len(normalized['specifications']) > 0:
            logger.info(f"  Specifications: {len(normalized['specifications'])} (thông số kỹ thuật từ JSONB)")
        else:
            logger.info(f"  Specifications: 0 (bỏ qua - không có dữ liệu)")
            
        logger.info(f"  Product-Category links: {len(normalized['product_categories'])} (Many-to-Many junction table)")
        
        logger.info(f"✅ Phase 3 hoàn tất: 3NF normalization")
        return normalized
    
    def save_normalized_data(self, normalized: dict, output_dir: str = "data/processed"):
        """Lưu dữ liệu normalized vào file"""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Lưu từng entity thành file riêng
        files = {
            'products_3nf.json': normalized['products'],
            'categories_3nf.json': normalized['categories'],
            'sellers_3nf.json': normalized['sellers'],
            'ratings_3nf.json': normalized['ratings'],
            'specifications_3nf.json': normalized['specifications'],
            'product_categories_3nf.json': normalized['product_categories'],
        }
        
        for filename, data in files.items():
            filepath = output_path / filename
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
            logger.info(f"💾 Saved: {filename} ({len(data)} records)")
    
    def run(self) -> dict:
        """Chạy toàn bộ pipeline"""
        logger.info("\n" + "#" * 70)
        logger.info("# Data Cleanup & 3NF Normalization Pipeline")
        logger.info("#" * 70)
        
        try:
            # Create database if not exists
            self._create_database_if_not_exists()
            
            # Phase 1: Extract & Filter
            products = self.phase1_extract_from_db()
            
            # Phase 2: Extract dict fields
            products, extracted = self.phase2_extract_dict_fields(products)
            
            # Phase 3: Normalize 3NF
            normalized = self.phase3_normalize_3nf(products, extracted)
            
            # Load vào DB (always - không lưu local files)
            logger.info("\n" + "=" * 70)
            logger.info("Loading 3NF data to database...")
            logger.info("=" * 70)
            
            self.load_3nf_to_db(normalized)
            
            # Summary
            logger.info("\n" + "=" * 70)
            logger.info("✅ PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 70)
            logger.info(f"Total products processed: {self.stats.total_products}")
            logger.info(f"Fields dropped (null > 70%): {self.stats.dropped_null_fields}")
            logger.info(f"Dict fields extracted: {self.stats.extracted_fields}")
            logger.info(f"Normalized records: {self.stats.normalized_records}")
            
            return {
                'success': True,
                'stats': self.stats.__dict__,
                'output': 'data/processed/*_3nf.json'
            }
        
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}
    
    # ========== LOAD 3NF to DB ==========
    
    def load_3nf_to_db(self, normalized: dict) -> bool:
        """
        Load dữ liệu 3NF vào database mới
        
        Creates 6 tables: products, categories, sellers, ratings, 
                         specifications, product_categories
        """
        logger.info("\n" + "=" * 70)
        logger.info("LOADING 3NF DATA TO DATABASE")
        logger.info("=" * 70)
        
        if not self.PostgresStorage:
            raise ImportError("Cannot import PostgresStorage")
        
        storage = self.PostgresStorage(
            host=self.db_host, port=self.db_port,
            database=self.target_db, user=self.db_user, password=self.db_password
        )
        
        try:
            with storage.get_connection() as conn:
                # 1. Create 3NF tables
                self._create_3nf_tables(conn)
                
                # 2. Load categories
                self._load_categories(conn, normalized['categories'])
                
                # 3. Load sellers
                self._load_sellers(conn, normalized['sellers'])
                
                # 4. Load products
                self._load_products(conn, normalized['products'])
                
                # 5. Load ratings
                self._load_ratings(conn, normalized['ratings'])
                
                # 6. Load specifications (chỉ nếu có dữ liệu)
                if len(normalized['specifications']) > 0:
                    self._load_specifications(conn, normalized['specifications'])
                else:
                    logger.info("⊘ Skipping specifications table - no data")
                
                # 7. Load product_categories
                self._load_product_categories(conn, normalized['product_categories'])
                
                conn.commit()
                logger.info("✅ Data committed to database")
        
        finally:
            storage.close()
        
        return True
    
    def _create_3nf_tables(self, conn):
        """Create 3NF tables"""
        logger.info("📋 Creating 3NF tables...")
        
        with conn.cursor() as cur:
            # Drop tables if exist (optional)
            drop_sql = """
            DROP TABLE IF EXISTS product_categories CASCADE;
            DROP TABLE IF EXISTS specifications CASCADE;
            DROP TABLE IF EXISTS ratings CASCADE;
            DROP TABLE IF EXISTS products CASCADE;
            DROP TABLE IF EXISTS categories CASCADE;
            DROP TABLE IF EXISTS sellers CASCADE;
            """
            
            for sql in drop_sql.split(';'):
                if sql.strip():
                    cur.execute(sql)
            
            # Create categories table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS categories (
                category_id BIGINT PRIMARY KEY,
                category_name TEXT,
                category_path TEXT,
                level_1 TEXT,
                level_2 TEXT,
                level_3 TEXT,
                level_4 TEXT,
                level_5 TEXT,
                depth INT
            )
            """)
            logger.info("  ✓ Created/Checked: categories")
            
            # Create sellers table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS sellers (
                seller_id BIGINT PRIMARY KEY,
                seller_name TEXT,
                seller_type VARCHAR(20),
                seller_link TEXT
            )
            """)
            logger.info("  ✓ Created/Checked: sellers")
            
            # Create products table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS products (
                product_id VARCHAR(50) PRIMARY KEY,
                name TEXT NOT NULL,
                url TEXT,
                brand TEXT,
                price NUMERIC(12, 2),
                original_price NUMERIC(12, 2),
                discount_percent INT,
                discount_amount NUMERIC(12, 2),
                sales_count INT,
                review_count INT,
                rating_average NUMERIC(5, 2),
                value_score NUMERIC(5, 2),
                popularity_score NUMERIC(5, 2),
                price_savings NUMERIC(12, 2),
                price_category VARCHAR(20),
                sales_velocity INT,
                category_id BIGINT REFERENCES categories(category_id),
                seller_id BIGINT REFERENCES sellers(seller_id),
                seller_is_official BOOLEAN,
                shipping_provider TEXT,
                shipping_fee NUMERIC(10, 2),
                stock_available BOOLEAN,
                stock_quantity INT,
                stock_status VARCHAR(20),
                image_url TEXT,
                estimated_revenue NUMERIC(12, 2),
                crawled_at TIMESTAMP,
                updated_at TIMESTAMP
            )
            """)
            logger.info("  ✓ Created/Checked: products")
            
            # Create ratings table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS ratings (
                product_id VARCHAR(50) PRIMARY KEY,
                rating_average NUMERIC(3, 1),
                review_count INT,
                FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE
            )
            """)
            logger.info("  ✓ Created/Checked: ratings")
            
            # Create specifications table (dimension table cho product attributes)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS specifications (
                spec_id SERIAL PRIMARY KEY,
                product_id VARCHAR(50),
                spec_key TEXT,
                spec_value TEXT,
                FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE
            )
            """)
            logger.info("  ✓ Created/Checked: specifications (dimension table)")
            
            # Create product_categories junction table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS product_categories (
                product_id VARCHAR(50),
                category_id BIGINT,
                PRIMARY KEY (product_id, category_id),
                FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE,
                FOREIGN KEY (category_id) REFERENCES categories(category_id) ON DELETE CASCADE
            )
            """)
            logger.info("  ✓ Created: product_categories")
            
            conn.commit()
    
    def _load_categories(self, conn, categories: list):
        """Load categories"""
        if not categories:
            logger.info("⊘ No categories to load")
            return
        
        logger.info(f"📥 Loading {len(categories)} categories...")
        
        with conn.cursor() as cur:
            for cat in categories:
                cur.execute("""
                INSERT INTO categories (
                    category_id, category_name, category_path,
                    level_1, level_2, level_3, level_4, level_5, depth
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (category_id) DO UPDATE SET
                    category_name = EXCLUDED.category_name
                """, (
                    cat.get('category_id'),
                    cat.get('category_name'),
                    cat.get('category_path'),
                    cat.get('level_1'),
                    cat.get('level_2'),
                    cat.get('level_3'),
                    cat.get('level_4'),
                    cat.get('level_5'),
                    cat.get('depth')
                ))
            
            conn.commit()
            logger.info(f"  ✓ Loaded {len(categories)} categories")
    
    def _load_sellers(self, conn, sellers: list):
        """Load sellers"""
        if not sellers:
            logger.info("⊘ No sellers to load")
            return
        
        logger.info(f"📥 Loading {len(sellers)} sellers...")
        
        with conn.cursor() as cur:
            for seller in sellers:
                cur.execute("""
                INSERT INTO sellers (seller_id, seller_name, seller_type, seller_link)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (seller_id) DO UPDATE SET
                    seller_name = EXCLUDED.seller_name,
                    seller_type = EXCLUDED.seller_type,
                    seller_link = EXCLUDED.seller_link
                """, (seller.get('seller_id'), seller.get('seller_name'), 
                      seller.get('seller_type'), seller.get('seller_link')))
            
            conn.commit()
            logger.info(f"  ✓ Loaded {len(sellers)} sellers")
    
    def _load_products(self, conn, products: list):
        """Load products"""
        if not products:
            logger.info("⊘ No products to load")
            return
        
        logger.info(f"📥 Loading {len(products)} products...")
        
        with conn.cursor() as cur:
            for product in products:
                cur.execute("""
                INSERT INTO products (
                    product_id, name, url, brand, price, original_price,
                    discount_percent, sales_count, rating_average,
                    category_id, seller_id, seller_is_official,
                    image_url, crawled_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    price = EXCLUDED.price,
                    updated_at = EXCLUDED.updated_at
                """, (
                    product.get('product_id'),
                    product.get('name'),
                    product.get('url'),
                    product.get('brand'),
                    product.get('price'),
                    product.get('original_price'),
                    product.get('discount_percent'),
                    product.get('sales_count'),
                    product.get('rating_average'),
                    product.get('category_id'),
                    product.get('seller_id'),
                    product.get('seller_is_official'),
                    product.get('image_url'),
                    product.get('crawled_at'),
                    product.get('updated_at')
                ))
            
            conn.commit()
            logger.info(f"  ✓ Loaded {len(products)} products")
    
    def _load_ratings(self, conn, ratings: list):
        """Load ratings"""
        if not ratings:
            logger.info("⊘ No ratings to load")
            return
        
        logger.info(f"📥 Loading {len(ratings)} ratings...")
        
        with conn.cursor() as cur:
            for rating in ratings:
                cur.execute("""
                INSERT INTO ratings (product_id, rating_average, review_count)
                VALUES (%s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE SET
                    rating_average = EXCLUDED.rating_average,
                    review_count = EXCLUDED.review_count
                """, (rating.get('product_id'), rating.get('rating_average'), rating.get('review_count')))
            
            conn.commit()
            logger.info(f"  ✓ Loaded {len(ratings)} ratings")
    
    def _load_specifications(self, conn, specifications: list):
        """Load specifications"""
        if not specifications:
            logger.info("⊘ No specifications to load")
            return
        
        logger.info(f"📥 Loading {len(specifications)} specifications...")
        
        with conn.cursor() as cur:
            for spec in specifications:
                cur.execute("""
                INSERT INTO specifications (product_id, spec_key, spec_value)
                VALUES (%s, %s, %s)
                """, (spec.get('product_id'), spec.get('spec_key'), spec.get('spec_value')))
            
            conn.commit()
            logger.info(f"  ✓ Loaded {len(specifications)} specifications")
    
    def _load_product_categories(self, conn, product_categories: list):
        """Load product_categories links"""
        if not product_categories:
            logger.info("⊘ No product_categories to load")
            return
        
        logger.info(f"📥 Loading {len(product_categories)} product_categories links...")
        
        with conn.cursor() as cur:
            for link in product_categories:
                cur.execute("""
                INSERT INTO product_categories (product_id, category_id)
                VALUES (%s, %s)
                ON CONFLICT (product_id, category_id) DO NOTHING
                """, (link.get('product_id'), link.get('category_id')))
            
            conn.commit()
            logger.info(f"  ✓ Loaded {len(product_categories)} product_categories")


if __name__ == '__main__':
    import sys
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Parse arguments
    db_host = next((arg.split('=')[1] for arg in sys.argv if arg.startswith('--db-host=')), 'localhost')
    
    # Run pipeline
    pipeline = DataCleanupPipeline(db_host=db_host)
    result = pipeline.run()
    
    sys.exit(0 if result['success'] else 1)
