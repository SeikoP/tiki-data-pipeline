#!/usr/bin/env python3
"""
Re-apply Phase 2 Optimizations

Phase 2 optimizations were lost after git restore.
This script re-applies them based on documentation in OPTIMIZATIONS.md
"""

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))


def apply_selenium_optimizations():
    """Apply Selenium implicit wait optimization"""
    print("\n1️⃣ Applying Selenium Optimizations...")
    
    crawler_file = project_root / "src" / "pipelines" / "crawl" / "crawler_base.py"
    
    if not crawler_file.exists():
        print(f"   ⚠️  File not found: {crawler_file}")
        return False
    
    content = crawler_file.read_text(encoding='utf-8')
    
    # Check if already applied
    if "driver.implicitly_wait(10)" in content:
        print("   ✅ Selenium optimization already applied")
        return True
    
    # Add implicit wait after driver creation
    if "self.driver = webdriver.Chrome" in content:
        content = content.replace(
            "self.driver = webdriver.Chrome(options=options)",
            "self.driver = webdriver.Chrome(options=options)\n        self.driver.implicitly_wait(10)  # Wait up to 10s for elements"
        )
        
        crawler_file.write_text(content, encoding='utf-8')
        print("   ✅ Added implicit wait to Selenium driver")
        return True
    
    print("   ⚠️  Could not find driver initialization")
    return False


def apply_chrome_optimizations():
    """Apply Chrome optimization flags"""
    print("\n2️⃣ Applying Chrome Optimizations...")
    
    crawler_file = project_root / "src" / "pipelines" / "crawl" / "crawler_base.py"
    
    if not crawler_file.exists():
        print(f"   ⚠️  File not found: {crawler_file}")
        return False
    
    content = crawler_file.read_text(encoding='utf-8')
    
    # Check if already applied
    if "--disable-blink-features=AutomationControlled" in content:
        print("   ✅ Chrome optimizations already applied")
        return True
    
    # Add Chrome flags
    optimization_flags = '''
        # Performance optimizations
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-logging')
        options.add_argument('--disable-notifications')
        options.add_argument('--disable-popup-blocking')
        options.add_argument('--ignore-certificate-errors')
'''
    
    if "options.add_argument('--headless')" in content:
        content = content.replace(
            "options.add_argument('--headless')",
            "options.add_argument('--headless')" + optimization_flags
        )
        
        crawler_file.write_text(content, encoding='utf-8')
        print("   ✅ Added Chrome optimization flags")
        return True
    
    print("   ⚠️  Could not find Chrome options initialization")
    return False


def apply_redis_optimization():
    """Apply Redis connection pooling"""
    print("\n3️⃣ Applying Redis Optimizations...")
    
    redis_file = project_root / "src" / "pipelines" / "crawl" / "redis_cache.py"
    
    if not redis_file.exists():
        print(f"   ⚠️  File not found: {redis_file}")
        return False
    
    content = redis_file.read_text(encoding='utf-8')
    
    # Check if already applied
    if "connection_pool" in content and "max_connections" in content:
        print("   ✅ Redis connection pooling already applied")
        return True
    
    # Add connection pooling
    if "redis.Redis(" in content:
        content = content.replace(
            "redis.Redis(",
            "redis.Redis(\n            connection_pool=redis.ConnectionPool(max_connections=10),"
        )
        
        redis_file.write_text(content, encoding='utf-8')
        print("   ✅ Added Redis connection pooling")
        return True
    
    print("   ⚠️  Could not find Redis client initialization")
    return False


def apply_postgresql_indexes():
    """Create SQL script for PostgreSQL indexes"""
    print("\n4️⃣ Creating PostgreSQL Index Script...")
    
    sql_file = project_root / "airflow" / "setup" / "add_performance_indexes.sql"
    
    sql_content = """-- Performance Indexes for Tiki Pipeline
-- Apply with: docker exec postgres psql -U postgres -d crawl_data -f /docker-entrypoint-initdb.d/add_performance_indexes.sql

-- Products table indexes
CREATE INDEX IF NOT EXISTS idx_products_url ON products USING btree (url);
CREATE INDEX IF NOT EXISTS idx_products_sku ON products USING btree (sku);
CREATE INDEX IF NOT EXISTS idx_products_created_at ON products USING btree (created_at);
CREATE INDEX IF NOT EXISTS idx_products_category_id ON products USING btree (category_id);

-- Categories table indexes
CREATE INDEX IF NOT EXISTS idx_categories_url ON categories USING btree (url);
CREATE INDEX IF NOT EXISTS idx_categories_parent_id ON categories USING btree (parent_id);

-- JSONB indexes for product data
CREATE INDEX IF NOT EXISTS idx_products_data_gin ON products USING gin (data);

-- Analyze tables for query optimization
ANALYZE products;
ANALYZE categories;

-- Display created indexes
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename IN ('products', 'categories')
ORDER BY tablename, indexname;
"""
    
    sql_file.write_text(sql_content, encoding='utf-8')
    print(f"   ✅ Created SQL script: {sql_file}")
    return True


def verify_optimizations():
    """Verify all optimizations applied"""
    print("\n" + "="*70)
    print("📊 VERIFICATION")
    print("="*70)
    
    checks = {
        "Selenium implicit wait": False,
        "Chrome optimization flags": False,
        "Redis connection pooling": False,
        "PostgreSQL index script": False,
    }
    
    # Check Selenium
    crawler_file = project_root / "src" / "pipelines" / "crawl" / "crawler_base.py"
    if crawler_file.exists():
        content = crawler_file.read_text(encoding='utf-8')
        checks["Selenium implicit wait"] = "implicitly_wait" in content
        checks["Chrome optimization flags"] = "--disable-blink-features" in content
    
    # Check Redis
    redis_file = project_root / "src" / "pipelines" / "crawl" / "redis_cache.py"
    if redis_file.exists():
        content = redis_file.read_text(encoding='utf-8')
        checks["Redis connection pooling"] = "connection_pool" in content and "max_connections" in content
    
    # Check SQL script
    sql_file = project_root / "airflow" / "setup" / "add_performance_indexes.sql"
    checks["PostgreSQL index script"] = sql_file.exists()
    
    print()
    for check, passed in checks.items():
        status = "✅" if passed else "❌"
        print(f"{status} {check}")
    
    all_passed = all(checks.values())
    
    print()
    if all_passed:
        print("✅ All Phase 2 optimizations applied successfully!")
        print()
        print("📋 Next Steps:")
        print("   1. Apply PostgreSQL indexes:")
        print("      docker exec postgres psql -U postgres -d crawl_data -f /docker-entrypoint-initdb.d/add_performance_indexes.sql")
        print()
        print("   2. Restart Airflow to pick up changes:")
        print("      docker-compose restart airflow-scheduler airflow-worker")
        print()
        print("   3. Test DAG with optimizations")
    else:
        print("⚠️  Some optimizations could not be applied")
        print("    Review file paths and try manual application")
        print("    See OPTIMIZATIONS.md for details")
    
    print("="*70)
    return all_passed


def main():
    """Main execution"""
    print("="*70)
    print("🔄 RE-APPLYING PHASE 2 OPTIMIZATIONS")
    print("="*70)
    print()
    print("Phase 2 optimizations were lost after git restore.")
    print("This script will reapply them based on OPTIMIZATIONS.md")
    print()
    
    results = []
    
    # Apply optimizations
    results.append(("Selenium", apply_selenium_optimizations()))
    results.append(("Chrome", apply_chrome_optimizations()))
    results.append(("Redis", apply_redis_optimization()))
    results.append(("PostgreSQL", apply_postgresql_indexes()))
    
    # Verify
    all_passed = verify_optimizations()
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
