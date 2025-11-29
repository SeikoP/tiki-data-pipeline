#!/usr/bin/env python3
"""
Verify Tiki Warehouse Schema from Actual Database
Provides accurate, up-to-date schema information and metrics
"""

import psycopg2
import os
from typing import List, Dict, Any
from datetime import datetime


class WarehouseSchemaVerifier:
    """Verify actual warehouse schema structure and contents"""
    
    def __init__(self, host: str = "localhost", port: int = 5432, 
                 database: str = "tiki_warehouse"):
        self.host = host
        self.port = port
        self.database = database
        self.conn = None
        self.cursor = None
    
    def connect(self) -> bool:
        """Connect to tiki_warehouse database"""
        try:
            user = os.getenv("POSTGRES_USER", "airflow")
            password = os.getenv("POSTGRES_PASSWORD", "airflow")
            
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=user,
                password=password
            )
            self.cursor = self.conn.cursor()
            print(f"✅ Connected to {self.database} database")
            return True
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False
    
    def get_tables(self) -> List[str]:
        """Get all tables in public schema"""
        try:
            query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
            """
            self.cursor.execute(query)
            tables = [row[0] for row in self.cursor.fetchall()]
            return tables
        except Exception as e:
            print(f"Error fetching tables: {e}")
            return []
    
    def get_table_structure(self, table_name: str) -> Dict[str, Any]:
        """Get structure of a specific table"""
        try:
            # Get columns
            columns_query = """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """
            self.cursor.execute(columns_query, (table_name,))
            columns = self.cursor.fetchall()
            
            # Get row count
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = self.cursor.fetchone()[0]
            
            # Get primary key
            pk_query = """
            SELECT constraint_name, column_name
            FROM information_schema.constraint_column_usage
            WHERE table_schema = 'public' AND table_name = %s 
            AND constraint_name LIKE '%pkey%'
            """
            self.cursor.execute(pk_query, (table_name,))
            pk = self.cursor.fetchall()
            
            # Get foreign keys
            fk_query = f"""
            SELECT constraint_name, column_name, 
                   table_name as foreign_table, column_name as foreign_column
            FROM (
                SELECT constraint_name, column_name, table_name, column_name
                FROM information_schema.constraint_column_usage
                WHERE table_schema = 'public'
            ) AS fk
            WHERE table_name IN (
                SELECT column_name FROM information_schema.columns 
                WHERE table_schema = 'public' AND table_name = '{table_name}'
                    AND data_type LIKE '%INT%'
            )
            """
            # Simplified: just get references
            self.cursor.execute(f"""
            SELECT tc.constraint_name, kcu.column_name, 
                   ccu.table_name, ccu.column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu 
                ON tc.table_name = kcu.table_name
            JOIN information_schema.constraint_column_usage AS ccu 
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.table_schema = 'public' AND tc.table_name = %s 
                AND tc.constraint_type = 'FOREIGN KEY'
            """, (table_name,))
            foreign_keys = self.cursor.fetchall()
            
            # Get indexes
            idx_query = """
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = 'public' AND tablename = %s
            """
            self.cursor.execute(idx_query, (table_name,))
            indexes = self.cursor.fetchall()
            
            return {
                'name': table_name,
                'row_count': row_count,
                'columns': columns,
                'primary_key': pk,
                'foreign_keys': foreign_keys,
                'indexes': indexes
            }
        except Exception as e:
            print(f"Error fetching structure for {table_name}: {e}")
            return {}
    
    def get_all_views(self) -> List[str]:
        """Get all views in public schema"""
        try:
            query = """
            SELECT table_name 
            FROM information_schema.views 
            WHERE table_schema = 'public'
            ORDER BY table_name
            """
            self.cursor.execute(query)
            views = [row[0] for row in self.cursor.fetchall()]
            return views
        except Exception as e:
            print(f"Error fetching views: {e}")
            return []
    
    def get_view_definition(self, view_name: str) -> str:
        """Get SQL definition of a view"""
        try:
            query = """
            SELECT view_definition
            FROM information_schema.views
            WHERE table_schema = 'public' AND table_name = %s
            """
            self.cursor.execute(query, (view_name,))
            result = self.cursor.fetchone()
            return result[0] if result else ""
        except Exception as e:
            print(f"Error fetching view {view_name}: {e}")
            return ""
    
    def verify_star_schema(self) -> Dict[str, Any]:
        """Verify expected Star Schema structure"""
        expected_tables = {
            'fact_product_sales': 'Fact Table',
            'dim_product': 'Product Dimension',
            'dim_category': 'Category Dimension',
            'dim_seller': 'Seller Dimension',
            'dim_brand': 'Brand Dimension',
            'dim_date': 'Date Dimension',
            'dim_price_segment': 'Price Segment Dimension'
        }
        
        actual_tables = self.get_tables()
        
        print("\n" + "="*60)
        print("STAR SCHEMA VERIFICATION")
        print("="*60)
        
        verification = {}
        for expected, description in expected_tables.items():
            exists = expected in actual_tables
            status = "✅" if exists else "❌"
            print(f"{status} {expected:30} ({description})")
            verification[expected] = exists
        
        return verification
    
    def print_summary(self):
        """Print comprehensive warehouse summary"""
        print("\n" + "="*60)
        print("TIKI WAREHOUSE SCHEMA SUMMARY")
        print("="*60)
        
        tables = self.get_tables()
        print(f"\n📊 Tables in {self.database}:")
        for table in tables:
            structure = self.get_table_structure(table)
            print(f"  • {table:30} ({structure['row_count']:,} rows)")
        
        views = self.get_all_views()
        if views:
            print(f"\n📈 Views in {self.database}:")
            for view in views:
                print(f"  • {view}")
        
        # Star Schema verification
        self.verify_star_schema()
        
        # Detailed table info
        print("\n" + "-"*60)
        print("DETAILED TABLE STRUCTURES")
        print("-"*60)
        
        for table in tables:
            structure = self.get_table_structure(table)
            print(f"\n📋 Table: {table}")
            print(f"   Rows: {structure['row_count']:,}")
            
            if structure['columns']:
                print(f"   Columns ({len(structure['columns'])}):")
                for col_name, col_type, nullable, default in structure['columns']:
                    null_str = "NULL" if nullable == "YES" else "NOT NULL"
                    print(f"     • {col_name:25} {col_type:15} {null_str}")
            
            if structure['foreign_keys']:
                print(f"   Foreign Keys:")
                for fk_name, fk_col, fk_table, fk_ref_col in structure['foreign_keys']:
                    print(f"     • {fk_col} → {fk_table}({fk_ref_col})")
            
            if structure['indexes']:
                print(f"   Indexes:")
                for idx_name, idx_def in structure['indexes']:
                    print(f"     • {idx_name}")
    
    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("\n✅ Connection closed")


def main():
    """Main entry point"""
    print(f"\n🔍 Tiki Warehouse Schema Verification")
    print(f"   Started: {datetime.now().isoformat()}\n")
    
    verifier = WarehouseSchemaVerifier()
    
    if verifier.connect():
        try:
            verifier.print_summary()
        except Exception as e:
            print(f"Error: {e}")
        finally:
            verifier.close()
    else:
        print("\n⚠️  Could not connect to warehouse database")
        print("   Ensure PostgreSQL is running and .env file is configured")
        print("   Connection: postgresql://user:password@localhost:5432/tiki_warehouse")


if __name__ == "__main__":
    main()
