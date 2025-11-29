#!/usr/bin/env python3
"""
Introspect tiki_warehouse database schema and generate documentation
"""
import psycopg2
import json
from typing import List, Dict, Any

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'tiki_warehouse',
    'user': 'postgres',
    'password': 'postgres'
}

def get_database_schema() -> Dict[str, Any]:
    """Get complete database schema from tiki_warehouse"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Get all tables
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        tables = [row[0] for row in cur.fetchall()]
        
        schema_info = {}
        
        for table_name in tables:
            # Get columns
            cur.execute(f"""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    character_maximum_length
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            
            columns = []
            for col in cur.fetchall():
                col_info = {
                    'name': col[0],
                    'type': col[1],
                    'nullable': col[2] == 'YES',
                    'default': col[3],
                    'max_length': col[4]
                }
                columns.append(col_info)
            
            # Get constraints
            cur.execute(f"""
                SELECT constraint_name, constraint_type
                FROM information_schema.table_constraints
                WHERE table_name = %s
                ORDER BY constraint_name
            """, (table_name,))
            
            constraints = [{'name': row[0], 'type': row[1]} for row in cur.fetchall()]
            
            schema_info[table_name] = {
                'columns': columns,
                'constraints': constraints,
                'row_count': 0
            }
            
            # Get row count
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                schema_info[table_name]['row_count'] = cur.fetchone()[0]
            except:
                pass
        
        cur.close()
        conn.close()
        
        return schema_info
        
    except psycopg2.OperationalError as e:
        print(f"❌ Database connection failed: {e}")
        print("\nMake sure:")
        print("1. PostgreSQL is running on localhost:5432")
        print("2. Database 'tiki_warehouse' exists")
        print("3. Credentials are correct (user: postgres)")
        return {}

def print_schema_info(schema_info: Dict[str, Any]):
    """Print schema information in readable format"""
    
    print("\n" + "="*80)
    print("TIKI WAREHOUSE DATABASE SCHEMA")
    print("="*80)
    
    if not schema_info:
        print("\n❌ Could not retrieve schema information")
        return
    
    print(f"\n📊 Total Tables: {len(schema_info)}\n")
    
    for table_name in sorted(schema_info.keys()):
        table_info = schema_info[table_name]
        print(f"\n{'─'*80}")
        print(f"📋 TABLE: {table_name.upper()}")
        print(f"{'─'*80}")
        print(f"   Rows: {table_info['row_count']:,}")
        print(f"   Columns: {len(table_info['columns'])}\n")
        
        print(f"   {'Column Name':<30} {'Data Type':<20} {'Nullable':<10} {'Default':<20}")
        print(f"   {'-'*30} {'-'*20} {'-'*10} {'-'*20}")
        
        for col in table_info['columns']:
            nullable = "YES" if col['nullable'] else "NO"
            default = str(col['default'])[:20] if col['default'] else "—"
            data_type = col['type']
            if col['max_length']:
                data_type += f"({col['max_length']})"
            
            print(f"   {col['name']:<30} {data_type:<20} {nullable:<10} {default:<20}")
        
        if table_info['constraints']:
            print(f"\n   Constraints:")
            for const in table_info['constraints']:
                print(f"   • {const['name']}: {const['type']}")

if __name__ == "__main__":
    print("🔍 Introspecting tiki_warehouse database...")
    schema = get_database_schema()
    print_schema_info(schema)
    
    # Save to JSON
    if schema:
        with open('warehouse_schema.json', 'w') as f:
            json.dump(schema, f, indent=2)
        print(f"\n✅ Schema saved to warehouse_schema.json")
