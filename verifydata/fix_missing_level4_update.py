import psycopg2
import json

conn = psycopg2.connect(
    host='localhost',
    database='crawl_data',
    user='postgres',
    password='postgres'
)
cur = conn.cursor()

# 4 sản phẩm cần fix
fixes = [
    (480, ["Nhà Cửa - Đời Sống", "Sửa chữa nhà cửa", "Xe đẩy hàng", "Phụ tùng xe đẩy hàng"]),
    (852, ["Nhà Cửa - Đời Sống", "Sửa chữa nhà cửa", "Xe đẩy hàng", "Phụ tùng xe đẩy hàng"]),
    (861, ["Nhà Cửa - Đời Sống", "Sửa chữa nhà cửa", "Xe đẩy hàng", "Xe đẩy hàng 2 bánh"]),
    (1125, ["Nhà Cửa - Đời Sống", "Sửa chữa nhà cửa", "Xe đẩy hàng", "Xe đẩy hàng 2 bánh"]),
]

print("Fixing 4 products with missing level 4:\n")

for pid, new_path in fixes:
    cur.execute('''
        UPDATE products
        SET category_path = %s
        WHERE id = %s;
    ''', (json.dumps(new_path), pid))
    
    # Verify
    cur.execute('SELECT name, category_path FROM products WHERE id = %s;', (pid,))
    name, cat_path = cur.fetchone()
    print(f"✓ ID {pid}: {name[:50]}...")
    print(f"  Updated to: {cat_path}")
    print()

conn.commit()

# Check result
cur.execute('''
    SELECT 
        jsonb_array_length(category_path) as path_length,
        COUNT(*) as count
    FROM products
    WHERE category_path ->> 2 = 'Xe đẩy hàng'
    GROUP BY path_length
    ORDER BY path_length;
''')

print("\nFinal distribution:")
for path_len, count in cur.fetchall():
    print(f"  {path_len} levels: {count} products")

cur.close()
conn.close()

print("\n✅ All 4 products fixed!")
