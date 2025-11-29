import psycopg2

conn = psycopg2.connect(
    host='localhost',
    database='crawl_data',
    user='postgres',
    password='postgres'
)
cur = conn.cursor()

# Analysis
products = [
    (480, "XE ĐẨY DỤNG CỤ 3 NGĂN CÓ NGĂN KÉO TỦ ĐỒ NGHỀ 3 NGĂN CÓ NGĂN KÉO CÓ TẤM TREO XE ĐẨY DỤNG CỤ CHO GAGARE"),
    (852, "XE ĐỰNG CÔNG CỤ TOTAL THPTC301 - HÀNG CHÍNH HÃNG"),
    (861, "Xe Đẩy Hành Lý - Xe Đẩy Hàng Gấp Gọn Thông Minh - PaKaSa - Hàng Chính Hãng"),
    (1125, "Xe kéo hàng mini gấp gọn đa năng cao cấp NiNDA NDX003- Hàng chính hãng"),
]

print("Analysis of 4 products with missing level 4:\n")

for pid, name in products:
    print(f"ID {pid}: {name}")
    
    # Check keywords
    lower_name = name.lower()
    
    if "công cụ" in lower_name or "dụng cụ" in lower_name or "nghề" in lower_name:
        suggested = "Phụ tùng xe đẩy hàng"
        print(f"  → Likely: {suggested}")
    elif "2 bánh" in lower_name or "2 banh" in lower_name:
        suggested = "Xe đẩy hàng 2 bánh"
        print(f"  → Likely: {suggested}")
    elif "4 bánh" in lower_name or "4 banh" in lower_name:
        suggested = "Xe đẩy hàng 4 bánh"
        print(f"  → Likely: {suggested}")
    elif "leo bậc" in lower_name or "bậc thang" in lower_name:
        suggested = "Xe đẩy hàng leo bậc thang"
        print(f"  → Likely: {suggested}")
    elif "hành lý" in lower_name or "gấp gọn" in lower_name or "mini" in lower_name:
        # These are small, collapsible - likely 2-wheel
        suggested = "Xe đẩy hàng 2 bánh"
        print(f"  → Likely: {suggested} (small/collapsible)")
    else:
        suggested = "Xe đẩy hàng 2 bánh"
        print(f"  → Uncertain - default to: {suggested}")
    print()

cur.close()
conn.close()
