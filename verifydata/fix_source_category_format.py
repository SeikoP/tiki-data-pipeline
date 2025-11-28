import json

print('=== FIX CATEGORY FORMAT IN SOURCE FILE ===')
print()

file_path = 'data/raw/categories_recursive_optimized.json'

# Load the source file
with open(file_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

print(f'📖 Loaded {len(data)} categories')

# Fix category_path entries with old format
fixed_count = 0
for cat in data:
    category_path = cat.get('category_path', [])
    
    # Check if first item is old format
    if category_path and category_path[0] == 'Nhà cửa - đời sống':
        category_path[0] = 'Nhà Cửa - Đời Sống'
        fixed_count += 1
    
    # Also check other items in path
    for i, item in enumerate(category_path):
        if item == 'Nhà cửa - đời sống':
            category_path[i] = 'Nhà Cửa - Đời Sống'

if fixed_count > 0:
    print(f'✅ Fixed {fixed_count} categories with old format in category_path')
    
    # Save back
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f'✅ Saved updated categories to {file_path}')
else:
    print('ℹ️  No categories with old format found')

print()
print('✅ Category format unification complete!')
