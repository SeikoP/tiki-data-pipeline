import json

with open('data/raw/category_hierarchy_map.json', encoding='utf-8') as f:
    hierarchy = json.load(f)

print('Looking for categories containing "Ngoài trời"...')
for url, info in hierarchy.items():
    if 'Ngoài trời' in info['name']:
        print(f"  {info['name']}")
        print(f"    Level: {info['level']}")
        parent_chain = info['parent_chain']
        print(f"    Parents: {len(parent_chain)}")
        if parent_chain:
            root = hierarchy.get(parent_chain[0])
            print(f"    Root parent: {root['name'] if root else 'N/A'}")
        print()
