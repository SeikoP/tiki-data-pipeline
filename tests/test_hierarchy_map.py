#!/usr/bin/env python
import json

# Load hierarchy map
with open("data/raw/category_hierarchy_map.json", encoding="utf-8") as f:
    hierarchy = json.load(f)

# Find one level 1 category and check its hierarchy
for info in hierarchy.values():
    if info.get("level") == 1:
        print("Level 1 Category:")
        print(f"  Name: {info['name']}")
        print(f"  Parent URL: {info.get('parent_url')}")
        print(f"  Parent Chain: {info.get('parent_chain')}")

        # Get root parent info
        parent_chain = info.get("parent_chain", [])
        if parent_chain:
            root_url = parent_chain[0]
            root_info = hierarchy.get(root_url)
            print(f"  Root Parent: {root_info.get('name')}")
        break

# Test finding a level 2 category
print("\n" + "=" * 60)
for info in hierarchy.values():
    if info.get("level") == 2:
        print("Level 2 Category:")
        print(f"  Name: {info['name']}")
        parent_chain = info.get("parent_chain", [])
        print(f"  Parent Chain ({len(parent_chain)} parents):")
        for i, parent_url in enumerate(parent_chain):
            parent_info = hierarchy.get(parent_url)
            level = parent_info.get("level", "unknown")
            name = parent_info.get("name", "unknown")
            print(f"    {i}. Level {level}: {name}")
        break
