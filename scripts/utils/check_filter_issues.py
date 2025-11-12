"""Check if filtering logic loses any data"""
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../src'))

from pipelines.crawl.tiki.extract_category_link import load_categories_from_json

# Load all demo data
demo_categories = load_categories_from_json('data/raw/demo/demo_categories.json')
demo_sub_categories = load_categories_from_json('data/raw/demo/demo_sub_categories.json')

print("\n" + "="*70)
print("CHECKING FOR DATA LOSS IN FILTERING")
print("="*70)

# Combine all data
all_data = demo_categories + demo_sub_categories
print(f"\nTotal input categories: {len(all_data)}")

# Check for potential filtering issues
issues = {
    'missing_id': [],
    'missing_parent_field': [],
    'empty_parent': [],
    'self_reference': [],
    'no_name': []
}

for cat in all_data:
    cat_id = cat.get('category_id')
    parent_id = cat.get('parent_id')
    name = cat.get('name')
    
    # Check 1: Missing category_id
    if not cat_id:
        issues['missing_id'].append(cat)
    
    # Check 2: Missing parent_id field
    if 'parent_id' not in cat:
        issues['missing_parent_field'].append(cat)
    
    # Check 3: Empty parent_id string
    if cat_id and parent_id == '':
        issues['empty_parent'].append(cat)
    
    # Check 4: Self-reference
    if cat_id and parent_id == cat_id:
        issues['self_reference'].append(cat)
    
    # Check 5: Missing name
    if not name:
        issues['no_name'].append(cat)

print("\n" + "-"*70)
print("POTENTIAL FILTER ISSUES:")
print("-"*70)

for issue_type, items in issues.items():
    if items:
        print(f"\n❌ {issue_type.upper()}: {len(items)} items")
        for i, item in enumerate(items[:3], 1):
            print(f"   {i}. ID: {item.get('category_id')}, Parent: {item.get('parent_id')}, Name: {item.get('name', 'N/A')}")
        if len(items) > 3:
            print(f"   ... and {len(items) - 3} more")
    else:
        print(f"✅ {issue_type.upper()}: None")

# Check which would be filtered out
print("\n" + "-"*70)
print("ITEMS THAT WOULD BE FILTERED OUT:")
print("-"*70)

filtered_out = []

for cat in all_data:
    cat_id = cat.get('category_id')
    parent_id = cat.get('parent_id')
    
    # Check filtering logic from extract_category_link.py
    
    # Would be skipped: no category_id
    if not cat_id:
        filtered_out.append((cat, 'no category_id'))
        continue
    
    # Would be skipped: self-reference
    if parent_id == cat_id:
        filtered_out.append((cat, 'self-reference'))
        continue
    
    # Assuming no circular references for this demo
    
    # Would be skipped: missing parent (but treated as root, not skipped)
    # So this is OK

print(f"\nTotal that would be filtered out: {len(filtered_out)}")
for i, (cat, reason) in enumerate(filtered_out[:5], 1):
    print(f"  {i}. {cat.get('category_id')} - Reason: {reason}")
if len(filtered_out) > 5:
    print(f"  ... and {len(filtered_out) - 5} more")

# Summary
print("\n" + "="*70)
print("SUMMARY")
print("="*70)
print(f"Input total: {len(all_data)}")
print(f"Would be filtered out: {len(filtered_out)}")
print(f"Would remain: {len(all_data) - len(filtered_out)}")

if filtered_out:
    print(f"\n⚠️  WARNING: {len(filtered_out)} items would be filtered out")
    print("   Review the reasons above to ensure they are legitimate")
else:
    print(f"\n✅ No data would be lost in filtering")

print("\n" + "="*70)

