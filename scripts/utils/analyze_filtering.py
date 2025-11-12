"""Analyze filtering logic - show what's kept vs rejected"""
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../src'))

from pipelines.crawl.tiki.extract_category_link import (
    load_categories_from_json,
    build_hierarchical_structure
)

print("\n" + "="*70)
print("FILTERING ANALYSIS - WHAT'S KEPT VS REJECTED")
print("="*70)

# Load demo data
demo_categories = load_categories_from_json('data/raw/demo/demo_categories.json')
demo_sub_categories = load_categories_from_json('data/raw/demo/demo_sub_categories.json')

all_input = demo_categories + demo_sub_categories
print(f"\nTotal input: {len(all_input)} categories")

# Analyze input
print("\n" + "-"*70)
print("INPUT ANALYSIS:")
print("-"*70)

no_id = [c for c in all_input if not c.get('category_id')]
has_id = [c for c in all_input if c.get('category_id')]
no_parent_field = [c for c in all_input if 'parent_id' not in c]
has_parent_field = [c for c in all_input if 'parent_id' in c]
no_parent_value = [c for c in all_input if c.get('parent_id') is None]
self_ref = [c for c in all_input if c.get('category_id') == c.get('parent_id')]

print(f"✅ With category_id: {len(has_id)}")
print(f"❌ Without category_id: {len(no_id)}")
print(f"✅ With parent_id field: {len(has_parent_field)}")
print(f"❌ Without parent_id field: {len(no_parent_field)}")
print(f"✅ parent_id not None/empty: {len([c for c in all_input if c.get('parent_id')])}")
print(f"⚠️  Self-references: {len(self_ref)}")

# Now build hierarchical
print("\n" + "-"*70)
print("BUILDING HIERARCHICAL STRUCTURE:")
print("-"*70)

hierarchical = build_hierarchical_structure(all_input)

# Count what's in output
collected = []
def collect(cats):
    for cat in cats:
        collected.append(cat)
        if cat.get('sub_categories'):
            collect(cat['sub_categories'])

collect(hierarchical)

print(f"\n✅ Total in output: {len(collected)} categories")
print(f"   (From {len(all_input)} input)")

# What was kept
kept_ids = {c.get('category_id') for c in collected if c.get('category_id')}
lost_ids = {c.get('category_id') for c in all_input if c.get('category_id')} - kept_ids

if lost_ids:
    print(f"\n❌ LOST (not in output): {len(lost_ids)} categories")
    for cat_id in list(lost_ids)[:5]:
        cat = next((c for c in all_input if c.get('category_id') == cat_id), None)
        if cat:
            print(f"   - {cat_id}: {cat.get('name', 'N/A')}")
    if len(lost_ids) > 5:
        print(f"   ... and {len(lost_ids) - 5} more")
else:
    print(f"\n✅ PERFECT: 0 categories lost")

# Summary
print("\n" + "="*70)
print("SUMMARY")
print("="*70)
print(f"Input: {len(all_input)}")
print(f"Output: {len(collected)}")
print(f"Lost: {len(lost_ids)}")
print(f"Filtering efficiency: {(len(collected) / len(all_input) * 100):.1f}%")

if len(lost_ids) == 0:
    print("\n✅ NO DATA LOSS - All valid categories preserved")
else:
    print(f"\n⚠️  WARNING: {len(lost_ids)} categories lost during filtering")

print("\n" + "="*70)

