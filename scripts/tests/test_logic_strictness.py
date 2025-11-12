"""Test if logic is too strict - check data loss scenarios"""
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../src'))

from pipelines.crawl.tiki.extract_category_link import build_hierarchical_structure

print("\n" + "="*70)
print("TESTING LOGIC STRICTNESS - CHECK FOR OVER-FILTERING")
print("="*70)

# Test scenarios
test_cases = [
    {
        "name": "Normal case",
        "data": [
            {"category_id": "1", "parent_id": None, "name": "Root"},
            {"category_id": "2", "parent_id": "1", "name": "Child"},
        ],
        "expect_output": 2
    },
    {
        "name": "Category without ID but has parent",
        "data": [
            {"category_id": "1", "parent_id": None, "name": "Root"},
            {"name": "NoID", "parent_id": "1"},  # Missing category_id
        ],
        "expect_output": 2  # Should keep both
    },
    {
        "name": "Missing parent - should treat as root",
        "data": [
            {"category_id": "1", "parent_id": None, "name": "Root"},
            {"category_id": "2", "parent_id": "999", "name": "MissingParent"},
        ],
        "expect_output": 2  # Should keep both (2 becomes root)
    },
    {
        "name": "Self-reference - should reject",
        "data": [
            {"category_id": "1", "parent_id": None, "name": "Root"},
            {"category_id": "2", "parent_id": "2", "name": "SelfRef"},  # Self-reference
        ],
        "expect_output": 1  # Should reject self-ref, keep 1
    },
    {
        "name": "Circular reference - should reject",
        "data": [
            {"category_id": "1", "parent_id": "2", "name": "A"},
            {"category_id": "2", "parent_id": "1", "name": "B"},
        ],
        "expect_output": 0  # Both circular, reject both
    },
    {
        "name": "Deep hierarchy with valid data",
        "data": [
            {"category_id": "1", "parent_id": None, "name": "L0"},
            {"category_id": "2", "parent_id": "1", "name": "L1"},
            {"category_id": "3", "parent_id": "2", "name": "L2"},
            {"category_id": "4", "parent_id": "3", "name": "L3"},
        ],
        "expect_output": 4  # All valid, keep all
    },
]

# Run tests
total_tests = len(test_cases)
passed_tests = 0

for i, test in enumerate(test_cases, 1):
    print(f"\n[Test {i}/{total_tests}] {test['name']}")
    print("-" * 70)
    
    # Build hierarchical
    try:
        result = build_hierarchical_structure(test['data'])
        
        # Count total items
        total_output = len(result)
        def count_subs(cats):
            count = 0
            for cat in cats:
                count += 1
                if cat.get('sub_categories'):
                    count += count_subs(cat['sub_categories'])
            return count
        
        total_output = count_subs(result)
        
        # Check result
        expected = test['expect_output']
        print(f"  Input: {len(test['data'])} items")
        print(f"  Output: {total_output} items")
        print(f"  Expected: {expected} items")
        
        if total_output == expected:
            print(f"  ✅ PASS")
            passed_tests += 1
        else:
            print(f"  ❌ FAIL - Got {total_output}, expected {expected}")
            # Show what we got
            print(f"  Root categories: {len(result)}")
            for root in result:
                print(f"    - {root.get('category_id')}: {root.get('name')}")
    
    except Exception as e:
        print(f"  ❌ ERROR: {e}")

# Summary
print("\n" + "="*70)
print("TEST SUMMARY")
print("="*70)
print(f"Passed: {passed_tests}/{total_tests}")

if passed_tests == total_tests:
    print("\n✅ Logic is appropriate - not too strict")
else:
    print(f"\n⚠️  Logic may be too strict or too loose")
    print(f"   {total_tests - passed_tests} test(s) failed")

print("\n" + "="*70)

