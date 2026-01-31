"""Example demonstrating data quality preservation features.

This example shows how to use the data quality tracking components:
- DataCompletenessTracker: Track and alert on data completeness
- ValidationPreservationChecker: Ensure validation rules are preserved
- CacheFreshnessEnforcer: Enforce TTL on cached data
- AuditTrailLogger: Log all key operations for audit trails
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from common.data_quality import (
    DataCompletenessTracker,
    ValidationPreservationChecker,
    CacheFreshnessEnforcer,
    AuditTrailLogger,
    register_tiki_product_validations,
    get_global_completeness_tracker,
    get_global_validation_checker,
    get_global_cache_enforcer,
    get_global_audit_logger,
)
import time


def example_completeness_tracking():
    """Example: Track data completeness and alert on low completeness."""
    print("=" * 60)
    print("EXAMPLE 1: Data Completeness Tracking")
    print("=" * 60)
    
    # Define required fields for Tiki products
    required_fields = ["id", "name", "price", "original_price", "rating_average"]
    
    # Create tracker with 70% threshold
    tracker = DataCompletenessTracker(required_fields=required_fields, threshold=70.0)
    
    # Simulate processing products
    products = [
        {"id": "1", "name": "Product A", "price": 100, "original_price": 150, "rating_average": 4.5},
        {"id": "2", "name": "Product B", "price": 200, "original_price": 200, "rating_average": 3.8},
        {"id": "3", "name": "Product C", "price": 300},  # Missing fields
        {"id": "4", "name": "Product D", "price": 400, "original_price": 500, "rating_average": 4.2},
        {"id": "5", "price": 500},  # Missing name
    ]
    
    print(f"\nProcessing {len(products)} products...")
    
    for product in products:
        is_complete = tracker.track_record(product)
        if not is_complete:
            print(f"  ⚠️ Incomplete record: {product.get('id', 'unknown')}")
    
    # Get metrics
    metrics = tracker.get_metrics()
    
    print(f"\nResults:")
    print(f"  Total Records: {metrics.total_records}")
    print(f"  Complete: {metrics.complete_records}")
    print(f"  Incomplete: {metrics.incomplete_records}")
    print(f"  Completeness: {metrics.completeness_percent:.2f}%")
    
    if metrics.threshold_met:
        print(f"  ✅ Threshold met ({metrics.completeness_percent:.2f}% >= 70%)")
    else:
        print(f"  ❌ Threshold NOT met ({metrics.completeness_percent:.2f}% < 70%)")
    
    # Show full report
    print("\n" + tracker.generate_report())


def example_validation_preservation():
    """Example: Validate data and ensure validation rules are preserved."""
    print("\n" + "=" * 60)
    print("EXAMPLE 2: Validation Preservation")
    print("=" * 60)
    
    # Create checker and register Tiki product validations
    checker = ValidationPreservationChecker()
    register_tiki_product_validations(checker)
    
    # Test valid product
    valid_product = {
        "id": "123",
        "name": "Valid Product",
        "price": 100.0,
        "original_price": 150.0,
        "discount_rate": 33.3,
        "rating_average": 4.5,
        "review_count": 100,
    }
    
    print("\nValidating valid product...")
    result = checker.validate(valid_product)
    if result.is_valid:
        print("  ✅ Product is valid")
    else:
        print(f"  ❌ Validation failed: {result.error_message}")
    
    # Test invalid products
    invalid_products = [
        {"id": "", "name": "Product", "price": 100},  # Empty ID
        {"id": "2", "name": "", "price": 200},  # Empty name
        {"id": "3", "name": "Product", "price": -10},  # Negative price
        {"id": "4", "name": "Product", "price": 100, "discount_rate": 150},  # Invalid discount
        {"id": "5", "name": "Product", "price": 100, "rating_average": 6.0},  # Invalid rating
    ]
    
    print("\nValidating invalid products...")
    for product in invalid_products:
        result = checker.validate(product)
        if not result.is_valid:
            print(f"  ❌ {result.error_message} (field: {result.field_name})")
    
    # Show validation stats
    print("\n" + checker.generate_report())


def example_cache_freshness():
    """Example: Enforce cache freshness with TTL."""
    print("\n" + "=" * 60)
    print("EXAMPLE 3: Cache Freshness Enforcement")
    print("=" * 60)
    
    # Create enforcer with 2-second default TTL
    enforcer = CacheFreshnessEnforcer(default_ttl=2)
    
    print("\nSetting cache entries...")
    enforcer.set("product_123", {"name": "Product A", "price": 100})
    enforcer.set("product_456", {"name": "Product B", "price": 200}, ttl=1)  # Custom short TTL
    
    # Get immediately (should be fresh)
    print("\nGetting entries immediately:")
    value = enforcer.get("product_123")
    print(f"  product_123: {value} (fresh)")
    
    value = enforcer.get("product_456")
    print(f"  product_456: {value} (fresh)")
    
    # Wait 1.5 seconds
    print("\nWaiting 1.5 seconds...")
    time.sleep(1.5)
    
    # Get again
    print("\nGetting entries after 1.5 seconds:")
    value = enforcer.get("product_123")
    print(f"  product_123: {value} (still fresh, TTL=2s)")
    
    value = enforcer.get("product_456")
    print(f"  product_456: {value} (expired, TTL=1s)")
    
    # Wait another 1 second
    print("\nWaiting another 1 second...")
    time.sleep(1)
    
    # Get again
    print("\nGetting entries after 2.5 seconds total:")
    value = enforcer.get("product_123")
    print(f"  product_123: {value} (expired, TTL=2s)")
    
    # Show freshness report
    print("\n" + enforcer.generate_report())


def example_audit_trail():
    """Example: Log operations for audit trail."""
    print("\n" + "=" * 60)
    print("EXAMPLE 4: Audit Trail Logging")
    print("=" * 60)
    
    # Create audit logger
    logger = AuditTrailLogger()
    
    print("\nSimulating pipeline operations...")
    
    # Log fetch operations
    logger.log_fetch("product_123", success=True, details="Fetched from API")
    logger.log_fetch("product_456", success=True, details="Fetched from cache")
    logger.log_fetch("product_789", success=False, details="Failed to fetch",
                    error_message="HTTP 404 Not Found")
    
    # Log transform operations
    logger.log_transform("batch_001", success=True, record_count=100,
                        details="Transformed product batch")
    logger.log_transform("batch_002", success=False, record_count=50,
                        details="Transform failed",
                        error_message="Invalid data format")
    
    # Log load operations
    logger.log_load("batch_001", success=True, record_count=100,
                   details="Loaded to database")
    
    # Log errors
    logger.log_error("database_connection", "Connection timeout after 30s",
                    context={"host": "localhost", "port": 5432})
    
    print("\nOperation counts:")
    counts = logger.get_operation_counts()
    for op_type, count in counts.items():
        if count > 0:
            print(f"  {op_type}: {count}")
    
    # Show recent errors
    print("\nRecent errors:")
    error_entries = logger.get_error_entries(limit=5)
    for entry in error_entries:
        print(f"  [{entry.timestamp.strftime('%H:%M:%S')}] {entry.details}")
        if entry.error_message:
            print(f"    → {entry.error_message}")
    
    # Export to file
    output_file = Path(__file__).parent / "audit_trail_example.json"
    logger.export_to_file(str(output_file), format="json")
    print(f"\n✅ Audit trail exported to: {output_file}")
    
    # Show full report
    print("\n" + logger.generate_report())


def example_integrated_usage():
    """Example: Using all components together in a pipeline."""
    print("\n" + "=" * 60)
    print("EXAMPLE 5: Integrated Usage")
    print("=" * 60)
    
    # Get global instances
    completeness_tracker = get_global_completeness_tracker()
    validation_checker = get_global_validation_checker()
    cache_enforcer = get_global_cache_enforcer(default_ttl=300)
    audit_logger = get_global_audit_logger()
    
    print("\nSimulating integrated pipeline run...")
    
    # Simulate fetching products
    products = [
        {"id": "1", "name": "Product A", "price": 100, "original_price": 150,
         "discount_rate": 33.3, "rating_average": 4.5, "review_count": 100,
         "thumbnail_url": "http://example.com/a.jpg", "url_key": "product-a"},
        {"id": "2", "name": "Product B", "price": 200, "original_price": 200,
         "discount_rate": 0, "rating_average": 3.8, "review_count": 50,
         "thumbnail_url": "http://example.com/b.jpg", "url_key": "product-b"},
        {"id": "3", "name": "Product C", "price": 300},  # Incomplete
    ]
    
    valid_count = 0
    complete_count = 0
    
    for product in products:
        product_id = product.get("id", "unknown")
        
        # Log fetch
        audit_logger.log_fetch(product_id, success=True, details="Fetched from API")
        
        # Validate
        validation_result = validation_checker.validate(product)
        if validation_result.is_valid:
            valid_count += 1
            
            # Check completeness
            is_complete = completeness_tracker.track_record(product)
            if is_complete:
                complete_count += 1
                
                # Cache the product
                cache_enforcer.set(f"product_{product_id}", product)
                
                # Log success
                audit_logger.log_transform(f"product_{product_id}", success=True,
                                          record_count=1, details="Processed successfully")
            else:
                audit_logger.log_operation("transform", "warning",
                                          f"Product {product_id} is incomplete")
        else:
            audit_logger.log_error("validation_failed",
                                  validation_result.error_message,
                                  context={"product_id": product_id})
    
    print(f"\nProcessed {len(products)} products:")
    print(f"  Valid: {valid_count}")
    print(f"  Complete: {complete_count}")
    print(f"  Cached: {cache_enforcer.size()}")
    
    # Check completeness threshold
    metrics = completeness_tracker.get_metrics()
    if metrics.threshold_met:
        print(f"  ✅ Completeness threshold met: {metrics.completeness_percent:.2f}%")
    else:
        print(f"  ⚠️ Completeness below threshold: {metrics.completeness_percent:.2f}%")
    
    print("\n✅ Integrated pipeline run complete")


if __name__ == "__main__":
    print("Data Quality Preservation Examples")
    print("=" * 60)
    
    example_completeness_tracking()
    example_validation_preservation()
    example_cache_freshness()
    example_audit_trail()
    example_integrated_usage()
    
    print("\n" + "=" * 60)
    print("All examples completed successfully!")
    print("=" * 60)
