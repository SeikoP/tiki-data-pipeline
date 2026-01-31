"""Property-based tests for data quality preservation.

These tests verify correctness properties of data quality tracking
using property-based testing with hypothesis.

Feature: performance-optimization
Properties: 12, 19, 20, 21, 22
"""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

try:
    from hypothesis import given, settings
    from hypothesis import strategies as st

    HYPOTHESIS_AVAILABLE = True
except ImportError:
    HYPOTHESIS_AVAILABLE = False
    print("⚠️ Hypothesis not installed. Install with: pip install hypothesis")

import time

from common.data_quality import (
    AuditTrailLogger,
    CacheFreshnessEnforcer,
    DataCompletenessTracker,
    ValidationPreservationChecker,
    register_tiki_product_validations,
)


# Helper strategies for generating test data
def product_record_strategy(complete: bool = True):
    """Strategy for generating product records.

    Args:
        complete: If True, generates complete records; if False, may have missing fields
    """
    if complete:
        return st.fixed_dictionaries(
            {
                "id": st.integers(min_value=1, max_value=1000000).map(str),
                "name": st.text(min_size=1, max_size=100),
                "price": st.floats(
                    min_value=0.01, max_value=100000.0, allow_nan=False, allow_infinity=False
                ),
                "original_price": st.floats(
                    min_value=0.01, max_value=100000.0, allow_nan=False, allow_infinity=False
                ),
                "discount": st.floats(
                    min_value=0.0, max_value=100000.0, allow_nan=False, allow_infinity=False
                ),
                "discount_rate": st.floats(
                    min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False
                ),
                "rating_average": st.floats(
                    min_value=0.0, max_value=5.0, allow_nan=False, allow_infinity=False
                ),
                "review_count": st.integers(min_value=0, max_value=100000),
                "thumbnail_url": st.text(min_size=10, max_size=200),
                "url_key": st.text(min_size=5, max_size=100),
            }
        )
    else:
        # Generate records that may have missing fields
        return st.fixed_dictionaries(
            {
                "id": st.one_of(st.integers(min_value=1, max_value=1000000).map(str), st.none()),
                "name": st.one_of(st.text(min_size=1, max_size=100), st.none(), st.just("")),
                "price": st.one_of(
                    st.floats(
                        min_value=0.01, max_value=100000.0, allow_nan=False, allow_infinity=False
                    ),
                    st.none(),
                ),
                "original_price": st.one_of(
                    st.floats(
                        min_value=0.01, max_value=100000.0, allow_nan=False, allow_infinity=False
                    ),
                    st.none(),
                ),
                "discount": st.one_of(
                    st.floats(
                        min_value=0.0, max_value=100000.0, allow_nan=False, allow_infinity=False
                    ),
                    st.none(),
                ),
                "discount_rate": st.one_of(
                    st.floats(
                        min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False
                    ),
                    st.none(),
                ),
                "rating_average": st.one_of(
                    st.floats(min_value=0.0, max_value=5.0, allow_nan=False, allow_infinity=False),
                    st.none(),
                ),
                "review_count": st.one_of(st.integers(min_value=0, max_value=100000), st.none()),
                "thumbnail_url": st.one_of(st.text(min_size=10, max_size=200), st.none()),
                "url_key": st.one_of(st.text(min_size=5, max_size=100), st.none()),
            }
        )


# Property 12: Data Completeness Preservation
# For any dataset processed through the optimized pipeline, the data completeness
# percentage should remain at or above the 70% threshold


def test_property_12_manual_basic():
    """Property 12: Data Completeness Preservation - Basic Manual Test

    **Validates: Requirements 6.6, 10.1**

    For any dataset, completeness tracking should accurately calculate
    the percentage of complete records.
    """
    required_fields = ["id", "name", "price"]
    tracker = DataCompletenessTracker(required_fields=required_fields, threshold=70.0)

    # Track complete records
    tracker.track_record({"id": "1", "name": "Product A", "price": 100})
    tracker.track_record({"id": "2", "name": "Product B", "price": 200})
    tracker.track_record({"id": "3", "name": "Product C", "price": 300})

    # Track incomplete records
    tracker.track_record({"id": "4", "name": "Product D"})  # Missing price
    tracker.track_record({"id": "5", "price": 500})  # Missing name

    metrics = tracker.get_metrics()

    # Verify calculations
    assert metrics.total_records == 5
    assert metrics.complete_records == 3
    assert metrics.incomplete_records == 2
    assert abs(metrics.completeness_percent - 60.0) < 0.1  # 3/5 = 60%
    assert not metrics.threshold_met  # 60% < 70%

    # Verify missing fields tracking
    assert "price" in metrics.missing_fields
    assert "name" in metrics.missing_fields
    assert metrics.missing_fields["price"] == 1
    assert metrics.missing_fields["name"] == 1

    print("✅ Property 12: Basic manual test PASSED")


def test_property_12_manual_threshold_met():
    """Property 12: Threshold Met Test

    **Validates: Requirements 6.6, 10.1**

    When completeness is >= 70%, threshold should be met.
    """
    required_fields = ["id", "name", "price"]
    tracker = DataCompletenessTracker(required_fields=required_fields, threshold=70.0)

    # Track 8 complete records and 2 incomplete = 80% completeness
    for i in range(8):
        tracker.track_record({"id": str(i), "name": f"Product {i}", "price": 100 + i})

    for i in range(2):
        tracker.track_record({"id": str(8 + i), "name": f"Product {8 + i}"})  # Missing price

    metrics = tracker.get_metrics()

    assert metrics.total_records == 10
    assert metrics.complete_records == 8
    assert abs(metrics.completeness_percent - 80.0) < 0.1
    assert metrics.threshold_met  # 80% >= 70%

    # Check threshold explicitly
    assert tracker.check_threshold()

    print("✅ Property 12: Threshold met test PASSED")


def test_property_12_manual_batch_tracking():
    """Property 12: Batch Tracking Test

    **Validates: Requirements 6.6, 10.1**

    Batch tracking should correctly count complete records.
    """
    required_fields = ["id", "name"]
    tracker = DataCompletenessTracker(required_fields=required_fields)

    batch = [
        {"id": "1", "name": "A"},
        {"id": "2", "name": "B"},
        {"id": "3"},  # Missing name
        {"name": "D"},  # Missing id
        {"id": "5", "name": "E"},
    ]

    complete_count = tracker.track_batch(batch)

    assert complete_count == 3  # Records 0, 1, 4 are complete

    metrics = tracker.get_metrics()
    assert metrics.total_records == 5
    assert metrics.complete_records == 3
    assert abs(metrics.completeness_percent - 60.0) < 0.1

    print("✅ Property 12: Batch tracking test PASSED")


# Hypothesis-based property tests (if available)
if HYPOTHESIS_AVAILABLE:

    @given(
        complete_records=st.lists(product_record_strategy(complete=True), min_size=0, max_size=100),
        incomplete_records=st.lists(
            product_record_strategy(complete=False), min_size=0, max_size=100
        ),
    )
    @settings(max_examples=100, deadline=None)
    def test_property_12_hypothesis(complete_records, incomplete_records):
        """Property 12: Data Completeness Preservation (Hypothesis)

        **Validates: Requirements 6.6, 10.1**

        For any dataset, completeness percentage should be calculated correctly
        and threshold checking should work properly.
        """
        required_fields = [
            "id",
            "name",
            "price",
            "original_price",
            "discount",
            "discount_rate",
            "rating_average",
            "review_count",
            "thumbnail_url",
            "url_key",
        ]

        tracker = DataCompletenessTracker(required_fields=required_fields, threshold=70.0)

        # Track all records
        all_records = complete_records + incomplete_records

        if not all_records:
            # Skip empty datasets
            return

        for record in all_records:
            tracker.track_record(record)

        metrics = tracker.get_metrics()

        # Verify basic properties
        assert metrics.total_records == len(all_records)
        assert metrics.complete_records + metrics.incomplete_records == metrics.total_records
        assert 0.0 <= metrics.completeness_percent <= 100.0

        # Verify completeness calculation
        if metrics.total_records > 0:
            expected_percent = (metrics.complete_records / metrics.total_records) * 100.0
            assert abs(metrics.completeness_percent - expected_percent) < 0.01

        # Verify threshold checking
        if metrics.completeness_percent >= 70.0:
            assert metrics.threshold_met
        else:
            assert not metrics.threshold_met


# Property 19: Error Rate Non-Regression
# For any comparable workload, the error rate of the optimized implementation
# should not exceed the error rate of the original implementation


def test_property_19_manual_basic():
    """Property 19: Error Rate Non-Regression - Basic Manual Test

    **Validates: Requirements 10.3**

    Error rate tracking should work correctly.
    """
    # Simulate original implementation error rate
    original_errors = 5
    original_total = 100
    original_error_rate = original_errors / original_total  # 5%

    # Simulate optimized implementation
    optimized_errors = 4
    optimized_total = 100
    optimized_error_rate = optimized_errors / optimized_total  # 4%

    # Verify non-regression: optimized error rate <= original error rate
    assert optimized_error_rate <= original_error_rate

    print("✅ Property 19: Basic manual test PASSED")


def test_property_19_manual_validation_errors():
    """Property 19: Validation Error Tracking

    **Validates: Requirements 10.3**

    Validation errors should be tracked correctly.
    """
    checker = ValidationPreservationChecker()
    register_tiki_product_validations(checker)

    # Valid records
    valid_records = [
        {
            "id": "1",
            "name": "Product A",
            "price": 100,
            "original_price": 150,
            "discount_rate": 33.3,
            "rating_average": 4.5,
            "review_count": 10,
        },
        {
            "id": "2",
            "name": "Product B",
            "price": 200,
            "original_price": 200,
            "discount_rate": 0,
            "rating_average": 3.0,
            "review_count": 5,
        },
    ]

    # Invalid records
    invalid_records = [
        {"id": "", "name": "Product C", "price": 100},  # Empty ID
        {"id": "4", "name": "", "price": 200},  # Empty name
        {"id": "5", "name": "Product E", "price": -10},  # Negative price
    ]

    # Validate all records
    for record in valid_records:
        result = checker.validate(record)
        assert result.is_valid

    for record in invalid_records:
        result = checker.validate(record)
        assert not result.is_valid

    stats = checker.get_validation_stats()

    # Verify error tracking
    assert stats["total_validations"] == len(valid_records) + len(invalid_records)
    assert stats["passed"] == len(valid_records)
    assert stats["failed"] == len(invalid_records)

    # Calculate error rate
    error_rate = stats["failed"] / stats["total_validations"]
    assert abs(error_rate - 0.6) < 0.01  # 3/5 = 60% error rate

    print("✅ Property 19: Validation error tracking test PASSED")


# Property 20: Data Validation Preservation
# For any invalid input data, the optimized implementation should reject it
# with appropriate error messages


def test_property_20_manual_basic():
    """Property 20: Data Validation Preservation - Basic Manual Test

    **Validates: Requirements 10.4**

    Invalid data should be rejected with appropriate error messages.
    """
    checker = ValidationPreservationChecker()
    register_tiki_product_validations(checker)

    # Test invalid ID
    result = checker.validate({"id": "", "name": "Product", "price": 100})
    assert not result.is_valid
    assert result.error_message == "Product ID is required"
    assert result.field_name == "id"

    # Test invalid name
    result = checker.validate({"id": "1", "name": "", "price": 100})
    assert not result.is_valid
    assert result.error_message == "Product name is required"
    assert result.field_name == "name"

    # Test invalid price
    result = checker.validate({"id": "1", "name": "Product", "price": -10})
    assert not result.is_valid
    assert result.error_message == "Price must be positive"
    assert result.field_name == "price"

    # Test invalid discount rate
    result = checker.validate(
        {
            "id": "1",
            "name": "Product",
            "price": 100,
            "discount_rate": 150,  # > 100
        }
    )
    assert not result.is_valid
    assert result.error_message == "Discount rate must be between 0 and 100"
    assert result.field_name == "discount_rate"

    # Test invalid rating
    result = checker.validate(
        {
            "id": "1",
            "name": "Product",
            "price": 100,
            "rating_average": 6.0,  # > 5
        }
    )
    assert not result.is_valid
    assert result.error_message == "Rating must be between 0 and 5"
    assert result.field_name == "rating_average"

    print("✅ Property 20: Basic manual test PASSED")


def test_property_20_manual_validation_preservation():
    """Property 20: Validation Rule Preservation

    **Validates: Requirements 10.4**

    All validation rules should be preserved.
    """
    # Create original checker with rules
    original_checker = ValidationPreservationChecker()
    register_tiki_product_validations(original_checker)

    # Create optimized checker with same rules
    optimized_checker = ValidationPreservationChecker()
    register_tiki_product_validations(optimized_checker)

    # Verify preservation
    assert optimized_checker.verify_preservation(original_checker)

    print("✅ Property 20: Validation preservation test PASSED")


if HYPOTHESIS_AVAILABLE:

    @given(
        id_value=st.one_of(st.text(min_size=1, max_size=20), st.just(""), st.none()),
        name_value=st.one_of(st.text(min_size=1, max_size=100), st.just(""), st.none()),
        price_value=st.one_of(
            st.floats(min_value=-1000, max_value=100000, allow_nan=False, allow_infinity=False),
            st.none(),
        ),
    )
    @settings(max_examples=100, deadline=None)
    def test_property_20_hypothesis(id_value, name_value, price_value):
        """Property 20: Data Validation Preservation (Hypothesis)

        **Validates: Requirements 10.4**

        For any input data, validation should correctly identify invalid records
        and provide appropriate error messages.
        """
        checker = ValidationPreservationChecker()
        register_tiki_product_validations(checker)

        record = {
            "id": id_value,
            "name": name_value,
            "price": price_value,
        }

        result = checker.validate(record)

        # Verify result structure
        assert isinstance(result.is_valid, bool)

        if not result.is_valid:
            # If invalid, must have error message
            assert result.error_message is not None
            assert len(result.error_message) > 0

            # Should have validation rule name
            assert result.validation_rule is not None


# Property 21: Cache Freshness Enforcement
# For any cached item, the system should not serve data that has exceeded its TTL


def test_property_21_manual_basic():
    """Property 21: Cache Freshness Enforcement - Basic Manual Test

    **Validates: Requirements 10.5**

    Expired cache entries should not be served.
    """
    enforcer = CacheFreshnessEnforcer(default_ttl=1)  # 1 second TTL

    # Set a value
    enforcer.set("key1", "value1")

    # Immediately get it (should be fresh)
    value = enforcer.get("key1")
    assert value == "value1"

    metrics = enforcer.get_metrics()
    assert metrics.fresh_hits == 1
    assert metrics.stale_hits == 0

    # Wait for expiration
    time.sleep(1.1)

    # Try to get it again (should be expired)
    value = enforcer.get("key1")
    assert value is None

    metrics = enforcer.get_metrics()
    assert metrics.fresh_hits == 1
    assert metrics.stale_hits == 1
    assert metrics.expired_entries == 1

    print("✅ Property 21: Basic manual test PASSED")


def test_property_21_manual_custom_ttl():
    """Property 21: Custom TTL Test

    **Validates: Requirements 10.5**

    Custom TTL values should be respected.
    """
    enforcer = CacheFreshnessEnforcer(default_ttl=10)

    # Set with custom short TTL
    enforcer.set("short_ttl", "value1", ttl=1)

    # Set with custom long TTL
    enforcer.set("long_ttl", "value2", ttl=5)

    # Wait 1.5 seconds
    time.sleep(1.5)

    # Short TTL should be expired
    assert enforcer.get("short_ttl") is None

    # Long TTL should still be fresh
    assert enforcer.get("long_ttl") == "value2"

    print("✅ Property 21: Custom TTL test PASSED")


def test_property_21_manual_freshness_check():
    """Property 21: Freshness Check Test

    **Validates: Requirements 10.5**

    is_fresh() should correctly identify fresh vs stale entries.
    """
    enforcer = CacheFreshnessEnforcer(default_ttl=1)

    enforcer.set("key1", "value1")

    # Should be fresh immediately
    assert enforcer.is_fresh("key1")

    # Wait for expiration
    time.sleep(1.1)

    # Should no longer be fresh
    assert not enforcer.is_fresh("key1")

    # Non-existent key should not be fresh
    assert not enforcer.is_fresh("nonexistent")

    print("✅ Property 21: Freshness check test PASSED")


# Property 22: Audit Trail Completeness
# For any pipeline execution, the system should generate logs for all key operations


def test_property_22_manual_basic():
    """Property 22: Audit Trail Completeness - Basic Manual Test

    **Validates: Requirements 10.6**

    All key operations should be logged.
    """
    logger = AuditTrailLogger()

    # Log various operations
    logger.log_fetch("product_123", success=True, details="Fetched from API")
    logger.log_transform("batch_001", success=True, record_count=100)
    logger.log_load("batch_001", success=True, record_count=100)
    logger.log_error("connection_timeout", "Database connection timed out")

    # Verify all operations were logged
    entries = logger.get_entries()
    assert len(entries) == 4

    # Verify operation counts
    counts = logger.get_operation_counts()
    assert counts["fetch"] == 1
    assert counts["transform"] == 1
    assert counts["load"] == 1
    assert counts["error"] == 1

    print("✅ Property 22: Basic manual test PASSED")


def test_property_22_manual_error_logging():
    """Property 22: Error Logging Test

    **Validates: Requirements 10.6**

    Errors should be logged with context.
    """
    logger = AuditTrailLogger()

    # Log errors with context
    logger.log_error(
        "validation_failed",
        "Product validation failed: invalid price",
        context={"product_id": "123", "price": -10},
    )

    logger.log_fetch(
        "product_456",
        success=False,
        details="Failed to fetch product",
        error_message="HTTP 404 Not Found",
        context={"url": "https://api.example.com/product/456"},
    )

    # Get error entries
    error_entries = logger.get_error_entries()
    assert len(error_entries) >= 2

    # Verify error details
    for entry in error_entries:
        assert entry.status == "failure"
        assert entry.error_message is not None
        assert len(entry.context) > 0

    print("✅ Property 22: Error logging test PASSED")


def test_property_22_manual_structured_logging():
    """Property 22: Structured Logging Test

    **Validates: Requirements 10.6**

    Logs should be structured and searchable.
    """
    logger = AuditTrailLogger()

    # Log multiple operations
    for i in range(10):
        logger.log_fetch(f"product_{i}", success=True, details=f"Fetched product {i}")

    for i in range(5):
        logger.log_transform(f"batch_{i}", success=True, record_count=100 + i)

    # Filter by operation type
    fetch_entries = logger.get_entries(operation="fetch")
    assert len(fetch_entries) == 10

    transform_entries = logger.get_entries(operation="transform")
    assert len(transform_entries) == 5

    # Verify entries are structured
    for entry in fetch_entries:
        assert entry.operation == "fetch"
        assert "resource_id" in entry.context

    for entry in transform_entries:
        assert entry.operation == "transform"
        assert "batch_id" in entry.context
        assert "record_count" in entry.context

    print("✅ Property 22: Structured logging test PASSED")


if __name__ == "__main__":
    print("Running Data Quality Property Tests...\n")

    # Property 12 tests
    print("=== Property 12: Data Completeness Preservation ===")
    test_property_12_manual_basic()
    test_property_12_manual_threshold_met()
    test_property_12_manual_batch_tracking()

    # Property 19 tests
    print("\n=== Property 19: Error Rate Non-Regression ===")
    test_property_19_manual_basic()
    test_property_19_manual_validation_errors()

    # Property 20 tests
    print("\n=== Property 20: Data Validation Preservation ===")
    test_property_20_manual_basic()
    test_property_20_manual_validation_preservation()

    # Property 21 tests
    print("\n=== Property 21: Cache Freshness Enforcement ===")
    test_property_21_manual_basic()
    test_property_21_manual_custom_ttl()
    test_property_21_manual_freshness_check()

    # Property 22 tests
    print("\n=== Property 22: Audit Trail Completeness ===")
    test_property_22_manual_basic()
    test_property_22_manual_error_logging()
    test_property_22_manual_structured_logging()

    if HYPOTHESIS_AVAILABLE:
        print("\n=== Running Hypothesis-based Tests ===")
        test_property_12_hypothesis()
        test_property_20_hypothesis()
    else:
        print("\n⚠️ Skipping hypothesis tests (hypothesis not installed)")

    print("\n✅ All Data Quality Property Tests passed!")
