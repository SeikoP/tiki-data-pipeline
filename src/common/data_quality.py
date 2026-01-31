"""Data quality preservation and tracking for pipeline operations.

This module provides utilities for tracking data completeness, validation preservation,
and quality metrics to ensure optimizations don't compromise data integrity.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class DataCompletenessMetrics:
    """Metrics for tracking data completeness.

    Attributes:
        total_records: Total number of records processed
        complete_records: Number of records with all required fields
        incomplete_records: Number of records missing required fields
        completeness_percent: Percentage of complete records (0.0-100.0)
        missing_fields: Dictionary mapping field names to count of missing occurrences
        threshold_met: Whether completeness meets the 70% threshold
    """

    total_records: int = 0
    complete_records: int = 0
    incomplete_records: int = 0
    completeness_percent: float = 0.0
    missing_fields: dict[str, int] = field(default_factory=dict)
    threshold_met: bool = False

    def calculate_completeness(self) -> None:
        """Calculate completeness percentage and check threshold."""
        if self.total_records > 0:
            self.completeness_percent = (self.complete_records / self.total_records) * 100.0
            self.threshold_met = self.completeness_percent >= 70.0
        else:
            self.completeness_percent = 0.0
            self.threshold_met = False

    def to_dict(self) -> dict:
        """Convert metrics to dictionary for serialization."""
        return {
            "total_records": self.total_records,
            "complete_records": self.complete_records,
            "incomplete_records": self.incomplete_records,
            "completeness_percent": round(self.completeness_percent, 2),
            "missing_fields": dict(self.missing_fields),
            "threshold_met": self.threshold_met,
        }


class DataCompletenessTracker:
    """Tracks data completeness for pipeline runs.

    Monitors the percentage of complete records and alerts if completeness
    drops below the 70% threshold.

    Example:
        >>> tracker = DataCompletenessTracker(required_fields=["id", "name", "price"])
        >>> tracker.track_record({"id": "1", "name": "Product", "price": 100})
        >>> tracker.track_record({"id": "2", "name": "Product"})  # Missing price
        >>> metrics = tracker.get_metrics()
        >>> print(f"Completeness: {metrics.completeness_percent}%")
        >>> if not metrics.threshold_met:
        ...     print("⚠️ Completeness below 70% threshold!")
    """

    def __init__(self, required_fields: list[str], threshold: float = 70.0):
        """Initialize the completeness tracker.

        Args:
            required_fields: List of field names that must be present for a record to be complete
            threshold: Minimum completeness percentage (default: 70.0)
        """
        self.required_fields = set(required_fields)
        self.threshold = threshold
        self.metrics = DataCompletenessMetrics()
        self._alerts_triggered = False

    def track_record(self, record: dict[str, Any]) -> bool:
        """Track a single record and check completeness.

        Args:
            record: Dictionary representing a data record

        Returns:
            True if record is complete, False otherwise
        """
        self.metrics.total_records += 1

        # Check for missing fields
        missing = []
        for field_name in self.required_fields:
            value = record.get(field_name)
            # Consider None, empty string, or missing key as incomplete
            if value is None or value == "":
                missing.append(field)
                self.metrics.missing_fields[field] = self.metrics.missing_fields.get(field, 0) + 1

        if missing:
            self.metrics.incomplete_records += 1
            return False
        else:
            self.metrics.complete_records += 1
            return True

    def track_batch(self, records: list[dict[str, Any]]) -> int:
        """Track a batch of records.

        Args:
            records: List of record dictionaries

        Returns:
            Number of complete records in the batch
        """
        complete_count = 0
        for record in records:
            if self.track_record(record):
                complete_count += 1
        return complete_count

    def get_metrics(self) -> DataCompletenessMetrics:
        """Get current completeness metrics.

        Returns:
            DataCompletenessMetrics with calculated completeness percentage
        """
        self.metrics.calculate_completeness()
        return self.metrics

    def check_threshold(self) -> bool:
        """Check if completeness meets the threshold.

        Returns:
            True if completeness >= threshold, False otherwise
        """
        self.metrics.calculate_completeness()

        if not self.metrics.threshold_met and not self._alerts_triggered:
            self._trigger_alert()

        return self.metrics.threshold_met

    def _trigger_alert(self) -> None:
        """Trigger alert for low completeness."""
        self._alerts_triggered = True
        logger.warning(
            f"⚠️ DATA COMPLETENESS ALERT: Completeness is {self.metrics.completeness_percent:.2f}%, "
            f"below threshold of {self.threshold}%"
        )
        logger.warning(
            f"   Complete records: {self.metrics.complete_records}/{self.metrics.total_records}"
        )
        if self.metrics.missing_fields:
            logger.warning("   Most common missing fields:")
            sorted_fields = sorted(
                self.metrics.missing_fields.items(), key=lambda x: x[1], reverse=True
            )
            for field, count in sorted_fields[:5]:
                logger.warning(f"     - {field}: {count} occurrences")

    def reset(self) -> None:
        """Reset all metrics and alerts."""
        self.metrics = DataCompletenessMetrics()
        self._alerts_triggered = False

    def generate_report(self) -> str:
        """Generate a human-readable completeness report.

        Returns:
            Formatted string report
        """
        self.metrics.calculate_completeness()

        lines = ["=" * 60]
        lines.append("DATA COMPLETENESS REPORT")
        lines.append("=" * 60)
        lines.append("")

        lines.append(f"Total Records: {self.metrics.total_records:,}")
        lines.append(f"Complete Records: {self.metrics.complete_records:,}")
        lines.append(f"Incomplete Records: {self.metrics.incomplete_records:,}")
        lines.append(f"Completeness: {self.metrics.completeness_percent:.2f}%")
        lines.append("")

        # Threshold status
        if self.metrics.threshold_met:
            lines.append(
                f"✅ Threshold Met: {self.metrics.completeness_percent:.2f}% >= {self.threshold}%"
            )
        else:
            lines.append(
                f"❌ Threshold NOT Met: {self.metrics.completeness_percent:.2f}% < {self.threshold}%"
            )
        lines.append("")

        # Missing fields breakdown
        if self.metrics.missing_fields:
            lines.append("Missing Fields Breakdown:")
            sorted_fields = sorted(
                self.metrics.missing_fields.items(), key=lambda x: x[1], reverse=True
            )
            for field, count in sorted_fields:
                percentage = (count / self.metrics.total_records) * 100
                lines.append(f"  {field}: {count:,} ({percentage:.2f}%)")
        else:
            lines.append("No missing fields detected.")

        lines.append("")
        lines.append("=" * 60)

        return "\n".join(lines)


# Global completeness tracker instance
_global_completeness_tracker: DataCompletenessTracker | None = None


def get_global_completeness_tracker(
    required_fields: list[str] | None = None, threshold: float = 70.0
) -> DataCompletenessTracker:
    """Get the global completeness tracker instance.

    Creates the instance if it doesn't exist.

    Args:
        required_fields: List of required field names (only used on first call)
        threshold: Completeness threshold percentage (only used on first call)

    Returns:
        Global DataCompletenessTracker instance
    """
    global _global_completeness_tracker

    if _global_completeness_tracker is None:
        if required_fields is None:
            # Default required fields for Tiki products
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
        _global_completeness_tracker = DataCompletenessTracker(
            required_fields=required_fields, threshold=threshold
        )

    return _global_completeness_tracker


def reset_global_completeness_tracker() -> None:
    """Reset the global completeness tracker."""
    global _global_completeness_tracker
    if _global_completeness_tracker is not None:
        _global_completeness_tracker.reset()


@dataclass
class ValidationResult:
    """Result of a validation check.

    Attributes:
        is_valid: Whether the data passed validation
        error_message: Error message if validation failed
        field_name: Name of the field that failed validation (if applicable)
        validation_rule: Name of the validation rule that was applied
    """

    is_valid: bool
    error_message: str | None = None
    field_name: str | None = None
    validation_rule: str | None = None

    def to_dict(self) -> dict:
        """Convert result to dictionary."""
        return {
            "is_valid": self.is_valid,
            "error_message": self.error_message,
            "field_name": self.field_name,
            "validation_rule": self.validation_rule,
        }


class ValidationPreservationChecker:
    """Ensures validation logic is preserved during optimizations.

    Tracks validation rules and ensures that invalid data is still rejected
    with appropriate error messages after optimizations are applied.

    Example:
        >>> checker = ValidationPreservationChecker()
        >>> checker.register_validation("price_positive", lambda x: x.get("price", 0) > 0, "Price must be positive")
        >>> result = checker.validate({"price": -10})
        >>> if not result.is_valid:
        ...     print(f"Validation failed: {result.error_message}")
    """

    def __init__(self):
        """Initialize the validation checker."""
        self._validation_rules: dict[str, tuple] = {}
        self._validation_stats = {
            "total_validations": 0,
            "passed": 0,
            "failed": 0,
            "rules_applied": {},
        }

    def register_validation(
        self,
        rule_name: str,
        validation_func: callable,
        error_message: str,
        field_name: str | None = None,
    ) -> None:
        """Register a validation rule.

        Args:
            rule_name: Unique name for the validation rule
            validation_func: Function that takes a record and returns True if valid
            error_message: Error message to return if validation fails
            field_name: Optional field name this validation applies to
        """
        self._validation_rules[rule_name] = (validation_func, error_message, field_name)
        self._validation_stats["rules_applied"][rule_name] = {"passed": 0, "failed": 0}

    def validate(self, record: dict[str, Any], rule_name: str | None = None) -> ValidationResult:
        """Validate a record against registered rules.

        Args:
            record: Dictionary representing a data record
            rule_name: Optional specific rule to apply (if None, applies all rules)

        Returns:
            ValidationResult indicating success or failure
        """
        self._validation_stats["total_validations"] += 1

        # Determine which rules to apply
        rules_to_apply = {}
        if rule_name:
            if rule_name not in self._validation_rules:
                raise ValueError(f"Unknown validation rule: {rule_name}")
            rules_to_apply[rule_name] = self._validation_rules[rule_name]
        else:
            rules_to_apply = self._validation_rules

        # Apply validation rules
        for name, (func, error_msg, _field_name) in rules_to_apply.items():
            try:
                is_valid = func(record)

                if not is_valid:
                    self._validation_stats["failed"] += 1
                    self._validation_stats["rules_applied"][name]["failed"] += 1
                    return ValidationResult(
                        is_valid=False,
                        error_message=error_msg,
                        field_name=field,
                        validation_rule=name,
                    )
                else:
                    self._validation_stats["rules_applied"][name]["passed"] += 1
            except Exception as e:
                # Validation function raised an exception
                self._validation_stats["failed"] += 1
                self._validation_stats["rules_applied"][name]["failed"] += 1
                return ValidationResult(
                    is_valid=False,
                    error_message=f"Validation error: {str(e)}",
                    field_name=field,
                    validation_rule=name,
                )

        # All validations passed
        self._validation_stats["passed"] += 1
        return ValidationResult(is_valid=True)

    def validate_batch(
        self, records: list[dict[str, Any]], rule_name: str | None = None
    ) -> list[ValidationResult]:
        """Validate a batch of records.

        Args:
            records: List of record dictionaries
            rule_name: Optional specific rule to apply

        Returns:
            List of ValidationResult objects, one per record
        """
        return [self.validate(record, rule_name) for record in records]

    def get_validation_stats(self) -> dict:
        """Get validation statistics.

        Returns:
            Dictionary with validation statistics
        """
        return dict(self._validation_stats)

    def get_failed_validations(self) -> list[str]:
        """Get list of validation rules that have failures.

        Returns:
            List of rule names that have recorded failures
        """
        failed_rules = []
        for rule_name, stats in self._validation_stats["rules_applied"].items():
            if stats["failed"] > 0:
                failed_rules.append(rule_name)
        return failed_rules

    def verify_preservation(self, original_checker: "ValidationPreservationChecker") -> bool:
        """Verify that validation rules are preserved from original implementation.

        Args:
            original_checker: ValidationPreservationChecker from original implementation

        Returns:
            True if all original rules are present, False otherwise
        """
        original_rules = set(original_checker._validation_rules.keys())
        current_rules = set(self._validation_rules.keys())

        missing_rules = original_rules - current_rules

        if missing_rules:
            logger.error(
                f"❌ VALIDATION PRESERVATION FAILURE: Missing validation rules: {missing_rules}"
            )
            return False

        logger.info("✅ All validation rules preserved")
        return True

    def generate_report(self) -> str:
        """Generate a human-readable validation report.

        Returns:
            Formatted string report
        """
        lines = ["=" * 60]
        lines.append("VALIDATION PRESERVATION REPORT")
        lines.append("=" * 60)
        lines.append("")

        lines.append(f"Total Validations: {self._validation_stats['total_validations']:,}")
        lines.append(f"Passed: {self._validation_stats['passed']:,}")
        lines.append(f"Failed: {self._validation_stats['failed']:,}")
        lines.append("")

        # Calculate pass rate
        if self._validation_stats["total_validations"] > 0:
            pass_rate = (
                self._validation_stats["passed"] / self._validation_stats["total_validations"]
            ) * 100
            lines.append(f"Pass Rate: {pass_rate:.2f}%")
        else:
            lines.append("Pass Rate: N/A (no validations performed)")
        lines.append("")

        # Rules breakdown
        lines.append("Validation Rules:")
        for rule_name, stats in self._validation_stats["rules_applied"].items():
            total = stats["passed"] + stats["failed"]
            if total > 0:
                rule_pass_rate = (stats["passed"] / total) * 100
                lines.append(
                    f"  {rule_name}: {stats['passed']}/{total} passed ({rule_pass_rate:.2f}%)"
                )
            else:
                lines.append(f"  {rule_name}: Not applied")

        lines.append("")
        lines.append("=" * 60)

        return "\n".join(lines)


# Common validation rules for Tiki products
def register_tiki_product_validations(checker: ValidationPreservationChecker) -> None:
    """Register common validation rules for Tiki product data.

    Args:
        checker: ValidationPreservationChecker instance to register rules with
    """
    # ID validation
    checker.register_validation(
        "id_required",
        lambda x: x.get("id") is not None and str(x.get("id")).strip() != "",
        "Product ID is required",
        "id",
    )

    # Name validation
    checker.register_validation(
        "name_required",
        lambda x: x.get("name") is not None and str(x.get("name")).strip() != "",
        "Product name is required",
        "name",
    )

    # Price validation
    checker.register_validation(
        "price_positive",
        lambda x: x.get("price") is not None and float(x.get("price", 0)) > 0,
        "Price must be positive",
        "price",
    )

    # Original price validation
    checker.register_validation(
        "original_price_valid",
        lambda x: (
            x.get("original_price") is None
            or float(x.get("original_price", 0)) >= float(x.get("price", 0))
        ),
        "Original price must be >= current price",
        "original_price",
    )

    # Discount rate validation
    checker.register_validation(
        "discount_rate_range",
        lambda x: (x.get("discount_rate") is None or 0 <= float(x.get("discount_rate", 0)) <= 100),
        "Discount rate must be between 0 and 100",
        "discount_rate",
    )

    # Rating validation
    checker.register_validation(
        "rating_range",
        lambda x: (x.get("rating_average") is None or 0 <= float(x.get("rating_average", 0)) <= 5),
        "Rating must be between 0 and 5",
        "rating_average",
    )

    # Review count validation
    checker.register_validation(
        "review_count_non_negative",
        lambda x: (x.get("review_count") is None or int(x.get("review_count", 0)) >= 0),
        "Review count must be non-negative",
        "review_count",
    )


# Global validation checker instance
_global_validation_checker: ValidationPreservationChecker | None = None


def get_global_validation_checker(register_defaults: bool = True) -> ValidationPreservationChecker:
    """Get the global validation checker instance.

    Creates the instance if it doesn't exist.

    Args:
        register_defaults: Whether to register default Tiki product validations

    Returns:
        Global ValidationPreservationChecker instance
    """
    global _global_validation_checker

    if _global_validation_checker is None:
        _global_validation_checker = ValidationPreservationChecker()
        if register_defaults:
            register_tiki_product_validations(_global_validation_checker)

    return _global_validation_checker


@dataclass
class CacheFreshnessMetrics:
    """Metrics for tracking cache freshness.

    Attributes:
        total_cache_gets: Total number of cache get operations
        fresh_hits: Number of cache hits with fresh data
        stale_hits: Number of cache hits with stale data (expired)
        misses: Number of cache misses
        expired_entries: Number of entries that were expired
        freshness_rate: Percentage of cache hits that were fresh
    """

    total_cache_gets: int = 0
    fresh_hits: int = 0
    stale_hits: int = 0
    misses: int = 0
    expired_entries: int = 0
    freshness_rate: float = 0.0

    def calculate_freshness_rate(self) -> None:
        """Calculate freshness rate."""
        total_hits = self.fresh_hits + self.stale_hits
        if total_hits > 0:
            self.freshness_rate = (self.fresh_hits / total_hits) * 100.0
        else:
            self.freshness_rate = 0.0

    def to_dict(self) -> dict:
        """Convert metrics to dictionary."""
        return {
            "total_cache_gets": self.total_cache_gets,
            "fresh_hits": self.fresh_hits,
            "stale_hits": self.stale_hits,
            "misses": self.misses,
            "expired_entries": self.expired_entries,
            "freshness_rate": round(self.freshness_rate, 2),
        }


class CacheFreshnessEnforcer:
    """Enforces cache freshness by checking TTL and expiring stale entries.

    Ensures that cached data is not served beyond its configured TTL
    (time-to-live), maintaining data freshness requirements.

    Example:
        >>> enforcer = CacheFreshnessEnforcer(default_ttl=300)  # 5 minutes
        >>> enforcer.set("key1", "value1")
        >>> # ... 6 minutes later ...
        >>> value = enforcer.get("key1")  # Returns None (expired)
        >>> metrics = enforcer.get_metrics()
        >>> print(f"Freshness rate: {metrics.freshness_rate}%")
    """

    def __init__(self, default_ttl: int = 300):
        """Initialize the cache freshness enforcer.

        Args:
            default_ttl: Default time-to-live in seconds (default: 300 = 5 minutes)
        """
        self.default_ttl = default_ttl
        self._cache: dict[str, tuple] = {}  # key -> (value, timestamp, ttl)
        self.metrics = CacheFreshnessMetrics()

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        """Set a cache entry with TTL.

        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds (uses default if None)
        """
        ttl = ttl if ttl is not None else self.default_ttl
        timestamp = datetime.now().timestamp()
        self._cache[key] = (value, timestamp, ttl)

    def get(self, key: str) -> Any | None:
        """Get a cache entry, checking freshness.

        Args:
            key: Cache key

        Returns:
            Cached value if fresh, None if expired or not found
        """
        self.metrics.total_cache_gets += 1

        if key not in self._cache:
            self.metrics.misses += 1
            return None

        value, timestamp, ttl = self._cache[key]
        current_time = datetime.now().timestamp()
        age = current_time - timestamp

        if age > ttl:
            # Entry is stale, expire it
            self.metrics.stale_hits += 1
            self.metrics.expired_entries += 1
            del self._cache[key]
            logger.debug(f"Cache entry expired: {key} (age: {age:.1f}s, TTL: {ttl}s)")
            return None

        # Entry is fresh
        self.metrics.fresh_hits += 1
        return value

    def delete(self, key: str) -> bool:
        """Delete a cache entry.

        Args:
            key: Cache key

        Returns:
            True if entry was deleted, False if not found
        """
        if key in self._cache:
            del self._cache[key]
            return True
        return False

    def expire_stale_entries(self) -> int:
        """Expire all stale entries in the cache.

        Returns:
            Number of entries expired
        """
        current_time = datetime.now().timestamp()
        expired_keys = []

        for key, (_value, timestamp, ttl) in self._cache.items():
            age = current_time - timestamp
            if age > ttl:
                expired_keys.append(key)

        for key in expired_keys:
            del self._cache[key]
            self.metrics.expired_entries += 1

        if expired_keys:
            logger.info(f"Expired {len(expired_keys)} stale cache entries")

        return len(expired_keys)

    def get_entry_age(self, key: str) -> float | None:
        """Get the age of a cache entry in seconds.

        Args:
            key: Cache key

        Returns:
            Age in seconds, or None if entry not found
        """
        if key not in self._cache:
            return None

        _, timestamp, _ = self._cache[key]
        current_time = datetime.now().timestamp()
        return current_time - timestamp

    def is_fresh(self, key: str) -> bool:
        """Check if a cache entry is fresh (not expired).

        Args:
            key: Cache key

        Returns:
            True if entry exists and is fresh, False otherwise
        """
        if key not in self._cache:
            return False

        _, timestamp, ttl = self._cache[key]
        current_time = datetime.now().timestamp()
        age = current_time - timestamp

        return age <= ttl

    def get_metrics(self) -> CacheFreshnessMetrics:
        """Get cache freshness metrics.

        Returns:
            CacheFreshnessMetrics with calculated freshness rate
        """
        self.metrics.calculate_freshness_rate()
        return self.metrics

    def reset_metrics(self) -> None:
        """Reset all metrics."""
        self.metrics = CacheFreshnessMetrics()

    def clear(self) -> None:
        """Clear all cache entries."""
        entry_count = len(self._cache)
        self._cache.clear()
        logger.info(f"Cleared {entry_count} cache entries")

    def size(self) -> int:
        """Get the number of entries in the cache.

        Returns:
            Number of cache entries
        """
        return len(self._cache)

    def generate_report(self) -> str:
        """Generate a human-readable freshness report.

        Returns:
            Formatted string report
        """
        self.metrics.calculate_freshness_rate()

        lines = ["=" * 60]
        lines.append("CACHE FRESHNESS REPORT")
        lines.append("=" * 60)
        lines.append("")

        lines.append(f"Total Cache Gets: {self.metrics.total_cache_gets:,}")
        lines.append(f"Fresh Hits: {self.metrics.fresh_hits:,}")
        lines.append(f"Stale Hits: {self.metrics.stale_hits:,}")
        lines.append(f"Misses: {self.metrics.misses:,}")
        lines.append(f"Expired Entries: {self.metrics.expired_entries:,}")
        lines.append("")

        lines.append(f"Freshness Rate: {self.metrics.freshness_rate:.2f}%")
        lines.append(f"Current Cache Size: {self.size():,} entries")
        lines.append(f"Default TTL: {self.default_ttl} seconds")
        lines.append("")

        # Calculate hit rate
        total_attempts = self.metrics.fresh_hits + self.metrics.stale_hits + self.metrics.misses
        if total_attempts > 0:
            hit_rate = ((self.metrics.fresh_hits + self.metrics.stale_hits) / total_attempts) * 100
            lines.append(f"Overall Hit Rate: {hit_rate:.2f}%")

        lines.append("")
        lines.append("=" * 60)

        return "\n".join(lines)


# Global cache freshness enforcer instance
_global_cache_enforcer: CacheFreshnessEnforcer | None = None


def get_global_cache_enforcer(default_ttl: int = 300) -> CacheFreshnessEnforcer:
    """Get the global cache freshness enforcer instance.

    Creates the instance if it doesn't exist.

    Args:
        default_ttl: Default TTL in seconds (only used on first call)

    Returns:
        Global CacheFreshnessEnforcer instance
    """
    global _global_cache_enforcer

    if _global_cache_enforcer is None:
        _global_cache_enforcer = CacheFreshnessEnforcer(default_ttl=default_ttl)

    return _global_cache_enforcer


@dataclass
class AuditLogEntry:
    """Single audit log entry.

    Attributes:
        timestamp: When the operation occurred
        operation: Type of operation (fetch, transform, load, error)
        status: Status of the operation (success, failure, warning)
        details: Additional details about the operation
        error_message: Error message if operation failed
        context: Additional context (e.g., record ID, batch size)
    """

    timestamp: datetime
    operation: str
    status: str
    details: str
    error_message: str | None = None
    context: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert entry to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "operation": self.operation,
            "status": self.status,
            "details": self.details,
            "error_message": self.error_message,
            "context": dict(self.context),
        }

    def to_log_string(self) -> str:
        """Convert entry to log string format."""
        parts = [
            f"[{self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}]",
            f"[{self.operation.upper()}]",
            f"[{self.status.upper()}]",
            self.details,
        ]

        if self.error_message:
            parts.append(f"Error: {self.error_message}")

        if self.context:
            context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            parts.append(f"({context_str})")

        return " ".join(parts)


class AuditTrailLogger:
    """Logs all key operations for audit trail and debugging.

    Maintains structured, searchable logs of all pipeline operations
    including fetches, transformations, loads, and errors.

    Example:
        >>> logger = AuditTrailLogger()
        >>> logger.log_fetch("product_123", success=True, details="Fetched from API")
        >>> logger.log_transform("batch_001", success=True, record_count=100)
        >>> logger.log_error("load_failed", "Database connection timeout", context={"batch": "batch_001"})
        >>> logger.export_to_file("audit_trail.json")
    """

    def __init__(self, max_entries: int = 10000):
        """Initialize the audit trail logger.

        Args:
            max_entries: Maximum number of entries to keep in memory (default: 10000)
        """
        self.max_entries = max_entries
        self._entries: list[AuditLogEntry] = []
        self._operation_counts = {
            "fetch": 0,
            "transform": 0,
            "load": 0,
            "error": 0,
            "other": 0,
        }

    def log_operation(
        self,
        operation: str,
        status: str,
        details: str,
        error_message: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        """Log a generic operation.

        Args:
            operation: Type of operation (fetch, transform, load, etc.)
            status: Status (success, failure, warning)
            details: Description of what happened
            error_message: Optional error message
            context: Optional additional context
        """
        entry = AuditLogEntry(
            timestamp=datetime.now(),
            operation=operation,
            status=status,
            details=details,
            error_message=error_message,
            context=context or {},
        )

        self._add_entry(entry)

        # Update operation counts
        op_type = operation.lower()
        if op_type in self._operation_counts:
            self._operation_counts[op_type] += 1
        else:
            self._operation_counts["other"] += 1

        # Log to standard logger
        log_level = logging.ERROR if status == "failure" else logging.INFO
        logger.log(log_level, entry.to_log_string())

    def log_fetch(
        self,
        resource_id: str,
        success: bool,
        details: str,
        error_message: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        """Log a fetch operation.

        Args:
            resource_id: ID of the resource being fetched
            success: Whether the fetch succeeded
            details: Description of the fetch
            error_message: Optional error message
            context: Optional additional context
        """
        ctx = context or {}
        ctx["resource_id"] = resource_id

        self.log_operation(
            operation="fetch",
            status="success" if success else "failure",
            details=details,
            error_message=error_message,
            context=ctx,
        )

    def log_transform(
        self,
        batch_id: str,
        success: bool,
        record_count: int,
        details: str | None = None,
        error_message: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        """Log a transform operation.

        Args:
            batch_id: ID of the batch being transformed
            success: Whether the transform succeeded
            record_count: Number of records in the batch
            details: Optional description
            error_message: Optional error message
            context: Optional additional context
        """
        ctx = context or {}
        ctx["batch_id"] = batch_id
        ctx["record_count"] = record_count

        details_str = details or f"Transformed {record_count} records"

        self.log_operation(
            operation="transform",
            status="success" if success else "failure",
            details=details_str,
            error_message=error_message,
            context=ctx,
        )

    def log_load(
        self,
        batch_id: str,
        success: bool,
        record_count: int,
        details: str | None = None,
        error_message: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        """Log a load operation.

        Args:
            batch_id: ID of the batch being loaded
            success: Whether the load succeeded
            record_count: Number of records loaded
            details: Optional description
            error_message: Optional error message
            context: Optional additional context
        """
        ctx = context or {}
        ctx["batch_id"] = batch_id
        ctx["record_count"] = record_count

        details_str = details or f"Loaded {record_count} records to database"

        self.log_operation(
            operation="load",
            status="success" if success else "failure",
            details=details_str,
            error_message=error_message,
            context=ctx,
        )

    def log_error(
        self, error_type: str, error_message: str, context: dict[str, Any] | None = None
    ) -> None:
        """Log an error.

        Args:
            error_type: Type of error
            error_message: Error message
            context: Optional additional context
        """
        self.log_operation(
            operation="error",
            status="failure",
            details=f"Error: {error_type}",
            error_message=error_message,
            context=context or {},
        )

    def _add_entry(self, entry: AuditLogEntry) -> None:
        """Add an entry to the log, maintaining max size."""
        self._entries.append(entry)

        # Trim if exceeds max size
        if len(self._entries) > self.max_entries:
            # Remove oldest 10% of entries
            trim_count = self.max_entries // 10
            self._entries = self._entries[trim_count:]
            logger.debug(f"Trimmed {trim_count} oldest audit log entries")

    def get_entries(
        self, operation: str | None = None, status: str | None = None, limit: int | None = None
    ) -> list[AuditLogEntry]:
        """Get audit log entries with optional filtering.

        Args:
            operation: Filter by operation type
            status: Filter by status
            limit: Maximum number of entries to return

        Returns:
            List of matching AuditLogEntry objects
        """
        entries = self._entries

        # Apply filters
        if operation:
            entries = [e for e in entries if e.operation == operation]
        if status:
            entries = [e for e in entries if e.status == status]

        # Apply limit
        if limit:
            entries = entries[-limit:]

        return entries

    def get_error_entries(self, limit: int | None = None) -> list[AuditLogEntry]:
        """Get all error entries.

        Args:
            limit: Maximum number of entries to return

        Returns:
            List of error AuditLogEntry objects
        """
        return self.get_entries(status="failure", limit=limit)

    def get_operation_counts(self) -> dict[str, int]:
        """Get counts of operations by type.

        Returns:
            Dictionary mapping operation types to counts
        """
        return dict(self._operation_counts)

    def export_to_file(self, filepath: str, format: str = "json") -> None:
        """Export audit log to file.

        Args:
            filepath: Path to output file
            format: Output format ("json" or "text")
        """
        import json
        from pathlib import Path

        output_path = Path(filepath)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if format == "json":
            # Export as JSON
            data = {
                "exported_at": datetime.now().isoformat(),
                "entry_count": len(self._entries),
                "operation_counts": self._operation_counts,
                "entries": [entry.to_dict() for entry in self._entries],
            }

            with open(output_path, "w") as f:
                json.dump(data, f, indent=2)

        elif format == "text":
            # Export as text
            with open(output_path, "w") as f:
                f.write(f"Audit Trail Export - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 80 + "\n\n")

                for entry in self._entries:
                    f.write(entry.to_log_string() + "\n")

        else:
            raise ValueError(f"Unsupported format: {format}. Use 'json' or 'text'.")

        logger.info(f"Exported {len(self._entries)} audit log entries to {filepath}")

    def clear(self) -> None:
        """Clear all audit log entries."""
        entry_count = len(self._entries)
        self._entries.clear()
        self._operation_counts = dict.fromkeys(self._operation_counts, 0)
        logger.info(f"Cleared {entry_count} audit log entries")

    def generate_report(self) -> str:
        """Generate a human-readable audit trail report.

        Returns:
            Formatted string report
        """
        lines = ["=" * 60]
        lines.append("AUDIT TRAIL REPORT")
        lines.append("=" * 60)
        lines.append("")

        lines.append(f"Total Entries: {len(self._entries):,}")
        lines.append("")

        lines.append("Operation Counts:")
        for op_type, count in sorted(self._operation_counts.items()):
            lines.append(f"  {op_type.capitalize()}: {count:,}")
        lines.append("")

        # Recent errors
        error_entries = self.get_error_entries(limit=10)
        if error_entries:
            lines.append("Recent Errors (last 10):")
            for entry in error_entries:
                lines.append(f"  [{entry.timestamp.strftime('%H:%M:%S')}] {entry.details}")
                if entry.error_message:
                    lines.append(f"    → {entry.error_message}")
        else:
            lines.append("No errors recorded.")

        lines.append("")
        lines.append("=" * 60)

        return "\n".join(lines)


# Global audit trail logger instance
_global_audit_logger: AuditTrailLogger | None = None


def get_global_audit_logger(max_entries: int = 10000) -> AuditTrailLogger:
    """Get the global audit trail logger instance.

    Creates the instance if it doesn't exist.

    Args:
        max_entries: Maximum entries to keep (only used on first call)

    Returns:
        Global AuditTrailLogger instance
    """
    global _global_audit_logger

    if _global_audit_logger is None:
        _global_audit_logger = AuditTrailLogger(max_entries=max_entries)

    return _global_audit_logger
