"""Data validator module for product data quality assessment.

This module provides validation logic to determine whether product data is complete enough to save,
needs retry, or should be skipped.
"""

from typing import Any

# Import config constants
try:
    from pipelines.crawl.config import (
        DATA_QUALITY_CRITICAL_FIELDS,
        DATA_QUALITY_IMPORTANT_FIELDS,
        DATA_QUALITY_MIN_SCORE,
        DATA_QUALITY_OPTIONAL_FIELDS,
        PRODUCT_RETRY_MAX_ATTEMPTS,
    )
except ImportError:
    # Fallback defaults if config not available
    DATA_QUALITY_CRITICAL_FIELDS = ["name", "price", "product_id"]
    DATA_QUALITY_IMPORTANT_FIELDS = ["seller_name", "brand", "category_id"]
    DATA_QUALITY_OPTIONAL_FIELDS = ["rating_average", "sales_count"]
    DATA_QUALITY_MIN_SCORE = 0.7
    PRODUCT_RETRY_MAX_ATTEMPTS = 2


def get_missing_fields(product: dict[str, Any], field_list: list[str] | None = None) -> list[str]:
    """Get list of missing fields from product data.

    Args:
        product: Product data dictionary
        field_list: List of fields to check (default: important fields)

    Returns:
        List of missing field names
    """
    if field_list is None:
        field_list = DATA_QUALITY_IMPORTANT_FIELDS

    # Import seller validation patterns
    try:
        from pipelines.crawl.config import (
            INVALID_SELLER_PATTERNS,
            SELLER_NAME_MAX_LENGTH,
            SELLER_NAME_MIN_LENGTH,
        )
    except ImportError:
        INVALID_SELLER_PATTERNS = ["đã mua", "đã bán", "sold", "bought", "loading"]
        SELLER_NAME_MIN_LENGTH = 2
        SELLER_NAME_MAX_LENGTH = 100

    missing = []
    for field in field_list:
        value = product.get(field)

        # 1. Basic check: None, empty string, empty collection
        if value is None or value == "" or value == [] or value == {}:
            missing.append(field)
            continue

        # 2. Specific check for seller_name
        if field == "seller_name" and isinstance(value, str):
            value_lower = value.lower().strip()

            # Check length constraints
            if len(value) < SELLER_NAME_MIN_LENGTH or len(value) > SELLER_NAME_MAX_LENGTH:
                missing.append(field)
                continue

            # Check invalid patterns
            is_invalid = False
            for pattern in INVALID_SELLER_PATTERNS:
                if pattern.lower() in value_lower:
                    is_invalid = True
                    break

            if is_invalid:
                missing.append(field)
                continue

            # Check if just numbers string
            if value.replace(".", "").replace(",", "").isdigit():
                missing.append(field)
                continue

    return missing


def calculate_completeness_score(product: dict[str, Any]) -> float:
    """
    Calculate data completeness score (0.0 - 1.0).

    Score is weighted:
    - Critical fields: 50% weight (must have all)
    - Important fields: 35% weight
    - Optional fields: 15% weight

    Args:
        product: Product data dictionary

    Returns:
        Completeness score between 0.0 and 1.0
    """
    # Check critical fields (50% weight)
    critical_missing = get_missing_fields(product, DATA_QUALITY_CRITICAL_FIELDS)
    critical_score = 1.0 - (len(critical_missing) / len(DATA_QUALITY_CRITICAL_FIELDS))

    # If critical fields are missing, score is 0
    if critical_score < 1.0:
        return 0.0

    # Check important fields (35% weight)
    important_missing = get_missing_fields(product, DATA_QUALITY_IMPORTANT_FIELDS)
    important_score = 1.0 - (len(important_missing) / len(DATA_QUALITY_IMPORTANT_FIELDS))

    # Check optional fields (15% weight)
    optional_missing = get_missing_fields(product, DATA_QUALITY_OPTIONAL_FIELDS)
    optional_score = 1.0 - (len(optional_missing) / len(DATA_QUALITY_OPTIONAL_FIELDS))

    # Weighted average
    total_score = (0.5 * critical_score) + (0.35 * important_score) + (0.15 * optional_score)

    return round(total_score, 2)


def validate_product_data(
    product: dict[str, Any], retry_count: int = 0, max_retries: int | None = None
) -> str:
    """Validate product data and determine action.

    Decision logic:
    1. If missing critical fields → 'skip' (cannot save)
    2. If all important fields present → 'accept' (good quality)
    3. If missing important fields and retry_count < max → 'retry'
    4. If missing important fields and retry_count >= max → 'accept' (partial data)

    Args:
        product: Product data dictionary
        retry_count: Current retry count (default: 0)
        max_retries: Maximum retry attempts (default: from config)

    Returns:
        Action string: 'accept', 'retry', or 'skip'
    """
    if max_retries is None:
        max_retries = PRODUCT_RETRY_MAX_ATTEMPTS

    # Check critical fields
    critical_missing = get_missing_fields(product, DATA_QUALITY_CRITICAL_FIELDS)
    if critical_missing:
        # Cannot save product without critical fields
        return "skip"

    # Check important fields
    important_missing = get_missing_fields(product, DATA_QUALITY_IMPORTANT_FIELDS)

    if not important_missing:
        # Perfect data - all important fields present
        return "accept"

    # Has missing important fields
    if retry_count < max_retries:
        # Retry to get missing fields
        return "retry"
    else:
        # Exceeded max retries - check if we should accept partial data

        # ENHANCEMENT: Skip logic for strict fields
        # Nếu vẫn thiếu seller_name hoặc brand sau khi retry -> SKIP để crawl lại lần sau
        # Yêu cầu này giúp đảm bảo dữ liệu seller/brand luôn đầy đủ
        strict_fields = ["seller_name"]  # Có thể thêm "brand" nếu muốn áp dụng giống nhau
        for field in strict_fields:
            if field in important_missing:
                return "skip"

        # Calculate completeness score to decide for other fields
        score = calculate_completeness_score(product)
        if score >= DATA_QUALITY_MIN_SCORE:
            return "accept"
        else:
            # Score too low even after retries - skip
            return "skip"


def enrich_product_metadata(
    product: dict[str, Any], retry_count: int = 0, crawl_status: str = "success"
) -> dict[str, Any]:
    """Enrich product data with metadata for tracking.

    Adds:
    - _metadata.missing_fields: List of missing important fields
    - _metadata.data_completeness_score: Completeness score (0.0-1.0)
    - _metadata.retry_count: Number of retries
    - _metadata.data_quality: Quality label (complete/partial/incomplete)

    Args:
        product: Product data dictionary
        retry_count: Current retry count
        crawl_status: Crawl status (success/partial/failed)

    Returns:
        Enriched product dictionary
    """
    if "_metadata" not in product:
        product["_metadata"] = {}

    # Calculate missing fields and completeness
    missing_important = get_missing_fields(product, DATA_QUALITY_IMPORTANT_FIELDS)
    completeness_score = calculate_completeness_score(product)

    # Determine quality label
    if not missing_important:
        quality_label = "complete"
    elif completeness_score >= DATA_QUALITY_MIN_SCORE:
        quality_label = "partial"
    else:
        quality_label = "incomplete"

    # Update metadata
    product["_metadata"]["missing_fields"] = missing_important
    product["_metadata"]["data_completeness_score"] = completeness_score
    product["_metadata"]["retry_count"] = retry_count
    product["_metadata"]["data_quality"] = quality_label
    product["_metadata"]["crawl_status"] = crawl_status

    return product


def should_retry_for_field(field_name: str) -> bool:
    """Check if a specific field is worth retrying for.

    Args:
        field_name: Field name to check

    Returns:
        True if field is in important fields list
    """
    return field_name in DATA_QUALITY_IMPORTANT_FIELDS
