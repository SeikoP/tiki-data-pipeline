from __future__ import annotations

import math
from typing import Any


def plan_detail_batches(
    products: list[dict[str, Any]],
    *,
    active_batches: int = 6,
    base_batch_size: int = 500,
) -> list[dict[str, Any]]:
    """Build mapped-task kwargs for detail crawl batches.

    The planner prefers filling the available Airflow slots first. Larger runs can exceed one
    wave only when the configured base batch size would otherwise make each task too large.
    """
    if not products:
        return []

    total_products = len(products)
    active_batches = max(1, active_batches)
    base_batch_size = max(1, base_batch_size)

    target_batches = max(active_batches, math.ceil(total_products / base_batch_size))
    target_batches = min(total_products, target_batches)
    if total_products > active_batches and target_batches > active_batches:
        remainder = target_batches % active_batches
        if remainder == 1:
            target_batches -= 1
        elif remainder:
            target_batches = min(total_products, target_batches + active_batches - remainder)
    batch_size = max(1, math.ceil(total_products / target_batches))

    batches = [products[i : i + batch_size] for i in range(0, total_products, batch_size)]
    return [{"product_batch": batch, "batch_index": idx} for idx, batch in enumerate(batches)]
