"""
Vectorized transformer using Pandas/NumPy for CPU optimization.

This module provides a VectorizedTransformer that uses vectorized operations
instead of Python loops for batch transformations, significantly improving
performance for CPU-bound operations.
"""

import logging
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class VectorizedTransformer:
    """
    Transformer that uses NumPy/Pandas for batch transformations.

    Replaces loop-based operations with vectorized operations for:
    - Price normalization
    - String cleaning
    - Discount calculations
    - Data type conversions
    """

    def __init__(self):
        """Initialize VectorizedTransformer."""
        pass

    def transform_batch(self, products: list[dict[str, Any]]) -> pd.DataFrame:
        """
        Transform product batch using vectorized operations.

        Args:
            products: List of product dictionaries

        Returns:
            DataFrame with transformed products
        """
        if not products:
            logger.warning("Empty product list provided")
            return pd.DataFrame()

        # Convert to DataFrame for vectorized operations
        df = pd.DataFrame(products)

        # Vectorized price normalization
        df = self._normalize_prices(df)

        # Vectorized string cleaning
        df = self._clean_strings(df)

        # Vectorized discount calculations
        df = self._calculate_discounts(df)

        return df

    def _normalize_prices(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize price fields using vectorized operations.

        Handles nested price dictionaries and converts to flat structure.
        """
        # Extract price from nested dict if present
        if "price" in df.columns and df["price"].dtype == "object":
            # Check if price is a dict
            price_is_dict = df["price"].apply(
                lambda x: isinstance(x, dict) if pd.notna(x) else False
            )

            if price_is_dict.any():
                # Extract current_price from dict
                df["current_price"] = df["price"].apply(
                    lambda x: x.get("current_price") if isinstance(x, dict) else x
                )
                # Extract original_price from dict
                df["original_price"] = df["price"].apply(
                    lambda x: x.get("original_price") if isinstance(x, dict) else None
                )
            else:
                df["current_price"] = df["price"]
                df["original_price"] = None
        elif "price" in df.columns:
            df["current_price"] = df["price"]
            df["original_price"] = None
        else:
            df["current_price"] = None
            df["original_price"] = None

        # Convert to numeric, coercing errors to NaN
        df["current_price"] = pd.to_numeric(df["current_price"], errors="coerce")
        df["original_price"] = pd.to_numeric(df["original_price"], errors="coerce")

        # Fill original_price with current_price if missing
        df["original_price"] = df["original_price"].fillna(df["current_price"])

        # Ensure non-negative prices (vectorized)
        df.loc[df["current_price"] < 0, "current_price"] = np.nan
        df.loc[df["original_price"] < 0, "original_price"] = np.nan

        return df

    def _clean_strings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean string fields using vectorized operations.

        Applies normalization to text fields like name, brand, seller_name.
        """
        # Clean name field
        if "name" in df.columns:
            # Vectorized string operations
            df["name"] = df["name"].astype(str).str.strip()
            # Remove extra whitespace (vectorized)
            df["name"] = df["name"].str.replace(r"\s+", " ", regex=True)

        # Clean brand field
        if "brand" in df.columns:
            # Only convert non-None values to string
            mask = df["brand"].notna()
            df.loc[mask, "brand"] = df.loc[mask, "brand"].astype(str).str.strip()
            # Remove "Thương hiệu: " prefix (vectorized)
            df.loc[mask, "brand"] = df.loc[mask, "brand"].str.replace(
                "Thương hiệu: ", "", regex=False
            )
            # Replace empty strings and 'None' with None
            df.loc[df["brand"] == "", "brand"] = None
            df.loc[df["brand"] == "None", "brand"] = None

        # Clean seller_name from nested seller dict
        if "seller" in df.columns:
            df["seller_name"] = df["seller"].apply(
                lambda x: x.get("name") if isinstance(x, dict) else None
            )
            # Only convert non-None values to string
            mask = df["seller_name"].notna()
            df.loc[mask, "seller_name"] = df.loc[mask, "seller_name"].astype(str).str.strip()
            # Clean up 'None' strings and empty strings
            df.loc[df["seller_name"] == "None", "seller_name"] = None
            df.loc[df["seller_name"] == "", "seller_name"] = None

        # Clean product_id
        if "product_id" in df.columns:
            df["product_id"] = df["product_id"].astype(str).str.strip()

        # Clean URL fields
        for url_field in ["url", "category_url"]:
            if url_field in df.columns:
                mask = df[url_field].notna()
                df.loc[mask, url_field] = df.loc[mask, url_field].astype(str).str.strip()
                df.loc[df[url_field] == "None", url_field] = None

        return df

    def _calculate_discounts(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate discount percentages using vectorized operations.

        Formula: discount_percent = (original_price - current_price) / original_price * 100
        """
        # Ensure price columns exist
        if "current_price" not in df.columns or "original_price" not in df.columns:
            df["discount_percent"] = None
            return df

        # Vectorized discount calculation
        # Only calculate where both prices are valid and original > 0
        valid_mask = (
            df["current_price"].notna() & df["original_price"].notna() & (df["original_price"] > 0)
        )

        # Initialize discount_percent column
        df["discount_percent"] = np.nan

        # Calculate discount only for valid rows (vectorized)
        df.loc[valid_mask, "discount_percent"] = (
            (df.loc[valid_mask, "original_price"] - df.loc[valid_mask, "current_price"])
            / df.loc[valid_mask, "original_price"]
            * 100
        )

        # Round to 2 decimal places (vectorized)
        df["discount_percent"] = df["discount_percent"].round(2)

        # Ensure discount is non-negative (vectorized)
        df.loc[df["discount_percent"] < 0, "discount_percent"] = 0

        return df

    def transform_to_dict_list(self, df: pd.DataFrame) -> list[dict[str, Any]]:
        """
        Convert DataFrame back to list of dictionaries.

        Args:
            df: Transformed DataFrame

        Returns:
            List of product dictionaries
        """
        # Replace NaN with None for JSON compatibility
        df = df.replace({np.nan: None})

        # Convert to list of dicts
        return df.to_dict("records")
