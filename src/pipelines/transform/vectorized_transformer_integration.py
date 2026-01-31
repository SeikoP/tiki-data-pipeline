"""
Vectorized transformer integration for performance optimization.

This module provides integration between the vectorized transformer and the
existing DataTransformer, with feature flags for gradual rollout.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

from common.config import OPTIMIZATION_CONFIG
from common.vectorized_transformer import VectorizedTransformer

logger = logging.getLogger(__name__)


class VectorizedTransformerIntegration:
    """Integration layer for vectorized transformer with backward compatibility.
    
    This class provides a unified interface that can use either vectorized or
    traditional loop-based transformations based on feature flags.
    """
    
    def __init__(
        self,
        use_vectorized: Optional[bool] = None,
        strict_validation: bool = False,
        remove_invalid: bool = True,
        normalize_fields: bool = True
    ):
        """Initialize vectorized transformer integration.
        
        Args:
            use_vectorized: Enable vectorized mode (defaults to config)
            strict_validation: If True, raise on validation errors
            remove_invalid: If True, remove invalid products
            normalize_fields: If True, normalize data fields
        """
        # Use config value as default
        self.use_vectorized = (
            use_vectorized if use_vectorized is not None 
            else OPTIMIZATION_CONFIG["use_vectorized_transform"]
        )
        
        self.strict_validation = strict_validation
        self.remove_invalid = remove_invalid
        self.normalize_fields = normalize_fields
        
        # Initialize vectorized transformer if enabled
        self._vectorized_transformer: Optional[VectorizedTransformer] = None
        
        if self.use_vectorized:
            try:
                self._vectorized_transformer = VectorizedTransformer()
                logger.info("✅ Vectorized transformer enabled")
            except Exception as e:
                logger.error(f"Failed to initialize vectorized transformer: {e}")
                self.use_vectorized = False
                logger.warning("Falling back to traditional transformer")
        else:
            logger.info("ℹ️  Vectorized transformer disabled, using traditional mode")
    
    def transform_products(
        self,
        products: List[Dict[str, Any]],
        validate: bool = True
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Transform products using vectorized or traditional method.
        
        This method provides a unified interface that internally uses
        vectorized operations if enabled, maintaining backward compatibility.
        
        Args:
            products: List of product dictionaries
            validate: Whether to validate products
            
        Returns:
            Tuple of (transformed_products, stats)
        """
        if not products:
            logger.warning("⚠️  Empty product list provided")
            return [], {
                "total_processed": 0,
                "valid_products": 0,
                "invalid_products": 0,
                "duplicates_removed": 0,
                "errors": []
            }
        
        if self.use_vectorized and self._vectorized_transformer:
            # Use vectorized implementation
            return self._transform_vectorized(products, validate)
        else:
            # Fallback to traditional implementation
            return self._transform_traditional(products, validate)
    
    def _transform_vectorized(
        self,
        products: List[Dict[str, Any]],
        validate: bool
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Vectorized transformation implementation."""
        stats = {
            "total_processed": len(products),
            "valid_products": 0,
            "invalid_products": 0,
            "duplicates_removed": 0,
            "errors": []
        }
        
        try:
            # Transform using vectorized operations
            df = self._vectorized_transformer.transform_batch(products)
            
            # Remove duplicates (vectorized)
            initial_count = len(df)
            df = df.drop_duplicates(subset=['product_id'], keep='first')
            stats["duplicates_removed"] = initial_count - len(df)
            
            # Validate if requested
            if validate:
                # Vectorized validation
                valid_mask = self._validate_vectorized(df)
                invalid_count = (~valid_mask).sum()
                
                if self.remove_invalid:
                    # Remove invalid products
                    df = df[valid_mask]
                    stats["invalid_products"] = invalid_count
                elif self.strict_validation and invalid_count > 0:
                    raise ValueError(f"Found {invalid_count} invalid products")
            
            # Convert back to list of dicts
            transformed_products = self._vectorized_transformer.transform_to_dict_list(df)
            stats["valid_products"] = len(transformed_products)
            
            logger.info(
                f"✅ Vectorized transform complete: "
                f"{stats['valid_products']}/{stats['total_processed']} products valid"
            )
            
            return transformed_products, stats
            
        except Exception as e:
            error_msg = f"Vectorized transformation failed: {e}"
            logger.error(f"❌ {error_msg}")
            stats["errors"].append(error_msg)
            
            if self.strict_validation:
                raise
            
            # Fallback to traditional method
            logger.warning("Falling back to traditional transformation")
            return self._transform_traditional(products, validate)
    
    def _validate_vectorized(self, df) -> Any:
        """Vectorized validation of products.
        
        Returns:
            Boolean mask indicating valid products
        """
        import pandas as pd
        
        # Check required fields (vectorized)
        has_product_id = df['product_id'].notna() & (df['product_id'] != '')
        has_name = df['name'].notna() & (df['name'] != '')
        has_url = df['url'].notna() & (df['url'] != '')
        
        # Check seller_name (required)
        has_seller_name = df['seller_name'].notna() & (df['seller_name'] != '')
        
        # Check product_id format (digits only)
        product_id_valid = df['product_id'].astype(str).str.match(r'^\d+$', na=False)
        
        # Check URL format (starts with http)
        url_valid = df['url'].astype(str).str.startswith(('http://', 'https://'), na=False)
        
        # Check price validity (if present)
        price_valid = pd.Series(True, index=df.index)
        if 'current_price' in df.columns and 'original_price' in df.columns:
            # Prices should be non-negative
            price_valid = (
                (df['current_price'].isna() | (df['current_price'] >= 0)) &
                (df['original_price'].isna() | (df['original_price'] >= 0))
            )
            # Current price should not exceed original price
            both_present = df['current_price'].notna() & df['original_price'].notna()
            price_valid = price_valid & (~both_present | (df['current_price'] <= df['original_price']))
        
        # Check rating validity (if present)
        rating_valid = pd.Series(True, index=df.index)
        if 'rating_average' in df.columns:
            rating_valid = df['rating_average'].isna() | (
                (df['rating_average'] >= 0) & (df['rating_average'] <= 5)
            )
        
        # Check sales_count validity (if present)
        sales_valid = pd.Series(True, index=df.index)
        if 'sales_count' in df.columns:
            sales_valid = df['sales_count'].isna() | (df['sales_count'] >= 0)
        
        # Combine all validation checks
        valid_mask = (
            has_product_id &
            has_name &
            has_url &
            has_seller_name &
            product_id_valid &
            url_valid &
            price_valid &
            rating_valid &
            sales_valid
        )
        
        return valid_mask
    
    def _transform_traditional(
        self,
        products: List[Dict[str, Any]],
        validate: bool
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Traditional transformation fallback.
        
        Uses the existing DataTransformer for backward compatibility.
        """
        try:
            from .transformer import DataTransformer
            
            transformer = DataTransformer(
                strict_validation=self.strict_validation,
                remove_invalid=self.remove_invalid,
                normalize_fields=self.normalize_fields
            )
            
            return transformer.transform_products(products, validate=validate)
            
        except Exception as e:
            logger.error(f"Traditional transformation failed: {e}")
            
            # Return empty result with error
            stats = {
                "total_processed": len(products),
                "valid_products": 0,
                "invalid_products": len(products),
                "duplicates_removed": 0,
                "errors": [str(e)]
            }
            
            if self.strict_validation:
                raise
            
            return [], stats


def get_vectorized_transformer_integration(
    use_vectorized: Optional[bool] = None,
    **kwargs
) -> VectorizedTransformerIntegration:
    """Factory function to create VectorizedTransformerIntegration instance.
    
    Args:
        use_vectorized: Enable vectorized mode (defaults to config)
        **kwargs: Additional arguments passed to VectorizedTransformerIntegration
        
    Returns:
        VectorizedTransformerIntegration instance
    """
    return VectorizedTransformerIntegration(
        use_vectorized=use_vectorized,
        **kwargs
    )
