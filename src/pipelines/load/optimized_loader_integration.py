"""
Optimized batch loader integration for performance optimization.

This module provides integration between the OptimizedBatchLoader and the
existing DataLoader, with feature flags for gradual rollout.
"""

import logging
from typing import Any, Dict, List, Optional

from common.config import OPTIMIZATION_CONFIG
from common.optimized_batch_loader import OptimizedBatchLoader

logger = logging.getLogger(__name__)


class OptimizedLoaderIntegration:
    """Integration layer for optimized batch loader with backward compatibility.
    
    This class provides a unified interface that can use either the optimized
    batch loader or the traditional loader based on feature flags.
    """
    
    def __init__(
        self,
        use_optimized: Optional[bool] = None,
        batch_size: Optional[int] = None,
        use_copy_command: Optional[bool] = None,
        enable_db: bool = True,
        db_config: Optional[Dict[str, Any]] = None,
        show_progress: bool = True,
        continue_on_error: bool = True
    ):
        """Initialize optimized loader integration.
        
        Args:
            use_optimized: Enable optimized loader (defaults to config)
            batch_size: Batch size for loading (defaults to config)
            use_copy_command: Use COPY command for inserts (defaults to config)
            enable_db: Enable database loading
            db_config: Database configuration
            show_progress: Show progress logs
            continue_on_error: Continue if a batch fails
        """
        # Use config values as defaults
        self.use_optimized = (
            use_optimized if use_optimized is not None 
            else OPTIMIZATION_CONFIG["use_optimized_loader"]
        )
        self.batch_size = batch_size or OPTIMIZATION_CONFIG["optimized_batch_size"]
        self.use_copy_command = (
            use_copy_command if use_copy_command is not None
            else OPTIMIZATION_CONFIG["use_copy_command"]
        )
        
        self.enable_db = enable_db
        self.db_config = db_config or {}
        self.show_progress = show_progress
        self.continue_on_error = continue_on_error
        
        # Initialize optimized loader if enabled
        self._optimized_loader: Optional[OptimizedBatchLoader] = None
        self._db_pool = None
        
        if self.use_optimized and self.enable_db:
            try:
                self._initialize_optimized_loader()
                logger.info(
                    f"✅ Optimized batch loader enabled "
                    f"(batch_size={self.batch_size}, use_copy={self.use_copy_command})"
                )
            except Exception as e:
                logger.error(f"Failed to initialize optimized loader: {e}")
                self.use_optimized = False
                logger.warning("Falling back to traditional loader")
        else:
            logger.info("ℹ️  Optimized loader disabled, using traditional mode")
    
    def _initialize_optimized_loader(self):
        """Initialize optimized batch loader with connection pool."""
        from .db_pool import get_db_pool, initialize_db_pool
        
        # Initialize database pool
        initialize_db_pool(**self.db_config)
        self._db_pool = get_db_pool()
        
        # Create optimized loader
        self._optimized_loader = OptimizedBatchLoader(
            connection_pool=self._db_pool,
            batch_size=self.batch_size,
            use_copy_for_inserts=self.use_copy_command
        )
    
    def load_products(
        self,
        products: List[Dict[str, Any]],
        upsert: bool = True,
        validate_before_load: bool = True,
        save_to_file: Optional[str] = None
    ) -> Dict[str, Any]:
        """Load products using optimized or traditional method.
        
        This method provides a unified interface that internally uses
        optimized batch operations if enabled, maintaining backward compatibility.
        
        Args:
            products: List of product dictionaries
            upsert: If True, use INSERT ON CONFLICT UPDATE
            validate_before_load: Validate products before loading
            save_to_file: Optional file path to save results
            
        Returns:
            Dictionary with loading statistics
        """
        if not products:
            logger.warning("⚠️  No products to load")
            return {
                "total_products": 0,
                "db_loaded": 0,
                "file_loaded": 0,
                "total_loaded": 0,
                "success_count": 0,
                "failed_count": 0,
                "errors": []
            }
        
        if self.use_optimized and self._optimized_loader and self.enable_db:
            # Use optimized implementation
            return self._load_optimized(products, upsert, validate_before_load, save_to_file)
        else:
            # Fallback to traditional implementation
            return self._load_traditional(products, upsert, validate_before_load, save_to_file)
    
    def _load_optimized(
        self,
        products: List[Dict[str, Any]],
        upsert: bool,
        validate_before_load: bool,
        save_to_file: Optional[str]
    ) -> Dict[str, Any]:
        """Optimized loading implementation."""
        stats = {
            "total_products": len(products),
            "db_loaded": 0,
            "file_loaded": 0,
            "total_loaded": 0,
            "success_count": 0,
            "failed_count": 0,
            "errors": []
        }
        
        try:
            # Validate if requested
            if validate_before_load:
                products = self._validate_products(products, stats)
            
            # Load to database using optimized loader
            load_stats = self._optimized_loader.load_products(products, upsert=upsert)
            
            # Update stats
            stats["db_loaded"] = load_stats["total_processed"]
            stats["success_count"] = load_stats["inserted"] + load_stats.get("updated", 0)
            stats["failed_count"] = load_stats["failed"]
            
            # Save to file if requested
            if save_to_file:
                self._save_to_file(products, save_to_file, stats)
            
            stats["total_loaded"] = max(stats["db_loaded"], stats["file_loaded"])
            
            if self.show_progress:
                logger.info(
                    f"✅ Optimized loading complete: "
                    f"{stats['db_loaded']} products loaded "
                    f"(COPY: {load_stats['copy_operations']}, "
                    f"Upsert: {load_stats['upsert_operations']})"
                )
            
            return stats
            
        except Exception as e:
            error_msg = f"Optimized loading failed: {e}"
            logger.error(f"❌ {error_msg}")
            stats["errors"].append(error_msg)
            stats["failed_count"] = len(products)
            
            # Fallback to traditional method
            logger.warning("Falling back to traditional loader")
            return self._load_traditional(products, upsert, validate_before_load, save_to_file)
    
    def _load_traditional(
        self,
        products: List[Dict[str, Any]],
        upsert: bool,
        validate_before_load: bool,
        save_to_file: Optional[str]
    ) -> Dict[str, Any]:
        """Traditional loading fallback.
        
        Uses the existing OptimizedDataLoader for backward compatibility.
        """
        try:
            from .loader import OptimizedDataLoader
            
            loader = OptimizedDataLoader(
                batch_size=self.batch_size,
                enable_db=self.enable_db,
                db_config=self.db_config,
                show_progress=self.show_progress,
                continue_on_error=self.continue_on_error
            )
            
            return loader.load_products(
                products,
                upsert=upsert,
                validate_before_load=validate_before_load,
                save_to_file=save_to_file
            )
            
        except Exception as e:
            logger.error(f"Traditional loading failed: {e}")
            
            # Return error stats
            stats = {
                "total_products": len(products),
                "db_loaded": 0,
                "file_loaded": 0,
                "total_loaded": 0,
                "success_count": 0,
                "failed_count": len(products),
                "errors": [str(e)]
            }
            
            return stats
    
    def _validate_products(
        self,
        products: List[Dict[str, Any]],
        stats: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Validate products and remove invalid ones."""
        valid_products = []
        
        for product in products:
            # Check required fields
            if not product.get("product_id") or not product.get("name"):
                stats["failed_count"] += 1
                stats["errors"].append(
                    f"Missing required fields: product_id={product.get('product_id')}"
                )
                continue
            
            valid_products.append(product)
        
        if self.show_progress:
            failed = len(products) - len(valid_products)
            if failed > 0:
                logger.warning(f"⚠️  Removed {failed} invalid products")
        
        return valid_products
    
    def _save_to_file(
        self,
        products: List[Dict[str, Any]],
        file_path: str,
        stats: Dict[str, Any]
    ):
        """Save products to JSON file."""
        import json
        from datetime import datetime
        from pathlib import Path
        
        try:
            path = Path(file_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            
            output_data = {
                "loaded_at": datetime.now().isoformat(),
                "total_products": len(products),
                "stats": stats,
                "products": products
            }
            
            with open(path, "w", encoding="utf-8") as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2, default=str)
            
            stats["file_loaded"] = len(products)
            
            if self.show_progress:
                logger.info(f"✅ Saved {len(products)} products to: {file_path}")
                
        except Exception as e:
            error_msg = f"File saving failed: {e}"
            stats["errors"].append(error_msg)
            logger.error(f"❌ {error_msg}")
    
    def close(self):
        """Close database resources."""
        if self._db_pool:
            try:
                from .db_pool import close_db_pool
                close_db_pool()
                logger.info("✅ Database connection pool closed")
            except Exception as e:
                logger.error(f"❌ Failed to close DB pool: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


def get_optimized_loader_integration(
    use_optimized: Optional[bool] = None,
    **kwargs
) -> OptimizedLoaderIntegration:
    """Factory function to create OptimizedLoaderIntegration instance.
    
    Args:
        use_optimized: Enable optimized mode (defaults to config)
        **kwargs: Additional arguments passed to OptimizedLoaderIntegration
        
    Returns:
        OptimizedLoaderIntegration instance
    """
    return OptimizedLoaderIntegration(
        use_optimized=use_optimized,
        **kwargs
    )
