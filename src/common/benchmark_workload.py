"""Benchmark workload generator for performance testing.

This module generates consistent, reproducible workloads for benchmarking
pipeline performance across different dataset sizes and product types.
"""

import hashlib
import json
import random
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class WorkloadConfig:
    """Configuration for a benchmark workload.
    
    Attributes:
        workload_id: Unique identifier for this workload
        size: Number of products in the workload
        seed: Random seed for reproducibility
        product_types: Distribution of product types
        include_edge_cases: Whether to include edge case products
        description: Description of the workload
    """
    
    workload_id: str
    size: int
    seed: int
    product_types: Dict[str, float]  # type -> percentage
    include_edge_cases: bool
    description: str
    
    def to_dict(self) -> Dict:
        """Convert config to dictionary for serialization."""
        return {
            "workload_id": self.workload_id,
            "size": self.size,
            "seed": self.seed,
            "product_types": self.product_types,
            "include_edge_cases": self.include_edge_cases,
            "description": self.description,
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "WorkloadConfig":
        """Create WorkloadConfig from dictionary."""
        return cls(
            workload_id=data["workload_id"],
            size=data["size"],
            seed=data["seed"],
            product_types=data["product_types"],
            include_edge_cases=data["include_edge_cases"],
            description=data["description"],
        )


@dataclass
class BenchmarkWorkload:
    """A benchmark workload with product IDs and metadata.
    
    Attributes:
        config: Workload configuration
        product_ids: List of product IDs to process
        checksum: SHA256 checksum of product IDs for verification
        created_at: When the workload was created
    """
    
    config: WorkloadConfig
    product_ids: List[str]
    checksum: str
    created_at: datetime
    
    def to_dict(self) -> Dict:
        """Convert workload to dictionary for serialization."""
        return {
            "config": self.config.to_dict(),
            "product_ids": self.product_ids,
            "checksum": self.checksum,
            "created_at": self.created_at.isoformat(),
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "BenchmarkWorkload":
        """Create BenchmarkWorkload from dictionary."""
        return cls(
            config=WorkloadConfig.from_dict(data["config"]),
            product_ids=data["product_ids"],
            checksum=data["checksum"],
            created_at=datetime.fromisoformat(data["created_at"]),
        )
    
    def verify_checksum(self) -> bool:
        """Verify that the workload hasn't been modified.
        
        Returns:
            True if checksum matches, False otherwise
        """
        computed = self._compute_checksum(self.product_ids)
        return computed == self.checksum
    
    @staticmethod
    def _compute_checksum(product_ids: List[str]) -> str:
        """Compute SHA256 checksum of product IDs.
        
        Args:
            product_ids: List of product IDs
        
        Returns:
            Hex digest of checksum
        """
        # Sort to ensure consistent ordering
        sorted_ids = sorted(product_ids)
        content = ",".join(sorted_ids)
        return hashlib.sha256(content.encode()).hexdigest()


class BenchmarkWorkloadGenerator:
    """Generates reproducible benchmark workloads for performance testing.
    
    Creates workloads of different sizes (small, medium, large) with diverse
    product types and edge cases. Workloads are deterministic based on seed
    for reproducibility.
    
    Example:
        >>> generator = BenchmarkWorkloadGenerator()
        >>> # Generate small workload
        >>> small = generator.generate_workload(
        ...     workload_id="small_001",
        ...     size=100,
        ...     seed=42
        ... )
        >>> # Save for later use
        >>> generator.save_workload(small)
        >>> # Load and verify
        >>> loaded = generator.load_workload("small_001")
        >>> assert loaded.verify_checksum()
    """
    
    # Standard workload sizes
    SIZE_SMALL = 100
    SIZE_MEDIUM = 1000
    SIZE_LARGE = 10000
    
    # Product type categories
    PRODUCT_TYPES = {
        "electronics": 0.25,
        "fashion": 0.20,
        "home": 0.15,
        "books": 0.10,
        "sports": 0.10,
        "beauty": 0.10,
        "toys": 0.05,
        "food": 0.05,
    }
    
    # Edge case product IDs (special cases for testing)
    EDGE_CASE_IDS = [
        "999999999",  # Very large ID
        "1",          # Minimal ID
        "0",          # Zero ID
        "abc123",     # Alphanumeric ID
        "special-chars-!@#",  # Special characters
    ]
    
    def __init__(self, storage_dir: str = "data/profiling/workloads"):
        """Initialize the workload generator.
        
        Args:
            storage_dir: Directory to store workload definitions
        """
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_workload(
        self,
        workload_id: str,
        size: int,
        seed: int = 42,
        product_types: Optional[Dict[str, float]] = None,
        include_edge_cases: bool = True,
        description: str = ""
    ) -> BenchmarkWorkload:
        """Generate a benchmark workload.
        
        Args:
            workload_id: Unique identifier for this workload
            size: Number of products in the workload
            seed: Random seed for reproducibility
            product_types: Distribution of product types (defaults to standard)
            include_edge_cases: Whether to include edge case products
            description: Description of the workload
        
        Returns:
            BenchmarkWorkload with generated product IDs
        """
        if product_types is None:
            product_types = self.PRODUCT_TYPES.copy()
        
        # Create config
        config = WorkloadConfig(
            workload_id=workload_id,
            size=size,
            seed=seed,
            product_types=product_types,
            include_edge_cases=include_edge_cases,
            description=description or f"Benchmark workload with {size} products",
        )
        
        # Generate product IDs deterministically
        product_ids = self._generate_product_ids(config)
        
        # Compute checksum
        checksum = BenchmarkWorkload._compute_checksum(product_ids)
        
        return BenchmarkWorkload(
            config=config,
            product_ids=product_ids,
            checksum=checksum,
            created_at=datetime.now(),
        )
    
    def _generate_product_ids(self, config: WorkloadConfig) -> List[str]:
        """Generate product IDs based on configuration.
        
        Args:
            config: Workload configuration
        
        Returns:
            List of product IDs
        """
        # Set random seed for reproducibility
        rng = random.Random(config.seed)
        
        product_ids = []
        
        # Add edge cases if requested
        edge_case_count = 0
        if config.include_edge_cases:
            edge_case_count = min(len(self.EDGE_CASE_IDS), config.size // 20)  # 5% edge cases
            product_ids.extend(self.EDGE_CASE_IDS[:edge_case_count])
        
        # Generate remaining IDs by product type
        remaining = config.size - edge_case_count
        
        for product_type, percentage in config.product_types.items():
            count = int(remaining * percentage)
            
            # Generate IDs for this product type
            # Use type-specific ID ranges for diversity
            type_ids = self._generate_type_ids(product_type, count, rng)
            product_ids.extend(type_ids)
        
        # Fill any remaining slots (due to rounding)
        while len(product_ids) < config.size:
            product_ids.append(str(rng.randint(1000000, 9999999)))
        
        # Shuffle to mix types
        rng.shuffle(product_ids)
        
        # Ensure exact size
        return product_ids[:config.size]
    
    def _generate_type_ids(
        self,
        product_type: str,
        count: int,
        rng: random.Random
    ) -> List[str]:
        """Generate product IDs for a specific type.
        
        Args:
            product_type: Type of product
            count: Number of IDs to generate
            rng: Random number generator
        
        Returns:
            List of product IDs
        """
        # Use type-specific ID ranges for realism
        type_ranges = {
            "electronics": (1000000, 1999999),
            "fashion": (2000000, 2999999),
            "home": (3000000, 3999999),
            "books": (4000000, 4999999),
            "sports": (5000000, 5999999),
            "beauty": (6000000, 6999999),
            "toys": (7000000, 7999999),
            "food": (8000000, 8999999),
        }
        
        start, end = type_ranges.get(product_type, (1000000, 9999999))
        
        return [str(rng.randint(start, end)) for _ in range(count)]
    
    def generate_standard_workloads(self, seed: int = 42) -> Dict[str, BenchmarkWorkload]:
        """Generate standard small, medium, and large workloads.
        
        Args:
            seed: Random seed for reproducibility
        
        Returns:
            Dictionary mapping size name to workload
        """
        workloads = {}
        
        # Small workload
        workloads["small"] = self.generate_workload(
            workload_id=f"standard_small_{seed}",
            size=self.SIZE_SMALL,
            seed=seed,
            description=f"Standard small workload ({self.SIZE_SMALL} products)",
        )
        
        # Medium workload
        workloads["medium"] = self.generate_workload(
            workload_id=f"standard_medium_{seed}",
            size=self.SIZE_MEDIUM,
            seed=seed + 1,  # Different seed for diversity
            description=f"Standard medium workload ({self.SIZE_MEDIUM} products)",
        )
        
        # Large workload
        workloads["large"] = self.generate_workload(
            workload_id=f"standard_large_{seed}",
            size=self.SIZE_LARGE,
            seed=seed + 2,  # Different seed for diversity
            description=f"Standard large workload ({self.SIZE_LARGE} products)",
        )
        
        return workloads
    
    def save_workload(self, workload: BenchmarkWorkload) -> None:
        """Save workload to disk.
        
        Args:
            workload: BenchmarkWorkload to save
        """
        filepath = self.storage_dir / f"{workload.config.workload_id}.json"
        
        with open(filepath, "w") as f:
            json.dump(workload.to_dict(), f, indent=2)
    
    def load_workload(self, workload_id: str) -> Optional[BenchmarkWorkload]:
        """Load workload from disk.
        
        Args:
            workload_id: ID of workload to load
        
        Returns:
            BenchmarkWorkload or None if not found
        """
        filepath = self.storage_dir / f"{workload_id}.json"
        
        if not filepath.exists():
            return None
        
        with open(filepath, "r") as f:
            data = json.load(f)
        
        return BenchmarkWorkload.from_dict(data)
    
    def list_workloads(self) -> List[str]:
        """List all available workload IDs.
        
        Returns:
            List of workload IDs
        """
        workload_ids = []
        for filepath in self.storage_dir.glob("*.json"):
            workload_ids.append(filepath.stem)
        return sorted(workload_ids)
    
    def delete_workload(self, workload_id: str) -> bool:
        """Delete a workload.
        
        Args:
            workload_id: ID of workload to delete
        
        Returns:
            True if deleted, False if not found
        """
        filepath = self.storage_dir / f"{workload_id}.json"
        if filepath.exists():
            filepath.unlink()
            return True
        return False
    
    def export_workload(self, workload_id: str, output_path: str) -> None:
        """Export workload to a specific file path.
        
        Args:
            workload_id: ID of workload to export
            output_path: Path to export to
        
        Raises:
            ValueError: If workload not found
        """
        workload = self.load_workload(workload_id)
        if workload is None:
            raise ValueError(f"Workload not found: {workload_id}")
        
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, "w") as f:
            json.dump(workload.to_dict(), f, indent=2)
    
    def import_workload(self, input_path: str) -> BenchmarkWorkload:
        """Import workload from a file.
        
        Args:
            input_path: Path to workload file
        
        Returns:
            Imported BenchmarkWorkload
        
        Raises:
            FileNotFoundError: If file doesn't exist
        """
        input_file = Path(input_path)
        
        if not input_file.exists():
            raise FileNotFoundError(f"Workload file not found: {input_path}")
        
        with open(input_file, "r") as f:
            data = json.load(f)
        
        workload = BenchmarkWorkload.from_dict(data)
        self.save_workload(workload)
        
        return workload


# Global workload generator instance
_global_workload_generator: Optional[BenchmarkWorkloadGenerator] = None


def get_global_workload_generator() -> BenchmarkWorkloadGenerator:
    """Get the global workload generator instance.
    
    Creates the instance if it doesn't exist.
    """
    global _global_workload_generator
    if _global_workload_generator is None:
        _global_workload_generator = BenchmarkWorkloadGenerator()
    return _global_workload_generator
