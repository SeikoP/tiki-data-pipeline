"""Baseline metrics capture system for performance benchmarking.

This module provides functionality to capture and store baseline performance
metrics from the original pipeline implementation, enabling accurate before/after
comparisons during optimization.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .metrics_collector import PipelineRunMetrics


@dataclass
class BaselineMetrics:
    """Baseline performance metrics for comparison.

    Attributes:
        baseline_id: Unique identifier for this baseline
        captured_at: When the baseline was captured
        description: Description of the baseline (e.g., "Original implementation")
        metrics: Pipeline run metrics
        environment: Environmental conditions during capture
    """

    baseline_id: str
    captured_at: datetime
    description: str
    metrics: PipelineRunMetrics
    environment: dict

    def to_dict(self) -> dict:
        """Convert baseline to dictionary for serialization."""
        return {
            "baseline_id": self.baseline_id,
            "captured_at": self.captured_at.isoformat(),
            "description": self.description,
            "metrics": self.metrics.to_dict(),
            "environment": self.environment,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "BaselineMetrics":
        """Create BaselineMetrics from dictionary.

        Args:
            data: Dictionary representation

        Returns:
            BaselineMetrics instance
        """
        # Reconstruct PipelineRunMetrics
        metrics_data = data["metrics"]
        metrics = PipelineRunMetrics(
            run_id=metrics_data["run_id"],
            start_time=datetime.fromisoformat(metrics_data["start_time"]),
            end_time=datetime.fromisoformat(metrics_data["end_time"])
            if metrics_data["end_time"]
            else None,
            duration_seconds=metrics_data["duration_seconds"],
            products_processed=metrics_data["products_processed"],
            throughput_products_per_second=metrics_data["throughput_products_per_second"],
            latency_per_product_ms=metrics_data["latency_per_product_ms"],
            error_count=metrics_data["error_count"],
            error_rate=metrics_data["error_rate"],
            cpu_utilization_percent=metrics_data["cpu_utilization_percent"],
            memory_peak_mb=metrics_data["memory_peak_mb"],
            memory_avg_mb=metrics_data["memory_avg_mb"],
            disk_read_mb=metrics_data["disk_read_mb"],
            disk_write_mb=metrics_data["disk_write_mb"],
            network_sent_mb=metrics_data["network_sent_mb"],
            network_recv_mb=metrics_data["network_recv_mb"],
            cache_l1_hits=metrics_data["cache_l1_hits"],
            cache_l1_misses=metrics_data["cache_l1_misses"],
            cache_l2_hits=metrics_data["cache_l2_hits"],
            cache_l2_misses=metrics_data["cache_l2_misses"],
            cache_l3_hits=metrics_data["cache_l3_hits"],
            cache_l3_misses=metrics_data["cache_l3_misses"],
            cache_l1_hit_rate=metrics_data["cache_l1_hit_rate"],
            cache_l2_hit_rate=metrics_data["cache_l2_hit_rate"],
            cache_l3_hit_rate=metrics_data["cache_l3_hit_rate"],
            data_completeness_percent=metrics_data["data_completeness_percent"],
        )

        return cls(
            baseline_id=data["baseline_id"],
            captured_at=datetime.fromisoformat(data["captured_at"]),
            description=data["description"],
            metrics=metrics,
            environment=data["environment"],
        )


class BaselineCaptureSystem:
    """System for capturing and managing baseline performance metrics.

    Captures metrics from the original pipeline implementation and stores them
    for comparison with optimized versions. Supports multiple baselines for
    different configurations or dataset sizes.

    Example:
        >>> capture_system = BaselineCaptureSystem()
        >>> # Run pipeline and collect metrics
        >>> collector = MetricsCollector()
        >>> collector.start_run("baseline_001")
        >>> # ... execute pipeline ...
        >>> metrics = collector.end_run()
        >>> # Capture baseline
        >>> baseline = capture_system.capture_baseline(
        ...     baseline_id="baseline_001",
        ...     description="Original implementation - 1000 products",
        ...     metrics=metrics
        ... )
        >>> capture_system.save_baseline(baseline)
    """

    def __init__(self, storage_dir: str = "data/profiling/baselines"):
        """Initialize the baseline capture system.

        Args:
            storage_dir: Directory to store baseline metrics
        """
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self._baselines: dict[str, BaselineMetrics] = {}
        self._load_existing_baselines()

    def capture_baseline(
        self,
        baseline_id: str,
        description: str,
        metrics: PipelineRunMetrics,
        environment: dict | None = None,
    ) -> BaselineMetrics:
        """Capture a baseline from pipeline run metrics.

        Args:
            baseline_id: Unique identifier for this baseline
            description: Description of the baseline
            metrics: Pipeline run metrics to capture
            environment: Optional environmental conditions

        Returns:
            BaselineMetrics instance
        """
        if environment is None:
            environment = {}

        baseline = BaselineMetrics(
            baseline_id=baseline_id,
            captured_at=datetime.now(),
            description=description,
            metrics=metrics,
            environment=environment,
        )

        self._baselines[baseline_id] = baseline
        return baseline

    def save_baseline(self, baseline: BaselineMetrics) -> None:
        """Save baseline to disk.

        Args:
            baseline: BaselineMetrics to save
        """
        filepath = self.storage_dir / f"{baseline.baseline_id}.json"

        with open(filepath, "w") as f:
            json.dump(baseline.to_dict(), f, indent=2)

    def load_baseline(self, baseline_id: str) -> BaselineMetrics | None:
        """Load baseline from disk.

        Args:
            baseline_id: ID of baseline to load

        Returns:
            BaselineMetrics or None if not found
        """
        # Check in-memory cache first
        if baseline_id in self._baselines:
            return self._baselines[baseline_id]

        # Try loading from disk
        filepath = self.storage_dir / f"{baseline_id}.json"

        if not filepath.exists():
            return None

        with open(filepath) as f:
            data = json.load(f)

        baseline = BaselineMetrics.from_dict(data)
        self._baselines[baseline_id] = baseline
        return baseline

    def list_baselines(self) -> list[str]:
        """List all available baseline IDs.

        Returns:
            List of baseline IDs
        """
        # Get from memory
        memory_ids = set(self._baselines.keys())

        # Get from disk
        disk_ids = set()
        for filepath in self.storage_dir.glob("*.json"):
            disk_ids.add(filepath.stem)

        return sorted(memory_ids | disk_ids)

    def get_baseline(self, baseline_id: str) -> BaselineMetrics | None:
        """Get baseline by ID (loads from disk if needed).

        Args:
            baseline_id: ID of baseline to retrieve

        Returns:
            BaselineMetrics or None if not found
        """
        return self.load_baseline(baseline_id)

    def delete_baseline(self, baseline_id: str) -> bool:
        """Delete a baseline.

        Args:
            baseline_id: ID of baseline to delete

        Returns:
            True if deleted, False if not found
        """
        # Remove from memory
        if baseline_id in self._baselines:
            del self._baselines[baseline_id]

        # Remove from disk
        filepath = self.storage_dir / f"{baseline_id}.json"
        if filepath.exists():
            filepath.unlink()
            return True

        return False

    def _load_existing_baselines(self) -> None:
        """Load all existing baselines from disk into memory."""
        for filepath in self.storage_dir.glob("*.json"):
            try:
                with open(filepath) as f:
                    data = json.load(f)
                baseline = BaselineMetrics.from_dict(data)
                self._baselines[baseline.baseline_id] = baseline
            except Exception as e:
                print(f"Warning: Failed to load baseline from {filepath}: {e}")

    def get_latest_baseline(self) -> BaselineMetrics | None:
        """Get the most recently captured baseline.

        Returns:
            Most recent BaselineMetrics or None if no baselines exist
        """
        if not self._baselines:
            return None

        return max(self._baselines.values(), key=lambda b: b.captured_at)

    def export_baseline(self, baseline_id: str, output_path: str) -> None:
        """Export baseline to a specific file path.

        Args:
            baseline_id: ID of baseline to export
            output_path: Path to export to

        Raises:
            ValueError: If baseline not found
        """
        baseline = self.get_baseline(baseline_id)
        if baseline is None:
            raise ValueError(f"Baseline not found: {baseline_id}")

        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w") as f:
            json.dump(baseline.to_dict(), f, indent=2)

    def import_baseline(self, input_path: str) -> BaselineMetrics:
        """Import baseline from a file.

        Args:
            input_path: Path to baseline file

        Returns:
            Imported BaselineMetrics

        Raises:
            FileNotFoundError: If file doesn't exist
        """
        input_file = Path(input_path)

        if not input_file.exists():
            raise FileNotFoundError(f"Baseline file not found: {input_path}")

        with open(input_file) as f:
            data = json.load(f)

        baseline = BaselineMetrics.from_dict(data)
        self._baselines[baseline.baseline_id] = baseline
        self.save_baseline(baseline)

        return baseline


# Global baseline capture system instance
_global_baseline_system: BaselineCaptureSystem | None = None


def get_global_baseline_system() -> BaselineCaptureSystem:
    """Get the global baseline capture system instance.

    Creates the instance if it doesn't exist.
    """
    global _global_baseline_system
    if _global_baseline_system is None:
        _global_baseline_system = BaselineCaptureSystem()
    return _global_baseline_system
