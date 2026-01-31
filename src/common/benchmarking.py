"""Comprehensive benchmarking infrastructure for performance testing.

This module provides a complete benchmarking system that integrates baseline
capture, workload generation, environmental recording, and reproducibility
validation.
"""

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .baseline_capture import BaselineCaptureSystem, BaselineMetrics, get_global_baseline_system
from .benchmark_reproducibility import (
    BenchmarkReproducibilityValidator,
    ReproducibilityReport,
    get_global_reproducibility_validator,
)
from .benchmark_workload import (
    BenchmarkWorkload,
    BenchmarkWorkloadGenerator,
    get_global_workload_generator,
)
from .environment_recorder import (
    EnvironmentalConditions,
    EnvironmentRecorder,
    get_global_environment_recorder,
)
from .metrics_collector import PipelineRunMetrics


@dataclass
class BenchmarkRun:
    """A single benchmark run with all associated data.

    Attributes:
        run_id: Unique identifier for this run
        benchmark_id: ID of the benchmark being run
        workload_id: ID of the workload used
        metrics: Pipeline run metrics
        environment: Environmental conditions
        timestamp: When the run was executed
    """

    run_id: str
    benchmark_id: str
    workload_id: str
    metrics: PipelineRunMetrics
    environment: EnvironmentalConditions
    timestamp: datetime

    def to_dict(self) -> dict:
        """Convert run to dictionary for serialization."""
        return {
            "run_id": self.run_id,
            "benchmark_id": self.benchmark_id,
            "workload_id": self.workload_id,
            "metrics": self.metrics.to_dict(),
            "environment": self.environment.to_dict(),
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class BenchmarkSuite:
    """A suite of benchmark runs for comprehensive testing.

    Attributes:
        suite_id: Unique identifier for this suite
        description: Description of the suite
        workload_sizes: List of workload sizes tested
        runs: List of benchmark runs
        baseline: Optional baseline metrics for comparison
        reproducibility_report: Optional reproducibility analysis
        created_at: When the suite was created
    """

    suite_id: str
    description: str
    workload_sizes: list[str]
    runs: list[BenchmarkRun]
    baseline: BaselineMetrics | None
    reproducibility_report: ReproducibilityReport | None
    created_at: datetime

    def to_dict(self) -> dict:
        """Convert suite to dictionary for serialization."""
        return {
            "suite_id": self.suite_id,
            "description": self.description,
            "workload_sizes": self.workload_sizes,
            "runs": [r.to_dict() for r in self.runs],
            "baseline": self.baseline.to_dict() if self.baseline else None,
            "reproducibility_report": self.reproducibility_report.to_dict()
            if self.reproducibility_report
            else None,
            "created_at": self.created_at.isoformat(),
        }


class BenchmarkingSystem:
    """Comprehensive benchmarking system for performance testing.

    Integrates all benchmarking components to provide a complete solution for
    capturing baselines, generating workloads, recording environmental conditions,
    and validating reproducibility.

    Example:
        >>> system = BenchmarkingSystem()
        >>>
        >>> # Generate standard workloads
        >>> workloads = system.generate_standard_workloads()
        >>>
        >>> # Run benchmark with baseline capture
        >>> def run_pipeline(workload):
        ...     # Execute pipeline with workload
        ...     return metrics
        >>>
        >>> baseline = system.run_baseline_benchmark(
        ...     benchmark_id="baseline_001",
        ...     workload=workloads["small"],
        ...     pipeline_func=run_pipeline
        ... )
        >>>
        >>> # Validate reproducibility
        >>> report = system.validate_reproducibility(
        ...     benchmark_id="baseline_001",
        ...     workload=workloads["small"],
        ...     pipeline_func=run_pipeline,
        ...     num_runs=5
        ... )
    """

    def __init__(
        self,
        storage_dir: str = "data/profiling",
        baseline_system: BaselineCaptureSystem | None = None,
        workload_generator: BenchmarkWorkloadGenerator | None = None,
        environment_recorder: EnvironmentRecorder | None = None,
        reproducibility_validator: BenchmarkReproducibilityValidator | None = None,
    ):
        """Initialize the benchmarking system.

        Args:
            storage_dir: Base directory for storing benchmark data
            baseline_system: Optional baseline capture system (uses global if None)
            workload_generator: Optional workload generator (uses global if None)
            environment_recorder: Optional environment recorder (uses global if None)
            reproducibility_validator: Optional reproducibility validator (uses global if None)
        """
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)

        self.baseline_system = baseline_system or get_global_baseline_system()
        self.workload_generator = workload_generator or get_global_workload_generator()
        self.environment_recorder = environment_recorder or get_global_environment_recorder()
        self.reproducibility_validator = (
            reproducibility_validator or get_global_reproducibility_validator()
        )

        self.suites_dir = self.storage_dir / "suites"
        self.suites_dir.mkdir(exist_ok=True)

    def generate_standard_workloads(self, seed: int = 42) -> dict[str, BenchmarkWorkload]:
        """Generate standard small, medium, and large workloads.

        Args:
            seed: Random seed for reproducibility

        Returns:
            Dictionary mapping size name to workload
        """
        workloads = self.workload_generator.generate_standard_workloads(seed=seed)

        # Save workloads
        for workload in workloads.values():
            self.workload_generator.save_workload(workload)

        return workloads

    def run_baseline_benchmark(
        self,
        benchmark_id: str,
        workload: BenchmarkWorkload,
        pipeline_func: Callable[[BenchmarkWorkload], PipelineRunMetrics],
        description: str = "",
    ) -> BaselineMetrics:
        """Run a benchmark and capture it as a baseline.

        Args:
            benchmark_id: Unique identifier for this benchmark
            workload: Workload to use for the benchmark
            pipeline_func: Function that runs the pipeline and returns metrics
            description: Description of the baseline

        Returns:
            BaselineMetrics with captured data
        """
        # Record environmental conditions
        environment = self.environment_recorder.record_conditions()

        # Run pipeline
        metrics = pipeline_func(workload)

        # Capture baseline
        baseline = self.baseline_system.capture_baseline(
            baseline_id=benchmark_id,
            description=description or f"Baseline for {workload.config.workload_id}",
            metrics=metrics,
            environment=environment.to_dict(),
        )

        # Save baseline
        self.baseline_system.save_baseline(baseline)

        return baseline

    def validate_reproducibility(
        self,
        benchmark_id: str,
        workload: BenchmarkWorkload,
        pipeline_func: Callable[[BenchmarkWorkload], PipelineRunMetrics],
        num_runs: int = 5,
    ) -> ReproducibilityReport:
        """Validate benchmark reproducibility by running multiple times.

        Args:
            benchmark_id: Unique identifier for this benchmark
            workload: Workload to use for the benchmark
            pipeline_func: Function that runs the pipeline and returns metrics
            num_runs: Number of runs to execute (default: 5)

        Returns:
            ReproducibilityReport with variance analysis
        """
        runs = []

        for _i in range(num_runs):
            # Run pipeline
            metrics = pipeline_func(workload)
            runs.append(metrics)

        # Validate reproducibility
        report = self.reproducibility_validator.validate_reproducibility(
            benchmark_id=benchmark_id, runs=runs
        )

        return report

    def run_benchmark_suite(
        self,
        suite_id: str,
        workloads: dict[str, BenchmarkWorkload],
        pipeline_func: Callable[[BenchmarkWorkload], PipelineRunMetrics],
        description: str = "",
        capture_baseline: bool = True,
        validate_reproducibility: bool = False,
        reproducibility_runs: int = 3,
    ) -> BenchmarkSuite:
        """Run a complete benchmark suite across multiple workload sizes.

        Args:
            suite_id: Unique identifier for this suite
            workloads: Dictionary of workloads to test
            pipeline_func: Function that runs the pipeline and returns metrics
            description: Description of the suite
            capture_baseline: Whether to capture baseline metrics
            validate_reproducibility: Whether to validate reproducibility
            reproducibility_runs: Number of runs for reproducibility validation

        Returns:
            BenchmarkSuite with all results
        """
        runs = []
        baseline = None
        reproducibility_report = None

        # Run benchmarks for each workload
        for size_name, workload in workloads.items():
            # Record environment
            environment = self.environment_recorder.record_conditions()

            # Run pipeline
            run_id = f"{suite_id}_{size_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            metrics = pipeline_func(workload)

            # Create benchmark run
            run = BenchmarkRun(
                run_id=run_id,
                benchmark_id=suite_id,
                workload_id=workload.config.workload_id,
                metrics=metrics,
                environment=environment,
                timestamp=datetime.now(),
            )
            runs.append(run)

            # Capture baseline for first workload if requested
            if capture_baseline and baseline is None:
                baseline = self.baseline_system.capture_baseline(
                    baseline_id=f"{suite_id}_baseline",
                    description=f"Baseline for {suite_id}",
                    metrics=metrics,
                    environment=environment.to_dict(),
                )
                self.baseline_system.save_baseline(baseline)

        # Validate reproducibility if requested (using first workload)
        if validate_reproducibility and workloads:
            first_workload = list(workloads.values())[0]
            reproducibility_report = self.validate_reproducibility(
                benchmark_id=suite_id,
                workload=first_workload,
                pipeline_func=pipeline_func,
                num_runs=reproducibility_runs,
            )

        # Create suite
        suite = BenchmarkSuite(
            suite_id=suite_id,
            description=description or f"Benchmark suite {suite_id}",
            workload_sizes=list(workloads.keys()),
            runs=runs,
            baseline=baseline,
            reproducibility_report=reproducibility_report,
            created_at=datetime.now(),
        )

        # Save suite
        self.save_suite(suite)

        return suite

    def save_suite(self, suite: BenchmarkSuite) -> None:
        """Save benchmark suite to disk.

        Args:
            suite: BenchmarkSuite to save
        """
        filepath = self.suites_dir / f"{suite.suite_id}.json"

        with open(filepath, "w") as f:
            json.dump(suite.to_dict(), f, indent=2)

    def load_suite(self, suite_id: str) -> BenchmarkSuite | None:
        """Load benchmark suite from disk.

        Args:
            suite_id: ID of suite to load

        Returns:
            BenchmarkSuite or None if not found
        """
        filepath = self.suites_dir / f"{suite_id}.json"

        if not filepath.exists():
            return None

        with open(filepath) as f:
            json.load(f)

        # Reconstruct suite (simplified - full reconstruction would need more work)
        # This is a basic implementation
        return None  # TODO: Implement full deserialization

    def list_suites(self) -> list[str]:
        """List all available benchmark suite IDs.

        Returns:
            List of suite IDs
        """
        suite_ids = []
        for filepath in self.suites_dir.glob("*.json"):
            suite_ids.append(filepath.stem)
        return sorted(suite_ids)

    def generate_suite_report(self, suite: BenchmarkSuite) -> str:
        """Generate a comprehensive report for a benchmark suite.

        Args:
            suite: BenchmarkSuite to report on

        Returns:
            Formatted report string
        """
        lines = ["=" * 80]
        lines.append("BENCHMARK SUITE REPORT")
        lines.append("=" * 80)
        lines.append("")

        lines.append(f"Suite ID: {suite.suite_id}")
        lines.append(f"Description: {suite.description}")
        lines.append(f"Created: {suite.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Workload Sizes: {', '.join(suite.workload_sizes)}")
        lines.append(f"Number of Runs: {len(suite.runs)}")
        lines.append("")

        # Baseline info
        if suite.baseline:
            lines.append("BASELINE METRICS:")
            lines.append(
                f"  Throughput: {suite.baseline.metrics.throughput_products_per_second:.2f} products/sec"
            )
            lines.append(
                f"  Latency: {suite.baseline.metrics.latency_per_product_ms:.2f} ms/product"
            )
            lines.append(f"  CPU: {suite.baseline.metrics.cpu_utilization_percent:.1f}%")
            lines.append(f"  Memory: {suite.baseline.metrics.memory_peak_mb:.1f} MB")
            lines.append("")

        # Run results
        lines.append("RUN RESULTS:")
        lines.append("")

        for run in suite.runs:
            lines.append(f"Run: {run.run_id}")
            lines.append(f"  Workload: {run.workload_id}")
            lines.append(
                f"  Throughput: {run.metrics.throughput_products_per_second:.2f} products/sec"
            )
            lines.append(f"  Latency: {run.metrics.latency_per_product_ms:.2f} ms/product")
            lines.append(f"  CPU: {run.metrics.cpu_utilization_percent:.1f}%")
            lines.append(f"  Memory: {run.metrics.memory_peak_mb:.1f} MB")
            lines.append(f"  Errors: {run.metrics.error_count}")
            lines.append("")

        # Reproducibility report
        if suite.reproducibility_report:
            lines.append("REPRODUCIBILITY ANALYSIS:")
            if suite.reproducibility_report.overall_reproducible:
                lines.append("  ✓ Benchmark is reproducible")
            else:
                lines.append("  ✗ Benchmark has high variance")
                lines.append(
                    f"  Flagged metrics: {', '.join(suite.reproducibility_report.flagged_metrics)}"
                )
            lines.append("")

        lines.append("=" * 80)

        return "\n".join(lines)


# Global benchmarking system instance
_global_benchmarking_system: BenchmarkingSystem | None = None


def get_global_benchmarking_system() -> BenchmarkingSystem:
    """Get the global benchmarking system instance.

    Creates the instance if it doesn't exist.
    """
    global _global_benchmarking_system
    if _global_benchmarking_system is None:
        _global_benchmarking_system = BenchmarkingSystem()
    return _global_benchmarking_system
