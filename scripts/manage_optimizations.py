#!/usr/bin/env python3
"""
Optimization Management Script

This script helps manage performance optimization feature flags in Airflow.
"""

import argparse
import subprocess
import sys


class OptimizationManager:
    """Manages performance optimization feature flags"""

    FEATURE_FLAGS = {
        "profiling": {
            "variable": "ENABLE_PROFILING",
            "description": "Enable profiling for all major DAG tasks",
            "default": "false",
            "impact": "Adds ~5-10% overhead but provides detailed performance metrics",
        },
        "vectorized": {
            "variable": "USE_VECTORIZED_TRANSFORM",
            "description": "Use vectorized transformer (Pandas/NumPy)",
            "default": "false",
            "impact": "5-10x faster transformations, requires pandas/numpy",
        },
        "async": {
            "variable": "USE_ASYNC_CRAWLER",
            "description": "Use async HTTP client for concurrent requests",
            "default": "false",
            "impact": "3-4x faster I/O, requires aiohttp",
        },
        "metrics": {
            "variable": "ENABLE_METRICS_COLLECTION",
            "description": "Collect and store performance metrics",
            "default": "false",
            "impact": "Minimal overhead, enables trend analysis",
        },
    }

    def __init__(self):
        self.airflow_available = self._check_airflow()

    def _check_airflow(self) -> bool:
        """Check if Airflow CLI is available"""
        try:
            result = subprocess.run(
                ["airflow", "version"], capture_output=True, text=True, timeout=5
            )
            return result.returncode == 0
        except Exception:
            return False

    def list_flags(self):
        """List all available feature flags"""
        print("\n" + "=" * 80)
        print("AVAILABLE OPTIMIZATION FEATURE FLAGS")
        print("=" * 80)

        for name, config in self.FEATURE_FLAGS.items():
            print(f"\n{name.upper()}")
            print(f"  Variable: {config['variable']}")
            print(f"  Description: {config['description']}")
            print(f"  Default: {config['default']}")
            print(f"  Impact: {config['impact']}")

            # Try to get current value
            if self.airflow_available:
                current = self._get_variable(config["variable"])
                if current:
                    print(f"  Current: {current}")

        print("\n" + "=" * 80)

    def _get_variable(self, variable_name: str) -> str:
        """Get Airflow variable value"""
        try:
            result = subprocess.run(
                ["airflow", "variables", "get", variable_name],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except Exception:
            pass
        return None

    def _set_variable(self, variable_name: str, value: str) -> bool:
        """Set Airflow variable value"""
        try:
            result = subprocess.run(
                ["airflow", "variables", "set", variable_name, value],
                capture_output=True,
                text=True,
                timeout=5,
            )
            return result.returncode == 0
        except Exception:
            return False

    def enable(self, optimization: str):
        """Enable an optimization"""
        if optimization not in self.FEATURE_FLAGS:
            print(f"❌ Unknown optimization: {optimization}")
            print(f"Available: {', '.join(self.FEATURE_FLAGS.keys())}")
            return False

        config = self.FEATURE_FLAGS[optimization]
        variable = config["variable"]

        if not self.airflow_available:
            print("⚠️  Airflow CLI not available. Set manually:")
            print(f"   airflow variables set {variable} true")
            print(f"   OR export {variable}=true")
            return False

        print(f"🔧 Enabling {optimization}...")
        if self._set_variable(variable, "true"):
            print(f"✅ {optimization} enabled ({variable}=true)")
            print(f"   Impact: {config['impact']}")
            print("\n⚠️  Remember to restart Airflow scheduler for changes to take effect!")
            return True
        else:
            print(f"❌ Failed to enable {optimization}")
            return False

    def disable(self, optimization: str):
        """Disable an optimization"""
        if optimization not in self.FEATURE_FLAGS:
            print(f"❌ Unknown optimization: {optimization}")
            print(f"Available: {', '.join(self.FEATURE_FLAGS.keys())}")
            return False

        config = self.FEATURE_FLAGS[optimization]
        variable = config["variable"]

        if not self.airflow_available:
            print("⚠️  Airflow CLI not available. Set manually:")
            print(f"   airflow variables set {variable} false")
            print(f"   OR export {variable}=false")
            return False

        print(f"🔧 Disabling {optimization}...")
        if self._set_variable(variable, "false"):
            print(f"✅ {optimization} disabled ({variable}=false)")
            print("\n⚠️  Remember to restart Airflow scheduler for changes to take effect!")
            return True
        else:
            print(f"❌ Failed to disable {optimization}")
            return False

    def status(self):
        """Show status of all optimizations"""
        print("\n" + "=" * 80)
        print("OPTIMIZATION STATUS")
        print("=" * 80)

        if not self.airflow_available:
            print("\n⚠️  Airflow CLI not available. Cannot check current status.")
            print("Set variables manually using:")
            print("  airflow variables set <VARIABLE_NAME> <true|false>")
            print("  OR export <VARIABLE_NAME>=<true|false>")
            return

        for name, config in self.FEATURE_FLAGS.items():
            variable = config["variable"]
            current = self._get_variable(variable)

            status_icon = "✅" if current == "true" else "❌"
            status_text = "ENABLED" if current == "true" else "DISABLED"

            print(f"\n{status_icon} {name.upper()}: {status_text}")
            print(f"   Variable: {variable}")
            print(f"   Current: {current or 'not set (default: false)'}")

        print("\n" + "=" * 80)

    def enable_all(self):
        """Enable all optimizations"""
        print("\n🚀 Enabling ALL optimizations...")
        success_count = 0

        for name in self.FEATURE_FLAGS.keys():
            if self.enable(name):
                success_count += 1
            print()

        print(f"\n✅ Enabled {success_count}/{len(self.FEATURE_FLAGS)} optimizations")
        print("\n⚠️  Remember to restart Airflow scheduler!")

    def disable_all(self):
        """Disable all optimizations"""
        print("\n🛑 Disabling ALL optimizations...")
        success_count = 0

        for name in self.FEATURE_FLAGS.keys():
            if self.disable(name):
                success_count += 1
            print()

        print(f"\n✅ Disabled {success_count}/{len(self.FEATURE_FLAGS)} optimizations")
        print("\n⚠️  Remember to restart Airflow scheduler!")

    def recommended_rollout(self):
        """Show recommended rollout strategy"""
        print("\n" + "=" * 80)
        print("RECOMMENDED ROLLOUT STRATEGY")
        print("=" * 80)

        print("\n📅 WEEK 1: Profiling & Configuration")
        print("  1. Run configuration analyzer:")
        print("     python scripts/apply_optimal_configuration.py")
        print("  2. Review and apply recommended configuration")
        print("  3. Enable profiling:")
        print("     python scripts/manage_optimizations.py enable profiling")
        print("  4. Run DAG with normal workload")
        print("  5. Analyze profiling results in XCom")
        print("  6. Identify bottlenecks")

        print("\n📅 WEEK 2: Transform Optimization")
        print("  1. If transform is slow, enable vectorized:")
        print("     python scripts/manage_optimizations.py enable vectorized")
        print("  2. Run DAG and compare execution times")
        print("  3. Verify output correctness")

        print("\n📅 WEEK 2-3: Metrics Collection")
        print("  1. Enable metrics:")
        print("     python scripts/manage_optimizations.py enable metrics")
        print("  2. Collect baseline metrics")
        print("  3. Setup regression detection")

        print("\n📅 WEEK 3-4: Async Crawler")
        print("  1. If I/O is slow, enable async:")
        print("     python scripts/manage_optimizations.py enable async")
        print("  2. Test with small dataset first")
        print("  3. Monitor error rates")

        print("\n" + "=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description="Manage performance optimization feature flags",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all available optimizations
  python scripts/manage_optimizations.py list

  # Show current status
  python scripts/manage_optimizations.py status

  # Enable specific optimization
  python scripts/manage_optimizations.py enable profiling

  # Disable specific optimization
  python scripts/manage_optimizations.py disable vectorized

  # Enable all optimizations
  python scripts/manage_optimizations.py enable-all

  # Show recommended rollout strategy
  python scripts/manage_optimizations.py rollout
        """,
    )

    parser.add_argument(
        "action",
        choices=["list", "status", "enable", "disable", "enable-all", "disable-all", "rollout"],
        help="Action to perform",
    )

    parser.add_argument(
        "optimization",
        nargs="?",
        choices=["profiling", "vectorized", "async", "metrics"],
        help="Optimization to enable/disable (required for enable/disable actions)",
    )

    args = parser.parse_args()

    manager = OptimizationManager()

    if args.action == "list":
        manager.list_flags()
    elif args.action == "status":
        manager.status()
    elif args.action == "enable":
        if not args.optimization:
            print("❌ Error: optimization name required for 'enable' action")
            print("Available: profiling, vectorized, async, metrics")
            sys.exit(1)
        manager.enable(args.optimization)
    elif args.action == "disable":
        if not args.optimization:
            print("❌ Error: optimization name required for 'disable' action")
            print("Available: profiling, vectorized, async, metrics")
            sys.exit(1)
        manager.disable(args.optimization)
    elif args.action == "enable-all":
        manager.enable_all()
    elif args.action == "disable-all":
        manager.disable_all()
    elif args.action == "rollout":
        manager.recommended_rollout()


if __name__ == "__main__":
    main()
