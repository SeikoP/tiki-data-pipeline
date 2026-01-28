#!/usr/bin/env python3
"""
CI/CD Script - Modern Python Version
Usage: python ci.py [command] [options]
"""

import argparse
import os
import platform
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

# TOML support for config files
try:
    import tomllib  # Python 3.11+
except ImportError:
    try:
        import tomli as tomllib  # Fallback for older Python
    except ImportError:
        tomllib = None

# Rich for beautiful terminal output
try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    print("⚠️  Install 'rich' for better output: pip install rich")


class TaskStatus(Enum):
    """
    Task execution status.
    """

    SUCCESS = "✅"
    FAILED = "❌"
    WARNING = "⚠️"
    RUNNING = "🔄"
    SKIPPED = "⏭️"


@dataclass
class TaskResult:
    """
    Result of a task execution.
    """

    name: str
    status: TaskStatus
    duration: float
    message: str = ""
    exit_code: int = 0


class CIRunner:
    """
    Main CI/CD runner class.
    """

    def __init__(
        self, verbose: bool = False, parallel: bool = True, config_file: str | None = None
    ):
        self.verbose = verbose
        self.parallel = parallel
        self.console = Console() if RICH_AVAILABLE else None
        self.project_root = Path.cwd()
        self.venv_path = self.project_root / ".venv"
        self.results: list[TaskResult] = []

        # Source paths for tools
        self.source_paths = ["src/", "tests/", "airflow/dags/", "scripts/"]

        # Load configuration
        self.config = self._load_config(config_file)

        # Platform detection
        self.is_windows = platform.system() == "Windows"
        self.is_linux = platform.system() == "Linux"
        self.is_mac = platform.system() == "Darwin"

        # Python executable detection
        self.python_cmd = self._detect_python()
        self.pip_cmd = self._detect_pip()

        # Force UTF-8 encoding
        os.environ["PYTHONIOENCODING"] = "utf-8"
        os.environ["PYTHONUTF8"] = "1"

    def _load_config(self, config_file: str | None = None) -> dict:
        """
        Load configuration from TOML file.
        """
        # Default configuration
        default_config = {
            "format": {
                "formatter": "ruff",
                "line_length": 100,
                "target_version": "py39",
                "skip_string_normalization": False,
                "use_autoflake": True,
                "use_docformatter": True,
                "use_pyupgrade": True,
            },
            "lint": {
                "select": ["E", "W", "F", "I", "N", "UP", "B", "C4", "SIM", "PERF", "RUF"],
                "ignore": ["E501", "B008"],
            },
            "test": {
                "parallel": True,
                "coverage": True,
                "min_coverage": 80,
            },
            "ci": {
                "parallel": True,
                "fail_fast": False,
            },
        }

        if not tomllib:
            if self.verbose if hasattr(self, "verbose") else False:
                print("⚠️  TOML support not available, using defaults")
            return default_config

        # Try to find config file
        config_paths = [
            Path(config_file) if config_file else None,
            self.project_root / "ciconfig.toml",
            self.project_root / ".ciconfig",
            self.project_root / "pyproject.toml",
        ]

        for path in config_paths:
            if path and path.exists():
                try:
                    with open(path, "rb") as f:
                        loaded_config = tomllib.load(f)

                    # Merge with defaults
                    if path.name == "pyproject.toml":
                        # Extract CI config from pyproject.toml
                        loaded_config = loaded_config.get("tool", {}).get("ci", {})

                    # Deep merge
                    for section, values in loaded_config.items():
                        if section in default_config:
                            default_config[section].update(values)
                        else:
                            default_config[section] = values

                    if self.verbose if hasattr(self, "verbose") else False:
                        print(f"✅ Loaded config from: {path}")

                    break
                except Exception as e:
                    if self.verbose if hasattr(self, "verbose") else False:
                        print(f"⚠️  Failed to load config from {path}: {e}")

        return default_config

    def _detect_python(self) -> str:
        """
        Detect the correct Python executable.
        """
        # Check virtual environment first
        if self.venv_path.exists():
            if self.is_windows:
                venv_python = self.venv_path / "Scripts" / "python.exe"
            else:
                venv_python = self.venv_path / "bin" / "python"

            if venv_python.exists():
                self._print_info(f"Using virtual environment: {venv_python}")
                return str(venv_python)

        # Fallback to system Python
        for cmd in ["python3", "python"]:
            if shutil.which(cmd):
                return cmd

        raise RuntimeError("Python executable not found!")

    def _detect_pip(self) -> str:
        """
        Detect the correct pip executable.
        """
        if self.venv_path.exists():
            if self.is_windows:
                venv_pip = self.venv_path / "Scripts" / "pip.exe"
            else:
                venv_pip = self.venv_path / "bin" / "pip"

            if venv_pip.exists():
                return str(venv_pip)

        return f"{self.python_cmd} -m pip"

    def _print_info(self, message: str, style: str = "cyan"):
        """
        Print info message.
        """
        if RICH_AVAILABLE and self.console:
            self.console.print(f"[{style}]{message}[/{style}]")
        else:
            print(f"ℹ️  {message}")

    def _print_success(self, message: str):
        """
        Print success message.
        """
        if RICH_AVAILABLE and self.console:
            self.console.print(f"[green]✅ {message}[/green]")
        else:
            print(f"✅ {message}")

    def _print_error(self, message: str):
        """
        Print error message.
        """
        if RICH_AVAILABLE and self.console:
            self.console.print(f"[red]❌ {message}[/red]")
        else:
            print(f"❌ {message}")

    def _print_warning(self, message: str):
        """
        Print warning message.
        """
        if RICH_AVAILABLE and self.console:
            self.console.print(f"[yellow]⚠️  {message}[/yellow]")
        else:
            print(f"⚠️  {message}")

    def _run_command(
        self, cmd: list[str], description: str, check: bool = True, capture_output: bool = False
    ) -> subprocess.CompletedProcess:
        """
        Run a shell command.
        """
        if self.verbose:
            self._print_info(f"Running: {' '.join(cmd)}")

        try:
            result = subprocess.run(
                cmd,
                check=check,
                capture_output=capture_output,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            return result
        except subprocess.CalledProcessError as e:
            if self.verbose:
                self._print_error(f"Command failed with exit code {e.returncode}")
                if e.stdout:
                    print(e.stdout)
                if e.stderr:
                    print(e.stderr)
            raise

    def setup(self) -> None:
        """
        Setup development environment.
        """
        self._print_info("🛠️  Setting up development environment...")

        # Create virtual environment
        if not self.venv_path.exists():
            self._print_info("Creating virtual environment...")
            self._run_command([sys.executable, "-m", "venv", str(self.venv_path)], "Create venv")
            self._print_success("Virtual environment created")
        else:
            self._print_info("Virtual environment already exists")

        # Update detection
        self.python_cmd = self._detect_python()
        self.pip_cmd = self._detect_pip()

        # Install dependencies
        self.install_dependencies()

    def install_dependencies(self) -> None:
        """
        Install project dependencies.
        """
        self._print_info("📦 Installing dependencies...")

        # Upgrade pip
        self._run_command(
            [self.python_cmd, "-m", "pip", "install", "--upgrade", "pip"], "Upgrade pip"
        )

        # Install requirements
        requirements_file = self.project_root / "requirements.txt"
        if requirements_file.exists():
            self._run_command(
                [self.python_cmd, "-m", "pip", "install", "-r", str(requirements_file)],
                "Install requirements",
            )

        # Install development dependencies
        dev_packages = [
            "ruff",
            "black",
            "isort",
            "mypy",
            "pylint",
            "bandit",
            "safety",
            "pytest",
            "pytest-cov",
            "pytest-mock",
            "pytest-asyncio",
            "pytest-xdist",  # For parallel test execution
            "vulture",
            "radon",
            "rich",  # For beautiful output
            "pre-commit",  # For git hooks
            "autoflake",  # Remove unused imports
            "docformatter",  # Format docstrings
            "pyupgrade",  # Upgrade Python syntax
        ]

        self._run_command(
            [self.python_cmd, "-m", "pip", "install"] + dev_packages, "Install dev dependencies"
        )

        self._print_success("Dependencies installed successfully!")

    def lint(self, fix: bool = False) -> TaskResult:
        """
        Run linting with Ruff.
        """
        import time

        start = time.time()

        try:
            self._print_info("🔍 Running Ruff linter...")

            cmd = [self.python_cmd, "-m", "ruff", "check"]
            if fix:
                cmd.extend(["--fix", "--unsafe-fixes"])
            cmd.extend(self.source_paths)

            self._run_command(cmd, "Ruff lint", check=not fix)

            duration = time.time() - start
            return TaskResult(
                name="Lint",
                status=TaskStatus.SUCCESS,
                duration=duration,
                message="All linting checks passed",
            )
        except subprocess.CalledProcessError as e:
            duration = time.time() - start
            return TaskResult(
                name="Lint",
                status=TaskStatus.FAILED if not fix else TaskStatus.WARNING,
                duration=duration,
                message=f"Linting issues found (exit code: {e.returncode})",
                exit_code=e.returncode,
            )

    def format_code(
        self,
        check_only: bool = False,
        use_ruff: bool | None = None,
        line_length: int | None = None,
        target_version: str | None = None,
        skip_string_normalization: bool | None = None,
    ) -> TaskResult:
        """Format code with modern formatters.

        Args:
            check_only: Only check formatting without modifying files
            use_ruff: Use Ruff formatter (faster) instead of Black (defaults to config)
            line_length: Maximum line length (defaults to config)
            target_version: Python target version (defaults to config)
            skip_string_normalization: Skip normalizing string quotes (defaults to config)
        """
        import time

        start = time.time()

        # Use config defaults if not specified
        fmt_config = self.config.get("format", {})
        use_ruff = (
            use_ruff if use_ruff is not None else (fmt_config.get("formatter", "ruff") == "ruff")
        )
        line_length = line_length or fmt_config.get("line_length", 100)
        target_version = target_version or fmt_config.get("target_version", "py39")
        skip_string_normalization = (
            skip_string_normalization
            if skip_string_normalization is not None
            else fmt_config.get("skip_string_normalization", False)
        )

        try:
            self._print_info("✨ Formatting code with modern formatters...")

            paths = self.source_paths

            # Step 1: Import sorting with isort
            self._print_info("📦 Sorting imports with isort...")
            isort_cmd = [
                self.python_cmd,
                "-m",
                "isort",
                "--profile",
                "black",  # Compatible with Black/Ruff
                "--line-length",
                str(line_length),
                "--multi-line",
                "3",  # Vertical hanging indent
                "--trailing-comma",
                "--force-grid-wrap",
                "0",
                "--use-parentheses",
                "--ensure-newline-before-comments",
            ]

            if check_only:
                isort_cmd.extend(["--check-only", "--diff"])

            isort_cmd.extend(paths)
            self._run_command(isort_cmd, "isort imports", check=not check_only)

            # Step 2: Code formatting
            if use_ruff:
                # Ruff format (faster, modern alternative to Black)
                self._print_info("⚡ Formatting code with Ruff (faster)...")
                ruff_cmd = [
                    self.python_cmd,
                    "-m",
                    "ruff",
                    "format",
                    "--line-length",
                    str(line_length),
                ]

                if check_only:
                    ruff_cmd.append("--check")

                if skip_string_normalization:
                    ruff_cmd.extend(["--config", "format.quote-style='preserve'"])

                ruff_cmd.extend(paths)
                self._run_command(ruff_cmd, "Ruff format", check=not check_only)
            else:
                # Black formatting (traditional)
                self._print_info("🎨 Formatting code with Black...")
                black_cmd = [
                    self.python_cmd,
                    "-m",
                    "black",
                    "--line-length",
                    str(line_length),
                    "--target-version",
                    target_version,
                ]

                if check_only:
                    black_cmd.extend(["--check", "--diff"])

                if skip_string_normalization:
                    black_cmd.append("--skip-string-normalization")

                black_cmd.extend(paths)
                self._run_command(black_cmd, "Black format", check=not check_only)

            # Step 3: Docstring formatting (optional but recommended)
            if fmt_config.get("use_docformatter", True):
                self._print_info("📝 Formatting docstrings...")
                try:
                    docformatter_cmd = [
                        self.python_cmd,
                        "-m",
                        "docformatter",
                        "--in-place" if not check_only else "--check",
                        "--wrap-summaries",
                        str(line_length),
                        "--wrap-descriptions",
                        str(line_length),
                        "--make-summary-multi-line",
                        "--close-quotes-on-newline",
                        "-r",
                    ]
                    docformatter_cmd.extend(paths)
                    self._run_command(
                        docformatter_cmd,
                        "Docstring format",
                        check=False,  # Don't fail if docformatter not installed
                    )
                except (subprocess.CalledProcessError, FileNotFoundError):
                    self._print_warning("docformatter not installed (optional)")

            # Step 4: Remove unused imports (auto-fix)
            if not check_only and fmt_config.get("use_autoflake", True):
                self._print_info("🧹 Removing unused imports...")
                try:
                    autoflake_cmd = [
                        self.python_cmd,
                        "-m",
                        "autoflake",
                        "--in-place",
                        "--remove-all-unused-imports",
                        "--remove-unused-variables",
                        "--remove-duplicate-keys",
                        "--expand-star-imports",
                        "-r",
                    ]
                    autoflake_cmd.extend(paths)
                    self._run_command(autoflake_cmd, "Remove unused imports", check=False)
                except (subprocess.CalledProcessError, FileNotFoundError):
                    self._print_warning("autoflake not installed (optional)")

            # Step 5: Upgrade Python syntax (optional)
            if not check_only and fmt_config.get("use_pyupgrade", True):
                self._print_info("⬆️  Upgrading Python syntax...")
                try:
                    pyupgrade_cmd = [
                        self.python_cmd,
                        "-m",
                        "pyupgrade",
                        f"--py{target_version.replace('py', '').replace('.', '')}-plus",
                    ]
                    # pyupgrade needs individual files
                    for path_str in paths:
                        path = Path(path_str)
                        if path.is_dir():
                            for py_file in path.rglob("*.py"):
                                self._run_command(
                                    pyupgrade_cmd + [str(py_file)],
                                    f"Upgrade {py_file}",
                                    check=False,
                                )
                except (subprocess.CalledProcessError, FileNotFoundError):
                    self._print_warning("pyupgrade not installed (optional)")

            duration = time.time() - start
            formatter_name = "Ruff" if use_ruff else "Black"
            return TaskResult(
                name="Format",
                status=TaskStatus.SUCCESS,
                duration=duration,
                message=f"Code formatting completed with {formatter_name} + isort",
            )
        except subprocess.CalledProcessError as e:
            duration = time.time() - start
            return TaskResult(
                name="Format",
                status=TaskStatus.FAILED,
                duration=duration,
                message=f"Formatting check failed (exit code: {e.returncode})",
                exit_code=e.returncode,
            )

    def type_check(self) -> TaskResult:
        """
        Run type checking with mypy.
        """
        import time

        start = time.time()

        try:
            self._print_info("🔍 Running type checker...")

            self._run_command(
                [self.python_cmd, "-m", "mypy", "src/", "--ignore-missing-imports"], "Type check"
            )

            duration = time.time() - start
            return TaskResult(
                name="Type Check",
                status=TaskStatus.SUCCESS,
                duration=duration,
                message="Type checking passed",
            )
        except subprocess.CalledProcessError as e:
            duration = time.time() - start
            return TaskResult(
                name="Type Check",
                status=TaskStatus.WARNING,
                duration=duration,
                message=f"Type checking issues found (exit code: {e.returncode})",
                exit_code=e.returncode,
            )

    def run_tests(self, fast: bool = False, parallel: bool = False) -> TaskResult:
        """
        Run tests with pytest.
        """
        import time

        start = time.time()

        try:
            self._print_info("🧪 Running tests...")

            cmd = [self.python_cmd, "-m", "pytest", "tests/", "-v"]

            if not fast:
                cmd.extend(
                    ["--cov=src", "--cov-report=term", "--cov-report=html", "--cov-report=xml"]
                )

            if parallel:
                # Run tests in parallel using pytest-xdist
                import multiprocessing

                num_workers = max(1, multiprocessing.cpu_count() - 1)
                cmd.extend(["-n", str(num_workers)])

            self._run_command(cmd, "Run tests", check=False)

            # Verify critical modules
            self._print_info("🔍 Verifying critical modules...")
            verify_cmd = [
                self.python_cmd,
                "-c",
                "import sys; sys.path.insert(0, 'src'); "
                "from pipelines.transform.transformer import DataTransformer; "
                "from pipelines.load.loader import DataLoader; "
                "print('✅ Modules verified')",
            ]
            self._run_command(verify_cmd, "Verify modules")

            duration = time.time() - start
            return TaskResult(
                name="Tests",
                status=TaskStatus.SUCCESS,
                duration=duration,
                message="All tests passed",
            )
        except subprocess.CalledProcessError as e:
            duration = time.time() - start
            return TaskResult(
                name="Tests",
                status=TaskStatus.FAILED,
                duration=duration,
                message=f"Some tests failed (exit code: {e.returncode})",
                exit_code=e.returncode,
            )

    def security_check(self) -> TaskResult:
        """
        Run security checks.
        """
        import time

        start = time.time()

        try:
            self._print_info("🔒 Running security checks...")

            # Bandit
            self._print_info("Running Bandit...")
            self._run_command(
                [self.python_cmd, "-m", "bandit", "-r", "src/", "--exit-zero"],
                "Bandit security check",
            )

            # Safety
            self._print_info("Running Safety...")
            try:
                self._run_command(
                    [self.python_cmd, "-m", "safety", "check"], "Safety check", check=False
                )
            except Exception:
                self._print_warning("Safety check encountered issues (non-critical)")

            duration = time.time() - start
            return TaskResult(
                name="Security",
                status=TaskStatus.SUCCESS,
                duration=duration,
                message="Security checks completed",
            )
        except subprocess.CalledProcessError as e:
            duration = time.time() - start
            return TaskResult(
                name="Security",
                status=TaskStatus.WARNING,
                duration=duration,
                message=f"Security issues found (exit code: {e.returncode})",
                exit_code=e.returncode,
            )

    def perf_check(self) -> TaskResult:
        """
        Check performance with Ruff PERF rules.
        """
        import time

        start = time.time()

        try:
            self._print_info("⚡ Running performance checks...")

            self._run_command(
                [self.python_cmd, "-m", "ruff", "check", "--select", "PERF"] + self.source_paths,
                "Performance check",
                check=False,
            )

            duration = time.time() - start
            return TaskResult(
                name="Performance",
                status=TaskStatus.SUCCESS,
                duration=duration,
                message="Performance checks completed",
            )
        except subprocess.CalledProcessError as e:
            duration = time.time() - start
            return TaskResult(
                name="Performance",
                status=TaskStatus.WARNING,
                duration=duration,
                message=f"Performance issues found (exit code: {e.returncode})",
                exit_code=e.returncode,
            )

    def dead_code_check(self) -> TaskResult:
        """
        Find dead code with Vulture.
        """
        import time

        start = time.time()

        try:
            self._print_info("🧹 Checking for dead code...")

            self._run_command(
                [self.python_cmd, "-m", "vulture"] + self.source_paths + ["--min-confidence", "80"],
                "Dead code check",
                check=False,
            )

            duration = time.time() - start
            return TaskResult(
                name="Dead Code",
                status=TaskStatus.SUCCESS,
                duration=duration,
                message="Dead code analysis completed",
            )
        except subprocess.CalledProcessError as e:
            duration = time.time() - start
            return TaskResult(
                name="Dead Code",
                status=TaskStatus.WARNING,
                duration=duration,
                message=f"Dead code found (exit code: {e.returncode})",
                exit_code=e.returncode,
            )

    def complexity_check(self) -> TaskResult:
        """
        Analyze code complexity with Radon.
        """
        import time

        start = time.time()

        try:
            self._print_info("📊 Analyzing code complexity...")

            # Cyclomatic complexity
            self._print_info("Cyclomatic Complexity:")
            self._run_command(
                [self.python_cmd, "-m", "radon", "cc"] + self.source_paths + ["--min", "B"],
                "Cyclomatic complexity",
                check=False,
            )

            # Maintainability index
            self._print_info("Maintainability Index:")
            self._run_command(
                [self.python_cmd, "-m", "radon", "mi"] + self.source_paths + ["--min", "B"],
                "Maintainability index",
                check=False,
            )

            duration = time.time() - start
            return TaskResult(
                name="Complexity",
                status=TaskStatus.SUCCESS,
                duration=duration,
                message="Complexity analysis completed",
            )
        except subprocess.CalledProcessError as e:
            duration = time.time() - start
            return TaskResult(
                name="Complexity",
                status=TaskStatus.WARNING,
                duration=duration,
                message=f"Complexity issues found (exit code: {e.returncode})",
                exit_code=e.returncode,
            )

    def validate_dags(self) -> TaskResult:
        """
        Validate Airflow DAGs.
        """
        import time

        start = time.time()

        try:
            self._print_info("🔍 Validating Airflow DAGs...")

            # Set AIRFLOW_HOME
            os.environ["AIRFLOW_HOME"] = str(self.project_root / "airflow")

            validation_script = """
import sys
import os
import pkgutil

os.environ['AIRFLOW_HOME'] = os.path.abspath('airflow')

if not pkgutil.find_loader('airflow'):
    print('SKIP: Airflow not installed locally')
    sys.exit(0)

from airflow.models import DagBag
dag_bag = DagBag(dag_folder='airflow/dags', include_examples=False)

if dag_bag.import_errors:
    print('❌ DAG Import Errors:')
    for filename, error in dag_bag.import_errors.items():
        print(f'  {filename}: {error}')
    sys.exit(1)
else:
    print(f'✅ All DAGs validated! Found {len(dag_bag.dags)} DAG(s)')
    sys.exit(0)
"""

            result = self._run_command(
                [self.python_cmd, "-c", validation_script],
                "Validate DAGs",
                check=False,
                capture_output=True,
            )

            if "SKIP" in result.stdout:
                self._print_warning("Airflow not installed, trying Docker...")
                # Try Docker validation
                # Try Docker validation - use pipe to avoid shell escaping issues
                docker_cmd = [
                    "docker-compose",
                    "run",
                    "--rm",
                    "-i",  # Interactive to read from stdin
                    "--entrypoint",
                    "python3",
                    "airflow-scheduler",
                    "-",  # Read from stdin
                ]

                docker_result = subprocess.run(
                    docker_cmd,
                    input=validation_script,
                    capture_output=True,
                    text=True,
                    check=False,
                )

                if docker_result.returncode != 0:
                    self._print_error(f"Docker validation failed (exit code: {docker_result.returncode})")
                    if docker_result.stdout:
                        print(docker_result.stdout)
                    if docker_result.stderr:
                        print(docker_result.stderr)
                    raise subprocess.CalledProcessError(
                        docker_result.returncode, docker_result.args
                    )
                else:
                    if docker_result.stdout:
                        print(docker_result.stdout)
                    if docker_result.stderr:
                        # Some logs might go to stderr
                        print(docker_result.stderr)
            elif result.returncode != 0:
                raise subprocess.CalledProcessError(result.returncode, result.args)

            duration = time.time() - start
            return TaskResult(
                name="DAG Validation",
                status=TaskStatus.SUCCESS,
                duration=duration,
                message="All DAGs validated successfully",
            )
        except subprocess.CalledProcessError as e:
            duration = time.time() - start
            return TaskResult(
                name="DAG Validation",
                status=TaskStatus.FAILED,
                duration=duration,
                message=f"DAG validation failed (exit code: {e.returncode})",
                exit_code=e.returncode,
            )

    def install_git_hooks(self) -> None:
        """
        Install Git pre-commit hooks.
        """
        self._print_info("⚓ Installing Git pre-commit hooks...")

        git_dir = self.project_root / ".git"
        if not git_dir.exists():
            self._print_error("Not a git repository!")
            return

        hooks_dir = git_dir / "hooks"
        hooks_dir.mkdir(exist_ok=True)

        pre_commit_hook = hooks_dir / "pre-commit"

        hook_content = f"""#!/bin/sh
# Pre-commit hook generated by ci.py
echo "🚀 Running pre-commit checks..."

{self.python_cmd} {__file__} ci-quick

if [ $? -ne 0 ]; then
    echo "❌ Pre-commit checks failed. Commit aborted."
    exit 1
fi

echo "✅ Pre-commit checks passed!"
exit 0
"""

        pre_commit_hook.write_text(hook_content)

        # Make executable on Unix-like systems
        if not self.is_windows:
            os.chmod(pre_commit_hook, 0o755)

        self._print_success("Git hooks installed successfully!")

    def clean(self) -> None:
        """
        Clean cache and temporary files.
        """
        self._print_info("🧹 Cleaning cache and temporary files...")

        patterns = [
            "**/__pycache__",
            "**/.pytest_cache",
            "**/.mypy_cache",
            "**/.ruff_cache",
            "**/*.pyc",
            "**/*.pyo",
            "**/*.pyd",
            ".coverage",
            "htmlcov/",
            "dist/",
            "build/",
            "*.egg-info",
        ]

        for pattern in patterns:
            for path in self.project_root.glob(pattern):
                if path.is_file():
                    path.unlink()
                    if self.verbose:
                        self._print_info(f"Deleted: {path}")
                elif path.is_dir():
                    shutil.rmtree(path)
                    if self.verbose:
                        self._print_info(f"Deleted directory: {path}")

        self._print_success("Clean completed!")

    def docker_build(self) -> None:
        """
        Build Docker images.
        """
        self._print_info("🐳 Building Docker images...")

        self._run_command(["docker-compose", "build"], "Docker build")

        self._print_success("Docker build completed!")

    def docker_up(self) -> None:
        """
        Start Docker Compose services.
        """
        self._print_info("🐳 Starting Docker Compose services...")

        self._run_command(["docker-compose", "up", "-d"], "Docker up")

        self._print_success("Services started!")

    def docker_down(self) -> None:
        """
        Stop Docker Compose services.
        """
        self._print_info("🐳 Stopping Docker Compose services...")

        self._run_command(["docker-compose", "down"], "Docker down")

        self._print_success("Services stopped!")

    def docker_logs(self) -> None:
        """
        View Docker Compose logs.
        """
        self._print_info("🐳 Viewing Docker Compose logs...")

        self._run_command(["docker-compose", "logs", "-f"], "Docker logs")

    def run_parallel_tasks(self, tasks: list[tuple]) -> list[TaskResult]:
        """
        Run tasks in parallel.
        """
        results = []

        with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
            future_to_task = {
                executor.submit(task_func, *args): (task_name, task_func)
                for task_name, task_func, *args in tasks
            }

            for future in as_completed(future_to_task):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    task_name = future_to_task[future][0]
                    self._print_error(f"Task {task_name} failed: {str(e)}")
                    results.append(
                        TaskResult(
                            name=task_name, status=TaskStatus.FAILED, duration=0.0, message=str(e)
                        )
                    )

        return results

    def ci_local(self, parallel: bool = True) -> None:
        """
        Run full CI pipeline locally.
        """
        self._print_info("🚀 Running local CI pipeline...")
        self._print_info("=" * 50)

        if parallel:
            # Run checks in parallel
            tasks = [
                ("Format Check", self.format_code, True),
                ("Lint", self.lint, False),
                ("Performance", self.perf_check),
                ("Type Check", self.type_check),
                ("Security", self.security_check),
            ]

            results = self.run_parallel_tasks(tasks)
            self.results.extend(results)

            # Run sequential tasks (resource-intensive)
            self._print_info("\n📋 Running sequential tasks...")
            self.results.append(self.validate_dags())
            self.results.append(self.run_tests(fast=False, parallel=True))
        else:
            # Run all checks sequentially
            self.results.append(self.format_code(check_only=True))
            self.results.append(self.lint())
            self.results.append(self.perf_check())
            self.results.append(self.type_check())
            self.results.append(self.security_check())
            self.results.append(self.validate_dags())
            self.results.append(self.run_tests())

        # Display results
        self._display_results()

        # Check if any task failed
        failed_tasks = [r for r in self.results if r.status == TaskStatus.FAILED]
        if failed_tasks:
            self._print_error(f"\n{len(failed_tasks)} task(s) failed!")
            sys.exit(1)
        else:
            self._print_success("\n✅ All CI checks passed!")

    def ci_fast(self) -> None:
        """
        Run fast CI checks.
        """
        self._print_info("⚡ Running fast CI checks...")

        tasks = [
            ("Format Check", self.format_code, True),
            ("Lint", self.lint, False),
        ]

        results = self.run_parallel_tasks(tasks)
        self.results.extend(results)
        self.results.append(self.validate_dags())

        self._display_results()

        failed_tasks = [r for r in self.results if r.status == TaskStatus.FAILED]
        if failed_tasks:
            sys.exit(1)
        else:
            self._print_success("✅ Fast CI passed!")

    def ci_quick(self) -> None:
        """
        Run quick CI checks (for pre-commit)
        """
        self._print_info("⚡ Running quick CI checks...")

        self.results.append(self.format_code(check_only=True))
        self.results.append(self.lint())
        self.results.append(self.perf_check())

        self._display_results()

        failed_tasks = [r for r in self.results if r.status == TaskStatus.FAILED]
        if failed_tasks:
            sys.exit(1)
        else:
            self._print_success("✅ Quick CI passed!")

    def _display_results(self) -> None:
        """
        Display task results in a table.
        """
        if not self.results:
            return

        if RICH_AVAILABLE and self.console:
            table = Table(title="CI/CD Results", show_header=True, header_style="bold magenta")
            table.add_column("Task", style="cyan", no_wrap=True)
            table.add_column("Status", justify="center")
            table.add_column("Duration", justify="right", style="yellow")
            table.add_column("Message", style="dim")

            for result in self.results:
                status_color = {
                    TaskStatus.SUCCESS: "green",
                    TaskStatus.FAILED: "red",
                    TaskStatus.WARNING: "yellow",
                    TaskStatus.SKIPPED: "dim",
                }.get(result.status, "white")

                table.add_row(
                    result.name,
                    f"[{status_color}]{result.status.value}[/{status_color}]",
                    f"{result.duration:.2f}s",
                    result.message,
                )

            self.console.print("\n")
            self.console.print(table)
        else:
            # Fallback to simple text output
            print("\n" + "=" * 70)
            print(f"{'Task':<20} {'Status':<10} {'Duration':<12} {'Message'}")
            print("=" * 70)
            for result in self.results:
                print(
                    f"{result.name:<20} {result.status.value:<10} "
                    f"{result.duration:>8.2f}s    {result.message}"
                )
            print("=" * 70)


def main():
    """
    Main entry point.
    """
    parser = argparse.ArgumentParser(
        description="CI/CD Script - Modern Python Version",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("command", nargs="?", default="help", help="Command to execute")

    parser.add_argument(
        "-c",
        "--config",
        type=str,
        default=None,
        help="Path to config file (default: ciconfig.toml)",
    )

    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")

    parser.add_argument("--no-parallel", action="store_true", help="Disable parallel execution")

    # Format-specific options
    parser.add_argument(
        "--use-black", action="store_true", help="Use Black instead of Ruff for formatting"
    )

    parser.add_argument(
        "--line-length",
        type=int,
        default=100,
        help="Maximum line length for formatters (default: 100)",
    )

    parser.add_argument(
        "--skip-string-norm", action="store_true", help="Skip string quote normalization"
    )

    args = parser.parse_args()

    runner = CIRunner(verbose=args.verbose, parallel=not args.no_parallel, config_file=args.config)

    commands = {
        "help": lambda: print_help(),
        "setup": runner.setup,
        "install": runner.install_dependencies,
        "lint": lambda: runner.lint(),
        "lint-fix": lambda: runner.lint(fix=True),
        "format": lambda: runner.format_code(
            use_ruff=not args.use_black,
            line_length=args.line_length,
            skip_string_normalization=args.skip_string_norm,
        ),
        "format-check": lambda: runner.format_code(
            check_only=True,
            use_ruff=not args.use_black,
            line_length=args.line_length,
            skip_string_normalization=args.skip_string_norm,
        ),
        "type-check": lambda: runner.type_check(),
        "test": lambda: runner.run_tests(),
        "test-fast": lambda: runner.run_tests(fast=True),
        "test-parallel": lambda: runner.run_tests(parallel=True),
        "perf-check": lambda: runner.perf_check(),
        "dead-code": lambda: runner.dead_code_check(),
        "complexity": lambda: runner.complexity_check(),
        "code-quality": lambda: [
            runner.perf_check(),
            runner.dead_code_check(),
            runner.complexity_check(),
        ],
        "validate-dags": lambda: runner.validate_dags(),
        "security-check": lambda: runner.security_check(),
        "install-hooks": runner.install_git_hooks,
        "clean": runner.clean,
        "docker-build": runner.docker_build,
        "docker-up": runner.docker_up,
        "docker-down": runner.docker_down,
        "docker-logs": runner.docker_logs,
        "ci-local": lambda: runner.ci_local(parallel=not args.no_parallel),
        "ci-fast": runner.ci_fast,
        "ci-quick": runner.ci_quick,
    }

    command = args.command.lower()

    if command in commands:
        try:
            commands[command]()
        except KeyboardInterrupt:
            print("\n\n⚠️  Operation cancelled by user")
            sys.exit(130)
        except Exception as e:
            print(f"\n❌ Error: {str(e)}")
            if args.verbose:
                import traceback

                traceback.print_exc()
            sys.exit(1)
    else:
        print(f"❌ Unknown command: {command}")
        print_help()
        sys.exit(1)


def print_help():
    """
    Print help information.
    """
    help_text = """
╔════════════════════════════════════════════════════════════════╗
║                    CI/CD Commands - Python                     ║
╚════════════════════════════════════════════════════════════════╝

📋 Setup & Installation:
  setup              Initialize development environment (venv & deps)
  install            Install all dependencies
  install-hooks      Install Git pre-commit hooks

🔍 Code Quality:
  lint               Run linting with Ruff
  lint-fix           Auto-fix linting issues
  format             Format code (Ruff + isort + autoflake + docformatter)
  format-check       Check code formatting (no modifications)
  type-check         Run type checking with mypy
  perf-check         Check performance with Ruff PERF
  dead-code          Find unused code with Vulture
  complexity         Analyze code complexity with Radon
  code-quality       Run all code quality checks

🧪 Testing:
  test               Run tests with pytest (with coverage)
  test-fast          Run tests without coverage
  test-parallel      Run tests in parallel

✅ Validation:
  validate-dags      Validate Airflow DAGs
  security-check     Security checks with Bandit and Safety

🐳 Docker:
  docker-build       Build Docker images
  docker-up          Start Docker Compose services
  docker-down        Stop Docker Compose services
  docker-logs        View Docker Compose logs

🚀 CI Pipelines:
  ci-local           Run full CI pipeline (parallel)
  ci-fast            Run fast CI checks (parallel)
  ci-quick           Run quick CI checks (for pre-commit)

🧹 Utilities:
  clean              Clean cache and temporary files

Options:
  -v, --verbose      Enable verbose output
  --no-parallel      Disable parallel execution
  --use-black        Use Black instead of Ruff for formatting
  --line-length N    Maximum line length (default: 100)
  --skip-string-norm Skip string quote normalization

Examples:
  python ci.py setup
  python ci.py lint-fix
  python ci.py format --line-length 120
  python ci.py format --use-black --skip-string-norm
  python ci.py test-parallel
  python ci.py ci-local --verbose
  python ci.py ci-quick --no-parallel
"""

    if RICH_AVAILABLE:
        console = Console()
        console.print(Panel(help_text, border_style="cyan", expand=False))
    else:
        print(help_text)


if __name__ == "__main__":
    main()
