from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from common.config_validator import has_errors, validate_registry, validate_settings  # noqa: E402
from common.settings import (  # noqa: E402
    SETTINGS_REGISTRY,
    airflow_variable_defaults,
    get_pipeline_settings,
)

CORE_ENV_EXAMPLE_KEYS = {
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_DB",
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE",
    "REDIS_PASSWORD",
    "TIKI_PRODUCTS_PER_DAY",
    "TIKI_DETAIL_ACTIVE_BATCHES",
    "TIKI_DETAIL_BATCH_SIZE",
    "TIKI_DETAIL_MAX_CONCURRENT_TASKS",
    "TIKI_DETAIL_CONCURRENCY_MIN",
    "TIKI_DETAIL_CONCURRENCY_MAX",
    "TIKI_LOAD_MODE",
    "TIKI_LOAD_OPTIMIZED_THRESHOLD",
    "AI_ENABLED",
    "AI_API_KEY",
    "AI_API_BASE",
    "AI_MODEL",
    "GROQ_ENABLED",
    "GROQ_API_KEY",
    "DISCORD_ENABLED",
    "DISCORD_WEBHOOK_URL",
    "DISCORD_MONITORING_WEBHOOK_URL",
}

DOC_REQUIRED_KEYS = {
    "TIKI_CATEGORY_NUM_BATCHES",
    "TIKI_PRODUCTS_PER_DAY",
    "TIKI_DETAIL_ACTIVE_BATCHES",
    "TIKI_DETAIL_BATCH_SIZE",
    "TIKI_DETAIL_MAX_CONCURRENT_TASKS",
    "TIKI_DETAIL_CONCURRENCY_MIN",
    "TIKI_DETAIL_CONCURRENCY_MAX",
    "TIKI_LOAD_MODE",
    "DISCORD_ENABLED",
}

FORBIDDEN_DEFAULT_PATTERNS = [
    re.compile(r'os\.getenv\("POSTGRES_PASSWORD",\s*"[^"]+"\)'),
    re.compile(r'os\.getenv\("REDIS_PASSWORD",\s*"[^"]+"\)'),
    re.compile(r'get_variable\("POSTGRES_PASSWORD",\s*default=[^)]+\)'),
]

HARDCODE_SCAN_PATHS = [
    ROOT / "airflow" / "dags" / "tiki_crawl_products_v2",
    ROOT / "airflow" / "setup",
    ROOT / "src",
]


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _extract_env_keys(path: Path) -> set[str]:
    keys = set()
    for line in _read(path).splitlines():
        match = re.match(r"^([A-Z_][A-Z0-9_]*)=", line.strip())
        if match:
            keys.add(match.group(1))
    return keys


def _check_env_example() -> list[str]:
    env_keys = _extract_env_keys(ROOT / ".env.example")
    return sorted(CORE_ENV_EXAMPLE_KEYS - env_keys)


def _check_docs() -> list[str]:
    docs = _read(ROOT / "docs" / "01-PARAMETERS" / "README.md")
    return sorted(key for key in DOC_REQUIRED_KEYS if f"`{key}`" not in docs)


def _check_setup_airflow() -> list[str]:
    setup = _read(ROOT / "airflow" / "setup" / "setup_airflow.py")
    missing = []
    for key in airflow_variable_defaults():
        if key not in setup and key not in SETTINGS_REGISTRY:
            missing.append(key)
    return sorted(missing)


def _check_forbidden_defaults() -> list[str]:
    findings: list[str] = []
    for root in HARDCODE_SCAN_PATHS:
        for path in root.rglob("*.py"):
            if "__pycache__" in path.parts:
                continue
            text = _read(path)
            for pattern in FORBIDDEN_DEFAULT_PATTERNS:
                for match in pattern.finditer(text):
                    rel = path.relative_to(ROOT)
                    findings.append(f"{rel}: forbidden credential fallback `{match.group(0)}`")
    return findings


def check() -> int:
    errors: list[str] = []

    registry_issues = validate_registry()
    runtime_issues = validate_settings(get_pipeline_settings(use_airflow=False))
    errors.extend(issue.message for issue in registry_issues if issue.severity == "error")
    if has_errors(runtime_issues):
        errors.extend(issue.message for issue in runtime_issues if issue.severity == "error")

    missing_env = _check_env_example()
    if missing_env:
        errors.append(".env.example missing: " + ", ".join(missing_env))

    missing_docs = _check_docs()
    if missing_docs:
        errors.append("docs/01-PARAMETERS missing: " + ", ".join(missing_docs))

    missing_setup = _check_setup_airflow()
    if missing_setup:
        errors.append("airflow setup missing defaults: " + ", ".join(missing_setup))

    errors.extend(_check_forbidden_defaults())

    if errors:
        print("Config check failed:")
        for error in errors:
            print(f" - {error}")
        return 1

    print("Config check passed")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate config registry/docs consistency")
    parser.add_argument("--check", action="store_true", help="Validate without writing files")
    args = parser.parse_args()
    if not args.check:
        parser.error("Only --check is currently supported")
    return check()


if __name__ == "__main__":
    raise SystemExit(main())
