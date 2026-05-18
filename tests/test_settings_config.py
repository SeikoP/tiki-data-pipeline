import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from common.config_validator import has_errors, validate_registry, validate_settings  # noqa: E402
from common.settings import (  # noqa: E402
    SETTINGS_REGISTRY,
    DatabaseSettings,
    get_bool_setting,
    get_int_setting,
    get_pipeline_settings,
    get_setting,
)


def test_settings_env_precedence(monkeypatch):
    monkeypatch.setenv("TIKI_DETAIL_ACTIVE_BATCHES", "9")
    monkeypatch.setenv("DISCORD_ENABLED", "true")

    assert get_int_setting("TIKI_DETAIL_ACTIVE_BATCHES", use_airflow=False) == 9
    assert get_bool_setting("DISCORD_ENABLED", use_airflow=False) is True


def test_settings_legacy_ai_key_precedence(monkeypatch):
    monkeypatch.delenv("AI_API_KEY", raising=False)
    monkeypatch.setenv("GROQ_API_KEY", "legacy-key")

    settings = get_pipeline_settings(use_airflow=False)

    assert settings.ai.api_key == "legacy-key"


def test_stable_local_resource_profile_defaults(monkeypatch):
    for key in (
        "TIKI_CATEGORY_NUM_BATCHES",
        "TIKI_CATEGORY_CONCURRENCY",
        "TIKI_DETAIL_ACTIVE_BATCHES",
        "TIKI_DETAIL_BATCH_SIZE",
        "TIKI_DETAIL_MAX_CONCURRENT_TASKS",
        "TIKI_DETAIL_CONCURRENCY_MIN",
        "TIKI_DETAIL_CONCURRENCY_MAX",
        "TIKI_SAVE_BATCH_SIZE",
    ):
        monkeypatch.delenv(key, raising=False)

    settings = get_pipeline_settings(use_airflow=False)

    assert settings.airflow.category_num_batches == 6
    assert settings.airflow.max_active_tasks == 3
    assert settings.crawl.category_concurrency == 1
    assert settings.detail.active_batches == 3
    assert settings.detail.batch_size == 300
    assert settings.detail.max_concurrent_tasks == 4
    assert settings.detail.concurrency_min == 2
    assert settings.detail.concurrency_max == 6
    assert settings.load.save_batch_size == 1000


def test_secret_settings_have_empty_defaults():
    secret_defaults = {key: spec.default for key, spec in SETTINGS_REGISTRY.items() if spec.secret}

    assert secret_defaults
    assert all(value == "" for value in secret_defaults.values())
    assert not has_errors(validate_registry())


def test_validator_catches_detail_batches_above_pool_slots():
    settings = get_pipeline_settings(use_airflow=False)
    bad_settings = settings.__class__(
        database=DatabaseSettings(
            host=settings.database.host,
            port=settings.database.port,
            database=settings.database.database,
            user=settings.database.user,
            password=settings.database.password,
        ),
        redis=settings.redis,
        airflow=settings.airflow,
        crawl=settings.crawl,
        detail=settings.detail.__class__(
            products_per_day=settings.detail.products_per_day,
            active_batches=999,
            batch_size=settings.detail.batch_size,
            max_concurrent_tasks=settings.detail.max_concurrent_tasks,
            concurrency_min=settings.detail.concurrency_min,
            concurrency_max=settings.detail.concurrency_max,
            rate_limit_delay=settings.detail.rate_limit_delay,
            crawl_timeout=settings.detail.crawl_timeout,
            pool_size=settings.detail.pool_size,
            batch_output_dir=settings.detail.batch_output_dir,
            cache_relax_days=settings.detail.cache_relax_days,
        ),
        load=settings.load,
        ai=settings.ai,
        notifications=settings.notifications,
        short_name_use_ai=settings.short_name_use_ai,
        optimization=settings.optimization,
    )

    issues = validate_settings(bad_settings)

    assert has_errors(issues)
    assert any("TIKI_DETAIL_ACTIVE_BATCHES" in issue.message for issue in issues)


@pytest.mark.parametrize("key", ["POSTGRES_DB", "TIKI_DETAIL_BATCH_SIZE", "AI_MODEL"])
def test_registry_values_are_available(key):
    assert get_setting(key, use_airflow=False) is not None


def test_config_sync_check_passes():
    result = subprocess.run(
        [sys.executable, "scripts/config/sync_config_docs.py", "--check"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
