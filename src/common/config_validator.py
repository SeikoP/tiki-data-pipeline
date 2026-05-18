from __future__ import annotations

from dataclasses import dataclass

from common.settings import (
    POOL_DEFAULTS,
    SETTINGS_REGISTRY,
    PipelineSettings,
    airflow_variable_defaults,
)


@dataclass(frozen=True)
class ConfigIssue:
    severity: str
    message: str


def validate_settings(settings: PipelineSettings) -> list[ConfigIssue]:
    issues: list[ConfigIssue] = []

    if settings.detail.concurrency_min > settings.detail.concurrency_max:
        issues.append(
            ConfigIssue(
                "error",
                "TIKI_DETAIL_CONCURRENCY_MIN must be <= TIKI_DETAIL_CONCURRENCY_MAX",
            )
        )

    fast_pool_slots = int(POOL_DEFAULTS["fast_pool"]["slots"])
    if settings.detail.active_batches > fast_pool_slots:
        issues.append(
            ConfigIssue(
                "error",
                "TIKI_DETAIL_ACTIVE_BATCHES must not exceed fast_pool slots",
            )
        )

    if settings.detail.active_batches > settings.airflow.max_active_tasks:
        issues.append(
            ConfigIssue(
                "error",
                "TIKI_DETAIL_ACTIVE_BATCHES must not exceed DAG max_active_tasks",
            )
        )

    if settings.notifications.discord_enabled and not (
        settings.notifications.webhook_url
        or settings.notifications.webhook_url_notification
        or settings.notifications.webhook_url_alert
    ):
        issues.append(ConfigIssue("warning", "Discord enabled but no webhook URL is configured"))

    if settings.ai.enabled and not settings.ai.api_key:
        issues.append(
            ConfigIssue("warning", "AI enabled but no AI_API_KEY/GROQ_API_KEY is configured")
        )

    return issues


def validate_registry() -> list[ConfigIssue]:
    issues: list[ConfigIssue] = []

    for key, spec in SETTINGS_REGISTRY.items():
        if spec.secret and spec.default:
            issues.append(ConfigIssue("error", f"Secret setting {key} must not have a default"))

    variable_defaults = airflow_variable_defaults()
    for key in variable_defaults:
        if key not in SETTINGS_REGISTRY:
            issues.append(ConfigIssue("error", f"Airflow default {key} is missing from registry"))

    return issues


def has_errors(issues: list[ConfigIssue]) -> bool:
    return any(issue.severity == "error" for issue in issues)


__all__ = ["ConfigIssue", "has_errors", "validate_registry", "validate_settings"]
