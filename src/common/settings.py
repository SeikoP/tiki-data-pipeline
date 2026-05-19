from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

SettingType = Literal["str", "int", "float", "bool"]


@dataclass(frozen=True)
class SettingSpec:
    env_name: str
    default: Any
    value_type: SettingType = "str"
    description: str = ""
    secret: bool = False
    airflow_variable: bool = False
    env_example: bool = True

    def parse(self, raw_value: Any) -> Any:
        if raw_value is None:
            return self.default
        if self.value_type == "bool":
            return str(raw_value).strip().lower() in {"true", "1", "yes", "y", "on"}
        if self.value_type == "int":
            return int(raw_value)
        if self.value_type == "float":
            return float(raw_value)
        return str(raw_value)


def _get_airflow_variable(key: str) -> str | None:
    try:
        try:
            from airflow.sdk import Variable
        except ImportError:
            from airflow.models import Variable  # type: ignore[no-redef]

        try:
            return Variable.get(key, default=None)
        except TypeError:
            return Variable.get(key, default_var=None)
    except Exception:
        return None


def _get_value(spec: SettingSpec, *, use_airflow: bool = True) -> Any:
    env_value = os.getenv(spec.env_name)
    if env_value is not None:
        return spec.parse(env_value)
    if use_airflow and spec.airflow_variable:
        airflow_value = _get_airflow_variable(spec.env_name)
        if airflow_value is not None:
            return spec.parse(airflow_value)
    return spec.default


SETTINGS_REGISTRY: dict[str, SettingSpec] = {
    # Database
    "POSTGRES_HOST": SettingSpec(
        "POSTGRES_HOST", "postgres", description="PostgreSQL host", airflow_variable=True
    ),
    "POSTGRES_PORT": SettingSpec(
        "POSTGRES_PORT", 5432, "int", description="PostgreSQL port", airflow_variable=True
    ),
    "POSTGRES_DB": SettingSpec(
        "POSTGRES_DB", "tiki", description="Crawl database name", airflow_variable=True
    ),
    "POSTGRES_USER": SettingSpec(
        "POSTGRES_USER", "airflow_user", description="PostgreSQL user", airflow_variable=True
    ),
    "POSTGRES_PASSWORD": SettingSpec(
        "POSTGRES_PASSWORD", "", description="PostgreSQL password", secret=True
    ),
    # Redis
    "REDIS_URL": SettingSpec(
        "REDIS_URL", "redis://redis:6379/1", description="Redis cache URL", airflow_variable=True
    ),
    "REDIS_PASSWORD": SettingSpec("REDIS_PASSWORD", "", description="Redis password", secret=True),
    # Airflow runtime
    "TIKI_DAG_SCHEDULE_MODE": SettingSpec(
        "TIKI_DAG_SCHEDULE_MODE",
        "manual",
        description="DAG schedule mode: manual or scheduled",
        airflow_variable=True,
    ),
    "TIKI_DAG_SCHEDULE_HOURS": SettingSpec(
        "TIKI_DAG_SCHEDULE_HOURS",
        1,
        "int",
        description="Scheduled mode interval hours",
        airflow_variable=True,
    ),
    "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE": SettingSpec(
        "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE",
        "/opt/airflow/external/passwords.json",
        description="Airflow simple auth password file path inside the container",
    ),
    "TIKI_CATEGORY_NUM_BATCHES": SettingSpec(
        "TIKI_CATEGORY_NUM_BATCHES",
        6,
        "int",
        description="Mapped category batch count",
        airflow_variable=True,
    ),
    "TIKI_CATEGORY_CONCURRENCY": SettingSpec(
        "TIKI_CATEGORY_CONCURRENCY",
        1,
        "int",
        description="Concurrent category requests inside a category batch",
        airflow_variable=True,
    ),
    "TIKI_CATEGORY_MAX_ACTIVE_TASKS": SettingSpec(
        "TIKI_CATEGORY_MAX_ACTIVE_TASKS",
        2,
        "int",
        description="Maximum mapped category batch tasks allowed to run at once",
        airflow_variable=True,
    ),
    "TIKI_CATEGORY_BATCH_OUTPUT_DIR": SettingSpec(
        "TIKI_CATEGORY_BATCH_OUTPUT_DIR",
        "/opt/airflow/data/raw/category_batches",
        description="Directory for file-backed category mapped results",
    ),
    "TIKI_MIN_CATEGORY_LEVEL": SettingSpec(
        "TIKI_MIN_CATEGORY_LEVEL",
        2,
        "int",
        description="Minimum category level to crawl",
        airflow_variable=True,
    ),
    "TIKI_MAX_CATEGORY_LEVEL": SettingSpec(
        "TIKI_MAX_CATEGORY_LEVEL",
        4,
        "int",
        description="Maximum category level to crawl",
        airflow_variable=True,
    ),
    "TIKI_MAX_CATEGORY_LEVELS": SettingSpec(
        "TIKI_MAX_CATEGORY_LEVELS", 5, "int", description="Maximum known Tiki category depth"
    ),
    "TIKI_MAX_CATEGORIES": SettingSpec(
        "TIKI_MAX_CATEGORIES",
        0,
        "int",
        description="Optional max categories cap",
        airflow_variable=True,
    ),
    "TIKI_CRAWL_ONLY_LEAF": SettingSpec(
        "TIKI_CRAWL_ONLY_LEAF",
        True,
        "bool",
        description="Only crawl leaf categories",
        airflow_variable=True,
    ),
    "TIKI_CATEGORY_SELECTION_MODE": SettingSpec(
        "TIKI_CATEGORY_SELECTION_MODE",
        "window",
        description="Category selection mode: window or segment_quota",
        airflow_variable=True,
    ),
    "TIKI_CATEGORY_SEGMENT_QUOTAS": SettingSpec(
        "TIKI_CATEGORY_SEGMENT_QUOTAS",
        "kitchen_cooking:30,cleaning_homecare:30,storage_organization:25,bathroom:20,laundry_garment:20,home_comfort:30,decor_living:25",
        description="Comma-separated category quotas per analytics home segment",
        airflow_variable=True,
    ),
    "TIKI_MAX_PAGES_PER_CATEGORY": SettingSpec(
        "TIKI_MAX_PAGES_PER_CATEGORY",
        20,
        "int",
        description="Max listing pages per category",
        airflow_variable=True,
    ),
    "TIKI_CRAWL_TIMEOUT": SettingSpec(
        "TIKI_CRAWL_TIMEOUT", 300, "int", description="Category crawl timeout seconds"
    ),
    "TIKI_CACHE_TTL": SettingSpec(
        "TIKI_CACHE_TTL", 86400, "int", description="Category/listing cache TTL seconds"
    ),
    "TIKI_USE_SELENIUM": SettingSpec(
        "TIKI_USE_SELENIUM", False, "bool", description="Use Selenium for category crawl"
    ),
    "TIKI_FORCE_REFRESH": SettingSpec(
        "TIKI_FORCE_REFRESH", False, "bool", description="Force category/listing refresh"
    ),
    # Detail crawl
    "TIKI_PRODUCTS_PER_DAY": SettingSpec(
        "TIKI_PRODUCTS_PER_DAY",
        1000,
        "int",
        description="Products selected for detail per run",
        airflow_variable=True,
    ),
    "TIKI_MAX_PRODUCTS_FOR_DETAIL": SettingSpec(
        "TIKI_MAX_PRODUCTS_FOR_DETAIL",
        0,
        "int",
        description="Hard cap for detail products",
        airflow_variable=True,
    ),
    "TIKI_DETAIL_ACTIVE_BATCHES": SettingSpec(
        "TIKI_DETAIL_ACTIVE_BATCHES",
        3,
        "int",
        description="Mapped detail batches intended to run in one Airflow wave",
        airflow_variable=True,
    ),
    "TIKI_DETAIL_BATCH_SIZE": SettingSpec(
        "TIKI_DETAIL_BATCH_SIZE",
        300,
        "int",
        description="Soft target detail batch size",
        airflow_variable=True,
    ),
    "TIKI_DETAIL_MAX_CONCURRENT_TASKS": SettingSpec(
        "TIKI_DETAIL_MAX_CONCURRENT_TASKS",
        4,
        "int",
        description="Async detail requests inside one mapped task",
        airflow_variable=True,
    ),
    "TIKI_DETAIL_CONCURRENCY_MIN": SettingSpec(
        "TIKI_DETAIL_CONCURRENCY_MIN",
        2,
        "int",
        description="Minimum detail async concurrency",
        airflow_variable=True,
    ),
    "TIKI_DETAIL_CONCURRENCY_MAX": SettingSpec(
        "TIKI_DETAIL_CONCURRENCY_MAX",
        6,
        "int",
        description="Maximum detail async concurrency",
        airflow_variable=True,
    ),
    "TIKI_DETAIL_RATE_LIMIT_DELAY": SettingSpec(
        "TIKI_DETAIL_RATE_LIMIT_DELAY", 0.05, "float", description="Detail request stagger delay"
    ),
    "TIKI_DETAIL_CRAWL_TIMEOUT": SettingSpec(
        "TIKI_DETAIL_CRAWL_TIMEOUT", 180, "int", description="Detail crawl timeout seconds"
    ),
    "TIKI_DETAIL_POOL_SIZE": SettingSpec(
        "TIKI_DETAIL_POOL_SIZE", 2, "int", description="Selenium detail driver pool size"
    ),
    "TIKI_DETAIL_BATCH_OUTPUT_DIR": SettingSpec(
        "TIKI_DETAIL_BATCH_OUTPUT_DIR",
        "/opt/airflow/data/raw/detail_batches",
        description="Directory for file-backed detail mapped results",
    ),
    "TIKI_CACHE_RELAX_DAYS": SettingSpec(
        "TIKI_CACHE_RELAX_DAYS",
        2,
        "int",
        description="Recent detail data freshness window",
        airflow_variable=True,
    ),
    "TIKI_CHECK_RECENCY_ONLY": SettingSpec(
        "TIKI_CHECK_RECENCY_ONLY", False, "bool", description="Only check updated_at recency"
    ),
    "TIKI_FORCE_CRAWL": SettingSpec(
        "TIKI_FORCE_CRAWL",
        False,
        "bool",
        description="Force detail recrawl",
        airflow_variable=True,
    ),
    "TIKI_FORCE_REFRESH_CACHE": SettingSpec(
        "TIKI_FORCE_REFRESH_CACHE", False, "bool", description="Bypass detail file cache"
    ),
    "TIKI_AUDIT_HTTP_TIMEOUT": SettingSpec(
        "TIKI_AUDIT_HTTP_TIMEOUT", 20, "int", description="Live Tiki audit HTTP timeout seconds"
    ),
    "TIKI_AUDIT_REQUEST_DELAY": SettingSpec(
        "TIKI_AUDIT_REQUEST_DELAY", 0.1, "float", description="Delay between live audit requests"
    ),
    "TIKI_AUDIT_MAX_PRODUCTS": SettingSpec(
        "TIKI_AUDIT_MAX_PRODUCTS", 0, "int", description="Optional live audit product cap"
    ),
    # Load
    "TIKI_SAVE_BATCH_SIZE": SettingSpec(
        "TIKI_SAVE_BATCH_SIZE",
        1000,
        "int",
        description="DB save batch size",
        airflow_variable=True,
    ),
    "TIKI_LOAD_MODE": SettingSpec(
        "TIKI_LOAD_MODE",
        "auto",
        description="Product load strategy: auto, safe, optimized",
        airflow_variable=True,
    ),
    "TIKI_LOAD_OPTIMIZED_THRESHOLD": SettingSpec(
        "TIKI_LOAD_OPTIMIZED_THRESHOLD",
        1000,
        "int",
        description="Row threshold for optimized load mode",
        airflow_variable=True,
    ),
    "TIKI_ALLOW_FILE_ONLY_ON_DB_FAILURE": SettingSpec(
        "TIKI_ALLOW_FILE_ONLY_ON_DB_FAILURE",
        False,
        "bool",
        description="Allow final file output when DB load fails",
    ),
    # Analytics
    "TIKI_ANALYTICS_SCHEMA": SettingSpec(
        "TIKI_ANALYTICS_SCHEMA",
        "analytics",
        description="PostgreSQL schema for analysis-ready mart tables",
        airflow_variable=True,
    ),
    # Resilience
    "TIKI_CIRCUIT_BREAKER_FAILURE_THRESHOLD": SettingSpec(
        "TIKI_CIRCUIT_BREAKER_FAILURE_THRESHOLD",
        5,
        "int",
        description="Circuit breaker failures",
        airflow_variable=True,
    ),
    "TIKI_CIRCUIT_BREAKER_RECOVERY_TIMEOUT": SettingSpec(
        "TIKI_CIRCUIT_BREAKER_RECOVERY_TIMEOUT",
        60,
        "int",
        description="Circuit breaker recovery timeout seconds",
        airflow_variable=True,
    ),
    "TIKI_DEGRADATION_FAILURE_THRESHOLD": SettingSpec(
        "TIKI_DEGRADATION_FAILURE_THRESHOLD",
        3,
        "int",
        description="Degradation failure threshold",
        airflow_variable=True,
    ),
    "TIKI_DEGRADATION_RECOVERY_THRESHOLD": SettingSpec(
        "TIKI_DEGRADATION_RECOVERY_THRESHOLD",
        5,
        "int",
        description="Degradation recovery threshold",
        airflow_variable=True,
    ),
    # AI / Discord
    "AI_ENABLED": SettingSpec("AI_ENABLED", False, "bool", description="Enable AI summaries"),
    "AI_API_KEY": SettingSpec("AI_API_KEY", "", description="AI API key", secret=True),
    "AI_API_BASE": SettingSpec(
        "AI_API_BASE", "https://api.groq.com/openai/v1", description="OpenAI-compatible API base"
    ),
    "AI_MODEL": SettingSpec("AI_MODEL", "llama-3.3-70b-versatile", description="AI summary model"),
    "GROQ_ENABLED": SettingSpec("GROQ_ENABLED", False, "bool", description="Legacy Groq enable"),
    "GROQ_API_KEY": SettingSpec("GROQ_API_KEY", "", description="Legacy Groq API key", secret=True),
    "GROQ_API_BASE": SettingSpec(
        "GROQ_API_BASE",
        "https://api.groq.com/openai/v1",
        description="Legacy Groq API base",
        env_example=False,
    ),
    "GROQ_MODEL": SettingSpec(
        "GROQ_MODEL",
        "llama-3.3-70b-versatile",
        description="Legacy Groq model",
        env_example=False,
    ),
    "DISCORD_ENABLED": SettingSpec(
        "DISCORD_ENABLED", False, "bool", description="Enable Discord notifications"
    ),
    "DISCORD_WEBHOOK_URL": SettingSpec(
        "DISCORD_WEBHOOK_URL", "", description="Default Discord webhook", secret=True
    ),
    "DISCORD_WEBHOOK_URL_NOTIFICATION": SettingSpec(
        "DISCORD_WEBHOOK_URL_NOTIFICATION",
        "",
        description="Discord summary webhook",
        secret=True,
    ),
    "DISCORD_WEBHOOK_URL_ALERT": SettingSpec(
        "DISCORD_WEBHOOK_URL_ALERT", "", description="Discord alert webhook", secret=True
    ),
    "DISCORD_FALLBACK_WEBHOOK_URL": SettingSpec(
        "DISCORD_FALLBACK_WEBHOOK_URL", "", description="Discord fallback webhook", secret=True
    ),
    "SHORT_NAME_USE_AI": SettingSpec(
        "SHORT_NAME_USE_AI", False, "bool", description="Use AI for short product names"
    ),
    # Feature flags
    "ENABLE_PROFILING": SettingSpec(
        "ENABLE_PROFILING",
        False,
        "bool",
        description="Enable task profiling",
        airflow_variable=True,
    ),
    "USE_ASYNC_CRAWLER": SettingSpec(
        "USE_ASYNC_CRAWLER", False, "bool", description="Enable async crawler feature path"
    ),
    "ASYNC_CONNECTOR_LIMIT": SettingSpec(
        "ASYNC_CONNECTOR_LIMIT", 300, "int", description="Async crawler connector limit"
    ),
    "ASYNC_BATCH_SIZE": SettingSpec(
        "ASYNC_BATCH_SIZE", 50, "int", description="Async crawler batch size"
    ),
    "ASYNC_TIMEOUT_TOTAL": SettingSpec(
        "ASYNC_TIMEOUT_TOTAL", 15, "int", description="Async crawler total timeout"
    ),
    "ASYNC_TIMEOUT_CONNECT": SettingSpec(
        "ASYNC_TIMEOUT_CONNECT", 10, "int", description="Async crawler connect timeout"
    ),
    "USE_VECTORIZED_TRANSFORM": SettingSpec(
        "USE_VECTORIZED_TRANSFORM",
        False,
        "bool",
        description="Enable vectorized transform",
        airflow_variable=True,
    ),
    "ENABLE_METRICS_COLLECTION": SettingSpec(
        "ENABLE_METRICS_COLLECTION",
        False,
        "bool",
        description="Enable metrics collection",
        airflow_variable=True,
    ),
    "USE_OPTIMIZED_LOADER": SettingSpec(
        "USE_OPTIMIZED_LOADER", False, "bool", description="Enable optimized loader integration"
    ),
    "OPTIMIZED_BATCH_SIZE": SettingSpec(
        "OPTIMIZED_BATCH_SIZE", 1000, "int", description="Optimized loader batch size"
    ),
    "USE_COPY_COMMAND": SettingSpec(
        "USE_COPY_COMMAND", True, "bool", description="Use PostgreSQL COPY for insert-only loads"
    ),
    # HTTP/crawl constants
    "HTTP_CONNECTOR_LIMIT": SettingSpec(
        "HTTP_CONNECTOR_LIMIT", 300, "int", description="Total aiohttp connector limit"
    ),
    "HTTP_CONNECTOR_LIMIT_PER_HOST": SettingSpec(
        "HTTP_CONNECTOR_LIMIT_PER_HOST", 50, "int", description="Per-host aiohttp connector limit"
    ),
    "HTTP_TIMEOUT_TOTAL": SettingSpec(
        "HTTP_TIMEOUT_TOTAL", 15, "int", description="Total HTTP timeout seconds"
    ),
    "HTTP_TIMEOUT_CONNECT": SettingSpec(
        "HTTP_TIMEOUT_CONNECT", 10, "int", description="HTTP connect timeout seconds"
    ),
    "HTTP_DNS_CACHE_TTL": SettingSpec(
        "HTTP_DNS_CACHE_TTL", 1800, "int", description="HTTP DNS cache TTL seconds"
    ),
    "FIRECRAWL_API_URL": SettingSpec(
        "FIRECRAWL_API_URL", "http://localhost:3002", description="Firecrawl API URL"
    ),
    "CATEGORY_BATCH_SIZE": SettingSpec(
        "CATEGORY_BATCH_SIZE", 20, "int", description="Categories per crawl batch"
    ),
    "CATEGORY_TIMEOUT": SettingSpec(
        "CATEGORY_TIMEOUT", 120, "int", description="Category request timeout seconds"
    ),
    "CATEGORY_CONCURRENT_REQUESTS": SettingSpec(
        "CATEGORY_CONCURRENT_REQUESTS", 10, "int", description="Category API concurrency"
    ),
    "CATEGORY_POOL_SIZE": SettingSpec(
        "CATEGORY_POOL_SIZE", 8, "int", description="Category Selenium pool size"
    ),
    "PRODUCT_BATCH_SIZE": SettingSpec(
        "PRODUCT_BATCH_SIZE", 50, "int", description="Product detail batch size"
    ),
    "PRODUCT_TIMEOUT": SettingSpec(
        "PRODUCT_TIMEOUT", 60, "int", description="Product crawl timeout seconds"
    ),
    "PRODUCT_POOL_SIZE": SettingSpec(
        "PRODUCT_POOL_SIZE", 12, "int", description="Product Selenium pool size"
    ),
    "REDIS_CACHE_TTL_PRODUCT_DETAIL": SettingSpec(
        "REDIS_CACHE_TTL_PRODUCT_DETAIL", 604800, "int", description="Product detail cache TTL"
    ),
    "REDIS_CACHE_TTL_PRODUCT_LIST": SettingSpec(
        "REDIS_CACHE_TTL_PRODUCT_LIST", 43200, "int", description="Product list cache TTL"
    ),
    "REDIS_CACHE_TTL_HTML": SettingSpec(
        "REDIS_CACHE_TTL_HTML", 604800, "int", description="HTML cache TTL"
    ),
    "CACHE_ACCEPT_PARTIAL_DATA": SettingSpec(
        "CACHE_ACCEPT_PARTIAL_DATA", True, "bool", description="Allow partial cache data"
    ),
    "PRODUCT_RETRY_MAX_ATTEMPTS": SettingSpec(
        "PRODUCT_RETRY_MAX_ATTEMPTS", 2, "int", description="Product retry attempts"
    ),
    "PRODUCT_RETRY_DELAY_BASE": SettingSpec(
        "PRODUCT_RETRY_DELAY_BASE", 3, "int", description="Product retry base delay seconds"
    ),
    "PRODUCT_RETRY_WAIT_MULTIPLIER": SettingSpec(
        "PRODUCT_RETRY_WAIT_MULTIPLIER", 2, "int", description="Product retry wait multiplier"
    ),
    "DATA_QUALITY_MIN_SCORE": SettingSpec(
        "DATA_QUALITY_MIN_SCORE", 0.7, "float", description="Minimum product data quality score"
    ),
    "CRAWL_IMPLICIT_WAIT_NORMAL": SettingSpec(
        "CRAWL_IMPLICIT_WAIT_NORMAL", 4, "int", description="Normal Selenium implicit wait"
    ),
    "CRAWL_DYNAMIC_CONTENT_WAIT_NORMAL": SettingSpec(
        "CRAWL_DYNAMIC_CONTENT_WAIT_NORMAL",
        4,
        "int",
        description="Normal dynamic content wait",
    ),
    "CRAWL_POST_SCROLL_SLEEP_NORMAL": SettingSpec(
        "CRAWL_POST_SCROLL_SLEEP_NORMAL", 1.2, "float", description="Normal post-scroll sleep"
    ),
    "CRAWL_IMPLICIT_WAIT_RETRY": SettingSpec(
        "CRAWL_IMPLICIT_WAIT_RETRY", 8, "int", description="Retry Selenium implicit wait"
    ),
    "CRAWL_DYNAMIC_CONTENT_WAIT_RETRY": SettingSpec(
        "CRAWL_DYNAMIC_CONTENT_WAIT_RETRY",
        8,
        "int",
        description="Retry dynamic content wait",
    ),
    "CRAWL_POST_SCROLL_SLEEP_RETRY": SettingSpec(
        "CRAWL_POST_SCROLL_SLEEP_RETRY", 2.4, "float", description="Retry post-scroll sleep"
    ),
}


@dataclass(frozen=True)
class DatabaseSettings:
    host: str
    port: int
    database: str
    user: str
    password: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": self.password,
        }


@dataclass(frozen=True)
class RedisSettings:
    url: str
    password: str


@dataclass(frozen=True)
class AirflowRuntimeSettings:
    schedule_mode: str
    schedule_hours: int
    category_num_batches: int
    max_active_tasks: int


@dataclass(frozen=True)
class CrawlSettings:
    category_concurrency: int
    min_category_level: int
    max_category_level: int
    max_category_levels: int
    max_pages_per_category: int
    crawl_timeout: int
    cache_ttl: int
    use_selenium: bool
    force_refresh: bool
    http_connector_limit: int
    http_connector_limit_per_host: int
    http_timeout_total: int
    http_timeout_connect: int
    http_dns_cache_ttl: int


@dataclass(frozen=True)
class DetailSettings:
    products_per_day: int
    active_batches: int
    batch_size: int
    max_concurrent_tasks: int
    concurrency_min: int
    concurrency_max: int
    rate_limit_delay: float
    crawl_timeout: int
    pool_size: int
    batch_output_dir: str
    cache_relax_days: int


@dataclass(frozen=True)
class LoadSettings:
    save_batch_size: int
    mode: str
    optimized_threshold: int
    allow_file_only_on_db_failure: bool


@dataclass(frozen=True)
class AISettings:
    enabled: bool
    api_key: str
    base_url: str
    model: str

    def as_config(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "api_key": self.api_key,
            "base_url": self.base_url,
            "model": self.model,
        }


@dataclass(frozen=True)
class NotificationSettings:
    discord_enabled: bool
    webhook_url: str
    webhook_url_notification: str
    webhook_url_alert: str
    fallback_webhook_url: str

    def as_discord_config(self) -> dict[str, Any]:
        return {
            "webhook_url": self.webhook_url,
            "webhook_url_notification": self.webhook_url_notification,
            "webhook_url_alert": self.webhook_url_alert,
            "fallback_webhook_url": self.fallback_webhook_url,
            "enabled": self.discord_enabled,
        }


@dataclass(frozen=True)
class PipelineSettings:
    database: DatabaseSettings
    redis: RedisSettings
    airflow: AirflowRuntimeSettings
    crawl: CrawlSettings
    detail: DetailSettings
    load: LoadSettings
    ai: AISettings
    notifications: NotificationSettings
    short_name_use_ai: bool
    optimization: dict[str, Any]


def get_setting(name: str, *, use_airflow: bool = True) -> Any:
    return _get_value(SETTINGS_REGISTRY[name], use_airflow=use_airflow)


def get_bool_setting(name: str, *, use_airflow: bool = True) -> bool:
    return bool(get_setting(name, use_airflow=use_airflow))


def get_int_setting(name: str, *, use_airflow: bool = True) -> int:
    return int(get_setting(name, use_airflow=use_airflow))


def get_float_setting(name: str, *, use_airflow: bool = True) -> float:
    return float(get_setting(name, use_airflow=use_airflow))


def get_pipeline_settings(*, use_airflow: bool = True) -> PipelineSettings:
    ai_enabled = get_bool_setting("AI_ENABLED", use_airflow=use_airflow) or get_bool_setting(
        "GROQ_ENABLED", use_airflow=use_airflow
    )
    ai_key = str(get_setting("AI_API_KEY", use_airflow=use_airflow)) or str(
        get_setting("GROQ_API_KEY", use_airflow=use_airflow)
    )
    ai_base = str(get_setting("AI_API_BASE", use_airflow=use_airflow)) or str(
        get_setting("GROQ_API_BASE", use_airflow=use_airflow)
    )
    ai_model = str(get_setting("AI_MODEL", use_airflow=use_airflow)) or str(
        get_setting("GROQ_MODEL", use_airflow=use_airflow)
    )

    return PipelineSettings(
        database=DatabaseSettings(
            host=str(get_setting("POSTGRES_HOST", use_airflow=use_airflow)),
            port=get_int_setting("POSTGRES_PORT", use_airflow=use_airflow),
            database=str(get_setting("POSTGRES_DB", use_airflow=use_airflow)),
            user=str(get_setting("POSTGRES_USER", use_airflow=use_airflow)),
            password=str(get_setting("POSTGRES_PASSWORD", use_airflow=use_airflow)),
        ),
        redis=RedisSettings(
            url=str(get_setting("REDIS_URL", use_airflow=use_airflow)),
            password=str(get_setting("REDIS_PASSWORD", use_airflow=use_airflow)),
        ),
        airflow=AirflowRuntimeSettings(
            schedule_mode=str(get_setting("TIKI_DAG_SCHEDULE_MODE", use_airflow=use_airflow)),
            schedule_hours=get_int_setting("TIKI_DAG_SCHEDULE_HOURS", use_airflow=use_airflow),
            category_num_batches=get_int_setting(
                "TIKI_CATEGORY_NUM_BATCHES", use_airflow=use_airflow
            ),
            max_active_tasks=get_int_setting("TIKI_DETAIL_ACTIVE_BATCHES", use_airflow=use_airflow),
        ),
        crawl=CrawlSettings(
            category_concurrency=get_int_setting(
                "TIKI_CATEGORY_CONCURRENCY", use_airflow=use_airflow
            ),
            min_category_level=get_int_setting("TIKI_MIN_CATEGORY_LEVEL", use_airflow=use_airflow),
            max_category_level=get_int_setting("TIKI_MAX_CATEGORY_LEVEL", use_airflow=use_airflow),
            max_category_levels=get_int_setting(
                "TIKI_MAX_CATEGORY_LEVELS", use_airflow=use_airflow
            ),
            max_pages_per_category=get_int_setting(
                "TIKI_MAX_PAGES_PER_CATEGORY", use_airflow=use_airflow
            ),
            crawl_timeout=get_int_setting("TIKI_CRAWL_TIMEOUT", use_airflow=use_airflow),
            cache_ttl=get_int_setting("TIKI_CACHE_TTL", use_airflow=use_airflow),
            use_selenium=get_bool_setting("TIKI_USE_SELENIUM", use_airflow=use_airflow),
            force_refresh=get_bool_setting("TIKI_FORCE_REFRESH", use_airflow=use_airflow),
            http_connector_limit=get_int_setting("HTTP_CONNECTOR_LIMIT", use_airflow=use_airflow),
            http_connector_limit_per_host=get_int_setting(
                "HTTP_CONNECTOR_LIMIT_PER_HOST", use_airflow=use_airflow
            ),
            http_timeout_total=get_int_setting("HTTP_TIMEOUT_TOTAL", use_airflow=use_airflow),
            http_timeout_connect=get_int_setting("HTTP_TIMEOUT_CONNECT", use_airflow=use_airflow),
            http_dns_cache_ttl=get_int_setting("HTTP_DNS_CACHE_TTL", use_airflow=use_airflow),
        ),
        detail=DetailSettings(
            products_per_day=get_int_setting("TIKI_PRODUCTS_PER_DAY", use_airflow=use_airflow),
            active_batches=get_int_setting("TIKI_DETAIL_ACTIVE_BATCHES", use_airflow=use_airflow),
            batch_size=get_int_setting("TIKI_DETAIL_BATCH_SIZE", use_airflow=use_airflow),
            max_concurrent_tasks=get_int_setting(
                "TIKI_DETAIL_MAX_CONCURRENT_TASKS", use_airflow=use_airflow
            ),
            concurrency_min=get_int_setting("TIKI_DETAIL_CONCURRENCY_MIN", use_airflow=use_airflow),
            concurrency_max=get_int_setting("TIKI_DETAIL_CONCURRENCY_MAX", use_airflow=use_airflow),
            rate_limit_delay=get_float_setting(
                "TIKI_DETAIL_RATE_LIMIT_DELAY", use_airflow=use_airflow
            ),
            crawl_timeout=get_int_setting("TIKI_DETAIL_CRAWL_TIMEOUT", use_airflow=use_airflow),
            pool_size=get_int_setting("TIKI_DETAIL_POOL_SIZE", use_airflow=use_airflow),
            batch_output_dir=str(
                get_setting("TIKI_DETAIL_BATCH_OUTPUT_DIR", use_airflow=use_airflow)
            ),
            cache_relax_days=get_int_setting("TIKI_CACHE_RELAX_DAYS", use_airflow=use_airflow),
        ),
        load=LoadSettings(
            save_batch_size=get_int_setting("TIKI_SAVE_BATCH_SIZE", use_airflow=use_airflow),
            mode=str(get_setting("TIKI_LOAD_MODE", use_airflow=use_airflow)),
            optimized_threshold=get_int_setting(
                "TIKI_LOAD_OPTIMIZED_THRESHOLD", use_airflow=use_airflow
            ),
            allow_file_only_on_db_failure=get_bool_setting(
                "TIKI_ALLOW_FILE_ONLY_ON_DB_FAILURE", use_airflow=use_airflow
            ),
        ),
        ai=AISettings(enabled=ai_enabled, api_key=ai_key, base_url=ai_base, model=ai_model),
        notifications=NotificationSettings(
            discord_enabled=get_bool_setting("DISCORD_ENABLED", use_airflow=use_airflow),
            webhook_url=str(get_setting("DISCORD_WEBHOOK_URL", use_airflow=use_airflow)),
            webhook_url_notification=str(
                get_setting("DISCORD_WEBHOOK_URL_NOTIFICATION", use_airflow=use_airflow)
            ),
            webhook_url_alert=str(
                get_setting("DISCORD_WEBHOOK_URL_ALERT", use_airflow=use_airflow)
            ),
            fallback_webhook_url=str(
                get_setting("DISCORD_FALLBACK_WEBHOOK_URL", use_airflow=use_airflow)
            ),
        ),
        short_name_use_ai=get_bool_setting("SHORT_NAME_USE_AI", use_airflow=use_airflow),
        optimization={
            "use_async_crawler": get_bool_setting("USE_ASYNC_CRAWLER", use_airflow=use_airflow),
            "async_connector_limit": get_int_setting(
                "ASYNC_CONNECTOR_LIMIT", use_airflow=use_airflow
            ),
            "async_batch_size": get_int_setting("ASYNC_BATCH_SIZE", use_airflow=use_airflow),
            "async_timeout_total": get_int_setting("ASYNC_TIMEOUT_TOTAL", use_airflow=use_airflow),
            "async_timeout_connect": get_int_setting(
                "ASYNC_TIMEOUT_CONNECT", use_airflow=use_airflow
            ),
            "use_vectorized_transform": get_bool_setting(
                "USE_VECTORIZED_TRANSFORM", use_airflow=use_airflow
            ),
            "use_optimized_loader": get_bool_setting(
                "USE_OPTIMIZED_LOADER", use_airflow=use_airflow
            ),
            "optimized_batch_size": get_int_setting(
                "OPTIMIZED_BATCH_SIZE", use_airflow=use_airflow
            ),
            "use_copy_command": get_bool_setting("USE_COPY_COMMAND", use_airflow=use_airflow),
        },
    )


def airflow_variable_defaults() -> dict[str, str]:
    airflow_keys = {
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "REDIS_URL",
        "TIKI_MIN_CATEGORY_LEVEL",
        "TIKI_MAX_CATEGORY_LEVEL",
        "TIKI_MAX_CATEGORIES",
        "TIKI_DAG_SCHEDULE_MODE",
        "TIKI_DAG_SCHEDULE_HOURS",
        "TIKI_SAVE_BATCH_SIZE",
        "TIKI_CATEGORY_NUM_BATCHES",
        "TIKI_CATEGORY_CONCURRENCY",
        "TIKI_CATEGORY_MAX_ACTIVE_TASKS",
        "TIKI_DETAIL_ACTIVE_BATCHES",
        "TIKI_DETAIL_BATCH_SIZE",
        "TIKI_DETAIL_MAX_CONCURRENT_TASKS",
        "TIKI_DETAIL_CONCURRENCY_MIN",
        "TIKI_DETAIL_CONCURRENCY_MAX",
        "TIKI_PRODUCTS_PER_DAY",
        "TIKI_LOAD_MODE",
        "TIKI_LOAD_OPTIMIZED_THRESHOLD",
        "TIKI_ANALYTICS_SCHEMA",
        "TIKI_CACHE_RELAX_DAYS",
        "TIKI_CIRCUIT_BREAKER_FAILURE_THRESHOLD",
        "TIKI_CIRCUIT_BREAKER_RECOVERY_TIMEOUT",
        "TIKI_DEGRADATION_FAILURE_THRESHOLD",
        "TIKI_DEGRADATION_RECOVERY_THRESHOLD",
        "ENABLE_PROFILING",
        "USE_VECTORIZED_TRANSFORM",
        "ENABLE_METRICS_COLLECTION",
    }
    return {
        key: str(spec.default).lower() if spec.value_type == "bool" else str(spec.default)
        for key, spec in SETTINGS_REGISTRY.items()
        if (spec.airflow_variable or key in airflow_keys) and not spec.secret
    }


def env_example_defaults() -> dict[str, str]:
    return {
        key: ""
        if spec.secret
        else str(spec.default).lower()
        if spec.value_type == "bool"
        else str(spec.default)
        for key, spec in SETTINGS_REGISTRY.items()
        if spec.env_example
    }


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


POOL_DEFAULTS = {
    "fast_pool": {
        "slots": SETTINGS_REGISTRY["TIKI_DETAIL_ACTIVE_BATCHES"].default,
        "description": "Pool for Fast API and Data Processing tasks",
    },
    "crawl_pool": {
        "slots": SETTINGS_REGISTRY["TIKI_DETAIL_ACTIVE_BATCHES"].default,
        "description": "Pool for Tiki crawling tasks (Selenium)",
    },
}


__all__ = [
    "AISettings",
    "AirflowRuntimeSettings",
    "CrawlSettings",
    "DatabaseSettings",
    "DetailSettings",
    "LoadSettings",
    "NotificationSettings",
    "POOL_DEFAULTS",
    "PipelineSettings",
    "SETTINGS_REGISTRY",
    "SettingSpec",
    "airflow_variable_defaults",
    "env_example_defaults",
    "get_bool_setting",
    "get_float_setting",
    "get_int_setting",
    "get_pipeline_settings",
    "get_setting",
    "project_root",
]
