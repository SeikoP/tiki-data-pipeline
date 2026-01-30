---
name: Tiki Crawler Optimizer
description: Advanced techniques for Tiki crawling, including Selenium pool management, API rate limiting, and resilience patterns.
---

# Tiki Crawler Optimizer

This skill focuses on making the Tiki product crawler as fast, resilient, and "invisible" as possible.

## Core Components

### 1. Hybrid Crawling Strategy
- **Speed Layer**: Uses direct API calls (`/api/v2/products/...`) and `aiohttp` for high-concurrency data ingestion.
- **Enrichment Layer**: Uses Selenium for pages that require JavaScript rendering or when the API is blocked by Cloudflare.

### 2. Selenium Driver Pool Management
- Manages a pool of `WebDriver` instances to avoid the high cost of starting/stopping browsers for every task.
- Ensures properly configured `headless` modes and anti-detection headers.
- **Resource limit**: Monitors memory usage (approx. 500MB-1GB per driver instance).

### 3. Resilience & Self-Healing
- **Circuit Breaker**: Automatically pauses crawling if error rates (403, 500) exceed a threshold.
- **Dead Letter Queue (DLQ)**: Captures failed product URLs for later retry.
- **Graceful Degradation**: Switches from high-concurrency API mode to slow Selenium mode when rate limiting is detected.

## Parameters Tuning
- `TIKI_DETAIL_BATCH_SIZE`: Recommended 100 for API mode, 5-10 for Selenium mode.
- `TIKI_DETAIL_MAX_CONCURRENT_TASKS`: Recommended 30 for API mode.
- `TIKI_DETAIL_RATE_LIMIT_DELAY`: Optimized at 0.05s - 0.1s.

## Troubleshooting
- **403 Forbidden**: Usually means IP rate limiting or browser fingerprint mismatch. Switching to Selenium or rotating User-Agents is required.
- **Zero products preparing**: Check `crawl_progress.json` and database for existing data that might be triggering "skip" logic.
