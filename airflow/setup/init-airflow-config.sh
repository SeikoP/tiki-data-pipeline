#!/bin/bash
# =============================================================================
# Airflow Configuration Initialization Script
# Run after DB migration to configure pools, variables, and connections
# =============================================================================
set -e

echo "=========================================="
echo "🚀 Initializing Airflow Configuration..."
echo "=========================================="

# Wait for Airflow DB to be ready
MAX_RETRIES=30
RETRY_COUNT=0
while ! airflow db check >/dev/null 2>&1; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        echo "❌ Airflow DB not ready after $MAX_RETRIES retries. Exiting."
        exit 1
    fi
    echo "⏳ Waiting for Airflow DB... ($RETRY_COUNT/$MAX_RETRIES)"
    sleep 2
done
echo "✅ Airflow DB is ready"

# =============================================================================
# CREATE POOLS
# =============================================================================
echo ""
echo "📦 Creating Airflow Pools..."

# fast_pool: Optimized for API-based and data processing tasks (high concurrency)
airflow pools set fast_pool 32 "Pool for Fast API and Data Processing tasks" 2>/dev/null || \
    echo "   Pool 'fast_pool' already exists or update failed"

# crawl_pool: Restricted concurrency for Selenium tasks to avoid OOM
# 8 slots recommended for systems with 16GB+ RAM
airflow pools set crawl_pool 8 "Pool for Tiki crawling tasks (Selenium)" 2>/dev/null || \
    echo "   Pool 'crawl_pool' already exists or update failed"

echo "✅ Pools configured"

# =============================================================================
# SET AIRFLOW VARIABLES
# =============================================================================
echo ""
echo "⚙️  Setting Airflow Variables..."

# Category filtering
airflow variables set TIKI_MIN_CATEGORY_LEVEL "2" 2>/dev/null || true
airflow variables set TIKI_MAX_CATEGORY_LEVEL "4" 2>/dev/null || true
airflow variables set TIKI_MAX_CATEGORIES "0" 2>/dev/null || true

# DAG scheduling mode (manual for testing, scheduled for production)
airflow variables set TIKI_DAG_SCHEDULE_MODE "manual" 2>/dev/null || true

# Batch processing configuration (optimized for 16GB RAM)
airflow variables set TIKI_SAVE_BATCH_SIZE "100" 2>/dev/null || true
airflow variables set TIKI_CRAWL_BATCH_SIZE "5" 2>/dev/null || true

# Circuit breaker configuration
airflow variables set TIKI_CIRCUIT_BREAKER_FAILURE_THRESHOLD "5" 2>/dev/null || true
airflow variables set TIKI_CIRCUIT_BREAKER_RECOVERY_TIMEOUT "60" 2>/dev/null || true

# Graceful degradation
airflow variables set TIKI_DEGRADATION_FAILURE_THRESHOLD "3" 2>/dev/null || true
airflow variables set TIKI_DEGRADATION_RECOVERY_THRESHOLD "5" 2>/dev/null || true

# Database configuration (use environment variables as default)
airflow variables set POSTGRES_HOST "${POSTGRES_HOST:-postgres}" 2>/dev/null || true
airflow variables set POSTGRES_PORT "${POSTGRES_PORT:-5432}" 2>/dev/null || true
airflow variables set POSTGRES_DB "${POSTGRES_DB:-tiki}" 2>/dev/null || true
airflow variables set POSTGRES_USER "${POSTGRES_USER:-airflow_user}" 2>/dev/null || true
# Note: Password should be set via Airflow UI or encrypted connection for security

# Redis configuration
airflow variables set REDIS_URL "redis://redis:6379/1" 2>/dev/null || true

echo "✅ Variables configured"

# =============================================================================
# CONFIGURE CONNECTIONS
# =============================================================================
echo ""
echo "🔌 Configuring Airflow Connections..."

# tiki_postgres: Connection to the project database
# Using environment variables from .env / docker-compose
airflow connections add tiki_postgres \
    --conn-type postgres \
    --conn-host "${POSTGRES_HOST:-postgres}" \
    --conn-login "${POSTGRES_USER:-admin}" \
    --conn-password "${POSTGRES_PASSWORD:-admin}" \
    --conn-port "${POSTGRES_PORT:-5432}" \
    --conn-schema "${POSTGRES_DB:-tiki}" 2>/dev/null || \
    echo "   Connection 'tiki_postgres' already exists or update failed"

echo "✅ Connections configured"

# =============================================================================
# FORCE DAG PARSING
# =============================================================================
echo ""
echo "🔄 Triggering DAG parsing..."

# Force reserialize to ensure DAGs are loaded immediately
airflow dags reserialize 2>/dev/null || echo "   DAG reserialize skipped (will happen on scheduler start)"

echo "✅ DAG parsing triggered"

# =============================================================================
# DISPLAY SUMMARY
# =============================================================================
echo ""
echo "=========================================="
echo "📋 Configuration Summary"
echo "=========================================="
echo ""
echo "Pools:"
airflow pools list 2>/dev/null | grep -E "(crawl_pool|Pool)" | head -5
echo ""
echo "Variables (sample):"
airflow variables list 2>/dev/null | head -10
echo ""
echo "DAGs:"
airflow dags list 2>/dev/null | head -10 || echo "   DAGs will be loaded by scheduler"
echo ""
echo "=========================================="
echo "✅ Airflow configuration completed!"
echo "=========================================="
