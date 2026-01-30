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
# OPTIMIZED CONFIGURATION (ONE-SHOT)
# =============================================================================
echo ""
echo "⚙️  Running Optimized Configuration (One-shot)..."

# Run the python setup script which handles Pools, Variables, and Connections in a single session
python /opt/airflow/airflow/setup/setup_airflow.py

echo "✅ Configuration completed via setup_airflow.py"

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
