# Pre-Deployment Checklist ✅

## Prerequisites

### 1. Environment Setup
- [ ] Docker & Docker Compose installed (version 20.10+)
- [ ] Python 3.10+ installed locally
- [ ] 4GB+ RAM available for Docker
- [ ] 20GB+ disk space available
- [ ] PostgreSQL client installed (optional, for testing)

### 2. Configuration Files
- [ ] `.env` file created from `.env.example`
- [ ] `POSTGRES_USER` set in `.env`
- [ ] `POSTGRES_PASSWORD` set in `.env`
- [ ] `_AIRFLOW_WWW_USER_USERNAME` set in `.env`
- [ ] `_AIRFLOW_WWW_USER_PASSWORD` set in `.env`

### 3. Repository State
- [ ] All optimization files present (run: `python tests/final_validation.py`)
- [ ] On `optimize` branch or merged to main
- [ ] No uncommitted changes
- [ ] Latest changes pulled from remote

## Code Validation

### 4. Module Checks
```bash
# Run validation script
python tests/final_validation.py
```

Expected output:
- ✅ 16 optimization files present
- ✅ All modules import successfully
- ✅ Phase 4 optimizations validated
- ⚠️ Phase 2 optimizations need reapplication (noted in OPTIMIZATIONS.md)

### 5. File Structure
- [ ] `src/pipelines/load/db_pool.py` exists (4355 bytes)
- [ ] `src/pipelines/load/loader_optimized.py` exists (14279 bytes)
- [ ] `src/pipelines/crawl/parallel_crawler.py` exists (4779 bytes)
- [ ] `src/pipelines/crawl/crawl_products_parallel.py` exists (4393 bytes)
- [ ] `src/pipelines/optimized_pipeline.py` exists (6103 bytes)
- [ ] `src/common/batch_processor.py` exists (3693 bytes)
- [ ] `src/common/async_batch.py` exists (2539 bytes)
- [ ] `src/common/cache_utils.py` exists (2805 bytes)
- [ ] `src/common/memory_utils.py` exists (2700 bytes)
- [ ] `airflow/setup/postgresql_tuning.conf` exists
- [ ] `docker-compose.performance.yml` exists
- [ ] `src/common/infrastructure_monitor.py` exists (7252 bytes)
- [ ] `airflow/dags/dag_integration.py` exists
- [ ] `airflow/dags/tiki_crawl_products_optimized_dag.py` exists

### 6. DAG Validation
```bash
# Check DAG structure (local - will show import error, that's expected)
python tests/validate_optimized_dag.py

# In Docker (after startup)
docker exec tiki-data-pipeline-airflow-scheduler-1 airflow dags list | grep optimized
docker exec tiki-data-pipeline-airflow-scheduler-1 python -c "from airflow.models import DagBag; print(DagBag().import_errors)"
```

Expected:
- ✅ All optimization modules found in DAG code
- ✅ XCom keys pushed by integration functions
- ❌ Local import error (expected - no Airflow installed locally)
- ✅ Docker import successful (after deployment)

## Infrastructure Validation

### 7. Docker Services
```bash
# Start services
docker-compose -f docker-compose.yml -f docker-compose.performance.yml up -d

# Wait 60 seconds for startup
Start-Sleep -Seconds 60

# Check service health
docker-compose ps
```

Expected output:
- ✅ `postgres` - Up (healthy)
- ✅ `redis` - Up (healthy)
- ✅ `airflow-scheduler` - Up (healthy)
- ✅ `airflow-worker` - Up
- ✅ `airflow-apiserver` - Up
- ✅ `airflow-webserver` - Up (healthy)

### 8. Database Connectivity
```bash
# Test PostgreSQL connection
docker exec tiki-data-pipeline-postgres-1 psql -U postgres -c "SELECT 1"

# Check database exists
docker exec tiki-data-pipeline-postgres-1 psql -U postgres -l | grep crawl_data

# Verify tables
docker exec tiki-data-pipeline-postgres-1 psql -U postgres -d crawl_data -c "\dt"
```

Expected:
- ✅ Connection successful
- ✅ `crawl_data` database exists
- ✅ `categories` table exists
- ✅ `products` table exists

### 9. Redis Connectivity
```bash
# Test Redis connection
docker exec tiki-data-pipeline-redis-1 redis-cli PING

# Check Redis info
docker exec tiki-data-pipeline-redis-1 redis-cli INFO server
```

Expected:
- ✅ PONG response
- ✅ Redis version 7.2+

### 10. Airflow Web UI
- [ ] Access http://localhost:8080
- [ ] Login successful (username/password from `.env`)
- [ ] DAG `tiki_crawl_products_optimized` visible
- [ ] No import errors in Admin → DAGs

## Configuration Validation

### 11. Airflow Variables
```bash
# Set variables
docker exec tiki-data-pipeline-airflow-scheduler-1 airflow variables set tiki_data_dir "/opt/airflow/data"
docker exec tiki-data-pipeline-airflow-scheduler-1 airflow variables set tiki_max_categories "5"
docker exec tiki-data-pipeline-airflow-scheduler-1 airflow variables set tiki_max_products_per_category "100"
docker exec tiki-data-pipeline-airflow-scheduler-1 airflow variables set tiki_parallel_workers "5"
docker exec tiki-data-pipeline-airflow-scheduler-1 airflow variables set tiki_rate_limit "0.5"

# Verify
docker exec tiki-data-pipeline-airflow-scheduler-1 airflow variables list
```

Expected variables:
- ✅ `tiki_data_dir` = `/opt/airflow/data`
- ✅ `tiki_max_categories` = `5`
- ✅ `tiki_max_products_per_category` = `100`
- ✅ `tiki_parallel_workers` = `5`
- ✅ `tiki_rate_limit` = `0.5`

### 12. Database Indexes
```bash
# Apply performance indexes (if not already applied)
docker exec tiki-data-pipeline-postgres-1 psql -U postgres -d crawl_data -c "
  CREATE INDEX IF NOT EXISTS idx_products_url ON products USING btree (url);
  CREATE INDEX IF NOT EXISTS idx_products_sku ON products USING btree (sku);
  CREATE INDEX IF NOT EXISTS idx_products_created_at ON products USING btree (created_at);
  CREATE INDEX IF NOT EXISTS idx_categories_url ON categories USING btree (url);
"

# Verify indexes
docker exec tiki-data-pipeline-postgres-1 psql -U postgres -d crawl_data -c "\di"
```

Expected:
- ✅ `idx_products_url`
- ✅ `idx_products_sku`
- ✅ `idx_products_created_at`
- ✅ `idx_categories_url`

## Test Execution

### 13. Enable DAG
```bash
# Unpause DAG
docker exec tiki-data-pipeline-airflow-scheduler-1 airflow dags unpause tiki_crawl_products_optimized
```

### 14. Test Run (Small Scale)
```bash
# Set test configuration
docker exec tiki-data-pipeline-airflow-scheduler-1 airflow variables set tiki_max_categories "2"
docker exec tiki-data-pipeline-airflow-scheduler-1 airflow variables set tiki_max_products_per_category "10"

# Trigger DAG
docker exec tiki-data-pipeline-airflow-scheduler-1 airflow dags trigger tiki_crawl_products_optimized

# Monitor execution
docker exec tiki-data-pipeline-airflow-scheduler-1 airflow dags list-runs -d tiki_crawl_products_optimized --state running
```

### 15. Verify Test Results
Access Airflow UI → DAGs → `tiki_crawl_products_optimized` → Graph View

Expected task sequence:
1. ✅ `crawl_categories` - Success
2. ✅ `extract_product_urls` - Success  
3. ✅ `crawl_product_details` - Success (parallel)
4. ✅ `transform_products` - Success
5. ✅ `load_products` - Success
6. ✅ `monitoring.get_cache_stats` - Success
7. ✅ `monitoring.monitor_infrastructure` - Success
8. ✅ `cleanup_temp_files` - Success
9. ✅ `send_notification` - Success

### 16. Check XCom Data
In Airflow UI → XCom tab, verify keys:
- [ ] `categories` - List of categories
- [ ] `category_count` - Integer
- [ ] `product_urls` - List of URLs
- [ ] `product_count` - Integer
- [ ] `crawled_products` - List of products
- [ ] `crawl_stats` - Dictionary with stats
- [ ] `transformed_products` - List of transformed products
- [ ] `load_stats` - Dictionary with load results
- [ ] `cache_stats` - Dictionary with cache metrics
- [ ] `infrastructure_stats` - Dictionary with system health

### 17. Performance Validation
Check logs in Airflow UI for:
- [ ] Crawl rate >30 products/second
- [ ] Cache hit rate >80%
- [ ] No memory errors
- [ ] No connection pool exhaustion
- [ ] Load time <50% of original

Example log output:
```
✅ Crawled 10/10 products
⏱️  Time: 0.33s
📈 Rate: 30.3 products/s
💾 Starting optimized load of 10 products
✅ Loaded 10/10 products
⏱️  Time: 0.15s
📊 Cache Stats:
   Hits: 8
   Misses: 2
   Hit rate: 80.0%
```

## Production Readiness

### 18. Resource Monitoring
```bash
# Monitor Docker resources
docker stats --no-stream

# Check PostgreSQL performance
docker exec tiki-data-pipeline-postgres-1 psql -U postgres -d crawl_data -c "
  SELECT 
    sum(heap_blks_hit) / nullif(sum(heap_blks_hit) + sum(heap_blks_read), 0) * 100 as cache_hit_ratio
  FROM pg_statio_user_tables;
"

# Check Redis memory
docker exec tiki-data-pipeline-redis-1 redis-cli INFO memory | grep used_memory_human
```

Expected:
- ✅ CPU usage <80%
- ✅ Memory usage <80%
- ✅ PostgreSQL cache hit ratio >90%
- ✅ Redis memory <400MB

### 19. Scale to Production Settings
```bash
# Update variables for production
docker exec tiki-data-pipeline-airflow-scheduler-1 airflow variables set tiki_max_categories "20"
docker exec tiki-data-pipeline-airflow-scheduler-1 airflow variables set tiki_max_products_per_category "1000"
docker exec tiki-data-pipeline-airflow-scheduler-1 airflow variables set tiki_parallel_workers "10"
```

### 20. Backup Strategy
- [ ] PostgreSQL backup script tested: `.\scripts\backup-postgres.ps1`
- [ ] Backup directory exists: `backups/postgres/`
- [ ] Restore tested: `.\scripts\restore-postgres.ps1`
- [ ] Automated backup scheduled (cron/Task Scheduler)

## Monitoring & Alerts

### 21. Logging Setup
- [ ] Airflow logs directory accessible: `airflow/logs/`
- [ ] Log rotation configured
- [ ] Error tracking set up

### 22. Health Checks
```bash
# Create health check script
.\scripts\health_check.ps1
```

Content:
```powershell
# Check all services
$services = @("postgres", "redis", "airflow-scheduler", "airflow-worker")
foreach ($service in $services) {
    $status = docker-compose ps -q $service
    if ($status) {
        Write-Host "✅ $service is running"
    } else {
        Write-Host "❌ $service is down" -ForegroundColor Red
    }
}

# Check DAG state
docker exec tiki-data-pipeline-airflow-scheduler-1 airflow dags state tiki_crawl_products_optimized
```

### 23. Alert Configuration
- [ ] Email notifications configured in Airflow
- [ ] Slack/Discord webhook set up (optional)
- [ ] Error threshold alerts configured

## Documentation

### 24. Team Handoff
- [ ] DEPLOYMENT.md reviewed by team
- [ ] OPTIMIZATIONS.md reviewed by team
- [ ] Runbook created for common issues
- [ ] On-call rotation established

### 25. Rollback Plan
- [ ] Original DAG available: `tiki_crawl_products`
- [ ] Rollback procedure documented
- [ ] Database backup before deployment
- [ ] Quick switch tested: `airflow dags pause tiki_crawl_products_optimized` + `airflow dags unpause tiki_crawl_products`

## Final Sign-off

### Deployment Decision
- [ ] All critical checks passed (1-17)
- [ ] Test run successful (18-20)
- [ ] Team reviewed and approved
- [ ] Stakeholders notified

### Deployment Timing
- Date: _______________
- Time: _______________ (off-peak hours recommended)
- Deployment lead: _______________
- Backup operator: _______________

### Post-Deployment
- [ ] Monitor for 24 hours
- [ ] Verify scheduled runs
- [ ] Document any issues
- [ ] Update team on performance gains

---

## Quick Command Reference

```bash
# Start optimized stack
docker-compose -f docker-compose.yml -f docker-compose.performance.yml up -d

# Check health
docker-compose ps
docker-compose logs -f airflow-scheduler

# Trigger DAG
docker exec tiki-data-pipeline-airflow-scheduler-1 airflow dags trigger tiki_crawl_products_optimized

# Monitor
docker exec tiki-data-pipeline-airflow-scheduler-1 airflow dags list-runs -d tiki_crawl_products_optimized

# Rollback
docker exec tiki-data-pipeline-airflow-scheduler-1 airflow dags pause tiki_crawl_products_optimized
docker exec tiki-data-pipeline-airflow-scheduler-1 airflow dags unpause tiki_crawl_products
```

---

**Deployment Status**: [ ] Ready / [ ] Needs Fixes / [ ] Blocked

**Notes**:
_______________________________________
_______________________________________
_______________________________________
