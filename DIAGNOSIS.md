# 🔍 Diagnosis Report - Tiki Data Pipeline

## Current Issues Found

### Issue 1: PostgreSQL - Missing Airflow Tables ⚠️ CRITICAL

**Status:** ❌ NEEDS FIX

**Error Log:**
```
postgres | ERROR:  relation "log" does not exist at character 13
postgres | STATEMENT:  INSERT INTO log (dttm, event, owner, extra) VALUES ...
```

**Root Cause:**
- Airflow database `airflow` was created but tables were NOT initialized
- When Airflow services try to insert logs, table `log` doesn't exist
- This happens when `airflow db migrate` was not executed

**Impact:**
- 🔴 Airflow cannot store logs
- 🔴 Airflow API may fail to start properly
- 🔴 Airflow tasks cannot be tracked
- 🔴 All services depending on Airflow will fail

**Solution:**
```bash
# Option A: Automatic (Recommended)
python scripts/verify_services.py

# Option B: Manual
docker-compose run --rm airflow-init
docker-compose restart
```

**Verification:**
```bash
# Check if tables exist
docker-compose exec postgres psql -U airflow -d airflow -c '\dt'

# Should show tables like: log, dag, task_instance, etc.
```

---

### Issue 2: Service Initialization Order

**Status:** ⚠️ NEEDS ATTENTION

**Problem:**
- Services may start before database is fully ready
- airflow-init might not complete before other services start

**Solution:**
```bash
# Full restart with proper ordering
docker-compose down
docker-compose up -d postgres redis
sleep 10
docker-compose run --rm airflow-init
docker-compose up -d
```

---

### Issue 3: Deprecated Airflow Permissions Warning

**Status:** ⚠️ WARNING ONLY

**Warning Log:**
```
RemovedInAirflow4Warning: The airflow.security.permissions module is deprecated
```

**Impact:**
- ⚠️ Non-critical - just a deprecation notice
- Will be removed in Airflow 4.0
- Current version (3.1.2) still supports it

**Action:** Not urgent - can be ignored for now

---

## 🛠️ Quick Diagnosis

### Run this to check current status:
```bash
bash scripts/quick-check.sh
```

### Or use Python script for auto-fix:
```bash
python scripts/verify_services.py
```

---

## 📋 Checklist - Before Fixing

- [ ] Docker Desktop is running
- [ ] All ports are available (8080, 3002, 5432, 6379)
- [ ] Enough disk space (at least 10GB)
- [ ] Enough RAM (4GB+)

---

## 🔧 Fix Steps

### Step 1: Prepare
```bash
# Check current status
docker-compose ps

# View any errors
docker-compose logs postgres | grep ERROR
```

### Step 2: Fix (Choose ONE)

**Option A - Full Auto-Fix (RECOMMENDED):**
```bash
python scripts/verify_services.py
```

**Option B - Targeted Fix:**
```bash
# Run Airflow initialization only
docker-compose run --rm airflow-init

# Restart affected services
docker-compose restart airflow-apiserver airflow-scheduler airflow-worker
```

**Option C - Complete Reset:**
```bash
docker-compose down -v
docker-compose up -d
sleep 15
docker-compose run --rm airflow-init
docker-compose logs postgres
```

### Step 3: Verify
```bash
# All services should show "Up" status
docker-compose ps

# Check key services
docker-compose exec postgres pg_isready
docker-compose exec redis redis-cli ping
curl http://localhost:8080/api/v2/version
```

### Step 4: Test
```bash
# Access Airflow UI
open http://localhost:8080  # macOS
# or
xdg-open http://localhost:8080  # Linux
# or
start http://localhost:8080  # Windows

# Login with: airflow / airflow
```

---

## 📊 Expected Results After Fix

| Service | Status | Check Command |
|---------|--------|---------------|
| PostgreSQL | ✓ Up | `docker-compose exec postgres pg_isready` |
| Redis | ✓ Up | `docker-compose exec redis redis-cli ping` |
| Airflow API | ✓ Up | `curl http://localhost:8080/health` |
| Firecrawl API | ✓ Up | `curl http://localhost:3002/health` |
| Airflow Tables | ✓ Exist | `docker-compose exec postgres psql -U airflow -d airflow -c '\dt'` |

---

## 🎯 Success Criteria

Fix is successful when:
1. ✓ All containers show "Up" in `docker-compose ps`
2. ✓ No ERROR messages in logs
3. ✓ Airflow Web UI is accessible at localhost:8080
4. ✓ Can login to Airflow (default: airflow/airflow)
5. ✓ PostgreSQL shows Airflow tables: `\dt` returns 30+ tables
6. ✓ Redis is responding to pings

---

## 🚨 If Fix Fails

1. **Check disk space:**
   ```bash
   df -h
   # Need at least 10GB free
   ```

2. **Check logs for specific errors:**
   ```bash
   docker-compose logs postgres --tail=50
   docker-compose logs airflow-init --tail=50
   ```

3. **Nuclear option - Full reset:**
   ```bash
   docker-compose down -v
   # This DELETES all data!
   docker system prune -a
   docker-compose up -d
   ```

4. **Check Docker resource allocation:**
   ```bash
   docker stats
   # Need: 4GB RAM, 2 CPUs minimum
   ```

---

## 📞 Support Resources

- [Airflow Troubleshooting](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [PostgreSQL Logs](https://www.postgresql.org/docs/16/runtime-config-logging.html)
- [Docker Compose Debugging](https://docs.docker.com/compose/compose-file/)

---

**Report Generated:** 2025-11-10
**Tiki Data Pipeline Version:** 1.0

