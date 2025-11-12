# 🚀 Quick Fix - Tiki Data Pipeline Services

## Problem
```
ERROR: relation "log" does not exist
```

## Solution (30 seconds)

### 1️⃣ Run the fix script
```bash
python scripts/verify_services.py
```

### 2️⃣ Or manual steps
```bash
# Restart docker-compose
docker-compose down
docker-compose up -d

# Wait for containers to start
sleep 15

# Initialize Airflow database
docker-compose run --rm airflow-init

# Restart services
docker-compose restart airflow-apiserver airflow-scheduler airflow-worker
```

### 3️⃣ Verify everything works
```bash
# Check all services are healthy
docker-compose ps

# Check Airflow API is working
curl http://localhost:8080/api/v2/version

# Check logs for errors
docker-compose logs -f postgres | grep ERROR
```

---

## ✅ Expected Results

After fix:
- ✓ All containers showing "Up" status
- ✓ Airflow Web UI accessible at http://localhost:8080
- ✓ No more "relation log does not exist" errors
- ✓ Redis responding: `docker-compose exec redis redis-cli ping` → PONG
- ✓ PostgreSQL ready: `docker-compose exec postgres pg_isready` → accepting connections

---

## 📚 See Also
- `TROUBLESHOOTING.md` - Full troubleshooting guide
- `scripts/verify_services.py` - Automated service checker
- `docker-compose.yaml` - Service configuration

---

**Next:** Access http://localhost:8080 (username: airflow, password: airflow)

