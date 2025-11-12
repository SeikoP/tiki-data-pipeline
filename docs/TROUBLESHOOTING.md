# Hướng dẫn Fix Lỗi Services - Tiki Data Pipeline

## 🔍 Lỗi Hiện Tại

### PostgreSQL Error: `relation "log" does not exist`

**Nguyên nhân:** Airflow database `airflow` đã được tạo nhưng các tables chưa được initialize.

**Logs Error:**
```
postgres              | 2025-11-10 05:43:48.688 UTC [49] ERROR:  relation "log" does not exist at character 13
postgres              | 2025-11-10 05:43:48.688 UTC [49] STATEMENT:  INSERT INTO log (dttm, event, owner, extra) VALUES ...
```

---

## ✅ Giải Pháp Fix

### Option 1: Tự động Fix bằng Python Script (Khuyến Nghị)

```bash
# Chạy script kiểm tra và auto-fix
python scripts/verify_services.py
```

Script này sẽ:
1. ✓ Kiểm tra Docker Compose
2. ✓ Kiểm tra trạng thái tất cả services
3. ✓ Kiểm tra PostgreSQL connection
4. ✓ Kiểm tra Redis connection
5. ✓ Kiểm tra Airflow API
6. ✗ Nếu phát hiện lỗi, tự động fix:
   - Chạy `airflow db migrate`
   - Restart các services

---

### Option 2: Manual Fix

#### Step 1: Kiểm tra trạng thái services
```bash
docker-compose ps
```

**Kỳ vọng Output:**
- postgres: Up
- redis: Up
- airflow-apiserver: Up
- airflow-scheduler: Up
- airflow-worker: Up
- airflow-triggerer: Up
- airflow-dag-processor: Up

#### Step 2: Kiểm tra PostgreSQL
```bash
# Test connection
docker-compose exec postgres pg_isready -U postgres

# Liệt kê databases
docker-compose exec postgres psql -U postgres -l

# Kiểm tra tables trong airflow database
docker-compose exec postgres psql -U airflow -d airflow -c '\dt'
```

#### Step 3: Nếu Airflow tables chưa được tạo
```bash
# Chạy Airflow database initialization
docker-compose run --rm airflow-init
```

Điều này sẽ:
- Chạy database migrations
- Tạo tất cả required tables
- Tạo admin user (mặc định: admin/admin)

#### Step 4: Restart services
```bash
# Restart all Airflow services
docker-compose restart airflow-apiserver
docker-compose restart airflow-scheduler
docker-compose restart airflow-worker
docker-compose restart airflow-triggerer
docker-compose restart airflow-dag-processor
```

Hoặc restart tất cả cùng lúc:
```bash
docker-compose restart
```

---

## 📊 Kiểm Tra Lại

### Sau khi fix, chạy lại verification:
```bash
python scripts/verify_services.py
```

### Kiểm tra Airflow Web UI:
```
http://localhost:8080
Default username: airflow
Default password: airflow
```

### Kiểm tra logs:
```bash
# Airflow worker
docker-compose logs -f airflow-worker

# PostgreSQL
docker-compose logs -f postgres

# Redis
docker-compose logs -f redis

# API (Firecrawl)
docker-compose logs -f api
```

---

## 🔧 Các Lỗi Phổ Biến Khác

### 1. **`ERROR: relation "user" does not exist`**
- **Nguyên nhân:** Airflow tables chưa được create
- **Fix:** Chạy `docker-compose run --rm airflow-init`

### 2. **`connection refused` - Postgres**
- **Nguyên nhân:** PostgreSQL container chưa sẵn sàng
- **Fix:** 
  ```bash
  docker-compose restart postgres
  sleep 10
  docker-compose run --rm airflow-init
  ```

### 3. **`AIRFLOW_UID not set`** (Linux)
- **Nguyên nhân:** UID của airflow user chưa được set
- **Fix:**
  ```bash
  echo -e "AIRFLOW_UID=$(id -u)" > .env
  docker-compose restart
  ```

### 4. **`Worker not responding to pings`**
- **Nguyên nhân:** Redis hoặc connection issue
- **Fix:**
  ```bash
  docker-compose restart redis
  docker-compose restart airflow-worker
  ```

### 5. **`Firecrawl API not responding`**
- **Nguyên nhân:** Playwright service hoặc API service chưa sẵn sàng
- **Fix:**
  ```bash
  docker-compose restart playwright-service
  docker-compose restart api
  sleep 30
  curl http://localhost:3002/health
  ```

---

## 🚀 Full Reset (Nuclear Option)

Nếu vẫn không work sau tất cả các bước trên:

```bash
# 1. Stop tất cả services
docker-compose down

# 2. Remove volumes (THIS WILL DELETE ALL DATA!)
docker-compose down -v

# 3. Rebuild images
docker-compose build

# 4. Start fresh
docker-compose up -d

# 5. Wait for services to be ready
sleep 30

# 6. Initialize Airflow
docker-compose run --rm airflow-init

# 7. Check status
python scripts/verify_services.py
```

---

## 📝 Monitoring & Health Checks

### Real-time logs monitoring:
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f airflow-worker

# Follow only errors
docker-compose logs -f | grep -i error
```

### Service health:
```bash
# Docker health status
docker-compose exec postgres pg_isready
docker-compose exec redis redis-cli ping
docker-compose exec -T airflow-apiserver curl -s localhost:8080/api/v2/version
```

---

## 🔗 Useful Links

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [Docker Compose Reference](https://docs.docker.com/compose/compose-file/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Redis Documentation](https://redis.io/documentation)

---

**Last Updated:** 2025-11-10
**Version:** 1.0

