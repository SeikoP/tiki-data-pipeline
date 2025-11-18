# ⚙️ 04-CONFIGURATION - THIẾT LẬP & CẤU HÌNH

**Thư mục này chứa**: Hướng dẫn cấu hình Redis, PostgreSQL, HTTP client

---

## 📁 FILE STRUCTURE

| File | Mô Tả | Sử Dụng Khi |
|------|--------|-----------|
| `CACHE_CONFIGURATION.md` | 🔴 Redis cache setup | Cấu hình caching |
| `REDIS_USAGE.md` | 📌 Redis chi tiết | Hiểu Redis usage |
| `README.md` | 📌 File này | Overview |

---

## 🎯 QUICK START

### Bạn muốn...

| Mục Đích | Đọc File |
|---------|----------|
| Setup Redis cache | `CACHE_CONFIGURATION.md` |
| Hiểu Redis usage | `REDIS_USAGE.md` |

---

## 🔧 CONFIGURATION LAYERS

### Layer 1: Docker Compose (Container Setup)
```yaml
# docker-compose.yaml

redis:
  image: redis:7-alpine
  ports:
    - "6379:6379"
  environment:
    - MAXMEMORY=2gb
    - MAXMEMORY_POLICY=allkeys-lru

postgres:
  image: postgres:15
  ports:
    - "5432:5432"
  environment:
    - POSTGRES_DB=crawl_data
    - MAX_CONNECTIONS=100

airflow-scheduler:
  depends_on:
    - redis
    - postgres
  environment:
    - AIRFLOW__CORE__EXECUTOR=CeleryExecutor
    - AIRFLOW__CELERY__BROKER_URL=redis://redis:6379/0
```

### Layer 2: Environment Variables (.env)
```bash
# .env file
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=crawl_data

REDIS_HOST=redis
REDIS_PORT=6379
REDIS_DB_CACHE=1

AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql://...
AIRFLOW__CELERY__BROKER_URL=redis://redis:6379/0
```

### Layer 3: Python Configuration (src/pipelines/crawl/config.py)
```python
# Code-level defaults
PRODUCT_BATCH_SIZE = 12
CATEGORY_TIMEOUT = 120
HTTP_TIMEOUT_TOTAL = 20
SELENIUM_POOL_SIZE = 15
REDIS_POOL_SIZE = 20
DB_POOL_MIN = 2
DB_POOL_MAX = 20
```

### Layer 4: Airflow Variables (Dynamic)
```
Admin → Variables (UI override)

Variable              | Value  | Type
---------------------|--------|-------
TIKI_DETAIL_POOL_SIZE| 15     | Airflow
TIKI_CATEGORY_TIMEOUT| 120s   | Airflow
TIKI_PRODUCTS_PER_DAY| 280    | Airflow
```

---

## 📊 CONNECTION POOLING CONFIGURATION

### PostgreSQL Pool

```python
# ThreadedConnectionPool configuration
from psycopg2.pool import ThreadedConnectionPool

pool = ThreadedConnectionPool(
    minconn=2,          # Min idle connections
    maxconn=20,         # Max connections
    host='localhost',
    port=5432,
    database='crawl_data',
    user='airflow',
    password='airflow',
    connect_timeout=5
)

# Connection reuse benefits:
# - 78% reuse rate (vs 22% creation rate)
# - 67% reduction in connection creation overhead
# - Stable performance across runs
```

### Redis Pool

```python
# Redis connection pool
import redis

redis_pool = redis.ConnectionPool(
    host='redis',
    port=6379,
    db=1,
    max_connections=20,
    socket_connect_timeout=5,
    socket_keepalive=True
)

redis_client = redis.Redis(connection_pool=redis_pool)

# Cache performance:
# - Hit ratio: 34-42% on subsequent runs
# - Memory: 1.2 GB allocated
# - TTL: 1 hour per cache entry
```

### HTTP Client Pool (aiohttp)

```python
# aiohttp TCPConnector configuration
import aiohttp

connector = aiohttp.TCPConnector(
    limit=100,                    # Total connections
    limit_per_host=10,           # Per-host limit
    ssl=False,                   # For Tiki HTTP
    keepalive_timeout=15,
    ttl_dns_cache=300
)

session = aiohttp.ClientSession(connector=connector)

# Performance improvements:
# - Connection reuse prevents bottleneck
# - Per-host limit prevents 429 Too Many Requests
# - DNS caching reduces lookup time
```

---

## 🚀 CONFIGURATION PROFILES

### Profile 1: FAST (Aggressive)
```python
# src/pipelines/crawl/config.py (FAST profile)

SELENIUM_POOL_SIZE = 20           # Aggressive
PRODUCT_BATCH_SIZE = 10           # Smaller batches
CATEGORY_CONCURRENT_REQUESTS = 8  # High concurrency
HTTP_CONNECTOR_LIMIT = 150        # High limit
HTTP_TIMEOUT_TOTAL = 15           # Aggressive timeout
PRODUCT_TIMEOUT = 45              # Aggressive
RETRY_COUNT = 0                   # No retry

# Expected: 5-10 min
# Risk: Rate limit, OOM
```

### Profile 2: BALANCED (Recommended)
```python
# src/pipelines/crawl/config.py (BALANCED profile)

SELENIUM_POOL_SIZE = 15           # Moderate
PRODUCT_BATCH_SIZE = 12           # Default
CATEGORY_CONCURRENT_REQUESTS = 5  # Moderate
HTTP_CONNECTOR_LIMIT = 100        # Default
HTTP_TIMEOUT_TOTAL = 20           # Default
PRODUCT_TIMEOUT = 60              # Default
RETRY_COUNT = 1                   # Single retry

# Expected: 12-15 min
# Risk: Low
```

### Profile 3: SAFE (Conservative)
```python
# src/pipelines/crawl/config.py (SAFE profile)

SELENIUM_POOL_SIZE = 8            # Conservative
PRODUCT_BATCH_SIZE = 15           # Larger batches
CATEGORY_CONCURRENT_REQUESTS = 3  # Low concurrency
HTTP_CONNECTOR_LIMIT = 50         # Low limit
HTTP_TIMEOUT_TOTAL = 30           # Conservative
PRODUCT_TIMEOUT = 90              # Conservative
RETRY_COUNT = 2                   # Double retry

# Expected: 20-25 min
# Risk: Very low
```

---

## 🔐 SECURITY CONFIGURATION

### Credential Management

```bash
# ✅ GOOD: Use environment variables
POSTGRES_USER=${POSTGRES_USER}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD}

# ❌ BAD: Hardcode in code
POSTGRES_USER="airflow"
POSTGRES_PASSWORD="airflow123"  # NEVER!

# ✅ GOOD: Use .env file (gitignored)
# .env file (in .gitignore)
POSTGRES_USER=airflow
POSTGRES_PASSWORD=secure_password_123

# ❌ BAD: Commit .env file
git add .env  # NEVER!
```

### SSL/TLS Configuration

```python
# For production PostgreSQL over network
import ssl

ssl_context = ssl.create_default_context(
    cafile="path/to/ca-cert.pem"
)

connection = psycopg2.connect(
    host="db.example.com",
    port=5432,
    user="airflow",
    password="password",
    sslmode="verify-full",
    sslcert="path/to/client-cert.pem",
    sslkey="path/to/client-key.pem"
)
```

---

## 📋 CONFIGURATION CHECKLIST

- [ ] `.env` file created from `.env.example`
- [ ] PostgreSQL credentials set
- [ ] Redis host configured
- [ ] Connection pools tuned
- [ ] Timeouts reviewed
- [ ] Retry logic configured
- [ ] Cache TTL set
- [ ] SSL/TLS enabled (if needed)
- [ ] Monitoring configured
- [ ] Backups scheduled

---

## 🚨 COMMON CONFIGURATION ISSUES

| Issue | Cause | Solution |
|-------|-------|----------|
| `Connection timeout` | Pool too small | Increase maxconn |
| `429 Too Many Requests` | High concurrency | Reduce limit_per_host |
| `Out of Memory` | Pool too large | Reduce maxconn |
| `Cache miss` | Wrong Redis DB | Use DB 1, not 0 |
| `Slow queries` | Missing index | Add index on category_id |
| `Selenium crash` | Too many drivers | Reduce pool size |

---

## ✅ NEXT STEPS

- [ ] Apply recommended settings
- [ ] Test each profile (FAST/BALANCED/SAFE)
- [ ] Monitor performance metrics
- [ ] Document any custom configurations
- [ ] Setup alerts for connection pool usage

---

**Last Updated**: 18/11/2025  
**Status**: ✅ Configuration Guide Complete
