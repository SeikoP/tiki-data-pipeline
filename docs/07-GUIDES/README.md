# 📖 07-GUIDES - HƯỚNG DẪN & TÀI LIỆU

**Thư mục này chứa**: Setup guides, test guides, field documentation

---

## 📁 FILE STRUCTURE

| File | Mô Tả | Sử Dụng Khi |
|------|--------|-----------|
| `TEST_DAG_GUIDE.md` | 🧪 DAG testing | Test tuning |
| `products_final_fields_vi.md` | 📋 Product fields | Hiểu dữ liệu |
| `README.md` | 📌 File này | Overview |

---

## 🎯 QUICK START

### Bạn muốn...

| Mục Đích | Đọc File |
|---------|----------|
| Test DAG | `TEST_DAG_GUIDE.md` |
| Understand fields | `products_final_fields_vi.md` |

---

## 🧪 TESTING GUIDE

### Unit Tests

```bash
# Run all unit tests
python -m pytest tests/

# Run specific test file
python -m pytest tests/test_crawl_products.py

# Run with coverage
python -m pytest --cov=src tests/

# Run with verbose output
python -m pytest -v tests/
```

### Integration Tests

```bash
# Test E2E flow locally
python demos/demo_e2e_full.py

# Test individual steps
python src/pipelines/crawl/crawl_products.py
python src/pipelines/transform/transformer.py
python src/pipelines/load/loader.py
```

### DAG Tests

```bash
# Test DAG parsing (no syntax errors)
airflow dags list

# Test specific DAG
airflow dags validate tiki_crawl_products_dag.py

# Trigger DAG with test config
airflow dags trigger tiki_crawl_products \
  --conf '{"TIKI_PRODUCTS_PER_DAY": 50}'
```

---

## 📊 PRODUCT FIELDS

### Schema Overview

```
Product (JSONB)
├─ Identification
│  ├─ tiki_id: str (unique)
│  ├─ name: str (product name)
│  ├─ url: str (product URL)
│  └─ category_id: int (FK categories)
│
├─ Pricing
│  ├─ price: float (current price)
│  ├─ original_price: float (before discount)
│  ├─ discount_percent: float (calculated)
│  └─ currency: str (VND)
│
├─ Reviews & Ratings
│  ├─ rating: float (0-5 stars)
│  ├─ review_count: int (number of reviews)
│  ├─ rating_distribution: dict (breakdown)
│  └─ verified_buyers: int (count)
│
├─ Availability
│  ├─ in_stock: bool (true if available)
│  ├─ stock_count: int (approximate)
│  ├─ status: str (active/inactive/discontinued)
│  └─ seller_id: int (seller reference)
│
├─ Metadata
│  ├─ crawl_timestamp: datetime (when crawled)
│  ├─ last_updated: datetime (last change)
│  ├─ active: bool (data validity)
│  └─ source: str ("tiki_vn")
│
└─ Derived/Computed
   ├─ popularity_score: float (rating * review_count)
   ├─ is_premium: bool (rating > 4.5 and reviews > 100)
   ├─ discount_category: str (cheap/medium/expensive)
   └─ recommendation_score: float (ML score)
```

---

## 🚀 DEPLOYMENT GUIDE

### Development Environment

```bash
# 1. Clone repository
git clone <repo-url>
cd tiki-data-pipeline

# 2. Create .env from template
cp .env.example .env
# Edit .env with your credentials

# 3. Build Docker images
docker-compose build

# 4. Start services
docker-compose up -d

# 5. Verify services
docker-compose ps

# 6. Access Airflow UI
open http://localhost:8080
```

### Production Environment

```bash
# 1. Use production .env
cp .env.prod .env

# 2. Build with production tag
docker-compose -f docker-compose.prod.yaml build

# 3. Start with production settings
docker-compose -f docker-compose.prod.yaml up -d

# 4. Setup monitoring
# (See 05-PERFORMANCE docs)

# 5. Configure backups
# (See scripts/backup-postgres.ps1)
```

---

## 🔧 TROUBLESHOOTING GUIDE

### Common Issues

| Issue | Symptom | Solution |
|-------|---------|----------|
| Airflow can't import DAG | `DAG parse failed` | Check Python path in docker-compose |
| PostgreSQL connection error | `Connection refused` | Check `.env` credentials, restart postgres |
| Redis cache miss | `No cache data` | Check Redis DB (should be DB 1) |
| Selenium timeout | `WebDriver timeout` | Increase SELENIUM_POOL_SIZE or reduce batch |
| Memory OOM error | `Killed (exit 137)` | Reduce pool sizes or cache limits |
| Rate limiting 429 | `Too Many Requests` | Reduce HTTP_CONNECTOR_LIMIT_PER_HOST |

### Debug Commands

```bash
# Check service logs
docker-compose logs airflow-scheduler
docker-compose logs postgres
docker-compose logs redis

# Verify database connection
psql -h localhost -U airflow -d crawl_data -c "SELECT COUNT(*) FROM products;"

# Check Redis cache
redis-cli -n 1 DBSIZE

# Test Selenium driver
python -c "from selenium import webdriver; d = webdriver.Chrome(); print('OK')"

# Monitor Docker resources
docker stats

# SSH into container
docker exec -it tiki-data-pipeline-airflow-scheduler-1 /bin/bash
```

---

## 📋 FIELD REFERENCE

### Pricing Fields

| Field | Type | Example | Notes |
|-------|------|---------|-------|
| price | float | 299000 | Current price in VND |
| original_price | float | 399000 | Price before discount |
| discount_percent | float | 25.1 | Calculated: (1-price/original)*100 |
| currency | str | "VND" | Always VND for Tiki |

### Rating Fields

| Field | Type | Example | Notes |
|-------|------|---------|-------|
| rating | float | 4.5 | 0-5 stars, aggregated |
| review_count | int | 1240 | Total number of reviews |
| rating_distribution | dict | {5:800, 4:300, ...} | Breakdown by star |
| verified_buyers | int | 950 | Reviews from verified purchasers |

### Status Fields

| Field | Type | Values | Notes |
|-------|------|--------|-------|
| in_stock | bool | true/false | Current availability |
| stock_count | int | 1-1000 | Approximate quantity |
| status | str | active/inactive/discontinued | Product status |
| seller_id | int | 1-10000 | Seller identifier |

---

## ✅ DEPLOYMENT CHECKLIST

- [ ] Clone repository
- [ ] Copy and configure .env
- [ ] Build Docker images
- [ ] Start services (docker-compose up)
- [ ] Verify all services running
- [ ] Test database connection
- [ ] Test Redis connection
- [ ] Trigger test DAG
- [ ] Verify data in database
- [ ] Setup monitoring
- [ ] Configure backups
- [ ] Document custom changes

---

## 📚 ADDITIONAL RESOURCES

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Redis Documentation](https://redis.io/documentation)
- [Selenium Documentation](https://www.selenium.dev/documentation/)
- [Docker Documentation](https://docs.docker.com/)

---

**Last Updated**: 18/11/2025  
**Status**: ✅ Guides Available
