# Đánh Giá Khả Năng Chuyển Sang JavaScript cho Crawl

## 📋 Tổng Quan Dự Án Hiện Tại

### Tech Stack:
- **Orchestration**: Apache Airflow 3.1.2 (Python-based)
- **Crawling**: Selenium WebDriver + BeautifulSoup4 (Python)
- **Database**: PostgreSQL 16
- **Cache**: Redis 7.2
- **Containerization**: Docker + Docker Compose
- **Language**: Python 3.8+

### Cấu Trúc Dự Án:
```
src/pipelines/crawl/
├── crawl_categories_optimized.py      # Crawl categories
├── crawl_products.py                  # Crawl product list
├── crawl_products_detail.py          # Crawl product details
├── extract_category_link_selenium.py # Selenium utilities
├── utils.py                          # Shared utilities
├── storage/                          # Redis, PostgreSQL storage
├── resilience/                       # Error handling, circuit breaker
└── utils/                            # Batch processing
```

---

## ✅ CÓ THỂ CHUYỂN SANG JAVASCRIPT

### Kết Luận: **CÓ THỂ**, nhưng cần lưu ý một số điểm

---

## 🎯 Các Phương Án Migration

### Phương Án 1: Hybrid Approach (Khuyến Nghị) ⭐⭐⭐⭐⭐

**Giữ Airflow (Python) + Crawl bằng Node.js**

#### Cách thực hiện:

1. **Viết crawler bằng Node.js**:
   ```javascript
   // src/pipelines/crawl-js/
   ├── crawl-categories.js
   ├── crawl-products.js
   ├── crawl-product-detail.js
   └── utils.js
   ```

2. **Tích hợp với Airflow qua DockerOperator hoặc BashOperator**:
   ```python
   # Trong Airflow DAG
   from airflow.providers.docker.operators.docker import DockerOperator
   
   crawl_task = DockerOperator(
       task_id='crawl_products',
       image='node:20-alpine',
       command='node /opt/airflow/src/pipelines/crawl-js/crawl-products.js',
       docker_url='unix://var/run/docker.sock',
       network_mode='bridge',
       volumes=[
           '/path/to/src:/opt/airflow/src',
           '/path/to/data:/opt/airflow/data'
       ]
   )
   ```

#### Ưu điểm:
- ✅ Giữ nguyên Airflow infrastructure
- ✅ Tận dụng tốc độ Node.js cho crawl
- ✅ Không cần thay đổi database, Redis
- ✅ Có thể migrate từng phần (crawl trước, transform/load sau)

#### Nhược điểm:
- ⚠️ Cần quản lý 2 ngôn ngữ
- ⚠️ Debug phức tạp hơn (2 environments)

#### Effort: **2-3 tuần**

---

### Phương Án 2: Full Migration (Thay thế hoàn toàn)

**Thay Airflow bằng Node.js orchestrator (Temporal, BullMQ, hoặc custom)**

#### Cách thực hiện:

1. **Chọn orchestrator**:
   - **Temporal**: Workflow engine mạnh mẽ, có TypeScript SDK
   - **BullMQ**: Job queue với Redis
   - **Custom**: Express.js + cron jobs

2. **Viết lại toàn bộ pipeline**:
   ```javascript
   // src/pipelines/
   ├── crawl/          // Node.js crawlers
   ├── transform/      // Node.js transformers
   ├── load/           // Node.js loaders
   └── orchestration/  // Temporal/BullMQ workflows
   ```

#### Ưu điểm:
- ✅ Tốc độ tối đa (toàn bộ bằng Node.js)
- ✅ Đơn giản hóa stack (chỉ 1 ngôn ngữ)
- ✅ Dễ maintain và scale

#### Nhược điểm:
- ⚠️ Mất Airflow UI và ecosystem
- ⚠️ Cần học orchestrator mới
- ⚠️ Effort lớn (4-6 tuần)

#### Effort: **4-6 tuần**

---

### Phương Án 3: Microservices Approach

**Tách crawl thành service riêng (Node.js), giữ Airflow cho orchestration**

#### Cách thực hiện:

1. **Tạo Node.js service**:
   ```javascript
   // crawl-service/
   ├── server.js          // Express API
   ├── routes/
   │   ├── categories.js
   │   ├── products.js
   │   └── product-detail.js
   └── Dockerfile
   ```

2. **Airflow gọi qua HTTP**:
   ```python
   from airflow.providers.http.operators.http import SimpleHttpOperator
   
   crawl_task = SimpleHttpOperator(
       task_id='crawl_products',
       http_conn_id='crawl_service',
       endpoint='/api/crawl/products',
       method='POST',
       data=json.dumps({'category_url': '...'})
   )
   ```

#### Ưu điểm:
- ✅ Tách biệt concerns
- ✅ Có thể scale crawl service độc lập
- ✅ Dễ test và debug
- ✅ Có thể reuse cho projects khác

#### Nhược điểm:
- ⚠️ Thêm network overhead
- ⚠️ Cần quản lý thêm service

#### Effort: **3-4 tuần**

---

## 📊 So Sánh Các Phương Án

| Tiêu chí | Hybrid | Full Migration | Microservices |
|----------|--------|----------------|---------------|
| **Effort** | ⭐⭐ 2-3 tuần | ⭐⭐⭐⭐ 4-6 tuần | ⭐⭐⭐ 3-4 tuần |
| **Risk** | ⭐⭐ Thấp | ⭐⭐⭐⭐ Cao | ⭐⭐⭐ Trung bình |
| **Performance** | ⭐⭐⭐⭐ Tốt | ⭐⭐⭐⭐⭐ Tốt nhất | ⭐⭐⭐⭐ Tốt |
| **Maintainability** | ⭐⭐⭐ Trung bình | ⭐⭐⭐⭐⭐ Tốt nhất | ⭐⭐⭐⭐ Tốt |
| **Flexibility** | ⭐⭐⭐ Trung bình | ⭐⭐⭐⭐⭐ Tốt nhất | ⭐⭐⭐⭐⭐ Tốt nhất |
| **Khuyến nghị** | ✅ **Nên dùng** | ⚠️ Chỉ nếu cần | ✅ Tốt cho scale |

---

## 🔧 Chi Tiết Implementation - Hybrid Approach (Khuyến Nghị)

### Bước 1: Setup Node.js trong Docker

**Tạo Dockerfile cho Node.js crawler**:
```dockerfile
# Dockerfile.crawler
FROM node:20-alpine

WORKDIR /app

# Install dependencies
COPY package.json package-lock.json ./
RUN npm ci --only=production

# Copy source code
COPY src/pipelines/crawl-js ./src

# Install Puppeteer dependencies
RUN apk add --no-cache \
    chromium \
    nss \
    freetype \
    freetype-dev \
    harfbuzz \
    ca-certificates \
    ttf-freefont

# Set Puppeteer to use installed Chromium
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium-browser

CMD ["node", "src/crawl-products.js"]
```

### Bước 2: Viết Crawler bằng Node.js

**crawl-product-detail.js**:
```javascript
const puppeteer = require('puppeteer');
const fs = require('fs').promises;
const path = require('path');

async function crawlProductDetail(url, options = {}) {
  const {
    timeout = 30000,
    useCache = true,
    useRateLimit = true,
    rateLimitDelay = 1000
  } = options;

  // Check cache
  if (useCache) {
    const cached = await getCached(url);
    if (cached) return cached;
  }

  // Rate limiting
  if (useRateLimit) {
    await rateLimit(rateLimitDelay);
  }

  const browser = await puppeteer.launch({
    headless: true,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu'
    ]
  });

  try {
    const page = await browser.newPage();
    await page.goto(url, { waitUntil: 'networkidle2', timeout });
    await page.waitForTimeout(2000);

    // Scroll to load lazy content
    await page.evaluate(() => window.scrollTo(0, 500));
    await page.waitForTimeout(500);
    await page.evaluate(() => window.scrollTo(0, 1500));
    await page.waitForTimeout(500);
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(1000);

    const html = await page.content();
    
    // Cache result
    if (useCache) {
      await cacheResult(url, html);
    }

    return html;
  } finally {
    await browser.close();
  }
}

// Batch crawling với concurrency
async function crawlProductsBatch(urls, concurrency = 200) {
  const pLimit = require('p-limit');
  const limit = pLimit(concurrency);

  const results = await Promise.all(
    urls.map(url =>
      limit(async () => {
        try {
          return await crawlProductDetail(url);
        } catch (error) {
          console.error(`Error crawling ${url}:`, error);
          return null;
        }
      })
    )
  );

  return results.filter(r => r !== null);
}

module.exports = { crawlProductDetail, crawlProductsBatch };
```

### Bước 3: Tích hợp với Airflow

**Cập nhật DAG**:
```python
from airflow.providers.docker.operators.docker import DockerOperator
from airflow import DAG

def create_crawl_dag():
    dag = DAG(
        'tiki_crawl_products_js',
        default_args={...},
        schedule_interval='@daily'
    )

    # Crawl categories (giữ Python hoặc chuyển sang Node.js)
    crawl_categories = PythonOperator(
        task_id='crawl_categories',
        python_callable=crawl_categories_python,  # Hoặc dùng DockerOperator
        dag=dag
    )

    # Crawl products với Node.js
    crawl_products = DockerOperator(
        task_id='crawl_products',
        image='tiki-crawler-node:latest',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        environment={
            'REDIS_URL': 'redis://redis:6379/1',
            'POSTGRES_URL': 'postgresql://user:pass@postgres:5432/crawl_data'
        },
        volumes=[
            '${AIRFLOW_PROJ_DIR}/src:/app/src',
            '${AIRFLOW_PROJ_DIR}/data:/app/data'
        ],
        command='node src/pipelines/crawl-js/crawl-products.js',
        dag=dag
    )

    # Transform và Load (giữ Python)
    transform_products = PythonOperator(
        task_id='transform_products',
        python_callable=transform_products_python,
        dag=dag
    )

    crawl_categories >> crawl_products >> transform_products
    return dag

dag = create_crawl_dag()
```

---

## 📦 Dependencies Cần Thiết

### Node.js Packages:
```json
{
  "name": "tiki-crawler",
  "version": "1.0.0",
  "dependencies": {
    "puppeteer": "^21.0.0",
    "p-limit": "^4.0.0",
    "redis": "^4.6.0",
    "pg": "^8.11.0",
    "cheerio": "^1.0.0",
    "axios": "^1.6.0"
  }
}
```

### Tương đương với Python:
| Python | Node.js |
|--------|---------|
| `selenium` | `puppeteer` hoặc `playwright` |
| `beautifulsoup4` | `cheerio` |
| `requests` | `axios` hoặc `node-fetch` |
| `redis` | `redis` |
| `psycopg2` | `pg` |
| `concurrent.futures` | `p-limit` |

---

## ⚠️ Những Điểm Cần Lưu Ý

### 1. **Airflow Integration**
- Airflow là Python-based, nhưng có thể gọi Node.js qua:
  - `DockerOperator`: Chạy Node.js trong container
  - `BashOperator`: Chạy `node script.js`
  - `SimpleHttpOperator`: Nếu dùng microservices

### 2. **Database & Redis**
- PostgreSQL và Redis có clients cho Node.js
- Không cần thay đổi database schema
- Có thể dùng chung connection pool

### 3. **Error Handling**
- Cần viết lại error handling logic
- Circuit breaker, retry logic cần implement lại

### 4. **Testing**
- Cần setup test environment cho Node.js
- Integration tests với Airflow

### 5. **Deployment**
- Cần build Docker image cho Node.js crawler
- Update docker-compose.yaml

---

## 🚀 Migration Roadmap (Hybrid Approach)

### Week 1: Setup & Proof of Concept
- [ ] Setup Node.js environment
- [ ] Viết crawler đơn giản (1 file)
- [ ] Test với 10-20 products
- [ ] So sánh tốc độ với Python

### Week 2: Full Crawler Implementation
- [ ] Viết lại tất cả crawl functions
- [ ] Implement Redis cache
- [ ] Implement PostgreSQL storage
- [ ] Error handling & retry logic

### Week 3: Integration & Testing
- [ ] Tích hợp với Airflow DAG
- [ ] Integration tests
- [ ] Performance testing
- [ ] Load testing với 1000+ products

### Week 4: Deployment & Monitoring
- [ ] Deploy to production
- [ ] Monitor performance
- [ ] Tune concurrency settings
- [ ] Documentation

---

## 📈 Expected Performance Improvement

### Với Hybrid Approach:

| Metric | Python (Hiện tại) | Node.js | Cải thiện |
|--------|------------------|---------|-----------|
| **Crawl 1 product** | 6.5-8s | 5-6s | ⚡ 20-30% |
| **11k products (8 threads)** | 23 phút | - | - |
| **11k products (200 concurrent)** | - | 3.7 phút | ⚡ **6.2x nhanh hơn** |

---

## ✅ Kết Luận

### Có thể chuyển sang JavaScript không?
**CÓ**, và nên làm theo **Hybrid Approach**:

1. ✅ **Giữ Airflow** cho orchestration (đã setup sẵn)
2. ✅ **Chuyển crawl sang Node.js** để tận dụng tốc độ
3. ✅ **Giữ transform/load bằng Python** (nếu cần pandas, numpy)
4. ✅ **Migrate từng phần** để giảm risk

### Khuyến nghị:
- **Bắt đầu với crawl-product-detail.js** (phần tốn thời gian nhất)
- **Test với sample nhỏ** (100-1000 products)
- **So sánh performance** trước khi migrate toàn bộ
- **Giữ Python code** làm backup trong giai đoạn transition

### Effort Estimate:
- **Hybrid Approach**: 2-3 tuần
- **Full Migration**: 4-6 tuần
- **Microservices**: 3-4 tuần

---

## 📝 Next Steps

1. **Quyết định phương án**: Hybrid / Full / Microservices
2. **Setup Node.js environment**: Docker, dependencies
3. **Viết POC**: Crawl 1 product detail với Node.js
4. **Benchmark**: So sánh với Python version
5. **Plan migration**: Timeline, resources, testing

