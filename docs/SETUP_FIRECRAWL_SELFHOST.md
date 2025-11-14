# Hướng dẫn Setup Firecrawl Self-Host

Hướng dẫn này sẽ giúp bạn setup Firecrawl self-host để crawl product detail từ Tiki.vn với AI extraction.

## Yêu cầu

- Docker và Docker Compose
- Python 3.8+
- OpenAI API Key (để enable AI features)

## Bước 1: Setup Firecrawl Self-Host

### 1.1. Di chuyển vào thư mục Firecrawl

```bash
cd firecrawl
```

### 1.2. Tạo file .env

Tạo file `.env` trong thư mục `firecrawl`:

```bash
# ===== Required ENVS ======
PORT=3002
HOST=0.0.0.0

# ===== AI features (Required cho extract với schema) =====
# Cung cấp OpenAI API key để enable AI features
OPENAI_API_KEY=sk-your-openai-api-key

# Hoặc sử dụng Ollama (experimental)
# OLLAMA_BASE_URL=http://localhost:11434/api
# MODEL_NAME=deepseek-r1:7b
# MODEL_EMBEDDING_NAME=nomic-embed-text

# ===== Optional ENVS =====
USE_DB_AUTHENTICATION=false
BULL_AUTH_KEY=CHANGEME
```

### 1.3. Build và chạy Docker containers

```bash
docker compose build
docker compose up -d
```

Firecrawl sẽ chạy tại: `http://localhost:3002`

### 1.4. Kiểm tra Firecrawl đang chạy

```bash
curl http://localhost:3002/health
```

Hoặc mở trình duyệt: `http://localhost:3002/admin/CHANGEME/queues` để xem Bull Queue Manager UI.

## Bước 2: Cài đặt Python Dependencies

```bash
pip install firecrawl-py tqdm
```

## Bước 3: Test kết nối

Chạy test script:

```bash
python tests/test_crawl_product_detail_firecrawl.py
```

## Bước 4: Chạy crawl product detail

### 4.1. Set environment variables (optional)

```bash
# Windows PowerShell
$env:FIRECRAWL_API_URL="http://localhost:3002"
$env:FIRECRAWL_API_KEY=""  # Optional cho self-host

# Linux/Mac
export FIRECRAWL_API_URL="http://localhost:3002"
export FIRECRAWL_API_KEY=""  # Optional cho self-host
```

### 4.2. Chạy script crawl

```bash
python src/pipelines/crawl/crawl_product_detail_firecrawl_selfhost.py
```

## Cấu hình

### Thay đổi số lượng products

Sửa trong `main()` function:

```python
max_products = 10  # Số sản phẩm tối đa
max_workers = 2    # Số thread song song
```

### Thay đổi API URL

Nếu Firecrawl chạy ở port khác:

```python
api_url = "http://localhost:3003"  # Thay đổi port
```

## Troubleshooting

### Lỗi: "Không thể kết nối Firecrawl"

**Nguyên nhân:** Firecrawl chưa chạy hoặc sai URL

**Giải pháp:**
1. Kiểm tra Firecrawl đang chạy:
   ```bash
   docker compose ps
   ```
2. Kiểm tra logs:
   ```bash
   docker compose logs api
   ```
3. Đảm bảo port 3002 không bị chiếm:
   ```bash
   netstat -an | findstr 3002  # Windows
   lsof -i :3002               # Linux/Mac
   ```

### Lỗi: "OpenAI API key required"

**Nguyên nhân:** Chưa set OPENAI_API_KEY trong .env

**Giải pháp:**
1. Thêm vào file `firecrawl/.env`:
   ```
   OPENAI_API_KEY=sk-your-key
   ```
2. Restart containers:
   ```bash
   docker compose restart
   ```

### Lỗi: "Extraction timeout"

**Nguyên nhân:** Trang web load chậm hoặc AI processing lâu

**Giải pháp:**
1. Tăng timeout trong script:
   ```python
   timeout=600  # 10 phút
   ```
2. Giảm số workers:
   ```python
   max_workers = 1
   ```

### Lỗi: "ModuleNotFoundError: No module named 'firecrawl'"

**Giải pháp:**
```bash
pip install firecrawl-py
```

## Cấu trúc dữ liệu output

File output: `data/demo/products/detail/products_detail.json`

```json
{
  "total_products": 10,
  "total_crawled": 10,
  "total_success": 9,
  "total_failed": 1,
  "crawled_at": "2025-11-14 20:00:00",
  "products": [
    {
      "product_id": "198454009",
      "name": "Product name",
      "url": "https://tiki.vn/p/198454009",
      "detail": {
        "price": 1500000,
        "original_price": 2000000,
        "discount_percent": 25,
        "rating": 4.5,
        "review_count": 123,
        "sales_count": 456,
        "stock_status": "in_stock",
        "brand": "JOYO",
        "seller": "Tiki Trading",
        "description": "Mô tả sản phẩm...",
        "specifications": {
          "Công suất": "40W",
          "Kích thước": "30x20x15 cm"
        },
        "images": [
          "https://example.com/image1.jpg"
        ],
        "warranty": "12 tháng",
        "shipping_info": "Miễn phí vận chuyển"
      },
      "detail_crawled_at": "2025-11-14 20:00:00"
    }
  ]
}
```

## Tối ưu hóa

### 1. Sử dụng cache

Script tự động cache kết quả trong `data/demo/products/detail/cache/`. 
Nếu chạy lại, sẽ không crawl lại các products đã có cache.

### 2. Resume từ vị trí bất kỳ

```python
start_from = 10  # Bắt đầu từ product thứ 11
```

### 3. Batch processing

Chia nhỏ file products và chạy song song nhiều instances.

## Lưu ý

- Firecrawl self-host không có rate limit như cloud version
- Cần có OpenAI API key để sử dụng AI extraction
- Mỗi extraction có thể mất 30-60 giây
- Giới hạn số workers để tránh quá tải server

