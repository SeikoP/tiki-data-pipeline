# Cấu hình Groq API cho Firecrawl

## Tổng quan

Firecrawl hỗ trợ sử dụng Groq API thay vì OpenAI cho AI extraction features. Project này đã được cấu hình để:
- Sử dụng Groq API với multiple keys
- Round-robin rotation để phân tải requests
- Thread-safe cho multi-threaded environment

## Cấu hình

### 1. Single API Key

Trong file `.env`:

```bash
# Groq API - Single key
GROQ_API_KEY=gsk_your_groq_api_key_here
```

### 2. Multiple API Keys (Round-Robin)

Để sử dụng nhiều keys luân phiên:

```bash
# Groq API - Multiple keys (comma-separated)
GROQ_API_KEYS=gsk_key1,gsk_key2,gsk_key3
```

### 3. Model Configuration

```bash
# Groq Model (optional, default: llama-3.1-70b-versatile)
GROQ_MODEL=llama-3.1-70b-versatile

# Hoặc các model khác:
# - llama-3.1-8b-instant
# - llama-3.1-70b-versatile
# - mixtral-8x7b-32768
# - gemma-7b-it
```

### 4. Firecrawl Configuration

Firecrawl sẽ tự động sử dụng Groq nếu:
- `GROQ_API_KEY` được set
- Hoặc `GROQ_API_KEYS` được set

Firecrawl sử dụng Groq qua environment variable `GROQ_API_KEY`. Khi có multiple keys, cần implement custom logic để rotate.

## Sử dụng trong Code

### Basic Usage

```python
from pipelines.crawl.tiki.groq_config import get_groq_api_key

# Lấy key tiếp theo (round-robin)
api_key = get_groq_api_key()
```

### Advanced Usage

```python
from pipelines.crawl.tiki.groq_config import get_groq_manager

manager = get_groq_manager()

# Lấy key
key = manager.get_key()

# Ghi nhận lỗi (nếu có)
manager.record_error(key)

# Xem stats
stats = manager.get_stats()
print(f"Total requests: {stats['total_requests']}")
print(f"Total errors: {stats['total_errors']}")
```

## Docker Compose

Các biến môi trường đã được thêm vào `docker-compose.yaml`:

```yaml
environment:
  GROQ_API_KEY: ${GROQ_API_KEY}
  GROQ_API_KEYS: ${GROQ_API_KEYS}
  GROQ_MODEL: ${GROQ_MODEL:-llama-3.1-70b-versatile}
```

## Testing

Chạy test script để kiểm tra:

```bash
python scripts/test_groq_config.py
```

## Lưu ý

1. **Rate Limits**: Groq có rate limits. Sử dụng multiple keys giúp tăng throughput.

2. **Key Rotation**: Round-robin rotation đảm bảo phân tải đều giữa các keys.

3. **Error Handling**: Nếu một key bị lỗi, có thể dùng `get_healthiest_key()` để lấy key có error rate thấp nhất.

4. **Thread Safety**: `GroqKeyManager` là thread-safe, an toàn cho multi-threaded environment.

## Ví dụ .env

```bash
# Groq Configuration
GROQ_API_KEYS=gsk_key1_here,gsk_key2_here,gsk_key3_here
GROQ_MODEL=llama-3.1-70b-versatile

# Firecrawl sẽ tự động detect và sử dụng Groq
```

