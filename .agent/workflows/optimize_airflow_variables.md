---
description: Optimize Airflow Variables for Tiki Crawler
---

# Tối ưu hóa biến môi trường Airflow cho Tiki Crawler

Để đạt tốc độ crawl tối đa trên máy Local (16GB RAM) với Selenium, bạn cần cấu hình các biến sau trong Airflow UI -> Admin -> Variables.

## 1. Cấu hình Performance (Quan trọng)

| Key | Value | Mô tả |
| :--- | :--- | :--- |
| `TIKI_PRODUCTS_PER_DAY` | `500` | Số lượng sản phẩm tối đa crawl mỗi lần chạy. Phù hợp cho máy cấu hình thấp. |
| `TIKI_DETAIL_POOL_SIZE` | `2` | Số lượng Chrome Driver tối đa cho mỗi Task. Giảm xuống 2 để tiết kiệm RAM. |
| `TIKI_DETAIL_MAX_CONCURRENT_TASKS` | `4` | Số lượng request song song. Gấp đôi Pool Size. |
| `TIKI_DETAIL_RATE_LIMIT_DELAY` | `0.1` | Giảm delay giữa các request xuống 0.1s. |

## 2. Cấu hình Timeout & Retry

| Key | Value | Mô tả |
| :--- | :--- | :--- |
| `TIKI_DETAIL_CRAWL_TIMEOUT` | `180` | Thời gian tối đa (giây) cho 1 sản phẩm. |
| `TIKI_CIRCUIT_BREAKER_FAILURE_THRESHOLD` | `10` | Số lỗi cho phép trước khi ngắt mạch. |

## 3. Cách thêm Variable nhanh bằng CLI

Bạn có thể chạy lệnh sau trong terminal để set nhanh các biến này:

```bash
docker-compose exec airflow-webserver airflow variables set TIKI_PRODUCTS_PER_DAY 500
docker-compose exec airflow-webserver airflow variables set TIKI_DETAIL_POOL_SIZE 2
docker-compose exec airflow-webserver airflow variables set TIKI_DETAIL_MAX_CONCURRENT_TASKS 4
docker-compose exec airflow-webserver airflow variables set TIKI_DETAIL_RATE_LIMIT_DELAY 0.1
```
