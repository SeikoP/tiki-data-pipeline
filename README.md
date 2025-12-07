# Tiki Data Pipeline 

## 1. Giới Thiệu
**Tiki Data Pipeline** là hệ thống ETL thu thập và xử lý dữ liệu sản phẩm từ Tiki.vn phục vụ phân tích và nghiên cứu Data Engineering. Mục tiêu: tự động hóa thu thập, chuẩn hóa và lưu trữ dữ liệu với độ tin cậy cao.

## 2. Mục Tiêu
- Xây dựng pipeline ETL hoàn chỉnh (Extract → Transform → Load)
- Tối ưu crawl & lưu trữ qua batching, caching, retry
- Nâng cao chất lượng dữ liệu (validation + computed fields)

## 2.1 Phạm Vi Hiện Tại
- Tập trung crawl duy nhất danh mục Nhà Cửa & Đời Sống (c1883): https://tiki.vn/nha-cua-doi-song/c1883
- Chỉ thu thập sản phẩm và chi tiết trong cây danh mục này.

## 3. Kiến Trúc Tổng Quan
```
Website → Crawl → Transform → Load → PostgreSQL (+ JSON backup)
```
Thành phần chính: Selenium + aiohttp (crawl), Python (transform), PostgreSQL (load), Airflow (orchestrate), Redis (cache/broker).

## 4. Thành Phần Chính
- `crawl_categories_recursive.py`: Danh mục đệ quy
- `crawl_products.py`: Sản phẩm theo danh mục
- `crawl_products_detail.py`: Chi tiết sản phẩm (giá, rating,...)
- `transformer.py`: Chuẩn hóa + tính `estimated_revenue`, `popularity_score`, `value_score`
- `loader.py`: Batch upsert vào PostgreSQL
- `tiki_crawl_products_dag.py`: Airflow DAG (Dynamic Task Mapping)

## 5. Công Nghệ
Python 3.8+, Selenium, aiohttp, BeautifulSoup4, PostgreSQL, Redis, Apache Airflow, Docker.

## 6. Hạn Chế & Hướng Phát Triển
Hạn chế: chưa real-time, phụ thuộc một máy, rate limiting từ nguồn. 
Tương lai: distributed crawling, event-driven updates, API truy xuất, ML phân loại nâng cao.

## 7. Báo cáo
- Dashboard Overview
    <img width="1515" height="853" alt="image" src="https://github.com/user-attachments/assets/2d22919f-f810-4643-ba03-f5579252c84c" />
- Dashboard Sản phẩm, Thương hiệu & Seller
    <img width="1516" height="855" alt="image" src="https://github.com/user-attachments/assets/224522ba-b05e-474e-a911-13095d4fdfb6" />
- Dashboard Danh mục & giá 
    <img width="1514" height="851" alt="image" src="https://github.com/user-attachments/assets/ee87f37e-a71c-4918-a2f9-3315b86a751e" />




