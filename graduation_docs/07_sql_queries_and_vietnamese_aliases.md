# SQL Query Cho Dashboard

File này dùng cho Power BI, export CSV và slide. Dashboard đọc trực tiếp từ:

```sql
analytics.product_analysis_ready
```

Không cần tạo thêm `mart_*`, `category_summary`, `seller_summary`, hoặc view có
tên cột tiếng Việt. Tên cột vật lý vẫn là English `snake_case`, còn giá trị của
các cột phân loại đã là tiếng Việt ngắn.

## 1. Query Phân Bố Segment

```sql
SELECT
    home_segment,
    COUNT(*) AS product_count,
    ROUND(AVG(price), 0) AS avg_price,
    ROUND(AVG(discount_percent), 2) AS avg_discount_percent,
    ROUND(AVG(rating_average), 2) AS avg_rating,
    SUM(review_count) AS total_reviews,
    SUM(sales_count) AS total_sold
FROM analytics.product_analysis_ready
GROUP BY home_segment
ORDER BY product_count DESC;
```

## 2. Query Top Product Theo Value Score

```sql
SELECT
    short_name,
    product_name,
    home_segment,
    seller_name,
    seller_type,
    price,
    discount_percent,
    rating_average,
    review_count,
    sales_count,
    value_score,
    market_position,
    product_url
FROM analytics.product_analysis_ready
ORDER BY value_score DESC NULLS LAST
LIMIT 20;
```

## 3. Query Chất Lượng Tín Hiệu Phân Tích

Đây không phải crawl quality. Query này chỉ xem sản phẩm phân tích đang có đủ
tín hiệu rating/review/sold đến mức nào.

```sql
SELECT
    rating_bucket,
    review_bucket,
    sold_bucket,
    COUNT(*) AS product_count
FROM analytics.product_analysis_ready
GROUP BY rating_bucket, review_bucket, sold_bucket
ORDER BY product_count DESC;
```

## 4. Query Discount Theo Segment

```sql
SELECT
    home_segment,
    discount_bucket,
    COUNT(*) AS product_count,
    ROUND(AVG(discount_percent), 2) AS avg_discount_percent,
    ROUND(AVG(rating_average), 2) AS avg_rating,
    SUM(sales_count) AS total_sold
FROM analytics.product_analysis_ready
WHERE discount_bucket <> 'Không KM'
GROUP BY home_segment, discount_bucket
ORDER BY home_segment, avg_discount_percent DESC;
```

## 5. Query Seller Trong Từng Segment

```sql
SELECT
    home_segment,
    seller_name,
    seller_type,
    COUNT(*) AS product_count,
    ROUND(AVG(price), 0) AS avg_price,
    ROUND(AVG(rating_average), 2) AS avg_rating,
    SUM(sales_count) AS total_sold
FROM analytics.product_analysis_ready
GROUP BY home_segment, seller_name, seller_type
HAVING COUNT(*) >= 3
ORDER BY home_segment, product_count DESC;
```

## 6. Power BI Measures Gợi Ý

```DAX
Total Products = DISTINCTCOUNT('product_analysis_ready'[product_id])

Total Sellers = DISTINCTCOUNT('product_analysis_ready'[seller_id])

Average Price = AVERAGE('product_analysis_ready'[price])

Average Rating = AVERAGE('product_analysis_ready'[rating_average])

Average Discount = AVERAGE('product_analysis_ready'[discount_percent])

Total Sold = SUM('product_analysis_ready'[sales_count])

Total Reviews = SUM('product_analysis_ready'[review_count])
```

Legend/slicer nên dùng trực tiếp:

- `home_segment`
- `market_scope`
- `seller_type`
- `price_bucket`
- `discount_bucket`
- `rating_bucket`
- `review_bucket`
- `sold_bucket`
- `market_position`
