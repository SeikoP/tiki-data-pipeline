# Tài Liệu Đồ Án Tốt Nghiệp

Bộ tài liệu này dùng để làm đồ án tốt nghiệp theo hướng Data Analytics với đề
tài:

**Phân tích thị trường sản phẩm tiện ích gia đình trên Tiki: giá bán, khuyến
mãi, đánh giá của khách hàng và mức độ cạnh tranh giữa các seller.**

Thư mục này được ignore trong Git và không đẩy lên GitHub.

## Cấu Trúc Tài Liệu

1. `01_project_scope_and_plan.md`
   - Mục tiêu đồ án.
   - Câu hỏi nghiên cứu.
   - Phạm vi crawl và phạm vi phân tích.
   - Lộ trình 4 tuần.
   - Tiêu chí hoàn thành.

2. `02_pipeline_cleaning_and_model.md`
   - Kiến trúc pipeline.
   - Quy trình làm sạch dữ liệu.
   - Mapping category/segment.
   - Mô hình analytics cho Power BI.
   - Data dictionary rút gọn.

3. `03_dashboard_report_and_powerbi.md`
   - Storyboard dashboard Power BI.
   - Checklist dựng Power BI.
   - Dàn ý slide bảo vệ.
   - Dàn ý báo cáo Word.

4. `04_insights_limitations_and_defense.md`
   - Template viết insight.
   - Danh sách insight cần tìm.
   - Hạn chế và rủi ro dữ liệu.
   - Câu hỏi hội đồng có thể hỏi và câu trả lời mẫu.

5. `05_analytics_schema_data_dictionary.md`
   - Giải thích schema analytics hiện tại chỉ dùng `product_analysis_ready`.
   - Grain của bảng phân tích chính.
   - Data dictionary chi tiết theo schema hiện tại.
   - Quy tắc bucket tiếng Việt trực tiếp trong cột gốc.

6. `06_step_by_step_cleaning_for_slides.md`
   - Quy trình làm sạch dữ liệu theo từng bước để đưa vào slide.
   - Cách chứng minh dữ liệu đủ chất lượng.
   - Checklist kiểm tra trước khi vẽ biểu đồ.

7. `07_sql_queries_and_vietnamese_aliases.md`
   - Query mẫu trực tiếp trên `analytics.product_analysis_ready`.
   - Query mẫu cho dashboard và slide.
   - Measure gợi ý cho Power BI.

8. `08_current_run_evidence_and_metrics.md`
   - Bằng chứng run E2E gần nhất.
   - Số lượng category/product/detail/load/normalize.
   - Phân bố segment hiện tại dùng được cho phân tích rộng.

9. `09_airflow_analytics_nodes.md`
   - Cách hoạt động từng node Airflow của DAG `tiki_analytics_normalize`.
   - Lý do chỉ còn current snapshot analytics.
   - Dependency graph của normalize DAG.

## Nguyên Tắc Chính

- Không sửa raw data.
- Làm sạch theo từng lớp: raw -> core normalized -> analytics mart.
- Analytics schema hiện tại chỉ giữ sản phẩm đủ điều kiện phân tích trong
  `product_analysis_ready`.
- Power BI chỉ dùng bảng analytics đã sẵn sàng cho phân tích.
- Mọi cleaning rule cần có input, rule, output và lý do.
- Missing value không được tự động biến thành 0 nếu không có bằng chứng rõ ràng.
- Tên cột vật lý trong database giữ dạng English `snake_case`.
- Giá trị trong các cột phân loại dùng tiếng Việt ngắn trực tiếp, ví dụ `Giá TB`,
  `KM cao`, `Rating 0`, `0 review`, `Sold TB`.
- Không đưa crawl quality, run status, daily facts, hoặc price tracking vào
  analytics schema khi chưa có yêu cầu phân tích lịch sử.

## Cách Dùng Nhanh

- Khi bắt đầu làm: đọc `01_project_scope_and_plan.md`.
- Khi xây DAG analytics: đọc `02_pipeline_cleaning_and_model.md`.
- Khi dựng dashboard/báo cáo: đọc `03_dashboard_report_and_powerbi.md`.
- Khi chuẩn bị bảo vệ: đọc `04_insights_limitations_and_defense.md`.
- Khi cần giải thích schema với hội đồng: đọc `05_analytics_schema_data_dictionary.md`.
- Khi viết phần tiền xử lý dữ liệu trong slide: đọc `06_step_by_step_cleaning_for_slides.md`.
- Khi cần query hoặc Việt hóa tên cột: đọc `07_sql_queries_and_vietnamese_aliases.md`.
- Khi cần số liệu chứng minh pipeline đã chạy: đọc `08_current_run_evidence_and_metrics.md`.
- Khi cần giải thích từng node Airflow normalize: đọc `09_airflow_analytics_nodes.md`.
