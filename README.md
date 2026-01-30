# 🛒 Tiki Data Pipeline - Hệ Thống Thu Thập Dữ Liệu Chuyên Sâu

Dữ liệu trên các sàn thương mại điện tử luôn biến động từng giây. Dự án này được xây dựng để giải quyết thách thức về việc thu thập và quản lý nguồn thông tin khổng lồ từ Tiki một cách tự động và bền vững. Mục tiêu cốt lõi là chuyển hóa những dữ liệu thô trên web thành một kho lưu trữ có cấu trúc, cho phép quan sát được sự thay đổi của thị trường theo thời gian.

## 🎯 Điểm Nhấn Kỹ Thuật (v1.2) - Mới Nhất

Hệ thống được thiết kế hướng tới sự bền bỉ, độ chính xác cao và hiệu suất tối đa:

- **Kiến trúc Layered (Speed & Enrichment):** 
    - **Speed Layer:** Sử dụng API-based crawling để lấy hàng ngàn sản phẩm trong vài phút.
    - **Enrichment Layer:** Sử dụng Selenium (Pool) để lấy thông tin chi tiết (Review, Brand, Seller) một cách đầy đủ.
- **Tối ưu Performance (22x Speedup):** 
    - **Connection Pooling:** Đạt tỷ lệ tái sử dụng connection >75% cho Postgres, Redis và HTTP.
    - **Dynamic Batching:** Tự động chia task thành 4 luồng song song (Optimize Pool) để tận dụng tối đa tài nguyên worker.
- **Hệ thống Resilience (Độ tin cậy):** 
    - **Circuit Breaker:** Tự động ngắt kết nối khi server source bị lỗi hoặc rate limit, tránh "cascade failure".
    - **Dead Letter Queue (DLQ):** Toàn bộ dữ liệu lỗi được gom lại để xử lý độc lập, không làm dừng pipeline.
    - **Adaptive Rate Limiting:** Tự điều chỉnh tốc độ crawl dựa trên tình trạng server đích.
- **Quản lý Dữ liệu Thông Minh:**
    - **Smart Pagination:** Phân trang thông minh dựa trên API metadata (paging), lấy đủ 100% sản phẩm (không bị giới hạn 40).
    - **Data Persistence:** Logic COALESCE trong Database giúp giữ lại dữ liệu lịch sử nếu bản crawl mới bị thiếu trường thông tin.
- **Multi-Level Caching:** Hệ thống cache 3 lớp (L1 Memory, L2 Redis, L3 File) đảm bảo tốc độ phản hồi tính bằng milisecond.

## 🚀 Cách thức vận hành

Thay vì thu thập dữ liệu một cách tuần tự, hệ thống được thiết kế để phân rã công việc thành hàng trăm luồng xử lý độc lập thông qua Airflow Dynamic Task Mapping. Cơ chế này cho phép pipeline tự điều chỉnh quy mô dựa trên số lượng danh mục sản phẩm.

Dữ liệu sau khi thu thập sẽ đi qua một bộ lọc khắt khe để đảm bảo tính đồng nhất. Những thông tin nhiễu hoặc sai lệch được loại bỏ ngay từ đầu. Một điểm đặc biệt là sự tích lũy lịch sử giúp ghi lại từng thay đổi về giá cả, số lượng bán và xếp hạng của sản phẩm theo thời gian.

## 📁 Tổ Chức Tài Liệu

| Tài liệu | Nội dung | Link |
|----------|----------|------|
| **01-PARAMETERS** | Tất cả tham số cấu hình Airflow & Env | [Xem chi tiết](./docs/01-PARAMETERS/README.md) |
| **02-OPTIMIZATION** | Roadmap và các kỹ thuật tăng tốc 22x | [Xem chi tiết](./docs/02-OPTIMIZATION/README.md) |
| **03-ARCHITECTURE** | Kiến trúc 3-stage ETL & Database Schema | [Xem chi tiết](./docs/03-ARCHITECTURE/README.md) |
| **04-CONFIGURATION** | Hướng dẫn thiết lập hạ tầng (Redis, DB, HTTP) | [Xem chi tiết](./docs/04-CONFIGURATION/README.md) |
| **CRAWL PIPELINE** | Hướng dẫn sử dụng module crawl chi tiết | [Xem chi tiết](./src/pipelines/crawl/README.md) |

---

**Trạng thái:** ✅ Hoàn thiện & Ổn định (v1.2)
**Cập nhật lần cuối:** 2026-01-31
*Dự án là sự kết hợp giữa tư duy logic chặt chẽ và các giải pháp kỹ thuật hiện đại để xây dựng nên một dòng chảy dữ liệu thông suốt.*
