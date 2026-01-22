# PHÂN TÍCH LOGIC & HỆ THỐNG: TIKI DATA PIPELINE

Hệ thống này có logic xử lý theo mô hình **Batch Processing** được orchestrate bởi Airflow, tập trung vào việc thu thập dữ liệu sản phẩm Tiki ở quy mô lớn với các cơ chế tối ưu hiệu năng (bulk load, caching) và tăng tính ổn định (resilience patterns).

## 1. PHÂN TÍCH LOGIC CORE

**Trace luồng xử lý (Data Flow):**
1. **Crawl**: Sử dụng Dynamic Task Mapping (Airflow) để fetch dữ liệu song song theo danh mục. Dữ liệu thô lưu vào các file JSON tạm.
2. **Aggregation**: Gộp (Merge) hàng trăm file JSON nhỏ thành một bản ghi lớn.
3. **Enrichment**: (Điểm yếu logic) Một bước riêng biệt (`enrich_category_path`) đọc lại file JSON và file categories để bổ sung thông tin hierarchy. Đây là bước redundant về I/O.
4. **Transformation**: Chuyển đổi Schema từ API JSON sang DB Schema (normalize IDs, timestamps).
5. **Storage**: Sử dụng phương pháp `BULK COPY` qua bảng staging trong PostgreSQL để tối ưu tốc độ ghi thay vì `INSERT` từng dòng.

**Decision Making & Design:**
- **Decision placement**: Các logic quan trọng về validation và business rules bị trộn lẫn giữa `PostgresStorage` và file DAG.
- **Leak logic**: Có sự rò rỉ abstraction rất lớn. File DAG chứa hàng ngàn dòng code xử lý dữ liệu (enrichment, transformation) thay vì chỉ giữ vai trò điều phối (orchestration).
- **Edge cases**: Xử lý lỗi khá tốt với `Dead Letter Queue` và `Circuit Breaker`, cho thấy tư duy về "distrusting the source" (crawl error).

## 2. PHÂN TÍCH KIẾN TRÚC HỆ THỐNG

- **Technical Decisions**:
    - **PostgreSQL + Copy**: Rất hợp lý cho bài toán crawl triệu record.
    - **Airflow Task Mapping**: Giải quyết tốt vấn đề concurrency.
    - **Redis Cache**: Dùng để tránh crawl trùng (deduplication) là một điểm cộng về architecture.
- **Bottlenecks**:
    - **Memory OOM**: Việc xử lý Dictionary khổng lồ trong bước Merge/Enrich sẽ khiến Worker bị "treo" khi quy mô sản phẩm tăng lên 10x.
    - **Scheduler Performance**: File DAG quá lớn (~6000 lines) khiến Airflow Scheduler tốn nhiều tài nguyên để parse thường xuyên.
- **Coupling**: High coupling giữa code và môi trường (`/opt/airflow/src`). Nếu đổi môi trường deploy, toàn bộ logic import sẽ break.

## 3. NHẬN ĐỊNH LEVEL (Mid-Senior)

Developer hiện tại đang ở level **High Mid-level** hoặc **Senior (Execution focus)** vì:

- **Indicators của Senior (Mindset tốt):**
    - [x] **Performance mindset**: Biết dùng `SimpleConnectionPool`, `execute_values`, và `COPY FROM` để tối ưu DB.
    - [x] **Resilience mindset**: Có implementation của `Circuit Breaker` và `Dead Letter Queue`. Code không chỉ chạy "happy path".
    - [x] **Tooling mastery**: Sử dụng các tính năng nâng cao của Airflow (Dynamic Mapping, TaskGroup).

- **Gaps (Cần cải thiện để lên Lead/Principal):**
    - [!] **Project Organization**: Gặp lỗi "Monolithic DAG". Tư duy tổ chức code ở mức Junior-Mid khi nhồi nhét business logic vào file điều phối.
    - [!] **Brittle Infrastructure**: Sử dụng các trick `sys.path` và hardcoded paths thay vì đóng gói project (`pip installable`).
    - [!] **Abstractions Leak**: Logic đệ quy xây dựng path (`build_category_path`) nên nằm ở một Service Layer riêng, không nên nằm sâu trong Storage method.

## 4. ROADMAP NÂNG CẤP (GAP THỰC TẾ)

**Vấn đề cụ thể & Refactor:**
1. **Monolithic DAG**: 
    - *Vấn đề*: Khó maintain, khó unit test các task nhỏ. 
    - *Refactor*: Tách toàn bộ `python_callable` ra các file riêng trong `src/pipelines/crawl/tasks/`. File DAG chỉ nên chứa định nghĩa Task và Dependency.
    - *Skill*: Learning Airflow Best Practices (Clean DAGs).
2. **In-memory Enrichment**: 
    - *Vấn đề*: Gây OOM. 
    - *Refactor*: Thực hiện enrichment ngay trong lúc transform hoặc sử dụng SQL Join (ưu tiên xử lý logic ở nơi dữ liệu đang nằm).
    - *Skill*: Stream processing, SQL Optimization.
3. **Spaghetti Imports**: 
    - *Vấn đề*: Code "fragile" khi di chuyển folder. 
    - *Refactor*: Tạo `pyproject.toml`, cài đặt project theo chế độ `editable`. Xóa bỏ toàn bộ block `sys.path.append`.
    - *Skill*: Python Packaging, CI/CD environment management.

**Gap lớn nhất**: **Thinking in Systems (Symmetry & Organization)**. Developer giỏi code tính năng nhưng chưa giỏi trong việc thiết kế một "giao diện" code sạch cho đồng nghiệp maintain.

**Priority học**: **Software Design Patterns (SOLID)** để tách bạch các layer và **Data Engineering Patterns** để xử lý data stream thay vì in-memory batch.
