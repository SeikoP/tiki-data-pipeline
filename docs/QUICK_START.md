# ⚡ Quick Start - Push Template lên GitHub

## 🎯 Mục tiêu

Đẩy repository này lên GitHub như một **template repository** để có thể tái sử dụng cho các dự án sau này.

## 📋 Checklist trước khi push

- [x] ✅ Đã tạo `.env.example` với tất cả biến môi trường
- [x] ✅ Đã cập nhật `.gitignore` để loại trừ file nhạy cảm
- [x] ✅ Đã tạo `README.md` chi tiết
- [x] ✅ Đã tạo `TEMPLATE.md` hướng dẫn sử dụng template
- [x] ✅ Đã tạo example DAGs và pipelines
- [x] ✅ Đã tạo LICENSE và CONTRIBUTING.md
- [x] ✅ Đã tạo GitHub templates (issues, PRs)

## 🚀 Các bước thực hiện

### Bước 1: Commit tất cả thay đổi

```bash
git commit -m "feat: Create reusable template for Airflow + Firecrawl

- Add docker-compose.yaml with shared databases optimization
- Add comprehensive README.md and TEMPLATE.md
- Add .env.example with all environment variables
- Add example DAGs (airflow/dags/example_dag.py)
- Add example pipelines (src/pipelines/example_pipeline.py)
- Add init script for multiple databases
- Add LICENSE (MIT), CONTRIBUTING.md
- Add GitHub issue and PR templates
- Add setup script for new projects"
```

### Bước 2: Thêm remote và push

```bash
# Thay <username> bằng username GitHub của bạn
git remote add origin https://github.com/<username>/tiki-data-pipeline.git

# Push lên GitHub
git branch -M main
git push -u origin main
```

### Bước 3: Setup Template Repository

1. Vào: https://github.com/<username>/tiki-data-pipeline
2. Click **Settings** → Scroll xuống **Template repository**
3. ✅ **Check "Template repository"**
4. Click **Save**

### Bước 4: Thêm Description và Topics

**Description:**
```
🚀 Reusable template for Apache Airflow + Firecrawl self-hosted data pipelines. Includes Docker Compose setup with optimized shared databases.
```

**Topics:**
- `airflow`
- `firecrawl`
- `data-pipeline`
- `docker-compose`
- `template`
- `self-hosted`
- `web-scraping`

## ✅ Verify

Sau khi push, kiểm tra:

- [ ] Repository đã có trên GitHub
- [ ] Nút "Use this template" xuất hiện
- [ ] README.md hiển thị đúng
- [ ] Tất cả file đã được push
- [ ] Không có file `.env` hoặc secrets

## 🎉 Sử dụng Template cho dự án mới

### Cách 1: Dùng nút Template (Khuyến nghị)

1. Vào repository → Click **"Use this template"**
2. Đặt tên repository mới
3. Click **"Create repository from template"**
4. Clone về máy và bắt đầu dự án!

### Cách 2: Clone và Customize

```bash
git clone https://github.com/<username>/tiki-data-pipeline.git my-project
cd my-project
rm -rf .git
git init
# Customize và commit
```

## 📚 Tài liệu tham khảo

- [SETUP_GITHUB.md](docs/SETUP_GITHUB.md) - Hướng dẫn chi tiết
- [TEMPLATE.md](docs/TEMPLATE.md) - Cách sử dụng template
- [CONTRIBUTING.md](docs/CONTRIBUTING.md) - Hướng dẫn contribute

