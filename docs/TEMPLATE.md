# 🚀 Sử dụng Repository này như Template

Repository này được thiết kế để sử dụng như một **template** cho các dự án data pipeline mới.

## Cách sử dụng

### Option 1: Sử dụng GitHub Template (Khuyến nghị)

1. Truy cập repository: https://github.com/your-username/tiki-data-pipeline
2. Click nút **"Use this template"** (màu xanh)
3. Chọn **"Create a new repository"**
4. Đặt tên repository mới cho dự án của bạn
5. Clone repository mới về máy với submodules:
   ```bash
   git clone --recursive <repository-url>
   cd <project-name>
   ```
   
   **Lưu ý**: Nếu đã clone mà chưa có `--recursive`, chạy:
   ```bash
   git submodule init
   git submodule update --recursive
   ```

### Option 2: Fork và Customize

1. Fork repository này
2. Clone về máy với submodules: `git clone --recursive <your-fork-url>`
3. Khởi tạo submodules nếu chưa có: `git submodule init && git submodule update --recursive`
4. Đổi tên remote: `git remote rename origin upstream`
5. Thêm remote mới cho dự án của bạn

### Option 3: Clone và Setup thủ công

```bash
# Clone repository với submodules
git clone --recursive https://github.com/your-username/tiki-data-pipeline.git my-new-project
cd my-new-project

# Nếu chưa có submodules, khởi tạo:
git submodule init
git submodule update --recursive

# Xóa git history cũ (nếu muốn bắt đầu mới)
rm -rf .git
git init
git add .
git commit -m "Initial commit from template"

# Thêm remote mới
git remote add origin https://github.com/your-username/my-new-project.git
```

## Setup cho dự án mới

### 0. Khởi tạo Submodules (nếu chưa có)

```bash
# Nếu clone mà chưa có --recursive, chạy:
git submodule init
git submodule update --recursive
```

### 1. Cấu hình môi trường

```bash
# Copy file mẫu
cp .env.example .env

# Chỉnh sửa các biến môi trường
nano .env
```

### 2. Customize cho dự án của bạn

- **Đổi tên trong `docker-compose.yaml`**: 
  ```yaml
  name: your-project-name
  ```

- **Tạo DAGs mới trong `airflow/dags/`**

- **Thêm pipelines trong `src/pipelines/`**

- **Cập nhật README.md** với thông tin dự án của bạn

### 3. Khởi động

```bash
docker-compose up -d
```

## Cấu trúc Template

```
tiki-data-pipeline/
├── docker-compose.yaml          # Cấu hình chính
├── .env.example                 # Template biến môi trường
├── scripts/                     # Utility scripts
├── airflow/
│   ├── dags/                    # Đặt DAGs của bạn ở đây
│   └── plugins/                  # Airflow plugins
└── src/                         # Source code dự án
    ├── pipelines/               # Data pipelines
    ├── models/                   # Data models
    └── utils/                    # Utilities
```

## Best Practices

1. **Đổi tên project**: Cập nhật `docker-compose.yaml` và README
2. **Thêm DAGs**: Tạo DAGs trong `airflow/dags/`
3. **Customize config**: Điều chỉnh resource limits nếu cần
4. **Documentation**: Cập nhật README với thông tin dự án cụ thể
5. **Version control**: Commit thường xuyên

## Lưu ý

- **Submodules**: Repository này sử dụng submodule `firecrawl`. Khi clone, nhớ dùng `--recursive` hoặc chạy `git submodule init && git submodule update --recursive` sau khi clone
- File `.env` không được commit (đã có trong .gitignore)
- Thay đổi mật khẩu mặc định cho production
- Cân nhắc tách databases nếu cần isolation cao

