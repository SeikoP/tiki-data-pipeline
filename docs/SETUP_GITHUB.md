# 🚀 Hướng dẫn Push Template lên GitHub

## Bước 1: Kiểm tra lại các file

```bash
# Xem tất cả file sẽ commit
git status

# Đảm bảo KHÔNG có file nhạy cảm
git status | grep -E "\.env$|secrets|credentials"
```

## Bước 2: Add và Commit

```bash
# Add tất cả file cần thiết
git add .

# Kiểm tra lại
git status

# Commit với message rõ ràng
git commit -m "feat: Create reusable template for Airflow + Firecrawl

- Add docker-compose.yaml with shared databases optimization
- Add comprehensive README.md and TEMPLATE.md
- Add .env.example with all environment variables
- Add example DAGs and pipelines
- Add init script for multiple databases
- Add LICENSE, CONTRIBUTING.md
- Add GitHub issue and PR templates"
```

## Bước 3: Push lên GitHub

```bash
# Thêm remote (thay <username> bằng username GitHub của bạn)
git remote add origin https://github.com/<username>/tiki-data-pipeline.git

# Hoặc nếu đã có remote, kiểm tra
git remote -v

# Push lên GitHub
git branch -M main
git push -u origin main
```

## Bước 4: Setup Template Repository trên GitHub

### 4.1. Đánh dấu là Template Repository

1. Truy cập: https://github.com/<username>/tiki-data-pipeline
2. Click **Settings** (tab trên cùng)
3. Scroll xuống phần **Template repository**
4. ✅ **Check box "Template repository"**
5. Click **Save**

### 4.2. Thêm Topics/Tags

1. Ở trang chính repository, click **⚙️ Settings** hoặc **Edit** button
2. Thêm các topics:
   - `airflow`
   - `firecrawl`
   - `data-pipeline`
   - `docker-compose`
   - `template`
   - `self-hosted`
   - `web-scraping`

### 4.3. Thêm Description

Thêm description ngắn gọn:
```
🚀 Reusable template for Apache Airflow + Firecrawl self-hosted data pipelines. Includes Docker Compose setup with optimized shared databases.
```

### 4.4. Tạo Release đầu tiên (Optional)

```bash
# Tạo tag
git tag -a v1.0.0 -m "Initial template release"

# Push tag
git push origin v1.0.0
```

Sau đó trên GitHub:
1. Go to **Releases** tab
2. Click **Create a new release**
3. Chọn tag `v1.0.0`
4. Title: `v1.0.0 - Initial Template Release`
5. Description: Copy từ README.md
6. Click **Publish release**

## Bước 5: Verify

### Kiểm tra Template hoạt động

1. Vào repository trên GitHub
2. Click nút **"Use this template"** (màu xanh lá)
3. Tạo repository test để đảm bảo template hoạt động

### Checklist

- [ ] Repository đã được đánh dấu là Template
- [ ] README.md hiển thị đúng
- [ ] .env.example có đầy đủ biến môi trường
- [ ] Tất cả file cần thiết đã được commit
- [ ] Không có file nhạy cảm (.env, secrets, etc.)
- [ ] Nút "Use this template" xuất hiện

## Bước 6: Sử dụng Template cho dự án mới

### Cách 1: Sử dụng nút Template (Khuyến nghị)

1. Vào repository: https://github.com/<username>/tiki-data-pipeline
2. Click **"Use this template"** → **"Create a new repository"**
3. Đặt tên repository mới
4. Chọn Public/Private
5. Click **"Create repository from template"**
6. Clone repository mới về máy

### Cách 2: Clone và Customize

```bash
# Clone template
git clone https://github.com/<username>/tiki-data-pipeline.git my-new-project
cd my-new-project

# Xóa git history cũ
rm -rf .git
git init

# Customize
# - Đổi tên trong docker-compose.yaml
# - Cập nhật README.md
# - Tạo .env từ .env.example

# Commit và push
git add .
git commit -m "Initial commit from template"
git remote add origin https://github.com/<username>/my-new-project.git
git push -u origin main
```

## Troubleshooting

### Lỗi: Permission denied

```bash
# Kiểm tra SSH key
ssh -T git@github.com

# Hoặc dùng HTTPS với Personal Access Token
```

### Lỗi: File quá lớn

```bash
# Kiểm tra file lớn
git ls-files | xargs ls -la | sort -k5 -rn | head

# Thêm vào .gitignore nếu cần
```

### Lỗi: Template button không xuất hiện

- Đảm bảo đã check "Template repository" trong Settings
- Refresh trang
- Đảm bảo bạn là owner của repository

## Next Steps

Sau khi push thành công:

1. ✅ Share repository với team
2. ✅ Tạo documentation cho team về cách sử dụng
3. ✅ Cập nhật template khi có cải tiến mới
4. ✅ Nhận feedback và cải thiện template

## Tips

- **Versioning**: Tạo tags cho các version quan trọng
- **Changelog**: Giữ CHANGELOG.md để track changes
- **Examples**: Thêm nhiều example DAGs và pipelines
- **Documentation**: Luôn cập nhật README khi có thay đổi

