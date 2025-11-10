# Hướng dẫn đẩy code lên GitHub

## Checklist trước khi commit

- [ ] Đã kiểm tra `.gitignore` - không commit file nhạy cảm
- [ ] Đã tạo `.env.example` với tất cả biến môi trường
- [ ] Đã cập nhật `README.md` với hướng dẫn đầy đủ
- [ ] Đã test cấu hình với `docker-compose up -d`
- [ ] Đã xóa các file backup/temp không cần thiết

## Các file KHÔNG nên commit

- `.env` - Chứa thông tin nhạy cảm
- `airflow/logs/` - Logs tự động tạo
- `data/raw/` và `data/processed/` - Dữ liệu lớn
- `*.sql`, `*.dump`, `*.backup` - Backup files
- Docker volumes data
- IDE config files (`.vscode/`, `.idea/`)

## Các file NÊN commit

- ✅ `docker-compose.yaml` - Cấu hình chính
- ✅ `docker-compose.separate-db.yaml` - Backup version
- ✅ `.env.example` - Template biến môi trường
- ✅ `README.md` - Documentation
- ✅ `LICENSE` - License file
- ✅ `scripts/init-multiple-databases.sh` - Init script
- ✅ `.gitignore` và `.gitattributes`
- ✅ Source code trong `src/`
- ✅ Airflow DAGs mẫu (nếu có)

## Các bước commit và push

### 1. Kiểm tra trạng thái Git

```bash
git status
```

### 2. Thêm các file cần thiết

```bash
# Thêm tất cả file đã thay đổi
git add .

# Hoặc thêm từng file cụ thể
git add docker-compose.yaml
git add README.md
git add .env.example
git add scripts/
git add LICENSE
```

### 3. Kiểm tra lại các file sẽ commit

```bash
git status
```

**Đảm bảo KHÔNG có:**
- `.env`
- `airflow/logs/`
- `data/raw/`, `data/processed/`
- `*.sql`, `*.backup`

### 4. Commit

```bash
git commit -m "feat: Add Airflow + Firecrawl self-host configuration

- Add docker-compose.yaml with shared databases optimization
- Add .env.example with all required environment variables
- Add comprehensive README.md with setup instructions
- Add init script for multiple databases
- Add LICENSE file"
```

### 5. Tạo repository trên GitHub (nếu chưa có)

1. Đăng nhập GitHub
2. Click "New repository"
3. Đặt tên: `tiki-data-pipeline` (hoặc tên khác)
4. Chọn Public hoặc Private
5. **KHÔNG** tạo README, .gitignore, LICENSE (đã có sẵn)
6. Click "Create repository"

### 6. Thêm remote và push

```bash
# Thêm remote (thay <username> và <repo-name>)
git remote add origin https://github.com/<username>/<repo-name>.git

# Hoặc dùng SSH
git remote add origin git@github.com:<username>/<repo-name>.git

# Push lên GitHub
git branch -M main
git push -u origin main
```

### 7. Verify

Kiểm tra trên GitHub xem tất cả file đã được push đúng chưa.

## Best Practices

### Commit Messages

Sử dụng [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: Add new feature
fix: Fix bug
docs: Update documentation
refactor: Code refactoring
chore: Maintenance tasks
```

### Branch Strategy

- `main` - Production-ready code
- `develop` - Development branch
- `feature/*` - Feature branches

### Tags

Tạo tags cho các version:

```bash
git tag -a v1.0.0 -m "Initial release with Airflow + Firecrawl"
git push origin v1.0.0
```

## Security Notes

⚠️ **QUAN TRỌNG**: 

- **KHÔNG BAO GIỜ** commit file `.env` chứa API keys thật
- Nếu vô tình commit, cần:
  1. Xóa file khỏi Git history: `git filter-branch` hoặc `git filter-repo`
  2. Đổi tất cả API keys đã commit
  3. Thêm vào `.gitignore`

## Troubleshooting

### Lỗi: File quá lớn

```bash
# Xóa file khỏi Git cache
git rm --cached large-file.sql

# Thêm vào .gitignore
echo "*.sql" >> .gitignore
```

### Lỗi: Permission denied

```bash
# Kiểm tra SSH key
ssh -T git@github.com

# Hoặc dùng HTTPS với Personal Access Token
```

### Rollback commit

```bash
# Xóa commit cuối (giữ thay đổi)
git reset --soft HEAD~1

# Xóa commit cuối (xóa thay đổi)
git reset --hard HEAD~1
```

