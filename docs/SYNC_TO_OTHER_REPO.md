# Sync Files to Other Repository

Hướng dẫn cách sync các file/folder cụ thể từ repository này sang repository khác.

## 🎯 Mục đích

Tự động đồng bộ các file/folder sau sang repository khác:
- `docker-compose.yaml`
- `scripts/` (toàn bộ thư mục)

## 🔧 Setup

### 1. Tạo GitHub Personal Access Token

1. Vào GitHub → Settings → Developer settings → Personal access tokens → Tokens (classic)
2. Click "Generate new token (classic)"
3. Đặt tên token (ví dụ: `sync-repo-token`)
4. Chọn scopes:
   - ✅ `repo` (Full control of private repositories)
5. Click "Generate token"
6. Copy token (chỉ hiển thị một lần!)

### 2. Thêm Secret vào Repository

1. Vào repository settings → Secrets and variables → Actions
2. Click "New repository secret"
3. Name: `SYNC_REPO_TOKEN`
4. Value: Paste token đã tạo ở bước 1
5. Click "Add secret"

### 3. Cấu hình Repository đích

Đảm bảo repository đích (`SeikoP/airflow-firecrawl-data-pipeline`) có:
- Branch `main` hoặc `master`
- Token có quyền push vào repository này

## 🚀 Cách sử dụng

### Tự động (GitHub Actions)

Workflow sẽ tự động chạy khi:
- Có push vào branch `main` hoặc `master`
- Có thay đổi ở `docker-compose.yaml` hoặc `scripts/`

**Xem workflow runs:**
- Vào repository → Actions tab
- Xem workflow "Sync to Other Repository"

### Thủ công (Local)

#### Linux/Mac (Bash)

```bash
# Chạy script sync
bash scripts/utils/sync_to_other_repo.sh

# Hoặc chỉ định đường dẫn target repo
bash scripts/utils/sync_to_other_repo.sh /path/to/target-repo
```

#### Windows (PowerShell)

```powershell
# Chạy script sync
.\scripts\utils\sync_to_other_repo.ps1

# Hoặc chỉ định đường dẫn target repo
.\scripts\utils\sync_to_other_repo.ps1 -TargetRepo "C:\path\to\target-repo"
```

## 📋 Workflow Configuration

### Trigger Conditions

Workflow sẽ trigger khi:
- Push vào branch `main` hoặc `master`
- Có thay đổi ở:
  - `docker-compose.yaml`
  - `scripts/**`
  - `.github/workflows/sync-to-other-repo.yml`

### Manual Trigger

Bạn cũng có thể trigger thủ công:
1. Vào repository → Actions tab
2. Chọn workflow "Sync to Other Repository"
3. Click "Run workflow"
4. Chọn branch và options
5. Click "Run workflow"

## 🔍 Kiểm tra

### Xem workflow runs

```bash
# Xem workflow runs
gh run list --workflow="sync-to-other-repo.yml"

# Xem chi tiết workflow run
gh run view <run-id>
```

### Xem changes trong target repo

1. Vào repository đích: https://github.com/SeikoP/airflow-firecrawl-data-pipeline
2. Kiểm tra commits mới nhất
3. Xem changes trong `docker-compose.yaml` và `scripts/`

## 🛠️ Troubleshooting

### Lỗi: Permission denied

**Nguyên nhân:** Token không có quyền push vào target repository.

**Giải pháp:**
1. Kiểm tra token có quyền `repo`
2. Đảm bảo token có quyền push vào target repository
3. Tạo token mới với đầy đủ quyền

### Lỗi: Target repository not found

**Nguyên nhân:** Repository đích không tồn tại hoặc không accessible.

**Giải pháp:**
1. Kiểm tra repository URL: `https://github.com/SeikoP/airflow-firecrawl-data-pipeline`
2. Đảm bảo token có quyền access repository này
3. Kiểm tra repository có tồn tại không

### Lỗi: No changes to sync

**Nguyên nhân:** Files không có thay đổi hoặc đã được sync trước đó.

**Giải pháp:**
- Đây không phải lỗi, chỉ là thông báo không có thay đổi để sync

### Lỗi: Branch not found

**Nguyên nhân:** Target repository không có branch `main` hoặc `master`.

**Giải pháp:**
1. Tạo branch `main` hoặc `master` trong target repository
2. Hoặc cập nhật workflow để sử dụng branch khác

## 📝 Customization

### Thay đổi files/folders cần sync

Chỉnh sửa file `.github/workflows/sync-to-other-repo.yml`:

```yaml
# Thêm file mới
- name: Sync new file
  run: |
    cp new-file.txt target-repo/new-file.txt

# Thêm thư mục mới
- name: Sync new directory
  run: |
    rm -rf target-repo/new-dir
    cp -r new-dir target-repo/new-dir
```

### Thay đổi target repository

Chỉnh sửa file `.github/workflows/sync-to-other-repo.yml`:

```yaml
# Thay đổi target repository URL
git clone https://${{ secrets.SYNC_REPO_TOKEN }}@github.com/USERNAME/REPO_NAME.git target-repo
```

### Thay đổi trigger paths

Chỉnh sửa file `.github/workflows/sync-to-other-repo.yml`:

```yaml
on:
  push:
    paths:
      - 'docker-compose.yaml'
      - 'scripts/**'
      - 'new-file.txt'  # Thêm file mới
      - 'new-dir/**'    # Thêm thư mục mới
```

## 🔒 Security

### Best Practices

1. **Không commit token vào code:**
   - Sử dụng GitHub Secrets
   - Không hardcode token trong workflow files

2. **Sử dụng fine-grained tokens:**
   - Chỉ cấp quyền cần thiết
   - Giới hạn scope của token

3. **Regular token rotation:**
   - Đổi token định kỳ
   - Revoke token cũ khi không dùng

4. **Review workflow changes:**
   - Review các thay đổi workflow trước khi merge
   - Đảm bảo không có thay đổi bất thường

## 📚 References

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [GitHub Secrets](https://docs.github.com/en/actions/security-guides/encrypted-secrets)
- [Personal Access Tokens](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token)

---

**Last Updated:** 2025-11-12  
**Status:** ✅ Ready to use

