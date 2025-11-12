# Sync Files to Other Repository

Hướng dẫn cách sync các file/folder cụ thể từ repository này sang repository khác.

## 🎯 Mục đích

Tự động đồng bộ các file/folder sau sang repository khác:
- `docker-compose.yaml`
- `scripts/` (toàn bộ thư mục)

## 🔧 Setup

### ⚠️ QUAN TRỌNG: Đọc hướng dẫn chi tiết

**Xem hướng dẫn setup chi tiết:** [docs/GITHUB_ACTION_SETUP.md](GITHUB_ACTION_SETUP.md)

### 1. Tạo GitHub Personal Access Token

1. Vào GitHub → Settings → Developer settings → Personal access tokens → Tokens (classic)
2. Click "Generate new token (classic)"
3. Đặt tên token (ví dụ: `sync-repo-token`)
4. Chọn scopes:
   - ✅ **`repo`** (Full control of private repositories) - **BẮT BUỘC**
5. Click "Generate token"
6. **Copy token ngay lập tức** (chỉ hiển thị một lần!)

**Lưu ý quan trọng:**
- Token phải có quyền `repo` (Full control)
- Token phải có quyền push vào target repository
- Nếu target repository là private, token phải có quyền access
- Token nên có expiration date để bảo mật

### 2. Thêm Secret vào Repository

1. Vào repository settings → Secrets and variables → Actions
2. Click "New repository secret"
3. **Name**: `SYNC_REPO_TOKEN` (phải đúng tên này, không có khoảng trắng!)
4. **Value**: Paste token đã tạo ở bước 1
5. Click "Add secret"

**Kiểm tra secret đã được thêm:**
- Vào repository settings → Secrets and variables → Actions
- Xem secret `SYNC_REPO_TOKEN` trong danh sách
- Secret sẽ hiển thị dạng: `SYNC_REPO_TOKEN` (value sẽ bị ẩn)

### 3. Cấu hình Repository đích

Đảm bảo repository đích (`SeikoP/airflow-firecrawl-data-pipeline`) có:
- Branch `main` hoặc `master`
- Token có quyền push vào repository này
- Repository tồn tại và accessible

### 4. Kiểm tra Workflow File

Đảm bảo file `.github/workflows/sync-to-other-repo.yml` tồn tại và đúng:
- Workflow trigger khi có thay đổi ở `docker-compose.yaml` hoặc `scripts/`
- Sử dụng secret `SYNC_REPO_TOKEN` để authenticate

## 🚀 Cách sử dụng

### Tự động (GitHub Actions)

Workflow sẽ tự động chạy khi:
- Có push vào branch `main` hoặc `master`
- Có thay đổi ở:
  - `docker-compose.yaml`
  - `scripts/**`
  - `.github/workflows/sync-to-other-repo.yml`

**Xem workflow runs:**
1. Vào repository → Actions tab
2. Xem workflow "Sync to Other Repository"
3. Click vào run để xem chi tiết

### Manual Trigger

Bạn cũng có thể trigger thủ công:
1. Vào repository → Actions tab
2. Chọn workflow "Sync to Other Repository"
3. Click "Run workflow"
4. Chọn branch và options
5. Click "Run workflow"

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

## 🔍 Troubleshooting

### Lỗi: "SYNC_REPO_TOKEN secret is not set"

**Nguyên nhân:** Secret không được thêm vào repository hoặc tên secret sai.

**Giải pháp:**
1. Kiểm tra secret đã được thêm chưa:
   - Vào repository settings → Secrets and variables → Actions
   - Xem secret `SYNC_REPO_TOKEN` trong danh sách
2. Đảm bảo tên secret đúng: `SYNC_REPO_TOKEN` (không có khoảng trắng)
3. Thêm lại secret nếu chưa có

### Lỗi: "could not read Password for 'https://github.com'"

**Nguyên nhân:** Token không được sử dụng đúng cách trong git clone.

**Giải pháp:**
1. Kiểm tra token có đúng không
2. Đảm bảo token có quyền `repo`
3. Kiểm tra workflow file đã được cập nhật chưa
4. Thử tạo token mới và update secret

### Lỗi: "Permission denied" hoặc "Authentication failed"

**Nguyên nhân:** Token không có quyền push vào target repository.

**Giải pháp:**
1. Kiểm tra token có quyền `repo` (Full control)
2. Đảm bảo token có quyền push vào target repository
3. Nếu target repository là private, token phải có quyền access
4. Tạo token mới với đầy đủ quyền và update secret

### Lỗi: "Target repository not found"

**Nguyên nhân:** Repository đích không tồn tại hoặc không accessible.

**Giải pháp:**
1. Kiểm tra repository URL: `https://github.com/SeikoP/airflow-firecrawl-data-pipeline`
2. Đảm bảo repository tồn tại
3. Kiểm tra token có quyền access repository này không
4. Thử clone repository thủ công để kiểm tra

### Lỗi: "Branch not found" hoặc "No such branch"

**Nguyên nhân:** Target repository không có branch `main` hoặc `master`.

**Giải pháp:**
1. Tạo branch `main` hoặc `master` trong target repository
2. Hoặc cập nhật workflow để sử dụng branch khác

### Lỗi: "No changes to sync"

**Nguyên nhân:** Files không có thay đổi hoặc đã được sync trước đó.

**Giải pháp:**
- Đây không phải lỗi, chỉ là thông báo không có thay đổi để sync
- Workflow sẽ skip commit và push nếu không có thay đổi

## 📝 Kiểm tra Workflow

### Xem workflow runs

```bash
# Xem workflow runs (nếu có GitHub CLI)
gh run list --workflow="sync-to-other-repo.yml"

# Xem chi tiết workflow run
gh run view <run-id>
```

### Xem logs

1. Vào repository → Actions tab
2. Click vào workflow run
3. Xem logs từng step
4. Kiểm tra error messages

### Test workflow

1. Tạo test commit với thay đổi ở `docker-compose.yaml` hoặc `scripts/`
2. Push lên branch `main` hoặc `master`
3. Xem workflow chạy trong Actions tab
4. Kiểm tra kết quả sync trong target repository

## 🔒 Security

### Best Practices

1. **Không commit token vào code:**
   - Sử dụng GitHub Secrets
   - Không hardcode token trong workflow files
   - Không log token trong workflow

2. **Sử dụng fine-grained tokens:**
   - Chỉ cấp quyền cần thiết
   - Giới hạn scope của token
   - Sử dụng token với expiration date

3. **Regular token rotation:**
   - Đổi token định kỳ (ví dụ: mỗi 90 ngày)
   - Revoke token cũ khi không dùng
   - Update secret khi đổi token

4. **Review workflow changes:**
   - Review các thay đổi workflow trước khi merge
   - Đảm bảo không có thay đổi bất thường
   - Kiểm tra permissions và secrets usage

5. **Monitor workflow runs:**
   - Xem workflow runs thường xuyên
   - Kiểm tra logs để phát hiện issues
   - Đặt up alerts nếu workflow fails

## 📚 Customization

### Thay đổi files/folders cần sync

Chỉnh sửa file `.github/workflows/sync-to-other-repo.yml`:

```yaml
# Thêm file mới
- name: Sync new file
  run: |
    if [ -f "new-file.txt" ]; then
      cp new-file.txt target-repo/new-file.txt
    fi

# Thêm thư mục mới
- name: Sync new directory
  run: |
    if [ -d "new-dir" ]; then
      rm -rf target-repo/new-dir
      cp -r new-dir target-repo/new-dir
    fi
```

### Thay đổi target repository

Chỉnh sửa file `.github/workflows/sync-to-other-repo.yml`:

```yaml
# Thay đổi target repository URL
git clone https://${SYNC_REPO_TOKEN}@github.com/USERNAME/REPO_NAME.git target-repo

# Và update remote URL
git remote set-url origin https://${SYNC_REPO_TOKEN}@github.com/USERNAME/REPO_NAME.git
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

### Thay đổi branch

Chỉnh sửa file `.github/workflows/sync-to-other-repo.yml`:

```yaml
on:
  push:
    branches:
      - main
      - master
      - develop  # Thêm branch mới
```

## 📊 Workflow Summary

Workflow sẽ tạo summary sau mỗi lần chạy:
- Source repository và commit
- Target repository
- Triggered by (user)
- Status (success/failure)
- Synced files

## 🔗 References

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [GitHub Secrets](https://docs.github.com/en/actions/security-guides/encrypted-secrets)
- [Personal Access Tokens](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token)
- [Git Clone with Token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token#using-a-token-on-the-command-line)

---

**Last Updated:** 2025-11-12  
**Status:** ✅ Ready to use
