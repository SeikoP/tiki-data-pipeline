# GitHub Action Setup - Sync to Other Repository

Hướng dẫn chi tiết cách setup GitHub Action để sync files sang repository khác.

## 🎯 Mục đích

Tự động đồng bộ `docker-compose.yaml` và `scripts/` sang repository: `https://github.com/SeikoP/airflow-firecrawl-data-pipeline`

## 🔧 Setup - Bước 1: Tạo GitHub Personal Access Token

### 1.1 Truy cập GitHub Settings

1. Vào GitHub → Click avatar (góc phải trên) → **Settings**
2. Scroll xuống → **Developer settings** (ở cuối sidebar bên trái)
3. Click **Personal access tokens** → **Tokens (classic)**
4. Click **Generate new token** → **Generate new token (classic)**

### 1.2 Cấu hình Token

1. **Note**: Đặt tên token (ví dụ: `sync-repo-token` hoặc `tiki-pipeline-sync`)
2. **Expiration**: Chọn expiration date (ví dụ: 90 days hoặc No expiration)
3. **Select scopes**: Chọn các quyền sau:
   - ✅ **`repo`** (Full control of private repositories)
     - `repo:status`
     - `repo_deployment`
     - `public_repo`
     - `repo:invite`
     - `security_events`
4. Click **Generate token**

### 1.3 Copy Token

⚠️ **QUAN TRỌNG**: Token chỉ hiển thị một lần! Copy token ngay lập tức.

Token sẽ có dạng: `ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`

**Lưu ý:**
- Không chia sẻ token với ai
- Không commit token vào code
- Lưu token ở nơi an toàn (password manager)

---

## 🔐 Setup - Bước 2: Thêm Secret vào Repository

### 2.1 Truy cập Repository Settings

1. Vào repository: `https://github.com/YOUR_USERNAME/tiki-data-pipeline`
2. Click **Settings** tab (ở trên cùng)
3. Scroll xuống sidebar bên trái → **Secrets and variables** → **Actions**

### 2.2 Thêm Secret

1. Click **New repository secret** button
2. **Name**: `SYNC_REPO_TOKEN` (phải đúng tên này, không có khoảng trắng)
3. **Secret**: Paste token đã copy ở bước 1
4. Click **Add secret**

### 2.3 Kiểm tra Secret

1. Xem danh sách secrets trong repository
2. Đảm bảo có secret `SYNC_REPO_TOKEN` trong danh sách
3. Secret sẽ hiển thị dạng: `SYNC_REPO_TOKEN` (value sẽ bị ẩn)

---

## ✅ Setup - Bước 3: Kiểm tra Workflow File

### 3.1 Kiểm tra Workflow File tồn tại

Đảm bảo file `.github/workflows/sync-to-other-repo.yml` tồn tại trong repository.

### 3.2 Kiểm tra Workflow Configuration

Đảm bảo workflow file có:
- Trigger khi có thay đổi ở `docker-compose.yaml` hoặc `scripts/`
- Sử dụng secret `SYNC_REPO_TOKEN`
- Target repository: `SeikoP/airflow-firecrawl-data-pipeline`

---

## 🚀 Test Workflow

### 4.1 Test Manual Trigger

1. Vào repository → **Actions** tab
2. Chọn workflow **"Sync to Other Repository"**
3. Click **Run workflow** button
4. Chọn branch (ví dụ: `main`)
5. Click **Run workflow** button

### 4.2 Xem Workflow Run

1. Click vào workflow run vừa tạo
2. Xem logs từng step
3. Kiểm tra có lỗi không

### 4.3 Test với thay đổi thật

1. Tạo thay đổi ở `docker-compose.yaml` hoặc `scripts/`
2. Commit và push lên branch `main` hoặc `master`
3. Workflow sẽ tự động chạy
4. Xem kết quả trong Actions tab

---

## 🔍 Troubleshooting

### Lỗi: "SYNC_REPO_TOKEN secret is not set!"

**Nguyên nhân:**
- Secret chưa được thêm vào repository
- Tên secret sai (không phải `SYNC_REPO_TOKEN`)

**Giải pháp:**
1. Kiểm tra secret đã được thêm chưa:
   - Vào repository settings → Secrets and variables → Actions
   - Xem secret `SYNC_REPO_TOKEN` trong danh sách
2. Đảm bảo tên secret đúng: `SYNC_REPO_TOKEN` (chính xác, không có khoảng trắng)
3. Thêm lại secret nếu chưa có:
   - Click **New repository secret**
   - Name: `SYNC_REPO_TOKEN`
   - Value: Paste token
   - Click **Add secret**

### Lỗi: "could not read Password for 'https://github.com'"

**Nguyên nhân:**
- Token không được sử dụng đúng cách trong git clone
- Token không có quyền truy cập repository đích

**Giải pháp:**
1. Kiểm tra token có đúng không:
   - Token phải bắt đầu với `ghp_`
   - Token phải có quyền `repo`
2. Kiểm tra token có quyền truy cập repository đích không:
   - Repository đích: `SeikoP/airflow-firecrawl-data-pipeline`
   - Token phải có quyền push vào repository này
3. Tạo token mới với đầy đủ quyền:
   - Quyền `repo` (Full control)
   - Quyền truy cập repository đích
4. Update secret với token mới:
   - Vào repository settings → Secrets and variables → Actions
   - Click vào secret `SYNC_REPO_TOKEN`
   - Click **Update** và paste token mới

### Lỗi: "Permission denied" hoặc "Authentication failed"

**Nguyên nhân:**
- Token không có quyền push vào target repository
- Token đã hết hạn
- Repository đích không tồn tại hoặc không accessible

**Giải pháp:**
1. Kiểm tra token có quyền `repo` (Full control)
2. Kiểm tra token có quyền push vào target repository:
   - Target repository: `SeikoP/airflow-firecrawl-data-pipeline`
   - Token phải có quyền access repository này
3. Kiểm tra token có hết hạn không:
   - Vào GitHub → Settings → Developer settings → Personal access tokens
   - Xem token expiration date
4. Tạo token mới nếu cần:
   - Generate new token với đầy đủ quyền
   - Update secret với token mới

### Lỗi: "Target repository not found"

**Nguyên nhân:**
- Repository đích không tồn tại
- Repository đích là private và token không có quyền access
- URL repository sai

**Giải pháp:**
1. Kiểm tra repository đích tồn tại:
   - Truy cập: `https://github.com/SeikoP/airflow-firecrawl-data-pipeline`
   - Đảm bảo repository tồn tại
2. Kiểm tra token có quyền access repository đích:
   - Nếu repository là private, token phải có quyền access
   - Token phải có quyền `repo` (Full control)
3. Kiểm tra URL repository trong workflow file:
   - File: `.github/workflows/sync-to-other-repo.yml`
   - Đảm bảo URL đúng: `https://github.com/SeikoP/airflow-firecrawl-data-pipeline.git`

### Lỗi: "Branch not found" hoặc "No such branch"

**Nguyên nhân:**
- Target repository không có branch `main` hoặc `master`
- Branch đích không tồn tại

**Giải pháp:**
1. Kiểm tra branch trong target repository:
   - Truy cập: `https://github.com/SeikoP/airflow-firecrawl-data-pipeline`
   - Xem branches có `main` hoặc `master` không
2. Tạo branch nếu chưa có:
   - Tạo branch `main` hoặc `master` trong target repository
3. Hoặc cập nhật workflow để sử dụng branch khác:
   - File: `.github/workflows/sync-to-other-repo.yml`
   - Thay đổi branch trong git push command

### Lỗi: "No changes to sync"

**Nguyên nhân:**
- Files không có thay đổi
- Files đã được sync trước đó

**Giải pháp:**
- Đây không phải lỗi, chỉ là thông báo
- Workflow sẽ skip commit và push nếu không có thay đổi
- Đây là behavior bình thường

---

## 📝 Checklist Setup

Trước khi sử dụng workflow, đảm bảo:

- [ ] ✅ GitHub Personal Access Token đã được tạo
- [ ] ✅ Token có quyền `repo` (Full control)
- [ ] ✅ Token có quyền truy cập repository đích
- [ ] ✅ Secret `SYNC_REPO_TOKEN` đã được thêm vào repository
- [ ] ✅ Tên secret đúng: `SYNC_REPO_TOKEN` (không có khoảng trắng)
- [ ] ✅ Workflow file `.github/workflows/sync-to-other-repo.yml` tồn tại
- [ ] ✅ Target repository `SeikoP/airflow-firecrawl-data-pipeline` tồn tại
- [ ] ✅ Target repository có branch `main` hoặc `master`
- [ ] ✅ Workflow đã được test và hoạt động đúng

---

## 🔒 Security Best Practices

1. **Không commit token vào code:**
   - Sử dụng GitHub Secrets
   - Không hardcode token trong workflow files
   - Không log token trong workflow logs

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

---

## 📊 Workflow Summary

Sau khi workflow chạy thành công, bạn sẽ thấy:

1. **Summary trong Actions tab:**
   - Source repository và commit
   - Target repository
   - Triggered by (user)
   - Status (success/failure)
   - Synced files

2. **Commit trong target repository:**
   - Commit message chứa thông tin source
   - Commit được tạo tự động
   - Changes được push vào target repository

---

## 🔗 References

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [GitHub Secrets](https://docs.github.com/en/actions/security-guides/encrypted-secrets)
- [Personal Access Tokens](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token)
- [Git Clone with Token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token#using-a-token-on-the-command-line)

---

**Last Updated:** 2025-11-12  
**Status:** ✅ Ready to use

