# Setup SYNC_REPO_TOKEN Secret - Hướng dẫn chi tiết

## 🎯 Mục đích

Thêm secret `SYNC_REPO_TOKEN` vào repository để GitHub Action có thể sync files sang repository khác.

---

## 📋 Bước 1: Tạo GitHub Personal Access Token

### 1.1 Truy cập GitHub Settings

1. Đăng nhập vào GitHub
2. Click vào **avatar** (góc phải trên cùng)
3. Click **Settings**

### 1.2 Vào Developer Settings

1. Scroll xuống cuối sidebar bên trái
2. Click **Developer settings**
3. Click **Personal access tokens**
4. Click **Tokens (classic)**

### 1.3 Tạo Token mới

1. Click **Generate new token** → **Generate new token (classic)**
2. **Note**: Đặt tên token (ví dụ: `sync-repo-token` hoặc `tiki-pipeline-sync`)
3. **Expiration**: Chọn expiration date:
   - **90 days** (khuyến nghị cho security)
   - **No expiration** (nếu muốn token không bao giờ hết hạn)
4. **Select scopes**: Chọn các quyền sau:
   - ✅ **`repo`** (Full control of private repositories)
     - Chọn checkbox này sẽ tự động chọn tất cả sub-permissions
     - Bao gồm: `repo:status`, `repo_deployment`, `public_repo`, `repo:invite`, `security_events`
5. Scroll xuống cuối
6. Click **Generate token**

### 1.4 Copy Token

⚠️ **QUAN TRỌNG**: Token chỉ hiển thị **một lần duy nhất**!

1. Token sẽ hiển thị dạng: `ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`
2. **Copy token ngay lập tức**
3. Lưu token ở nơi an toàn (password manager, text file tạm, etc.)
4. **KHÔNG** chia sẻ token với ai
5. **KHÔNG** commit token vào code

**Lưu ý:**
- Nếu bạn đóng trang này, bạn sẽ không thể xem lại token
- Bạn sẽ phải tạo token mới nếu mất token

---

## 🔐 Bước 2: Thêm Secret vào Repository

### 2.1 Truy cập Repository Settings

1. Vào repository: `https://github.com/YOUR_USERNAME/tiki-data-pipeline`
2. Click **Settings** tab (ở trên cùng, bên cạnh **Code**, **Issues**, etc.)
3. Scroll xuống sidebar bên trái

### 2.2 Vào Secrets and Variables

1. Trong sidebar bên trái, tìm **Secrets and variables**
2. Click **Actions** (dưới **Secrets and variables**)
3. Bạn sẽ thấy trang **Secrets and variables** → **Actions**

### 2.3 Thêm Secret mới

1. Click **New repository secret** button (ở góc phải trên)
2. **Name**: Nhập `SYNC_REPO_TOKEN` (phải đúng tên này, không có khoảng trắng!)
   - Chữ hoa/chữ thường: `SYNC_REPO_TOKEN` (tất cả chữ hoa)
   - Không có khoảng trắng
   - Không có ký tự đặc biệt
3. **Secret**: Paste token đã copy ở bước 1
   - Token sẽ có dạng: `ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`
4. Click **Add secret**

### 2.4 Kiểm tra Secret đã được thêm

1. Xem danh sách secrets trong repository
2. Bạn sẽ thấy secret `SYNC_REPO_TOKEN` trong danh sách
3. Secret sẽ hiển thị dạng: `SYNC_REPO_TOKEN` (value sẽ bị ẩn với dấu `****`)
4. Bạn có thể click vào secret để xem chi tiết hoặc update

**Lưu ý:**
- Secret value sẽ bị ẩn sau khi thêm
- Bạn không thể xem lại secret value
- Nếu cần update, bạn phải tạo secret mới và xóa secret cũ

---

## ✅ Bước 3: Kiểm tra Secret

### 3.1 Kiểm tra trong Repository Settings

1. Vào repository settings → **Secrets and variables** → **Actions**
2. Xem secret `SYNC_REPO_TOKEN` trong danh sách
3. Đảm bảo secret có tên đúng: `SYNC_REPO_TOKEN`

### 3.2 Test Workflow

1. Vào repository → **Actions** tab
2. Chọn workflow **"Sync to Other Repository"**
3. Click **Run workflow** button
4. Chọn branch (ví dụ: `main`)
5. Click **Run workflow**
6. Xem workflow run để kiểm tra secret có hoạt động không

### 3.3 Kiểm tra Logs

1. Click vào workflow run vừa tạo
2. Xem logs từng step
3. Kiểm tra step **"Checkout target repository"**
4. Nếu secret đúng, workflow sẽ clone repository thành công
5. Nếu secret sai, bạn sẽ thấy lỗi authentication

---

## 🔍 Troubleshooting

### Lỗi: "SYNC_REPO_TOKEN secret is not set!"

**Nguyên nhân:**
- Secret chưa được thêm vào repository
- Tên secret sai (không phải `SYNC_REPO_TOKEN`)

**Giải pháp:**
1. Kiểm tra secret đã được thêm chưa:
   - Vào repository settings → **Secrets and variables** → **Actions**
   - Xem secret `SYNC_REPO_TOKEN` trong danh sách
2. Đảm bảo tên secret đúng: `SYNC_REPO_TOKEN` (chính xác, không có khoảng trắng)
3. Thêm lại secret nếu chưa có:
   - Click **New repository secret**
   - Name: `SYNC_REPO_TOKEN`
   - Value: Paste token
   - Click **Add secret**

### Lỗi: "could not read Password" hoặc "Authentication failed"

**Nguyên nhân:**
- Token không đúng
- Token không có quyền `repo`
- Token đã hết hạn

**Giải pháp:**
1. Kiểm tra token có đúng không:
   - Token phải bắt đầu với `ghp_`
   - Token phải có độ dài hợp lệ
2. Kiểm tra token có quyền `repo` không:
   - Vào GitHub → Settings → Developer settings → Personal access tokens
   - Xem token có quyền `repo` không
3. Kiểm tra token có hết hạn không:
   - Vào GitHub → Settings → Developer settings → Personal access tokens
   - Xem token expiration date
4. Tạo token mới nếu cần:
   - Generate new token với quyền `repo`
   - Update secret với token mới

### Lỗi: "Permission denied" hoặc "Repository not found"

**Nguyên nhân:**
- Token không có quyền push vào target repository
- Target repository không tồn tại
- Token không có quyền access target repository

**Giải pháp:**
1. Kiểm tra token có quyền push vào target repository:
   - Target repository: `SeikoP/airflow-firecrawl-data-pipeline`
   - Token phải có quyền `repo` (Full control)
2. Kiểm tra target repository có tồn tại không:
   - Truy cập: `https://github.com/SeikoP/airflow-firecrawl-data-pipeline`
   - Đảm bảo repository tồn tại
3. Kiểm tra token có quyền access target repository không:
   - Nếu repository là private, token phải có quyền access
   - Token phải được tạo bởi user có quyền access repository

---

## 📝 Checklist

Trước khi sử dụng workflow, đảm bảo:

- [ ] ✅ GitHub Personal Access Token đã được tạo
- [ ] ✅ Token có quyền `repo` (Full control)
- [ ] ✅ Token đã được copy và lưu ở nơi an toàn
- [ ] ✅ Secret `SYNC_REPO_TOKEN` đã được thêm vào repository
- [ ] ✅ Tên secret đúng: `SYNC_REPO_TOKEN` (không có khoảng trắng)
- [ ] ✅ Secret value là token đã tạo ở bước 1
- [ ] ✅ Workflow file `.github/workflows/sync-to-other-repo.yml` tồn tại
- [ ] ✅ Target repository `SeikoP/airflow-firecrawl-data-pipeline` tồn tại
- [ ] ✅ Token có quyền push vào target repository
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

4. **Monitor token usage:**
   - Xem token usage trong GitHub Settings
   - Kiểm tra workflow runs thường xuyên
   - Đặt up alerts nếu workflow fails

---

## 🔗 References

- [GitHub Secrets Documentation](https://docs.github.com/en/actions/security-guides/encrypted-secrets)
- [Personal Access Tokens](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)

---

**Last Updated:** 2025-11-12  
**Status:** ✅ Ready to use

