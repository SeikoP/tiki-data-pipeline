# Quick Setup - SYNC_REPO_TOKEN Secret

## ⚡ Hướng dẫn nhanh (5 phút)

### Bước 1: Tạo Token (2 phút)

1. Vào: https://github.com/settings/tokens
2. Click **"Generate new token"** → **"Generate new token (classic)"**
3. **Note**: `sync-repo-token`
4. **Expiration**: Chọn `90 days` hoặc `No expiration`
5. **Select scopes**: ✅ Chọn **`repo`** (Full control)
6. Click **"Generate token"**
7. **Copy token ngay** (chỉ hiển thị một lần!)

### Bước 2: Thêm Secret (2 phút)

1. Vào repository: `https://github.com/YOUR_USERNAME/tiki-data-pipeline`
2. Click **Settings** tab
3. Click **Secrets and variables** → **Actions**
4. Click **"New repository secret"**
5. **Name**: `SYNC_REPO_TOKEN` (phải đúng tên này!)
6. **Value**: Paste token đã copy ở bước 1
7. Click **"Add secret"**

### Bước 3: Kiểm tra (1 phút)

1. Vào repository → **Settings** → **Secrets and variables** → **Actions**
2. Xem secret `SYNC_REPO_TOKEN` trong danh sách
3. Đảm bảo secret có tên đúng: `SYNC_REPO_TOKEN`

### Bước 4: Test Workflow

1. Vào repository → **Actions** tab
2. Chọn workflow **"Sync to Other Repository"**
3. Click **"Run workflow"**
4. Chọn branch `main` hoặc `master`
5. Click **"Run workflow"**
6. Xem kết quả

---

## ✅ Checklist

- [ ] ✅ Token đã được tạo với quyền `repo`
- [ ] ✅ Token đã được copy và lưu
- [ ] ✅ Secret `SYNC_REPO_TOKEN` đã được thêm vào repository
- [ ] ✅ Tên secret đúng: `SYNC_REPO_TOKEN` (không có khoảng trắng)
- [ ] ✅ Workflow đã được test và hoạt động

---

## 🔍 Troubleshooting

### Lỗi: "SYNC_REPO_TOKEN secret is not set!"

**Giải pháp:**
1. Kiểm tra secret đã được thêm chưa:
   - Repository → Settings → Secrets and variables → Actions
   - Xem secret `SYNC_REPO_TOKEN` trong danh sách
2. Đảm bảo tên secret đúng: `SYNC_REPO_TOKEN` (chính xác)
3. Thêm lại secret nếu chưa có

### Lỗi: "could not read Password" hoặc "Authentication failed"

**Giải pháp:**
1. Kiểm tra token có đúng không:
   - Token phải bắt đầu với `ghp_`
   - Token phải có quyền `repo`
2. Kiểm tra token có hết hạn không:
   - Vào GitHub → Settings → Developer settings → Personal access tokens
   - Xem token expiration date
3. Tạo token mới nếu cần:
   - Generate new token với quyền `repo`
   - Update secret với token mới

---

## 📚 Tài liệu chi tiết

**Xem hướng dẫn chi tiết:** [docs/SETUP_SYNC_SECRET.md](SETUP_SYNC_SECRET.md)

---

**Last Updated:** 2025-11-12  
**Status:** ✅ Ready to use

