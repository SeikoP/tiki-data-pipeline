# 🧹 KẾ HOẠCH DỌN DẸP DỰ ÁN TIKI DATA PIPELINE

**Ngày tạo:** 29 Tháng 11, 2025  
**Branch:** optimize  
**Trạng thái:** 📋 Draft - Chưa thực hiện

---

## 📊 TỔNG QUAN

Dự án hiện tại có nhiều files test, demo, archived scripts cần được dọn dẹp và tổ chức lại. Kế hoạch này sẽ:

1. ✅ **Xóa files thừa** - Files cũ, deprecated, test output
2. 📁 **Di chuyển files** - Đưa files về đúng thư mục
3. 🔍 **Kiểm tra code quality** - Ruff, mypy, isort, black, vulture
4. 📝 **Cập nhật README** - Đồng bộ documentation với hiện trạng

---

## 🎯 MỤC TIÊU

- [ ] Giảm 20-30% số lượng files không cần thiết
- [ ] Tổ chức lại cấu trúc thư mục rõ ràng hơn
- [ ] Fix tất cả code quality issues (56 ruff errors hiện tại)
- [ ] Cập nhật documentation cho phản ánh đúng hiện trạng

---

## 📋 PHASE 1: XÓA FILES THỪA VÀ KHÔNG CẦN THIẾT

### 1.1 Test Output Files (data/test_output/)

**Files cần XÓA** (24 files):
```
data/test_output/
├── ALL_8_METHODS_COMPARISON.json          ❌ DELETE (test comparison)
├── ALL_8_METHODS_COMPREHENSIVE.json       ❌ DELETE (test comparison)
├── all_methods_comparison.json            ❌ DELETE (duplicate)
├── COMPARISON_SUMMARY.txt                 ❌ DELETE (old summary)
├── CRAWL_COMPARISON_INDEX.md              ❌ DELETE (index file)
├── demo_crawl_comparison_detailed.json    ❌ DELETE (old demo output)
├── demo_crawl_detail_comparison.json      ❌ DELETE (old demo output)
├── DEMO_RESULTS_SUMMARY.txt               ❌ DELETE (old summary)
├── failed_tasks_analysis.json             ❌ DELETE (old analysis)
├── FINAL_SUMMARY.md                       ❌ DELETE (old summary)
├── full_page.html                         ❌ DELETE (test HTML)
├── http_crawl_results.json                ❌ DELETE (old crawl results)
├── selenium_ai_test_result.json           ❌ DELETE (test result)
├── selenium_product_detail.html           ❌ DELETE (test HTML)
├── selenium_product_detail.json           ❌ DELETE (test result)
├── selenium_test_result.html              ❌ DELETE (test HTML)
├── selenium_test_result.json              ❌ DELETE (test result)
├── test_products.json                     ❌ DELETE (old test data)
├── test_products_sales_count.json         ❌ DELETE (old test data)
├── test_product_detail_sales_count.json   ❌ DELETE (old test data)
├── THREE_METHODS_COMPARISON.txt           ❌ DELETE (old comparison)
└── products/                              ❌ DELETE FOLDER (empty or old)
```

**Lý do:** Test output files từ development phase, không còn cần thiết sau khi tính năng đã stable.

**Action:**
```powershell
Remove-Item -Path "data/test_output/*" -Recurse -Force
# Giữ lại folder nhưng xóa tất cả nội dung
```

---

### 1.2 Archived Test Files (tests/archive/)

**Files CÓ THỂ XÓA** (7 files - đã archived):
```
tests/archive/
├── check_sales_count_in_data.py           ⚠️  ARCHIVE (one-time check)
├── final_validation.py                    ⚠️  ARCHIVE (phase validation)
├── test_performance_optimizations.py      ⚠️  ARCHIVE (old benchmark)
├── test_phase4_complete.py                ⚠️  ARCHIVE (phase test)
├── test_phase4_loader.py                  ⚠️  ARCHIVE (phase test)
├── test_quick_crawl.py                    ⚠️  ARCHIVE (prototype)
└── validate_optimized_dag.py              ⚠️  ARCHIVE (one-time validation)
```

**Lý do:** Files này đã được moved vào archive và không còn sử dụng. Có thể giữ lại nếu cần tham khảo, hoặc xóa để clean up.

**Khuyến nghị:** GIỮ LẠI trong archive thêm 1-2 tháng, sau đó xóa nếu không cần.

---

### 1.3 Archived Scripts (scripts/archive/)

**Files CÓ THỂ XÓA** (14 files - đã archived):
```
scripts/archive/
├── analyze_db_and_propose_3nf.py          ⚠️  ARCHIVE (one-time analysis)
├── apply_schema_changes.py                ⚠️  ARCHIVE (one-time migration)
├── create_test_dag.py                     ⚠️  ARCHIVE (example)
├── enrich_categories_with_paths.py        ⚠️  ARCHIVE (one-time fix)
├── example_save_to_postgres.py            ⚠️  ARCHIVE (example)
├── fix_category_path_issue.py             ⚠️  ARCHIVE (one-time fix)
├── fix_category_path_null.py              ⚠️  ARCHIVE (one-time fix)
├── monitor_crawl_optimization.py          ⚠️  ARCHIVE (old monitoring)
├── optimization_analysis.py               ⚠️  ARCHIVE (old analysis)
├── optimization_summary.py                ⚠️  ARCHIVE (old summary)
├── phase4_optimization.py                 ⚠️  ARCHIVE (phase script)
├── phase4_step2_parallel.py               ⚠️  ARCHIVE (phase script)
├── phase5_infrastructure.py               ⚠️  ARCHIVE (phase script)
└── reapply_phase2.py                      ⚠️  ARCHIVE (phase script)
```

**Lý do:** Phase scripts và one-time fixes không còn cần thiết sau khi tính năng đã được merge vào main code.

**Khuyến nghị:** GIỮ LẠI trong archive thêm 1-2 tháng để tham khảo lịch sử development, sau đó xóa.

---

### 1.4 Verify Data Scripts (verifydata/)

**Files CÓ THỂ CHUYỂN SANG ARCHIVE** (25+ files):

Hầu hết các scripts trong `verifydata/` đều là one-time verification scripts:

```
verifydata/
├── check_breadcrumb.py                    ⚠️  One-time check
├── check_category_format.py               ⚠️  One-time check
├── check_hierarchy.py                     ⚠️  One-time check
├── check_incomplete_categories.py         ⚠️  One-time check
├── check_long_categories.py               ⚠️  One-time check
├── check_suspicious_categories.py         ⚠️  One-time check
├── check_warehouse_data.py                ✅ KEEP (useful for ongoing checks)
├── check_xe_day_hang.py                   ⚠️  One-time check
├── fix_*.py                               ⚠️  One-time fixes
├── test_*.py                              ⚠️  One-time tests
├── validate_*.py                          ⚠️  One-time validations
└── verify_*.py                            ⚠️  One-time verifications
```

**Khuyến nghị:**
- GIỮ LẠI: `check_warehouse_data.py`, `verify_warehouse_cleaned.py` (useful)
- DI CHUYỂN SANG `verifydata/archive/`: Tất cả các file còn lại

**Action:**
```powershell
New-Item -Path "verifydata/archive" -ItemType Directory -Force
Move-Item -Path "verifydata/fix_*.py" -Destination "verifydata/archive/"
Move-Item -Path "verifydata/test_*.py" -Destination "verifydata/archive/"
Move-Item -Path "verifydata/validate_*.py" -Destination "verifydata/archive/"
# Keep only essential verification scripts
```

---

### 1.5 Demo Comparison Files (demos/)

**Files CÓ THỂ XÓA hoặc ARCHIVE:**

```
demos/
├── compare_three_methods.py               ⚠️  ARCHIVE (old comparison)
├── COMPARISON_ANALYSIS.py                 ⚠️  ARCHIVE (old analysis)
├── CRAWL_COMPARISON_GUIDE.md              ⚠️  ARCHIVE (old guide)
├── demo_all_crawl_methods_comprehensive.py ⚠️  ARCHIVE (superseded)
├── demo_all_methods.py                    ⚠️  ARCHIVE (superseded)
├── show_comparison_analysis.py            ⚠️  ARCHIVE (old analysis)
├── test_all_8_methods.py                  ⚠️  ARCHIVE (old test)
```

**GIỮ LẠI:**
```
demos/
├── demo_e2e_full.py                       ✅ KEEP (main demo)
├── demo_step1_crawl.py                    ✅ KEEP (step-by-step)
├── demo_step2_transform.py                ✅ KEEP (step-by-step)
├── demo_step3_load.py                     ✅ KEEP (step-by-step)
├── demo_crawl_detail_async.py             ✅ KEEP (async comparison)
├── demo_crawl_detail_comparison.py        ✅ KEEP (benchmark)
├── demo_crawl_http_requests.py            ✅ KEEP (HTTP method)
├── demo_transform_output.py               ✅ KEEP (transform demo)
├── check_env.py                           ✅ KEEP (environment check)
└── test_setup.py                          ✅ KEEP (setup test)
```

**Action:**
```powershell
New-Item -Path "demos/archive" -ItemType Directory -Force
Move-Item -Path "demos/compare_three_methods.py" -Destination "demos/archive/"
Move-Item -Path "demos/COMPARISON_ANALYSIS.py" -Destination "demos/archive/"
Move-Item -Path "demos/CRAWL_COMPARISON_GUIDE.md" -Destination "demos/archive/"
Move-Item -Path "demos/demo_all_crawl_methods_comprehensive.py" -Destination "demos/archive/"
Move-Item -Path "demos/demo_all_methods.py" -Destination "demos/archive/"
Move-Item -Path "demos/show_comparison_analysis.py" -Destination "demos/archive/"
Move-Item -Path "demos/test_all_8_methods.py" -Destination "demos/archive/"
```

---

## 📁 PHASE 2: DI CHUYỂN FILES VÀO ĐÚNG THƯ MỤC

### 2.1 Root Directory Cleanup

**Hiện tại:**
```
e:\Project\tiki-data-pipeline\
├── verify_format.py                       ❓ Mục đích?
```

**Action:** Kiểm tra `verify_format.py`:
- Nếu là CI tool → di chuyển vào `scripts/utils/`
- Nếu là one-time script → xóa hoặc archive

---

### 2.2 Data Directory Organization

**Hiện tại:**
```
data/
├── categories.json                        ✅ OK (input data)
├── demo/                                  ✅ OK (demo outputs)
├── processed/                             ✅ OK (processed data)
├── raw/                                   ✅ OK (raw crawled data)
└── test_output/                           ❌ CLEAN (delete all content)
```

**Action:** Xóa nội dung `data/test_output/` như đã nói ở Phase 1.1

---

## 🔍 PHASE 3: KIỂM TRA VÀ FIX CODE QUALITY

### 3.1 Ruff Linting Issues

**Hiện trạng:** 56 errors
```
50  W293  blank-line-with-whitespace        [*] Fixable
 2  F541  f-string-missing-placeholders     [*] Fixable
 2  W291  trailing-whitespace               [*] Fixable
 1  B007  unused-loop-control-variable      [ ] Manual fix
 1  I001  unsorted-imports                  [*] Fixable
```

**Action:**
```powershell
# Auto-fix tất cả fixable issues
ruff check src/ --fix

# Fix DAG files
ruff check airflow/dags/ --fix

# Fix scripts
ruff check scripts/ --fix

# Fix tests
ruff check tests/ --fix

# Fix demos
ruff check demos/ --fix

# Fix verifydata
ruff check verifydata/ --fix
```

**Manual fix:** `B007 unused-loop-control-variable` - cần review code thủ công.

---

### 3.2 Black Formatting

**Hiện trạng:** Một số files chưa format đúng

**Action:**
```powershell
# Format all Python files
black src/
black airflow/dags/
black scripts/
black tests/
black demos/
black verifydata/
```

---

### 3.3 Isort Import Sorting

**Hiện trạng:** 1 file có import không đúng thứ tự
```
ERROR: src\pipelines\crawl\build_category_hierarchy.py
```

**Action:**
```powershell
# Fix import sorting
isort src/
isort airflow/dags/
isort scripts/
isort tests/
isort demos/
isort verifydata/
```

---

### 3.4 Mypy Type Checking

**Action:**
```powershell
# Run mypy to check type issues
mypy src/ --ignore-missing-imports
mypy airflow/dags/ --ignore-missing-imports
```

**Expected:** Fix type errors if any found.

---

### 3.5 Vulture Dead Code Detection

**Action:**
```powershell
# Install vulture if not installed
pip install vulture

# Check for dead code
vulture src/ --min-confidence 80
vulture scripts/ --min-confidence 80
```

**Expected:** Remove unused functions, variables, imports.

---

## 📝 PHASE 4: CẬP NHẬT README VÀ DOCUMENTATION

### 4.1 Root README.md

**Cần cập nhật:**
- [x] Section 2.1 "Phạm Vi Hiện Tại" - ĐÃ CÓ (chỉ crawl c1883)
- [ ] Section 6 "Kết Quả Đạt Được" - Cập nhật số liệu mới nhất
- [ ] Section 7 "Hạn Chế Và Hướng Phát Triển" - Bổ sung tính năng mới
- [ ] Section 8 "Tác Giả" - Thêm thông tin contact/portfolio

**Action:** Review và cập nhật từng section.

---

### 4.2 demos/README.md

**Hiện trạng:** ✅ ĐÃ CẬP NHẬT GẦN ĐÂY (chứa demo_crawl_detail_async.py, demo_crawl_detail_comparison.py)

**Cần cập nhật:**
- [ ] Xóa references đến archived demo files
- [ ] Thêm warning nếu demo nào deprecated

---

### 4.3 scripts/README.md

**Hiện trạng:** ✅ ĐÃ CẬP NHẬT (November 19, 2025)

**Cần kiểm tra:**
- [ ] Verify tất cả scripts trong "Active Scripts" vẫn hoạt động
- [ ] Xóa references đến scripts đã xóa (nếu có)

---

### 4.4 tests/README.md

**Hiện trạng:** ✅ ĐÃ CẬP NHẬT (November 19, 2025)

**Cần kiểm tra:**
- [ ] Verify danh sách tests còn chính xác
- [ ] Update test coverage statistics

---

### 4.5 docs/INDEX.md và các docs khác

**Action:** Review từng documentation file trong `docs/` và cập nhật:
- [ ] `docs/INDEX.md` - Main index
- [ ] `docs/PROJECT_DOCUMENTATION_VI.md` - Vietnamese documentation
- [ ] `docs/01-PARAMETERS/` - Parameter guides
- [ ] `docs/02-OPTIMIZATION/` - Optimization guides
- [ ] `docs/03-ARCHITECTURE/` - Architecture docs
- [ ] Các docs khác...

---

## 🚀 PHASE 5: VALIDATION VÀ TESTING

### 5.1 CI Pipeline

**Action:**
```powershell
# Run full CI pipeline
.\scripts\ci.ps1 ci-local
```

**Expected:** All checks pass.

---

### 5.2 Run Active Tests

**Action:**
```powershell
# Run core tests
python tests/test_cache_hit_rate_fix.py
python tests/test_utils.py
python tests/test_transform_load.py
python tests/test_crawl_products.py
```

**Expected:** All tests pass.

---

### 5.3 Validate Demos

**Action:**
```powershell
# Test main demo
python demos/demo_e2e_full.py

# Test step-by-step demos
python demos/demo_step1_crawl.py
python demos/demo_step2_transform.py
python demos/demo_step3_load.py
```

**Expected:** All demos run without errors.

---

### 5.4 Docker Compose Validation

**Action:**
```powershell
# Clean up Docker
.\scripts\docker-cleanup.ps1

# Start fresh
docker-compose up -d --build

# Verify all services running
docker-compose ps

# Check logs
docker-compose logs airflow-scheduler | Select-String -Pattern "error" -CaseSensitive:$false
```

**Expected:** All services running, no errors.

---

## 📊 PHASE 6: COMMIT VÀ DOCUMENTATION

### 6.1 Git Commit Structure

**Commits:**
```bash
# 1. Cleanup files
git add .
git commit -m "chore: clean up test output files and archived scripts"

# 2. Fix code quality
git add .
git commit -m "style: fix ruff, black, isort issues (56 errors → 0)"

# 3. Update documentation
git add .
git commit -m "docs: update README files with current project status"

# 4. Final validation
git add .
git commit -m "test: validate all tests and demos after cleanup"
```

---

### 6.2 Create Cleanup Summary

**Action:** Tạo file `CLEANUP_SUMMARY.md` với:
- Files đã xóa (count + size)
- Files đã di chuyển (count)
- Code quality improvements (before/after)
- Updated documentation files
- Next steps

---

## ✅ CHECKLIST TỔNG HỢP

### Phase 1: Xóa Files Thừa
- [ ] Xóa `data/test_output/` (24 files)
- [ ] Archive `tests/archive/` (7 files) - hoặc giữ lại
- [ ] Archive `scripts/archive/` (14 files) - hoặc giữ lại
- [ ] Archive `verifydata/` (20+ files → verifydata/archive/)
- [ ] Archive `demos/` (7 files → demos/archive/)

### Phase 2: Di Chuyển Files
- [ ] Kiểm tra `verify_format.py` và di chuyển/xóa
- [ ] Verify data directory structure

### Phase 3: Code Quality
- [ ] Fix ruff issues (56 → 0)
- [ ] Run black formatting
- [ ] Fix isort import sorting
- [ ] Run mypy type checking
- [ ] Run vulture dead code detection

### Phase 4: Documentation
- [ ] Update root `README.md`
- [ ] Update `demos/README.md`
- [ ] Update `scripts/README.md`
- [ ] Update `tests/README.md`
- [ ] Update `docs/` files

### Phase 5: Validation
- [ ] Run CI pipeline (`.\scripts\ci.ps1 ci-local`)
- [ ] Run all active tests
- [ ] Validate demos
- [ ] Test Docker Compose stack

### Phase 6: Commit & Summary
- [ ] Commit cleanup changes
- [ ] Commit code quality fixes
- [ ] Commit documentation updates
- [ ] Create `CLEANUP_SUMMARY.md`

---

## 🎯 KẾT QUẢ MONG ĐỢI

### Metrics
- **Files removed:** 50-80 files (test outputs, archived scripts)
- **Code quality:** 56 errors → 0 errors
- **Documentation:** 100% up-to-date
- **Test coverage:** All tests passing
- **Project size:** Giảm 10-20% tổng file count

### Benefits
- ✅ Dễ navigate và tìm files hơn
- ✅ Code quality chuẩn industrial level
- ✅ Documentation đồng bộ với code
- ✅ Onboarding mới dễ dàng hơn
- ✅ CI/CD pipeline stable

---

## 📅 TIMELINE THỰC HIỆN

**Estimated Time:** 3-4 giờ

| Phase | Estimated Time | Priority |
|-------|---------------|----------|
| Phase 1: Cleanup | 30 phút | 🔴 High |
| Phase 2: Move files | 15 phút | 🟡 Medium |
| Phase 3: Code quality | 1 giờ | 🔴 High |
| Phase 4: Documentation | 1 giờ | 🟡 Medium |
| Phase 5: Validation | 45 phút | 🔴 High |
| Phase 6: Commit | 15 phút | 🟢 Low |

---

## 🚨 NOTES VÀ WARNINGS

### ⚠️ Backup Trước Khi Cleanup
```powershell
# Backup toàn bộ project
Copy-Item -Path "E:\Project\tiki-data-pipeline" -Destination "E:\Project\tiki-data-pipeline-backup-$(Get-Date -Format 'yyyyMMdd')" -Recurse
```

### ⚠️ Không Xóa Ngay Archive Files
- Giữ archive files trong 1-2 tháng để tham khảo
- Sau đó mới xóa nếu chắc chắn không cần

### ⚠️ Test Kỹ Sau Khi Cleanup
- Run full test suite
- Test DAGs trong Airflow UI
- Verify Docker stack hoạt động

---

## 📞 SUPPORT

Nếu có vấn đề sau cleanup:
1. Restore từ backup
2. Review git history để rollback changes
3. Check CI logs để debug errors

---

**Last Updated:** 29 November 2025  
**Status:** 📋 Ready for Execution  
**Approved By:** [Your Name]
