# 📋 Kế hoạch Áp dụng Asset Scheduling

## 🎯 Mục tiêu

Áp dụng Asset-aware Scheduling để:
1. **Tách DAG thành nhiều DAGs độc lập** (Crawl, Transform, Load)
2. **Tự động hóa data flow** (DAG sau tự động chạy khi DAG trước hoàn thành)
3. **Data lineage tracking** trong Airflow UI
4. **Tạo DAGs mới** phụ thuộc vào data (Analytics, Reporting)

---

## 📊 Hiện trạng

### DAG hiện tại
- **File**: `tiki_crawl_products_dag.py`
- **Cấu trúc**: Single DAG với tất cả tasks (Extract → Crawl → Transform → Load → Validate)
- **Schedule**: Manual hoặc `@daily` (qua Variable)
- **Asset**: Đã có code nhưng **TẮT** (mặc định)

### Asset Code hiện tại
- ✅ Đã có `dag_assets/__init__.py` với 4 datasets
- ✅ Đã có `get_outlets_for_task()` function
- ✅ Đã tích hợp vào tasks (nhưng `outlets=None` vì Asset tắt)
- ⚠️ Chưa có DAGs phụ thuộc vào Asset

---

## 🗺️ Kiến trúc mới (sau khi áp dụng Asset)

### Phase 1: Bật Asset trong DAG hiện tại
```
tiki_crawl_products_dag (giữ nguyên)
  ├─ save_products → tạo tiki://products/raw
  ├─ save_products_with_detail → tạo tiki://products/with_detail
  ├─ transform_products → tạo tiki://products/transformed
  └─ load_products → tạo tiki://products/final
```

### Phase 2: Tách thành nhiều DAGs
```
tiki_crawl_dag (@daily)
  └─ Tạo: tiki://products/raw, tiki://products/with_detail

tiki_transform_dag (schedule=[tiki://products/with_detail])
  └─ Tạo: tiki://products/transformed

tiki_load_dag (schedule=[tiki://products/transformed])
  └─ Tạo: tiki://products/final

tiki_validate_dag (schedule=[tiki://products/final])
  └─ Validate & Notify
```

### Phase 3: Thêm DAGs mới
```
tiki_analytics_dag (schedule=[tiki://products/transformed])
  └─ Phân tích products

tiki_reporting_dag (schedule=[tiki://products/final])
  └─ Tạo báo cáo
```

---

## 📝 Kế hoạch thực hiện

### **Phase 1: Bật Asset trong DAG hiện tại** (1-2 ngày)

#### Bước 1.1: Kiểm tra Airflow version
```bash
# Kiểm tra Airflow version
airflow version

# Yêu cầu: Airflow >= 2.7.0
# Nếu < 2.7, cần upgrade hoặc bỏ qua Asset
```

#### Bước 1.2: Bật Asset scheduling
```bash
# Set Airflow Variable
airflow variables set TIKI_USE_ASSET_SCHEDULING true

# Hoặc trong Airflow UI:
# Admin > Variables > Add
# Key: TIKI_USE_ASSET_SCHEDULING
# Value: true
```

#### Bước 1.3: Test DAG hiện tại
```bash
# 1. Parse DAG để kiểm tra syntax
airflow dags list | grep tiki_crawl_products

# 2. Test DAG parsing
airflow dags test tiki_crawl_products 2025-01-01

# 3. Trigger DAG và kiểm tra:
# - Tasks có outlets không?
# - Datasets được tạo trong UI không?
```

#### Bước 1.4: Verify trong Airflow UI
1. Vào **Datasets** menu
2. Kiểm tra 4 datasets:
   - `tiki://products/raw`
   - `tiki://products/with_detail`
   - `tiki://products/transformed`
   - `tiki://products/final`
3. Trigger DAG và xem datasets được update

**✅ Deliverable**: DAG hiện tại chạy với Asset tracking

---

### **Phase 2: Tách DAG thành nhiều DAGs** (3-5 ngày)

#### Bước 2.1: Tạo DAG Crawl
```python
# airflow/dags/tiki_crawl_dag.py
# Chỉ chứa: Extract → Crawl → Save
# Schedule: @daily
# Outlets: tiki://products/raw, tiki://products/with_detail
```

#### Bước 2.2: Tạo DAG Transform
```python
# airflow/dags/tiki_transform_dag.py
# Chỉ chứa: Transform
# Schedule: [tiki://products/with_detail]
# Ins: tiki://products/with_detail
# Outlets: tiki://products/transformed
```

#### Bước 2.3: Tạo DAG Load
```python
# airflow/dags/tiki_load_dag.py
# Chỉ chứa: Load
# Schedule: [tiki://products/transformed]
# Ins: tiki://products/transformed
# Outlets: tiki://products/final
```

#### Bước 2.4: Tạo DAG Validate
```python
# airflow/dags/tiki_validate_dag.py
# Chỉ chứa: Validate & Aggregate
# Schedule: [tiki://products/final]
# Ins: tiki://products/final
```

#### Bước 2.5: Test từng DAG
```bash
# Test DAG crawl
airflow dags test tiki_crawl 2025-01-01

# Test DAG transform (sau khi crawl xong)
airflow dags test tiki_transform 2025-01-01

# Test DAG load (sau khi transform xong)
airflow dags test tiki_load 2025-01-01
```

#### Bước 2.6: Migration từ DAG cũ
1. **Disable DAG cũ**: `tiki_crawl_products_dag` (không xóa, giữ backup)
2. **Enable DAGs mới**: `tiki_crawl`, `tiki_transform`, `tiki_load`, `tiki_validate`
3. **Monitor**: Đảm bảo data flow đúng

**✅ Deliverable**: 4 DAGs độc lập chạy với Asset scheduling

---

### **Phase 3: Thêm DAGs mới** (2-3 ngày)

#### Bước 3.1: Tạo DAG Analytics
```python
# airflow/dags/tiki_analytics_dag.py
# Schedule: [tiki://products/transformed]
# Tasks:
#   - analyze_products: Phân tích products
#   - generate_metrics: Tạo metrics
#   - save_analytics: Lưu kết quả
```

#### Bước 3.2: Tạo DAG Reporting
```python
# airflow/dags/tiki_reporting_dag.py
# Schedule: [tiki://products/final]
# Tasks:
#   - generate_report: Tạo báo cáo
#   - send_email: Gửi email
```

#### Bước 3.3: Test DAGs mới
```bash
# Test analytics DAG
airflow dags test tiki_analytics 2025-01-01

# Test reporting DAG
airflow dags test tiki_reporting 2025-01-01
```

**✅ Deliverable**: DAGs Analytics và Reporting tự động chạy khi data sẵn sàng

---

## 🧪 Testing Plan

### Unit Tests
```python
# Test asset definitions
def test_asset_definitions():
    assert RAW_PRODUCTS_DATASET.uri == "tiki://products/raw"
    assert get_outlets_for_task("save_products") == [RAW_PRODUCTS_DATASET]

# Test asset scheduling enabled/disabled
def test_asset_scheduling_toggle():
    # Test khi TIKI_USE_ASSET_SCHEDULING = false
    # Test khi TIKI_USE_ASSET_SCHEDULING = true
```

### Integration Tests
```bash
# Test end-to-end flow
1. Trigger tiki_crawl_dag
2. Verify tiki://products/raw được tạo
3. Verify tiki_transform_dag tự động trigger
4. Verify tiki://products/transformed được tạo
5. Verify tiki_load_dag tự động trigger
```

### Manual Tests
1. **Test trong Airflow UI**:
   - Xem Datasets menu
   - Xem DAG graph với Asset arrows
   - Xem Dataset update history

2. **Test data flow**:
   - Trigger crawl → verify transform tự động chạy
   - Trigger transform → verify load tự động chạy

---

## 📊 Monitoring & Alerting

### Metrics cần theo dõi
1. **Dataset Update Frequency**
   - `tiki://products/raw` được update bao lâu một lần?
   - Có bị miss update không?

2. **DAG Trigger Latency**
   - Thời gian từ khi dataset update đến khi DAG trigger
   - Target: < 1 phút

3. **Data Flow Health**
   - Crawl → Transform → Load có chạy đúng thứ tự không?
   - Có DAG nào bị stuck không?

### Alerts
```python
# Alert khi dataset không được update trong 24h
# Alert khi DAG không trigger sau khi dataset update
# Alert khi data flow bị break
```

---

## 🔄 Rollback Plan

### Nếu có vấn đề

#### Option 1: Tắt Asset (nhanh)
```bash
airflow variables set TIKI_USE_ASSET_SCHEDULING false
# DAG sẽ quay về chạy như cũ (không có Asset)
```

#### Option 2: Quay về DAG cũ
```bash
# Disable DAGs mới
airflow dags pause tiki_crawl tiki_transform tiki_load tiki_validate

# Enable DAG cũ
airflow dags unpause tiki_crawl_products
```

#### Option 3: Fix và retry
- Fix lỗi trong DAGs mới
- Test lại
- Re-enable

---

## 📅 Timeline

| Phase | Thời gian | Người phụ trách | Status |
|-------|-----------|-----------------|--------|
| **Phase 1**: Bật Asset trong DAG hiện tại | 1-2 ngày | Dev | ⏳ Pending |
| **Phase 2**: Tách thành nhiều DAGs | 3-5 ngày | Dev | ⏳ Pending |
| **Phase 3**: Thêm DAGs mới | 2-3 ngày | Dev | ⏳ Pending |
| **Testing & Monitoring** | 2-3 ngày | QA + Dev | ⏳ Pending |
| **Tổng cộng** | **8-13 ngày** | | |

---

## ✅ Checklist

### Phase 1: Bật Asset
- [ ] Kiểm tra Airflow version >= 2.7
- [ ] Set Variable `TIKI_USE_ASSET_SCHEDULING = true`
- [ ] Test DAG parsing
- [ ] Verify datasets trong UI
- [ ] Test DAG chạy với Asset
- [ ] Document changes

### Phase 2: Tách DAGs
- [ ] Tạo `tiki_crawl_dag.py`
- [ ] Tạo `tiki_transform_dag.py`
- [ ] Tạo `tiki_load_dag.py`
- [ ] Tạo `tiki_validate_dag.py`
- [ ] Test từng DAG
- [ ] Test data flow end-to-end
- [ ] Disable DAG cũ
- [ ] Enable DAGs mới
- [ ] Monitor 1 tuần

### Phase 3: DAGs mới
- [ ] Tạo `tiki_analytics_dag.py`
- [ ] Tạo `tiki_reporting_dag.py`
- [ ] Test DAGs mới
- [ ] Document DAGs mới

### Final
- [ ] Update documentation
- [ ] Training team
- [ ] Setup monitoring
- [ ] Setup alerts

---

## 📚 Tài liệu tham khảo

- [Airflow Asset Scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html)
- [Airflow Datasets](https://airflow.apache.org/docs/apache-airflow/stable/concepts/datasets.html)
- [docs/ASSET_SCHEDULING.md](./ASSET_SCHEDULING.md)

---

## 🚀 Next Steps

1. **Review kế hoạch** với team
2. **Approve timeline** và resources
3. **Bắt đầu Phase 1**: Bật Asset trong DAG hiện tại
4. **Monitor và adjust** kế hoạch nếu cần

