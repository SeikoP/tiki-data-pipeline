"""
Test chi tiết cho Transform và Load pipeline.
"""

import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

# Fix encoding cho Windows console
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except AttributeError:
        pass

# Thêm src vào path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
src_path = os.path.join(project_root, "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from pipelines.load.loader import OptimizedDataLoader as DataLoader  # noqa: E402
from pipelines.transform.transformer import DataTransformer  # noqa: E402


def create_sample_products() -> list[dict[str, Any]]:
    """
    Tạo danh sách products mẫu để test.
    """
    return [
        {
            "product_id": "123456",
            "name": "Sản phẩm Test 1  ",
            "brand": "Thương hiệu: Test Brand",
            "url": "https://tiki.vn/p/123456",
            "image_url": "https://example.com/image1.jpg",
            "category_url": "https://tiki.vn/c/test",
            "sales_count": "500",
            "price": {
                "currency": "VND",
                "current_price": "100000",
                "original_price": "150000",
                "discount_percent": 33.3,
            },
            "rating": {
                "average": "4.5",
                "total_reviews": "100",
                "rating_distribution": {},
            },
            "stock": {
                "quantity": 10,
                "available": True,
                "stock_status": "in_stock",
            },
            "seller": {
                "name": "Test Seller",
                "seller_id": "seller_123",
                "is_official": True,
            },
            "shipping": {
                "delivery_time": "2-3 ngày",
                "fast_delivery": True,
                "free_shipping": True,
            },
            "description": "Mô tả sản phẩm test 1",
            "specifications": {"color": "red", "size": "M"},
            "images": ["https://example.com/img1.jpg"],
            "crawled_at": "2025-01-15 14:00:00",
        },
        {
            "product_id": "789012",
            "name": "Sản phẩm Test 2",
            "brand": "Test Brand 2",
            "url": "https://tiki.vn/p/789012",
            "image_url": "https://example.com/image2.jpg",
            "category_url": "https://tiki.vn/c/test2",
            "sales_count": 1000,
            "price": {
                "currency": "VND",
                "current_price": 200000,
                "original_price": 250000,
                "discount_percent": 20.0,
            },
            "rating": {
                "average": 4.8,
                "total_reviews": 200,
                "rating_distribution": {},
            },
            "stock": {
                "quantity": 5,
                "available": True,
                "stock_status": "in_stock",
            },
            "seller": {
                "name": "Test Seller 2",
                "seller_id": "seller_456",
                "is_official": False,
            },
            "shipping": {
                "delivery_time": "3-5 ngày",
                "fast_delivery": False,
                "free_shipping": False,
            },
            "description": "Mô tả sản phẩm test 2",
            "specifications": {"color": "blue", "size": "L"},
            "images": ["https://example.com/img2.jpg"],
            "detail_crawled_at": "2025-01-15T14:30:00",
        },
        # Product với dữ liệu không hợp lệ
        {
            "product_id": "invalid",
            "name": "",
            "url": "invalid-url",
            "price": {
                "current_price": -100,  # Price âm
                "original_price": 100,
            },
            "rating": {
                "average": 6.0,  # Rating > 5
                "total_reviews": -10,  # Review count âm
            },
            "sales_count": -50,  # Sales count âm
        },
        # Product thiếu required fields
        {
            "name": "Product without ID",
            "url": "https://tiki.vn/p/999999",
        },
        # Product duplicate (sẽ test sau)
        {
            "product_id": "123456",  # Duplicate với product đầu tiên
            "name": "Duplicate Product",
            "url": "https://tiki.vn/p/123456-duplicate",
            "sales_count": 999,
        },
    ]


def test_transform_validation():
    """Test 1: Transform validation"""
    print("=" * 70)
    print("🧪 TEST 1: Transform - Validation")
    print("=" * 70)

    products = create_sample_products()
    transformer = DataTransformer(strict_validation=False, remove_invalid=True)

    transformed, stats = transformer.transform_products(products, validate=True)

    print("\n📊 Thống kê:")
    print(f"   - Tổng products: {stats['total_processed']}")
    print(f"   - Valid: {stats['valid_products']}")
    print(f"   - Invalid: {stats['invalid_products']}")
    print(f"   - Duplicates removed: {stats['duplicates_removed']}")
    print(f"   - Errors: {len(stats['errors'])}")

    if stats["errors"]:
        print("\n⚠️  Lỗi:")
        for error in stats["errors"][:5]:  # Chỉ hiển thị 5 lỗi đầu
            print(f"   - {error}")

    # Kiểm tra kết quả
    assert stats["valid_products"] > 0, "Phải có ít nhất 1 product hợp lệ"
    assert stats["invalid_products"] > 0, "Phải có ít nhất 1 product không hợp lệ"
    assert len(transformed) == stats["valid_products"], "Số products transformed phải khớp"

    print("\n✅ Test validation thành công!")


def test_transform_normalization():
    """Test 2: Transform normalization"""
    print("\n" + "=" * 70)
    print("🧪 TEST 2: Transform - Normalization")
    print("=" * 70)

    products = create_sample_products()
    transformer = DataTransformer(normalize_fields=True)

    # Lấy product đầu tiên (có nhiều fields cần normalize)
    product = products[0]
    transformed = transformer.transform_product(product)

    print("\n📝 Product gốc:")
    print(f"   - name: '{product['name']}'")
    print(f"   - brand: '{product['brand']}'")
    print(f"   - sales_count: {product['sales_count']} (type: {type(product['sales_count'])})")
    print(
        f"   - price.current_price: {product['price']['current_price']} (type: {type(product['price']['current_price'])})"
    )

    print("\n📝 Product sau transform:")
    if transformed:
        print(f"   - name: '{transformed['name']}'")
        print(f"   - brand: '{transformed.get('brand')}'")
        print(
            f"   - sales_count: {transformed.get('sales_count')} (type: {type(transformed.get('sales_count'))})"
        )
        print(f"   - price: {transformed.get('price')} (type: {type(transformed.get('price'))})")
        print(f"   - original_price: {transformed.get('original_price')}")
        print(f"   - discount_percent: {transformed.get('discount_percent')}")
        print(f"   - rating_average: {transformed.get('rating_average')}")
        print(f"   - review_count: {transformed.get('review_count')}")

        # Kiểm tra normalization
        assert transformed["name"] == "Sản phẩm Test 1", "Name phải được trim"
        assert transformed.get("sales_count") == 500, "sales_count phải được parse thành int"
        assert transformed.get("price") == 100000.0, "price phải được parse thành float"
        assert transformed.get("original_price") == 150000.0, "original_price phải được parse"
        assert abs(transformed.get("discount_percent") - 33.33) < 0.01, (
            "discount_percent phải được tính lại và làm tròn 2 chữ số"
        )

    print("\n✅ Test normalization thành công!")


def test_transform_db_format():
    """Test 3: Transform to database format"""
    print("\n" + "=" * 70)
    print("🧪 TEST 3: Transform - Database Format")
    print("=" * 70)

    products = create_sample_products()
    transformer = DataTransformer()

    # Lấy 2 products đầu tiên
    for idx, product in enumerate(products[:2]):
        transformed = transformer.transform_product(product)

        if transformed:
            print(f"\n📦 Product {idx + 1} (DB format):")
            print(f"   - product_id: {transformed.get('product_id')}")
            print(f"   - name: {transformed.get('name')}")
            print(f"   - price: {transformed.get('price')}")
            print(f"   - original_price: {transformed.get('original_price')}")
            print(f"   - discount_percent: {transformed.get('discount_percent')}")
            print(f"   - rating_average: {transformed.get('rating_average')}")
            print(f"   - review_count: {transformed.get('review_count')}")
            print(f"   - sales_count: {transformed.get('sales_count')}")
            print(f"   - images: {type(transformed.get('images'))}")

            # Kiểm tra format
            assert transformed.get("product_id") is not None, "Phải có product_id"
            assert transformed.get("name") is not None, "Phải có name"
            assert "price" in transformed, "Phải có trường price (flatten từ dict)"
            assert "rating_average" in transformed, "Phải có rating_average (flatten từ dict)"

    print("\n✅ Test database format thành công!")


def test_transform_preserves_missing_metrics(monkeypatch):
    """Missing metrics must stay NULL-like instead of becoming fake zeroes."""
    monkeypatch.setenv("SHORT_NAME_USE_AI", "false")
    transformer = DataTransformer()

    transformed = transformer.transform_product(
        {
            "product_id": "111",
            "name": "Sản phẩm thiếu metric",
            "url": "https://tiki.vn/p/111",
            "price": {"current_price": 100000, "original_price": 100000},
            "seller": {"name": "Test Seller"},
            "brand": "Test Brand",
            "category_id": "123",
            "description": "<p>Mô tả <strong>sản phẩm</strong></p>",
        }
    )

    assert transformed is not None
    assert transformed["sales_count"] is None
    assert transformed["rating_average"] is None
    assert transformed["review_count"] is None
    assert transformed["description_text"] == "Mô tả sản phẩm"
    assert set(transformed["missing_fields"]) == {"review_count"}
    assert transformed["crawl_quality"] == "partial"
    assert transformed["detail_status"] == "partial_needs_enrichment"
    assert transformed["needs_enrichment"] is True


def test_transform_nullable_metrics_are_allowed_for_full_quality(monkeypatch):
    """Sales and rating can stay NULL when the source has no evidence for those metrics."""
    monkeypatch.setenv("SHORT_NAME_USE_AI", "false")
    transformer = DataTransformer()

    transformed = transformer.transform_product(
        {
            "product_id": "221",
            "name": "Sản phẩm không có specs",
            "url": "https://tiki.vn/p/221",
            "category_id": "123",
            "brand": "Test Brand",
            "price": {"current_price": 100000, "original_price": 120000},
            "rating": {"average": None, "total_reviews": 0},
            "sales_count": None,
            "seller": {"name": "Test Seller"},
            "images": [{"url": "https://example.com/image.jpg"}],
            "stock": {"available": False, "quantity": 0},
        }
    )

    assert transformed is not None
    assert "specifications" not in transformed
    assert transformed["rating_average"] is None
    assert transformed["review_count"] == 0
    assert transformed["sales_count"] is None
    assert transformed["stock_available"] is False
    assert transformed["stock_status"] == "out_of_stock"
    assert transformed["missing_fields"] == []
    assert transformed["crawl_quality"] == "full"
    assert transformed["detail_status"] == "full"
    assert transformed["needs_enrichment"] is False


def test_transform_stock_false_without_evidence_stays_unknown(monkeypatch):
    monkeypatch.setenv("SHORT_NAME_USE_AI", "false")
    transformer = DataTransformer()

    transformed = transformer.transform_product(
        {
            "product_id": "222",
            "name": "Sản phẩm fallback không rõ tồn kho",
            "url": "https://tiki.vn/p/222",
            "category_id": "123",
            "brand": "Test Brand",
            "price": {"current_price": 100000, "original_price": 100000},
            "rating": {"average": None, "total_reviews": 0},
            "seller": {"name": "Test Seller"},
            "images": [{"url": "https://example.com/image.jpg"}],
            "stock": {"available": False},
            "_metadata": {"crawl_status": "success_html", "extraction_method": "aiohttp"},
        }
    )

    assert transformed is not None
    assert transformed["stock_available"] is None
    assert transformed["stock_status"] == "unknown"
    assert transformed["stock_source"] == "detail_html"


def test_transform_inventory_out_of_stock_is_trusted(monkeypatch):
    monkeypatch.setenv("SHORT_NAME_USE_AI", "false")
    transformer = DataTransformer()

    transformed = transformer.transform_product(
        {
            "product_id": "9982840",
            "name": "Gel tắm trắng face & body D.S White (250ml)",
            "url": "https://tiki.vn/gel-tam-trang-face-body-d-s-white-250ml-p9982840.html",
            "category_id": "123",
            "brand": "D.S White",
            "price": {"current_price": 320000, "original_price": 320000},
            "rating": {"average": 5, "total_reviews": 1},
            "seller": {"name": "Tiki"},
            "images": [{"url": "https://example.com/image.jpg"}],
            "inventory_status": "out_of_stock",
            "_metadata": {"crawl_status": "success_api", "extraction_method": "api"},
        }
    )

    assert transformed is not None
    assert transformed["stock_available"] is False
    assert transformed["stock_status"] == "out_of_stock"
    assert transformed["stock_source"] == "detail_api"


def test_transform_missing_rating_is_flagged_when_reviews_exist(monkeypatch):
    monkeypatch.setenv("SHORT_NAME_USE_AI", "false")
    transformer = DataTransformer()

    transformed = transformer.transform_product(
        {
            "product_id": "223",
            "name": "Sản phẩm có review thiếu rating",
            "url": "https://tiki.vn/p/223",
            "category_id": "123",
            "brand": "Test Brand",
            "price": {"current_price": 100000, "original_price": 100000},
            "rating": {"average": None, "total_reviews": 12},
            "seller": {"name": "Test Seller"},
        }
    )

    assert transformed is not None
    assert transformed["discount_percent"] == 0.0
    assert transformed["missing_fields"] == ["rating_average"]
    assert transformed["crawl_quality"] == "partial"


def test_transform_full_quality_contract(monkeypatch):
    """A product with required and enrichment fields is marked full."""
    monkeypatch.setenv("SHORT_NAME_USE_AI", "false")
    transformer = DataTransformer()

    transformed = transformer.transform_product(
        {
            "product_id": "222",
            "name": "Sản phẩm đủ dữ liệu",
            "url": "https://tiki.vn/p/222",
            "category_id": "123",
            "brand": "Test Brand",
            "price": {"current_price": 100000, "original_price": 120000},
            "rating": {"average": 4.7, "total_reviews": 0},
            "sales_count": 0,
            "seller": {"name": "Test Seller"},
            "images": [{"url": "https://example.com/image.jpg"}],
        }
    )

    assert transformed is not None
    assert transformed["review_count"] == 0
    assert transformed["sales_count"] == 0
    assert transformed["image_url"] == "https://example.com/image.jpg"
    assert transformed["missing_fields"] == []
    assert transformed["crawl_quality"] == "full"
    assert transformed["detail_status"] == "full"
    assert transformed["needs_enrichment"] is False


def test_short_name_uses_local_heuristics_by_default(monkeypatch):
    """Short names should not depend on AI calls in the transform critical path."""
    monkeypatch.delenv("SHORT_NAME_USE_AI", raising=False)
    transformer = DataTransformer()

    assert transformer.ai_summarizer is None

    examples = [
        "Máy hút bụi mini cầm tay không dây cao cấp bảo hành 12 tháng",
        "Chổi Chà Sàn Nhà Tắm Kết Hợp Gạt Nước Đầu Chữ V Deli - Chính Hãng",
        "Bàn Chải Nylon Vệ Sinh Khe Hở Cửa Sổ / Khe Hở Nhà Tắm",
    ]

    for raw_name in examples:
        short_name = transformer._get_short_name(raw_name)
        assert short_name
        assert len(short_name.split()) <= 8
        assert "chính hãng" not in short_name.lower()
        assert "bảo hành" not in short_name.lower()

    assert "mini" in transformer._get_short_name(examples[0]).lower()
    assert "cầm tay" in transformer._get_short_name(examples[0]).lower()
    assert "không dây" in transformer._get_short_name(examples[0]).lower()


def test_transform_duplicates():
    """Test 4: Transform - Remove duplicates"""
    print("\n" + "=" * 70)
    print("🧪 TEST 4: Transform - Remove Duplicates")
    print("=" * 70)

    products = create_sample_products()
    # Thêm duplicate
    duplicate = products[0].copy()
    duplicate["name"] = "Duplicate Product"
    products.append(duplicate)

    transformer = DataTransformer(remove_invalid=True)
    transformed, stats = transformer.transform_products(products)

    print("\n📊 Thống kê:")
    print(f"   - Tổng products: {stats['total_processed']}")
    print(f"   - Valid: {stats['valid_products']}")
    print(f"   - Duplicates removed: {stats['duplicates_removed']}")

    # Kiểm tra không có duplicate
    product_ids = [p["product_id"] for p in transformed if p.get("product_id")]
    unique_ids = set(product_ids)
    assert len(product_ids) == len(unique_ids), "Không được có duplicate product_id"

    print("\n✅ Test remove duplicates thành công!")


def test_load_to_file():
    """Test 5: Load - Save to file"""
    print("\n" + "=" * 70)
    print("🧪 TEST 5: Load - Save to File")
    print("=" * 70)

    products = create_sample_products()
    transformer = DataTransformer(remove_invalid=True)
    transformed, _ = transformer.transform_products(products)

    # Filter valid products
    valid_products = [p for p in transformed if p.get("product_id") and p.get("name")]

    loader = DataLoader(enable_db=False)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as f:
        temp_file = f.name

    try:
        stats = loader.load_products(valid_products, save_to_file=temp_file)

        print("\n📊 Thống kê:")
        print(f"   - Tổng products: {stats['total_loaded']}")
        print(f"   - File loaded: {stats['file_loaded']}")
        print(f"   - Success: {stats['success_count']}")

        # Kiểm tra file đã được tạo
        assert Path(temp_file).exists(), "File phải được tạo"
        assert stats["file_loaded"] > 0, "Phải có products được lưu vào file"

        # Đọc và kiểm tra file
        with open(temp_file, encoding="utf-8") as f:
            data = json.load(f)
            assert "products" in data, "File phải có key 'products'"
            assert len(data["products"]) == stats["file_loaded"], "Số products phải khớp"
            assert "loaded_at" in data, "File phải có 'loaded_at'"

        print("\n✅ Test load to file thành công!")

    finally:
        # Cleanup
        if Path(temp_file).exists():
            Path(temp_file).unlink()


def test_load_from_file():
    """Test 6: Load - Load from file"""
    print("\n" + "=" * 70)
    print("🧪 TEST 6: Load - Load from File")
    print("=" * 70)

    # Tạo file test
    products = create_sample_products()
    transformer = DataTransformer(remove_invalid=True)
    transformed, _ = transformer.transform_products(products)
    valid_products = [p for p in transformed if p.get("product_id") and p.get("name")]

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as f:
        input_file = f.name
        json.dump({"products": valid_products}, f, ensure_ascii=False, indent=2)

    try:
        loader = DataLoader(enable_db=False)
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        ) as f:
            output_file = f.name

        try:
            stats = loader.load_from_file(input_file, save_to_db=False, save_to_file=output_file)

            print("\n📊 Thống kê:")
            print(f"   - File loaded: {stats['file_loaded']}")
            print(f"   - Success: {stats['success_count']}")

            assert stats["file_loaded"] > 0, "Phải có products được load từ file"

            print("\n✅ Test load from file thành công!")

        finally:
            if Path(output_file).exists():
                Path(output_file).unlink()

    finally:
        if Path(input_file).exists():
            Path(input_file).unlink()


def test_load_integration():
    """Test 7: Integration - Transform + Load"""
    print("\n" + "=" * 70)
    print("🧪 TEST 7: Integration - Transform + Load")
    print("=" * 70)

    products = create_sample_products()

    # Transform
    transformer = DataTransformer(remove_invalid=True, normalize_fields=True)
    transformed, transform_stats = transformer.transform_products(products, validate=True)

    print("\n📊 Transform stats:")
    print(f"   - Valid products: {transform_stats['valid_products']}")

    # Load
    loader = DataLoader(enable_db=False)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as f:
        output_file = f.name

    try:
        load_stats = loader.load_products(transformed, save_to_file=output_file)

        print("\n📊 Load stats:")
        print(f"   - File loaded: {load_stats['file_loaded']}")
        print(f"   - Success: {load_stats['success_count']}")

        assert transform_stats["valid_products"] == load_stats["file_loaded"], (
            "Số products transform và load phải khớp"
        )

        print("\n✅ Test integration thành công!")

    finally:
        if Path(output_file).exists():
            Path(output_file).unlink()


def test_edge_cases():
    """Test 8: Edge cases"""
    print("\n" + "=" * 70)
    print("🧪 TEST 8: Edge Cases")
    print("=" * 70)

    transformer = DataTransformer()

    # Test empty list
    transformed, stats = transformer.transform_products([])
    assert len(transformed) == 0, "Empty list phải trả về empty list"
    assert stats["total_processed"] == 0, "Stats phải có total_processed = 0"

    # Test None product
    transformed = transformer.transform_product(None)
    assert transformed is None, "None product phải trả về None"

    # Test product với missing fields
    minimal_product = {"product_id": "999", "name": "Test", "url": "https://tiki.vn/p/999"}
    transformed = transformer.transform_product(minimal_product)
    assert transformed is not None, "Minimal product phải được transform"

    print("\n✅ Test edge cases thành công!")


def main():
    """
    Chạy tất cả tests.
    """
    print("=" * 70)
    print("🧪 TEST TRANSFORM VÀ LOAD PIPELINE")
    print("=" * 70)

    results = {
        "transform_validation": False,
        "transform_normalization": False,
        "transform_db_format": False,
        "transform_duplicates": False,
        "load_to_file": False,
        "load_from_file": False,
        "integration": False,
        "edge_cases": False,
    }

    # Test 1: Transform validation
    try:
        test_transform_validation()
        results["transform_validation"] = True
    except Exception as e:
        print(f"\n❌ Test transform validation thất bại: {e}")
        import traceback

        traceback.print_exc()

    # Test 2: Transform normalization
    try:
        test_transform_normalization()
        results["transform_normalization"] = True
    except Exception as e:
        print(f"\n❌ Test transform normalization thất bại: {e}")

    # Test 3: Transform DB format
    try:
        test_transform_db_format()
        results["transform_db_format"] = True
    except Exception as e:
        print(f"\n❌ Test transform DB format thất bại: {e}")

    # Test 4: Transform duplicates
    try:
        test_transform_duplicates()
        results["transform_duplicates"] = True
    except Exception as e:
        print(f"\n❌ Test transform duplicates thất bại: {e}")

    # Test 5: Load to file
    try:
        test_load_to_file()
        results["load_to_file"] = True
    except Exception as e:
        print(f"\n❌ Test load to file thất bại: {e}")

    # Test 6: Load from file
    try:
        test_load_from_file()
        results["load_from_file"] = True
    except Exception as e:
        print(f"\n❌ Test load from file thất bại: {e}")

    # Test 7: Integration
    try:
        test_load_integration()
        results["integration"] = True
    except Exception as e:
        print(f"\n❌ Test integration thất bại: {e}")

    # Test 8: Edge cases
    try:
        test_edge_cases()
        results["edge_cases"] = True
    except Exception as e:
        print(f"\n❌ Test edge cases thất bại: {e}")

    # Tổng kết
    print("\n" + "=" * 70)
    print("📋 TỔNG KẾT")
    print("=" * 70)
    for test_name, success in results.items():
        status = "✅ Thành công" if success else "❌ Thất bại"
        print(f"   {test_name}: {status}")

    total = len(results)
    passed = sum(results.values())
    print(f"\n   Tổng: {passed}/{total} tests passed")
    print("=" * 70)


if __name__ == "__main__":
    main()
