"""
Script test để kiểm tra chức năng tổng hợp dữ liệu với AI và gửi thông báo qua Discord
"""

import json
import os
import sys
from pathlib import Path

# Fix encoding cho Windows console
if sys.platform == "win32":
    import io

    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# Thêm src vào path - cần thêm cả common, pipelines và crawl
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
src_path = os.path.join(project_root, "src")
common_path = os.path.join(src_path, "common")
pipelines_path = os.path.join(src_path, "pipelines")
crawl_path = os.path.join(pipelines_path, "crawl")

# Load .env từ src/common/ nếu có
env_file = os.path.join(common_path, ".env")
if os.path.exists(env_file):
    try:
        from dotenv import load_dotenv

        load_dotenv(env_file)
        print(f"✅ Đã load .env từ: {env_file}")
    except ImportError:
        print("⚠️  python-dotenv chưa được cài đặt, bỏ qua load .env")
    except Exception as e:
        print(f"⚠️  Lỗi khi load .env: {e}")

# Thêm các path vào sys.path
for path in [project_root, src_path, common_path, pipelines_path, crawl_path]:
    if path not in sys.path:
        sys.path.insert(0, path)

import importlib.util  # noqa: E402

# Setup package structure để relative imports hoạt động
import types  # noqa: E402

# Tạo package structure trong sys.modules
if "pipelines" not in sys.modules:
    sys.modules["pipelines"] = types.ModuleType("pipelines")
if "pipelines.crawl" not in sys.modules:
    sys.modules["pipelines.crawl"] = types.ModuleType("pipelines.crawl")
if "common" not in sys.modules:
    sys.modules["common"] = types.ModuleType("common")
if "common.analytics" not in sys.modules:
    sys.modules["common.analytics"] = types.ModuleType("common.analytics")
if "common.ai" not in sys.modules:
    sys.modules["common.ai"] = types.ModuleType("common.ai")
if "common.notifications" not in sys.modules:
    sys.modules["common.notifications"] = types.ModuleType("common.notifications")

# Import config trước (cần thiết cho các module khác)
config_path = os.path.join(crawl_path, "config.py")
spec = importlib.util.spec_from_file_location("pipelines.crawl.config", config_path)
config_module = importlib.util.module_from_spec(spec)
sys.modules["pipelines.crawl.config"] = config_module
spec.loader.exec_module(config_module)

# Import DataAggregator từ common/analytics/
aggregator_path = os.path.join(common_path, "analytics", "aggregator.py")
spec = importlib.util.spec_from_file_location("common.analytics.aggregator", aggregator_path)
aggregator_module = importlib.util.module_from_spec(spec)
sys.modules["common.analytics.aggregator"] = aggregator_module
spec.loader.exec_module(aggregator_module)
DataAggregator = aggregator_module.DataAggregator

# Import AISummarizer từ common/ai/
summarizer_path = os.path.join(common_path, "ai", "summarizer.py")
spec = importlib.util.spec_from_file_location("common.ai.summarizer", summarizer_path)
summarizer_module = importlib.util.module_from_spec(spec)
sys.modules["common.ai.summarizer"] = summarizer_module
spec.loader.exec_module(summarizer_module)
AISummarizer = summarizer_module.AISummarizer

# Import DiscordNotifier từ common/notifications/
discord_path = os.path.join(common_path, "notifications", "discord.py")
spec = importlib.util.spec_from_file_location("common.notifications.discord", discord_path)
discord_module = importlib.util.module_from_spec(spec)
sys.modules["common.notifications.discord"] = discord_module
spec.loader.exec_module(discord_module)
DiscordNotifier = discord_module.DiscordNotifier


def create_sample_data_file(output_path: str) -> str:
    """Tạo file dữ liệu mẫu để test"""
    sample_data = {
        "total_products": 100,
        "stats": {
            "cached": 5,
            "failed": 10,
            "timeout": 15,
            "degraded": 0,
            "with_detail": 85,
            "error_details": {"timeout": 15, "selenium_error": 10},
            "products_saved": 85,
            "total_products": 100,
            "products_skipped": 0,
            "circuit_breaker_open": 0,
            "failed_products_count": 10,
            "products_cached_skipped": 0,
            "products_failed_skipped": 0,
        },
        "crawled_at": "2025-11-15T14:00:00.000000",
        "note": "Dữ liệu test cho AI summary",
        "products": [
            {
                "url": "https://tiki.vn/p/123456",
                "name": "Sản phẩm Test 1",
                "brand": "Thương hiệu: Test",
                "price": {
                    "currency": "VND",
                    "current_price": 100000,
                    "original_price": 150000,
                    "discount_percent": 33.3,
                },
                "stock": {"quantity": 10, "available": True, "stock_status": "in_stock"},
                "rating": {"average": 4.5, "total_reviews": 100, "rating_distribution": {}},
                "seller": {"name": "Test Seller", "seller_id": "seller_123", "is_official": True},
                "shipping": {
                    "delivery_time": "2-3 ngày",
                    "fast_delivery": True,
                    "free_shipping": True,
                },
                "image_url": "https://example.com/image.jpg",
                "crawled_at": "2025-11-15 14:00:00",
                "product_id": "123456",
                "sales_count": 500,
                "category_url": "https://tiki.vn/test/c123",
                "detail_status": "success",
                "detail_crawled_at": "2025-11-15T14:00:00.000000",
            },
            {
                "url": "https://tiki.vn/p/789012",
                "name": "Sản phẩm Test 2",
                "brand": "Thương hiệu: Test 2",
                "price": {
                    "currency": "VND",
                    "current_price": 200000,
                    "original_price": 250000,
                    "discount_percent": 20.0,
                },
                "stock": {"quantity": 5, "available": True, "stock_status": "in_stock"},
                "rating": {"average": 4.8, "total_reviews": 200, "rating_distribution": {}},
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
                "image_url": "https://example.com/image2.jpg",
                "crawled_at": "2025-11-15 14:00:00",
                "product_id": "789012",
                "sales_count": 1000,
                "category_url": "https://tiki.vn/test/c456",
                "detail_status": "success",
                "detail_crawled_at": "2025-11-15T14:00:00.000000",
            },
        ],
    }

    # Tạo thư mục nếu chưa có
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    # Ghi file
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(sample_data, f, ensure_ascii=False, indent=2)

    print(f"✅ Đã tạo file dữ liệu mẫu: {output_path}")
    return output_path


def test_data_aggregator():
    """Test 1: Kiểm tra DataAggregator"""
    print("=" * 70)
    print("🧪 TEST 1: DataAggregator - Tổng hợp dữ liệu")
    print("=" * 70)

    # Tạo file dữ liệu mẫu
    test_data_file = os.path.join(
        os.path.dirname(__file__), "..", "data", "test_output", "test_products.json"
    )
    create_sample_data_file(test_data_file)

    try:
        aggregator = DataAggregator(test_data_file)

        # Test load data
        print("\n📂 Test load dữ liệu...")
        load_success = aggregator.load_data()
        assert load_success, "❌ Không thể load dữ liệu"
        print("✅ Load dữ liệu thành công")

        # Test aggregate
        print("\n📊 Test tổng hợp dữ liệu...")
        summary = aggregator.aggregate()

        assert summary, "❌ Summary không được tạo"
        assert "statistics" in summary, "❌ Thiếu statistics trong summary"
        assert "metadata" in summary, "❌ Thiếu metadata trong summary"

        stats = summary.get("statistics", {})
        print(f"   📦 Tổng sản phẩm: {stats.get('total_products', 0)}")
        print(f"   ✅ Có chi tiết: {stats.get('with_detail', 0)}")
        print(f"   ❌ Thất bại: {stats.get('failed', 0)}")
        print(f"   ⏱️  Timeout: {stats.get('timeout', 0)}")

        # Kiểm tra price analysis
        if "price_analysis" in summary:
            price_analysis = summary["price_analysis"]
            print("\n💰 Phân tích giá:")
            print(f"   - Giá trung bình: {price_analysis.get('avg_price', 0):,.0f} VND")
            print(f"   - Giá min: {price_analysis.get('min_price', 0):,.0f} VND")
            print(f"   - Giá max: {price_analysis.get('max_price', 0):,.0f} VND")
            if "avg_discount" in price_analysis:
                print(f"   - Giảm giá trung bình: {price_analysis.get('avg_discount', 0):.1f}%")

        # Kiểm tra rating analysis
        if "rating_analysis" in summary:
            rating_analysis = summary["rating_analysis"]
            print("\n⭐ Phân tích đánh giá:")
            print(f"   - Rating trung bình: {rating_analysis.get('avg_rating', 0):.2f}")
            print(f"   - Rating min: {rating_analysis.get('min_rating', 0):.2f}")
            print(f"   - Rating max: {rating_analysis.get('max_rating', 0):.2f}")

        print("\n✅ Test DataAggregator thành công!")
        return summary

    except Exception as e:
        print(f"\n❌ Test DataAggregator thất bại: {e}")
        import traceback

        traceback.print_exc()
        return None


def test_ai_summarizer(summary=None):
    """Test 2: Kiểm tra AISummarizer"""
    print("\n" + "=" * 70)
    print("🧪 TEST 2: AISummarizer - Tổng hợp với Groq AI")
    print("=" * 70)

    if not summary:
        print("⚠️  Không có summary để test, bỏ qua...")
        return None

    try:
        summarizer = AISummarizer()

        # Kiểm tra config
        print("\n⚙️  Cấu hình:")
        print(f"   - Enabled: {summarizer.enabled}")
        print(f"   - Model: {summarizer.model}")
        print(f"   - API Key: {'✅ Có' if summarizer.api_key else '❌ Không có'}")

        if not summarizer.enabled or not summarizer.api_key:
            print("\n⚠️  Groq AI chưa được bật hoặc thiếu API key")
            print("   💡 Để test đầy đủ, cần set environment variables:")
            print("      - GROQ_ENABLED=true")
            print("      - GROQ_API_KEY=your_api_key")
            print("\n   ⏭️  Bỏ qua test AI summary (chỉ test cấu hình)")
            return None

        # Test summarize
        print("\n🤖 Test tổng hợp với AI...")
        ai_summary = summarizer.summarize_data(summary, max_tokens=1000)

        if ai_summary:
            print(f"✅ Nhận được summary từ AI ({len(ai_summary)} ký tự)")
            print("\n📝 Nội dung summary:")
            print("-" * 70)
            print(ai_summary[:500] + "..." if len(ai_summary) > 500 else ai_summary)
            print("-" * 70)
            return ai_summary
        else:
            print("⚠️  Không nhận được summary từ AI")
            return None

    except Exception as e:
        print(f"\n❌ Test AISummarizer thất bại: {e}")
        import traceback

        traceback.print_exc()
        return None


def test_discord_notifier(ai_summary=None, stats=None):
    """Test 3: Kiểm tra DiscordNotifier"""
    print("\n" + "=" * 70)
    print("🧪 TEST 3: DiscordNotifier - Gửi thông báo qua Discord")
    print("=" * 70)

    try:
        notifier = DiscordNotifier()

        # Kiểm tra config
        print("\n⚙️  Cấu hình:")
        print(f"   - Enabled: {notifier.enabled}")
        print(f"   - Webhook URL: {'✅ Có' if notifier.webhook_url else '❌ Không có'}")

        if not notifier.enabled or not notifier.webhook_url:
            print("\n⚠️  Discord chưa được bật hoặc thiếu webhook URL")
            print("   💡 Để test đầy đủ, cần set environment variables:")
            print("      - DISCORD_ENABLED=true")
            print("      - DISCORD_WEBHOOK_URL=your_webhook_url")
            print("\n   ⏭️  Bỏ qua test gửi Discord (chỉ test cấu hình)")
            return False

        # Test send message
        print("\n📤 Test gửi thông báo...")

        if ai_summary and stats:
            # Gửi với AI summary
            success = notifier.send_summary(ai_summary=ai_summary, stats=stats)
        else:
            # Gửi message thông thường
            test_content = """📊 **Tổng hợp dữ liệu Tiki (Test)**

Đây là thông báo test từ script test_ai_summary_discord.py

**Thống kê:**
- 📦 Tổng sản phẩm: 100
- ✅ Có chi tiết: 85
- ❌ Thất bại: 10
- ⏱️ Timeout: 15

**Thời gian:** 2025-11-15T14:00:00
"""
            success = notifier.send_message(
                content=test_content,
                title="🧪 Test Notification",
                color=0x00FF00,
            )

        if success:
            print("✅ Đã gửi thông báo thành công qua Discord")
            return True
        else:
            print("⚠️  Không thể gửi thông báo qua Discord")
            return False

    except Exception as e:
        print(f"\n❌ Test DiscordNotifier thất bại: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_integration():
    """Test 4: Test tích hợp toàn bộ flow"""
    print("\n" + "=" * 70)
    print("🧪 TEST 4: Integration Test - Toàn bộ flow")
    print("=" * 70)

    try:
        # 1. Tạo và tổng hợp dữ liệu
        test_data_file = os.path.join(
            os.path.dirname(__file__), "..", "data", "test_output", "test_products.json"
        )
        create_sample_data_file(test_data_file)

        aggregator = DataAggregator(test_data_file)
        aggregator.load_data()
        summary = aggregator.aggregate()

        print("✅ Bước 1: Tổng hợp dữ liệu thành công")

        # 2. Tổng hợp với AI
        summarizer = AISummarizer()
        ai_summary = None
        if summarizer.enabled and summarizer.api_key:
            ai_summary = summarizer.summarize_data(summary, max_tokens=1000)
            if ai_summary:
                print("✅ Bước 2: Tổng hợp với AI thành công")
            else:
                print("⚠️  Bước 2: Không nhận được summary từ AI")
        else:
            print("⚠️  Bước 2: Bỏ qua (chưa cấu hình Groq)")

        # 3. Gửi thông báo Discord
        notifier = DiscordNotifier()
        if notifier.enabled and notifier.webhook_url:
            stats = summary.get("statistics", {})
            if ai_summary:
                success = notifier.send_summary(ai_summary=ai_summary, stats=stats)
            else:
                success = notifier.send_message(
                    content=f"📊 Tổng hợp dữ liệu test\n\nTổng sản phẩm: {stats.get('total_products', 0)}",
                    title="🧪 Test Integration",
                    color=0x3498DB,
                )

            if success:
                print("✅ Bước 3: Gửi thông báo Discord thành công")
            else:
                print("⚠️  Bước 3: Không thể gửi thông báo Discord")
        else:
            print("⚠️  Bước 3: Bỏ qua (chưa cấu hình Discord)")

        print("\n✅ Test tích hợp hoàn thành!")
        return True

    except Exception as e:
        print(f"\n❌ Test tích hợp thất bại: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_with_real_data():
    """Test 5: Test với dữ liệu thực từ products_with_detail.json"""
    print("\n" + "=" * 70)
    print("🧪 TEST 5: Test với dữ liệu thực")
    print("=" * 70)

    # Tìm file products_with_detail.json
    possible_paths = [
        os.path.join(
            os.path.dirname(__file__), "..", "data", "raw", "products", "products_with_detail.json"
        ),
        os.path.join(
            os.path.dirname(__file__), "..", "data", "demo", "products", "products_with_detail.json"
        ),
    ]

    data_file = None
    for path in possible_paths:
        if os.path.exists(path):
            data_file = path
            break

    if not data_file:
        print("⚠️  Không tìm thấy file products_with_detail.json")
        print("   💡 Cần crawl dữ liệu trước hoặc tạo file test")
        return None

    print(f"📂 Sử dụng file: {data_file}")

    try:
        aggregator = DataAggregator(data_file)
        if not aggregator.load_data():
            print("❌ Không thể load dữ liệu")
            return None

        summary = aggregator.aggregate()
        stats = summary.get("statistics", {})

        print("\n📊 Thống kê từ dữ liệu thực:")
        print(f"   📦 Tổng sản phẩm: {stats.get('total_products', 0)}")
        print(f"   ✅ Có chi tiết: {stats.get('with_detail', 0)}")
        print(f"   ❌ Thất bại: {stats.get('failed', 0)}")
        print(f"   ⏱️  Timeout: {stats.get('timeout', 0)}")

        # Test AI summary nếu có config
        summarizer = AISummarizer()
        if summarizer.enabled and summarizer.api_key:
            print("\n🤖 Đang tổng hợp với AI...")
            ai_summary = summarizer.summarize_data(summary, max_tokens=2000)
            if ai_summary:
                print(f"✅ Nhận được summary từ AI ({len(ai_summary)} ký tự)")
                print("\n📝 Preview (200 ký tự đầu):")
                print("-" * 70)
                print(ai_summary[:200] + "...")
                print("-" * 70)
                return ai_summary, stats

        return None, stats

    except Exception as e:
        print(f"\n❌ Test với dữ liệu thực thất bại: {e}")
        import traceback

        traceback.print_exc()
        return None, None


def main():
    """Chạy tất cả tests"""
    print("=" * 70)
    print("🧪 TEST AI SUMMARY VÀ DISCORD NOTIFICATION")
    print("=" * 70)

    results = {
        "data_aggregator": False,
        "ai_summarizer": False,
        "discord_notifier": False,
        "integration": False,
        "real_data": False,
    }

    # Test 1: DataAggregator
    try:
        summary = test_data_aggregator()
        if summary:
            results["data_aggregator"] = True
    except Exception as e:
        print(f"❌ Test DataAggregator lỗi: {e}")

    # Test 2: AISummarizer
    try:
        ai_summary = test_ai_summarizer(summary)
        if ai_summary:
            results["ai_summarizer"] = True
    except Exception as e:
        print(f"❌ Test AISummarizer lỗi: {e}")

    # Test 3: DiscordNotifier
    try:
        stats = summary.get("statistics", {}) if summary else {}
        discord_success = test_discord_notifier(ai_summary, stats)
        if discord_success:
            results["discord_notifier"] = True
    except Exception as e:
        print(f"❌ Test DiscordNotifier lỗi: {e}")

    # Test 4: Integration
    try:
        integration_success = test_integration()
        if integration_success:
            results["integration"] = True
    except Exception as e:
        print(f"❌ Test Integration lỗi: {e}")

    # Test 5: Real data
    try:
        real_ai_summary, real_stats = test_with_real_data()
        if real_ai_summary or real_stats:
            results["real_data"] = True
    except Exception as e:
        print(f"❌ Test Real Data lỗi: {e}")

    # Tổng kết
    print("\n" + "=" * 70)
    print("📋 TỔNG KẾT")
    print("=" * 70)
    print(f"✅ DataAggregator: {'✅ Thành công' if results['data_aggregator'] else '❌ Thất bại'}")
    print(
        f"✅ AISummarizer: {'✅ Thành công' if results['ai_summarizer'] else '⚠️  Bỏ qua (chưa config)'}"
    )
    print(
        f"✅ DiscordNotifier: {'✅ Thành công' if results['discord_notifier'] else '⚠️  Bỏ qua (chưa config)'}"
    )
    print(f"✅ Integration: {'✅ Thành công' if results['integration'] else '❌ Thất bại'}")
    print(f"✅ Real Data: {'✅ Thành công' if results['real_data'] else '⚠️  Bỏ qua'}")
    print("=" * 70)

    print("\n💡 Lưu ý:")
    print("   - Để test đầy đủ AI summary, cần set GROQ_ENABLED=true và GROQ_API_KEY")
    print("   - Để test đầy đủ Discord, cần set DISCORD_ENABLED=true và DISCORD_WEBHOOK_URL")
    print("=" * 70)


if __name__ == "__main__":
    main()
