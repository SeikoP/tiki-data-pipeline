"""
Demo script to test Discord notification with real webhook.
This will send actual messages to your Discord channel.
"""

import sys
import time

sys.path.insert(0, ".")

from src.common.notifications.discord import DiscordNotifier


def demo_basic_send():
    """Demo 1: Basic message send."""
    print("\n" + "=" * 70)
    print("DEMO 1: Basic Message Send")
    print("=" * 70)

    notifier = DiscordNotifier()

    if not notifier.enabled or not notifier.webhook_url:
        print("❌ Discord not enabled or no webhook URL")
        print("   Set DISCORD_ENABLED=true and DISCORD_WEBHOOK_URL in .env")
        return False

    print(f"✅ Discord enabled: {notifier.enabled}")
    print(f"✅ Webhook URL: {notifier.webhook_url[:50]}...")
    if notifier.fallback_webhook_url:
        print(f"✅ Fallback webhook: {notifier.fallback_webhook_url[:50]}...")

    print("\n📤 Sending basic message...")
    result = notifier.send_message(
        content="🧪 **Test Message**\n\nThis is a test from the improved notification system.",
        title="Test Notification",
        color=0x00FF00,
    )

    if result:
        print("✅ Message sent successfully!")
        return True
    else:
        print("❌ Failed to send message")
        return False


def demo_with_fields():
    """Demo 2: Message with fields (like real aggregate_and_notify)."""
    print("\n" + "=" * 70)
    print("DEMO 2: Message with Fields")
    print("=" * 70)

    notifier = DiscordNotifier()

    if not notifier.enabled or not notifier.webhook_url:
        print("❌ Discord not enabled")
        return False

    # Simulate real stats
    fields = [
        {"name": "Total Products", "value": "1,000", "inline": True},
        {"name": "Crawled", "value": "800", "inline": True},
        {"name": "Success", "value": "720 (90.0%)", "inline": True},
        {
            "name": "Error Analysis",
            "value": "**Total Errors: 80** (10.0%)\n• Failed: 50 (6.3%)\n• Timeout: 30 (3.8%)",
            "inline": False,
        },
        {"name": "Trend", "value": "📈 Success rate +2.5% vs previous", "inline": False},
        {"name": "Saved to DB", "value": "720", "inline": True},
    ]

    content = "Tổng hợp dữ liệu crawl Tiki.vn\n```\nThành công: 90.0% (720/800)\n```"

    print("\n📤 Sending message with fields...")
    result = notifier.send_message(
        content=content,
        title="📊 Tổng hợp dữ liệu Tiki",
        color=0x00B894,  # Green for success
        fields=fields,
        footer="Crawl lúc: 2026-02-01 10:00:00",
    )

    if result:
        print("✅ Message with fields sent successfully!")
        return True
    else:
        print("❌ Failed to send message")
        return False


def demo_long_message():
    """Demo 3: Long message that gets split."""
    print("\n" + "=" * 70)
    print("DEMO 3: Long Message (Auto-split)")
    print("=" * 70)

    notifier = DiscordNotifier()

    if not notifier.enabled or not notifier.webhook_url:
        print("❌ Discord not enabled")
        return False

    # Create a long message
    long_content = """
🤖 **BÁO CÁO PHÂN TÍCH DỮ LIỆU SẢN PHẨM TIKI**

I. Tổng Quan Thu Thập Dữ Liệu

📊 Quy mô dataset:
   • Tổng sản phẩm trong DB: 1,000
   • Sản phẩm có doanh số: 800 (80.0%)
   • Sản phẩm không có doanh số: 200 (20.0%)

✅ Chất lượng:
   • Hợp lệ đầy đủ: 800 / 1,000 = 80.0% ✓
   • Lỗi / thiếu dữ liệu: 20.0%
   • Đánh giá: Dữ liệu ở mức chấp nhận được

II. Phân Tích Chiến Lược Giảm Giá

💰 Mức giảm giá trên thị trường:
   • Trung bình: 15.5%
   • Phạm vi: 0.0% – 70.0%
   • Nhận định: Hầu hết sản phẩm áp dụng giảm giá nhẹ (<20%)

📌 Các sản phẩm giảm giá sâu (>20%):

1️⃣ Máy vệ sinh chăm sóc răng miệng bằng điện
   Giảm: 65.0% | Giá: 299,000đ | Bán: 1,234 cái

2️⃣ Bàn Ủi Đồ Để Bàn Thép ERMA
   Giảm: 60.0% | Giá: 450,000đ | Bán: 567 cái

3️⃣ Chổi Chà Sàn Nhà Tắm Kết Hợp Gạt Nước
   Giảm: 55.0% | Giá: 180,000đ | Bán: 890 cái

💡 Insight: Giảm 60-70% hiệu quả nếu sản phẩm có thương hiệu mạnh
   Giảm > 75% thường là tín hiệu 'thanh lý' hoặc 'giá gốc ảo'

III. Phân Tích Giá Cả

📈 Phân bố giá:
   • Min: 10,000đ
   • Max: 5,000,000đ
   • Trung bình: 500,000đ
   • Median: 350,000đ

💎 Phân khúc:
   • Giá rẻ (<200k): 35%
   • Trung bình (200k-1M): 50%
   • Cao cấp (>1M): 15%

IV. Đánh Giá & Rating

⭐ Chất lượng sản phẩm:
   • Rating trung bình: 4.3/5.0
   • Sản phẩm rating >4.0: 75%
   • Sản phẩm rating <3.0: 5%

📊 Phân bố rating:
   • 5 sao: 30%
   • 4-5 sao: 45%
   • 3-4 sao: 20%
   • <3 sao: 5%

V. Top Danh Mục

🏆 Top 5 danh mục bán chạy:
   1. Đồ gia dụng: 250 sản phẩm (31.3%)
   2. Điện tử: 180 sản phẩm (22.5%)
   3. Thời trang: 150 sản phẩm (18.8%)
   4. Sức khỏe: 120 sản phẩm (15.0%)
   5. Làm đẹp: 100 sản phẩm (12.5%)

VI. Nhận Xét & Đề Xuất

✅ Điểm mạnh:
   • Tỷ lệ thành công cao (90%)
   • Dữ liệu đầy đủ và chất lượng
   • Phân bố giá hợp lý

⚠️ Cần cải thiện:
   • Giảm timeout rate (3.8%)
   • Tăng coverage cho danh mục nhỏ
   • Optimize crawl speed

📈 Trend:
   • Success rate tăng 2.5% so với lần trước
   • Số lượng crawl tăng 100 products
   • Performance ổn định

🎯 Đề xuất:
   • Tiếp tục monitor timeout issues
   • Expand sang danh mục mới
   • Optimize cho mobile products
"""

    print(f"\n📝 Content length: {len(long_content)} chars")
    print("📤 Sending long message (will auto-split)...")

    result = notifier.send_message(
        content=long_content,
        title="🤖 Báo Cáo Phân Tích Chi Tiết",
        color=0x3498DB,
        footer="Generated by AI Summarizer",
    )

    if result:
        print("✅ Long message sent successfully (split into multiple embeds)!")
        return True
    else:
        print("❌ Failed to send message")
        return False


def demo_retry_simulation():
    """Demo 4: Simulate retry (will fail but show retry logic)."""
    print("\n" + "=" * 70)
    print("DEMO 4: Retry Logic (with invalid webhook)")
    print("=" * 70)

    notifier = DiscordNotifier()
    notifier.enabled = True
    notifier.webhook_url = "https://discord.com/api/webhooks/invalid/test"

    print("📤 Sending to invalid webhook (will retry 3 times)...")
    start = time.time()
    result = notifier.send_message(
        content="This should fail and retry",
        title="Retry Test",
        max_retries=3,
    )
    elapsed = time.time() - start

    print(f"\n⏱️  Total time: {elapsed:.2f}s (with exponential backoff)")
    print(f"Result: {'✅ Success' if result else '❌ Failed (as expected)'}")
    return True


def demo_color_coding():
    """Demo 5: Different colors based on success rate."""
    print("\n" + "=" * 70)
    print("DEMO 5: Color Coding by Success Rate")
    print("=" * 70)

    notifier = DiscordNotifier()

    if not notifier.enabled or not notifier.webhook_url:
        print("❌ Discord not enabled")
        return False

    scenarios = [
        (95, 0x00B894, "Excellent (>80%)"),
        (65, 0xF39C12, "Warning (50-80%)"),
        (35, 0xE74C3C, "Critical (<50%)"),
    ]

    for success_rate, color, label in scenarios:
        print(f"\n📤 Sending {label} notification...")
        result = notifier.send_message(
            content=f"Success rate: {success_rate}%\n```\nStatus: {label}\n```",
            title=f"Test: {label}",
            color=color,
            fields=[
                {"name": "Success Rate", "value": f"{success_rate}%", "inline": True},
                {"name": "Status", "value": label, "inline": True},
            ],
        )

        if result:
            print(f"   ✅ Sent")
        else:
            print(f"   ❌ Failed")

        time.sleep(1)  # Avoid rate limit

    return True


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("🚀 DISCORD NOTIFICATION DEMO")
    print("=" * 70)
    print("\nThis will send REAL messages to your Discord channel!")
    print("Make sure DISCORD_ENABLED=true and DISCORD_WEBHOOK_URL is set in .env")

    input("\nPress Enter to continue or Ctrl+C to cancel...")

    try:
        # Run demos
        demo_basic_send()
        time.sleep(2)

        demo_with_fields()
        time.sleep(2)

        demo_long_message()
        time.sleep(2)

        demo_color_coding()
        time.sleep(2)

        # Optional: Test retry (will fail)
        print("\n" + "=" * 70)
        test_retry = input("Test retry logic with invalid webhook? (y/n): ").lower() == "y"
        if test_retry:
            demo_retry_simulation()

        print("\n" + "=" * 70)
        print("✅ ALL DEMOS COMPLETED")
        print("=" * 70)
        print("\nCheck your Discord channel for the messages!")

    except KeyboardInterrupt:
        print("\n\n⚠️  Demo cancelled by user")
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback

        traceback.print_exc()
