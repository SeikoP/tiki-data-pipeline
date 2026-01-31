"""
Auto demo - sends all Discord notifications without user input.
"""

import sys
import time

sys.path.insert(0, ".")

from src.common.notifications.discord import DiscordNotifier

print("\n" + "=" * 70)
print("🚀 AUTO DISCORD NOTIFICATION DEMO")
print("=" * 70)

notifier = DiscordNotifier()

if not notifier.enabled or not notifier.webhook_url:
    print("\n❌ Discord not enabled or no webhook URL")
    print("   Set DISCORD_ENABLED=true and DISCORD_WEBHOOK_URL in .env")
    sys.exit(1)

print(f"\n✅ Discord enabled")
print(f"✅ Webhook configured")
if notifier.fallback_webhook_url:
    print(f"✅ Fallback webhook configured")

# Demo 1: Basic
print("\n📤 1/4: Sending basic message...")
notifier.send_message(
    content="🧪 **Test Message**\n\nImproved notification system with retry & fallback.",
    title="Test Notification",
    color=0x00FF00,
)
time.sleep(1)

# Demo 2: With fields
print("📤 2/4: Sending message with fields...")
notifier.send_message(
    content="Tổng hợp dữ liệu crawl Tiki.vn\n```\nThành công: 90.0% (720/800)\n```",
    title="📊 Tổng hợp dữ liệu Tiki",
    color=0x00B894,
    fields=[
        {"name": "Total", "value": "1,000", "inline": True},
        {"name": "Crawled", "value": "800", "inline": True},
        {"name": "Success", "value": "720 (90.0%)", "inline": True},
        {"name": "Trend", "value": "📈 +2.5% vs previous", "inline": False},
    ],
    footer="Crawl lúc: 2026-02-01 10:00:00",
)
time.sleep(1)

# Demo 3: Long message (auto-split)
print("📤 3/4: Sending long message (will auto-split)...")
long_content = "A" * 2500 + "\n\nThis message is over 1800 chars and will be split automatically."
notifier.send_message(
    content=long_content,
    title="🤖 Long Message Test",
    color=0x3498DB,
)
time.sleep(1)

# Demo 4: Color coding
print("📤 4/4: Sending color-coded messages...")
for success_rate, color, label in [(95, 0x00B894, "Excellent"), (65, 0xF39C12, "Warning"), (35, 0xE74C3C, "Critical")]:
    notifier.send_message(
        content=f"Success rate: {success_rate}%",
        title=f"Test: {label}",
        color=color,
    )
    time.sleep(0.5)

print("\n" + "=" * 70)
print("✅ ALL MESSAGES SENT")
print("=" * 70)
print("\nCheck your Discord channel!")
