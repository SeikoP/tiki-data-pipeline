"""
Script để gửi báo cáo chất lượng dữ liệu lên Discord
"""

import sys

import psycopg2

sys.path.insert(0, "src")

from common.ai.summarizer import AISummarizer
from common.notifications.discord import DiscordNotifier


def send_quality_report_to_discord():
    """Tạo báo cáo chất lượng và gửi lên Discord"""

    try:
        # Kết nối DB
        conn = psycopg2.connect("postgresql://postgres:postgres@localhost:5432/crawl_data")

        # Tạo báo cáo
        summarizer = AISummarizer()
        report = summarizer.generate_data_quality_report(conn)
        conn.close()

        # Gửi Discord
        notifier = DiscordNotifier()

        # Tách báo cáo thành các phần để hiển thị đẹp hơn
        lines = report.split("\n")

        # Tìm các phần để extract fields
        fields = []

        # Extract I. Tổng quan
        for i, line in enumerate(lines):
            if "Tổng sản phẩm trong DB:" in line:
                total = line.split(":")[1].strip()
                fields.append({"name": "📦 Tổng sản phẩm", "value": total, "inline": True})
            elif "Sản phẩm có doanh số:" in line:
                with_sales = line.split(":")[1].strip()
                fields.append({"name": "📊 Có doanh số", "value": with_sales, "inline": True})
            elif "Hợp lệ đầy đủ:" in line:
                quality = line.split(":")[1].strip()
                fields.append({"name": "✅ Tỷ lệ hoàn tất", "value": quality, "inline": True})
            elif "Trung bình:" in line and "giảm giá" in lines[i - 1]:
                avg_disc = line.split(":")[1].strip()
                fields.append({"name": "💰 Giảm giá TB", "value": avg_disc, "inline": True})
            elif "Phạm vi:" in line and "giảm" in lines[i - 1]:
                range_disc = line.split(":")[1].strip()
                fields.append({"name": "📈 Phạm vi giảm", "value": range_disc, "inline": True})

        # Gửi Discord
        success = notifier.send_message(
            content=report,
            title="🤖 BÁO CÁO CHẤT LƯỢNG DỮ LIỆU TIKI",
            color=0x3498DB,  # Xanh dương
            fields=fields[:5],  # Giới hạn 5 fields
            footer="Tiki Data Pipeline - AI Summary",
        )

        if success:
            print("✅ Đã gửi báo cáo lên Discord thành công!")
            return True
        else:
            print("❌ Lỗi gửi Discord (xem log để chi tiết)")
            return False

    except Exception as e:
        print(f"❌ Lỗi: {e}")
        return False


if __name__ == "__main__":
    send_quality_report_to_discord()
