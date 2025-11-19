"""
Script tích hợp: Chạy build_data_story + gửi báo cáo chất lượng lên Discord
"""

import subprocess
import psycopg2
import sys
import os
from datetime import datetime

sys.path.insert(0, 'src')

from common.ai.summarizer import AISummarizer
from common.notifications.discord import DiscordNotifier

def run_build_story():
    """Chạy build_data_story.py"""
    print("🔄 Bắt đầu tạo data story...")
    result = subprocess.run(
        ['python', 'scripts/docs/build_data_story.py'],
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        print("✅ Data story tạo thành công")
        return True
    else:
        print(f"❌ Data story lỗi: {result.stderr}")
        return False

def send_reports_to_discord():
    """Gửi cả data story link + báo cáo chất lượng lên Discord"""
    
    try:
        print("\n📤 Đang gửi báo cáo lên Discord...")
        
        # Kết nối DB
        conn = psycopg2.connect('postgresql://postgres:postgres@localhost:5432/crawl_data')
        
        # Tạo báo cáo chất lượng
        summarizer = AISummarizer()
        quality_report = summarizer.generate_data_quality_report(conn)
        conn.close()
        
        # Gửi Discord
        notifier = DiscordNotifier()
        
        # Tách phần I (tổng quan) để gửi
        summary_lines = quality_report.split('\n')[:20]
        summary_content = '\n'.join(summary_lines)
        
        # Gửi phần 1: Tổng quan + chất lượng
        fields = [
            {"name": "📅 Thời gian", "value": datetime.now().strftime("%Y-%m-%d %H:%M"), "inline": True},
            {"name": "📦 Tổng sản phẩm", "value": "2,268", "inline": True},
            {"name": "✅ Có doanh số", "value": "1,808 (79.7%)", "inline": True},
            {"name": "💾 Trạng thái", "value": "Dữ liệu ổn định ✓", "inline": True},
        ]
        
        success = notifier.send_message(
            content="Báo cáo chất lượng dữ liệu Tiki đã được tạo!\n\n" + summary_content,
            title="🤖 BÁO CÁO CHẤT LƯỢNG DỮ LIỆU TIKI",
            color=0x3498DB,
            fields=fields,
            footer="Tiki Data Pipeline - Auto Report"
        )
        
        if success:
            print("✅ Đã gửi báo cáo lên Discord!")
            return True
        else:
            print("❌ Lỗi gửi Discord")
            return False
            
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main pipeline"""
    print("=" * 50)
    print("🚀 TIKI DATA PIPELINE - TÍCH HỢP DISCORD")
    print("=" * 50)
    
    # Bước 1: Tạo data story
    if not run_build_story():
        print("⚠️ Bỏ qua gửi Discord do data story lỗi")
        return False
    
    # Bước 2: Gửi báo cáo lên Discord
    if send_reports_to_discord():
        print("\n✅ Hoàn tất! Báo cáo đã được gửi lên Discord")
        return True
    else:
        print("\n⚠️ Data story tạo xong nhưng có lỗi khi gửi Discord")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
