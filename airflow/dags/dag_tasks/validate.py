"""
Validate and aggregate tasks for Tiki crawl products DAG
"""
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from ..dag_helpers.config import OUTPUT_FILE, OUTPUT_FILE_WITH_DETAIL
from ..dag_helpers.shared_state import DataAggregator, AISummarizer, DiscordNotifier
from ..dag_helpers.utils import Variable, get_logger


def validate_data(**context) -> dict[str, Any]:
    """
    Task 5: Validate dữ liệu đã crawl

    Returns:
        Dict: Kết quả validation
    """
    logger = get_logger(context)
    logger.info("=" * 70)
    logger.info("✅ TASK: Validate Data")
    logger.info("=" * 70)

    try:
        ti = context["ti"]
        output_file = None

        # Ưu tiên: Lấy từ save_products_with_detail (có detail)
        # Cách 1: Lấy từ task_id với TaskGroup prefix
        try:
            output_file = ti.xcom_pull(task_ids="crawl_product_details.save_products_with_detail")
            logger.info(f"Lấy output_file từ 'crawl_product_details.save_products_with_detail': {output_file}")
        except Exception as e:
            logger.warning(f"Không lấy được từ 'crawl_product_details.save_products_with_detail': {e}")

        # Cách 2: Thử không có prefix
        if not output_file:
            try:
                output_file = ti.xcom_pull(task_ids="save_products_with_detail")
                logger.info(f"Lấy output_file từ 'save_products_with_detail': {output_file}")
            except Exception as e:
                logger.warning(f"Không lấy được từ 'save_products_with_detail': {e}")

        # Fallback: Lấy từ save_products (không có detail) nếu không có file với detail
        if not output_file:
            try:
                output_file = ti.xcom_pull(task_ids="process_and_save.save_products")
                logger.info(f"Lấy output_file từ 'process_and_save.save_products' (fallback): {output_file}")
            except Exception as e:
                logger.warning(f"Không lấy được từ 'process_and_save.save_products': {e}")

        # Cách 3: Thử không có prefix
        if not output_file:
            try:
                output_file = ti.xcom_pull(task_ids="save_products")
                logger.info(f"Lấy output_file từ 'save_products' (fallback): {output_file}")
            except Exception as e:
                logger.warning(f"Không lấy được từ 'save_products': {e}")

        if not output_file or not os.path.exists(output_file):
            raise FileNotFoundError(f"Không tìm thấy file output: {output_file}")

        logger.info(f"Đang validate file: {output_file}")

        with open(output_file, encoding="utf-8") as f:
            data = json.load(f)

        products = data.get("products", [])
        stats = data.get("stats", {})

        # Validation
        validation_result = {
            "file_exists": True,
            "total_products": len(products),
            "crawled_count": stats.get("crawled_count", 0),  # Số lượng products được crawl detail
            "valid_products": 0,
            "invalid_products": 0,
            "errors": [],
        }

        required_fields = ["product_id", "name", "url"]

        for i, product in enumerate(products):
            is_valid = True
            missing_fields = []

            for field in required_fields:
                if not product.get(field):
                    is_valid = False
                    missing_fields.append(field)

            if is_valid:
                validation_result["valid_products"] += 1
            else:
                validation_result["invalid_products"] += 1
                validation_result["errors"].append(
                    {
                        "index": i,
                        "product_id": product.get("product_id"),
                        "missing_fields": missing_fields,
                    }
                )

        logger.info("=" * 70)
        logger.info("📊 VALIDATION RESULTS")
        logger.info("=" * 70)
        logger.info(f"📦 Tổng số products trong file: {validation_result['total_products']}")
        
        # Log thông tin về crawl detail nếu có
        crawled_count = stats.get("crawled_count", 0)
        if crawled_count > 0:
            logger.info(f"🔄 Products được crawl detail: {crawled_count}")
            logger.info(f"✅ Products có detail (success): {stats.get('with_detail', 0)}")
            if stats.get("timeout", 0) > 0:
                logger.info(f"⏱️  Products timeout: {stats.get('timeout', 0)}")
            if stats.get("failed", 0) > 0:
                logger.info(f"❌ Products failed: {stats.get('failed', 0)}")
        
        logger.info(f"✅ Valid products: {validation_result['valid_products']}")
        logger.info(f"❌ Invalid products: {validation_result['invalid_products']}")
        logger.info("=" * 70)

        if validation_result["invalid_products"] > 0:
            logger.warning(f"Có {validation_result['invalid_products']} sản phẩm không hợp lệ")
            # Không fail task, chỉ warning

        return validation_result

    except Exception as e:
        logger.error(f"❌ Lỗi khi validate data: {e}", exc_info=True)
        raise

def aggregate_and_notify(**context) -> dict[str, Any]:
    """
    Task: Tổng hợp dữ liệu với AI và gửi thông báo qua Discord

    Returns:
        Dict: Kết quả tổng hợp và gửi thông báo
    """
    logger = get_logger(context)
    logger.info("=" * 70)
    logger.info("🤖 TASK: Aggregate Data and Send Discord Notification")
    logger.info("=" * 70)

    result = {
        "aggregation_success": False,
        "ai_summary_success": False,
        "discord_notification_success": False,
        "summary": None,
        "ai_summary": None,
    }

    try:
        # Lấy đường dẫn file products_with_detail.json
        output_file = str(OUTPUT_FILE_WITH_DETAIL)

        if not os.path.exists(output_file):
            logger.warning(f"⚠️  File không tồn tại: {output_file}")
            logger.info("   Thử lấy từ XCom...")

            ti = context["ti"]
            try:
                output_file = ti.xcom_pull(
                    task_ids="crawl_product_details.save_products_with_detail"
                )
                logger.info(f"   Lấy từ XCom: {output_file}")
            except Exception:
                try:
                    output_file = ti.xcom_pull(task_ids="save_products_with_detail")
                    logger.info(f"   Lấy từ XCom (không có prefix): {output_file}")
                except Exception as e:
                    logger.warning(f"   Không lấy được từ XCom: {e}")

        if not output_file or not os.path.exists(output_file):
            raise FileNotFoundError(f"Không tìm thấy file output: {output_file}")

        logger.info(f"📊 Đang tổng hợp dữ liệu từ: {output_file}")

        # 1. Tổng hợp dữ liệu
        if DataAggregator is None:
            logger.warning("⚠️  DataAggregator module chưa được import, bỏ qua tổng hợp")
        else:
            try:
                aggregator = DataAggregator(output_file)
                if aggregator.load_data():
                    summary = aggregator.aggregate()
                    result["summary"] = summary
                    result["aggregation_success"] = True
                    logger.info("✅ Tổng hợp dữ liệu thành công")

                    # Log thống kê
                    stats = summary.get("statistics", {})
                    total_products = stats.get('total_products', 0)
                    crawled_count = stats.get('crawled_count', 0)
                    with_detail = stats.get('with_detail', 0)
                    failed = stats.get('failed', 0)
                    timeout = stats.get('timeout', 0)
                    
                    logger.info(f"   📦 Tổng sản phẩm: {total_products}")
                    logger.info(f"   🔄 Products được crawl detail: {crawled_count}")
                    logger.info(f"   ✅ Có chi tiết (success): {with_detail}")
                    logger.info(f"   ❌ Thất bại: {failed}")
                    logger.info(f"   ⏱️  Timeout: {timeout}")
                    
                    # Tính và hiển thị tỷ lệ thành công
                    if crawled_count > 0:
                        success_rate = (with_detail / crawled_count) * 100
                        logger.info(f"   📈 Tỷ lệ thành công: {with_detail}/{crawled_count} ({success_rate:.1f}%)")
                    else:
                        logger.warning("   ⚠️  Không có products nào được crawl detail")
                else:
                    logger.error("❌ Không thể load dữ liệu để tổng hợp")
            except Exception as e:
                logger.error(f"❌ Lỗi khi tổng hợp dữ liệu: {e}", exc_info=True)

        # 2. Tổng hợp với AI
        if AISummarizer is None:
            logger.warning("⚠️  AISummarizer module chưa được import, bỏ qua tổng hợp AI")
        elif result.get("summary"):
            try:
                summarizer = AISummarizer()
                ai_summary = summarizer.summarize_data(result["summary"])
                if ai_summary:
                    result["ai_summary"] = ai_summary
                    result["ai_summary_success"] = True
                    logger.info("✅ Tổng hợp với AI thành công")
                    logger.info(f"   Độ dài summary: {len(ai_summary)} ký tự")
                else:
                    logger.warning("⚠️  Không nhận được summary từ AI")
            except Exception as e:
                logger.error(f"❌ Lỗi khi tổng hợp với AI: {e}", exc_info=True)

        # 3. Gửi thông báo qua Discord
        if DiscordNotifier is None:
            logger.warning("⚠️  DiscordNotifier module chưa được import, bỏ qua gửi thông báo")
        else:
            try:
                notifier = DiscordNotifier()

                # Chuẩn bị nội dung
                if result.get("ai_summary"):
                    # Gửi với AI summary
                    stats = result.get("summary", {}).get("statistics", {})
                    crawled_at = result.get("summary", {}).get("metadata", {}).get("crawled_at", "")
                    footer_text = f"Crawl lúc: {crawled_at}" if crawled_at else "Tiki Data Pipeline"
                    
                    success = notifier.send_summary(
                        ai_summary=result["ai_summary"],
                        stats=stats,
                    )
                    if success:
                        result["discord_notification_success"] = True
                        logger.info("✅ Đã gửi thông báo qua Discord (với AI summary)")
                    else:
                        logger.warning("⚠️  Không thể gửi thông báo qua Discord")
                elif result.get("summary"):
                    # Gửi với summary thông thường (không có AI) - sử dụng fields thay vì text
                    stats = result.get("summary", {}).get("statistics", {})
                    total_products = stats.get('total_products', 0)
                    crawled_count = stats.get('crawled_count', 0)
                    with_detail = stats.get('with_detail', 0)
                    failed = stats.get('failed', 0)
                    timeout = stats.get('timeout', 0)
                    products_saved = stats.get('products_saved', 0)
                    crawled_at = result.get("summary", {}).get("metadata", {}).get("crawled_at", "N/A")
                    
                    # Tính tỷ lệ thành công để chọn màu
                    if crawled_count > 0:
                        success_rate = (with_detail / crawled_count) * 100
                        if success_rate >= 80:
                            color = 0x00FF00  # Xanh lá
                        elif success_rate >= 50:
                            color = 0xFFA500  # Cam
                        else:
                            color = 0xFF0000  # Đỏ
                    else:
                        color = 0x808080  # Xám
                        success_rate = 0
                    
                    # Tạo fields cho Discord embed
                    fields = []
                    
                    # Row 1: Tổng quan
                    if total_products > 0:
                        fields.append({
                            "name": "📦 Tổng sản phẩm",
                            "value": f"**{total_products:,}**",
                            "inline": True,
                        })
                    
                    if crawled_count > 0:
                        fields.append({
                            "name": "🔄 Đã crawl detail",
                            "value": f"**{crawled_count:,}**",
                            "inline": True,
                        })
                    
                    if products_saved > 0:
                        fields.append({
                            "name": "💾 Đã lưu",
                            "value": f"**{products_saved:,}**",
                            "inline": True,
                        })
                    
                    # Row 2: Kết quả crawl
                    if crawled_count > 0:
                        fields.append({
                            "name": "✅ Thành công",
                            "value": f"**{with_detail:,}** ({success_rate:.1f}%)",
                            "inline": True,
                        })
                    
                    if timeout > 0:
                        timeout_rate = (timeout / crawled_count * 100) if crawled_count > 0 else 0
                        fields.append({
                            "name": "⏱️ Timeout",
                            "value": f"**{timeout:,}** ({timeout_rate:.1f}%)",
                            "inline": True,
                        })
                    
                    if failed > 0:
                        failed_rate = (failed / crawled_count * 100) if crawled_count > 0 else 0
                        fields.append({
                            "name": "❌ Thất bại",
                            "value": f"**{failed:,}** ({failed_rate:.1f}%)",
                            "inline": True,
                        })
                    
                    # Tạo content ngắn gọn
                    content = "📊 **Tổng hợp dữ liệu crawl từ Tiki.vn**\n\n"
                    if crawled_count > 0:
                        content += f"Tỷ lệ thành công: **{success_rate:.1f}%** ({with_detail}/{crawled_count} products)"
                    else:
                        content += "Chưa có products nào được crawl detail."
                    
                    success = notifier.send_message(
                        content=content,
                        title="📊 Tổng hợp dữ liệu Tiki",
                        color=color,
                        fields=fields if fields else None,
                        footer=f"Crawl lúc: {crawled_at}",
                    )
                    if success:
                        result["discord_notification_success"] = True
                        logger.info("✅ Đã gửi thông báo qua Discord (không có AI)")
                    else:
                        logger.warning("⚠️  Không thể gửi thông báo qua Discord")
                else:
                    logger.warning("⚠️  Không có dữ liệu để gửi thông báo")
            except Exception as e:
                logger.error(f"❌ Lỗi khi gửi thông báo Discord: {e}", exc_info=True)

        logger.info("=" * 70)
        logger.info("📊 KẾT QUẢ TỔNG HỢP VÀ THÔNG BÁO")
        logger.info("=" * 70)
        logger.info(
            f"✅ Tổng hợp dữ liệu: {'Thành công' if result['aggregation_success'] else 'Thất bại'}"
        )
        logger.info(
            f"✅ Tổng hợp AI: {'Thành công' if result['ai_summary_success'] else 'Thất bại'}"
        )
        logger.info(
            f"✅ Gửi Discord: {'Thành công' if result['discord_notification_success'] else 'Thất bại'}"
        )
        logger.info("=" * 70)

        return result

    except Exception as e:
        logger.error(f"❌ Lỗi khi tổng hợp và gửi thông báo: {e}", exc_info=True)
        # Không fail task, chỉ log lỗi
        return result