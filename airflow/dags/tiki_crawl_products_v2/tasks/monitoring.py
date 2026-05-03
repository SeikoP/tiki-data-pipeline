from __future__ import annotations

# Import all bootstrap globals (paths, config, dynamic imports, singletons).
# This preserves legacy behavior without renaming any globals referenced by task callables.
from tiki_crawl_products_v2.bootstrap import (
    OUTPUT_FILE_WITH_DETAIL,
    Any,
    datetime,
    get_AISummarizer,
    get_DataAggregator,
    get_DiscordNotifier,
    os,
)
from tiki_crawl_products_v2.tasks.common import (
    _fix_sys_path_for_pipelines_import,  # noqa: F401
    get_logger,  # noqa: F401
)


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
                detail_result = ti.xcom_pull(task_ids="crawl_product_details.merge_product_details")
                output_file = (
                    detail_result.get("output_file")
                    if isinstance(detail_result, dict)
                    else detail_result
                )
                logger.info(f"   Lấy từ XCom: {output_file}")
            except Exception:
                try:
                    detail_result = ti.xcom_pull(task_ids="merge_product_details")
                    output_file = (
                        detail_result.get("output_file")
                        if isinstance(detail_result, dict)
                        else detail_result
                    )
                    logger.info(f"   Lấy từ XCom (không có prefix): {output_file}")
                except Exception as e:
                    logger.warning(f"   Không lấy được từ XCom: {e}")

        if not output_file or not os.path.exists(output_file):
            raise FileNotFoundError(f"Không tìm thấy file output: {output_file}")

        logger.info(f"📊 Đang tổng hợp dữ liệu từ: {output_file}")

        # 1. Tổng hợp dữ liệu (with caching and trend analysis)
        DataAggregator = get_DataAggregator()
        if DataAggregator is None:
            logger.warning("⚠️  DataAggregator module chưa được import, bỏ qua tổng hợp")
        else:
            try:
                aggregator = DataAggregator(output_file)
                if aggregator.load_data():
                    summary = aggregator.aggregate()  # Now with caching
                    result["summary"] = summary
                    result["aggregation_success"] = True
                    logger.info("✅ Tổng hợp dữ liệu thành công")

                    # Save to history for trend analysis
                    try:
                        aggregator.save_to_history(summary)
                    except Exception as hist_err:
                        logger.warning(f"⚠️  Could not save to history: {hist_err}")

                    # Log thống kê
                    stats = summary.get("statistics", {})
                    total_products = stats.get("total_products", 0)
                    crawled_count = stats.get("crawled_count", 0)
                    with_detail = stats.get("with_detail", 0)
                    failed = stats.get("failed", 0)
                    timeout = stats.get("timeout", 0)

                    logger.info(f"   📦 Tổng sản phẩm: {total_products}")
                    logger.info(f"   🔄 Products được crawl detail: {crawled_count}")
                    logger.info(f"   ✅ Có chi tiết (success): {with_detail}")
                    logger.info(f"   ❌ Thất bại: {failed}")
                    logger.info(f"   ⏱️  Timeout: {timeout}")

                    # Tính và hiển thị tỷ lệ thành công
                    if crawled_count > 0:
                        success_rate = (with_detail / crawled_count) * 100
                        logger.info(
                            f"   📈 Tỷ lệ thành công: {with_detail}/{crawled_count} ({success_rate:.1f}%)"
                        )

                        # Log trends if available
                        trends = summary.get("trends", {})
                        if trends.get("has_previous_data"):
                            success_change = trends.get("success_rate_change", 0)
                            logger.info(
                                f"   📊 Trend: Success rate {success_change:+.1f}% vs previous run"
                            )
                    else:
                        logger.warning("   ⚠️  Không có products nào được crawl detail")
                else:
                    logger.error("❌ Không thể load dữ liệu để tổng hợp")
            except Exception as e:
                logger.error(f"❌ Lỗi khi tổng hợp dữ liệu: {e}", exc_info=True)

        # 2. Tổng hợp với AI (with caching, shorter prompt, faster timeout)
        AISummarizer = get_AISummarizer()
        if AISummarizer is None:
            logger.warning("⚠️  AISummarizer module chưa được import, bỏ qua tổng hợp AI")
        elif result.get("summary"):
            try:
                summarizer = AISummarizer()
                ai_summary = summarizer.summarize_data(result["summary"])  # Now with caching
                if ai_summary:
                    result["ai_summary"] = ai_summary
                    result["ai_summary_success"] = True
                    logger.info("✅ Tổng hợp với AI thành công")
                    logger.info(f"   Độ dài summary: {len(ai_summary)} ký tự")
                else:
                    logger.warning("⚠️  Không nhận được summary từ AI")
            except Exception as e:
                logger.error(f"❌ Lỗi khi tổng hợp với AI: {e}", exc_info=True)

        # 3. Gửi thông báo qua Discord (2 kênh: notification + alert)
        DiscordNotifier = get_DiscordNotifier()
        if DiscordNotifier is None:
            logger.warning("DiscordNotifier module chưa được import, bỏ qua gửi thông báo")
        else:
            try:
                notifier = DiscordNotifier()

                if result.get("summary"):
                    # Lấy stats
                    stats = result["summary"].get("statistics", {})
                    total_products = stats.get("total_products", 0)
                    crawled_count = stats.get("crawled_count", 0)
                    with_detail = stats.get("with_detail", 0)
                    failed = stats.get("failed", 0)
                    timeout = stats.get("timeout", 0)
                    products_saved = stats.get("products_saved", 0)
                    crawled_at = result["summary"].get("metadata", {}).get("crawled_at", "N/A")

                    # Tính màu theo success rate
                    if crawled_count > 0:
                        success_rate = (with_detail / crawled_count) * 100
                        color = (
                            0x00B894
                            if success_rate >= 80
                            else (0xF39C12 if success_rate >= 50 else 0xE74C3C)
                        )
                    else:
                        success_rate = 0
                        color = 0x95A5A6

                    # KÊNH 1: NOTIFICATION - Thông báo tổng hợp ngắn gọn
                    fields_notification = [
                        {"name": "📦 Total", "value": f"{total_products:,}", "inline": True},
                        {"name": "🔄 Crawled", "value": f"{crawled_count:,}", "inline": True},
                        {
                            "name": "✅ Success",
                            "value": f"{with_detail:,} ({success_rate:.1f}%)",
                            "inline": True,
                        },
                    ]

                    # Add trend if available
                    trends = result["summary"].get("trends", {})
                    if trends.get("has_previous_data"):
                        success_change = trends.get("success_rate_change", 0)
                        trend_emoji = (
                            "📈" if success_change > 0 else ("📉" if success_change < 0 else "➡️")
                        )
                        fields_notification.append(
                            {
                                "name": "📊 Trend",
                                "value": f"{trend_emoji} {success_change:+.1f}% vs previous",
                                "inline": False,
                            }
                        )

                    content_notification = f"Crawl hoàn tất: **{success_rate:.1f}%** thành công"

                    logger.info("📤 Sending to NOTIFICATION channel...")
                    success_notif = notifier.send_message(
                        content=content_notification,
                        title="📊 Tổng hợp Crawl Tiki",
                        color=color,
                        fields=fields_notification,
                        footer=f"⏰ {crawled_at}",
                        max_retries=3,
                        channel="notification",
                    )

                    # KÊNH 2: ALERT - Báo cáo chi tiết với AI summary
                    if result.get("ai_summary"):
                        logger.info("📤 Sending to ALERT channel (with AI summary)...")

                        # Fields chi tiết cho alert channel
                        fields_alert = []

                        # Error analysis nếu có
                        if failed > 0 or timeout > 0:
                            total_errors = failed + timeout
                            error_rate = (
                                (total_errors / crawled_count * 100) if crawled_count > 0 else 0
                            )
                            err_info = f"**{total_errors}** errors ({error_rate:.1f}%)"
                            if failed > 0:
                                failed_rate = (
                                    (failed / crawled_count * 100) if crawled_count > 0 else 0
                                )
                                err_info += f"\n• Failed: {failed} ({failed_rate:.1f}%)"
                            if timeout > 0:
                                timeout_rate = (
                                    (timeout / crawled_count * 100) if crawled_count > 0 else 0
                                )
                                err_info += f"\n• Timeout: {timeout} ({timeout_rate:.1f}%)"
                            fields_alert.append(
                                {
                                    "name": "⚠️ Errors",
                                    "value": err_info,
                                    "inline": False,
                                }
                            )

                        if products_saved:
                            fields_alert.append(
                                {
                                    "name": "💾 Saved to DB",
                                    "value": f"{products_saved:,}",
                                    "inline": True,
                                }
                            )

                        # Determine color for alert (more strict)
                        alert_color = (
                            0x00B894
                            if success_rate >= 90
                            else (0xF39C12 if success_rate >= 70 else 0xE74C3C)
                        )

                        success_alert = notifier.send_message(
                            content=result["ai_summary"],
                            title="🤖 Báo Cáo Chi Tiết (AI)",
                            color=alert_color,
                            fields=fields_alert if fields_alert else None,
                            footer=f"⏰ {crawled_at}",
                            max_retries=3,
                            channel="alert",
                        )

                        if success_notif or success_alert:
                            result["discord_notification_success"] = True
                            logger.info(
                                f"✅ Discord sent: Notification={success_notif}, Alert={success_alert}"
                            )
                        else:
                            logger.warning("⚠️  Both channels failed")
                    else:
                        # No AI summary, only send notification
                        if success_notif:
                            result["discord_notification_success"] = True
                            logger.info("✅ Discord notification sent")
                        else:
                            logger.warning("⚠️  Notification channel failed")
                else:
                    logger.warning("Không có dữ liệu để gửi thông báo")
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

        # Performance Summary (FIXED: Use crawled_count instead of with_detail)
        try:
            dag_run = context.get("dag_run")
            if dag_run and dag_run.start_date:
                start_time = dag_run.start_date
                end_time = datetime.now(start_time.tzinfo) if start_time.tzinfo else datetime.now()
                duration = (end_time - start_time).total_seconds()

                # FIX: Use crawled_count (actual products crawled) not with_detail
                stats = result.get("summary", {}).get("statistics", {})
                total_products = stats.get("crawled_count", 0)
                with_detail = stats.get("with_detail", 0)

                # Calculate throughput
                throughput = total_products / duration if duration > 0 else 0
                avg_time = duration / total_products if total_products > 0 else 0

                logger.info("=" * 70)
                logger.info("⚡ PERFORMANCE SUMMARY")
                logger.info(f"⏱️  Duration: {duration / 60:.1f} min")
                logger.info(f"📦 Products crawled: {total_products} | Success: {with_detail}")
                if throughput > 0:
                    logger.info(
                        f"📈 Throughput: {throughput:.2f} products/s | Avg: {avg_time:.1f}s/product"
                    )
                logger.info("=" * 70)

                result["performance"] = {
                    "duration_minutes": round(duration / 60, 2),
                    "total_products": total_products,
                    "with_detail": with_detail,
                    "throughput": round(throughput, 2),
                    "avg_time_per_product": round(avg_time, 2),
                }
        except Exception as perf_error:
            logger.warning(f"⚠️  Performance summary error: {perf_error}")

        return result

    except Exception as e:
        logger.error(f"❌ Lỗi khi tổng hợp và gửi thông báo: {e}", exc_info=True)
        # Không fail task, chỉ log lỗi
        return result
