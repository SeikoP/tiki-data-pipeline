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

from .common import (
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
        DataAggregator = get_DataAggregator()
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
                    else:
                        logger.warning("   ⚠️  Không có products nào được crawl detail")
                else:
                    logger.error("❌ Không thể load dữ liệu để tổng hợp")
            except Exception as e:
                logger.error(f"❌ Lỗi khi tổng hợp dữ liệu: {e}", exc_info=True)

        # 2. Tổng hợp với AI
        AISummarizer = get_AISummarizer()
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

        # 3. Gửi thông báo qua Discord (rút gọn nội dung nhưng giữ lại lỗi chi tiết)
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

                    # Fields với error analysis đầy đủ
                    fields = []
                    fields.append({"name": "Total", "value": f"{total_products:,}", "inline": True})
                    fields.append(
                        {"name": "Crawled", "value": f"{crawled_count:,}", "inline": True}
                    )
                    fields.append(
                        {
                            "name": "Success",
                            "value": f"{with_detail:,} ({success_rate:.1f}%)",
                            "inline": True,
                        }
                    )

                    # Thêm error analysis chi tiết
                    if failed > 0 or timeout > 0:
                        total_errors = failed + timeout
                        error_rate = (
                            (total_errors / crawled_count * 100) if crawled_count > 0 else 0
                        )
                        err_info = f"**Total Errors: {total_errors}** ({error_rate:.1f}%)\n"
                        if failed > 0:
                            failed_rate = (failed / crawled_count * 100) if crawled_count > 0 else 0
                            err_info += f"• Failed: {failed} ({failed_rate:.1f}%)\n"
                        if timeout > 0:
                            timeout_rate = (
                                (timeout / crawled_count * 100) if crawled_count > 0 else 0
                            )
                            err_info += f"• Timeout: {timeout} ({timeout_rate:.1f}%)"
                        fields.append(
                            {"name": "Error Analysis", "value": err_info.strip(), "inline": False}
                        )

                    if products_saved:
                        fields.append(
                            {"name": "Saved to DB", "value": f"{products_saved:,}", "inline": True}
                        )

                    # Nội dung rõ ràng
                    content = "Tổng hợp dữ liệu crawl Tiki.vn\n"
                    if crawled_count > 0:
                        content += f"```\nThành công: {success_rate:.1f}% ({with_detail}/{crawled_count})\n```"
                    else:
                        content += "Chưa có sản phẩm được crawl detail."

                    success = notifier.send_message(
                        content=content,
                        title="Tổng hợp dữ liệu Tiki",
                        color=color,
                        fields=fields,
                        footer=f"Crawl lúc: {crawled_at}",
                    )
                    if success:
                        result["discord_notification_success"] = True
                        logger.info("Đã gửi thông báo Discord")
                    else:
                        logger.warning("Không thể gửi thông báo qua Discord")
                else:
                    logger.warning("Không có dữ liệu để gửi thông báo")
            except Exception as e:
                logger.error(f"Lỗi khi gửi thông báo Discord: {e}", exc_info=True)

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

        # Performance Summary
        try:
            dag_run = context.get("dag_run")
            if dag_run and dag_run.start_date:
                start_time = dag_run.start_date
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                total_products = result.get("with_detail", 0)  # Use crawled products count

                # Calculate throughput
                throughput = total_products / duration if duration > 0 else 0
                avg_time = duration / total_products if total_products > 0 else 0

                logger.info("=" * 70)
                logger.info("⚡ PERFORMANCE SUMMARY")
                logger.info(f"⏱️  Duration: {duration / 60:.1f} min | Products: {total_products}")
                if throughput > 0:
                    logger.info(
                        f"📈 Throughput: {throughput:.2f} products/s | Avg: {avg_time:.1f}s/product"
                    )
                logger.info("=" * 70)

                result["performance"] = {
                    "duration_minutes": round(duration / 60, 2),
                    "total_products": total_products,
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
