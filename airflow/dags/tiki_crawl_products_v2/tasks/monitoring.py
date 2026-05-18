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


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _as_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _fmt_int(value: Any) -> str:
    return f"{_as_int(value):,}"


def _fmt_money(value: Any) -> str:
    amount = _as_float(value)
    if amount <= 0:
        return "N/A"
    return f"{amount:,.0f} VND"


def _pct(part: Any, total: Any) -> float:
    denominator = _as_float(total)
    if denominator <= 0:
        return 0.0
    return _as_float(part) / denominator * 100


def _delta_line(label: str, change: dict[str, Any]) -> str:
    current = _as_int(change.get("current"))
    previous = _as_int(change.get("previous"))
    diff = current - previous
    pct_change = _as_float(change.get("pct_change"))
    if diff == 0:
        return f"{label}: {_fmt_int(current)} (không đổi)"
    sign = "+" if diff > 0 else ""
    return f"{label}: {_fmt_int(current)} ({sign}{diff:,}, {sign}{pct_change:.1f}%)"


def _build_notification_fields(summary: dict[str, Any]) -> list[dict[str, Any]]:
    stats = summary.get("statistics", {})
    trends = summary.get("trends", {})
    price = summary.get("price_analysis", {})
    rating = summary.get("rating_analysis", {})
    categories = summary.get("category_analysis", {})
    sellers = summary.get("seller_analysis", {})

    crawled_count = _as_int(stats.get("crawled_count"))
    with_detail = _as_int(stats.get("with_detail"))
    failed = _as_int(stats.get("failed"))
    timeout = _as_int(stats.get("timeout"))
    products_saved = _as_int(stats.get("products_saved"))
    products_skipped = _as_int(stats.get("products_skipped"))
    cached = _as_int(stats.get("cached"))
    error_count = failed + timeout

    fields = [
        {
            "name": "Pipeline",
            "value": (
                f"Listed: **{_fmt_int(stats.get('total_products'))}**\n"
                f"Detail: **{_fmt_int(crawled_count)}**\n"
                f"Saved DB: **{_fmt_int(products_saved)}**"
            ),
            "inline": True,
        },
        {
            "name": "Detail result",
            "value": (
                f"Success: **{_fmt_int(with_detail)}** ({_pct(with_detail, crawled_count):.1f}%)\n"
                f"Failed: **{_fmt_int(failed)}** ({_pct(failed, crawled_count):.1f}%)\n"
                f"Timeout: **{_fmt_int(timeout)}** ({_pct(timeout, crawled_count):.1f}%)"
            ),
            "inline": True,
        },
        {
            "name": "Coverage",
            "value": (
                f"Price: **{_fmt_int(price.get('total_products_with_price'))}**\n"
                f"Rating: **{_fmt_int(rating.get('total_products_with_rating'))}**\n"
                f"Category/Seller: **{_fmt_int(categories.get('total_categories'))}** / "
                f"**{_fmt_int(sellers.get('total_sellers'))}**"
            ),
            "inline": True,
        },
    ]

    operational_notes = []
    if cached:
        operational_notes.append(f"Cached: **{_fmt_int(cached)}**")
    if products_skipped:
        operational_notes.append(f"Skipped: **{_fmt_int(products_skipped)}**")
    if error_count:
        operational_notes.append(f"Errors: **{_fmt_int(error_count)}**")
    if operational_notes:
        fields.append(
            {
                "name": "Operational notes",
                "value": "\n".join(operational_notes),
                "inline": False,
            }
        )

    if trends.get("has_previous_data"):
        changes = trends.get("changes", {})
        trend_lines = []
        for label, key in (
            ("Detail", "crawled_count"),
            ("Success", "with_detail"),
            ("Failed", "failed"),
            ("Timeout", "timeout"),
        ):
            if key in changes:
                trend_lines.append(_delta_line(label, changes[key]))
        trend_lines.append(
            f"Success rate: {_as_float(trends.get('success_rate_change')):+.1f} điểm"
        )
        fields.append(
            {"name": "Trend vs previous run", "value": "\n".join(trend_lines), "inline": False}
        )

    return fields


def _build_detail_report(summary: dict[str, Any], ai_summary: str | None) -> str:
    stats = summary.get("statistics", {})
    price = summary.get("price_analysis", {})
    rating = summary.get("rating_analysis", {})
    categories = summary.get("category_analysis", {})
    sellers = summary.get("seller_analysis", {})
    issues = summary.get("issues", [])

    crawled_count = _as_int(stats.get("crawled_count"))
    with_detail = _as_int(stats.get("with_detail"))
    failed = _as_int(stats.get("failed"))
    timeout = _as_int(stats.get("timeout"))

    lines = [
        "**Tổng quan phiên crawl**",
        f"- Listed products: {_fmt_int(stats.get('total_products'))}",
        f"- Detail crawled: {_fmt_int(crawled_count)}",
        f"- Detail success: {_fmt_int(with_detail)} ({_pct(with_detail, crawled_count):.1f}%)",
        f"- Failed/timeout: {_fmt_int(failed)} / {_fmt_int(timeout)}",
        f"- Saved/skipped: {_fmt_int(stats.get('products_saved'))} / {_fmt_int(stats.get('products_skipped'))}",
        "",
        "**Giá và giảm giá**",
        f"- Avg price: {_fmt_money(price.get('avg_price'))}",
        f"- Min/max price: {_fmt_money(price.get('min_price'))} / {_fmt_money(price.get('max_price'))}",
        (
            f"- Avg/max discount: {_as_float(price.get('avg_discount')):.1f}% / "
            f"{_as_float(price.get('max_discount')):.1f}%"
        ),
        "",
        "**Độ phủ dữ liệu**",
        f"- Products with price: {_fmt_int(price.get('total_products_with_price'))}",
        f"- Products with rating: {_fmt_int(rating.get('total_products_with_rating'))}",
        f"- Avg rating: {_as_float(rating.get('avg_rating')):.2f}",
        f"- Categories/sellers: {_fmt_int(categories.get('total_categories'))} / {_fmt_int(sellers.get('total_sellers'))}",
    ]

    if issues:
        lines.extend(["", "**Vấn đề cần chú ý**"])
        lines.extend(f"- {issue}" for issue in issues[:5])

    if ai_summary:
        lines.extend(["", "**Nhận định AI**", ai_summary])

    return "\n".join(lines)


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
                    fields_notification = _build_notification_fields(result["summary"])
                    content_notification = (
                        f"Crawl hoàn tất: **{with_detail:,}/{crawled_count:,}** detail thành công "
                        f"(**{success_rate:.1f}%**), lưu DB **{products_saved:,}** sản phẩm."
                    )

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

                    # KÊNH 2: ALERT - Báo cáo chi tiết, AI là phần bổ sung nếu có.
                    logger.info("📤 Sending to ALERT channel (detail report)...")
                    alert_color = (
                        0x00B894
                        if success_rate >= 90
                        else (0xF39C12 if success_rate >= 70 else 0xE74C3C)
                    )
                    detail_report = _build_detail_report(
                        result["summary"], result.get("ai_summary")
                    )
                    success_alert = notifier.send_message(
                        content=detail_report,
                        title="📋 Báo cáo dữ liệu crawl",
                        color=alert_color,
                        fields=None,
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
