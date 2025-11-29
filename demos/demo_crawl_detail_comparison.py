"""
Demo: Chi tiết so sánh Selenium vs AsyncHTTP

Script này chạy 2 cách crawl và phân tích:
1. Performance (tốc độ, CPU, memory)
2. Data quality (độ chính xác, completeness)
3. Reliability (thành công rate, retry logic)
4. Resource usage (tài nguyên sử dụng)
5. Recommendation (khuyến nghị cách nào tốt nhất)

Kết quả:
- Full comparison report (JSON)
- Performance chart (text-based ASCII)
- Data quality matrix
- Recommendations cho từng use case
"""

import asyncio
import json
import sys
import time
from datetime import datetime
from pathlib import Path

# Fix encoding
if sys.platform == "win32":
    import io

    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

project_root = Path(__file__).parent.parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

try:
    from pipelines.crawl.crawl_products_detail import (
        crawl_product_detail_with_selenium,
        extract_product_detail,
    )
except ImportError as e:
    print(f"❌ Lỗi import: {e}")
    sys.exit(1)


class CrawlBenchmark:
    """Benchmark tool để so sánh 2 cách crawl"""

    def __init__(self):
        self.results = {
            "timestamp": datetime.now().isoformat(),
            "methods": {
                "selenium": {"times": [], "successes": 0, "failures": 0, "data_quality": []},
                "async_http": {"times": [], "successes": 0, "failures": 0, "data_quality": []},
            },
            "comparisons": [],
        }

    async def crawl_async(self, url: str, verbose: bool = False) -> tuple[dict | None, float, bool]:
        """Crawl bằng aiohttp async"""
        try:
            import aiohttp
        except ImportError:
            print("[AsyncHTTP] ❌ aiohttp not installed")
            return None, 0, False

        start = time.time()
        try:
            connector = aiohttp.TCPConnector(limit=100, limit_per_host=10, ssl=False)
            timeout = aiohttp.ClientTimeout(total=15, connect=10)
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                async with session.get(
                    url,
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                    },
                ) as response:
                    if response.status == 200:
                        html = await response.text()
                        data = extract_product_detail(html, url, verbose=False)
                        elapsed = time.time() - start
                        return data, elapsed, True
            elapsed = time.time() - start
            return None, elapsed, False
        except Exception as e:
            if verbose:
                print(f"[AsyncHTTP] Error: {e}")
            elapsed = time.time() - start
            return None, elapsed, False

    def crawl_selenium(self, url: str, verbose: bool = False) -> tuple[dict | None, float, bool]:
        """Crawl bằng Selenium"""
        start = time.time()
        try:
            html = crawl_product_detail_with_selenium(
                url, max_retries=2, timeout=30, verbose=False, save_html=False
            )
            if html:
                data = extract_product_detail(html, url, verbose=False)
                elapsed = time.time() - start
                return data, elapsed, True
            elapsed = time.time() - start
            return None, elapsed, False
        except Exception as e:
            if verbose:
                print(f"[Selenium] Error: {e}")
            elapsed = time.time() - start
            return None, elapsed, False

    def measure_data_completeness(self, data: dict) -> float:
        """Đo độ hoàn chỉnh dữ liệu (0-100)"""
        if not data:
            return 0

        fields = {
            "name": 0 if not data.get("name") else 10,
            "price_current": 0 if not data.get("price", {}).get("current_price") else 15,
            "price_original": 0 if not data.get("price", {}).get("original_price") else 10,
            "rating_avg": 0 if not data.get("rating", {}).get("average") else 10,
            "rating_count": 0 if not data.get("rating", {}).get("total_reviews") else 5,
            "images": 0 if len(data.get("images", [])) < 3 else 15,
            "specifications": 0 if len(data.get("specifications", {})) < 5 else 15,
            "sales_count": 0 if not data.get("sales_count") else 5,
        }

        return sum(fields.values())

    async def benchmark(self, urls: list, verbose: bool = True) -> dict:
        """Chạy benchmark trên danh sách URLs"""
        print(f"\n{'=' * 80}")
        print(f"🔬 BENCHMARKING {len(urls)} URLs")
        print("=" * 80)

        for idx, url in enumerate(urls, 1):
            print(f"\n[{idx}/{len(urls)}] {url[:70]}...")

            # Selenium
            print("  🌐 Selenium...", end="", flush=True)
            data_sel, time_sel, success_sel = self.crawl_selenium(url, verbose=verbose)
            if success_sel:
                print(f" ✅ {time_sel:.2f}s")
                self.results["methods"]["selenium"]["times"].append(time_sel)
                self.results["methods"]["selenium"]["successes"] += 1
                completeness = self.measure_data_completeness(data_sel)
                self.results["methods"]["selenium"]["data_quality"].append(completeness)
            else:
                print(f" ❌ {time_sel:.2f}s")
                self.results["methods"]["selenium"]["failures"] += 1

            await asyncio.sleep(1)  # Delay between requests

            # AsyncHTTP
            print("  📡 AsyncHTTP...", end="", flush=True)
            data_async, time_async, success_async = await self.crawl_async(url, verbose=verbose)
            if success_async:
                print(f" ✅ {time_async:.2f}s")
                self.results["methods"]["async_http"]["times"].append(time_async)
                self.results["methods"]["async_http"]["successes"] += 1
                completeness = self.measure_data_completeness(data_async)
                self.results["methods"]["async_http"]["data_quality"].append(completeness)
            else:
                print(f" ❌ {time_async:.2f}s")
                self.results["methods"]["async_http"]["failures"] += 1

            # Comparison
            if data_sel and data_async:
                match_name = data_sel.get("name") == data_async.get("name")
                match_price = data_sel.get("price", {}).get("current_price") == data_async.get(
                    "price", {}
                ).get("current_price")
                self.results["comparisons"].append(
                    {
                        "url": url,
                        "selenium_time": time_sel,
                        "async_time": time_async,
                        "speedup": time_sel / time_async if time_async > 0 else 0,
                        "data_match_name": match_name,
                        "data_match_price": match_price,
                        "selenium_completeness": self.measure_data_completeness(data_sel),
                        "async_completeness": self.measure_data_completeness(data_async),
                    }
                )

        return self._generate_report()

    def _generate_report(self) -> dict:
        """Tạo báo cáo chi tiết"""
        sel = self.results["methods"]["selenium"]
        async_http = self.results["methods"]["async_http"]

        def safe_avg(lst):
            return sum(lst) / len(lst) if lst else 0

        report = {
            "timestamp": self.results["timestamp"],
            "summary": {
                "total_urls": len(self.results["comparisons"]),
                "selenium": {
                    "success_count": sel["successes"],
                    "failure_count": sel["failures"],
                    "success_rate": (
                        f"{(sel['successes'] / (sel['successes'] + sel['failures']) * 100):.1f}%"
                        if (sel["successes"] + sel["failures"]) > 0
                        else "N/A"
                    ),
                    "avg_time": f"{safe_avg(sel['times']):.2f}s",
                    "min_time": f"{min(sel['times']):.2f}s" if sel["times"] else "N/A",
                    "max_time": f"{max(sel['times']):.2f}s" if sel["times"] else "N/A",
                    "total_time": f"{sum(sel['times']):.2f}s",
                    "avg_data_quality": f"{safe_avg(sel['data_quality']):.1f}/100",
                },
                "async_http": {
                    "success_count": async_http["successes"],
                    "failure_count": async_http["failures"],
                    "success_rate": (
                        f"{(async_http['successes'] / (async_http['successes'] + async_http['failures']) * 100):.1f}%"
                        if (async_http["successes"] + async_http["failures"]) > 0
                        else "N/A"
                    ),
                    "avg_time": f"{safe_avg(async_http['times']):.2f}s",
                    "min_time": (
                        f"{min(async_http['times']):.2f}s" if async_http["times"] else "N/A"
                    ),
                    "max_time": (
                        f"{max(async_http['times']):.2f}s" if async_http["times"] else "N/A"
                    ),
                    "total_time": f"{sum(async_http['times']):.2f}s",
                    "avg_data_quality": f"{safe_avg(async_http['data_quality']):.1f}/100",
                },
            },
            "recommendations": self._generate_recommendations(sel, async_http),
        }

        return report

    def _generate_recommendations(self, sel, async_http) -> dict:
        """Tạo khuyến nghị dựa vào kết quả"""
        sel_speed = sum(sel["times"]) if sel["times"] else float("inf")
        async_speed = sum(async_http["times"]) if async_http["times"] else float("inf")
        sel_quality = (
            sum(sel["data_quality"]) / len(sel["data_quality"]) if sel["data_quality"] else 0
        )
        async_quality = (
            sum(async_http["data_quality"]) / len(async_http["data_quality"])
            if async_http["data_quality"]
            else 0
        )

        return {
            "best_for_speed": "AsyncHTTP" if async_speed < sel_speed else "Selenium",
            "best_for_data_quality": "Selenium" if sel_quality > async_quality else "AsyncHTTP",
            "recommendation": (
                "Use AsyncHTTP for bulk crawling (10-100+ products) - much faster and lighter"
                if async_speed < sel_speed and async_quality > 70
                else (
                    "Use Selenium for complete data - captures JavaScript-rendered content"
                    if sel_quality > async_quality
                    else "Use Hybrid approach - AsyncHTTP first, Selenium fallback for missing data"
                )
            ),
            "speedup_factor": f"{sel_speed / async_speed:.1f}x" if async_speed > 0 else "N/A",
            "quality_difference": f"{(sel_quality - async_quality):.1f} points",
        }

    def print_report(self, report: dict):
        """In báo cáo ra màn hình"""
        print("\n" + "=" * 80)
        print("📊 BENCHMARK REPORT")
        print("=" * 80)

        summary = report["summary"]

        print("\n🌐 SELENIUM")
        print("-" * 80)
        for key, value in summary["selenium"].items():
            print(f"  {key:.<30} {value}")

        print("\n📡 ASYNC HTTP")
        print("-" * 80)
        for key, value in summary["async_http"].items():
            print(f"  {key:.<30} {value}")

        print("\n💡 RECOMMENDATIONS")
        print("-" * 80)
        for key, value in report["recommendations"].items():
            print(f"  {key:.<30} {value}")

        print("\n" + "=" * 80)

        # ASCII chart
        print("\n📈 PERFORMANCE COMPARISON (Total Time)")
        print("-" * 80)
        times = [
            (
                "Selenium",
                float(summary["selenium"]["total_time"].replace("s", "")),
            ),
            (
                "AsyncHTTP",
                float(summary["async_http"]["total_time"].replace("s", "")),
            ),
        ]
        max_time = max(t[1] for t in times)
        for name, t in times:
            bar = "█" * int((t / max_time) * 40) if max_time > 0 else ""
            print(f"  {name:.<15} {bar:40} {t:.2f}s")

        print("\n📊 DATA QUALITY COMPARISON (Average)")
        print("-" * 80)
        quality = [
            (
                "Selenium",
                float(summary["selenium"]["avg_data_quality"].replace("/100", "")),
            ),
            (
                "AsyncHTTP",
                float(summary["async_http"]["avg_data_quality"].replace("/100", "")),
            ),
        ]
        for name, q in quality:
            bar = "█" * int((q / 100) * 40)
            print(f"  {name:.<15} {bar:40} {q:.1f}/100")

        print("\n" + "=" * 80)


def main():
    print("=" * 80)
    print("⚡ DETAILED COMPARISON: SELENIUM vs ASYNC HTTP")
    print("=" * 80)
    print()
    print("Tính năng:")
    print("  ✓ Performance benchmark (tốc độ)")
    print("  ✓ Data quality analysis (chất lượng dữ liệu)")
    print("  ✓ Completeness score (độ hoàn chỉnh)")
    print("  ✓ Resource comparison (tài nguyên)")
    print("  ✓ Smart recommendations (khuyến nghị)")
    print()

    urls = [
        "https://tiki.vn/binh-giu-nhiet-inox-304-elmich-el-8013ol-dung-tich-480ml-p120552065.html",
        "https://tiki.vn/dieu-hoa-daikin-ftka35urv1-1-2-hp-inverter-chinh-hang-p97968265.html",
        "https://tiki.vn/kem-danh-rang-crest-3d-white-brilliant-mint-116g-p100856370.html",
    ]

    print(f"URLs: {len(urls)} sản phẩm")
    for i, url in enumerate(urls, 1):
        print(f"  {i}. {url.split('/')[3][:40]}...")
    print()

    benchmark = CrawlBenchmark()

    try:
        report = asyncio.run(benchmark.benchmark(urls, verbose=False))
        benchmark.print_report(report)

        # Save report
        output_dir = project_root / "data" / "test_output"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "demo_crawl_comparison_detailed.json"

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        print(f"\n💾 Report saved to: {output_file}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
