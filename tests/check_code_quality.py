#!/usr/bin/env python3
"""
Script kiểm tra code quality với các tools:
- Ruff (PERF rules) - Performance
- Pylint - Code smell
- Mypy - Type checking
- Vulture - Dead code
- Radon - Complexity
"""

import subprocess
import sys
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent.parent
SRC_DIR = PROJECT_ROOT / "src" / "pipelines" / "crawl"


def run_command(cmd: list[str], description: str) -> tuple[int, str]:
    """Chạy command và trả về exit code và output"""
    print(f"\n{'='*60}")
    print(f"🔍 {description}")
    print(f"{'='*60}")
    try:
        result = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        output = result.stdout + result.stderr
        return result.returncode, output
    except Exception as e:
        return 1, f"Error: {e}"


def check_ruff_perf():
    """Kiểm tra performance với Ruff"""
    cmd = ["ruff", "check", str(SRC_DIR), "--select", "PERF"]
    return run_command(cmd, "Ruff PERF Rules - Performance Issues")


def check_ruff_all():
    """Kiểm tra tất cả rules với Ruff"""
    cmd = ["ruff", "check", str(SRC_DIR)]
    return run_command(cmd, "Ruff - All Rules")


def check_vulture():
    """Kiểm tra dead code với Vulture"""
    cmd = ["vulture", str(SRC_DIR), "--min-confidence", "80"]
    return run_command(cmd, "Vulture - Dead Code Detection")


def check_radon_complexity():
    """Kiểm tra complexity với Radon"""
    cmd = ["radon", "cc", str(SRC_DIR), "--min", "B", "--show-complexity"]
    return run_command(cmd, "Radon - Cyclomatic Complexity")


def check_radon_maintainability():
    """Kiểm tra maintainability với Radon"""
    cmd = ["radon", "mi", str(SRC_DIR), "--min", "B"]
    return run_command(cmd, "Radon - Maintainability Index")


def check_pylint():
    """Kiểm tra code smell với Pylint"""
    try:
        cmd = ["pylint", str(SRC_DIR), "--output-format=text"]
        return run_command(cmd, "Pylint - Code Smell Analysis")
    except FileNotFoundError:
        return 0, "⚠️  Pylint chưa được cài đặt. Cài đặt: pip install pylint"


def check_mypy():
    """Kiểm tra type với Mypy"""
    try:
        cmd = ["mypy", str(SRC_DIR), "--ignore-missing-imports"]
        return run_command(cmd, "Mypy - Type Checking")
    except FileNotFoundError:
        return 0, "⚠️  Mypy chưa được cài đặt. Cài đặt: pip install mypy"


def main():
    """Chạy tất cả checks"""
    import sys
    import io
    # Set UTF-8 encoding cho stdout
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    
    print("Bắt đầu kiểm tra code quality...")
    print(f"Thư mục: {SRC_DIR}")
    print(f"Thời gian: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    results = []
    
    # 1. Ruff PERF
    exit_code, output = check_ruff_perf()
    results.append(("Ruff PERF", exit_code, output))
    
    # 1b. Ruff All
    exit_code, output = check_ruff_all()
    results.append(("Ruff All", exit_code, output))
    
    # 2. Vulture
    exit_code, output = check_vulture()
    results.append(("Vulture", exit_code, output))
    
    # 3. Radon Complexity
    exit_code, output = check_radon_complexity()
    results.append(("Radon CC", exit_code, output))
    
    # 4. Radon Maintainability
    exit_code, output = check_radon_maintainability()
    results.append(("Radon MI", exit_code, output))
    
    # 5. Pylint (optional)
    exit_code, output = check_pylint()
    results.append(("Pylint", exit_code, output))
    
    # 6. Mypy (optional)
    exit_code, output = check_mypy()
    results.append(("Mypy", exit_code, output))
    
    # Tổng hợp kết quả
    print(f"\n{'='*60}")
    print("TONG HOP KET QUA")
    print(f"{'='*60}")
    
    total_issues = 0
    for name, exit_code, output in results:
        status = "[PASS]" if exit_code == 0 else "[FAIL]"
        issues = output.count("\n") if output else 0
        if "chưa được cài đặt" in output:
            status = "[SKIP]"
            issues = 0
        print(f"{status} {name:20s} - {issues} issues")
        if exit_code != 0 and "chưa được cài đặt" not in output:
            total_issues += issues
    
    print(f"\nTong so issues: {total_issues}")
    
    # Lưu report
    report_file = PROJECT_ROOT / "code_quality_report.txt"
    with open(report_file, "w", encoding="utf-8") as f:
        f.write(f"Code Quality Report - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("="*60 + "\n\n")
        for name, exit_code, output in results:
            f.write(f"\n{'='*60}\n")
            f.write(f"{name}\n")
            f.write(f"{'='*60}\n")
            f.write(output)
            f.write("\n")
    
    print(f"\nReport da duoc luu: {report_file}")
    
    return 0 if total_issues == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

