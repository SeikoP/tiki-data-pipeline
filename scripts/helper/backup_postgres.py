"""Script Python để backup PostgreSQL database.

Tối ưu và sửa lỗi:
- Fallback lấy biến môi trường (POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT)
    thay vì chỉ đọc từ .env (Airflow container thường không có file .env root).
- Tự động chọn phương thức backup:
        1. docker exec pg_dump (khi Docker daemon khả dụng và container Postgres đang chạy)
        2. pg_dump kết nối network trực tiếp (-h postgres -p 5432) khi không dùng được docker.
- Trả về exit code chính xác (0 thành công, 1 lỗi) để Airflow task nhận diện.
- In ra thông tin lỗi đầy đủ để stderr được capture (tránh dòng lỗi trống).
- Giữ định dạng file dump custom (-Fc) mặc định.
"""

import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# Đường dẫn project root
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
BACKUP_DIR = PROJECT_ROOT / "backups" / "postgres"

# Container name
CONTAINER_NAME = os.getenv("POSTGRES_CONTAINER_NAME", "tiki-data-pipeline-postgres-1")


def get_env_value(key: str, default: str | None = None) -> str | None:
    """
    Lấy giá trị từ environment hoặc fallback .env nếu tồn tại.
    """
    val = os.getenv(key)
    if val:
        return val
    env_files = [
        Path(os.getenv("ENV_FILE_PATH", "")) if os.getenv("ENV_FILE_PATH") else None,
        PROJECT_ROOT / ".env",
    ]
    for env_file in [path for path in env_files if path and path.exists()]:
        try:
            with open(env_file, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith(f"{key}="):
                        return line.split("=", 1)[1].strip()
        except Exception:
            continue
    return default


def docker_cli_available() -> bool:
    """
    Kiểm tra xem docker CLI và socket có sẵn không.
    """
    if shutil.which("docker") is None:
        return False
    # Nếu không có quyền truy cập socket /var/run/docker.sock thì không dùng được
    sock_path = Path("/var/run/docker.sock")
    if not sock_path.exists():
        return False
    return True


def check_container_running() -> bool:
    """
    Kiểm tra container Postgres có đang chạy không (yêu cầu docker)
    """
    if not docker_cli_available():
        return False
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", f"name={CONTAINER_NAME}", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
        return CONTAINER_NAME in result.stdout
    except Exception:
        return False


def backup_database(db_name: str, format_type: str = "custom") -> bool:
    """Backup một database.

    Args:
        db_name: Tên database
        format_type: Format backup ("custom", "sql", "tar")

    Returns:
        True nếu thành công, False nếu lỗi
    """
    # Lấy thông tin từ .env
    postgres_user = get_env_value("POSTGRES_USER", "airflow_user") or "airflow_user"
    postgres_password = get_env_value("POSTGRES_PASSWORD", "") or ""
    postgres_host = get_env_value("POSTGRES_HOST", "postgres") or "postgres"
    postgres_port = get_env_value("POSTGRES_PORT", "5432") or "5432"

    if not postgres_password:
        print("❌ Không tìm thấy POSTGRES_PASSWORD trong environment hoặc .env")
        return False

    # Type narrowing: postgres_password is guaranteed to be str here
    assert postgres_password is not None

    # Tạo tên file backup
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Đảm bảo thư mục tồn tại
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    # Xác định extension và format flag
    if format_type == "custom":
        backup_file = BACKUP_DIR / f"{db_name}_{timestamp}.dump"
        format_flag = "-Fc"
    elif format_type == "sql":
        backup_file = BACKUP_DIR / f"{db_name}_{timestamp}.sql"
        format_flag = "-Fp"
    elif format_type == "tar":
        backup_file = BACKUP_DIR / f"{db_name}_{timestamp}.tar"
        format_flag = "-Ft"
    else:
        print(f"❌ Format không hợp lệ: {format_type}")
        return False

    print(f"📦 Đang backup database: {db_name}...")
    print(f"   Format: {format_type}")
    print(f"   File: {backup_file}")

    # Quyết định phương thức backup
    use_docker = check_container_running()
    if use_docker:
        method = "docker-exec"
        # Dùng -f flag để pg_dump ghi file trực tiếp trong container
        # (tránh binary corruption khi stdout redirect qua docker exec)
        container_backup_file = f"/tmp/{backup_file.name}"
        cmd = [
            "docker",
            "exec",
            "-e",
            f"PGPASSWORD={postgres_password}",
            CONTAINER_NAME,
            "pg_dump",
            "-U",
            postgres_user,
            format_flag,
            "-f",
            container_backup_file,
            db_name,
        ]
    else:
        method = "network"
        cmd = [
            "pg_dump",
            "-h",
            postgres_host,
            "-p",
            str(postgres_port),
            "-U",
            postgres_user,
            format_flag,
            "-f",
            str(backup_file),
            db_name,
        ]

    print(f"🔧 Phương thức backup: {method}")
    if method == "network":
        print(f"   Host: {postgres_host}:{postgres_port}")

    try:
        env = os.environ.copy()
        env["PGPASSWORD"] = postgres_password
        result = subprocess.run(cmd, capture_output=True, check=False, timeout=600, env=env)

        # Nếu dùng docker, copy file từ container ra host
        if use_docker and result.returncode == 0:
            docker_copy_cmd = [
                "docker",
                "cp",
                f"{CONTAINER_NAME}:{container_backup_file}",
                str(backup_file),
            ]
            copy_result = subprocess.run(
                docker_copy_cmd, capture_output=True, check=False, timeout=60
            )
            if copy_result.returncode != 0:
                error_msg = copy_result.stderr.decode("utf-8", errors="ignore")
                print("❌ Lỗi khi copy file từ container:")
                print(error_msg)
                if backup_file.exists():
                    backup_file.unlink()
                return False

        if result.returncode == 0:
            file_size = backup_file.stat().st_size
            size_mb = file_size / (1024 * 1024)
            print(f"✅ Đã backup thành công: {backup_file.name}")
            print(f"   Size: {size_mb:.2f} MB")
            return True
        else:
            error_msg = result.stderr.decode("utf-8", errors="ignore") or "(Không có stderr)"
            print(f"❌ Lỗi khi backup {db_name} (method={method}):")
            print(error_msg)
            if backup_file.exists():
                backup_file.unlink()
            return False
    except FileNotFoundError:
        print("❌ pg_dump không tìm thấy. Cần cài đặt postgresql-client trong container.")
        if backup_file.exists():
            backup_file.unlink()
        return False
    except subprocess.TimeoutExpired:
        print("❌ Timeout khi chạy pg_dump")
        if backup_file.exists():
            backup_file.unlink()
        return False
    except Exception as e:
        print(f"❌ Exception khi backup {db_name}: {e}")
        if backup_file.exists():
            backup_file.unlink()
        return False


def list_backups():
    """
    Liệt kê các file backup.
    """
    if not BACKUP_DIR.exists():
        print("📁 Thư mục backup chưa có file nào")
        return

    backups = sorted(BACKUP_DIR.glob("*"), key=lambda p: p.stat().st_mtime, reverse=True)

    if not backups:
        print("📁 Thư mục backup chưa có file nào")
        return

    print("\n📋 Danh sách backup files (mới nhất trước):")
    for backup in backups[:10]:  # Hiển thị 10 file mới nhất
        size = backup.stat().st_size
        size_mb = size / (1024 * 1024)
        mtime = datetime.fromtimestamp(backup.stat().st_mtime)
        print(f"   - {backup.name} ({size_mb:.2f} MB, {mtime.strftime('%Y-%m-%d %H:%M:%S')})")


def main():
    """
    Main function.
    """
    import argparse

    parser = argparse.ArgumentParser(description="Backup PostgreSQL database")
    parser.add_argument(
        "--database",
        "-d",
        default="all",
        choices=["all", "airflow", "tiki", "crawl_data"],
        help="Database để backup (default: all)",
    )
    parser.add_argument(
        "--format",
        "-f",
        default="custom",
        choices=["custom", "sql", "tar"],
        help="Format backup (default: custom)",
    )
    parser.add_argument("--list", "-l", action="store_true", help="Liệt kê các file backup")

    args = parser.parse_args()

    print("=" * 70)
    print("🗄️  PostgreSQL Backup Script")
    print("=" * 70)
    print()

    # Nếu chỉ list backups
    if args.list:
        list_backups()
        return

    # Thông tin môi trường
    print(f"🔐 POSTGRES_USER: {get_env_value('POSTGRES_USER', 'airflow_user')}")
    # Ẩn password length only
    pwd = get_env_value("POSTGRES_PASSWORD", "") or ""
    print(f"🔐 POSTGRES_PASSWORD: {'*' * len(pwd) if pwd else '(missing)'}")
    print()

    # Thực hiện backup
    success = True

    if args.database == "all":
        print("🔄 Backup tất cả databases...")
        print()
        # Backup airflow metadata (optional) - ignore failure
        airflow_ok = backup_database("airflow", args.format)
        print()
        data_db = get_env_value("POSTGRES_DB", "tiki") or "tiki"
        data_ok = backup_database(data_db, args.format)
        success = airflow_ok and data_ok
    else:
        success = backup_database(args.database, args.format)

    print()
    print("=" * 70)
    if success:
        print("✅ Hoàn tất backup!")
    else:
        print("⚠️  Backup hoàn tất nhưng có lỗi!")
    print(f"📁 Thư mục backup: {BACKUP_DIR}")
    print("=" * 70)

    # Hiển thị danh sách backups
    list_backups()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
