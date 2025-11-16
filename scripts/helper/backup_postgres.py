"""
Script Python để backup PostgreSQL database
Backup vào thư mục backups/postgres với timestamp
"""

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# Đường dẫn project root
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
BACKUP_DIR = PROJECT_ROOT / "backups" / "postgres"

# Container name
CONTAINER_NAME = "tiki-data-pipeline-postgres-1"


def get_env_value(key: str, default: str = None) -> str:
    """Lấy giá trị từ .env file"""
    env_file = PROJECT_ROOT / ".env"
    if not env_file.exists():
        return default
    
    with open(env_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith(f"{key}="):
                return line.split("=", 1)[1].strip()
    return default


def check_container_running() -> bool:
    """Kiểm tra container có đang chạy không"""
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", f"name={CONTAINER_NAME}", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            check=False
        )
        return CONTAINER_NAME in result.stdout
    except Exception:
        return False


def backup_database(db_name: str, format_type: str = "custom") -> bool:
    """Backup một database
    
    Args:
        db_name: Tên database
        format_type: Format backup ("custom", "sql", "tar")
    
    Returns:
        True nếu thành công, False nếu lỗi
    """
    # Lấy thông tin từ .env
    postgres_user = get_env_value("POSTGRES_USER", "airflow_user")
    postgres_password = get_env_value("POSTGRES_PASSWORD", "")
    
    if not postgres_password:
        print(f"❌ Không tìm thấy POSTGRES_PASSWORD trong .env")
        return False
    
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
    
    try:
        # Chạy pg_dump trong container
        cmd = [
            "docker", "exec",
            "-e", f"PGPASSWORD={postgres_password}",
            CONTAINER_NAME,
            "pg_dump",
            "-U", postgres_user,
            format_flag,
            db_name
        ]
        
        # Mở file để ghi
        with open(backup_file, "wb") as f:
            result = subprocess.run(
                cmd,
                stdout=f,
                stderr=subprocess.PIPE,
                check=False
            )
        
        if result.returncode == 0:
            file_size = backup_file.stat().st_size
            size_mb = file_size / (1024 * 1024)
            print(f"✅ Đã backup thành công: {backup_file.name}")
            print(f"   Size: {size_mb:.2f} MB")
            return True
        else:
            error_msg = result.stderr.decode("utf-8", errors="ignore")
            print(f"❌ Lỗi khi backup {db_name}:")
            print(f"   {error_msg}")
            # Xóa file nếu backup lỗi
            if backup_file.exists():
                backup_file.unlink()
            return False
            
    except Exception as e:
        print(f"❌ Exception khi backup {db_name}: {e}")
        if backup_file.exists():
            backup_file.unlink()
        return False


def list_backups():
    """Liệt kê các file backup"""
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
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Backup PostgreSQL database")
    parser.add_argument(
        "--database",
        "-d",
        default="all",
        choices=["all", "airflow", "crawl_data"],
        help="Database để backup (default: all)"
    )
    parser.add_argument(
        "--format",
        "-f",
        default="custom",
        choices=["custom", "sql", "tar"],
        help="Format backup (default: custom)"
    )
    parser.add_argument(
        "--list",
        "-l",
        action="store_true",
        help="Liệt kê các file backup"
    )
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("🗄️  PostgreSQL Backup Script")
    print("=" * 70)
    print()
    
    # Nếu chỉ list backups
    if args.list:
        list_backups()
        return
    
    # Kiểm tra container
    if not check_container_running():
        print(f"❌ Container PostgreSQL không đang chạy: {CONTAINER_NAME}")
        print("💡 Chạy: docker compose up -d postgres")
        sys.exit(1)
    
    print(f"✅ Container PostgreSQL đang chạy: {CONTAINER_NAME}")
    print()
    
    # Thực hiện backup
    success = True
    
    if args.database == "all":
        print("🔄 Backup tất cả databases...")
        print()
        success = backup_database("airflow", args.format) and success
        print()
        success = backup_database("crawl_data", args.format) and success
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

