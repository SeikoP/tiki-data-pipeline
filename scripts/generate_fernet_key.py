#!/usr/bin/env python3
"""
Script để generate FERNET_KEY cho Airflow

FERNET_KEY được dùng để encrypt Variables và Connections trong Airflow.
Cần giữ nguyên key này để decrypt data đã encrypt trước đó.

Usage:
    python scripts/generate_fernet_key.py

    Hoặc:
    python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
"""

from cryptography.fernet import Fernet


def main():
    """Generate và hiển thị FERNET_KEY mới"""
    key = Fernet.generate_key().decode()

    print("=" * 70)
    print("🔐 FERNET_KEY Generated for Airflow")
    print("=" * 70)
    print()
    print("Copy dòng sau vào file .env:")
    print()
    print(f"AIRFLOW__CORE__FERNET_KEY={key}")
    print()
    print("⚠️  Lưu ý:")
    print("   - Giữ key này an toàn - mất key sẽ không decrypt được data đã encrypt")
    print("   - Nếu đã có data trong Airflow, không đổi key này")
    print("   - Key này cần giống nhau cho tất cả Airflow containers")
    print("=" * 70)


if __name__ == "__main__":
    main()
