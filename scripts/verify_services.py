#!/usr/bin/env python3
"""
Script kiểm tra và fix các lỗi services trong tiki-data-pipeline
"""

import subprocess
import sys
import time
import os
from typing import Tuple, Optional

# Fix Unicode issues on Windows
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

class ServiceChecker:
    def __init__(self):
        self.passed = []
        self.failed = []
        self.warnings = []

    def run_cmd(self, cmd: str, capture: bool = True) -> Tuple[int, str, str]:
        """Chạy command và trả về (return_code, stdout, stderr)"""
        try:
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=capture,
                text=True,
                timeout=30
            )
            return result.returncode, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            return -1, "", "Command timeout"
        except Exception as e:
            return -1, "", str(e)

    def check_docker_compose(self) -> bool:
        """Kiểm tra Docker Compose"""
        print("[*] Checking Docker Compose...")
        rc, out, err = self.run_cmd("docker-compose --version")
        if rc == 0:
            print(f"   [OK] {out.strip()}")
            self.passed.append("Docker Compose")
            return True
        else:
            print(f"   [FAIL] Docker Compose not installed")
            self.failed.append("Docker Compose")
            return False

    def check_services_status(self) -> dict:
        """Kiểm tra trạng thái các services"""
        print("\n[START] Checking services status...")
        rc, out, err = self.run_cmd("docker-compose ps")
        
        services_info = {}
        for line in out.split('\n')[1:]:  # Skip header
            if line.strip():
                parts = line.split()
                if len(parts) >= 2:
                    service_name = parts[0].split('_')[-1]  # Extract service name
                    status = parts[-1]
                    services_info[service_name] = status
                    if "Up" in status:
                        print(f"   [OK] {service_name}: {status}")
                    else:
                        print(f"   [ERR] {service_name}: {status}")
                        self.failed.append(service_name)
        
        return services_info

    def check_postgres(self) -> bool:
        """Kiểm tra PostgreSQL"""
        print("\n[DB] Checking PostgreSQL...")
        
        # Kiểm tra connection
        rc, out, err = self.run_cmd(
            "docker-compose exec -T postgres pg_isready -U postgres"
        )
        if rc == 0:
            print("   [OK] PostgreSQL is responding")
            self.passed.append("PostgreSQL connection")
        else:
            print("   [ERR] PostgreSQL not responding")
            self.failed.append("PostgreSQL connection")
            return False
        
        # Kiểm tra database airflow
        rc, out, err = self.run_cmd(
            "docker-compose exec -T postgres psql -U postgres -lqt | grep -c airflow"
        )
        if rc == 0 and "1" in out:
            print("   [OK] Database 'airflow' exists")
            self.passed.append("Airflow database")
        else:
            print("   [WARN] Database 'airflow' may not exist")
            self.warnings.append("Airflow database")
        
        # Kiểm tra database nuq
        rc, out, err = self.run_cmd(
            "docker-compose exec -T postgres psql -U postgres -lqt | grep -c nuq"
        )
        if rc == 0 and "1" in out:
            print("   [OK] Database 'nuq' exists")
            self.passed.append("NUQ database")
        else:
            print("   [WARN] Database 'nuq' may not exist")
            self.warnings.append("NUQ database")
        
        # Kiểm tra tables trong airflow database
        rc, out, err = self.run_cmd(
            "docker-compose exec -T postgres psql -U airflow -d airflow -c '\\dt' 2>/dev/null | wc -l"
        )
        if rc == 0:
            table_count = int(out.strip())
            if table_count > 0:
                print(f"   [OK] Airflow tables exist ({table_count} tables)")
                self.passed.append("Airflow tables")
            else:
                print("   [ERR] No tables found in airflow database - Need to run migration!")
                self.failed.append("Airflow tables")
                return False
        
        return True

    def check_redis(self) -> bool:
        """Kiểm tra Redis"""
        print("\n[REDIS] Checking Redis...")
        rc, out, err = self.run_cmd("docker-compose exec -T redis redis-cli ping")
        if rc == 0 and "PONG" in out:
            print("   [OK] Redis is responding")
            self.passed.append("Redis")
            return True
        else:
            print("   [ERR] Redis not responding")
            self.failed.append("Redis")
            return False

    def check_airflow_api(self) -> bool:
        """Kiểm tra Airflow API"""
        print("\n[API] Checking Airflow API...")
        rc, out, err = self.run_cmd("curl -s http://localhost:8080/api/v2/version")
        if rc == 0 and "version" in out.lower():
            print("   [OK] Airflow API is responding")
            self.passed.append("Airflow API")
            return True
        else:
            print("   [WARN] Airflow API not responding (may be starting up)")
            self.warnings.append("Airflow API")
            return False

    def fix_airflow_db(self) -> bool:
        """Fix Airflow database"""
        print("\n[FIX] Fixing Airflow database...")
        
        print("   Running: airflow db migrate...")
        rc, out, err = self.run_cmd(
            "docker-compose run --rm airflow-init"
        )
        
        if rc == 0:
            print("   [OK] Airflow DB migration completed")
            self.passed.append("Airflow DB fix")
            return True
        else:
            print(f"   [WARN] Airflow init had issues: {err[:200]}")
            return False

    def restart_services(self) -> bool:
        """Restart các services"""
        print("\n[RESTART] Restarting services...")
        services = ["airflow-apiserver", "airflow-scheduler", "airflow-worker", "airflow-triggerer"]
        
        for service in services:
            print(f"   Restarting {service}...")
            rc, _, _ = self.run_cmd(f"docker-compose restart {service}")
            if rc == 0:
                print(f"   [OK] {service} restarted")
            else:
                print(f"   [ERR] Failed to restart {service}")
        
        print("   Waiting for services to be ready (30s)...")
        time.sleep(30)
        return True

    def generate_report(self):
        """In báo cáo"""
        print("\n" + "="*50)
        print("SERVICE CHECK REPORT")
        print("="*50)
        
        if self.passed:
            print(f"\n[PASS] ({len(self.passed)}):")
            for item in self.passed:
                print(f"  - {item}")
        
        if self.warnings:
            print(f"\n[WARN] ({len(self.warnings)}):")
            for item in self.warnings:
                print(f"  - {item}")
        
        if self.failed:
            print(f"\n[FAIL] ({len(self.failed)}):")
            for item in self.failed:
                print(f"  - {item}")
            return False
        
        return True

    def run_full_check(self):
        """Chạy toàn bộ kiểm tra"""
        print("[START] Tiki Data Pipeline Service Checker\n")
        
        if not self.check_docker_compose():
            print("[ERROR] Docker Compose not installed!")
            sys.exit(1)
        
        self.check_services_status()
        self.check_postgres()
        self.check_redis()
        self.check_airflow_api()
        
        # Nếu có lỗi, thử fix
        if self.failed:
            print("\n[ALERT] Found issues, attempting fixes...\n")
            if "Airflow tables" in self.failed:
                self.fix_airflow_db()
                self.restart_services()
        
        success = self.generate_report()
        
        print("\n" + "="*50)
        if success:
            print("[SUCCESS] All checks passed! Services are healthy.")
            return 0
        else:
            print("[FAILED] Some checks failed. Please review the issues above.")
            print("\nNext steps:")
            print("  1. Check docker-compose logs: docker-compose logs -f")
            print("  2. Restart all services: docker-compose restart")
            print("  3. Review the errors and take corrective action")
            return 1

if __name__ == "__main__":
    checker = ServiceChecker()
    exit_code = checker.run_full_check()
    sys.exit(exit_code)

