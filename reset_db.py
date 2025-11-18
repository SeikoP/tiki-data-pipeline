#!/usr/bin/env python3
"""
Simple script to reset database and prepare for fresh crawl
"""

import subprocess
import sys
from pathlib import Path

# Color codes
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
END = "\033[0m"


def run_command(cmd, description):
    """Run command and show status"""
    print(f"\n{BLUE}→ {description}{END}")
    print(f"  Command: {cmd}")
    
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"{GREEN}✅ Success{END}")
            if result.stdout:
                print(f"  {result.stdout[:200]}")
            return True
        else:
            print(f"{RED}❌ Failed{END}")
            if result.stderr:
                print(f"  Error: {result.stderr[:200]}")
            return False
    except Exception as e:
        print(f"{RED}❌ Error: {e}{END}")
        return False


def main():
    print(f"\n{YELLOW}{'=' * 80}{END}")
    print(f"{YELLOW}{'🗄️ RESET DATABASE FOR FRESH CRAWL':^80}{END}")
    print(f"{YELLOW}{'=' * 80}{END}\n")
    
    print(f"{RED}⚠️ WARNING: This will DELETE all data in the database!{END}\n")
    
    confirm = input(f"{YELLOW}Continue? (yes/NO): {END}").strip().lower()
    if confirm != "yes":
        print(f"{RED}Cancelled.{END}")
        return
    
    print(f"\n{BLUE}Starting reset process...{END}\n")
    
    steps = [
        ("docker-compose down", "Stop all containers"),
        ("docker volume rm tiki-data-pipeline_postgres-shared-volume", "Delete database volume"),
        ("docker-compose up -d postgres redis", "Start postgres and redis"),
        ("timeout 30 docker-compose logs postgres | grep 'ready to accept'", "Wait for postgres to initialize"),
        ("docker-compose up -d", "Start all services"),
    ]
    
    for cmd, desc in steps:
        if not run_command(cmd, desc):
            print(f"\n{RED}Failed at: {desc}{END}")
            print(f"{YELLOW}Check the error above and retry manually{END}")
            return
    
    print(f"\n{GREEN}{'=' * 80}{END}")
    print(f"{GREEN}{'✅ DATABASE RESET COMPLETE':^80}{END}")
    print(f"{GREEN}{'=' * 80}{END}\n")
    
    print(f"""
{BLUE}Next steps:{END}

1. Wait for Airflow to start (1-2 minutes):
   $ docker-compose logs -f airflow-webserver
   
   Look for: "Serving Flask app 'airflow'"

2. Access Airflow UI:
   URL: http://localhost:8080
   User: airflow
   Password: airflow

3. Trigger DAG:
   - Find 'tiki_crawl_products' DAG
   - Click Play button (▶️)
   - Watch the logs

4. After DAG completes, verify data:
   $ python scripts/visualize_final_data.py

{YELLOW}Tips:{END}
  • Use: docker-compose logs -f airflow-scheduler  (to watch DAG execution)
  • Use: docker-compose logs -f postgres  (to debug DB issues)
  • Use: docker-compose exec postgres psql -U postgres -d crawl_data  (query DB)

    """)


if __name__ == "__main__":
    main()
