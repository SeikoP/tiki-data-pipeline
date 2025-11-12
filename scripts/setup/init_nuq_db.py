#!/usr/bin/env python3
"""Initialize NUQ database schema"""

import subprocess
import sys

def run_sql_file():
    """Execute NUQ SQL file in PostgreSQL"""
    
    print("[*] Reading NUQ SQL file...")
    try:
        with open("firecrawl/apps/nuq-postgres/nuq.sql", "r") as f:
            sql_content = f.read()
    except FileNotFoundError:
        print("[ERROR] SQL file not found!")
        return False
    
    print(f"[*] SQL file size: {len(sql_content)} bytes")
    
    # Execute SQL via psql
    print("[*] Executing SQL in nuq database...")
    
    cmd = [
        "docker-compose",
        "exec",
        "-T",
        "postgres",
        "psql",
        "-U", "postgres",
        "-d", "nuq",
        "-v", "ON_ERROR_STOP=1"
    ]
    
    try:
        result = subprocess.run(
            cmd,
            input=sql_content,
            capture_output=True,
            text=True,
            timeout=60
        )
        
        if result.returncode == 0:
            print("[OK] SQL executed successfully!")
            print("\n=== SQL Output ===")
            print(result.stdout)
            return True
        else:
            print(f"[ERROR] SQL execution failed!")
            print("\n=== Error Output ===")
            print(result.stderr)
            return False
            
    except subprocess.TimeoutExpired:
        print("[ERROR] SQL execution timeout!")
        return False
    except Exception as e:
        print(f"[ERROR] {str(e)}")
        return False

def verify_tables():
    """Verify that tables were created"""
    
    print("\n[*] Verifying tables...")
    
    cmd = [
        "docker-compose",
        "exec",
        "-T",
        "postgres",
        "psql",
        "-U", "postgres",
        "-d", "nuq",
        "-c", r"\dt nuq.*"
    ]
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            print("[OK] Tables verification:")
            print(result.stdout)
            if "queue_scrape" in result.stdout:
                print("[SUCCESS] nuq.queue_scrape table exists!")
                return True
            else:
                print("[WARNING] queue_scrape table not found")
                return False
        else:
            print(f"[ERROR] Verification failed!")
            print(result.stderr)
            return False
            
    except Exception as e:
        print(f"[ERROR] {str(e)}")
        return False

if __name__ == "__main__":
    print("=== NUQ Database Initialization ===\n")
    
    if not run_sql_file():
        print("\n[FAILED] Could not initialize database")
        sys.exit(1)
    
    if not verify_tables():
        print("\n[WARNING] Tables may not have been created")
        sys.exit(1)
    
    print("\n[SUCCESS] NUQ database initialization complete!")
    sys.exit(0)

