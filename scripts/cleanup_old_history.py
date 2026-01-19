"""
Cleanup Old Crawl History

This script implements TTL (Time To Live) policy for crawl_history table:
- Archive records older than 6 months
- Delete records older than 1 year
- Can be run as a cron job or Airflow task

Usage:
    python scripts/cleanup_old_history.py --archive-months 6 --delete-months 12
    python scripts/cleanup_old_history.py --dry-run  # Preview only
"""

import argparse
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from pipelines.crawl.storage.postgres_storage import PostgresStorage


def cleanup_old_history(
    archive_months: int = 6,
    delete_months: int = 12,
    dry_run: bool = False
):
    """
    Cleanup old crawl history records
    
    Args:
        archive_months: Archive records older than this many months
        delete_months: Delete records older than this many months
        dry_run: If True, only show what would be done without actually doing it
    """
    print(f"🧹 Crawl History Cleanup {'(DRY RUN)' if dry_run else ''}")
    print(f"   Archive threshold: {archive_months} months")
    print(f"   Delete threshold: {delete_months} months")
    
    storage = PostgresStorage()
    
    archive_date = datetime.now() - timedelta(days=archive_months * 30)
    delete_date = datetime.now() - timedelta(days=delete_months * 30)
    
    with storage.get_connection() as conn:
        with conn.cursor() as cur:
            # Step 1: Count records to be affected
            print("\n📊 Step 1: Analyzing data...")
            
            cur.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN crawled_at < %s THEN 1 END) as to_delete,
                    COUNT(CASE WHEN crawled_at >= %s AND crawled_at < %s THEN 1 END) as to_archive,
                    pg_size_pretty(pg_total_relation_size('crawl_history')) as table_size
                FROM crawl_history
            """, (delete_date, delete_date, archive_date))
            
            stats = cur.fetchone()
            print(f"   Total records: {stats[0]:,}")
            print(f"   To delete (> {delete_months} months): {stats[1]:,}")
            print(f"   To archive ({archive_months}-{delete_months} months): {stats[2]:,}")
            print(f"   Current table size: {stats[3]}")
            
            if dry_run:
                print("\n⚠️  DRY RUN MODE - No changes will be made")
                return
            
            # Step 2: Create archive table if not exists
            print("\n📦 Step 2: Creating archive table...")
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS crawl_history_archive (
                    LIKE crawl_history INCLUDING ALL
                );
            """)
            print("   ✅ Archive table ready")
            
            # Step 3: Archive old records
            if stats[2] > 0:
                print(f"\n📤 Step 3: Archiving {stats[2]:,} records...")
                
                cur.execute("""
                    INSERT INTO crawl_history_archive
                    SELECT * FROM crawl_history
                    WHERE crawled_at >= %s AND crawled_at < %s
                    ON CONFLICT DO NOTHING
                """, (delete_date, archive_date))
                
                archived_count = cur.rowcount
                print(f"   ✅ Archived {archived_count:,} records")
            else:
                print("\n✅ Step 3: No records to archive")
            
            # Step 4: Delete very old records
            if stats[1] > 0:
                print(f"\n🗑️  Step 4: Deleting {stats[1]:,} old records...")
                
                cur.execute("""
                    DELETE FROM crawl_history
                    WHERE crawled_at < %s
                """, (delete_date,))
                
                deleted_count = cur.rowcount
                print(f"   ✅ Deleted {deleted_count:,} records")
            else:
                print("\n✅ Step 4: No records to delete")
            
            # Step 5: Vacuum table to reclaim space
            print("\n🔧 Step 5: Vacuuming table...")
            
            conn.commit()  # Commit before VACUUM
            cur.execute("VACUUM ANALYZE crawl_history")
            
            print("   ✅ Table vacuumed")
            
            # Step 6: Show final stats
            print("\n📊 Step 6: Final statistics...")
            
            cur.execute("""
                SELECT 
                    COUNT(*) as total,
                    pg_size_pretty(pg_total_relation_size('crawl_history')) as table_size,
                    pg_size_pretty(pg_total_relation_size('crawl_history_archive')) as archive_size
                FROM crawl_history
            """)
            
            final_stats = cur.fetchone()
            print(f"   Remaining records: {final_stats[0]:,}")
            print(f"   Current table size: {final_stats[1]}")
            print(f"   Archive table size: {final_stats[2]}")
    
    print("\n✅ Cleanup completed successfully!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cleanup old crawl history records")
    parser.add_argument(
        "--archive-months",
        type=int,
        default=6,
        help="Archive records older than this many months (default: 6)"
    )
    parser.add_argument(
        "--delete-months",
        type=int,
        default=12,
        help="Delete records older than this many months (default: 12)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without actually modifying data"
    )
    
    args = parser.parse_args()
    
    # Set POSTGRES_HOST for local execution
    if "POSTGRES_HOST" not in os.environ:
        print("ℹ️  Setting POSTGRES_HOST=localhost for local execution")
        os.environ["POSTGRES_HOST"] = "localhost"
    
    cleanup_old_history(
        archive_months=args.archive_months,
        delete_months=args.delete_months,
        dry_run=args.dry_run
    )
