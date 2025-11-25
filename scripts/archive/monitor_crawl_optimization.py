#!/usr/bin/env python3
"""
Monitor crawl speed optimization in real-time
Tracks products/hour, error rate, and confirms 2-3x improvement
"""

import subprocess
import time
import json
from datetime import datetime, timedelta
from collections import deque

def get_product_count():
    """Get current product count from database"""
    try:
        cmd = """
        docker-compose exec -T postgres psql -U tiki_user -d crawl_data -t -c "
        SELECT COUNT(*) as total, 
               COUNT(CASE WHEN detail IS NOT NULL AND detail::text LIKE '%price%' THEN 1 END) as with_detail,
               COUNT(CASE WHEN detail IS NULL THEN 1 END) as failed
        FROM products 
        WHERE crawled_at > NOW() - INTERVAL '1 hour';"
        """
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd="e:\\Project\\tiki-data-pipeline")
        if result.returncode == 0:
            line = result.stdout.strip().split('\n')[-1]
            parts = [x.strip() for x in line.split('|')]
            return {
                'total': int(parts[0]) if parts[0].isdigit() else 0,
                'with_detail': int(parts[1]) if parts[1].isdigit() else 0,
                'failed': int(parts[2]) if parts[2].isdigit() else 0,
            }
    except Exception as e:
        print(f"Error getting product count: {e}")
    return {'total': 0, 'with_detail': 0, 'failed': 0}

def get_dag_status():
    """Get current DAG run status"""
    try:
        cmd = """
        docker-compose exec -T airflow-scheduler airflow tasks list tiki_crawl_products.crawl_product_details 2>/dev/null | wc -l
        """
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd="e:\\Project\\tiki-data-pipeline")
        return result.stdout.strip()
    except:
        return "N/A"

def monitor_crawl(duration_minutes=60):
    """Monitor crawl for specified duration"""
    print("=" * 80)
    print("🚀 CRAWL SPEED OPTIMIZATION - REAL-TIME MONITOR")
    print("=" * 80)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Monitor Duration: {duration_minutes} minutes")
    print("=" * 80)
    print()
    
    samples = deque(maxlen=6)  # Keep last 10 samples (10 min at 1 sample/min)
    start_time = datetime.now()
    
    for minute in range(duration_minutes):
        elapsed = datetime.now() - start_time
        stats = get_product_count()
        samples.append((elapsed.total_seconds() / 60, stats))
        
        # Calculate products/hour
        if len(samples) >= 2:
            oldest_minute, oldest_stats = samples[0]
            newest_minute, newest_stats = samples[-1]
            time_delta = newest_minute - oldest_minute
            
            if time_delta > 0:
                products_delta = newest_stats['total'] - oldest_stats['total']
                products_per_hour = (products_delta / time_delta) * 60 if time_delta > 0 else 0
            else:
                products_per_hour = 0
        else:
            products_per_hour = 0
        
        success_rate = (stats['with_detail'] / stats['total'] * 100) if stats['total'] > 0 else 0
        error_rate = (stats['failed'] / stats['total'] * 100) if stats['total'] > 0 else 0
        
        print(f"[{minute+1:2d}/{duration_minutes}] {datetime.now().strftime('%H:%M:%S')} | ", end="")
        print(f"Products: {stats['total']:4d} | ", end="")
        print(f"Speed: {products_per_hour:6.0f} p/h | ", end="")
        print(f"Success: {success_rate:5.1f}% | ", end="")
        print(f"Error: {error_rate:5.1f}%")
        
        # Alert on high error rate
        if error_rate > 5:
            print("  ⚠️  WARNING: Error rate > 5% - Consider reducing rate limit")
        
        # Check if target reached
        if products_per_hour >= 1000 and error_rate < 5:
            print("  ✅ TARGET REACHED: 1000+ products/hour with <5% error rate!")
        
        if minute < duration_minutes - 1:
            time.sleep(60)  # Wait 1 minute before next sample
    
    print()
    print("=" * 80)
    print("📊 FINAL RESULTS")
    print("=" * 80)
    final_stats = get_product_count()
    print(f"Total Products Crawled: {final_stats['total']}")
    print(f"With Detail Data: {final_stats['with_detail']}")
    print(f"Failed: {final_stats['failed']}")
    print()
    
    # Calculate average
    if len(samples) >= 2:
        total_time = (samples[-1][0] - samples[0][0]) / 60  # Convert to hours
        products_total = samples[-1][1]['total'] - samples[0][1]['total']
        if total_time > 0:
            avg_speed = products_total / total_time
        else:
            avg_speed = 0
    else:
        avg_speed = 0
    
    print(f"Average Speed: {avg_speed:.0f} products/hour")
    print(f"Expected Baseline: 300-500 products/hour")
    print(f"Improvement: {avg_speed/400:.1f}x (baseline 400 p/h)")
    print()
    
    if avg_speed >= 1000:
        print("✅ SUCCESS: Achieved 2-3x improvement target!")
    elif avg_speed >= 700:
        print("⚠️  PARTIAL: Good improvement but below 2-3x target")
    else:
        print("❌ NEEDS INVESTIGATION: Speed below expectations")
    
    print("=" * 80)

if __name__ == "__main__":
    import sys
    duration = int(sys.argv[1]) if len(sys.argv) > 1 else 60
    monitor_crawl(duration)
