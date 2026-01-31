"""Redis Manager - Quản lý và monitor Redis cache.

Usage:
    python scripts/helper/redis_manager.py stats
    python scripts/helper/redis_manager.py clear
    python scripts/helper/redis_manager.py list
    python scripts/helper/redis_manager.py test
"""

import argparse
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    print("❌ Redis not installed. Run: pip install redis")
    sys.exit(1)


def get_redis_client(host='localhost', port=6379, db=1):
    """Get Redis client."""
    return redis.Redis(
        host=host,
        port=port,
        db=db,
        decode_responses=True,
        socket_connect_timeout=5
    )


def show_stats(args):
    """Hiển thị thống kê Redis."""
    print("\n" + "=" * 70)
    print("📊 REDIS STATISTICS")
    print("=" * 70)
    
    try:
        client = get_redis_client(args.host, args.port, args.db)
        
        # Server info
        info = client.info()
        
        print("\n🖥️  Server Info:")
        print(f"   Redis version: {info.get('redis_version')}")
        print(f"   Uptime: {info.get('uptime_in_days')} days")
        print(f"   Connected clients: {info.get('connected_clients')}")
        
        # Memory info
        print("\n💾 Memory:")
        print(f"   Used: {info.get('used_memory_human')}")
        print(f"   Peak: {info.get('used_memory_peak_human')}")
        print(f"   Max: {info.get('maxmemory_human', 'unlimited')}")
        
        # Database info
        db_key = f"db{args.db}"
        db_info = info.get(db_key, {})
        keys_count = db_info.get('keys', 0)
        
        print(f"\n🗄️  Database {args.db}:")
        print(f"   Keys: {keys_count}")
        print(f"   Expires: {db_info.get('expires', 0)}")
        
        # Cache stats
        hits = info.get('keyspace_hits', 0)
        misses = info.get('keyspace_misses', 0)
        total = hits + misses
        hit_rate = (hits / total * 100) if total > 0 else 0
        
        print("\n📈 Cache Performance:")
        print(f"   Hits: {hits:,}")
        print(f"   Misses: {misses:,}")
        print(f"   Hit Rate: {hit_rate:.2f}%")
        
        # Commands stats
        print("\n⚡ Commands:")
        print(f"   Total: {info.get('total_commands_processed', 0):,}")
        print(f"   Per second: {info.get('instantaneous_ops_per_sec', 0)}")
        
    except redis.ConnectionError:
        print(f"❌ Cannot connect to Redis at {args.host}:{args.port}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


def list_keys(args):
    """Liệt kê các keys trong Redis."""
    print("\n" + "=" * 70)
    print(f"🔑 REDIS KEYS (Database {args.db})")
    print("=" * 70)
    
    try:
        client = get_redis_client(args.host, args.port, args.db)
        
        pattern = args.pattern or "*"
        keys = client.keys(pattern)
        
        if not keys:
            print(f"\n⚠️  No keys found matching pattern: {pattern}")
            return
        
        print(f"\nFound {len(keys)} keys:\n")
        
        for i, key in enumerate(keys[:args.limit], 1):
            # Get TTL
            ttl = client.ttl(key)
            ttl_str = f"{ttl}s" if ttl > 0 else "no expiry" if ttl == -1 else "expired"
            
            # Get type
            key_type = client.type(key)
            
            # Get size (approximate)
            if key_type == 'string':
                value = client.get(key)
                size = len(str(value)) if value else 0
            else:
                size = "N/A"
            
            print(f"{i:3d}. {key}")
            print(f"      Type: {key_type}, TTL: {ttl_str}, Size: {size}")
        
        if len(keys) > args.limit:
            print(f"\n... and {len(keys) - args.limit} more keys")
            print(f"Use --limit to show more")
        
    except redis.ConnectionError:
        print(f"❌ Cannot connect to Redis at {args.host}:{args.port}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


def clear_cache(args):
    """Xóa cache trong Redis."""
    print("\n" + "=" * 70)
    print(f"🧹 CLEAR REDIS CACHE (Database {args.db})")
    print("=" * 70)
    
    try:
        client = get_redis_client(args.host, args.port, args.db)
        
        if args.pattern:
            # Delete specific pattern
            keys = client.keys(args.pattern)
            if not keys:
                print(f"\n⚠️  No keys found matching pattern: {args.pattern}")
                return
            
            print(f"\nFound {len(keys)} keys matching pattern: {args.pattern}")
            
            if not args.force:
                confirm = input("Delete these keys? (yes/no): ")
                if confirm.lower() != 'yes':
                    print("❌ Cancelled")
                    return
            
            deleted = 0
            for key in keys:
                if client.delete(key):
                    deleted += 1
            
            print(f"✅ Deleted {deleted} keys")
        else:
            # Flush entire database
            if not args.force:
                confirm = input(f"⚠️  This will delete ALL keys in database {args.db}. Continue? (yes/no): ")
                if confirm.lower() != 'yes':
                    print("❌ Cancelled")
                    return
            
            client.flushdb()
            print(f"✅ Flushed database {args.db}")
        
    except redis.ConnectionError:
        print(f"❌ Cannot connect to Redis at {args.host}:{args.port}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


def test_connection(args):
    """Test kết nối Redis."""
    print("\n" + "=" * 70)
    print("🧪 TEST REDIS CONNECTION")
    print("=" * 70)
    
    try:
        client = get_redis_client(args.host, args.port, args.db)
        
        # Test ping
        print(f"\n1. Testing connection to {args.host}:{args.port}...")
        response = client.ping()
        print(f"   ✅ PING: {response}")
        
        # Test set/get
        print("\n2. Testing SET/GET...")
        test_key = "test:connection:key"
        test_value = "Hello Redis!"
        client.set(test_key, test_value, ex=60)
        retrieved = client.get(test_key)
        print(f"   ✅ SET/GET: {retrieved}")
        
        # Test delete
        print("\n3. Testing DELETE...")
        deleted = client.delete(test_key)
        print(f"   ✅ DELETE: {deleted}")
        
        # Test RedisCache class
        print("\n4. Testing RedisCache class...")
        from pipelines.crawl.storage.redis_cache import get_redis_cache
        
        redis_url = f"redis://{args.host}:{args.port}/{args.db}"
        cache = get_redis_cache(redis_url)
        
        if cache:
            cache.set("test:cache:key", {"data": "test"}, ttl=60)
            data = cache.get("test:cache:key")
            cache.delete("test:cache:key")
            print(f"   ✅ RedisCache: {data}")
        else:
            print("   ⚠️  RedisCache not available")
        
        print("\n✅ All tests passed!")
        
    except redis.ConnectionError:
        print(f"\n❌ Cannot connect to Redis at {args.host}:{args.port}")
        print("\n💡 Troubleshooting:")
        print("   1. Check if Redis is running: docker ps | grep redis")
        print("   2. If in Docker, try: --host redis")
        print("   3. If outside Docker, try: --host localhost")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Redis Manager - Quản lý và monitor Redis cache"
    )
    
    # Common arguments
    parser.add_argument('--host', default='localhost', help='Redis host (default: localhost)')
    parser.add_argument('--port', type=int, default=6379, help='Redis port (default: 6379)')
    parser.add_argument('--db', type=int, default=1, help='Redis database (default: 1)')
    
    # Subcommands
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Stats command
    subparsers.add_parser('stats', help='Show Redis statistics')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List Redis keys')
    list_parser.add_argument('--pattern', help='Key pattern (default: *)')
    list_parser.add_argument('--limit', type=int, default=50, help='Max keys to show (default: 50)')
    
    # Clear command
    clear_parser = subparsers.add_parser('clear', help='Clear Redis cache')
    clear_parser.add_argument('--pattern', help='Key pattern to delete (if not specified, flush entire DB)')
    clear_parser.add_argument('--force', action='store_true', help='Skip confirmation')
    
    # Test command
    subparsers.add_parser('test', help='Test Redis connection')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Execute command
    if args.command == 'stats':
        show_stats(args)
    elif args.command == 'list':
        list_keys(args)
    elif args.command == 'clear':
        clear_cache(args)
    elif args.command == 'test':
        test_connection(args)


if __name__ == '__main__':
    main()
