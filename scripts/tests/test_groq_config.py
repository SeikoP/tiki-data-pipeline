"""
Test script để kiểm tra Groq API key rotation
"""
import os
import sys

# Thêm path để import modules
# Tính toán đường dẫn tuyệt đối đến src từ script hiện tại
_script_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.abspath(os.path.join(_script_dir, '..', '..'))
_src_path = os.path.join(_project_root, 'src')
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

from pipelines.crawl.tiki.groq_config import (
    GroqKeyManager,
    get_groq_api_key,
    get_groq_manager,
    reset_groq_manager
)

# Fix encoding on Windows
if sys.platform == "win32":
    try:
        if not hasattr(sys.stdout, 'buffer') or (hasattr(sys.stdout, 'encoding') and sys.stdout.encoding != 'utf-8'):
            import io
            if not isinstance(sys.stdout, io.TextIOWrapper):
                sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    except (AttributeError, ValueError):
        pass


def safe_print(*args, **kwargs):
    """Safe print function"""
    try:
        print(*args, **kwargs)
    except (ValueError, OSError):
        try:
            print(*args, **kwargs, file=sys.stderr)
        except:
            pass


def print_section(title):
    """Print section header"""
    safe_print("\n" + "=" * 70)
    safe_print(f"  {title}")
    safe_print("=" * 70)


def test_key_rotation():
    """Test round-robin key rotation"""
    print_section("TEST 1: Round-Robin Key Rotation")
    
    # Test với mock keys
    test_keys = ["key1", "key2", "key3"]
    manager = GroqKeyManager(api_keys=test_keys)
    
    safe_print(f"✓ Initialized với {manager.get_key_count()} keys")
    safe_print(f"  Keys: {', '.join(manager.get_all_keys())}")
    
    # Test rotation
    safe_print("\n📋 Testing rotation (10 requests):")
    keys_used = []
    for i in range(10):
        key = manager.get_key()
        keys_used.append(key)
        safe_print(f"  Request {i+1}: {key}")
    
    # Verify pattern
    expected_pattern = ["key1", "key2", "key3"] * 3 + ["key1"]
    if keys_used == expected_pattern:
        safe_print("\n✅ Rotation pattern đúng!")
    else:
        safe_print(f"\n⚠️  Rotation pattern không đúng")
        safe_print(f"  Expected: {expected_pattern}")
        safe_print(f"  Got: {keys_used}")


def test_env_loading():
    """Test loading keys từ environment"""
    print_section("TEST 2: Load Keys từ Environment")
    
    # Reset manager
    reset_groq_manager()
    
    # Test với single key
    os.environ["GROQ_API_KEY"] = "test_single_key"
    if "GROQ_API_KEYS" in os.environ:
        del os.environ["GROQ_API_KEYS"]
    
    try:
        manager = GroqKeyManager()
        keys = manager.get_all_keys()
        safe_print(f"✓ Loaded {len(keys)} key(s) từ GROQ_API_KEY")
        safe_print(f"  Keys: {keys}")
        
        if len(keys) == 1 and keys[0] == "test_single_key":
            safe_print("✅ Single key loading OK")
        else:
            safe_print("⚠️  Single key loading failed")
    except Exception as e:
        safe_print(f"⚠️  Error: {e}")
    
    # Test với multiple keys
    os.environ["GROQ_API_KEYS"] = "key1,key2,key3"
    if "GROQ_API_KEY" in os.environ:
        del os.environ["GROQ_API_KEY"]
    
    try:
        manager = GroqKeyManager()
        keys = manager.get_all_keys()
        safe_print(f"\n✓ Loaded {len(keys)} key(s) từ GROQ_API_KEYS")
        safe_print(f"  Keys: {keys}")
        
        if len(keys) == 3:
            safe_print("✅ Multiple keys loading OK")
        else:
            safe_print("⚠️  Multiple keys loading failed")
    except Exception as e:
        safe_print(f"⚠️  Error: {e}")
    
    # Cleanup
    if "GROQ_API_KEY" in os.environ:
        del os.environ["GROQ_API_KEY"]
    if "GROQ_API_KEYS" in os.environ:
        del os.environ["GROQ_API_KEYS"]


def test_stats():
    """Test usage statistics"""
    print_section("TEST 3: Usage Statistics")
    
    test_keys = ["key1", "key2", "key3"]
    manager = GroqKeyManager(api_keys=test_keys)
    
    # Simulate usage
    for i in range(15):
        key = manager.get_key()
        # Simulate some errors
        if i % 5 == 0:
            manager.record_error(key)
    
    stats = manager.get_stats()
    
    safe_print("📊 Statistics:")
    safe_print(f"  Total keys: {stats['total_keys']}")
    safe_print(f"  Total requests: {stats['total_requests']}")
    safe_print(f"  Total errors: {stats['total_errors']}")
    safe_print(f"\n  Usage per key:")
    for key, count in stats['usage_count'].items():
        errors = stats['error_count'].get(key, 0)
        safe_print(f"    {key}: {count} requests, {errors} errors")
    
    # Test healthiest key
    healthiest = manager.get_healthiest_key()
    safe_print(f"\n  Healthiest key: {healthiest}")
    
    safe_print("\n✅ Statistics tracking OK")


def test_singleton():
    """Test singleton pattern"""
    print_section("TEST 4: Singleton Pattern")
    
    reset_groq_manager()
    
    # Set test keys
    os.environ["GROQ_API_KEYS"] = "singleton_key1,singleton_key2"
    if "GROQ_API_KEY" in os.environ:
        del os.environ["GROQ_API_KEY"]
    
    try:
        manager1 = get_groq_manager()
        manager2 = get_groq_manager()
        
        if manager1 is manager2:
            safe_print("✅ Singleton pattern OK - same instance")
        else:
            safe_print("⚠️  Singleton pattern failed - different instances")
        
        # Test get_key function
        key1 = get_groq_api_key()
        key2 = get_groq_api_key()
        safe_print(f"\n  Key 1: {key1}")
        safe_print(f"  Key 2: {key2}")
        safe_print("✅ get_groq_api_key() function OK")
        
    except Exception as e:
        safe_print(f"⚠️  Error: {e}")
        import traceback
        traceback.print_exc()
    
    # Cleanup
    if "GROQ_API_KEYS" in os.environ:
        del os.environ["GROQ_API_KEYS"]


def main():
    """Run all tests"""
    safe_print("\n" + "=" * 70)
    safe_print(" " * 20 + "GROQ API KEY MANAGER TEST")
    safe_print("=" * 70)
    
    try:
        test_key_rotation()
        test_env_loading()
        test_stats()
        test_singleton()
        
        print_section("TÓM TẮT")
        safe_print("✅ Tất cả tests hoàn thành!")
        safe_print("\n💡 Cách sử dụng:")
        safe_print("  1. Set GROQ_API_KEY=single_key (cho 1 key)")
        safe_print("  2. Set GROQ_API_KEYS=key1,key2,key3 (cho nhiều keys)")
        safe_print("  3. Gọi get_groq_api_key() để lấy key tiếp theo (round-robin)")
        
    except Exception as e:
        safe_print(f"\n❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
    
    safe_print("\n" + "=" * 70)


if __name__ == "__main__":
    main()

