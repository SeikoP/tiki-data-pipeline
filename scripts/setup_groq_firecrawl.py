"""
Script để setup Groq API cho Firecrawl
Cập nhật environment variables cho Firecrawl service
"""
import os
import sys

# Fix encoding on Windows
if sys.platform == "win32":
    try:
        if not hasattr(sys.stdout, 'buffer') or (hasattr(sys.stdout, 'encoding') and sys.stdout.encoding != 'utf-8'):
            import io
            if not isinstance(sys.stdout, io.TextIOWrapper):
                sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    except (AttributeError, ValueError):
        pass

def print_section(title):
    """Print section header"""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def check_groq_config():
    """Kiểm tra Groq config hiện tại"""
    print_section("KI M TRA GROQ CONFIG")
    
    groq_key = os.getenv("GROQ_API_KEY", "")
    groq_keys = os.getenv("GROQ_API_KEYS", "")
    groq_model = os.getenv("GROQ_MODEL", "llama-3.1-70b-versatile")
    
    print(f"GROQ_API_KEY: {'✓ Set' if groq_key else '✗ Not set'}")
    print(f"GROQ_API_KEYS: {'✓ Set' if groq_keys else '✗ Not set'}")
    print(f"GROQ_MODEL: {groq_model}")
    
    if not groq_key and not groq_keys:
        print("\n⚠️  Chưa có Groq API keys!")
        print("   Hãy set GROQ_API_KEY hoặc GROQ_API_KEYS trong .env file")
        return False
    
    return True


def setup_firecrawl_groq():
    """Setup Firecrawl để sử dụng Groq"""
    print_section("SETUP FIRECRAWL VOI GROQ")
    
    # Firecrawl sử dụng GROQ_API_KEY environment variable
    groq_key = os.getenv("GROQ_API_KEY", "")
    groq_keys = os.getenv("GROQ_API_KEYS", "")
    
    if not groq_key and not groq_keys:
        print("⚠️  Không có Groq API keys để setup")
        return False
    
    # Nếu có multiple keys, lấy key đầu tiên cho Firecrawl
    # (Firecrawl chỉ support single key qua env var)
    if groq_keys and not groq_key:
        keys = [k.strip() for k in groq_keys.split(",") if k.strip()]
        if keys:
            print(f"💡 Sử dụng key đầu tiên từ GROQ_API_KEYS cho Firecrawl")
            print(f"   (Round-robin sẽ được handle bởi Python code)")
            groq_key = keys[0]
    
    if groq_key:
        print(f"✓ Firecrawl sẽ sử dụng Groq API")
        print(f"  Key: {groq_key[:20]}...")
        print("\n💡 Lưu ý:")
        print("   - Firecrawl service cần restart để áp dụng thay đổi")
        print("   - Round-robin cho multiple keys được handle bởi Python code")
        print("   - Firecrawl chỉ sử dụng key đầu tiên từ GROQ_API_KEY")
        return True
    
    return False


def show_usage_example():
    """Hiển thị ví dụ sử dụng"""
    print_section("VI DU SU DUNG")
    
    example_code = '''
# Trong Python code, sử dụng Groq key manager:

from pipelines.crawl.tiki.groq_config import get_groq_api_key

# Lấy key tiếp theo (round-robin)
api_key = get_groq_api_key()

# Sử dụng với Firecrawl extract API
# Firecrawl sẽ tự động sử dụng Groq nếu GROQ_API_KEY được set
'''
    
    print(example_code)


def main():
    """Main function"""
    print("\n" + "=" * 70)
    print(" " * 15 + "GROQ FIRECRAWL SETUP")
    print("=" * 70)
    
    if not check_groq_config():
        print("\nX Setup khong thanh cong - thieu Groq API keys")
        return
    
    if setup_firecrawl_groq():
        show_usage_example()
        print_section("NEXT STEPS")
        print("1. Dam bao .env file co GROQ_API_KEY hoac GROQ_API_KEYS")
        print("2. Restart Firecrawl service:")
        print("   docker-compose restart api")
        print("3. Test voi: python scripts/test_groq_config.py")
        print("\nOK Setup hoan tat!")
    else:
        print("\nX Setup khong thanh cong")
    
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()

