"""
Wrapper để tích hợp Groq API với Firecrawl
Hỗ trợ multiple keys với round-robin rotation
"""
import os
import sys
import requests
from typing import Dict, Any, Optional, List
from urllib.parse import urljoin

from .groq_config import get_groq_api_key, get_groq_manager
from .config import FIRECRAWL_API_URL, GROQ_CONFIG

# Fix encoding on Windows
if sys.platform == "win32":
    try:
        if not hasattr(sys.stdout, 'buffer') or (hasattr(sys.stdout, 'encoding') and sys.stdout.encoding != 'utf-8'):
            import io
            if not isinstance(sys.stdout, io.TextIOWrapper):
                sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    except (AttributeError, ValueError):
        pass


def setup_firecrawl_groq_env():
    """
    Setup environment variable cho Firecrawl service để sử dụng Groq
    Firecrawl service sẽ tự động detect GROQ_API_KEY
    """
    if not GROQ_CONFIG.get("enabled"):
        return False
    
    # Lấy key đầu tiên (Firecrawl service chỉ support single key)
    groq_key = GROQ_CONFIG.get("api_key")
    if not groq_key and GROQ_CONFIG.get("api_keys"):
        groq_key = GROQ_CONFIG["api_keys"][0] if GROQ_CONFIG["api_keys"] else None
    
    if groq_key:
        os.environ["GROQ_API_KEY"] = groq_key
        return True
    
    return False


def call_firecrawl_extract_with_groq(
    urls: List[str],
    schema: Dict[str, Any],
    prompt: str,
    system_prompt: str = None,
    timeout: int = 60
) -> Optional[Dict[str, Any]]:
    """
    Gọi Firecrawl extract API với Groq (sử dụng round-robin keys)
    
    Args:
        urls: List URLs cần extract
        schema: JSON schema cho structured output
        prompt: Extraction prompt
        system_prompt: System prompt (optional)
        timeout: Request timeout
    
    Returns:
        Extracted data hoặc None nếu lỗi
    """
    # Setup Groq env cho Firecrawl
    setup_firecrawl_groq_env()
    
    # Lấy Groq key (round-robin)
    groq_key = get_groq_api_key()
    
    # Firecrawl extract endpoint
    extract_url = f"{FIRECRAWL_API_URL}/v1/extract"
    
    payload = {
        "urls": urls,
        "schema": schema,
        "prompt": prompt,
    }
    
    if system_prompt:
        payload["systemPrompt"] = system_prompt
    
    # Headers với Groq API key
    # Note: Firecrawl service sẽ sử dụng GROQ_API_KEY env var
    # Nhưng chúng ta có thể pass qua agent options nếu Firecrawl hỗ trợ
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(
            extract_url,
            json=payload,
            headers=headers,
            timeout=timeout
        )
        response.raise_for_status()
        response_data = response.json()
        
        if not response_data.get('success'):
            # Ghi nhận lỗi
            manager = get_groq_manager()
            manager.record_error(groq_key)
            return None
        
        # Firecrawl trả về job ID, cần poll
        job_id = response_data.get('id')
        if job_id:
            return poll_extraction_result(job_id, timeout)
        
        # Nếu có data trực tiếp
        if 'data' in response_data:
            data = response_data['data']
            if isinstance(data, list) and len(data) > 0:
                return data[0].get('extract')
        
        return None
        
    except requests.exceptions.RequestException as e:
        # Ghi nhận lỗi
        manager = get_groq_manager()
        manager.record_error(groq_key)
        print(f"⚠️  Lỗi khi gọi Firecrawl extract: {e}")
        return None
    except Exception as e:
        print(f"⚠️  Lỗi không mong đợi: {e}")
        import traceback
        traceback.print_exc()
        return None


def poll_extraction_result(job_id: str, timeout: int = 60, interval: int = 2) -> Optional[Dict[str, Any]]:
    """
    Poll Firecrawl extraction job để lấy kết quả
    
    Args:
        job_id: Job ID từ Firecrawl
        timeout: Total timeout (seconds)
        interval: Polling interval (seconds)
    
    Returns:
        Extracted data hoặc None
    """
    import time
    
    max_attempts = timeout // interval
    attempt = 0
    
    while attempt < max_attempts:
        try:
            response = requests.get(
                f"{FIRECRAWL_API_URL}/v1/extract/{job_id}",
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            
            if data.get('status') == 'completed':
                result = data.get('data', [])
                if result and len(result) > 0:
                    return result[0].get('extract')
                return None
            
            if data.get('status') == 'failed':
                print(f"⚠️  Extraction job failed: {data.get('error', 'Unknown error')}")
                return None
            
            # Still processing, wait
            time.sleep(interval)
            attempt += 1
            
        except requests.exceptions.RequestException as e:
            print(f"⚠️  Lỗi khi poll extraction result: {e}")
            return None
    
    print(f"⚠️  Timeout khi poll extraction result sau {timeout}s")
    return None


def get_groq_stats() -> Dict[str, Any]:
    """Lấy thống kê sử dụng Groq keys"""
    manager = get_groq_manager()
    return manager.get_stats()

