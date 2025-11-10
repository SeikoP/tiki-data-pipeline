"""
Cấu hình Groq API với multiple keys và round-robin rotation
"""
import os
import threading
from typing import List, Optional
from itertools import cycle


class GroqKeyManager:
    """
    Quản lý multiple Groq API keys với round-robin rotation
    Thread-safe để sử dụng trong multi-threaded environment
    """
    
    def __init__(self, api_keys: List[str] = None):
        """
        Initialize Groq Key Manager
        
        Args:
            api_keys: List các Groq API keys. Nếu None, sẽ load từ env vars
        """
        if api_keys is None:
            api_keys = self._load_keys_from_env()
        
        if not api_keys:
            raise ValueError("Không có Groq API keys. Set GROQ_API_KEY hoặc GROQ_API_KEYS")
        
        # Remove empty keys
        self.api_keys = [key.strip() for key in api_keys if key and key.strip()]
        
        if not self.api_keys:
            raise ValueError("Không có Groq API keys hợp lệ")
        
        # Create cycle iterator for round-robin
        self._key_cycle = cycle(self.api_keys)
        self._lock = threading.Lock()
        
        # Track usage stats
        self._usage_count = {key: 0 for key in self.api_keys}
        self._error_count = {key: 0 for key in self.api_keys}
    
    def _load_keys_from_env(self) -> List[str]:
        """
        Load API keys từ environment variables
        
        Hỗ trợ 2 format:
        1. GROQ_API_KEY=single_key
        2. GROQ_API_KEYS=key1,key2,key3 (comma-separated)
        """
        keys = []
        
        # Format 1: Single key
        single_key = os.getenv("GROQ_API_KEY")
        if single_key:
            keys.append(single_key)
        
        # Format 2: Multiple keys (comma-separated)
        multiple_keys = os.getenv("GROQ_API_KEYS")
        if multiple_keys:
            keys.extend([k.strip() for k in multiple_keys.split(",") if k.strip()])
        
        return keys
    
    def get_key(self) -> str:
        """
        Lấy API key tiếp theo theo round-robin
        
        Returns:
            Groq API key
        """
        with self._lock:
            key = next(self._key_cycle)
            self._usage_count[key] = self._usage_count.get(key, 0) + 1
            return key
    
    def get_all_keys(self) -> List[str]:
        """Lấy tất cả API keys"""
        return self.api_keys.copy()
    
    def get_key_count(self) -> int:
        """Số lượng API keys"""
        return len(self.api_keys)
    
    def record_error(self, key: str):
        """Ghi nhận lỗi cho một key"""
        with self._lock:
            self._error_count[key] = self._error_count.get(key, 0) + 1
    
    def get_stats(self) -> dict:
        """Lấy thống kê sử dụng keys"""
        with self._lock:
            return {
                "total_keys": len(self.api_keys),
                "usage_count": self._usage_count.copy(),
                "error_count": self._error_count.copy(),
                "total_requests": sum(self._usage_count.values()),
                "total_errors": sum(self._error_count.values()),
            }
    
    def get_healthiest_key(self) -> Optional[str]:
        """
        Lấy key có ít lỗi nhất (fallback khi cần)
        
        Returns:
            API key với error rate thấp nhất
        """
        if not self.api_keys:
            return None
        
        with self._lock:
            # Tính error rate cho mỗi key
            key_health = {}
            for key in self.api_keys:
                usage = self._usage_count.get(key, 0)
                errors = self._error_count.get(key, 0)
                error_rate = errors / usage if usage > 0 else 0
                key_health[key] = error_rate
            
            # Return key có error rate thấp nhất
            return min(key_health.items(), key=lambda x: x[1])[0]


# Global instance
_groq_manager: Optional[GroqKeyManager] = None
_groq_manager_lock = threading.Lock()


def get_groq_manager() -> GroqKeyManager:
    """
    Get hoặc tạo Groq Key Manager instance (singleton)
    
    Returns:
        GroqKeyManager instance
    """
    global _groq_manager
    
    if _groq_manager is None:
        with _groq_manager_lock:
            if _groq_manager is None:
                _groq_manager = GroqKeyManager()
    
    return _groq_manager


def get_groq_api_key() -> str:
    """
    Lấy Groq API key tiếp theo (round-robin)
    
    Returns:
        Groq API key
    """
    manager = get_groq_manager()
    return manager.get_key()


def reset_groq_manager():
    """Reset global manager (dùng cho testing)"""
    global _groq_manager
    with _groq_manager_lock:
        _groq_manager = None

