"""
Graceful Degradation
Hệ thống vẫn hoạt động ở mức độ thấp khi có lỗi
"""

from typing import Any, Callable, Optional, TypeVar, Dict
from functools import wraps
import time

T = TypeVar('T')


class DegradationLevel:
    """Các mức độ degradation"""
    FULL = "full"  # Hoạt động bình thường
    REDUCED = "reduced"  # Giảm chức năng
    MINIMAL = "minimal"  # Chức năng tối thiểu
    FAILED = "failed"  # Không hoạt động


class GracefulDegradation:
    """
    Graceful Degradation Manager
    Tự động giảm chức năng khi có lỗi, tăng lại khi phục hồi
    """
    
    def __init__(
        self,
        name: str = "default",
        failure_threshold: int = 3,
        recovery_threshold: int = 5,
        check_interval: int = 60,
    ):
        """
        Args:
            name: Tên degradation manager
            failure_threshold: Số lỗi liên tiếp để giảm level
            recovery_threshold: Số success liên tiếp để tăng level
            check_interval: Khoảng thời gian check (giây)
        """
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_threshold = recovery_threshold
        self.check_interval = check_interval
        
        self.current_level = DegradationLevel.FULL
        self.failure_count = 0
        self.success_count = 0
        self.last_check_time = time.time()
        
        # Callbacks cho từng level
        self.level_callbacks: Dict[str, Callable] = {}
    
    def register_callback(self, level: str, callback: Callable):
        """Đăng ký callback cho một level"""
        self.level_callbacks[level] = callback
    
    def record_success(self):
        """Ghi nhận success"""
        self.success_count += 1
        self.failure_count = 0
        self._check_level()
    
    def record_failure(self):
        """Ghi nhận failure"""
        self.failure_count += 1
        self.success_count = 0
        self._check_level()
    
    def _check_level(self):
        """Kiểm tra và cập nhật level"""
        old_level = self.current_level
        
        # Giảm level nếu có quá nhiều lỗi
        if self.failure_count >= self.failure_threshold:
            if self.current_level == DegradationLevel.FULL:
                self.current_level = DegradationLevel.REDUCED
            elif self.current_level == DegradationLevel.REDUCED:
                self.current_level = DegradationLevel.MINIMAL
            elif self.current_level == DegradationLevel.MINIMAL:
                self.current_level = DegradationLevel.FAILED
        
        # Tăng level nếu có nhiều success
        elif self.success_count >= self.recovery_threshold:
            if self.current_level == DegradationLevel.FAILED:
                self.current_level = DegradationLevel.MINIMAL
            elif self.current_level == DegradationLevel.MINIMAL:
                self.current_level = DegradationLevel.REDUCED
            elif self.current_level == DegradationLevel.REDUCED:
                self.current_level = DegradationLevel.FULL
        
        # Gọi callback nếu level thay đổi
        if old_level != self.current_level and self.current_level in self.level_callbacks:
            try:
                self.level_callbacks[self.current_level](old_level, self.current_level)
            except Exception:
                pass
    
    def get_level(self) -> str:
        """Lấy current level"""
        return self.current_level
    
    def is_available(self) -> bool:
        """Kiểm tra xem service có available không"""
        return self.current_level != DegradationLevel.FAILED
    
    def should_skip(self) -> bool:
        """Kiểm tra xem có nên skip operation không"""
        return self.current_level == DegradationLevel.FAILED
    
    def get_stats(self) -> Dict[str, Any]:
        """Lấy thống kê"""
        return {
            "name": self.name,
            "current_level": self.current_level,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "is_available": self.is_available(),
        }


def graceful_degradation(
    degradation_manager: GracefulDegradation,
    fallback_value: Any = None,
    skip_on_failed: bool = True,
):
    """
    Decorator để áp dụng graceful degradation cho function
    
    Args:
        degradation_manager: GracefulDegradation instance
        fallback_value: Giá trị trả về khi degraded
        skip_on_failed: Có skip function khi level = FAILED không
    
    Example:
        @graceful_degradation(degradation_manager, fallback_value=[])
        def crawl_products():
            return products
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            # Kiểm tra xem có nên skip không
            if skip_on_failed and degradation_manager.should_skip():
                return fallback_value
            
            try:
                result = func(*args, **kwargs)
                degradation_manager.record_success()
                return result
            except Exception as e:
                degradation_manager.record_failure()
                
                # Nếu ở level MINIMAL hoặc FAILED, return fallback
                if degradation_manager.get_level() in [
                    DegradationLevel.MINIMAL,
                    DegradationLevel.FAILED,
                ]:
                    return fallback_value
                
                # Ở level khác, raise exception
                raise
        
        return wrapper
    
    return decorator


class ServiceHealth:
    """
    Quản lý health của các services
    Tự động degrade khi service không available
    """
    
    def __init__(self):
        self.services: Dict[str, GracefulDegradation] = {}
    
    def register_service(
        self,
        name: str,
        failure_threshold: int = 3,
        recovery_threshold: int = 5,
    ) -> GracefulDegradation:
        """Đăng ký service"""
        degradation = GracefulDegradation(
            name=name,
            failure_threshold=failure_threshold,
            recovery_threshold=recovery_threshold,
        )
        self.services[name] = degradation
        return degradation
    
    def get_service(self, name: str) -> Optional[GracefulDegradation]:
        """Lấy service degradation"""
        return self.services.get(name)
    
    def is_service_available(self, name: str) -> bool:
        """Kiểm tra service có available không"""
        service = self.services.get(name)
        return service.is_available() if service else True
    
    def get_all_stats(self) -> Dict[str, Any]:
        """Lấy thống kê tất cả services"""
        return {
            name: service.get_stats()
            for name, service in self.services.items()
        }


# Global service health manager
_service_health = ServiceHealth()


def get_service_health() -> ServiceHealth:
    """Lấy global service health manager"""
    return _service_health

