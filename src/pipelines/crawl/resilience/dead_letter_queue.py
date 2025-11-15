"""
Dead Letter Queue (DLQ) cho failed tasks
Lưu trữ các task thất bại để xử lý sau
"""

import json
import time
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any

try:
    import redis

    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None


class DeadLetterQueue:
    """
    Dead Letter Queue để lưu trữ failed tasks

    Hỗ trợ cả file-based và Redis-based storage
    """

    def __init__(
        self,
        storage_type: str = "file",  # "file" hoặc "redis"
        storage_path: str | Path | None = None,
        redis_url: str | None = None,
        max_items: int = 10000,
    ):
        """
        Args:
            storage_type: Loại storage ("file" hoặc "redis")
            storage_path: Đường dẫn file storage (cho file type)
            redis_url: Redis URL (cho redis type)
            max_items: Số items tối đa (để tránh quá tải)
        """
        self.storage_type = storage_type
        self.max_items = max_items
        self._lock = Lock()

        if storage_type == "file":
            if storage_path is None:
                from ..utils import DEFAULT_DATA_DIR

                storage_path = Path(DEFAULT_DATA_DIR) / "dlq"
            self.storage_path = Path(storage_path)
            self.storage_path.mkdir(parents=True, exist_ok=True)
            self.redis_client = None
        elif storage_type == "redis":
            if not REDIS_AVAILABLE:
                raise ImportError("Redis chưa được cài đặt. Cài đặt: pip install redis")
            if redis_url is None:
                redis_url = "redis://redis:6379/3"  # Database 3 cho DLQ
            self.redis_client = redis.from_url(redis_url, decode_responses=False)
            self.storage_path = None
        else:
            raise ValueError(f"Unknown storage_type: {storage_type}")

    def add(
        self,
        task_id: str,
        task_type: str,
        error: Exception,
        context: dict[str, Any] | None = None,
        retry_count: int = 0,
    ) -> bool:
        """
        Thêm failed task vào DLQ

        Args:
            task_id: ID của task
            task_type: Loại task (e.g., "crawl_category", "crawl_product_detail")
            error: Exception đã xảy ra
            context: Context thêm (URL, product_id, etc.)
            retry_count: Số lần đã retry

        Returns:
            True nếu thành công
        """
        with self._lock:
            # Kiểm tra số lượng items
            if self._count() >= self.max_items:
                # Xóa items cũ nhất
                self._remove_oldest(100)

            # Tạo DLQ entry
            entry = {
                "task_id": task_id,
                "task_type": task_type,
                "error_type": type(error).__name__,
                "error_message": str(error),
                "error_context": getattr(error, "context", {}) if hasattr(error, "context") else {},
                "context": context or {},
                "retry_count": retry_count,
                "failed_at": datetime.now().isoformat(),
                "timestamp": time.time(),
            }

            if self.storage_type == "file":
                return self._add_to_file(task_id, entry)
            else:  # redis
                return self._add_to_redis(task_id, entry)

    def _add_to_file(self, task_id: str, entry: dict[str, Any]) -> bool:
        """Thêm entry vào file"""
        try:
            filepath = self.storage_path / f"{task_id}.json"
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(entry, f, ensure_ascii=False, indent=2)
            return True
        except Exception:
            return False

    def _add_to_redis(self, task_id: str, entry: dict[str, Any]) -> bool:
        """Thêm entry vào Redis"""
        try:
            key = f"dlq:{task_id}"
            value = json.dumps(entry, ensure_ascii=False)
            # Set với TTL 7 ngày
            self.redis_client.setex(key, 7 * 24 * 60 * 60, value.encode("utf-8"))
            # Thêm vào sorted set để có thể query theo thời gian
            self.redis_client.zadd("dlq:index", {task_id: entry["timestamp"]})
            return True
        except Exception:
            return False

    def get(self, task_id: str) -> dict[str, Any] | None:
        """Lấy entry từ DLQ"""
        if self.storage_type == "file":
            return self._get_from_file(task_id)
        else:
            return self._get_from_redis(task_id)

    def _get_from_file(self, task_id: str) -> dict[str, Any] | None:
        """Lấy entry từ file"""
        try:
            filepath = self.storage_path / f"{task_id}.json"
            if not filepath.exists():
                return None
            with open(filepath, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    def _get_from_redis(self, task_id: str) -> dict[str, Any] | None:
        """Lấy entry từ Redis"""
        try:
            key = f"dlq:{task_id}"
            value = self.redis_client.get(key)
            if value:
                return json.loads(value.decode("utf-8"))
            return None
        except Exception:
            return None

    def remove(self, task_id: str) -> bool:
        """Xóa entry khỏi DLQ"""
        with self._lock:
            if self.storage_type == "file":
                return self._remove_from_file(task_id)
            else:
                return self._remove_from_redis(task_id)

    def _remove_from_file(self, task_id: str) -> bool:
        """Xóa entry từ file"""
        try:
            filepath = self.storage_path / f"{task_id}.json"
            if filepath.exists():
                filepath.unlink()
                return True
            return False
        except Exception:
            return False

    def _remove_from_redis(self, task_id: str) -> bool:
        """Xóa entry từ Redis"""
        try:
            key = f"dlq:{task_id}"
            self.redis_client.delete(key)
            self.redis_client.zrem("dlq:index", task_id)
            return True
        except Exception:
            return False

    def list_all(
        self,
        task_type: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """
        Lấy danh sách tất cả entries

        Args:
            task_type: Lọc theo task type (None = tất cả)
            limit: Số lượng tối đa
            offset: Offset

        Returns:
            List entries
        """
        if self.storage_type == "file":
            return self._list_from_file(task_type, limit, offset)
        else:
            return self._list_from_redis(task_type, limit, offset)

    def _list_from_file(
        self,
        task_type: str | None,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        """Lấy danh sách từ file"""
        entries = []
        try:
            for filepath in sorted(self.storage_path.glob("*.json")):
                try:
                    with open(filepath, encoding="utf-8") as f:
                        entry = json.load(f)
                        if task_type is None or entry.get("task_type") == task_type:
                            entries.append(entry)
                except Exception:
                    continue

            # Sort theo timestamp (mới nhất trước)
            entries.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
            return entries[offset : offset + limit]
        except Exception:
            return []

    def _list_from_redis(
        self,
        task_type: str | None,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        """Lấy danh sách từ Redis"""
        entries = []
        try:
            # Lấy từ sorted set (sorted theo timestamp)
            task_ids = self.redis_client.zrevrange("dlq:index", offset, offset + limit - 1)

            for task_id_bytes in task_ids:
                task_id = task_id_bytes.decode("utf-8")
                entry = self._get_from_redis(task_id)
                if entry:
                    if task_type is None or entry.get("task_type") == task_type:
                        entries.append(entry)

            return entries
        except Exception:
            return []

    def _count(self) -> int:
        """Đếm số entries"""
        if self.storage_type == "file":
            try:
                return len(list(self.storage_path.glob("*.json")))
            except Exception:
                return 0
        else:
            try:
                return self.redis_client.zcard("dlq:index")
            except Exception:
                return 0

    def _remove_oldest(self, count: int) -> int:
        """Xóa các entries cũ nhất"""
        removed = 0
        if self.storage_type == "file":
            try:
                files = sorted(
                    self.storage_path.glob("*.json"),
                    key=lambda p: p.stat().st_mtime,
                )
                for filepath in files[:count]:
                    filepath.unlink()
                    removed += 1
            except Exception:
                pass
        else:
            try:
                # Lấy oldest entries từ sorted set
                task_ids = self.redis_client.zrange("dlq:index", 0, count - 1)
                for task_id_bytes in task_ids:
                    task_id = task_id_bytes.decode("utf-8")
                    if self._remove_from_redis(task_id):
                        removed += 1
            except Exception:
                pass
        return removed

    def get_stats(self) -> dict[str, Any]:
        """Lấy thống kê DLQ"""
        entries = self.list_all(limit=10000)  # Lấy tất cả để thống kê

        stats = {
            "total": len(entries),
            "by_task_type": {},
            "by_error_type": {},
            "oldest": None,
            "newest": None,
        }

        if entries:
            # Thống kê theo task type
            for entry in entries:
                task_type = entry.get("task_type", "unknown")
                stats["by_task_type"][task_type] = stats["by_task_type"].get(task_type, 0) + 1

                error_type = entry.get("error_type", "unknown")
                stats["by_error_type"][error_type] = stats["by_error_type"].get(error_type, 0) + 1

            # Oldest và newest
            sorted_entries = sorted(entries, key=lambda x: x.get("timestamp", 0))
            stats["oldest"] = sorted_entries[0].get("failed_at")
            stats["newest"] = sorted_entries[-1].get("failed_at")

        return stats


# Singleton instance
_dlq_instance: DeadLetterQueue | None = None


def get_dlq(
    storage_type: str = "file",
    storage_path: str | Path | None = None,
    redis_url: str | None = None,
) -> DeadLetterQueue:
    """
    Lấy DLQ instance (singleton)

    Args:
        storage_type: "file" hoặc "redis"
        storage_path: Đường dẫn file storage
        redis_url: Redis URL

    Returns:
        DeadLetterQueue instance
    """
    global _dlq_instance

    if _dlq_instance is None:
        _dlq_instance = DeadLetterQueue(
            storage_type=storage_type,
            storage_path=storage_path,
            redis_url=redis_url,
        )

    return _dlq_instance
