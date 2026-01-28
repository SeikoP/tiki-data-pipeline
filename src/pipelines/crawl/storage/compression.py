"""
Compression utilities cho data storage Hỗ trợ gzip và json compression để giảm disk usage.
"""

import gzip
import json
from pathlib import Path
from typing import Any


def compress_json(data: Any, level: int = 6) -> bytes:
    """Nén JSON data thành gzip bytes.

    Args:
        data: Data cần nén (sẽ được serialize thành JSON)
        level: Compression level (1-9, mặc định: 6)

    Returns:
        Compressed bytes
    """
    json_str = json.dumps(data, ensure_ascii=False)
    return gzip.compress(json_str.encode("utf-8"), compresslevel=level)


def decompress_json(compressed_data: bytes) -> Any:
    """Giải nén gzip bytes thành JSON data.

    Args:
        compressed_data: Compressed bytes

    Returns:
        Decompressed data (parsed JSON)
    """
    decompressed = gzip.decompress(compressed_data)
    json_str = decompressed.decode("utf-8")
    return json.loads(json_str)


def write_compressed_json(filepath: str | Path, data: Any, level: int = 6) -> bool:
    """Ghi JSON data vào file với compression.

    Args:
        filepath: Đường dẫn file (sẽ tự động thêm .gz nếu chưa có)
        data: Data cần ghi
        level: Compression level (1-9)

    Returns:
        True nếu thành công
    """
    filepath = Path(filepath)

    # Đảm bảo thư mục tồn tại
    filepath.parent.mkdir(parents=True, exist_ok=True)

    # Thêm .gz nếu chưa có
    if not filepath.suffix == ".gz":
        filepath = filepath.with_suffix(filepath.suffix + ".gz")

    try:
        compressed = compress_json(data, level=level)
        with open(filepath, "wb") as f:
            f.write(compressed)
        return True
    except Exception:
        return False


def read_compressed_json(filepath: str | Path, default: Any = None) -> Any:
    """Đọc JSON data từ compressed file.

    Args:
        filepath: Đường dẫn file (.gz hoặc không)
        default: Giá trị mặc định nếu lỗi

    Returns:
        Decompressed data hoặc default
    """
    filepath = Path(filepath)

    # Thử với .gz extension nếu chưa có
    if not filepath.suffix == ".gz":
        gz_filepath = filepath.with_suffix(filepath.suffix + ".gz")
        if gz_filepath.exists():
            filepath = gz_filepath

    if not filepath.exists():
        return default

    try:
        with open(filepath, "rb") as f:
            compressed_data = f.read()
        return decompress_json(compressed_data)
    except Exception:
        return default


def get_compression_ratio(original_data: Any, compressed_data: bytes) -> float:
    """Tính tỷ lệ compression.

    Args:
        original_data: Data gốc
        compressed_data: Data đã nén (bytes)

    Returns:
        Tỷ lệ compression (0-1, càng nhỏ càng tốt)
    """
    original_size = len(json.dumps(original_data, ensure_ascii=False).encode("utf-8"))
    compressed_size = len(compressed_data)

    if original_size == 0:
        return 0.0

    return compressed_size / original_size
