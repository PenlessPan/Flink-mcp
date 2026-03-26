from datetime import datetime
from typing import List, Dict, Any


def format_bytes(bytes_val) -> str:
    """Format bytes to human-readable string."""
    try:
        bytes_val = float(bytes_val)
        if bytes_val <= 0:
            return "0 B"

        units = ['B', 'KB', 'MB', 'GB', 'TB']
        unit_index = 0

        while bytes_val >= 1024 and unit_index < len(units) - 1:
            bytes_val /= 1024
            unit_index += 1

        return f"{bytes_val:.2f} {units[unit_index]}"
    except (ValueError, TypeError):
        return "N/A"


def format_duration(duration_ms) -> str:
    """Format duration in milliseconds to readable string."""
    try:
        duration_ms = float(duration_ms)
        if duration_ms <= 0:
            return "N/A"

        seconds = duration_ms / 1000

        if seconds < 60:
            return f"{seconds:.2f}s"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.2f}m ({seconds:.0f}s)"
        elif seconds < 86400:
            hours = seconds / 3600
            return f"{hours:.2f}h ({seconds:.0f}s)"
        else:
            days = seconds / 86400
            return f"{days:.2f}d ({seconds:.0f}s)"
    except (ValueError, TypeError):
        return "N/A"


def format_timestamp(ts: int) -> str:
    """Format Unix timestamp in milliseconds to readable string."""
    if ts <= 0:
        return "N/A"
    try:
        dt = datetime.fromtimestamp(ts / 1000.0)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return f"{ts}ms"


def to_num(v):
    try:
        if isinstance(v, (int, float)):
            return v
        if isinstance(v, str) and v.strip() != "":
            # prefer int if possible
            f = float(v)
            i = int(f)
            return i if i == f else f
    except:
        pass
    return None


def index_by_id(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {it.get("id"): it for it in items if isinstance(it, dict) and "id" in it}


def chunk(seq, n):
    buf = []
    for x in seq:
        buf.append(x)
        if len(buf) == n:
            yield buf
            buf = []
    if buf:
        yield buf
