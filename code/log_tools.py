import logging
from typing import Optional

import httpx

from .app import mcp
from .connection import get_settings, MAX_OUTPUT_CHARS
from .utils import format_bytes

logger = logging.getLogger("flink-mcp-server")


@mcp.tool()
async def list_flink_logs(
    target: str = "jobmanager",
    taskmanager_id: Optional[str] = None
) -> str:
    """
    List available log files on the JobManager or a specific TaskManager.

    Args:
        target: Either "jobmanager" (default) or "taskmanager".
        taskmanager_id: Required when target is "taskmanager". The TaskManager ID.
    """
    base = get_settings()["url"]

    if target == "taskmanager":
        if not taskmanager_id:
            return "❌ taskmanager_id is required when target is 'taskmanager'."
        url = f"{base}/taskmanagers/{taskmanager_id}/logs"
    else:
        url = f"{base}/jobmanager/logs"

    try:
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.get(url, timeout=10.0)
            response.raise_for_status()
            data = response.json()

        log_entries = data.get("logs", [])
        if not log_entries:
            return f"No log files found on {target}."

        lines = [f"Log Files on {target.capitalize()}"]
        if taskmanager_id:
            lines[0] += f" ({taskmanager_id})"
        lines.append("=" * 60)
        lines.append(f"{'File Name':<50} {'Size':>10}")
        lines.append("-" * 60)
        for entry in sorted(log_entries, key=lambda x: x.get("name", "")):
            name = entry.get("name", "N/A")
            size = entry.get("size", 0)
            lines.append(f"{name:<50} {format_bytes(size):>10}")
        lines.append("=" * 60)
        lines.append(f"Total: {len(log_entries)} file(s)")
        return "\n".join(lines)

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return f"❌ Log endpoint not found for {target}. The Flink REST API may not expose logs at this endpoint."
        return f"❌ HTTP Error ({e.response.status_code}): {str(e)}"
    except httpx.TimeoutException:
        return "❌ Request timeout while listing log files."
    except Exception as e:
        logger.error(f"Failed to list Flink logs: {e}")
        return f"❌ Error listing Flink logs: {str(e)}"


@mcp.tool()
async def read_flink_logs(
    log_file: str,
    target: str = "jobmanager",
    taskmanager_id: Optional[str] = None,
    tail: Optional[int] = None,
    level_filter: Optional[str] = None,
    keyword: Optional[str] = None
) -> str:
    """
    Read the content of a Flink log file from the JobManager or a TaskManager,
    with optional filtering by log level or keyword, and tail support.

    Args:
        log_file: Name of the log file to read (e.g., "flink--standalonesession-0.log").
                  Use list_flink_logs to discover available file names.
        target: Either "jobmanager" (default) or "taskmanager".
        taskmanager_id: Required when target is "taskmanager".
        tail: If set, return only the last N lines of the log.
        level_filter: If set, return only lines matching this log level
                      (e.g., "ERROR", "WARN", "INFO", "DEBUG").
        keyword: If set, return only lines containing this substring (case-insensitive).
    """
    base = get_settings()["url"]

    if target == "taskmanager":
        if not taskmanager_id:
            return "❌ taskmanager_id is required when target is 'taskmanager'."
        url = f"{base}/taskmanagers/{taskmanager_id}/logs/{log_file}"
    else:
        url = f"{base}/jobmanager/logs/{log_file}"

    try:
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.get(url, timeout=30.0)
            response.raise_for_status()
            content = response.text

        lines = content.splitlines()
        total_lines = len(lines)

        # Apply level filter
        if level_filter:
            level_upper = level_filter.strip().upper()
            lines = [l for l in lines if f" {level_upper} " in l or l.startswith(level_upper)]

        # Apply keyword filter
        if keyword:
            lines = [l for l in lines if keyword.lower() in l.lower()]

        # Apply tail
        if tail and tail > 0:
            lines = lines[-tail:]

        header = [
            f"Log File: {log_file}  [{target.capitalize()}{' / ' + taskmanager_id if taskmanager_id else ''}]",
            f"Total lines in file: {total_lines}",
        ]
        if level_filter:
            header.append(f"Level filter: {level_filter.upper()}")
        if keyword:
            header.append(f"Keyword filter: \"{keyword}\"")
        if tail:
            header.append(f"Showing last {tail} matching lines")
        header.append(f"Lines returned: {len(lines)}")
        header.append("=" * 70)

        if not lines:
            header.append("(no lines match the specified filters)")
        else:
            header.extend(lines)

        return "\n".join(header)

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return (
                f"❌ Log file '{log_file}' not found on {target}. "
                "Use list_flink_logs to see available files."
            )
        return f"❌ HTTP Error ({e.response.status_code}): {str(e)}"
    except httpx.TimeoutException:
        return f"❌ Request timeout while reading '{log_file}'."
    except Exception as e:
        logger.error(f"Failed to read Flink log '{log_file}': {e}")
        return f"❌ Error reading log file: {str(e)}"
