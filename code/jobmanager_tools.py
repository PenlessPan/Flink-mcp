import logging

import httpx

from .app import mcp
from .connection import get_settings, MAX_OUTPUT_CHARS
from .utils import format_bytes

logger = logging.getLogger("flink-mcp-server")


@mcp.tool()
async def get_jobmanager_metrics(metric_names: str = None) -> str:
    """Get metrics for the JobManager.

    Args:
        metric_names: Optional comma-separated metric names to query.
                      If omitted, lists all available metric IDs.

    Key metrics: Status.JVM.Memory.Heap.Used, Status.JVM.Memory.Heap.Max,
    Status.JVM.GarbageCollector.*.Count, Status.JVM.GarbageCollector.*.Time,
    Status.JVM.Threads.Count, Status.JVM.CPU.Load.
    """
    base_url = f"{get_settings()['url']}/jobmanager/metrics"
    try:
        async with httpx.AsyncClient(verify=False, timeout=10.0) as client:
            if metric_names:
                url = f"{base_url}?get={metric_names}"
                response = await client.get(url)
                response.raise_for_status()
                metrics = response.json()

                lines = []
                lines.append("=" * 70)
                lines.append("JOBMANAGER METRICS")
                lines.append("=" * 70)
                for m in metrics:
                    mid = m.get("id", "?")
                    val = m.get("value", "N/A")
                    if "Memory" in mid and "Ratio" not in mid:
                        try:
                            val = format_bytes(float(val))
                        except (ValueError, TypeError):
                            pass
                    elif "CPU.Load" in mid or "Ratio" in mid:
                        try:
                            fval = float(val)
                            val = f"{fval * 100:.2f}%" if fval <= 1 else f"{fval:.2f}%"
                        except (ValueError, TypeError):
                            pass
                    elif "Count" in mid or "Threads" in mid:
                        try:
                            val = f"{int(float(val)):,}"
                        except (ValueError, TypeError):
                            pass
                    lines.append(f"  {mid}: {val}")
                lines.append("=" * 70)
                return "\n".join(lines)
            else:
                response = await client.get(base_url)
                response.raise_for_status()
                metrics = response.json()

                lines = []
                lines.append("=" * 70)
                lines.append("AVAILABLE JOBMANAGER METRICS")
                lines.append("=" * 70)
                lines.append(f"\nTotal metrics available: {len(metrics)}\n")

                categories: dict = {}
                for m in metrics:
                    mid = m.get("id", "")
                    parts = mid.split(".")
                    cat = ".".join(parts[:3]) if len(parts) >= 3 else "Other"
                    categories.setdefault(cat, []).append(mid)

                for cat in sorted(categories.keys()):
                    lines.append(f"{cat}:")
                    for mid in sorted(categories[cat]):
                        lines.append(f"  - {mid}")

                lines.append("\n" + "=" * 70)
                lines.append("💡 TIP: To query specific metrics, pass metric_names as a comma-separated string.")
                lines.append("   Key metrics: Status.JVM.Memory.Heap.Used, Status.JVM.Memory.Heap.Max,")
                lines.append("   Status.JVM.Threads.Count, Status.JVM.CPU.Load")
                lines.append("=" * 70)
                return "\n".join(lines)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error fetching JobManager metrics: {e}")
        return f"❌ HTTP Error ({e.response.status_code}): {str(e)}"
    except httpx.TimeoutException:
        logger.error("Timeout fetching JobManager metrics")
        return "❌ Request timeout while fetching JobManager metrics"
    except Exception as e:
        logger.error(f"Failed to get JobManager metrics: {e}")
        return f"❌ Error getting JobManager metrics: {str(e)}"


@mcp.tool()
async def get_jobmanager_config() -> str:
    """Get the full effective cluster configuration from the JobManager.

    Returns all configuration key-value pairs grouped by key prefix
    (e.g. state.*, execution.*, taskmanager.*, rest.*).
    """
    url = f"{get_settings()['url']}/jobmanager/config"
    try:
        async with httpx.AsyncClient(verify=False, timeout=10.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            entries = response.json()

        # entries may be a list of {"key": ..., "value": ...} or a plain dict
        if isinstance(entries, list):
            config = {e.get("key", ""): e.get("value", "") for e in entries}
        elif isinstance(entries, dict):
            config = entries
        else:
            return f"Unexpected config format: {type(entries)}"

        # Group by prefix (first segment before ".")
        groups: dict = {}
        for key, value in sorted(config.items()):
            prefix = key.split(".")[0] if "." in key else "other"
            groups.setdefault(prefix, []).append((key, value))

        lines = []
        lines.append("=" * 70)
        lines.append("JOBMANAGER EFFECTIVE CONFIGURATION")
        lines.append("=" * 70)
        lines.append(f"Total entries: {len(config)}\n")

        for prefix in sorted(groups.keys()):
            lines.append(f"[{prefix}.*]")
            for key, value in groups[prefix]:
                lines.append(f"  {key} = {value}")
            lines.append("")

        lines.append("=" * 70)
        output = "\n".join(lines)
        if len(output) > MAX_OUTPUT_CHARS:
            output = output[:MAX_OUTPUT_CHARS] + f"\n\n⚠️ Output truncated at {MAX_OUTPUT_CHARS} characters."
        return output

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error fetching JobManager config: {e}")
        return f"❌ HTTP Error ({e.response.status_code}): {str(e)}"
    except httpx.TimeoutException:
        logger.error("Timeout fetching JobManager config")
        return "❌ Request timeout while fetching JobManager config"
    except Exception as e:
        logger.error(f"Failed to get JobManager config: {e}")
        return f"❌ Error getting JobManager config: {str(e)}"


@mcp.tool()
async def get_jobmanager_environment() -> str:
    """Get JVM and environment information from the JobManager.

    Returns JVM version, max heap size, classpath entries, and environment
    variables as reported by the JobManager.
    """
    url = f"{get_settings()['url']}/jobmanager/environment"
    try:
        async with httpx.AsyncClient(verify=False, timeout=10.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()

        lines = []
        lines.append("=" * 70)
        lines.append("JOBMANAGER ENVIRONMENT")
        lines.append("=" * 70)

        jvm = data.get("jvm", {})
        if jvm:
            lines.append("\nJVM:")
            lines.append(f"  Version:    {jvm.get('version', 'N/A')}")
            lines.append(f"  Arch:       {jvm.get('arch', 'N/A')}")
            max_heap = jvm.get("options", {})
            if isinstance(max_heap, dict):
                lines.append(f"  Max Heap:   {format_bytes(max_heap.get('maxHeapSize', 0))}")
            lines.append(f"  Options:    {jvm.get('options', 'N/A')}")

            classpath = jvm.get("classpath", [])
            if classpath:
                lines.append(f"\nClasspath ({len(classpath)} entries):")
                for entry in classpath:
                    lines.append(f"  - {entry}")

        env_vars = data.get("environment-variables", {})
        if env_vars:
            lines.append(f"\nEnvironment Variables ({len(env_vars)}):")
            for key in sorted(env_vars.keys()):
                lines.append(f"  {key} = {env_vars[key]}")

        # Some Flink versions return a flat list of key-value entries
        if not jvm and not env_vars and isinstance(data, list):
            lines.append("\nRaw entries:")
            for entry in data:
                lines.append(f"  {entry.get('key', '?')} = {entry.get('value', '?')}")

        lines.append("\n" + "=" * 70)
        output = "\n".join(lines)
        if len(output) > MAX_OUTPUT_CHARS:
            output = output[:MAX_OUTPUT_CHARS] + f"\n\n⚠️ Output truncated at {MAX_OUTPUT_CHARS} characters."
        return output

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error fetching JobManager environment: {e}")
        return f"❌ HTTP Error ({e.response.status_code}): {str(e)}"
    except httpx.TimeoutException:
        logger.error("Timeout fetching JobManager environment")
        return "❌ Request timeout while fetching JobManager environment"
    except Exception as e:
        logger.error(f"Failed to get JobManager environment: {e}")
        return f"❌ Error getting JobManager environment: {str(e)}"
