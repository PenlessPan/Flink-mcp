import json
import logging

import httpx

from .app import mcp
from .connection import get_settings, MAX_OUTPUT_CHARS
from .utils import format_bytes, format_duration, format_timestamp

logger = logging.getLogger("flink-mcp-server")


@mcp.tool()
async def get_vertex_info(job_id: str, vertex_id: str, info_category: str) -> str:
    """
    Retrieve information about a specific job vertex / operator.

    Args:
        job_id: The Flink job ID.
        vertex_id: The vertex (operator) ID.
        info_category: What to retrieve. One of:
            - "backpressure"      → Back-pressure ratio sampled across subtasks
                                    (GET /jobs/{job_id}/vertices/{vertex_id}/backpressure)
            - "metrics"           → Available or queried metrics for the vertex
                                    (GET /jobs/{job_id}/vertices/{vertex_id}/metrics)
            - "subtask_times"     → Per-subtask state-transition timestamps and durations
                                    (GET /jobs/{job_id}/vertices/{vertex_id}/subtasktimes)
            - "taskmanager_stats" → Per-TaskManager I/O stats for this vertex
                                    (GET /jobs/{job_id}/vertices/{vertex_id}/taskmanagers)
            - "accumulators"      → Per-subtask user-defined accumulators
                                    (GET /jobs/{job_id}/vertices/{vertex_id}/accumulators)
    """
    base = get_settings()["url"]
    CATEGORIES = {
        "backpressure": f"{base}/jobs/{job_id}/vertices/{vertex_id}/backpressure",
        "metrics": f"{base}/jobs/{job_id}/vertices/{vertex_id}/metrics",
        "subtask_times": f"{base}/jobs/{job_id}/vertices/{vertex_id}/subtasktimes",
        "taskmanager_stats": f"{base}/jobs/{job_id}/vertices/{vertex_id}/taskmanagers",
        "accumulators": f"{base}/jobs/{job_id}/vertices/{vertex_id}/accumulators",
    }

    if info_category not in CATEGORIES:
        return (
            f"❌ Unknown info_category '{info_category}'. "
            f"Valid options: {', '.join(CATEGORIES)}"
        )

    url = CATEGORIES[info_category]

    try:
        async with httpx.AsyncClient(verify=False, timeout=10.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()

        output = (
            f"Vertex {info_category} — Job {job_id} / Vertex {vertex_id}\n"
            + "=" * 70 + "\n"
            + json.dumps(data, indent=2)
        )
        if len(output) > MAX_OUTPUT_CHARS:
            output = output[:MAX_OUTPUT_CHARS] + f"\n\n⚠️ Output truncated at {MAX_OUTPUT_CHARS} characters."
        return output

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return f"❌ Job or vertex not found: {job_id} / {vertex_id}"
        return f"❌ HTTP Error ({e.response.status_code}): {str(e)}"
    except httpx.TimeoutException:
        return f"❌ Request timeout fetching {info_category} for vertex {vertex_id}."
    except Exception as e:
        logger.error(f"Failed to get vertex {info_category}: {e}")
        return f"❌ Error fetching vertex {info_category}: {str(e)}"


@mcp.tool()
async def get_vertex_details(job_id: str, vertex_id: str) -> str:
    """
    Return full details for a job vertex: name, status, parallelism,
    per-subtask breakdown (index, status, host, start/end time, duration),
    and aggregated I/O metrics. Subtasks not in RUNNING state are flagged.

    Args:
        job_id: The Flink job ID.
        vertex_id: The vertex (operator) ID.
    """
    url = f"{get_settings()['url']}/jobs/{job_id}/vertices/{vertex_id}"
    try:
        async with httpx.AsyncClient(verify=False, timeout=10.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()

        name = data.get("name", "N/A")
        parallelism = data.get("parallelism", "N/A")
        now_ts = data.get("now", 0)
        subtasks = data.get("subtasks", [])

        lines = [
            f"Vertex Details — {name}",
            "=" * 70,
            f"Vertex ID:   {vertex_id}",
            f"Job ID:      {job_id}",
            f"Parallelism: {parallelism}",
            f"Subtasks:    {len(subtasks)}",
            "",
            "Subtask Breakdown:",
            "-" * 70,
        ]

        non_running = []
        for st in subtasks:
            idx = st.get("subtask", "?")
            status = st.get("status", "UNKNOWN")
            host = st.get("host", "N/A")
            start_ms = st.get("start-time", 0)
            end_ms = st.get("end-time", -1)
            duration_ms = st.get("duration", 0)

            flag = "⚠️ " if status not in ("RUNNING", "FINISHED") else "   "
            if status not in ("RUNNING", "FINISHED"):
                non_running.append((idx, status))

            lines.append(f"{flag}[{idx:>3}]  {status:<12}  host={host}")
            lines.append(f"        start={format_timestamp(start_ms)}  "
                         f"end={'running' if end_ms < 0 else format_timestamp(end_ms)}  "
                         f"duration={format_duration(duration_ms)}")

            # Per-subtask metrics if present
            metrics = st.get("metrics", {})
            if metrics:
                r_rec = metrics.get("read-records", 0)
                w_rec = metrics.get("write-records", 0)
                r_bytes = metrics.get("read-bytes", 0)
                w_bytes = metrics.get("write-bytes", 0)
                if r_rec or r_bytes:
                    lines.append(f"        read:  {r_rec:,} records  {format_bytes(r_bytes)}")
                if w_rec or w_bytes:
                    lines.append(f"        write: {w_rec:,} records  {format_bytes(w_bytes)}")
            lines.append("")

        # Aggregated metrics
        agg = data.get("aggregated", {})
        if agg:
            lines.append("-" * 70)
            lines.append("Aggregated I/O Metrics:")
            r_bytes = agg.get("read-bytes", {}).get("sum", 0)
            w_bytes = agg.get("write-bytes", {}).get("sum", 0)
            r_rec = agg.get("read-records", {}).get("sum", 0)
            w_rec = agg.get("write-records", {}).get("sum", 0)
            lines.append(f"  Read:  {r_rec:,} records  {format_bytes(r_bytes)}")
            lines.append(f"  Write: {w_rec:,} records  {format_bytes(w_bytes)}")

        if non_running:
            lines.append("")
            lines.append("⚠️  Non-running subtasks detected:")
            for idx, status in non_running:
                lines.append(f"   Subtask {idx}: {status}")

        lines.append("=" * 70)
        return "\n".join(lines)

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return f"❌ Job or vertex not found: {job_id} / {vertex_id}"
        return f"❌ HTTP Error ({e.response.status_code}): {str(e)}"
    except httpx.TimeoutException:
        return f"❌ Request timeout fetching vertex details for {vertex_id}."
    except Exception as e:
        logger.error(f"Failed to get vertex details for {vertex_id}: {e}")
        return f"❌ Error fetching vertex details: {str(e)}"


@mcp.tool()
async def get_vertex_flamegraph(job_id: str, vertex_id: str) -> str:
    """
    Return CPU flame graph data for a job vertex.

    Requires Flink 1.17+ with rest.profiling.enabled: true set in the
    cluster configuration. If the cluster returns 404 or 405 this tool
    will explain what configuration is needed rather than surfacing a raw
    HTTP error.

    The flame graph JSON is returned as-is because it is intended for
    external rendering tools (e.g., d3-flame-graph, speedscope).

    Args:
        job_id: The Flink job ID.
        vertex_id: The vertex (operator) ID.
    """
    url = f"{get_settings()['url']}/jobs/{job_id}/vertices/{vertex_id}/flamegraph"
    try:
        async with httpx.AsyncClient(verify=False, timeout=10.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()

        output = (
            f"Flame Graph — Vertex {vertex_id}  (Job {job_id})\n"
            + "=" * 70 + "\n"
            + json.dumps(data, indent=2)
        )
        if len(output) > MAX_OUTPUT_CHARS:
            output = output[:MAX_OUTPUT_CHARS] + f"\n\n⚠️ Output truncated at {MAX_OUTPUT_CHARS} characters."
        return output

    except httpx.HTTPStatusError as e:
        if e.response.status_code in (404, 405):
            return (
                f"❌ Flame graph endpoint not available (HTTP {e.response.status_code}).\n\n"
                "To enable CPU profiling, add the following to your Flink cluster config:\n"
                "  rest.profiling.enabled: true\n\n"
                "This feature requires Flink 1.17 or later. After updating the config,\n"
                "restart the JobManager and re-run this tool."
            )
        return f"❌ HTTP Error ({e.response.status_code}): {str(e)}"
    except httpx.TimeoutException:
        return f"❌ Request timeout fetching flame graph for vertex {vertex_id}."
    except Exception as e:
        logger.error(f"Failed to get flame graph for {vertex_id}: {e}")
        return f"❌ Error fetching flame graph: {str(e)}"
