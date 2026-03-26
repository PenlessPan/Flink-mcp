import logging

import httpx

from .app import mcp
from .connection import get_settings, MAX_OUTPUT_CHARS
from .utils import format_bytes, format_duration, format_timestamp

logger = logging.getLogger("flink-mcp-server")


@mcp.tool()
async def get_job_checkpoints(job_id: str) -> str:
    """Get checkpoint history and summary counts for a Flink job.

    Args:
        job_id: The Flink job ID.

    Returns checkpoint counts, summary statistics, latest completed/failed
    checkpoint details, and a full checkpoint history table.
    """
    url = f"{get_settings()['url']}/jobs/{job_id}/checkpoints"
    try:
        async with httpx.AsyncClient(verify=False, timeout=10.0) as client:
            response = await client.get(url)
            if response.status_code == 404:
                return f"❌ Job not found or checkpointing not available: {job_id}"
            response.raise_for_status()
            data = response.json()

        counts = data.get("counts", {})
        summary = data.get("summary", {})
        latest = data.get("latest", {})
        history = data.get("history", [])

        lines = []
        lines.append("=" * 70)
        lines.append(f"CHECKPOINT HISTORY — Job {job_id}")
        lines.append("=" * 70)

        lines.append(f"\nCounts:")
        lines.append(f"  Restored:    {counts.get('restored', 0)}")
        lines.append(f"  Total:       {counts.get('total', 0)}")
        lines.append(f"  In Progress: {counts.get('in_progress', 0)}")
        lines.append(f"  Completed:   {counts.get('completed', 0)}")
        lines.append(f"  Failed:      {counts.get('failed', 0)}")

        if summary:
            lines.append(f"\nSummary (last completed):")
            state_size = summary.get("state_size", {})
            end_to_end = summary.get("end_to_end_duration", {})
            lines.append(
                f"  State Size (min/avg/max): "
                f"{format_bytes(state_size.get('min', 0))} / "
                f"{format_bytes(state_size.get('avg', 0))} / "
                f"{format_bytes(state_size.get('max', 0))}"
            )
            lines.append(
                f"  Duration   (min/avg/max): "
                f"{format_duration(end_to_end.get('min', 0))} / "
                f"{format_duration(end_to_end.get('avg', 0))} / "
                f"{format_duration(end_to_end.get('max', 0))}"
            )

        comp = latest.get("completed", {})
        if comp:
            lines.append(f"\nLatest Completed Checkpoint:")
            lines.append(f"  ID:         {comp.get('id', 'N/A')}")
            lines.append(f"  Status:     {comp.get('status', 'N/A')}")
            lines.append(f"  Duration:   {format_duration(comp.get('end_to_end_duration', 0))}")
            lines.append(f"  State Size: {format_bytes(comp.get('state_size', 0))}")
            lines.append(f"  Trigger:    {format_timestamp(comp.get('trigger_timestamp', 0))}")
            ext_path = comp.get("external_path", "")
            lines.append(f"  Ext. Path:  {ext_path if ext_path else 'N/A'}")

        failed = latest.get("failed", {})
        if failed:
            lines.append(f"\nLatest Failed Checkpoint:")
            lines.append(f"  ID:       {failed.get('id', 'N/A')}")
            lines.append(f"  Trigger:  {format_timestamp(failed.get('trigger_timestamp', 0))}")
            fail_msg = failed.get("failure_message", "N/A")
            lines.append(f"  Reason:   {fail_msg}")

        if history:
            lines.append(f"\nCheckpoint History (last {len(history)}):")
            lines.append(f"  {'ID':<8} {'Status':<12} {'Duration':<16} {'State Size':<16} {'Trigger Time'}")
            lines.append("  " + "-" * 65)
            for ckpt in history:
                cid = ckpt.get("id", "?")
                status = ckpt.get("status", "?")
                dur = format_duration(ckpt.get("end_to_end_duration", 0))
                sz = format_bytes(ckpt.get("state_size", 0))
                ts = format_timestamp(ckpt.get("trigger_timestamp", 0))
                lines.append(f"  {cid:<8} {status:<12} {dur:<16} {sz:<16} {ts}")

        lines.append("\n" + "=" * 70)
        return "\n".join(lines)

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return f"❌ Job not found or checkpointing not available: {job_id}"
        logger.error(f"HTTP error fetching checkpoints: {e}")
        return f"❌ HTTP Error ({e.response.status_code}): {str(e)}"
    except httpx.TimeoutException:
        logger.error("Timeout fetching checkpoints")
        return "❌ Request timeout while fetching checkpoints"
    except Exception as e:
        logger.error(f"Failed to get job checkpoints: {e}")
        return f"❌ Error getting job checkpoints: {str(e)}"


@mcp.tool()
async def get_checkpoint_details(job_id: str, checkpoint_id: int) -> str:
    """Get per-subtask breakdown for a specific checkpoint.

    Args:
        job_id: The Flink job ID.
        checkpoint_id: The checkpoint ID to inspect.

    Returns per-operator/subtask checkpoint duration, state size, and status.
    Useful for pinpointing which operator is causing checkpoint delays.
    """
    url = f"{get_settings()['url']}/jobs/{job_id}/checkpoints/details/{checkpoint_id}"
    try:
        async with httpx.AsyncClient(verify=False, timeout=10.0) as client:
            response = await client.get(url)
            if response.status_code == 404:
                return f"❌ Checkpoint {checkpoint_id} not found for job {job_id}"
            response.raise_for_status()
            data = response.json()

        lines = []
        lines.append("=" * 70)
        lines.append(f"CHECKPOINT DETAILS — Job {job_id} / Checkpoint {checkpoint_id}")
        lines.append("=" * 70)
        lines.append(f"  Status:       {data.get('status', 'N/A')}")
        lines.append(f"  Duration:     {format_duration(data.get('end_to_end_duration', 0))}")
        lines.append(f"  State Size:   {format_bytes(data.get('state_size', 0))}")
        lines.append(f"  Trigger Time: {format_timestamp(data.get('trigger_timestamp', 0))}")
        ext_path = data.get("external_path", "")
        lines.append(f"  Ext. Path:    {ext_path if ext_path else 'N/A'}")

        tasks = data.get("tasks", {})
        if tasks:
            lines.append(f"\nPer-Operator Breakdown ({len(tasks)} operator(s)):")
            for vertex_id, vertex_data in tasks.items():
                vertex_name = vertex_data.get("id", vertex_id)
                lines.append(f"\n  Operator: {vertex_name}")
                subtasks = vertex_data.get("subtasks", [])
                if subtasks:
                    lines.append(f"  {'#':<5} {'Status':<14} {'Duration':<14} {'State Size':<14} {'Sync Duration'}")
                    lines.append("  " + "-" * 65)
                    for st in subtasks:
                        idx = st.get("index", "?")
                        status = st.get("status", "?")
                        dur = format_duration(st.get("end_to_end_duration", 0))
                        sz = format_bytes(st.get("state_size", 0))
                        sync_dur = format_duration(st.get("sync_duration", 0))
                        lines.append(f"  {idx:<5} {status:<14} {dur:<14} {sz:<14} {sync_dur}")
                else:
                    lines.append("    No subtask data.")

        lines.append("\n" + "=" * 70)
        return "\n".join(lines)

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return f"❌ Checkpoint {checkpoint_id} not found for job {job_id}"
        logger.error(f"HTTP error fetching checkpoint details: {e}")
        return f"❌ HTTP Error ({e.response.status_code}): {str(e)}"
    except httpx.TimeoutException:
        logger.error("Timeout fetching checkpoint details")
        return "❌ Request timeout while fetching checkpoint details"
    except Exception as e:
        logger.error(f"Failed to get checkpoint details: {e}")
        return f"❌ Error getting checkpoint details: {str(e)}"
