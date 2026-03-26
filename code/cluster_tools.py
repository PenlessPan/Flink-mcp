import asyncio
import logging

import httpx

from .app import mcp
from .connection import get_settings, MAX_OUTPUT_CHARS
from .utils import format_bytes, format_duration, format_timestamp

logger = logging.getLogger("flink-mcp-server")


@mcp.tool()
async def get_cluster_info() -> str:
    """Fetch an overview of the Flink cluster: jobs, slots, taskmanagers."""
    url = f"{get_settings()['url']}/overview"
    try:
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()

        return (
            f"Flink Cluster Info:\n"
            f"- TaskManagers: {data.get('taskmanagers')}\n"
            f"- Slots Total: {data.get('slots-total')}\n"
            f"- Slots Available: {data.get('slots-available')}\n"
            f"- Jobs Running: {data.get('jobs-running')}\n"
            f"- Jobs Finished: {data.get('jobs-finished')}\n"
            f"- Jobs Cancelled: {data.get('jobs-cancelled')}\n"
            f"- Jobs Failed: {data.get('jobs-failed')}\n"
            f"\nNote: on batch clusters, intermediate datasets may exist and are "
            f"accessible via the Flink REST API at /datasets."
        )
    except Exception as e:
        logger.error(f"Failed to fetch cluster info: {e}")
        return f"Error fetching cluster info: {str(e)}"


@mcp.tool()
async def list_jar_files() -> str:
    """List all uploaded JARs in the Flink cluster."""
    url = f"{get_settings()['url']}/jars"
    try:
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.get(url)
            response.raise_for_status()
            jars = response.json().get("files", [])

        if not jars:
            return "No JARs uploaded."

        return "\n".join([f"{jar['id']} - {jar['name']}" for jar in jars])
    except Exception as e:
        logger.error(f"Failed to list jars: {e}")
        return f"Error: {str(e)}"


@mcp.tool()
async def get_cluster_health() -> str:
    """
    Return a full cluster health snapshot: slot capacity, per-TaskManager
    resource utilization, active jobs, recent failures, and an overall
    health assessment.

    Concurrently fetches /overview, /taskmanagers, and /jobs/overview.
    """
    base = get_settings()["url"]

    async def _fetch(client, path):
        try:
            r = await client.get(f"{base}{path}", timeout=10.0)
            r.raise_for_status()
            return r.json()
        except Exception:
            return {}

    try:
        async with httpx.AsyncClient(verify=False) as client:
            overview, tm_data, jobs_data = await asyncio.gather(
                _fetch(client, "/overview"),
                _fetch(client, "/taskmanagers"),
                _fetch(client, "/jobs/overview"),
            )
    except Exception as e:
        logger.error(f"Failed to fetch cluster health: {e}")
        return f"❌ Error fetching cluster health: {str(e)}"

    tms = tm_data.get("taskmanagers", [])
    jobs = jobs_data.get("jobs", [])

    sep = "=" * 70
    lines = [sep, "CLUSTER HEALTH SNAPSHOT", sep]

    # ── 1. CLUSTER SUMMARY ────────────────────────────────────────────────
    total_slots = overview.get("slots-total", 0)
    avail_slots = overview.get("slots-available", 0)
    used_slots = total_slots - avail_slots
    slot_pct = (used_slots / total_slots * 100) if total_slots > 0 else 0

    lines.append("\n── CLUSTER SUMMARY ─────────────────────────────────────────────")
    lines.append(f"  Flink Version:   {overview.get('flink-version', 'N/A')}")
    lines.append(f"  TaskManagers:    {overview.get('taskmanagers', len(tms))}")
    lines.append(f"  Slots:           {used_slots}/{total_slots} used ({slot_pct:.0f}%)")
    lines.append(f"  Jobs Running:    {overview.get('jobs-running', 0)}")
    lines.append(f"  Jobs Finished:   {overview.get('jobs-finished', 0)}")
    lines.append(f"  Jobs Cancelled:  {overview.get('jobs-cancelled', 0)}")
    lines.append(f"  Jobs Failed:     {overview.get('jobs-failed', 0)}")

    # ── 2. TASKMANAGER HEALTH ─────────────────────────────────────────────
    tm_warnings = []
    lines.append("\n── TASKMANAGER HEALTH ──────────────────────────────────────────")

    if not tms:
        lines.append("  No TaskManagers found.")
    else:
        for tm in tms:
            tm_id = tm.get("id", "N/A")
            tm_slots_total = tm.get("slotsNumber", 0)
            tm_slots_free = tm.get("freeSlots", 0)
            tm_slots_used = tm_slots_total - tm_slots_free
            slot_util = (tm_slots_used / tm_slots_total * 100) if tm_slots_total > 0 else 0

            hw = tm.get("hardware", {})
            phys_mem = hw.get("physicalMemory", 0)
            free_mem = hw.get("freeMemory", 0)
            mem_util = ((phys_mem - free_mem) / phys_mem * 100) if phys_mem > 0 else 0

            slot_flag = " ⚠️ HIGH" if slot_util >= 90 else ""
            mem_flag = " ⚠️ HIGH" if mem_util >= 85 else ""
            lines.append(f"  {tm_id}")
            lines.append(f"    Slots: {tm_slots_used}/{tm_slots_total} ({slot_util:.0f}%){slot_flag}")
            if phys_mem > 0:
                lines.append(f"    Memory: {format_bytes(phys_mem - free_mem)} / {format_bytes(phys_mem)} ({mem_util:.0f}%){mem_flag}")

            if slot_util >= 90:
                tm_warnings.append(f"TM {tm_id}: slot utilization {slot_util:.0f}%")
            if mem_util >= 85:
                tm_warnings.append(f"TM {tm_id}: memory utilization {mem_util:.0f}%")

    # ── 3. ACTIVE JOBS ────────────────────────────────────────────────────
    running_jobs = [j for j in jobs if j.get("state") == "RUNNING"]
    lines.append("\n── ACTIVE JOBS ─────────────────────────────────────────────────")
    if running_jobs:
        for j in running_jobs:
            dur = format_duration(j.get("duration", 0))
            lines.append(f"  [{j.get('jid', 'N/A')}]  {j.get('name', 'N/A')}  ({dur})")
    else:
        lines.append("  No jobs currently running.")

    # ── 4. RECENT FAILURES ────────────────────────────────────────────────
    failed_jobs = [j for j in jobs if j.get("state") in ("FAILED", "CANCELED")]
    lines.append("\n── RECENT FAILURES ─────────────────────────────────────────────")
    if failed_jobs:
        for j in sorted(failed_jobs, key=lambda x: x.get("start-time", 0), reverse=True)[:10]:
            end_ts = j.get("end-time", -1)
            end_str = format_timestamp(end_ts) if end_ts > 0 else "N/A"
            lines.append(f"  [{j.get('state')}]  {j.get('name', 'N/A')}  ended {end_str}")
    else:
        lines.append("  No failed or cancelled jobs.")

    # ── 5. OVERALL ASSESSMENT ─────────────────────────────────────────────
    lines.append("\n── OVERALL ASSESSMENT ──────────────────────────────────────────")
    num_failed = overview.get("jobs-failed", 0)
    no_free_slots = avail_slots == 0 and total_slots > 0

    if no_free_slots or (tm_warnings and num_failed > 0):
        assessment = "CRITICAL"
        reason = "no free slots" if no_free_slots else f"{num_failed} failed job(s) with overloaded TaskManagers"
    elif num_failed > 0 or tm_warnings:
        assessment = "DEGRADED"
        parts = []
        if num_failed > 0:
            parts.append(f"{num_failed} failed job(s)")
        if tm_warnings:
            parts.append(f"{len(tm_warnings)} overloaded TaskManager(s)")
        reason = ", ".join(parts)
    else:
        assessment = "HEALTHY"
        reason = "all systems nominal"

    lines.append(f"  Status: {assessment} — {reason}")
    if tm_warnings:
        for w in tm_warnings:
            lines.append(f"  ⚠️  {w}")

    lines.append(sep)
    output = "\n".join(lines)
    if len(output) > MAX_OUTPUT_CHARS:
        output = output[:MAX_OUTPUT_CHARS] + f"\n\n⚠️ Output truncated at {MAX_OUTPUT_CHARS} characters."
    return output
