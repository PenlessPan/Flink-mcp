import asyncio
import json
import logging

import httpx

from .app import mcp
from .connection import get_settings, MAX_OUTPUT_CHARS
from .utils import format_bytes, format_duration, format_timestamp

logger = logging.getLogger("flink-mcp-server")


@mcp.tool()
async def get_vertex_details(job_id: str, vertex_id: str) -> str:
    """
    Return full details for a job vertex: name, status, parallelism,
    per-subtask breakdown (index, status, host, start/end time, duration),
    aggregated I/O metrics, and user-defined accumulators (if any).
    Subtasks not in RUNNING state are flagged.

    Args:
        job_id: The Flink job ID.
        vertex_id: The vertex (operator) ID.
    """
    base = get_settings()["url"]
    details_url = f"{base}/jobs/{job_id}/vertices/{vertex_id}"
    accumulators_url = f"{base}/jobs/{job_id}/vertices/{vertex_id}/accumulators"

    async def _fetch(client, url):
        try:
            r = await client.get(url)
            r.raise_for_status()
            return r.json()
        except Exception:
            return None

    try:
        async with httpx.AsyncClient(verify=False, timeout=10.0) as client:
            data, acc_data = await asyncio.gather(
                _fetch(client, details_url),
                _fetch(client, accumulators_url),
            )

        if data is None:
            return f"❌ Job or vertex not found: {job_id} / {vertex_id}"

        name = data.get("name", "N/A")
        parallelism = data.get("parallelism", "N/A")
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

        # Accumulators
        if acc_data:
            subtask_accs = acc_data.get("subtasks-accumulators", [])
            # Flatten to unique accumulator names/values across subtasks
            acc_map: dict = {}
            for subtask_entry in subtask_accs:
                for acc in subtask_entry.get("user-accumulators", []):
                    name_key = acc.get("name", "")
                    if name_key and name_key not in acc_map:
                        acc_map[name_key] = {"type": acc.get("type", "N/A"), "values": []}
                    if name_key:
                        acc_map[name_key]["values"].append(acc.get("value", "N/A"))

            if acc_map:
                lines.append("")
                lines.append("-" * 70)
                lines.append(f"User Accumulators ({len(acc_map)}):")
                for acc_name, acc_info in acc_map.items():
                    lines.append(f"  {acc_name}  [{acc_info['type']}]")
                    for v in acc_info["values"]:
                        lines.append(f"    {v}")

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


@mcp.tool()
async def find_bottleneck(job_id: str) -> str:
    """
    Identify the bottleneck operator(s) in a running Flink job.

    Step 1: Fetches the job vertex list from GET /jobs/{job_id}.
    Step 2: Concurrently queries backpressure and I/O metrics for every vertex.
    Results are ranked by backpressure percentage (highest first).

    Returns a BOTTLENECK RANKING table, identifies the root cause candidate,
    and provides actionable recommendations. Non-RUNNING jobs are handled
    gracefully with a clear status message.

    Args:
        job_id: The Flink job ID.
    """
    base = get_settings()["url"]

    # Step 1 — get vertex list and job state
    try:
        async with httpx.AsyncClient(verify=False, timeout=10.0) as client:
            r = await client.get(f"{base}/jobs/{job_id}")
            r.raise_for_status()
            job_data = r.json()
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return f"❌ Job not found: {job_id}"
        return f"❌ HTTP Error ({e.response.status_code}): {str(e)}"
    except Exception as e:
        return f"❌ Error fetching job: {str(e)}"

    state = job_data.get("state", "UNKNOWN")
    if state != "RUNNING":
        return (
            f"Job {job_id} is not currently RUNNING (state: {state}).\n"
            "Bottleneck analysis requires a running job."
        )

    vertices = job_data.get("vertices", [])
    if not vertices:
        return f"❌ No vertices found for job {job_id}."

    # Step 2 — concurrent backpressure + metrics per vertex
    METRICS = "busyTimeMsPerSecond,backPressuredTimeMsPerSecond,numRecordsInPerSecond,numRecordsOutPerSecond"

    async def _fetch_vertex(client, vid):
        bp_url = f"{base}/jobs/{job_id}/vertices/{vid}/backpressure"
        metrics_url = f"{base}/jobs/{job_id}/vertices/{vid}/metrics?get={METRICS}"
        try:
            bp_r, m_r = await asyncio.gather(
                client.get(bp_url, timeout=10.0),
                client.get(metrics_url, timeout=10.0),
            )
            bp_data = bp_r.json() if bp_r.status_code == 200 else {}
            m_data = m_r.json() if m_r.status_code == 200 else []
        except Exception:
            bp_data, m_data = {}, []
        return bp_data, m_data

    try:
        async with httpx.AsyncClient(verify=False) as client:
            results = await asyncio.gather(
                *[_fetch_vertex(client, v["id"]) for v in vertices]
            )
    except Exception as e:
        return f"❌ Error fetching vertex metrics: {str(e)}"

    # Parse and rank
    ranked = []
    for v, (bp_data, m_data) in zip(vertices, results):
        vid = v.get("id", "?")
        name = v.get("name", vid)
        parallelism = v.get("parallelism", "?")

        # Backpressure: ratio field (0.0–1.0) or backPressuredTimeMsPerSecond (0–1000)
        bp_ratio = bp_data.get("backpressure-level", None)  # "ok" / "low" / "high"
        bp_pct: float = 0.0

        # Try metrics first (more precise)
        m_map = {}
        if isinstance(m_data, list):
            for m in m_data:
                try:
                    m_map[m.get("id", "")] = float(m.get("value", 0))
                except (TypeError, ValueError):
                    pass

        bp_ms = m_map.get("backPressuredTimeMsPerSecond", None)
        busy_ms = m_map.get("busyTimeMsPerSecond", None)
        if bp_ms is not None:
            bp_pct = bp_ms / 10.0  # convert ms/s → percentage

        # Fall back to subtask backpressure data
        if bp_pct == 0.0 and bp_data.get("subtasks"):
            subtask_bps = [
                s.get("backPressuredTimeMsPerSecond", s.get("ratio", 0) * 1000)
                for s in bp_data.get("subtasks", [])
            ]
            if subtask_bps:
                bp_pct = max(subtask_bps) / 10.0

        records_in = m_map.get("numRecordsInPerSecond", None)
        records_out = m_map.get("numRecordsOutPerSecond", None)
        busy_pct = (busy_ms / 10.0) if busy_ms is not None else None

        ranked.append({
            "id": vid,
            "name": name,
            "parallelism": parallelism,
            "bp_pct": bp_pct,
            "busy_pct": busy_pct,
            "records_in": records_in,
            "records_out": records_out,
        })

    ranked.sort(key=lambda x: x["bp_pct"], reverse=True)

    sep = "=" * 70
    lines = [sep, f"BOTTLENECK ANALYSIS — Job {job_id}", sep]

    # ── BOTTLENECK RANKING ────────────────────────────────────────────────
    lines.append("\n── BOTTLENECK RANKING (by backpressure %) ──────────────────────")
    lines.append(f"  {'#':<3}  {'Vertex':<35}  {'BP%':>5}  {'Busy%':>6}  {'Rec/s In':>10}  {'Rec/s Out':>10}")
    lines.append("  " + "-" * 65)
    for rank, v in enumerate(ranked, 1):
        bp_str = f"{v['bp_pct']:.0f}%"
        busy_str = f"{v['busy_pct']:.0f}%" if v["busy_pct"] is not None else "N/A"
        rin = f"{v['records_in']:.0f}" if v["records_in"] is not None else "N/A"
        rout = f"{v['records_out']:.0f}" if v["records_out"] is not None else "N/A"
        flag = " ⚠️" if v["bp_pct"] >= 50 else ("  " if v["bp_pct"] < 10 else "  ")
        lines.append(f"  {rank:<3}  {v['name'][:35]:<35}  {bp_str:>5}  {busy_str:>6}  {rin:>10}  {rout:>10}{flag}")

    # ── ROOT CAUSE CANDIDATE ──────────────────────────────────────────────
    top = ranked[0]
    lines.append("\n── ROOT CAUSE CANDIDATE ────────────────────────────────────────")
    if top["bp_pct"] >= 10:
        lines.append(f"  Vertex:      {top['name']}")
        lines.append(f"  ID:          {top['id']}")
        lines.append(f"  BP:          {top['bp_pct']:.0f}%")
        lines.append(f"  Parallelism: {top['parallelism']}")
        if top["busy_pct"] is not None:
            lines.append(f"  Busy:        {top['busy_pct']:.0f}%")
    else:
        lines.append("  No significant backpressure detected across all vertices.")

    # ── RECOMMENDATIONS ───────────────────────────────────────────────────
    lines.append("\n── RECOMMENDATIONS ─────────────────────────────────────────────")
    if top["bp_pct"] >= 80:
        lines.append(f"  ⚠️  SEVERE backpressure on '{top['name']}'.")
        lines.append("  → Increase parallelism of this operator.")
        lines.append("  → Check for slow sink I/O or downstream congestion.")
        lines.append("  → Review state backend performance (RocksDB compaction?)")
    elif top["bp_pct"] >= 30:
        lines.append(f"  Moderate backpressure on '{top['name']}'.")
        lines.append("  → Consider increasing parallelism for this operator.")
        lines.append("  → Profile CPU/memory usage via get_vertex_flamegraph.")
    elif top["bp_pct"] >= 10:
        lines.append(f"  Low backpressure on '{top['name']}' — monitor for growth.")
    else:
        lines.append("  Job appears to be running without significant backpressure.")

    lines.append(sep)
    output = "\n".join(lines)
    if len(output) > MAX_OUTPUT_CHARS:
        output = output[:MAX_OUTPUT_CHARS] + f"\n\n⚠️ Output truncated at {MAX_OUTPUT_CHARS} characters."
    return output
