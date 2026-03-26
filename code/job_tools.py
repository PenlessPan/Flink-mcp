import logging
from typing import List, Dict, Any

import httpx

from .app import mcp
from .connection import get_settings, MAX_OUTPUT_CHARS
from .utils import format_bytes, format_duration, format_timestamp, to_num, index_by_id, chunk

logger = logging.getLogger("flink-mcp-server")


@mcp.tool()
async def list_jobs() -> str:
    """List all current and recent Flink jobs with their status."""
    url = f"{get_settings()['url']}/jobs/overview"
    try:
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.get(url)
            response.raise_for_status()
            jobs = response.json().get("jobs", [])

        if not jobs:
            return "No jobs found."

        result = ["Flink Jobs Overview:"]
        for job in jobs:
            result.append(
                f"- ID: {job.get('jid')} | Name: {job.get('name')} | State: {job.get('state')}"
            )
        return "\n".join(result)
    except Exception as e:
        logger.error(f"Failed to fetch job list: {e}")
        return f"Error fetching jobs: {str(e)}"


@mcp.tool()
async def list_job_ids() -> str:
    """
    List all job IDs known to the cluster with their current status.
    Lighter alternative to list_jobs — hits GET /jobs instead of /jobs/overview.
    """
    url = f"{get_settings()['url']}/jobs"
    try:
        async with httpx.AsyncClient(verify=False, timeout=10.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()

        jobs = data.get("jobs", [])
        if not jobs:
            return "No jobs found on the cluster."

        lines = ["Job IDs on Cluster", "=" * 50]
        for job in jobs:
            jid = job.get("id", "N/A")
            status = job.get("status", "UNKNOWN")
            lines.append(f"  {jid}  [{status}]")
        lines.append("=" * 50)
        lines.append(f"Total: {len(jobs)} job(s)")
        return "\n".join(lines)

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return "❌ /jobs endpoint not found."
        return f"❌ HTTP Error ({e.response.status_code}): {str(e)}"
    except httpx.TimeoutException:
        return "❌ Request timeout while listing job IDs."
    except Exception as e:
        logger.error(f"Failed to list job IDs: {e}")
        return f"❌ Error listing job IDs: {str(e)}"


@mcp.tool()
async def get_job_details(job_id: str) -> str:
    """Get comprehensive details of a specific Flink job by job ID including configuration,
    vertices, metrics, and execution plan."""
    # Fetch both job details and config in parallel
    base = get_settings()["url"]
    details_url = f"{base}/jobs/{job_id}"
    config_url = f"{base}/jobs/{job_id}/config"

    job_data = None
    config_data = None

    try:
        async with httpx.AsyncClient(verify=False, timeout=10.0) as client:
            # Fetch job details
            try:
                details_response = await client.get(details_url)
                details_response.raise_for_status()
                job_data = details_response.json()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    return f"❌ Job not found: {job_id}\nThis job may have been removed from the cluster history."
                logger.error(f"HTTP error fetching job details: {e}")
                return f"❌ Error fetching job details (HTTP {e.response.status_code}): {str(e)}"
            except Exception as e:
                logger.error(f"Failed to fetch job details: {e}")
                return f"❌ Error fetching job details: {str(e)}"

            # Fetch job config
            try:
                config_response = await client.get(config_url)
                config_response.raise_for_status()
                config_data = config_response.json()
            except Exception as e:
                logger.warning(f"Failed to fetch job config (non-critical): {e}")
                # Config fetch failure is non-critical, continue with job data

    except Exception as e:
        logger.error(f"Failed to create HTTP client: {e}")
        return f"❌ Network error: {str(e)}"

    if not job_data:
        return f"❌ No data retrieved for job {job_id}"

    # Build comprehensive output
    output_lines = []

    # === BASIC INFO ===
    output_lines.append("=" * 60)
    output_lines.append("JOB OVERVIEW")
    output_lines.append("=" * 60)
    output_lines.append(f"Job ID: {job_data.get('jid', 'N/A')}")
    output_lines.append(f"Name: {job_data.get('name', 'N/A')}")
    output_lines.append(f"State: {job_data.get('state', 'UNKNOWN')}")
    output_lines.append(f"Type: {job_data.get('plan', {}).get('type', 'N/A')}")

    # Timing information
    start_time = job_data.get('start-time', 0)
    end_time = job_data.get('end-time', -1)
    duration = job_data.get('duration', 0)

    output_lines.append(f"\nStart Time: {start_time} ({format_timestamp(start_time)})")
    if end_time > 0:
        output_lines.append(f"End Time: {end_time} ({format_timestamp(end_time)})")
    output_lines.append(f"Duration: {format_duration(duration)}")

    # Timestamps
    timestamps = job_data.get('timestamps', {})
    if timestamps:
        output_lines.append("\nState Transitions:")
        for state, ts in timestamps.items():
            if ts > 0:
                output_lines.append(f"  - {state}: {format_timestamp(ts)}")

    # === CONFIGURATION ===
    output_lines.append("\n" + "=" * 60)
    output_lines.append("CONFIGURATION")
    output_lines.append("=" * 60)

    if config_data:
        exec_config = config_data.get('execution-config', {})
        output_lines.append(f"Execution Mode: {exec_config.get('execution-mode', 'N/A')}")
        output_lines.append(f"Restart Strategy: {exec_config.get('restart-strategy', 'N/A')}")
        output_lines.append(f"Job Parallelism: {exec_config.get('job-parallelism', 'N/A')}")
        output_lines.append(f"Object Reuse: {exec_config.get('object-reuse-mode', False)}")

        user_config = exec_config.get('user-config', {})
        if user_config:
            output_lines.append("\nUser Configuration:")
            for key, value in user_config.items():
                output_lines.append(f"  - {key}: {value}")
    else:
        output_lines.append("⚠️  Configuration data unavailable")

    output_lines.append(f"\nMax Parallelism: {job_data.get('maxParallelism', 'Not set')}")
    output_lines.append(f"Stoppable: {job_data.get('isStoppable', False)}")

    # === VERTICES (Operators) ===
    vertices = job_data.get('vertices', [])
    if vertices:
        output_lines.append("\n" + "=" * 60)
        output_lines.append(f"VERTICES ({len(vertices)} operators)")
        output_lines.append("=" * 60)

        for idx, vertex in enumerate(vertices, 1):
            output_lines.append(f"\n[{idx}] {vertex.get('name', 'Unknown')}")
            output_lines.append(f"    ID: {vertex.get('id', 'N/A')}")
            output_lines.append(f"    Status: {vertex.get('status', 'N/A')}")
            output_lines.append(f"    Parallelism: {vertex.get('parallelism', 'N/A')} (max: {vertex.get('maxParallelism', 'N/A')})")
            output_lines.append(f"    Duration: {format_duration(vertex.get('duration', 0))}")

            # Task status breakdown
            tasks = vertex.get('tasks', {})
            if tasks:
                task_summary = ", ".join([f"{state}: {count}" for state, count in tasks.items() if count > 0])
                if task_summary:
                    output_lines.append(f"    Tasks: {task_summary}")

            # Metrics
            metrics = vertex.get('metrics', {})
            if metrics:
                output_lines.append("    Metrics:")

                # I/O metrics
                read_records = metrics.get('read-records', 0)
                write_records = metrics.get('write-records', 0)
                read_bytes = metrics.get('read-bytes', 0)
                write_bytes = metrics.get('write-bytes', 0)

                if read_records > 0 or read_bytes > 0:
                    output_lines.append(f"      Read: {read_records:,} records, {format_bytes(read_bytes)}")
                if write_records > 0 or write_bytes > 0:
                    output_lines.append(f"      Write: {write_records:,} records, {format_bytes(write_bytes)}")

                # Performance metrics
                backpressured = metrics.get('accumulated-backpressured-time', 0)
                idle_time = metrics.get('accumulated-idle-time', 0)
                busy_time = metrics.get('accumulated-busy-time', 'NaN')

                if backpressured > 0:
                    output_lines.append(f"      ⚠️  Backpressured Time: {format_duration(backpressured)}")
                if idle_time > 0:
                    output_lines.append(f"      Idle Time: {format_duration(idle_time)}")
                if busy_time != 'NaN' and str(busy_time) != 'NaN':
                    output_lines.append(f"      Busy Time: {format_duration(busy_time)}")

                # Throughput calculation
                vertex_duration = vertex.get('duration', 0)
                if vertex_duration > 0 and write_records > 0:
                    throughput = (write_records / vertex_duration) * 1000  # records per second
                    output_lines.append(f"      Throughput: {throughput:,.2f} records/sec")

    # === STATUS SUMMARY ===
    status_counts = job_data.get('status-counts', {})
    if status_counts and any(count > 0 for count in status_counts.values()):
        output_lines.append("\n" + "=" * 60)
        output_lines.append("OVERALL TASK STATUS")
        output_lines.append("=" * 60)
        for status, count in status_counts.items():
            if count > 0:
                output_lines.append(f"{status}: {count}")

    # === EXECUTION PLAN ===
    plan = job_data.get('plan', {})
    if plan and plan.get('nodes'):
        output_lines.append("\n" + "=" * 60)
        output_lines.append("EXECUTION PLAN")
        output_lines.append("=" * 60)

        nodes = plan.get('nodes', [])
        output_lines.append(f"Total Nodes: {len(nodes)}\n")

        for idx, node in enumerate(nodes, 1):
            output_lines.append(f"[Node {idx}] ID: {node.get('id', 'N/A')}")
            output_lines.append(f"  Parallelism: {node.get('parallelism', 'N/A')}")

            # Clean up description (remove HTML tags)
            description = node.get('description', '')
            if description:
                clean_desc = description.replace('<br/>', '\n  ').replace('<', '').replace('>', '')
                output_lines.append(f"  Description:\n  {clean_desc}")

            # Inputs
            inputs = node.get('inputs', [])
            if inputs:
                output_lines.append("  Inputs:")
                for inp in inputs:
                    output_lines.append(f"    - From Node: {inp.get('id', 'N/A')}")
                    output_lines.append(f"      Strategy: {inp.get('ship_strategy', 'N/A')}")
                    output_lines.append(f"      Exchange: {inp.get('exchange', 'N/A')}")
            output_lines.append("")

    # === PERFORMANCE INSIGHTS ===
    output_lines.append("=" * 60)
    output_lines.append("PERFORMANCE INSIGHTS")
    output_lines.append("=" * 60)

    insights = []

    # Check for backpressure
    for vertex in vertices:
        metrics = vertex.get('metrics', {})
        backpressure = metrics.get('accumulated-backpressured-time', 0)
        if backpressure > 0:
            vertex_name = vertex.get('name', 'Unknown')
            insights.append(f"⚠️  BACKPRESSURE detected in '{vertex_name}': {format_duration(backpressure)}")

    # Check for idle operators
    for vertex in vertices:
        metrics = vertex.get('metrics', {})
        idle = metrics.get('accumulated-idle-time', 0)
        duration = vertex.get('duration', 1)
        if duration > 0 and idle > 0:
            idle_percent = (idle / duration) * 100
            if idle_percent > 50:
                vertex_name = vertex.get('name', 'Unknown')
                insights.append(f"ℹ️  High idle time in '{vertex_name}': {idle_percent:.1f}%")

    # Check for failed tasks
    for vertex in vertices:
        tasks = vertex.get('tasks', {})
        failed = tasks.get('FAILED', 0)
        if failed > 0:
            vertex_name = vertex.get('name', 'Unknown')
            insights.append(f"❌ Failed tasks in '{vertex_name}': {failed}")

    # Check parallelism vs available slots
    total_parallelism = sum(v.get('parallelism', 0) for v in vertices)
    if total_parallelism > 0:
        insights.append(f"📊 Total parallelism across all operators: {total_parallelism}")

    if insights:
        for insight in insights:
            output_lines.append(insight)
    else:
        output_lines.append("✅ No performance issues detected")

    output_lines.append("=" * 60)

    return "\n".join(output_lines)


@mcp.tool()
async def get_job_metrics(job_id: str) -> str:
    """Fetch selected useful metrics for a running Flink job; produce a diagnostic summary."""
    base_url = f"{get_settings()['url'].rstrip('/')}/jobs/{job_id}/metrics"

    # Curated set for analysis & tuning (kept small for clarity; extend as needed)
    common = [
        # lifecycle/time
        "uptime",
        "runningTime",
        "downtime",
        "initializingTime",
        "deployingTime",
        "restartingTime",
        "failingTime",
        "cancellingTime",

        # restarts & stability
        "numRestarts",
        "fullRestarts",

        # checkpoints overview
        "totalNumberOfCheckpoints",
        "numberOfCompletedCheckpoints",
        "numberOfFailedCheckpoints",
        "numberOfInProgressCheckpoints",

        # last checkpoint details
        "lastCompletedCheckpointId",
        "lastCheckpointDuration",
        "lastCheckpointSize",            # logical size (may be null)
        "lastCheckpointFullSize",        # physical size including overhead
        "lastCheckpointPersistedData",
        "lastCheckpointProcessedData",
        "lastCheckpointRestoreTimestamp",
        "lastCheckpointExternalPath",

        # creation time
        "createdTime",
    ]

    # Fetch with batching; fallback to per-metric if server rejects long GETs
    async def fetch_values(client: httpx.AsyncClient, selected: List[str]) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        try:
            for batch in chunk(selected, 50):
                resp = await client.get(base_url, params={"get": ",".join(batch)})
                resp.raise_for_status()
                results.extend(resp.json())
            return results
        except httpx.HTTPStatusError:
            results.clear()
            for m in selected:
                resp = await client.get(base_url, params={"get": m})
                resp.raise_for_status()
                results.extend(resp.json())
            return results

    try:
        async with httpx.AsyncClient(verify=False, timeout=httpx.Timeout(10.0)) as client:
            # 1) discover available metric ids
            r = await client.get(base_url)
            r.raise_for_status()
            metric_ids = [m["id"] for m in r.json() if "id" in m]

            selected = [m for m in common if m in metric_ids]
            if not selected:
                return "No common metrics available for this job."

            # 2) fetch values
            values = await fetch_values(client, selected)
            by_id = index_by_id(values)

        # 3) pull typed values
        uptime_ms                     = to_num(by_id.get("uptime", {}).get("value"))
        running_ms                    = to_num(by_id.get("runningTime", {}).get("value"))
        downtime_ms                   = to_num(by_id.get("downtime", {}).get("value"))
        initializing_ms               = to_num(by_id.get("initializingTime", {}).get("value"))
        deploying_ms                  = to_num(by_id.get("deployingTime", {}).get("value"))
        restarting_ms                 = to_num(by_id.get("restartingTime", {}).get("value"))
        failing_ms                    = to_num(by_id.get("failingTime", {}).get("value"))
        cancelling_ms                 = to_num(by_id.get("cancellingTime", {}).get("value"))

        num_restarts                  = to_num(by_id.get("numRestarts", {}).get("value")) or 0
        full_restarts                 = to_num(by_id.get("fullRestarts", {}).get("value")) or 0

        total_ckpt                    = to_num(by_id.get("totalNumberOfCheckpoints", {}).get("value")) or 0
        completed_ckpt                = to_num(by_id.get("numberOfCompletedCheckpoints", {}).get("value")) or 0
        failed_ckpt                   = to_num(by_id.get("numberOfFailedCheckpoints", {}).get("value")) or 0
        inprog_ckpt                   = to_num(by_id.get("numberOfInProgressCheckpoints", {}).get("value")) or 0

        last_ckpt_id                  = by_id.get("lastCompletedCheckpointId", {}).get("value")
        last_ckpt_duration_ms         = to_num(by_id.get("lastCheckpointDuration", {}).get("value"))
        last_ckpt_size_bytes          = to_num(by_id.get("lastCheckpointSize", {}).get("value"))
        last_ckpt_full_size_bytes     = to_num(by_id.get("lastCheckpointFullSize", {}).get("value"))
        last_ckpt_persisted_bytes     = to_num(by_id.get("lastCheckpointPersistedData", {}).get("value"))
        last_ckpt_processed_bytes     = to_num(by_id.get("lastCheckpointProcessedData", {}).get("value"))
        last_ckpt_restore_ts          = to_num(by_id.get("lastCheckpointRestoreTimestamp", {}).get("value"))
        last_ckpt_external_path       = by_id.get("lastCheckpointExternalPath", {}).get("value")

        created_time_ms               = to_num(by_id.get("createdTime", {}).get("value"))

        # 4) derived indicators
        success_ratio = (completed_ckpt / total_ckpt) if total_ckpt > 0 else None
        failure_ratio = (failed_ckpt / total_ckpt) if total_ckpt > 0 else None

        # 5) build diagnostic report
        lines: List[str] = []
        lines.append("=== Job Runtime ===")
        lines.append(f"Uptime:        {format_duration(uptime_ms) if uptime_ms is not None else 'N/A'}")
        lines.append(f"Running:       {format_duration(running_ms) if running_ms is not None else 'N/A'}")
        lines.append(f"Downtime:      {format_duration(downtime_ms) if downtime_ms is not None else 'N/A'}")
        lines.append(f"Init:          {format_duration(initializing_ms) if initializing_ms is not None else 'N/A'}")
        lines.append(f"Deploying:     {format_duration(deploying_ms) if deploying_ms is not None else 'N/A'}")
        lines.append(f"Restarting:    {format_duration(restarting_ms) if restarting_ms is not None else 'N/A'}")
        lines.append(f"Failing:       {format_duration(failing_ms) if failing_ms is not None else 'N/A'}")
        lines.append(f"Cancelling:    {format_duration(cancelling_ms) if cancelling_ms is not None else 'N/A'}")
        lines.append(f"Created:       {format_timestamp(created_time_ms) if created_time_ms is not None else 'N/A'}")
        lines.append("")

        lines.append("=== Stability ===")
        lines.append(f"Restarts:      {int(num_restarts)} (full: {int(full_restarts)})")
        lines.append("")

        lines.append("=== Checkpoints ===")
        lines.append(f"Total:         {int(total_ckpt)}  | Completed: {int(completed_ckpt)}  | Failed: {int(failed_ckpt)}  | In-Progress: {int(inprog_ckpt)}")
        lines.append(f"Success Ratio: {f'{success_ratio:.2%}' if success_ratio is not None else 'N/A'}"
                     f"  | Failure Ratio: {f'{failure_ratio:.2%}' if failure_ratio is not None else 'N/A'}")
        lines.append(f"Last Completed ID: {last_ckpt_id if last_ckpt_id not in (None, '') else 'N/A'}")
        lines.append(f"Last Duration:     {format_duration(last_ckpt_duration_ms) if last_ckpt_duration_ms is not None else 'N/A'}")
        lines.append(f"Last Size (logical): {format_bytes(last_ckpt_size_bytes) if last_ckpt_size_bytes is not None else 'N/A'}")
        lines.append(f"Last Size (full):    {format_bytes(last_ckpt_full_size_bytes) if last_ckpt_full_size_bytes is not None else 'N/A'}")
        lines.append(f"Last Persisted:      {format_bytes(last_ckpt_persisted_bytes) if last_ckpt_persisted_bytes is not None else 'N/A'}")
        lines.append(f"Last Processed:      {format_bytes(last_ckpt_processed_bytes) if last_ckpt_processed_bytes is not None else 'N/A'}")
        lines.append(f"Last Restored At:    {format_timestamp(last_ckpt_restore_ts) if last_ckpt_restore_ts is not None else 'N/A'}")
        lines.append(f"External Path:       {last_ckpt_external_path if last_ckpt_external_path else 'N/A'}")
        lines.append("")

        # 6) hints (light heuristics)
        hints: List[str] = []
        # High uptime but no checkpoints
        if (uptime_ms or 0) > 10 * 60 * 1000 and completed_ckpt == 0:
            hints.append("High uptime with 0 completed checkpoints → verify checkpointing is enabled and backend/storage is reachable.")
        # Failures dominating
        if total_ckpt >= 5 and failure_ratio is not None and failure_ratio > 0.3:
            hints.append("Checkpoint failure ratio > 30% → investigate operator backpressure, I/O sinks, or timeout settings.")
        # Restarts
        if num_restarts >= 3:
            hints.append("Multiple restarts observed → review TaskManager/JobManager logs for exceptions or OOMs.")
        # Long durations
        if last_ckpt_duration_ms and last_ckpt_duration_ms > 60_000:
            hints.append("Last checkpoint duration > 60s → consider tuning state.backend, increasing I/O throughput, or lowering concurrency on heavy operators.")
        # No external path (when expected)
        if (completed_ckpt > 0) and not last_ckpt_external_path:
            hints.append("Completed checkpoints without externalized path → check externalization settings if you expect retained checkpoints.")

        if hints:
            lines.append("=== Hints ===")
            lines.extend(f"- {h}" for h in hints)

        return "\n".join(lines)

    except Exception as e:
        logger.error(f"Failed to get job metrics: {e}")
        return f"Error: {str(e)}"


@mcp.tool()
async def get_job_exceptions(job_id: str) -> str:
    """Fetch exceptions that occurred in the specified job."""
    url = f"{get_settings()['url']}/jobs/{job_id}/exceptions"
    try:
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()

        # Check if there are any exceptions
        all_exceptions = data.get("all-exceptions", [])
        root_exception = data.get("root-exception", "")
        exception_history = data.get("exceptionHistory", {}).get("entries", [])

        if not all_exceptions and not root_exception:
            return "No exceptions found for this job."

        result = ["=" * 80, "JOB EXCEPTIONS REPORT", "=" * 80, ""]

        # Root exception (main failure reason)
        if root_exception:
            result.append("ROOT CAUSE:")
            result.append("-" * 80)
            result.append(root_exception)
            result.append("")

        # All exceptions with details
        if all_exceptions:
            result.append(f"EXCEPTION DETAILS ({len(all_exceptions)} exception(s)):")
            result.append("-" * 80)
            for idx, exc in enumerate(all_exceptions, 1):
                result.append(f"\n[Exception #{idx}]")
                result.append(f"Task: {exc.get('task', 'N/A')}")
                result.append(f"TaskManager ID: {exc.get('taskManagerId', 'N/A')}")
                result.append(f"Location: {exc.get('location', 'N/A')}")
                result.append(f"Endpoint: {exc.get('endpoint', 'N/A')}")
                result.append(f"Timestamp: {exc.get('timestamp', 'N/A')}")
                result.append(f"\nStack Trace:")
                result.append(exc.get('exception', 'No stack trace available'))
                result.append("-" * 80)

        # Exception history (if available)
        if exception_history:
            result.append(f"\nEXCEPTION HISTORY ({len(exception_history)} entry/entries):")
            result.append("-" * 80)
            for idx, entry in enumerate(exception_history, 1):
                result.append(f"\n[History Entry #{idx}]")
                result.append(f"Exception Type: {entry.get('exceptionName', 'N/A')}")
                result.append(f"Timestamp: {entry.get('timestamp', 'N/A')}")

                # Concurrent exceptions
                concurrent = entry.get('concurrentExceptions', [])
                if concurrent:
                    result.append(f"\nConcurrent Exceptions ({len(concurrent)}):")
                    for cidx, cexc in enumerate(concurrent, 1):
                        result.append(f"  [{cidx}] {cexc.get('exceptionName', 'N/A')}")
                        result.append(f"      Task: {cexc.get('taskName', 'N/A')}")
                        result.append(f"      Location: {cexc.get('location', 'N/A')}")
                        result.append(f"      TaskManager: {cexc.get('taskManagerId', 'N/A')}")

                result.append("-" * 80)

        # Summary
        result.append("\nSUMMARY:")
        result.append(f"- Total Exceptions: {len(all_exceptions)}")
        result.append(f"- History Entries: {len(exception_history)}")
        result.append(f"- Truncated: {data.get('truncated', False)}")
        result.append("=" * 80)

        return "\n".join(result)

    except Exception as e:
        logger.error(f"Failed to get job exceptions: {e}")
        return f"Error: {str(e)}"


@mcp.tool()
async def get_job_accumulators(job_id: str) -> str:
    """Get user-defined accumulators for a Flink job.

    Args:
        job_id: The Flink job ID.

    Returns each accumulator's name, type, and value.
    """
    url = f"{get_settings()['url']}/jobs/{job_id}/accumulators"
    try:
        async with httpx.AsyncClient(verify=False, timeout=10.0) as client:
            response = await client.get(url)
            if response.status_code == 404:
                return f"❌ Job not found: {job_id}"
            response.raise_for_status()
            data = response.json()

        # Flink may return a plain list or a dict depending on the version
        if isinstance(data, list):
            accumulators = data
        else:
            user_accumulators = data.get("job-accumulators", [])
            raw = data.get("user-task-accumulators", [])
            # In modern Flink, user-task-accumulators is directly a list.
            # In older versions it may be a dict with a nested key.
            if isinstance(raw, dict):
                serialized = raw.get("accumulated-user-accumulators", [])
            else:
                serialized = raw
            accumulators = user_accumulators or serialized

        if not accumulators:
            return f"No user-defined accumulators found for job {job_id}."

        lines = []
        lines.append("=" * 70)
        lines.append(f"JOB ACCUMULATORS — {job_id}")
        lines.append("=" * 70)
        lines.append(f"\nTotal accumulators: {len(accumulators)}\n")
        for acc in accumulators:
            lines.append(f"  Name:  {acc.get('name', 'N/A')}")
            lines.append(f"  Type:  {acc.get('type', 'N/A')}")
            lines.append(f"  Value: {acc.get('value', 'N/A')}")
            lines.append("")
        lines.append("=" * 70)
        return "\n".join(lines)

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return f"❌ Job not found: {job_id}"
        logger.error(f"HTTP error fetching job accumulators: {e}")
        return f"❌ HTTP Error ({e.response.status_code}): {str(e)}"
    except httpx.TimeoutException:
        logger.error("Timeout fetching job accumulators")
        return "❌ Request timeout while fetching job accumulators"
    except Exception as e:
        logger.error(f"Failed to get job accumulators: {e}")
        return f"❌ Error getting job accumulators: {str(e)}"


@mcp.tool()
async def get_job_plan(job_id: str) -> str:
    """
    Return the dataflow plan (DAG) for a job: node IDs, descriptions,
    parallelism, and input edges with their ship strategies.

    Args:
        job_id: The Flink job ID.
    """
    url = f"{get_settings()['url']}/jobs/{job_id}/plan"
    try:
        async with httpx.AsyncClient(verify=False, timeout=10.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()

        plan = data.get("plan", data)
        nodes = plan.get("nodes", [])
        if not nodes:
            return f"❌ No plan nodes returned for job {job_id}."

        # Build an index so we can resolve input node names
        node_index = {n.get("id"): n for n in nodes}

        lines = [
            f"Dataflow Plan — Job {job_id}",
            "=" * 70,
            f"Nodes: {len(nodes)}",
            "",
        ]

        for node in nodes:
            nid = node.get("id", "N/A")
            parallelism = node.get("parallelism", "N/A")
            description = node.get("description", "")
            # Strip HTML tags for readability
            clean_desc = (
                description
                .replace("<br/>", " | ")
                .replace("<b>", "").replace("</b>", "")
                .replace("<", "").replace(">", "")
            )
            inputs = node.get("inputs", [])

            lines.append(f"[Node {nid}]  parallelism={parallelism}")
            lines.append(f"  {clean_desc}")

            if inputs:
                lines.append("  Inputs:")
                for inp in inputs:
                    src_id = inp.get("id", "?")
                    src_node = node_index.get(src_id, {})
                    src_desc = (
                        src_node.get("description", src_id)
                        .replace("<br/>", " | ")
                        .replace("<b>", "").replace("</b>", "")
                        .replace("<", "").replace(">", "")
                    )
                    ship = inp.get("ship_strategy", "N/A")
                    exchange = inp.get("exchange", "N/A")
                    lines.append(f"    ← [{src_id}] {src_desc}")
                    lines.append(f"       ship_strategy={ship}  exchange={exchange}")
            else:
                lines.append("  (source — no inputs)")
            lines.append("")

        lines.append("=" * 70)
        return "\n".join(lines)

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return f"❌ Job not found: {job_id}"
        return f"❌ HTTP Error ({e.response.status_code}): {str(e)}"
    except httpx.TimeoutException:
        return f"❌ Request timeout fetching plan for job {job_id}."
    except Exception as e:
        logger.error(f"Failed to get job plan for {job_id}: {e}")
        return f"❌ Error fetching job plan: {str(e)}"


@mcp.tool()
async def get_job_checkpoint_config(job_id: str) -> str:
    """
    Return the checkpoint configuration active for a specific job:
    mode, interval, timeout, minimum pause, max concurrent checkpoints,
    and externalized checkpoint settings.

    Args:
        job_id: The Flink job ID.
    """
    url = f"{get_settings()['url']}/jobs/{job_id}/checkpoints/config"
    try:
        async with httpx.AsyncClient(verify=False, timeout=10.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            cfg = response.json()

        mode = cfg.get("mode", "N/A")
        interval = cfg.get("interval", -1)
        timeout = cfg.get("timeout", -1)
        min_pause = cfg.get("min_pause", -1)
        max_concurrent = cfg.get("max_concurrent", "N/A")

        ext = cfg.get("externalization", {})
        ext_enabled = ext.get("enabled", False)
        ext_cleanup = ext.get("delete_on_cancellation", None)

        lines = [
            f"Checkpoint Configuration — Job {job_id}",
            "=" * 60,
            f"Mode:                     {mode}",
            f"Interval:                 {format_duration(interval) if interval >= 0 else 'N/A'}",
            f"Timeout:                  {format_duration(timeout) if timeout >= 0 else 'N/A'}",
            f"Min pause between:        {format_duration(min_pause) if min_pause >= 0 else 'N/A'}",
            f"Max concurrent:           {max_concurrent}",
            "",
            "Externalized Checkpoints:",
            f"  Enabled:                {ext_enabled}",
        ]
        if ext_enabled:
            cleanup_label = "delete on cancellation" if ext_cleanup else "retain on cancellation"
            lines.append(f"  Cleanup mode:           {cleanup_label}")
        lines.append("=" * 60)

        return "\n".join(lines)

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return f"❌ Job not found or checkpointing not configured: {job_id}"
        return f"❌ HTTP Error ({e.response.status_code}): {str(e)}"
    except httpx.TimeoutException:
        return f"❌ Request timeout fetching checkpoint config for job {job_id}."
    except Exception as e:
        logger.error(f"Failed to get checkpoint config for {job_id}: {e}")
        return f"❌ Error fetching checkpoint config: {str(e)}"
