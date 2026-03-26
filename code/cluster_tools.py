import logging

import httpx

from .app import mcp
from .connection import get_settings, MAX_OUTPUT_CHARS
from .utils import format_bytes

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
            f"- Jobs Failed: {data.get('jobs-failed')}"
        )
    except Exception as e:
        logger.error(f"Failed to fetch cluster info: {e}")
        return f"Error fetching cluster info: {str(e)}"


@mcp.tool()
async def get_cluster_config() -> str:
    """
    Return the REST/web server configuration from GET /config.
    This covers the Flink version, web UI refresh interval, timezone, and
    related web-layer settings. This is distinct from get_jobmanager_config,
    which returns the full effective cluster/JobManager configuration.
    """
    url = f"{get_settings()['url']}/config"
    try:
        async with httpx.AsyncClient(verify=False, timeout=10.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()

        lines = [
            "Web UI / REST Server Configuration",
            "=" * 60,
            f"Flink Version:    {data.get('flink-version', 'N/A')}",
            f"Flink Revision:   {data.get('flink-revision', 'N/A')}",
            f"Refresh Interval: {data.get('refresh-interval', 'N/A')} ms",
            f"Timezone:         {data.get('timezone-name', 'N/A')} "
            f"(offset {data.get('timezone-offset', 'N/A')} ms)",
            f"Features:",
            f"  Web Submit:     {data.get('features', {}).get('web-submit', 'N/A')}",
            f"  Web Cancel:     {data.get('features', {}).get('web-cancel', 'N/A')}",
            "",
            "All fields returned by /config:",
            "-" * 60,
        ]
        for k, v in data.items():
            if k != "features":
                lines.append(f"  {k}: {v}")
        features = data.get("features", {})
        if features:
            lines.append("  features:")
            for k, v in features.items():
                lines.append(f"    {k}: {v}")
        lines.append("=" * 60)
        return "\n".join(lines)

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return "❌ /config endpoint not found on this Flink cluster."
        return f"❌ HTTP Error ({e.response.status_code}): {str(e)}"
    except httpx.TimeoutException:
        return "❌ Request timeout fetching cluster config."
    except Exception as e:
        logger.error(f"Failed to get cluster config: {e}")
        return f"❌ Error fetching cluster config: {str(e)}"


@mcp.tool()
async def list_datasets() -> str:
    """
    List intermediate batch datasets available on the cluster.
    Each entry shows the dataset ID, producing job ID, and size.
    Returns a clear message when no datasets exist, which is normal
    for streaming-only clusters.
    """
    url = f"{get_settings()['url']}/datasets"
    try:
        async with httpx.AsyncClient(verify=False, timeout=10.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()

        datasets = data.get("dataSets", data.get("datasets", []))
        if not datasets:
            return (
                "No intermediate datasets are currently available.\n"
                "This is expected for streaming-only clusters. Intermediate datasets\n"
                "are only produced by batch jobs that use blocking result partitions."
            )

        lines = [
            "Intermediate Batch Datasets",
            "=" * 70,
            f"{'Dataset ID':<40} {'Producing Job':<36} {'Size':>10}",
            "-" * 70,
        ]
        for ds in datasets:
            ds_id = ds.get("id", ds.get("dataSetId", "N/A"))
            job_id = ds.get("producingJobId", ds.get("job-id", "N/A"))
            size = ds.get("totalSize", ds.get("size", -1))
            size_str = format_bytes(size) if size >= 0 else "N/A"
            lines.append(f"{ds_id:<40} {job_id:<36} {size_str:>10}")
        lines.append("=" * 70)
        lines.append(f"Total: {len(datasets)} dataset(s)")
        return "\n".join(lines)

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return "❌ /datasets endpoint not found. This may not be supported by your Flink version."
        return f"❌ HTTP Error ({e.response.status_code}): {str(e)}"
    except httpx.TimeoutException:
        return "❌ Request timeout fetching datasets."
    except Exception as e:
        logger.error(f"Failed to list datasets: {e}")
        return f"❌ Error listing datasets: {str(e)}"


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
