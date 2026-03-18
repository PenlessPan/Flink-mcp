import logging
from typing import Optional, List, Dict, Union
import json

import sys
import os
import argparse
from fastmcp import FastMCP
import httpx
from typing import Dict, List, Any
import yaml
from dataclasses import dataclass


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("flink-mcp-server")


mcp = FastMCP("Apache Flink MCP Server")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass
class Settings:
    flink_url: str
    tls_verify: bool
    max_output_chars: int


with open("config.yaml") as _f:
    _cfg = yaml.safe_load(_f)

settings = Settings(
    flink_url=_cfg["flink"]["url"].rstrip("/"),
    tls_verify=_cfg["flink"]["tls_verify"],
    max_output_chars=_cfg["server"]["max_output_chars"],
)


@mcp.tool()
async def get_cluster_info() -> str:
    """Fetch an overview of the Flink cluster: jobs, slots, taskmanagers."""
    url = f"{settings.flink_url}/overview"
    try:
        async with httpx.AsyncClient(verify=settings.tls_verify) as client:
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
async def list_jobs() -> str:
    """List all current and recent Flink jobs with their status."""
    url = f"{settings.flink_url}/jobs/overview"
    try:
        async with httpx.AsyncClient(verify=settings.tls_verify) as client:
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
async def get_job_details(job_id: str) -> str:
    """Get comprehensive details of a specific Flink job by job ID including configuration, 
    vertices, metrics, and execution plan."""
    # Fetch both job details and config in parallel
    details_url = f"{settings.flink_url}/jobs/{job_id}"
    config_url = f"{settings.flink_url}/jobs/{job_id}/config"
    
    job_data = None
    config_data = None
    
    try:
        async with httpx.AsyncClient(verify=settings.tls_verify, timeout=10.0) as client:
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
    
    output_lines.append(f"\nStart Time: {start_time} ({_format_timestamp(start_time)})")
    if end_time > 0:
        output_lines.append(f"End Time: {end_time} ({_format_timestamp(end_time)})")
    output_lines.append(f"Duration: {_format_duration(duration)}")
    
    # Timestamps
    timestamps = job_data.get('timestamps', {})
    if timestamps:
        output_lines.append("\nState Transitions:")
        for state, ts in timestamps.items():
            if ts > 0:
                output_lines.append(f"  - {state}: {_format_timestamp(ts)}")
    
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
            output_lines.append(f"    Duration: {_format_duration(vertex.get('duration', 0))}")
            
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
                    output_lines.append(f"      Read: {read_records:,} records, {_format_bytes(read_bytes)}")
                if write_records > 0 or write_bytes > 0:
                    output_lines.append(f"      Write: {write_records:,} records, {_format_bytes(write_bytes)}")
                
                # Performance metrics
                backpressured = metrics.get('accumulated-backpressured-time', 0)
                idle_time = metrics.get('accumulated-idle-time', 0)
                busy_time = metrics.get('accumulated-busy-time', 'NaN')
                
                if backpressured > 0:
                    output_lines.append(f"      ⚠️  Backpressured Time: {_format_duration(backpressured)}")
                if idle_time > 0:
                    output_lines.append(f"      Idle Time: {_format_duration(idle_time)}")
                if busy_time != 'NaN' and str(busy_time) != 'NaN':
                    output_lines.append(f"      Busy Time: {_format_duration(busy_time)}")
                
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
            insights.append(f"⚠️  BACKPRESSURE detected in '{vertex_name}': {_format_duration(backpressure)}")
    
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


# Helper functions
def _format_timestamp(ts: int) -> str:
    """Format Unix timestamp in milliseconds to readable string."""
    if ts <= 0:
        return "N/A"
    try:
        from datetime import datetime
        dt = datetime.fromtimestamp(ts / 1000.0)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return f"{ts}ms"


def _format_duration(duration_ms) -> str:
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


def _format_bytes(bytes_val) -> str:
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


def _to_num(v):
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

def _index_by_id(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {it.get("id"): it for it in items if isinstance(it, dict) and "id" in it}

def _chunk(seq, n):
    buf = []
    for x in seq:
        buf.append(x)
        if len(buf) == n:
            yield buf
            buf = []
    if buf:
        yield buf
        
        
        
        

@mcp.tool()
async def list_taskmanagers() -> str:
    """List all registered TaskManagers in the Flink cluster with detailed resource information."""
    url = f"{settings.flink_url}/taskmanagers"
    try:
        async with httpx.AsyncClient(verify=settings.tls_verify, timeout=10.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            tms = response.json().get("taskmanagers", [])
        
        if not tms:
            return "⚠️  No TaskManagers registered in the cluster."
        
        result = []
        result.append("=" * 70)
        result.append(f"TASKMANAGERS OVERVIEW ({len(tms)} TaskManager(s))")
        result.append("=" * 70)
        
        # Cluster-wide summary
        total_slots = sum(tm.get('slotsNumber', 0) for tm in tms)
        total_free_slots = sum(tm.get('freeSlots', 0) for tm in tms)
        total_used_slots = total_slots - total_free_slots
        
        result.append(f"\nCluster Capacity:")
        result.append(f"  Total Slots: {total_slots}")
        result.append(f"  Used Slots: {total_used_slots} ({(total_used_slots/total_slots*100) if total_slots > 0 else 0:.1f}%)")
        result.append(f"  Free Slots: {total_free_slots} ({(total_free_slots/total_slots*100) if total_slots > 0 else 0:.1f}%)")
        
        # Detail each TaskManager
        for idx, tm in enumerate(tms, 1):
            result.append("\n" + "=" * 70)
            result.append(f"TaskManager #{idx}")
            result.append("=" * 70)
            
            # Basic Info
            result.append(f"\n📋 BASIC INFO")
            result.append(f"  ID: {tm.get('id', 'N/A')}")
            result.append(f"  Path: {tm.get('path', 'N/A')}")
            result.append(f"  Data Port: {tm.get('dataPort', 'N/A')}")
            result.append(f"  JMX Port: {tm.get('jmxPort', 'Disabled' if tm.get('jmxPort', -1) == -1 else tm.get('jmxPort'))}")
            
            # Heartbeat
            heartbeat = tm.get('timeSinceLastHeartbeat', 0)
            if heartbeat > 0:
                from datetime import datetime
                last_heartbeat = datetime.fromtimestamp(heartbeat / 1000.0)
                result.append(f"  Last Heartbeat: {last_heartbeat.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Slot Info
            slots_total = tm.get('slotsNumber', 0)
            slots_free = tm.get('freeSlots', 0)
            slots_used = slots_total - slots_free
            
            result.append(f"\n🎰 SLOT ALLOCATION")
            result.append(f"  Total Slots: {slots_total}")
            result.append(f"  Used Slots: {slots_used} ({(slots_used/slots_total*100) if slots_total > 0 else 0:.1f}%)")
            result.append(f"  Free Slots: {slots_free} ({(slots_free/slots_total*100) if slots_total > 0 else 0:.1f}%)")
            
            if slots_free == 0 and slots_total > 0:
                result.append(f"  ⚠️  WARNING: No free slots available!")
            
            # Hardware Resources
            hardware = tm.get('hardware', {})
            if hardware:
                result.append(f"\n💻 HARDWARE")
                result.append(f"  CPU Cores: {hardware.get('cpuCores', 'N/A')}")
                
                phys_mem = hardware.get('physicalMemory', 0)
                free_mem = hardware.get('freeMemory', 0)
                managed_mem = hardware.get('managedMemory', 0)
                
                result.append(f"  Physical Memory: {_format_bytes(phys_mem)}")
                result.append(f"  Free Memory: {_format_bytes(free_mem)} ({(free_mem/phys_mem*100) if phys_mem > 0 else 0:.1f}%)")
                result.append(f"  Managed Memory: {_format_bytes(managed_mem)}")
                
                if phys_mem > 0 and free_mem > 0:
                    used_mem = phys_mem - free_mem
                    result.append(f"  Used Memory: {_format_bytes(used_mem)} ({(used_mem/phys_mem*100):.1f}%)")
            
            # Total Resources (configured)
            total_res = tm.get('totalResource', {})
            if total_res:
                result.append(f"\n📊 CONFIGURED RESOURCES")
                result.append(f"  CPU Cores (slots): {total_res.get('cpuCores', 'N/A')}")
                result.append(f"  Task Heap Memory: {_format_bytes(total_res.get('taskHeapMemory', 0) * 1024 * 1024)}")
                result.append(f"  Task Off-Heap Memory: {_format_bytes(total_res.get('taskOffHeapMemory', 0) * 1024 * 1024)}")
                result.append(f"  Managed Memory: {_format_bytes(total_res.get('managedMemory', 0) * 1024 * 1024)}")
                result.append(f"  Network Memory: {_format_bytes(total_res.get('networkMemory', 0) * 1024 * 1024)}")
                
                extended = total_res.get('extendedResources', {})
                if extended:
                    result.append(f"  Extended Resources: {extended}")
            
            # Free Resources (available)
            free_res = tm.get('freeResource', {})
            if free_res:
                result.append(f"\n✅ AVAILABLE RESOURCES")
                result.append(f"  CPU Cores: {free_res.get('cpuCores', 0)}")
                result.append(f"  Task Heap Memory: {_format_bytes(free_res.get('taskHeapMemory', 0) * 1024 * 1024)}")
                result.append(f"  Task Off-Heap Memory: {_format_bytes(free_res.get('taskOffHeapMemory', 0) * 1024 * 1024)}")
                result.append(f"  Managed Memory: {_format_bytes(free_res.get('managedMemory', 0) * 1024 * 1024)}")
                result.append(f"  Network Memory: {_format_bytes(free_res.get('networkMemory', 0) * 1024 * 1024)}")
            
            # Memory Configuration Details
            mem_config = tm.get('memoryConfiguration', {})
            if mem_config:
                result.append(f"\n🧠 MEMORY CONFIGURATION")
                result.append(f"  Framework Heap: {_format_bytes(mem_config.get('frameworkHeap', 0))}")
                result.append(f"  Framework Off-Heap: {_format_bytes(mem_config.get('frameworkOffHeap', 0))}")
                result.append(f"  Task Heap: {_format_bytes(mem_config.get('taskHeap', 0))}")
                result.append(f"  Task Off-Heap: {_format_bytes(mem_config.get('taskOffHeap', 0))}")
                result.append(f"  Network Memory: {_format_bytes(mem_config.get('networkMemory', 0))}")
                result.append(f"  Managed Memory: {_format_bytes(mem_config.get('managedMemory', 0))}")
                result.append(f"  JVM Metaspace: {_format_bytes(mem_config.get('jvmMetaspace', 0))}")
                result.append(f"  JVM Overhead: {_format_bytes(mem_config.get('jvmOverhead', 0))}")
                result.append(f"  Total Flink Memory: {_format_bytes(mem_config.get('totalFlinkMemory', 0))}")
                result.append(f"  Total Process Memory: {_format_bytes(mem_config.get('totalProcessMemory', 0))}")
            
            # Resource Utilization Analysis
            result.append(f"\n📈 UTILIZATION ANALYSIS")
            
            # Slot utilization
            if slots_total > 0:
                slot_util = (slots_used / slots_total) * 100
                if slot_util >= 90:
                    result.append(f"  ⚠️  HIGH slot utilization: {slot_util:.1f}%")
                elif slot_util >= 70:
                    result.append(f"  ⚡ MODERATE slot utilization: {slot_util:.1f}%")
                else:
                    result.append(f"  ✅ LOW slot utilization: {slot_util:.1f}%")
            
            # Memory utilization
            if hardware:
                phys_mem = hardware.get('physicalMemory', 0)
                free_mem = hardware.get('freeMemory', 0)
                if phys_mem > 0:
                    mem_util = ((phys_mem - free_mem) / phys_mem) * 100
                    if mem_util >= 90:
                        result.append(f"  ⚠️  HIGH memory utilization: {mem_util:.1f}%")
                    elif mem_util >= 70:
                        result.append(f"  ⚡ MODERATE memory utilization: {mem_util:.1f}%")
                    else:
                        result.append(f"  ✅ LOW memory utilization: {mem_util:.1f}%")
            
            # CPU vs Slots mismatch warning
            hw_cpus = hardware.get('cpuCores', 0) if hardware else 0
            if hw_cpus > 0 and slots_total > 0:
                if slots_total < hw_cpus:
                    result.append(f"  💡 TIP: You have {hw_cpus} CPU cores but only {slots_total} slot(s). Consider increasing taskmanager.numberOfTaskSlots")
                elif slots_total > hw_cpus:
                    result.append(f"  ⚠️  WARNING: {slots_total} slots configured but only {hw_cpus} CPU cores available. Potential oversubscription!")
        
        result.append("\n" + "=" * 70)
        
        # Overall recommendations
        result.append("\n💡 RECOMMENDATIONS")
        if total_free_slots == 0:
            result.append("  ⚠️  No free slots available. Cannot schedule new jobs.")
            result.append("  → Consider adding more TaskManagers or increasing slots per TaskManager")
        elif total_free_slots < total_slots * 0.2:
            result.append("  ⚡ Low slot availability. Cluster is near capacity.")
        else:
            result.append("  ✅ Sufficient slot capacity available")
        
        result.append("=" * 70)
        
        return "\n".join(result)
        
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error listing TaskManagers: {e}")
        return f"❌ HTTP Error ({e.response.status_code}): {str(e)}"
    except httpx.TimeoutException:
        logger.error("Timeout listing TaskManagers")
        return "❌ Request timeout while listing TaskManagers"
    except Exception as e:
        logger.error(f"Failed to list TaskManagers: {e}")
        return f"❌ Error listing TaskManagers: {str(e)}"



@mcp.tool()
async def get_job_exceptions(job_id: str) -> str:
    """Fetch exceptions that occurred in the specified job."""
    url = f"{settings.flink_url}/jobs/{job_id}/exceptions"
    try:
        async with httpx.AsyncClient(verify=settings.tls_verify) as client:
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
async def list_jar_files() -> str:
    """List all uploaded JARs in the Flink cluster."""
    url = f"{settings.flink_url}/jars"
    try:
        async with httpx.AsyncClient(verify=settings.tls_verify) as client:
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
async def get_job_metrics(job_id: str) -> str:
    """Fetch selected useful metrics for a running Flink job; produce a diagnostic summary."""
    base_url = f"{settings.flink_url.rstrip('/')}/jobs/{job_id}/metrics"

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
            for batch in _chunk(selected, 50):
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
        async with httpx.AsyncClient(verify=settings.tls_verify, timeout=httpx.Timeout(10.0)) as client:
            # 1) discover available metric ids
            r = await client.get(base_url)
            r.raise_for_status()
            metric_ids = [m["id"] for m in r.json() if "id" in m]

            selected = [m for m in common if m in metric_ids]
            if not selected:
                return "No common metrics available for this job."

            # 2) fetch values
            values = await fetch_values(client, selected)
            by_id = _index_by_id(values)

        # 3) pull typed values
        uptime_ms                     = _to_num(by_id.get("uptime", {}).get("value"))
        running_ms                    = _to_num(by_id.get("runningTime", {}).get("value"))
        downtime_ms                   = _to_num(by_id.get("downtime", {}).get("value"))
        initializing_ms               = _to_num(by_id.get("initializingTime", {}).get("value"))
        deploying_ms                  = _to_num(by_id.get("deployingTime", {}).get("value"))
        restarting_ms                 = _to_num(by_id.get("restartingTime", {}).get("value"))
        failing_ms                    = _to_num(by_id.get("failingTime", {}).get("value"))
        cancelling_ms                 = _to_num(by_id.get("cancellingTime", {}).get("value"))

        num_restarts                  = _to_num(by_id.get("numRestarts", {}).get("value")) or 0
        full_restarts                 = _to_num(by_id.get("fullRestarts", {}).get("value")) or 0

        total_ckpt                    = _to_num(by_id.get("totalNumberOfCheckpoints", {}).get("value")) or 0
        completed_ckpt                = _to_num(by_id.get("numberOfCompletedCheckpoints", {}).get("value")) or 0
        failed_ckpt                   = _to_num(by_id.get("numberOfFailedCheckpoints", {}).get("value")) or 0
        inprog_ckpt                   = _to_num(by_id.get("numberOfInProgressCheckpoints", {}).get("value")) or 0

        last_ckpt_id                  = by_id.get("lastCompletedCheckpointId", {}).get("value")
        last_ckpt_duration_ms         = _to_num(by_id.get("lastCheckpointDuration", {}).get("value"))
        last_ckpt_size_bytes          = _to_num(by_id.get("lastCheckpointSize", {}).get("value"))
        last_ckpt_full_size_bytes     = _to_num(by_id.get("lastCheckpointFullSize", {}).get("value"))
        last_ckpt_persisted_bytes     = _to_num(by_id.get("lastCheckpointPersistedData", {}).get("value"))
        last_ckpt_processed_bytes     = _to_num(by_id.get("lastCheckpointProcessedData", {}).get("value"))
        last_ckpt_restore_ts          = _to_num(by_id.get("lastCheckpointRestoreTimestamp", {}).get("value"))
        last_ckpt_external_path       = by_id.get("lastCheckpointExternalPath", {}).get("value")

        created_time_ms               = _to_num(by_id.get("createdTime", {}).get("value"))

        # 4) derived indicators
        success_ratio = (completed_ckpt / total_ckpt) if total_ckpt > 0 else None
        failure_ratio = (failed_ckpt / total_ckpt) if total_ckpt > 0 else None

        # 5) build diagnostic report
        lines: List[str] = []
        lines.append("=== Job Runtime ===")
        lines.append(f"Uptime:        {_format_duration(uptime_ms) if uptime_ms is not None else 'N/A'}")
        lines.append(f"Running:       {_format_duration(running_ms) if running_ms is not None else 'N/A'}")
        lines.append(f"Downtime:      {_format_duration(downtime_ms) if downtime_ms is not None else 'N/A'}")
        lines.append(f"Init:          {_format_duration(initializing_ms) if initializing_ms is not None else 'N/A'}")
        lines.append(f"Deploying:     {_format_duration(deploying_ms) if deploying_ms is not None else 'N/A'}")
        lines.append(f"Restarting:    {_format_duration(restarting_ms) if restarting_ms is not None else 'N/A'}")
        lines.append(f"Failing:       {_format_duration(failing_ms) if failing_ms is not None else 'N/A'}")
        lines.append(f"Cancelling:    {_format_duration(cancelling_ms) if cancelling_ms is not None else 'N/A'}")
        lines.append(f"Created:       {_format_timestamp(created_time_ms) if created_time_ms is not None else 'N/A'}")
        lines.append("")

        lines.append("=== Stability ===")
        lines.append(f"Restarts:      {int(num_restarts)} (full: {int(full_restarts)})")
        lines.append("")

        lines.append("=== Checkpoints ===")
        lines.append(f"Total:         {int(total_ckpt)}  | Completed: {int(completed_ckpt)}  | Failed: {int(failed_ckpt)}  | In-Progress: {int(inprog_ckpt)}")
        lines.append(f"Success Ratio: {f'{success_ratio:.2%}' if success_ratio is not None else 'N/A'}"
                     f"  | Failure Ratio: {f'{failure_ratio:.2%}' if failure_ratio is not None else 'N/A'}")
        lines.append(f"Last Completed ID: {last_ckpt_id if last_ckpt_id not in (None, '') else 'N/A'}")
        lines.append(f"Last Duration:     {_format_duration(last_ckpt_duration_ms) if last_ckpt_duration_ms is not None else 'N/A'}")
        lines.append(f"Last Size (logical): {_format_bytes(last_ckpt_size_bytes) if last_ckpt_size_bytes is not None else 'N/A'}")
        lines.append(f"Last Size (full):    {_format_bytes(last_ckpt_full_size_bytes) if last_ckpt_full_size_bytes is not None else 'N/A'}")
        lines.append(f"Last Persisted:      {_format_bytes(last_ckpt_persisted_bytes) if last_ckpt_persisted_bytes is not None else 'N/A'}")
        lines.append(f"Last Processed:      {_format_bytes(last_ckpt_processed_bytes) if last_ckpt_processed_bytes is not None else 'N/A'}")
        lines.append(f"Last Restored At:    {_format_timestamp(last_ckpt_restore_ts) if last_ckpt_restore_ts is not None else 'N/A'}")
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

# New 

@mcp.tool()
async def get_taskmanager_details(taskmanager_id: str) -> str:
    """Get comprehensive details about a specific TaskManager including metrics.
    
    Args:
        taskmanager_id: TaskManager ID (e.g., '172.20.0.3:38373-66c42c')
        
    Returns detailed information about:
    - Basic info (ID, ports, heartbeat)
    - Slot allocation and usage
    - Hardware resources (CPU, memory)
    - Memory configuration breakdown
    - Currently allocated slots and jobs
    - Real-time metrics (heap, non-heap, GC stats)
    - Network buffer usage
    
    This is more detailed than list_taskmanagers which shows all TaskManagers.
    Use this when you need to deep-dive into a specific TaskManager.
    """
    url = f"{settings.flink_url}/taskmanagers/{taskmanager_id}"
    
    try:
        async with httpx.AsyncClient(verify=settings.tls_verify, timeout=10.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            tm = response.json()
        
        result = []
        result.append("=" * 80)
        result.append(f"TASKMANAGER DETAILS: {taskmanager_id}")
        result.append("=" * 80)
        
        # Basic Info
        result.append("\n📋 BASIC INFORMATION")
        result.append(f"  ID: {tm.get('id', 'N/A')}")
        result.append(f"  Path: {tm.get('path', 'N/A')}")
        result.append(f"  Data Port: {tm.get('dataPort', 'N/A')}")
        result.append(f"  JMX Port: {tm.get('jmxPort', 'Disabled' if tm.get('jmxPort', -1) == -1 else tm.get('jmxPort'))}")
        
        # Heartbeat
        heartbeat = tm.get('timeSinceLastHeartbeat', 0)
        if heartbeat > 0:
            from datetime import datetime
            last_heartbeat = datetime.fromtimestamp(heartbeat / 1000.0)
            result.append(f"  Last Heartbeat: {last_heartbeat.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Slot Information
        slots_total = tm.get('slotsNumber', 0)
        slots_free = tm.get('freeSlots', 0)
        slots_used = slots_total - slots_free
        
        result.append("\n🎰 SLOT INFORMATION")
        result.append(f"  Total Slots: {slots_total}")
        result.append(f"  Used Slots: {slots_used} ({(slots_used/slots_total*100) if slots_total > 0 else 0:.1f}%)")
        result.append(f"  Free Slots: {slots_free} ({(slots_free/slots_total*100) if slots_total > 0 else 0:.1f}%)")
        
        # Allocated Slots
        allocated_slots = tm.get('allocatedSlots', [])
        if allocated_slots:
            result.append(f"\n📌 ALLOCATED SLOTS ({len(allocated_slots)})")
            for idx, slot in enumerate(allocated_slots, 1):
                job_id = slot.get('jobId', 'N/A')
                resource = slot.get('resource', {})
                result.append(f"  [{idx}] Job: {job_id}")
                result.append(f"      CPU Cores: {resource.get('cpuCores', 'N/A')}")
                result.append(f"      Task Heap: {_format_bytes(resource.get('taskHeapMemory', 0) * 1024 * 1024)}")
                result.append(f"      Managed Memory: {_format_bytes(resource.get('managedMemory', 0) * 1024 * 1024)}")
                result.append(f"      Network Memory: {_format_bytes(resource.get('networkMemory', 0) * 1024 * 1024)}")
        
        # Hardware
        hardware = tm.get('hardware', {})
        if hardware:
            result.append("\n💻 HARDWARE")
            result.append(f"  CPU Cores: {hardware.get('cpuCores', 'N/A')}")
            phys_mem = hardware.get('physicalMemory', 0)
            free_mem = hardware.get('freeMemory', 0)
            managed_mem = hardware.get('managedMemory', 0)
            
            result.append(f"  Physical Memory: {_format_bytes(phys_mem)}")
            result.append(f"  Free Memory: {_format_bytes(free_mem)} ({(free_mem/phys_mem*100) if phys_mem > 0 else 0:.1f}%)")
            result.append(f"  Managed Memory: {_format_bytes(managed_mem)}")
        
        # Resource Configuration
        total_res = tm.get('totalResource', {})
        free_res = tm.get('freeResource', {})
        
        result.append("\n📊 RESOURCE ALLOCATION")
        result.append(f"  Configured:")
        result.append(f"    CPU Cores: {total_res.get('cpuCores', 'N/A')}")
        result.append(f"    Task Heap: {_format_bytes(total_res.get('taskHeapMemory', 0) * 1024 * 1024)}")
        result.append(f"    Task Off-Heap: {_format_bytes(total_res.get('taskOffHeapMemory', 0) * 1024 * 1024)}")
        result.append(f"    Managed Memory: {_format_bytes(total_res.get('managedMemory', 0) * 1024 * 1024)}")
        result.append(f"    Network Memory: {_format_bytes(total_res.get('networkMemory', 0) * 1024 * 1024)}")
        
        result.append(f"  Available:")
        result.append(f"    CPU Cores: {free_res.get('cpuCores', 'N/A')}")
        result.append(f"    Task Heap: {_format_bytes(free_res.get('taskHeapMemory', 0) * 1024 * 1024)}")
        result.append(f"    Managed Memory: {_format_bytes(free_res.get('managedMemory', 0) * 1024 * 1024)}")
        result.append(f"    Network Memory: {_format_bytes(free_res.get('networkMemory', 0) * 1024 * 1024)}")
        
        # Memory Configuration
        mem_config = tm.get('memoryConfiguration', {})
        if mem_config:
            result.append("\n🧠 MEMORY CONFIGURATION")
            result.append(f"  Framework Heap: {_format_bytes(mem_config.get('frameworkHeap', 0))}")
            result.append(f"  Framework Off-Heap: {_format_bytes(mem_config.get('frameworkOffHeap', 0))}")
            result.append(f"  Task Heap: {_format_bytes(mem_config.get('taskHeap', 0))}")
            result.append(f"  Task Off-Heap: {_format_bytes(mem_config.get('taskOffHeap', 0))}")
            result.append(f"  Network Memory: {_format_bytes(mem_config.get('networkMemory', 0))}")
            result.append(f"  Managed Memory: {_format_bytes(mem_config.get('managedMemory', 0))}")
            result.append(f"  JVM Metaspace: {_format_bytes(mem_config.get('jvmMetaspace', 0))}")
            result.append(f"  JVM Overhead: {_format_bytes(mem_config.get('jvmOverhead', 0))}")
            result.append(f"  Total Flink Memory: {_format_bytes(mem_config.get('totalFlinkMemory', 0))}")
            result.append(f"  Total Process Memory: {_format_bytes(mem_config.get('totalProcessMemory', 0))}")
        
        # Real-time Metrics
        metrics = tm.get('metrics', {})
        if metrics:
            result.append("\n📈 REAL-TIME METRICS")
            
            # Memory metrics
            heap_used = metrics.get('heapUsed', 0)
            heap_committed = metrics.get('heapCommitted', 0)
            heap_max = metrics.get('heapMax', 0)
            
            result.append(f"  Heap Memory:")
            result.append(f"    Used: {_format_bytes(heap_used)} / {_format_bytes(heap_max)} ({(heap_used/heap_max*100) if heap_max > 0 else 0:.1f}%)")
            result.append(f"    Committed: {_format_bytes(heap_committed)}")
            
            if heap_used / heap_max > 0.9 if heap_max > 0 else False:
                result.append(f"    ⚠️  WARNING: Heap usage above 90%!")
            
            non_heap_used = metrics.get('nonHeapUsed', 0)
            non_heap_max = metrics.get('nonHeapMax', 0)
            
            result.append(f"  Non-Heap Memory:")
            result.append(f"    Used: {_format_bytes(non_heap_used)} / {_format_bytes(non_heap_max)}")
            result.append(f"    Committed: {_format_bytes(metrics.get('nonHeapCommitted', 0))}")
            
            # Direct memory
            direct_used = metrics.get('directUsed', 0)
            direct_max = metrics.get('directMax', 0)
            result.append(f"  Direct Memory:")
            result.append(f"    Used: {_format_bytes(direct_used)} / {_format_bytes(direct_max)} ({(direct_used/direct_max*100) if direct_max > 0 else 0:.1f}%)")
            result.append(f"    Buffer Count: {metrics.get('directCount', 0):,}")
            
            # Network buffers
            mem_segs_avail = metrics.get('memorySegmentsAvailable', 0)
            mem_segs_total = metrics.get('memorySegmentsTotal', 0)
            
            result.append(f"  Network Buffers:")
            result.append(f"    Available: {mem_segs_avail:,} / {mem_segs_total:,}")
            result.append(f"    Netty Shuffle:")
            result.append(f"      Available: {_format_bytes(metrics.get('nettyShuffleMemoryAvailable', 0))}")
            result.append(f"      Used: {_format_bytes(metrics.get('nettyShuffleMemoryUsed', 0))}")
            result.append(f"      Total: {_format_bytes(metrics.get('nettyShuffleMemoryTotal', 0))}")
            
            # Garbage Collection
            gc_stats = metrics.get('garbageCollectors', [])
            if gc_stats:
                result.append(f"  Garbage Collection:")
                for gc in gc_stats:
                    gc_name = gc.get('name', 'Unknown')
                    gc_count = gc.get('count', 0)
                    gc_time = gc.get('time', 0)
                    result.append(f"    {gc_name}: {gc_count} collections, {gc_time}ms total")
        
        result.append("=" * 80)
        
        return "\n".join(result)
        
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return f"❌ TaskManager not found: {taskmanager_id}"
        logger.error(f"HTTP error fetching TaskManager details: {e}")
        return f"❌ HTTP Error ({e.response.status_code}): {str(e)}"
    except httpx.TimeoutException:
        logger.error("Timeout fetching TaskManager details")
        return "❌ Request timeout while fetching TaskManager details"
    except Exception as e:
        logger.error(f"Failed to get TaskManager details: {e}")
        return f"❌ Error getting TaskManager details: {str(e)}"


@mcp.tool()
async def get_taskmanager_metrics(
    taskmanager_id: str,
    metric_names: Optional[str] = None
) -> str:
    """Get specific metrics for a TaskManager.
    
    Args:
        taskmanager_id: TaskManager ID
        metric_names: Optional comma-separated list of metric names to query.
                     If not provided, returns list of available metrics.
                     
    Common metrics to query:
    - Status.JVM.CPU.Load: CPU usage
    - Status.JVM.Memory.Heap.Used/Max: Heap memory
    - Status.JVM.Memory.NonHeap.Used: Non-heap memory
    - Status.JVM.Threads.Count: Thread count
    - Status.Flink.Memory.Managed.Used/Total: Managed memory
    - Status.Network.AvailableMemorySegments: Network buffers
    
    Example:
        metric_names="Status.JVM.CPU.Load,Status.JVM.Memory.Heap.Used"
    """
    base_url = f"{settings.flink_url}/taskmanagers/{taskmanager_id}/metrics"
    
    try:
        async with httpx.AsyncClient(verify=settings.tls_verify, timeout=10.0) as client:
            if metric_names:
                # Query specific metrics
                url = f"{base_url}?get={metric_names}"
                response = await client.get(url)
                response.raise_for_status()
                metrics = response.json()
                
                result = []
                result.append("=" * 70)
                result.append(f"TASKMANAGER METRICS: {taskmanager_id}")
                result.append("=" * 70)
                
                for metric in metrics:
                    metric_id = metric.get('id', 'Unknown')
                    metric_value = metric.get('value', 'N/A')
                    
                    # Format based on metric type
                    if 'Memory' in metric_id and 'Ratio' not in metric_id:
                        if isinstance(metric_value, (int, float)):
                            formatted_value = _format_bytes(metric_value)
                        else:
                            formatted_value = metric_value
                    elif 'CPU.Load' in metric_id or 'Ratio' in metric_id:
                        if isinstance(metric_value, (int, float)):
                            formatted_value = f"{metric_value * 100:.2f}%" if metric_value <= 1 else f"{metric_value:.2f}%"
                        else:
                            formatted_value = metric_value
                    elif 'Count' in metric_id or 'Segments' in metric_id:
                        if isinstance(metric_value, (int, float)):
                            formatted_value = f"{int(metric_value):,}"
                        else:
                            formatted_value = metric_value
                    else:
                        formatted_value = metric_value
                    
                    result.append(f"  {metric_id}: {formatted_value}")
                
                result.append("=" * 70)
                return "\n".join(result)
            else:
                # List available metrics
                response = await client.get(base_url)
                response.raise_for_status()
                metrics = response.json()
                
                result = []
                result.append("=" * 70)
                result.append(f"AVAILABLE METRICS for {taskmanager_id}")
                result.append("=" * 70)
                result.append(f"\nTotal metrics available: {len(metrics)}\n")
                
                # Group metrics by category
                categories = {}
                for metric in metrics:
                    metric_id = metric.get('id', '')
                    parts = metric_id.split('.')
                    category = '.'.join(parts[:3]) if len(parts) >= 3 else 'Other'
                    
                    if category not in categories:
                        categories[category] = []
                    categories[category].append(metric_id)
                
                for category in sorted(categories.keys()):
                    result.append(f"\n{category}:")
                    for metric_id in sorted(categories[category]):
                        result.append(f"  - {metric_id}")
                
                result.append("\n" + "=" * 70)
                result.append("\n💡 TIP: To query specific metrics, use:")
                result.append('   metric_names="Status.JVM.CPU.Load,Status.JVM.Memory.Heap.Used"')
                result.append("=" * 70)
                
                return "\n".join(result)
        
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return f"❌ TaskManager not found: {taskmanager_id}"
        logger.error(f"HTTP error fetching TaskManager metrics: {e}")
        return f"❌ HTTP Error ({e.response.status_code}): {str(e)}"
    except httpx.TimeoutException:
        logger.error("Timeout fetching TaskManager metrics")
        return "❌ Request timeout while fetching TaskManager metrics"
    except Exception as e:
        logger.error(f"Failed to get TaskManager metrics: {e}")
        return f"❌ Error getting TaskManager metrics: {str(e)}"



@mcp.tool()
async def get_checkpoint_details(job_id: str, checkpoint_id: int) -> str:
    """Get per-subtask breakdown for a specific checkpoint.

    Args:
        job_id: The Flink job ID.
        checkpoint_id: The checkpoint ID to inspect.

    Returns per-operator/subtask checkpoint duration, state size, and status.
    Useful for pinpointing which operator is causing checkpoint delays.
    """
    url = f"{settings.flink_url}/jobs/{job_id}/checkpoints/details/{checkpoint_id}"
    try:
        async with httpx.AsyncClient(verify=settings.tls_verify, timeout=10.0) as client:
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
        lines.append(f"  Duration:     {_format_duration(data.get('end_to_end_duration', 0))}")
        lines.append(f"  State Size:   {_format_bytes(data.get('state_size', 0))}")
        lines.append(f"  Trigger Time: {_format_timestamp(data.get('trigger_timestamp', 0))}")
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
                        dur = _format_duration(st.get("end_to_end_duration", 0))
                        sz = _format_bytes(st.get("state_size", 0))
                        sync_dur = _format_duration(st.get("sync_duration", 0))
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



@mcp.tool()
async def get_job_accumulators(job_id: str) -> str:
    """Get user-defined accumulators for a Flink job.

    Args:
        job_id: The Flink job ID.

    Returns each accumulator's name, type, and value.
    """
    url = f"{settings.flink_url}/jobs/{job_id}/accumulators"
    try:
        async with httpx.AsyncClient(verify=settings.tls_verify, timeout=10.0) as client:
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
    CATEGORIES = {
        "backpressure": f"{settings.flink_url}/jobs/{job_id}/vertices/{vertex_id}/backpressure",
        "metrics": f"{settings.flink_url}/jobs/{job_id}/vertices/{vertex_id}/metrics",
        "subtask_times": f"{settings.flink_url}/jobs/{job_id}/vertices/{vertex_id}/subtasktimes",
        "taskmanager_stats": f"{settings.flink_url}/jobs/{job_id}/vertices/{vertex_id}/taskmanagers",
        "accumulators": f"{settings.flink_url}/jobs/{job_id}/vertices/{vertex_id}/accumulators",
    }

    if info_category not in CATEGORIES:
        return (
            f"❌ Unknown info_category '{info_category}'. "
            f"Valid options: {', '.join(CATEGORIES)}"
        )

    url = CATEGORIES[info_category]

    try:
        async with httpx.AsyncClient(verify=settings.tls_verify, timeout=10.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()

        output = (
            f"Vertex {info_category} — Job {job_id} / Vertex {vertex_id}\n"
            + "=" * 70 + "\n"
            + json.dumps(data, indent=2)
        )
        if len(output) > settings.max_output_chars:
            output = output[:settings.max_output_chars] + f"\n\n⚠️ Output truncated at {settings.max_output_chars} characters."
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
async def get_jobmanager_metrics(metric_names: Optional[str] = None) -> str:
    """Get metrics for the JobManager.

    Args:
        metric_names: Optional comma-separated metric names to query.
                      If omitted, lists all available metric IDs.

    Key metrics: Status.JVM.Memory.Heap.Used, Status.JVM.Memory.Heap.Max,
    Status.JVM.GarbageCollector.*.Count, Status.JVM.GarbageCollector.*.Time,
    Status.JVM.Threads.Count, Status.JVM.CPU.Load.
    """
    base_url = f"{settings.flink_url}/jobmanager/metrics"
    try:
        async with httpx.AsyncClient(verify=settings.tls_verify, timeout=10.0) as client:
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
                            val = _format_bytes(float(val))
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
    url = f"{settings.flink_url}/jobmanager/config"
    try:
        async with httpx.AsyncClient(verify=settings.tls_verify, timeout=10.0) as client:
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
        if len(output) > settings.max_output_chars:
            output = output[:settings.max_output_chars] + f"\n\n⚠️ Output truncated at {settings.max_output_chars} characters."
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
    url = f"{settings.flink_url}/jobmanager/environment"
    try:
        async with httpx.AsyncClient(verify=settings.tls_verify, timeout=10.0) as client:
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
                lines.append(f"  Max Heap:   {_format_bytes(max_heap.get('maxHeapSize', 0))}")
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
        if len(output) > settings.max_output_chars:
            output = output[:settings.max_output_chars] + f"\n\n⚠️ Output truncated at {settings.max_output_chars} characters."
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
    base = settings.flink_url

    if target == "taskmanager":
        if not taskmanager_id:
            return "❌ taskmanager_id is required when target is 'taskmanager'."
        url = f"{base}/taskmanagers/{taskmanager_id}/logs"
    else:
        url = f"{base}/jobmanager/logs"

    try:
        async with httpx.AsyncClient(verify=settings.tls_verify) as client:
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
            lines.append(f"{name:<50} {_format_bytes(size):>10}")
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
    base = settings.flink_url

    if target == "taskmanager":
        if not taskmanager_id:
            return "❌ taskmanager_id is required when target is 'taskmanager'."
        url = f"{base}/taskmanagers/{taskmanager_id}/logs/{log_file}"
    else:
        url = f"{base}/jobmanager/logs/{log_file}"

    try:
        async with httpx.AsyncClient(verify=settings.tls_verify) as client:
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


@mcp.tool()
async def list_job_ids() -> str:
    """
    List all job IDs known to the cluster with their current status.
    Lighter alternative to list_jobs — hits GET /jobs instead of /jobs/overview.
    """
    url = f"{settings.flink_url}/jobs"
    try:
        async with httpx.AsyncClient(verify=settings.tls_verify, timeout=10.0) as client:
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
async def get_job_plan(job_id: str) -> str:
    """
    Return the dataflow plan (DAG) for a job: node IDs, descriptions,
    parallelism, and input edges with their ship strategies.

    Args:
        job_id: The Flink job ID.
    """
    url = f"{settings.flink_url}/jobs/{job_id}/plan"
    try:
        async with httpx.AsyncClient(verify=settings.tls_verify, timeout=10.0) as client:
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
    url = f"{settings.flink_url}/jobs/{job_id}/checkpoints/config"
    try:
        async with httpx.AsyncClient(verify=settings.tls_verify, timeout=10.0) as client:
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
            f"Interval:                 {_format_duration(interval) if interval >= 0 else 'N/A'}",
            f"Timeout:                  {_format_duration(timeout) if timeout >= 0 else 'N/A'}",
            f"Min pause between:        {_format_duration(min_pause) if min_pause >= 0 else 'N/A'}",
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
    url = f"{settings.flink_url}/jobs/{job_id}/vertices/{vertex_id}"
    try:
        async with httpx.AsyncClient(verify=settings.tls_verify, timeout=10.0) as client:
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
            lines.append(f"        start={_format_timestamp(start_ms)}  "
                         f"end={'running' if end_ms < 0 else _format_timestamp(end_ms)}  "
                         f"duration={_format_duration(duration_ms)}")

            # Per-subtask metrics if present
            metrics = st.get("metrics", {})
            if metrics:
                r_rec = metrics.get("read-records", 0)
                w_rec = metrics.get("write-records", 0)
                r_bytes = metrics.get("read-bytes", 0)
                w_bytes = metrics.get("write-bytes", 0)
                if r_rec or r_bytes:
                    lines.append(f"        read:  {r_rec:,} records  {_format_bytes(r_bytes)}")
                if w_rec or w_bytes:
                    lines.append(f"        write: {w_rec:,} records  {_format_bytes(w_bytes)}")
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
            lines.append(f"  Read:  {r_rec:,} records  {_format_bytes(r_bytes)}")
            lines.append(f"  Write: {w_rec:,} records  {_format_bytes(w_bytes)}")

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
    url = f"{settings.flink_url}/jobs/{job_id}/vertices/{vertex_id}/flamegraph"
    try:
        async with httpx.AsyncClient(verify=settings.tls_verify, timeout=10.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()

        output = (
            f"Flame Graph — Vertex {vertex_id}  (Job {job_id})\n"
            + "=" * 70 + "\n"
            + json.dumps(data, indent=2)
        )
        if len(output) > settings.max_output_chars:
            output = output[:settings.max_output_chars] + f"\n\n⚠️ Output truncated at {settings.max_output_chars} characters."
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
async def get_taskmanager_thread_dump(taskmanager_id: str) -> str:
    """
    Return a full JVM thread dump for the specified TaskManager.
    Threads are grouped by state (RUNNABLE, WAITING, BLOCKED, etc.).
    BLOCKED threads are highlighted prominently as they may indicate
    deadlocks or lock contention. Stack traces are truncated to 15 frames.

    Args:
        taskmanager_id: The TaskManager ID (from list_taskmanagers).
    """
    url = f"{settings.flink_url}/taskmanagers/{taskmanager_id}/thread-dump"
    try:
        async with httpx.AsyncClient(verify=settings.tls_verify, timeout=10.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()

        threads = data.get("threadInfos", data.get("threads", []))
        if not threads:
            return f"No thread data returned for TaskManager {taskmanager_id}."

        # Group by state
        groups: Dict[str, list] = {}
        for t in threads:
            state = t.get("threadState", t.get("state", "UNKNOWN"))
            groups.setdefault(state, []).append(t)

        state_order = ["BLOCKED", "RUNNABLE", "WAITING", "TIMED_WAITING", "TERMINATED", "NEW", "UNKNOWN"]
        sorted_states = sorted(groups.keys(), key=lambda s: state_order.index(s) if s in state_order else 99)

        lines = [
            f"Thread Dump — TaskManager {taskmanager_id}",
            "=" * 70,
            f"Total threads: {len(threads)}",
        ]

        state_summary = "  ".join(f"{s}={len(v)}" for s, v in groups.items())
        lines.append(f"By state:      {state_summary}")
        lines.append("")

        for state in sorted_states:
            group = groups[state]
            if state == "BLOCKED":
                lines.append(f"🔴 BLOCKED THREADS ({len(group)})  ← possible deadlock / contention")
            else:
                lines.append(f"{'⚠️ ' if state == 'WAITING' else ''}{state} ({len(group)})")
            lines.append("-" * 70)

            for t in group:
                name = t.get("threadName", t.get("name", "unknown"))
                lines.append(f"  Thread: {name}")
                # Stack trace — accept several field name shapes
                stack = (
                    t.get("stackTrace")
                    or t.get("stacktrace")
                    or t.get("stack")
                    or []
                )
                if isinstance(stack, str):
                    frames = stack.splitlines()[:15]
                    for frame in frames:
                        lines.append(f"    {frame}")
                elif isinstance(stack, list):
                    for frame in stack[:15]:
                        if isinstance(frame, dict):
                            cls = frame.get("className", "")
                            method = frame.get("methodName", "")
                            file_ = frame.get("fileName", "")
                            line_ = frame.get("lineNumber", "")
                            lines.append(f"    at {cls}.{method}({file_}:{line_})")
                        else:
                            lines.append(f"    {frame}")
                    if len(stack) > 15:
                        lines.append(f"    ... {len(stack) - 15} more frames omitted")
                else:
                    lines.append("    (no stack trace available)")
                lines.append("")

        lines.append("=" * 70)
        output = "\n".join(lines)
        if len(output) > settings.max_output_chars:
            output = output[:settings.max_output_chars] + f"\n\n⚠️ Output truncated at {settings.max_output_chars} characters."
        return output

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return f"❌ TaskManager not found: {taskmanager_id}"
        return f"❌ HTTP Error ({e.response.status_code}): {str(e)}"
    except httpx.TimeoutException:
        return f"❌ Request timeout fetching thread dump for TaskManager {taskmanager_id}."
    except Exception as e:
        logger.error(f"Failed to get thread dump for {taskmanager_id}: {e}")
        return f"❌ Error fetching thread dump: {str(e)}"


@mcp.tool()
async def get_cluster_config() -> str:
    """
    Return the REST/web server configuration from GET /config.
    This covers the Flink version, web UI refresh interval, timezone, and
    related web-layer settings. This is distinct from get_jobmanager_config,
    which returns the full effective cluster/JobManager configuration.
    """
    url = f"{settings.flink_url}/config"
    try:
        async with httpx.AsyncClient(verify=settings.tls_verify, timeout=10.0) as client:
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
    url = f"{settings.flink_url}/datasets"
    try:
        async with httpx.AsyncClient(verify=settings.tls_verify, timeout=10.0) as client:
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
            size_str = _format_bytes(size) if size >= 0 else "N/A"
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


def main():
    try:
        logger.info("Starting Flink MCP server...")
        logger.info(f"Flink URL: {settings.flink_url}")
        logger.info("=" * 60)
        logger.info("Available tools:")
        logger.info("-" * 60)
        logger.info("  🏢 get_cluster_info: Overview of the Flink cluster")
        logger.info("  📋 list_jobs: List all Flink jobs with status")
        logger.info("  🔍 get_job_details: Comprehensive job details by ID")
        logger.info("  ⚠️  get_job_exceptions: Fetch job-level exceptions")
        logger.info("  📈 get_job_metrics: Fetch metrics for a job")
        logger.info("  💻 list_taskmanagers: List TaskManagers with resources")
        logger.info("  📦 list_jar_files: List uploaded JAR files")
        logger.info("-" * 60)
        logger.info("  🔖 get_job_checkpoints: Checkpoint history & counts for a job")
        logger.info("  🔬 get_checkpoint_details: Per-subtask breakdown for a checkpoint")
        logger.info("  🔎 get_vertex_info: Vertex info by category (backpressure/metrics/subtask_times/taskmanager_stats/accumulators)")
        logger.info("  🧮 get_job_accumulators: User-defined accumulators for a job")
        logger.info("  🖥️  get_jobmanager_metrics: List/query JobManager metrics")
        logger.info("  ⚙️  get_jobmanager_config: Full effective cluster configuration")
        logger.info("  🌍 get_jobmanager_environment: JVM/env info from JobManager")
        logger.info("  📄 list_flink_logs: List available log files on JobManager/TaskManager")
        logger.info("  📜 read_flink_logs: Read a log file with optional tail/filter support")
        logger.info("-" * 60)
        logger.info("  🆔 list_job_ids: Light job-ID + status list via GET /jobs")
        logger.info("  🗺️  get_job_plan: Dataflow DAG with nodes, edges, and ship strategies")
        logger.info("  ⚙️  get_job_checkpoint_config: Active checkpoint config for a job")
        logger.info("  🔬 get_vertex_details: Full per-subtask breakdown for a vertex")
        logger.info("  🔥 get_vertex_flamegraph: CPU flame graph data for a vertex (Flink 1.17+)")
        logger.info("  🧵 get_taskmanager_thread_dump: JVM thread dump grouped by state")
        logger.info("  🌐 get_cluster_config: REST/web UI config (version, timezone, refresh)")
        logger.info("  🗄️  list_datasets: Intermediate batch datasets on the cluster")
        logger.info("=" * 60)
        logger.info("Server starting on http://127.0.0.1:9090")
        logger.info("=" * 60)
        
        mcp.run(transport="streamable-http", host="127.0.0.1", port=9090)
    except KeyboardInterrupt:
        logger.info("\n" + "=" * 60)
        logger.info("Server stopped by user")
        logger.info("=" * 60)
    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"❌ Server error: {str(e)}")
        logger.error("=" * 60)
        sys.exit(1)

if __name__ == "__main__":
    main()