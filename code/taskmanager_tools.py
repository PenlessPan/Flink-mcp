import asyncio
import logging
from datetime import datetime
from typing import Dict, Optional

import httpx

from .app import mcp
from .connection import get_settings, MAX_OUTPUT_CHARS
from .utils import format_bytes, format_duration, format_timestamp

logger = logging.getLogger("flink-mcp-server")


@mcp.tool()
async def list_taskmanagers() -> str:
    """List all registered TaskManagers in the Flink cluster with detailed resource information."""
    url = f"{get_settings()['url']}/taskmanagers"
    try:
        async with httpx.AsyncClient(verify=False, timeout=10.0) as client:
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

                result.append(f"  Physical Memory: {format_bytes(phys_mem)}")
                result.append(f"  Free Memory: {format_bytes(free_mem)} ({(free_mem/phys_mem*100) if phys_mem > 0 else 0:.1f}%)")
                result.append(f"  Managed Memory: {format_bytes(managed_mem)}")

                if phys_mem > 0 and free_mem > 0:
                    used_mem = phys_mem - free_mem
                    result.append(f"  Used Memory: {format_bytes(used_mem)} ({(used_mem/phys_mem*100):.1f}%)")

            # Total Resources (configured)
            total_res = tm.get('totalResource', {})
            if total_res:
                result.append(f"\n📊 CONFIGURED RESOURCES")
                result.append(f"  CPU Cores (slots): {total_res.get('cpuCores', 'N/A')}")
                result.append(f"  Task Heap Memory: {format_bytes(total_res.get('taskHeapMemory', 0) * 1024 * 1024)}")
                result.append(f"  Task Off-Heap Memory: {format_bytes(total_res.get('taskOffHeapMemory', 0) * 1024 * 1024)}")
                result.append(f"  Managed Memory: {format_bytes(total_res.get('managedMemory', 0) * 1024 * 1024)}")
                result.append(f"  Network Memory: {format_bytes(total_res.get('networkMemory', 0) * 1024 * 1024)}")

                extended = total_res.get('extendedResources', {})
                if extended:
                    result.append(f"  Extended Resources: {extended}")

            # Free Resources (available)
            free_res = tm.get('freeResource', {})
            if free_res:
                result.append(f"\n✅ AVAILABLE RESOURCES")
                result.append(f"  CPU Cores: {free_res.get('cpuCores', 0)}")
                result.append(f"  Task Heap Memory: {format_bytes(free_res.get('taskHeapMemory', 0) * 1024 * 1024)}")
                result.append(f"  Task Off-Heap Memory: {format_bytes(free_res.get('taskOffHeapMemory', 0) * 1024 * 1024)}")
                result.append(f"  Managed Memory: {format_bytes(free_res.get('managedMemory', 0) * 1024 * 1024)}")
                result.append(f"  Network Memory: {format_bytes(free_res.get('networkMemory', 0) * 1024 * 1024)}")

            # Memory Configuration Details
            mem_config = tm.get('memoryConfiguration', {})
            if mem_config:
                result.append(f"\n🧠 MEMORY CONFIGURATION")
                result.append(f"  Framework Heap: {format_bytes(mem_config.get('frameworkHeap', 0))}")
                result.append(f"  Framework Off-Heap: {format_bytes(mem_config.get('frameworkOffHeap', 0))}")
                result.append(f"  Task Heap: {format_bytes(mem_config.get('taskHeap', 0))}")
                result.append(f"  Task Off-Heap: {format_bytes(mem_config.get('taskOffHeap', 0))}")
                result.append(f"  Network Memory: {format_bytes(mem_config.get('networkMemory', 0))}")
                result.append(f"  Managed Memory: {format_bytes(mem_config.get('managedMemory', 0))}")
                result.append(f"  JVM Metaspace: {format_bytes(mem_config.get('jvmMetaspace', 0))}")
                result.append(f"  JVM Overhead: {format_bytes(mem_config.get('jvmOverhead', 0))}")
                result.append(f"  Total Flink Memory: {format_bytes(mem_config.get('totalFlinkMemory', 0))}")
                result.append(f"  Total Process Memory: {format_bytes(mem_config.get('totalProcessMemory', 0))}")

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
    url = f"{get_settings()['url']}/taskmanagers/{taskmanager_id}"

    try:
        async with httpx.AsyncClient(verify=False, timeout=10.0) as client:
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
                result.append(f"      Task Heap: {format_bytes(resource.get('taskHeapMemory', 0) * 1024 * 1024)}")
                result.append(f"      Managed Memory: {format_bytes(resource.get('managedMemory', 0) * 1024 * 1024)}")
                result.append(f"      Network Memory: {format_bytes(resource.get('networkMemory', 0) * 1024 * 1024)}")

        # Hardware
        hardware = tm.get('hardware', {})
        if hardware:
            result.append("\n💻 HARDWARE")
            result.append(f"  CPU Cores: {hardware.get('cpuCores', 'N/A')}")
            phys_mem = hardware.get('physicalMemory', 0)
            free_mem = hardware.get('freeMemory', 0)
            managed_mem = hardware.get('managedMemory', 0)

            result.append(f"  Physical Memory: {format_bytes(phys_mem)}")
            result.append(f"  Free Memory: {format_bytes(free_mem)} ({(free_mem/phys_mem*100) if phys_mem > 0 else 0:.1f}%)")
            result.append(f"  Managed Memory: {format_bytes(managed_mem)}")

        # Resource Configuration
        total_res = tm.get('totalResource', {})
        free_res = tm.get('freeResource', {})

        result.append("\n📊 RESOURCE ALLOCATION")
        result.append(f"  Configured:")
        result.append(f"    CPU Cores: {total_res.get('cpuCores', 'N/A')}")
        result.append(f"    Task Heap: {format_bytes(total_res.get('taskHeapMemory', 0) * 1024 * 1024)}")
        result.append(f"    Task Off-Heap: {format_bytes(total_res.get('taskOffHeapMemory', 0) * 1024 * 1024)}")
        result.append(f"    Managed Memory: {format_bytes(total_res.get('managedMemory', 0) * 1024 * 1024)}")
        result.append(f"    Network Memory: {format_bytes(total_res.get('networkMemory', 0) * 1024 * 1024)}")

        result.append(f"  Available:")
        result.append(f"    CPU Cores: {free_res.get('cpuCores', 'N/A')}")
        result.append(f"    Task Heap: {format_bytes(free_res.get('taskHeapMemory', 0) * 1024 * 1024)}")
        result.append(f"    Managed Memory: {format_bytes(free_res.get('managedMemory', 0) * 1024 * 1024)}")
        result.append(f"    Network Memory: {format_bytes(free_res.get('networkMemory', 0) * 1024 * 1024)}")

        # Memory Configuration
        mem_config = tm.get('memoryConfiguration', {})
        if mem_config:
            result.append("\n🧠 MEMORY CONFIGURATION")
            result.append(f"  Framework Heap: {format_bytes(mem_config.get('frameworkHeap', 0))}")
            result.append(f"  Framework Off-Heap: {format_bytes(mem_config.get('frameworkOffHeap', 0))}")
            result.append(f"  Task Heap: {format_bytes(mem_config.get('taskHeap', 0))}")
            result.append(f"  Task Off-Heap: {format_bytes(mem_config.get('taskOffHeap', 0))}")
            result.append(f"  Network Memory: {format_bytes(mem_config.get('networkMemory', 0))}")
            result.append(f"  Managed Memory: {format_bytes(mem_config.get('managedMemory', 0))}")
            result.append(f"  JVM Metaspace: {format_bytes(mem_config.get('jvmMetaspace', 0))}")
            result.append(f"  JVM Overhead: {format_bytes(mem_config.get('jvmOverhead', 0))}")
            result.append(f"  Total Flink Memory: {format_bytes(mem_config.get('totalFlinkMemory', 0))}")
            result.append(f"  Total Process Memory: {format_bytes(mem_config.get('totalProcessMemory', 0))}")

        # Real-time Metrics
        metrics = tm.get('metrics', {})
        if metrics:
            result.append("\n📈 REAL-TIME METRICS")

            # Memory metrics
            heap_used = metrics.get('heapUsed', 0)
            heap_committed = metrics.get('heapCommitted', 0)
            heap_max = metrics.get('heapMax', 0)

            result.append(f"  Heap Memory:")
            result.append(f"    Used: {format_bytes(heap_used)} / {format_bytes(heap_max)} ({(heap_used/heap_max*100) if heap_max > 0 else 0:.1f}%)")
            result.append(f"    Committed: {format_bytes(heap_committed)}")

            if heap_used / heap_max > 0.9 if heap_max > 0 else False:
                result.append(f"    ⚠️  WARNING: Heap usage above 90%!")

            non_heap_used = metrics.get('nonHeapUsed', 0)
            non_heap_max = metrics.get('nonHeapMax', 0)

            result.append(f"  Non-Heap Memory:")
            result.append(f"    Used: {format_bytes(non_heap_used)} / {format_bytes(non_heap_max)}")
            result.append(f"    Committed: {format_bytes(metrics.get('nonHeapCommitted', 0))}")

            # Direct memory
            direct_used = metrics.get('directUsed', 0)
            direct_max = metrics.get('directMax', 0)
            result.append(f"  Direct Memory:")
            result.append(f"    Used: {format_bytes(direct_used)} / {format_bytes(direct_max)} ({(direct_used/direct_max*100) if direct_max > 0 else 0:.1f}%)")
            result.append(f"    Buffer Count: {metrics.get('directCount', 0):,}")

            # Network buffers
            mem_segs_avail = metrics.get('memorySegmentsAvailable', 0)
            mem_segs_total = metrics.get('memorySegmentsTotal', 0)

            result.append(f"  Network Buffers:")
            result.append(f"    Available: {mem_segs_avail:,} / {mem_segs_total:,}")
            result.append(f"    Netty Shuffle:")
            result.append(f"      Available: {format_bytes(metrics.get('nettyShuffleMemoryAvailable', 0))}")
            result.append(f"      Used: {format_bytes(metrics.get('nettyShuffleMemoryUsed', 0))}")
            result.append(f"      Total: {format_bytes(metrics.get('nettyShuffleMemoryTotal', 0))}")

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
    base_url = f"{get_settings()['url']}/taskmanagers/{taskmanager_id}/metrics"

    try:
        async with httpx.AsyncClient(verify=False, timeout=10.0) as client:
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
                            formatted_value = format_bytes(metric_value)
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
async def get_taskmanager_thread_dump(taskmanager_id: str) -> str:
    """
    Return a full JVM thread dump for the specified TaskManager.
    Threads are grouped by state (RUNNABLE, WAITING, BLOCKED, etc.).
    BLOCKED threads are highlighted prominently as they may indicate
    deadlocks or lock contention. Stack traces are truncated to 15 frames.

    Args:
        taskmanager_id: The TaskManager ID (from list_taskmanagers).
    """
    url = f"{get_settings()['url']}/taskmanagers/{taskmanager_id}/thread-dump"
    try:
        async with httpx.AsyncClient(verify=False, timeout=10.0) as client:
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
        if len(output) > MAX_OUTPUT_CHARS:
            output = output[:MAX_OUTPUT_CHARS] + f"\n\n⚠️ Output truncated at {MAX_OUTPUT_CHARS} characters."
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
async def diagnose_taskmanager(taskmanager_id: str) -> str:
    """
    Run a full diagnostic on a TaskManager and return a structured health report.

    Concurrently fetches TM details, key JVM/resource metrics, and a thread
    dump, then presents them in order of diagnostic priority with an overall
    assessment (HEALTHY / UNDER PRESSURE / CRITICAL).

    Args:
        taskmanager_id: The TaskManager ID (from list_taskmanagers).
    """
    base = get_settings()["url"]
    tm_url = f"{base}/taskmanagers/{taskmanager_id}"
    metrics_url = (
        f"{base}/taskmanagers/{taskmanager_id}/metrics"
        "?get=Status.JVM.CPU.Load"
        ",Status.JVM.Memory.Heap.Used"
        ",Status.JVM.Memory.Heap.Max"
        ",Status.JVM.Memory.NonHeap.Used"
        ",Status.JVM.GarbageCollector.G1_Young_Generation.Count"
        ",Status.JVM.GarbageCollector.G1_Young_Generation.Time"
        ",Status.JVM.GarbageCollector.G1_Old_Generation.Count"
        ",Status.JVM.GarbageCollector.G1_Old_Generation.Time"
        ",Status.JVM.Threads.Count"
    )
    thread_url = f"{base}/taskmanagers/{taskmanager_id}/thread-dump"

    async def _get(client, url):
        try:
            r = await client.get(url, timeout=10.0)
            r.raise_for_status()
            return r.json()
        except Exception:
            return None

    try:
        async with httpx.AsyncClient(verify=False) as client:
            tm_data, metrics_raw, thread_data = await asyncio.gather(
                _get(client, tm_url),
                _get(client, metrics_url),
                _get(client, thread_url),
            )
    except Exception as e:
        logger.error(f"Failed to fetch TM diagnostic data: {e}")
        return f"❌ Error fetching TaskManager diagnostic data: {str(e)}"

    if tm_data is None:
        return f"❌ TaskManager not found or unreachable: {taskmanager_id}"

    sep = "=" * 70
    lines = [sep, f"TASKMANAGER DIAGNOSTIC — {taskmanager_id}", sep]

    # ── 1. BASIC INFO ────────────────────────────────────────────────────
    lines.append("\n── BASIC INFO ──────────────────────────────────────────────────")
    lines.append(f"  Path:      {tm_data.get('path', 'N/A')}")
    lines.append(f"  Data Port: {tm_data.get('dataPort', 'N/A')}")
    slots_total = tm_data.get("slotsNumber", 0)
    slots_free = tm_data.get("freeSlots", 0)
    slots_used = slots_total - slots_free
    slot_pct = (slots_used / slots_total * 100) if slots_total > 0 else 0
    lines.append(f"  Slots:     {slots_used}/{slots_total} used ({slot_pct:.0f}%)")

    # ── 2. RESOURCE UTILIZATION ──────────────────────────────────────────
    lines.append("\n── RESOURCE UTILIZATION ────────────────────────────────────────")

    warnings = []

    # Parse metrics into a lookup
    metrics_map: Dict[str, Any] = {}
    if metrics_raw and isinstance(metrics_raw, list):
        for m in metrics_raw:
            mid = m.get("id", "")
            try:
                val = float(m.get("value", 0))
            except (TypeError, ValueError):
                val = 0.0
            metrics_map[mid] = val

    cpu_load = metrics_map.get("Status.JVM.CPU.Load", None)
    heap_used = metrics_map.get("Status.JVM.Memory.Heap.Used", None)
    heap_max = metrics_map.get("Status.JVM.Memory.Heap.Max", None)
    non_heap_used = metrics_map.get("Status.JVM.Memory.NonHeap.Used", None)

    if cpu_load is not None:
        cpu_pct = cpu_load * 100
        cpu_flag = " ⚠️ HIGH" if cpu_pct >= 80 else ""
        lines.append(f"  CPU Load:      {cpu_pct:.1f}%{cpu_flag}")
        if cpu_pct >= 80:
            warnings.append(f"CPU load {cpu_pct:.0f}%")

    if heap_used is not None and heap_max is not None and heap_max > 0:
        heap_pct = heap_used / heap_max * 100
        heap_flag = " ⚠️ HIGH" if heap_pct >= 85 else ""
        lines.append(f"  Heap Memory:   {format_bytes(int(heap_used))} / {format_bytes(int(heap_max))} ({heap_pct:.0f}%){heap_flag}")
        if heap_pct >= 85:
            warnings.append(f"heap {heap_pct:.0f}% full")
    elif tm_data.get("hardware"):
        hw = tm_data["hardware"]
        phys = hw.get("physicalMemory", 0)
        free = hw.get("freeMemory", 0)
        if phys > 0:
            mem_pct = (phys - free) / phys * 100
            mem_flag = " ⚠️ HIGH" if mem_pct >= 85 else ""
            lines.append(f"  Physical Mem:  {format_bytes(phys - free)} / {format_bytes(phys)} ({mem_pct:.0f}%){mem_flag}")
            if mem_pct >= 85:
                warnings.append(f"memory {mem_pct:.0f}% used")

    if non_heap_used is not None:
        lines.append(f"  Non-Heap Mem:  {format_bytes(int(non_heap_used))}")

    # GC pressure
    young_time = metrics_map.get("Status.JVM.GarbageCollector.G1_Young_Generation.Time", 0)
    old_time = metrics_map.get("Status.JVM.GarbageCollector.G1_Old_Generation.Time", 0)
    young_count = int(metrics_map.get("Status.JVM.GarbageCollector.G1_Young_Generation.Count", 0))
    old_count = int(metrics_map.get("Status.JVM.GarbageCollector.G1_Old_Generation.Count", 0))

    if young_count or old_count:
        lines.append(f"  GC (Young):    {young_count} collections, {format_duration(int(young_time))}")
        lines.append(f"  GC (Old):      {old_count} collections, {format_duration(int(old_time))}")
        if old_count > 5:
            warnings.append(f"high old-gen GC ({old_count} collections)")

    thread_count = metrics_map.get("Status.JVM.Threads.Count", None)
    if thread_count is not None:
        lines.append(f"  Threads:       {int(thread_count)}")

    # ── 3. THREAD SUMMARY ────────────────────────────────────────────────
    lines.append("\n── THREAD SUMMARY ──────────────────────────────────────────────")

    if thread_data:
        threads = thread_data.get("threadInfos", thread_data.get("threads", []))
        groups: Dict[str, list] = {}
        for t in threads:
            state = t.get("threadState", t.get("state", "UNKNOWN"))
            groups.setdefault(state, []).append(t)

        state_order = ["BLOCKED", "RUNNABLE", "WAITING", "TIMED_WAITING", "TERMINATED", "NEW", "UNKNOWN"]
        sorted_states = sorted(groups.keys(), key=lambda s: state_order.index(s) if s in state_order else 99)

        lines.append(f"  Total threads: {len(threads)}")
        summary = "  ".join(f"{s}={len(v)}" for s, v in groups.items())
        lines.append(f"  By state:      {summary}")

        blocked = groups.get("BLOCKED", [])
        if blocked:
            warnings.append(f"{len(blocked)} BLOCKED thread(s)")
            lines.append(f"\n  🔴 BLOCKED THREADS ({len(blocked)}) — possible deadlock / contention:")
            for t in blocked[:5]:
                name = t.get("threadName", t.get("name", "unknown"))
                lines.append(f"    Thread: {name}")
                stack = t.get("stackTrace") or t.get("stacktrace") or t.get("stack") or []
                if isinstance(stack, list):
                    for frame in stack[:5]:
                        if isinstance(frame, dict):
                            cls = frame.get("className", "")
                            method = frame.get("methodName", "")
                            file_ = frame.get("fileName", "")
                            line_ = frame.get("lineNumber", "")
                            lines.append(f"      at {cls}.{method}({file_}:{line_})")
                        else:
                            lines.append(f"      {frame}")
                elif isinstance(stack, str):
                    for frame in stack.splitlines()[:5]:
                        lines.append(f"      {frame}")
                lines.append("")
    else:
        lines.append("  (thread dump unavailable)")

    # ── 4. ASSESSMENT ────────────────────────────────────────────────────
    lines.append("── ASSESSMENT ──────────────────────────────────────────────────")

    if len(warnings) >= 2 or (warnings and any("BLOCKED" in w for w in warnings)):
        assessment = "CRITICAL"
    elif warnings:
        assessment = "UNDER PRESSURE"
    else:
        assessment = "HEALTHY"

    reason = ", ".join(warnings) if warnings else "all systems nominal"
    lines.append(f"  Status: {assessment} — {reason}")
    lines.append(sep)

    output = "\n".join(lines)
    if len(output) > MAX_OUTPUT_CHARS:
        output = output[:MAX_OUTPUT_CHARS] + f"\n\n⚠️ Output truncated at {MAX_OUTPUT_CHARS} characters."
    return output
