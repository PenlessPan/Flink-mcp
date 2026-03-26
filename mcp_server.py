import logging
import sys

from code.app import mcp

# Import all tool modules to register their @mcp.tool() decorators
import code.connection  # noqa: F401
import code.cluster_tools  # noqa: F401
import code.job_tools  # noqa: F401
import code.checkpoint_tools  # noqa: F401
import code.vertex_tools  # noqa: F401
import code.taskmanager_tools  # noqa: F401
import code.jobmanager_tools  # noqa: F401
import code.log_tools  # noqa: F401

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("flink-mcp-server")


def main():
    try:
        logger.info("Starting Flink MCP server...")
        logger.info("=" * 60)
        logger.info("Available tools:")
        logger.info("-" * 60)
        logger.info("  🔌 initialize_flink_connection: Connect to a Flink cluster by URL")
        logger.info("  📡 get_connection_status: Check current connection status")
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
