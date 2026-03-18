# Apache Flink MCP Server

A Model Context Protocol (MCP) server implementation for Apache Flink that enables AI assistants and large language models to interact with Flink clusters through natural language interfaces. This server provides comprehensive tools for monitoring, managing, and analyzing Apache Flink streaming applications.

![alt text](assets/image.png)

## Overview

The Apache Flink MCP Server bridges the gap between AI assistants and Apache Flink clusters by providing a standardized MCP interface. It allows users to perform complex Flink operations through conversational AI, making stream processing management more accessible and intuitive.

## Configuration

Copy `config.yaml.example` to `config.yaml` and edit it before running the server:

```bash
cp config.yaml.example config.yaml
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `flink.url` | string | `http://localhost:8081` | Base URL of the Flink REST API |
| `flink.tls_verify` | boolean | `true` | Verify TLS certificates. Set to `false` for self-signed certs. |
| `server.max_output_chars` | integer | `50000` | Maximum characters returned by any tool. Longer responses are truncated. |

## Features

### 🎯 Core Capabilities
- **Cluster Monitoring**: Get real-time cluster information including jobs, slots, and TaskManagers
- **Job Management**: List, monitor, and analyze Flink job details and metrics
- **Exception Tracking**: Retrieve and analyze job exceptions for debugging
- **Resource Management**: Monitor TaskManager resources and JAR file deployments
- **Metrics Collection**: Access comprehensive job and cluster metrics

### 🔧 Available Tools:

1. `get_cluster_info` – Overview of the Flink cluster
2. `list_jobs` – List all Flink jobs with status
3. `get_job_details` – Comprehensive job details by ID
4. `get_job_exceptions` – Fetch job-level exceptions
5. `get_job_metrics` – Fetch metrics for a job
6. `list_taskmanagers` – List TaskManagers with resources
7. `list_jar_files` – List uploaded JAR files
8. `get_job_checkpoints` – Checkpoint history and counts for a job
9. `get_checkpoint_details` – Per-subtask breakdown for a specific checkpoint
10. `get_vertex_info(job_id, vertex_id, info_category)` – Vertex info by category: `backpressure`, `metrics`, `subtask_times`, `taskmanager_stats`, `accumulators`
11. `get_job_accumulators` – User-defined accumulators for a job
12. `get_jobmanager_metrics` – List or query JobManager metrics
13. `get_jobmanager_config` – Full effective cluster configuration
14. `get_jobmanager_environment` – JVM and environment info from the JobManager
15. `list_flink_logs` – List available log files on JobManager or TaskManager
16. `read_flink_logs` – Read a log file with optional tail/filter support
17. `list_job_ids` – Lightweight job-ID and status list
18. `get_job_plan` – Dataflow DAG with nodes, edges, and ship strategies
19. `get_job_checkpoint_config` – Active checkpoint configuration for a job
20. `get_vertex_details` – Full per-subtask breakdown for a vertex
21. `get_vertex_flamegraph` – CPU flame graph data for a vertex (Flink 1.17+)
22. `get_taskmanager_thread_dump` – JVM thread dump grouped by state
23. `get_cluster_config` – REST/web UI config (version, timezone, refresh)
24. `list_datasets` – Intermediate batch datasets on the cluster

---

### 🚀 Benefits
- **Natural Language Interface**: Interact with Flink using conversational AI
- **Real-time Monitoring**: Get instant insights into cluster and job status
- **Debugging Support**: Easily access exception logs and metrics
- **Resource Optimization**: Monitor resource usage across TaskManagers
- **Developer Productivity**: Reduce time spent navigating Flink Web UI

## Installation

### Prerequisites
- Apache Flink cluster (running and accessible)
- Python 3.8 or higher
- MCP-compatible client (Claude Desktop, Continue etc.)


### Client Configuration

#### Continue.dev
Add to your Continue configuration (`Flink-mcp-server.yaml`):
```yaml
name: Sample MCP
version: 0.0.1
schema: v1
mcpServers:
  - name: Flink MCP Server
    type: streamable-http
    url:   http://127.0.0.1:9090/mcp/ 
```

## Usage Examples

### Basic Cluster Monitoring
```
Human: What's the status of my Flink cluster?
AI: I'll check your Flink cluster status for you.
[Uses get_cluster_info tool to fetch cluster overview]
```

### Job Analysis
```
Human: Show me all running jobs and their performance metrics
AI: Let me get the current jobs and their metrics.
[Uses list_jobs and get_job_metrics tools]
```

### Troubleshooting
```
Human: My job with ID abc123 is failing. Can you help me debug it?
AI: I'll check the job details and any exceptions for job abc123.
[Uses get_job_details and get_job_exceptions tools]
```

### Resource Management
```
Human: How are my TaskManager resources being utilized?
AI: Let me check your TaskManager status and resource allocation.
[Uses list_taskmanagers tool]
```

## API Reference

### Available MCP Tools

#### `get_cluster_info`
**Description**: Fetch an overview of the Flink cluster including jobs, slots, and TaskManagers.
**Parameters**: None
**Returns**: Cluster overview with resource information

#### `list_jobs`
**Description**: List all current and recent Flink jobs with their status.
**Parameters**: None
**Returns**: List of jobs with status, start time, and duration

#### `get_job_details`
**Description**: Get detailed information about a specific Flink job.
**Parameters**:
- `job_id` (string, required): The unique identifier of the Flink job

#### `list_taskmanagers`
**Description**: List all registered TaskManagers in the cluster.
**Parameters**: None
**Returns**: List of TaskManagers with resource information

#### `get_job_exceptions`
**Description**: Fetch exceptions that occurred in the specified job.
**Parameters**:
- `job_id` (string, required): The unique identifier of the Flink job

#### `list_jar_files`
**Description**: List all uploaded JAR files in the Flink cluster.
**Parameters**: None
**Returns**: List of available JAR files

#### `get_job_metrics`
**Description**: Fetch selected useful metrics for a running Flink job.
**Parameters**:
- `job_id` (string, required): The unique identifier of the Flink job


---

#### `get_vertex_info`
**Description**: Retrieve information about a specific job vertex / operator.
**Parameters**:
- `job_id` (string, required): The Flink job ID
- `vertex_id` (string, required): The vertex (operator) ID
- `info_category` (string, required): One of `backpressure`, `metrics`, `subtask_times`, `taskmanager_stats`, `accumulators`

---

## Troubleshooting

### Common Issues

**Connection Failed**
- Verify Flink cluster is running and accessible
- Ensure network connectivity to Flink JobManager

**Permission Errors**
- Verify Flink REST API is enabled
- Check if authentication is required for your Flink setup


### Debug Mode
Enable detailed logging:
```bash
export LOG_LEVEL=DEBUG
python mcp_server.py
```

## Contributing

We welcome contributions! Please follow these steps:

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make your changes and add tests
5. Commit your changes: `git commit -m "Add feature"`
6. Push to your fork: `git push origin feature-name`
7. Create a Pull Request

### Development Guidelines
- Follow PEP 8 style guidelines
- Update documentation as needed
- Ensure backward compatibility

## License

This project is licensed under the [MIT License](LICENSE).

## Related Projects

- [Model Context Protocol](https://github.com/modelcontextprotocol) - The MCP specification
- [Apache Flink](https://github.com/apache/flink) - Apache Flink stream processing framework
- [MCP Kafka](https://github.com/Ashfaqbs/KafkaIQ) - MCP server for Confluent/Kafka
- [MCP Container](https://github.com/Ashfaqbs/ContainMind) - MCP server forContainer's


## Support

- **Issues**: [GitHub Issues](https://github.com/Ashfaqbs/apache-flink-mcp-server/issues)
- **Documentation**: [Project Wiki](https://github.com/Ashfaqbs/apache-flink-mcp-server/wiki)

- **Discussions**: [Project Discussions](https://github.com/Ashfaqbs/apache-flink-mcp-server/discussions/)

## Acknowledgments

- Apache Flink community for the excellent stream processing framework
- Model Context Protocol team for the standardized interface
- Contributors and users of this project

---

**Note**: This MCP server provides read-only access to Flink cluster information by default. For write operations, additional configuration and security considerations may be required.
