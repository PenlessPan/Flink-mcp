# Flink MCP Tool Outputs
**Cluster URL:** http://64.176.165.222:30081  
**Report Generated:** 2026-03-12 20:40 UTC+2  
**Tools Executed:** 22 (all tools except `send_email_notification`)

---

## Available Tools

| # | Tool | Description |
|---|------|-------------|
| 1 | `initialize_flink_connection` | Initialize connection to the Flink REST API |
| 2 | `get_connection_status` | Check if connection is initialized and get current URL |
| 3 | `get_cluster_info` | Fetch overview: jobs, slots, taskmanagers |
| 4 | `list_jobs` | List all current and recent Flink jobs with their status |
| 5 | `list_taskmanagers` | List all registered TaskManagers with resource info |
| 6 | `list_jar_files` | List all uploaded JARs in the cluster |
| 7 | `get_jobmanager_config` | Get the full effective cluster configuration |
| 8 | `get_jobmanager_environment` | Get JVM and environment information from the JobManager |
| 9 | `get_jobmanager_metrics` | Get metrics for the JobManager |
| 10 | `get_job_details` | Get comprehensive details of a specific Flink job |
| 11 | `get_job_exceptions` | Fetch exceptions that occurred in the specified job |
| 12 | `get_job_checkpoints` | Get checkpoint history and statistics for a Flink job |
| 13 | `get_job_metrics` | Fetch selected useful metrics for a running Flink job |
| 14 | `get_job_accumulators` | Get user-defined accumulators for a Flink job |
| 15 | `get_taskmanager_details` | Get comprehensive details about a specific TaskManager |
| 16 | `get_taskmanager_metrics` | Get specific metrics for a TaskManager |
| 17 | `get_checkpoint_details` | Get per-subtask breakdown for a specific checkpoint |
| 18 | `get_vertex_backpressure` | Get backpressure information for a specific operator/vertex |
| 19 | `get_vertex_metrics` | Get metrics for a specific operator/vertex |
| 20 | `get_vertex_subtask_times` | Get per-subtask timing information for a vertex/operator |
| 21 | `get_vertex_taskmanager_stats` | Get aggregated I/O and buffer stats per TaskManager for a vertex |
| 22 | `get_vertex_accumulators` | Get per-subtask accumulator values for a specific vertex |

---

## Tool Outputs

### 1. `initialize_flink_connection`
**Parameters:** `flink_url = http://64.176.165.222:30081`
```
✓ Successfully connected to Flink cluster at http://64.176.165.222:30081
```

---

### 2. `get_connection_status`
```
✓ Connected to: http://64.176.165.222:30081
```

---

### 3. `get_cluster_info`
```
Flink Cluster Info:
- TaskManagers: 1
- Slots Total: 4
- Slots Available: 4
- Jobs Running: 0
- Jobs Finished: 0
- Jobs Cancelled: 0
- Jobs Failed: 1
```

---

### 4. `list_jobs`
```
Flink Jobs Overview:
- ID: 8bfbc7e34bb364a64edf019f59ad936c | Name: Flink MCP Test Job | State: FAILED
```

---

### 5. `list_taskmanagers`
```
======================================================================
TASKMANAGERS OVERVIEW (1 TaskManager(s))
======================================================================

Cluster Capacity:
  Total Slots: 4
  Used Slots: 0 (0.0%)
  Free Slots: 4 (100.0%)

======================================================================
TaskManager #1
======================================================================

📋 BASIC INFO
  ID: flink-taskmanager-0.flink-taskmanager-headless.flink.svc.cluster.local:6122-aaa1d1
  Path: pekko.tcp://flink@flink-taskmanager-0.flink-taskmanager-headless.flink.svc.cluster.local:6122/user/rpc/taskmanager_0
  Data Port: 6121
  JMX Port: -1
  Last Heartbeat: 2026-03-12 20:39:48

🎰 SLOT ALLOCATION
  Total Slots: 4
  Used Slots: 0 (0.0%)
  Free Slots: 4 (100.0%)

💻 HARDWARE
  CPU Cores: 2
  Physical Memory: 4.00 GB
  Free Memory: 512.00 MB (12.5%)
  Managed Memory: 512.00 MB
  Used Memory: 3.50 GB (87.5%)

📊 CONFIGURED RESOURCES
  CPU Cores (slots): 4.0
  Task Heap Memory: 383.00 MB
  Task Off-Heap Memory: 0 B
  Managed Memory: 512.00 MB
  Network Memory: 128.00 MB

✅ AVAILABLE RESOURCES
  CPU Cores: 4.0
  Task Heap Memory: 383.00 MB
  Task Off-Heap Memory: 0 B
  Managed Memory: 512.00 MB
  Network Memory: 128.00 MB

🧠 MEMORY CONFIGURATION
  Framework Heap: 128.00 MB
  Framework Off-Heap: 128.00 MB
  Task Heap: 384.00 MB
  Task Off-Heap: 0 B
  Network Memory: 128.00 MB
  Managed Memory: 512.00 MB
  JVM Metaspace: 256.00 MB
  JVM Overhead: 192.00 MB
  Total Flink Memory: 1.25 GB
  Total Process Memory: 1.69 GB

📈 UTILIZATION ANALYSIS
  ✅ LOW slot utilization: 0.0%
  ⚡ MODERATE memory utilization: 87.5%
  ⚠️  WARNING: 4 slots configured but only 2 CPU cores available. Potential oversubscription!

💡 RECOMMENDATIONS
  ✅ Sufficient slot capacity available
```

---

### 6. `list_jar_files`
```
7e543e06-e06c-451b-9330-b6f62b3231fb_flink-test-job-1.0.jar - flink-test-job-1.0.jar
```

---

### 7. `get_jobmanager_config`
```
======================================================================
JOBMANAGER EFFECTIVE CONFIGURATION
======================================================================
Total entries: 38

[blob.*]
  blob.server.port = 6124

[env.*]
  env.java.opts.all = --add-exports=java.base/sun.net.util=ALL-UNNAMED --add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED

[execution.*]
  execution.checkpointing.externalized-checkpoint-retention = RETAIN_ON_CANCELLATION
  execution.checkpointing.interval = 60000
  execution.checkpointing.min-pause = 30000

[jobmanager.*]
  jobmanager.archive.fs.dir = s3://flink-job-history/completed-jobs
  jobmanager.bind-host = 0.0.0.0
  jobmanager.execution.failover-strategy = region
  jobmanager.memory.heap.size = 1073741824b
  jobmanager.memory.jvm-metaspace.size = 268435456b
  jobmanager.memory.jvm-overhead.max = 201326592b
  jobmanager.memory.jvm-overhead.min = 201326592b
  jobmanager.memory.off-heap.size = 134217728b
  jobmanager.memory.process.size = 1600m
  jobmanager.rpc.address = flink-jobmanager
  jobmanager.rpc.bind-port = 6123
  jobmanager.rpc.port = 6123

[jobstore.*]
  jobstore.expiration-time = 86400
  jobstore.max-capacity = 20

[metrics.*]
  metrics.reporter.prom.factory.class = org.apache.flink.metrics.prometheus.PrometheusReporterFactory
  metrics.reporter.prom.port = 9249

[parallelism.*]
  parallelism.default = 1

[query.*]
  query.server.port = 6125

[rest.*]
  rest.address = flink-jobmanager
  rest.bind-address = 0.0.0.0
  rest.port = 8081

[s3.*]
  s3.access-key = minioadmin
  s3.endpoint = http://minio.minio.svc.cluster.local:9000
  s3.path.style.access = true
  s3.secret-key = ******

[state.*]
  state.backend = hashmap
  state.checkpoints.dir = s3://flink-checkpoints/checkpoints
  state.savepoints.dir = s3://flink-savepoints/savepoints

[taskmanager.*]
  taskmanager.bind-host = localhost
  taskmanager.host = localhost
  taskmanager.memory.process.size = 1728m
  taskmanager.numberOfTaskSlots = 4

[web.*]
  web.tmpdir = /tmp/flink-web-fa8e1aed-c356-4095-8730-9e7f941962bd
```

---

### 8. `get_jobmanager_environment`
```
======================================================================
JOBMANAGER ENVIRONMENT
======================================================================

JVM:
  Version:    OpenJDK 64-Bit Server VM - BellSoft - 11/11.0.26+9-LTS
  Arch:       amd64
  Options:    ['-Xmx1073741824', '-Xms1073741824', '-XX:MaxMetaspaceSize=268435456',
               '-XX:+IgnoreUnrecognizedVMOptions',
               '--add-exports=java.base/sun.net.util=ALL-UNNAMED', ...
               '-Dlog.file=/opt/bitnami/flink/log/flink--standalonesession-0-flink-jobmanager-f9488899c-w255j.log',
               '-Dlog4j.configuration=file:/opt/bitnami/flink/conf/log4j-console.properties',
               '-Dlogback.configurationFile=file:/opt/bitnami/flink/conf/logback-console.xml']
```

---

### 9. `get_jobmanager_metrics`
```
======================================================================
AVAILABLE JOBMANAGER METRICS
======================================================================

Total metrics available: 33

Other:
  - numRegisteredTaskManagers
  - numRunningJobs
  - taskSlotsAvailable
  - taskSlotsTotal
Status.JVM.CPU:
  - Status.JVM.CPU.Load
  - Status.JVM.CPU.Time
Status.JVM.ClassLoader:
  - Status.JVM.ClassLoader.ClassesLoaded
  - Status.JVM.ClassLoader.ClassesUnloaded
Status.JVM.GarbageCollector:
  - Status.JVM.GarbageCollector.All.Count
  - Status.JVM.GarbageCollector.All.Time
  - Status.JVM.GarbageCollector.All.TimeMsPerSecond
  - Status.JVM.GarbageCollector.G1_Old_Generation.Count
  - Status.JVM.GarbageCollector.G1_Old_Generation.Time
  - Status.JVM.GarbageCollector.G1_Old_Generation.TimeMsPerSecond
  - Status.JVM.GarbageCollector.G1_Young_Generation.Count
  - Status.JVM.GarbageCollector.G1_Young_Generation.Time
  - Status.JVM.GarbageCollector.G1_Young_Generation.TimeMsPerSecond
Status.JVM.Memory:
  - Status.JVM.Memory.Direct.Count / MemoryUsed / TotalCapacity
  - Status.JVM.Memory.Heap.Committed / Max / Used
  - Status.JVM.Memory.Mapped.Count / MemoryUsed / TotalCapacity
  - Status.JVM.Memory.Metaspace.Committed / Max / Used
  - Status.JVM.Memory.NonHeap.Committed / Max / Used
Status.JVM.Threads:
  - Status.JVM.Threads.Count

💡 TIP: Pass metric_names as a comma-separated string to query specific values.
```

---

### 10. `get_job_details`
**Parameters:** `job_id = 8bfbc7e34bb364a64edf019f59ad936c`
```
============================================================
JOB OVERVIEW
============================================================
Job ID: 8bfbc7e34bb364a64edf019f59ad936c
Name: Flink MCP Test Job
State: FAILED
Type: STREAMING

Start Time: 2026-03-12 19:24:38
End Time:   2026-03-12 19:44:39
Duration:   20.01m (1200s)

State Transitions:
  - INITIALIZING : 2026-03-12 19:24:38
  - CREATED      : 2026-03-12 19:24:39
  - RUNNING      : 2026-03-12 19:42:54
  - RESTARTING   : 2026-03-12 19:42:49
  - FAILING      : 2026-03-12 19:44:39
  - FAILED       : 2026-03-12 19:44:39

============================================================
CONFIGURATION
============================================================
Execution Mode: PIPELINED
Restart Strategy: Fixed delay (PT5S), max 10 attempts
Job Parallelism: 2
User Config: error-every-n=2000, parallelism=2, slowdown-ms=50

============================================================
VERTICES (3 operators)
============================================================

[1] Source: Word Generator Source -> Tokenizer + Accumulators -> Slow Map (Backpressure) -> Periodic Exception Injector
    ID: ae55b1e6fd610bc33dc60b54039a3fd7
    Status: FAILED | Parallelism: 2 | Duration: 1.74m
    Tasks: CANCELED: 1, FAILED: 1
    Write: 3,998 records, 60.99 KB | Throughput: 38.34 records/sec

[2] Keyed Word Count (Stateful)
    ID: c91d50a44b1fea7850d4d711755344d9
    Status: CANCELED | Parallelism: 2 | Duration: 1.74m
    Tasks: CANCELED: 2
    Read: 3,996 records, 67.72 KB | Write: 82 records, 1.52 KB
    Idle Time: 3.47m | Busy Time: 0.34s | Throughput: 0.79 records/sec

[3] Format Output -> Sink: Print Sink
    ID: 4a403148d28c9b400685a3dcd33bb736
    Status: CANCELED | Parallelism: 1 | Duration: 1.74m
    Tasks: CANCELED: 1
    Read: 82 records, 2.43 KB | Idle Time: 1.74m | Busy Time: 0.02s

============================================================
PERFORMANCE INSIGHTS
============================================================
ℹ️  High idle time in 'Keyed Word Count (Stateful)': 199.6%
ℹ️  High idle time in 'Format Output -> Sink: Print Sink': 99.9%
❌ Failed tasks in source chain: 1
📊 Total parallelism across all operators: 5
```

---

### 11. `get_job_exceptions`
**Parameters:** `job_id = 8bfbc7e34bb364a64edf019f59ad936c`
```
================================================================================
JOB EXCEPTIONS REPORT
================================================================================

ROOT CAUSE:
org.apache.flink.runtime.JobException: Recovery is suppressed by
  FixedDelayRestartBackoffTimeStrategy(maxNumberRestartAttempts=10, backoffTimeMS=5000)

Caused by: java.lang.RuntimeException: Simulated periodic failure at record #2000 in subtask 0
    at com.flinktest.FlinkTestJob$PeriodicExceptionMap.map(FlinkTestJob.java:302)
    ...
    at com.flinktest.FlinkTestJob$WordGeneratorSource.run(FlinkTestJob.java:184)

EXCEPTION DETAILS (1 exception):
  Task: Source: Word Generator Source -> ... -> Periodic Exception Injector (1/2)
  TaskManager: flink-taskmanager-0...flink.svc.cluster.local:6122-aaa1d1
  Location: flink-taskmanager-0...flink.svc.cluster.local:6121
  Timestamp: 1773337479251

EXCEPTION HISTORY (11 entries):
  #1  org.apache.flink.runtime.JobException  — 2026-03-12 19:44:39
  #2  java.lang.RuntimeException             — 2026-03-12 19:42:49 (~110s earlier)
  #3  java.lang.RuntimeException             — ~110s earlier
  ... (pattern repeats for all 10 restart cycles)
  #11 java.lang.RuntimeException             — 2026-03-12 19:23:06

SUMMARY: Total Exceptions: 1 | History Entries: 11 | Truncated: False
================================================================================
```

---

### 12. `get_job_checkpoints`
**Parameters:** `job_id = 8bfbc7e34bb364a64edf019f59ad936c`
```
======================================================================
CHECKPOINT OVERVIEW — Job 8bfbc7e34bb364a64edf019f59ad936c
======================================================================

Counts:
  Completed:   110
  Failed:      0
  In-Progress: 0
  Restored:    10
  Total:       110

Latest Completed Checkpoint:
  ID:            110
  Status:        COMPLETED
  Duration:      1.17s
  State Size:    5.23 KB
  Trigger Time:  2026-03-12 19:44:30
  External Path: s3://flink-checkpoints/checkpoints/8bfbc7e34bb364a64edf019f59ad936c/chk-110

Checkpoint History (last 10):
  ID   Status     Trigger Time            Duration   State Size
  110  COMPLETED  2026-03-12 19:44:30     1.17s      5.23 KB
  109  COMPLETED  2026-03-12 19:44:20     0.74s      5.23 KB
  108  COMPLETED  2026-03-12 19:44:10     0.31s      5.23 KB
  107  COMPLETED  2026-03-12 19:44:00     0.88s      5.23 KB
  106  COMPLETED  2026-03-12 19:43:50     2.01s      5.23 KB
  105  COMPLETED  2026-03-12 19:43:40     1.62s      5.23 KB
  104  COMPLETED  2026-03-12 19:43:30     1.18s      5.23 KB
  103  COMPLETED  2026-03-12 19:43:20     0.73s      5.23 KB
  102  COMPLETED  2026-03-12 19:43:10     0.29s      5.23 KB
  101  COMPLETED  2026-03-12 19:43:00     2.47s      5.23 KB
======================================================================
```

---

### 13. `get_job_metrics`
**Parameters:** `job_id = 8bfbc7e34bb364a64edf019f59ad936c`
```
=== Job Runtime ===
Uptime:    1.24m (75s)
Running:   1.24m (75s)

=== Stability ===
Restarts:  7 (full: 7)

=== Checkpoints ===
Total: 77 | Completed: 77 | Failed: 0 | In-Progress: 0
Success Ratio: 100.00% | Failure Ratio: 0.00%
Last Completed ID: 77
Last Duration:     2.34s
Last Size:         5.23 KB
Last Restored At:  2026-03-12 19:37:27
External Path:     s3://flink-checkpoints/checkpoints/8bfbc7e34bb364a64edf019f59ad936c/chk-77

=== Hints ===
- Multiple restarts observed → review TaskManager/JobManager logs for exceptions or OOMs.
```

---

### 14. `get_job_accumulators` ✅ *(fixed in this session)*
**Parameters:** `job_id = 8bfbc7e34bb364a64edf019f59ad936c`
```
======================================================================
JOB ACCUMULATORS — 8bfbc7e34bb364a64edf019f59ad936c
======================================================================

Total accumulators: 2

  Name:  records-processed
  Type:  LongCounter
  Value: 4000

  Name:  errors-skipped
  Type:  LongCounter
  Value: 0

======================================================================
```

---

### 15. `get_taskmanager_details`
**Parameters:** `taskmanager_id = flink-taskmanager-0...flink.svc.cluster.local:6122-aaa1d1`
```
================================================================================
TASKMANAGER DETAILS: flink-taskmanager-0...6122-aaa1d1
================================================================================

📋 BASIC INFORMATION
  ID: flink-taskmanager-0.flink-taskmanager-headless.flink.svc.cluster.local:6122-aaa1d1
  Data Port: 6121 | JMX Port: -1
  Last Heartbeat: 2026-03-12 20:39:58

🎰 SLOT INFORMATION
  Total Slots: 4 | Used: 0 (0.0%) | Free: 4 (100.0%)

💻 HARDWARE
  CPU Cores: 2 | Physical Memory: 4.00 GB
  Free Memory: 512.00 MB (12.5%) | Managed Memory: 512.00 MB

🧠 MEMORY CONFIGURATION
  Framework Heap: 128.00 MB | Framework Off-Heap: 128.00 MB
  Task Heap: 384.00 MB     | Task Off-Heap: 0 B
  Network Memory: 128.00 MB | Managed Memory: 512.00 MB
  JVM Metaspace: 256.00 MB  | JVM Overhead: 192.00 MB
  Total Flink Memory: 1.25 GB | Total Process Memory: 1.69 GB

📈 REAL-TIME METRICS
  Heap Memory:    277.75 MB / 512.00 MB (54.2%)
  Non-Heap:       106.19 MB used
  Direct Memory:  144.42 MB / 144.43 MB (100%)
  Network Buffers: 4,096 / 4,096 available
  GC (All):       13 collections, 180ms total
  GC (Young Gen): 13 collections, 180ms total
  GC (Old Gen):   0 collections, 0ms total
================================================================================
```

---

### 16. `get_taskmanager_metrics`
**Parameters:** `taskmanager_id = flink-taskmanager-0...6122-aaa1d1`
```
======================================================================
AVAILABLE METRICS (40 total)
======================================================================

Status.Flink.Memory:
  - Status.Flink.Memory.Managed.Total / Used

Status.JVM.CPU:
  - Status.JVM.CPU.Load / Time

Status.JVM.ClassLoader:
  - Status.JVM.ClassLoader.ClassesLoaded / ClassesUnloaded

Status.JVM.GarbageCollector:
  - Status.JVM.GarbageCollector.All.Count / Time / TimeMsPerSecond
  - Status.JVM.GarbageCollector.G1_Old_Generation.Count / Time / TimeMsPerSecond
  - Status.JVM.GarbageCollector.G1_Young_Generation.Count / Time / TimeMsPerSecond

Status.JVM.Memory:
  - Status.JVM.Memory.Direct.Count / MemoryUsed / TotalCapacity
  - Status.JVM.Memory.Heap.Committed / Max / Used
  - Status.JVM.Memory.Mapped.Count / MemoryUsed / TotalCapacity
  - Status.JVM.Memory.Metaspace.Committed / Max / Used
  - Status.JVM.Memory.NonHeap.Committed / Max / Used

Status.JVM.Threads:
  - Status.JVM.Threads.Count

Status.Network:
  - Status.Network.AvailableMemorySegments
  - Status.Network.TotalMemorySegments

Status.Shuffle.Netty:
  - Status.Shuffle.Netty.AvailableMemory / AvailableMemorySegments
  - Status.Shuffle.Netty.RequestedMemoryUsage
  - Status.Shuffle.Netty.TotalMemory / TotalMemorySegments
  - Status.Shuffle.Netty.UsedMemory / UsedMemorySegments

💡 TIP: metric_names="Status.JVM.CPU.Load,Status.JVM.Memory.Heap.Used"
======================================================================
```

---

### 17. `get_checkpoint_details`
**Parameters:** `job_id = 8bfbc7e34bb364a64edf019f59ad936c`, `checkpoint_id = 110`
```
======================================================================
CHECKPOINT DETAILS — Job 8bfbc7e34bb364a64edf019f59ad936c / Checkpoint 110
======================================================================
  Status:       COMPLETED
  Duration:     1.17s
  State Size:   5.23 KB
  Trigger Time: 2026-03-12 19:44:30
  Ext. Path:    s3://flink-checkpoints/checkpoints/8bfbc7e34bb364a64edf019f59ad936c/chk-110

Per-Operator Breakdown (3 operator(s)):
  (No subtask data — job was already in FAILED state at time of query)
======================================================================
```

---

### 18. `get_vertex_backpressure`
**Parameters:** `job_id = 8bfbc7e34bb364a64edf019f59ad936c`, `vertex_id = ae55b1e6fd610bc33dc60b54039a3fd7`
```
======================================================================
BACKPRESSURE ANALYSIS
======================================================================
Job ID: 8bfbc7e34bb364a64edf019f59ad936c
Vertex ID: ae55b1e6fd610bc33dc60b54039a3fd7

📊 OVERALL STATUS
  Status: ✅ OK — No issues
  Measured At: 2026-03-12 20:40:08

📋 PER-SUBTASK BREAKDOWN (2 subtasks)
  Subtask #0: ✅ OK | Backpressure: 0.00% | Idle: 0.00% | Busy: 0.00%
  Subtask #1: ✅ OK | Backpressure: 0.00% | Idle: 0.00% | Busy: 0.00%

  (All zeros — job is FAILED, no active processing)
======================================================================
```

---

### 19. `get_vertex_metrics`
**Parameters:** `job_id = 8bfbc7e34bb364a64edf019f59ad936c`, `vertex_id = ae55b1e6fd610bc33dc60b54039a3fd7`
```
214 metrics available across 2 subtasks (0 and 1), including:

Per-operator metrics (x2 subtasks each):
  - {n}.Source__Word_Generator_Source.numRecordsIn/Out, numBytesIn/Out (+ per-second variants)
  - {n}.Tokenizer_+_Accumulators.numRecordsIn/Out, numBytesIn/Out
  - {n}.Slow_Map_(Backpressure).numRecordsIn/Out, numBytesIn/Out
  - {n}.Periodic_Exception_Injector.numRecordsIn/Out, numBytesIn/Out

Buffer metrics per subtask:
  - {n}.buffers.inPoolUsage, inputExclusiveBuffersUsage, inputFloatingBuffersUsage
  - {n}.buffers.outPoolUsage, outputQueueLength, outputQueueSize
  - {n}.Shuffle.Netty.Input/Output.Buffers.*

Task timing:
  - {n}.accumulateBackPressuredTimeMs, accumulateBusyTimeMs, accumulateIdleTimeMs
  - {n}.mailboxLatencyMs_max/mean/min/p75/p90/p95/p99/p999/stddev
  - {n}.checkpointStartDelayNanos, initializationTime

(n = subtask index: 0 or 1)
```

---

### 20. `get_vertex_subtask_times`
**Parameters:** `job_id = 8bfbc7e34bb364a64edf019f59ad936c`, `vertex_id = ae55b1e6fd610bc33dc60b54039a3fd7`
```
======================================================================
SUBTASK TIMING — Source: Word Generator Source -> ... -> Periodic Exception Injector
======================================================================

  Subtask #0  (flink-taskmanager-0)  — Total: 1.74m (104s)
    CREATED      : 2026-03-12 19:42:54
    SCHEDULED    : 2026-03-12 19:42:54
    DEPLOYING    : 2026-03-12 19:42:54
    INITIALIZING : 2026-03-12 19:42:55
    RUNNING      : 2026-03-12 19:42:55
    FAILED       : 2026-03-12 19:44:39

  Subtask #1  (flink-taskmanager-0)  — Total: 1.74m (104s)
    CREATED      : 2026-03-12 19:42:54
    SCHEDULED    : 2026-03-12 19:42:54
    DEPLOYING    : 2026-03-12 19:42:54
    INITIALIZING : 2026-03-12 19:42:55
    RUNNING      : 2026-03-12 19:42:55
    CANCELING    : 2026-03-12 19:44:39
    CANCELED     : 2026-03-12 19:44:39

  ✅ No data skew detected (both subtasks ran for identical durations)
======================================================================
```

---

### 21. `get_vertex_taskmanager_stats`
**Parameters:** `job_id = 8bfbc7e34bb364a64edf019f59ad936c`, `vertex_id = ae55b1e6fd610bc33dc60b54039a3fd7`
```
======================================================================
TASKMANAGER STATS — Source: Word Generator Source -> ... -> Periodic Exception Injector
======================================================================

  Host:          flink-taskmanager-0:6121
  Status:        FAILED
  Records In:    0
  Records Out:   3,998
  Bytes In:      0 B
  Bytes Out:     60.99 KB
  Busy Time:     N/A
  Idle Time:     N/A
  Backpressure:  N/A
  Start Time:    2026-03-12 19:42:54
  End Time:      2026-03-12 19:44:39
  Duration:      1.74m (104s)
======================================================================
```

---

### 22. `get_vertex_accumulators`
**Parameters:** `job_id = 8bfbc7e34bb364a64edf019f59ad936c`, `vertex_id = ae55b1e6fd610bc33dc60b54039a3fd7`
```
No accumulator data found for vertex ae55b1e6fd610bc33dc60b54039a3fd7
in job 8bfbc7e34bb364a64edf019f59ad936c.
```
*(Accumulators are reported at job level via `get_job_accumulators`, not per-vertex in this job.)*

---

## Summary

| Category | Detail |
|---|---|
| **Cluster** | 1 TaskManager, 4 slots (all idle), 2 physical CPU cores |
| **Job** | "Flink MCP Test Job" — FAILED after 20 min, 10 restarts exhausted |
| **Root Cause** | `PeriodicExceptionMap` throws `RuntimeException` every 2,000 records (intentional test failure) |
| **Checkpoints** | 110 completed, 0 failed, stored in MinIO S3 (`s3://flink-checkpoints`) |
| **Accumulators** | `records-processed=4000`, `errors-skipped=0` |
| **State Backend** | `hashmap` with S3 checkpoints and savepoints |
| **Bug Fixed** | `get_job_accumulators` was crashing with `'list' object has no attribute 'get'` — now fixed |
