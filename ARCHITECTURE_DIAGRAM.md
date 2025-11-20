# Phase 2 Architecture Diagram

## System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                          User Interface                              │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │  Frontend (React + Nginx) - pratik9634/frontend:cache         │ │
│  │                                                                 │ │
│  │  ┌─────────────────┐  ┌────────────────────────────────────┐  │ │
│  │  │ Cache Toggle    │  │      Metrics Panel                 │  │ │
│  │  │ ☑ Enable cache  │  │  ┌──────────┬──────────┬─────────┐ │  │ │
│  │  │                 │  │  │ Cached   │Non-Cache │Last Run │ │  │ │
│  │  └─────────────────┘  │  │   12     │    8     │ 5/5 ok  │ │  │ │
│  │                       │  │Avg:15.2s │Avg:28.5s │0 failed │ │  │ │
│  │  ┌─────────────────┐  │  └──────────┴──────────┴─────────┘ │  │ │
│  │  │ YAML Editor     │  │  [Detailed Metrics Table...]       │  │ │
│  │  │ dagName: test   │  │                                     │  │ │
│  │  │ tasks: [...]    │  └────────────────────────────────────┘  │ │
│  │  └─────────────────┘                                          │ │
│  │           │                                                    │ │
│  │           ▼                                                    │ │
│  │  POST /api/v1/dags                                            │ │
│  │  { "dagName": "test", "useCache": true, "tasks": [...] }     │ │
│  └────────────────────────────────────────────────────────────────┘ │
└──────────────────────────┬────────────────────────────────────────┘
                           │
                           │ HTTP (Nginx Proxy)
                           │
┌──────────────────────────▼────────────────────────────────────────┐
│             Scheduler Service (Spring Boot)                        │
│              pratik9634/scheduler-service:cache                    │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │ DagController                                               │  │
│  │  • POST /api/v1/dags        → processNewDagSubmission()   │  │
│  │  • GET /api/v1/dags/{id}    → getDagStatus()              │  │
│  │  • GET /api/v1/dags/{name}/metrics → getDagRunMetrics()   │  │
│  │  • GET /api/v1/system/status → getSystemStatus()          │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                                 │                                   │
│  ┌──────────────────────────────▼──────────────────────────────┐  │
│  │ OrchestratorService                                         │  │
│  │                                                             │  │
│  │  1. Store DAG definition in Redis                          │  │
│  │  2. Store useCache flag → dag:{dagId}:useCache             │  │
│  │  3. Initialize metrics → dag:{dagId}:metrics               │  │
│  │     - dagId, dagName, startTime, cacheEnabled              │  │
│  │     - taskCount, taskSucceeded, taskFailed                 │  │
│  │     - cacheHitTasks, cacheMissTasks                        │  │
│  │  4. Track in history → dagRuns:{dagName} list              │  │
│  │  5. Evaluate DAG and dispatch ready tasks                  │  │
│  │                                                             │  │
│  │  For each task:                                            │  │
│  │    → createK8sJobDefinition(useCache)                      │  │
│  │    → Submit Job to Kubernetes                              │  │
│  └─────────────────────────────────────────────────────────────┘  │
└──────────────────────────┬────────────────────────────────────────┘
                           │
                           │ Kubernetes API
                           │
┌──────────────────────────▼────────────────────────────────────────┐
│                   Kubernetes Cluster                               │
│                                                                     │
│  ┌────────────────────────────────────────────────────────────┐   │
│  │ DaemonSet: helios-cache-prep                               │   │
│  │ (Runs on EVERY node)                                       │   │
│  │                                                             │   │
│  │  ┌──────┐  ┌──────┐  ┌──────┐                             │   │
│  │  │Node 1│  │Node 2│  │Node 3│                             │   │
│  │  │      │  │      │  │      │                             │   │
│  │  │ /var/helios/cache/                                     │   │
│  │  │   ├── dag-abc/                                         │   │
│  │  │   │   ├── data/                                        │   │
│  │  │   │   │   └── file.txt                                 │   │
│  │  │   │   └── model/                                       │   │
│  │  │   │       └── weights.pkl                              │   │
│  │  │   └── dag-def/                                         │   │
│  │  │       └── ...                                          │   │
│  │  └──────┘  └──────┘  └──────┘                             │   │
│  └────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  ┌────────────────────────────────────────────────────────────┐   │
│  │ Job: helios-task-process-xyz (example)                     │   │
│  │                                                             │   │
│  │  Volumes:                                                  │   │
│  │    • artifacts-workdir (emptyDir) → /artifacts            │   │
│  │    • artifact-cache (hostPath) → /cache                   │   │
│  │                                                             │   │
│  │  ┌──────────────────────────────────────────────────────┐ │   │
│  │  │ Init Container: smart-downloader                     │ │   │
│  │  │                                                       │ │   │
│  │  │  image: alpine:3.19                                  │ │   │
│  │  │                                                       │ │   │
│  │  │  Script Logic:                                       │ │   │
│  │  │  ┌────────────────────────────────────────────────┐ │ │   │
│  │  │  │ for each input artifact:                       │ │ │   │
│  │  │  │                                                 │ │ │   │
│  │  │  │   cachePath = /cache/{dagId}/{name}/{file}     │ │ │   │
│  │  │  │   workspacePath = /artifacts/{name}            │ │ │   │
│  │  │  │                                                 │ │ │   │
│  │  │  │   if HELIOS_CACHE_ENABLED == true:            │ │ │   │
│  │  │  │     if file exists in cachePath:              │ │ │   │
│  │  │  │       ✅ CACHE HIT                            │ │ │   │
│  │  │  │       cp cachePath → workspacePath            │ │ │   │
│  │  │  │       continue                                 │ │ │   │
│  │  │  │                                                 │ │ │   │
│  │  │  │   ❌ CACHE MISS                               │ │ │   │
│  │  │  │   wget from MinIO → tmpFile                   │ │ │   │
│  │  │  │   mv tmpFile → cachePath                      │ │ │   │
│  │  │  │   cp cachePath → workspacePath                │ │ │   │
│  │  │  └────────────────────────────────────────────────┘ │ │   │
│  │  │                                                       │ │   │
│  │  │  Environment Variables:                              │ │   │
│  │  │    HELIOS_CACHE_ENABLED=true                         │ │   │
│  │  │    HELIOS_CACHE_DIR=/cache                           │ │   │
│  │  │    HELIOS_ARTIFACTS_DIR=/artifacts                   │ │   │
│  │  │    HELIOS_INPUT_ARTIFACTS=data=dags/.../data.txt     │ │   │
│  │  └──────────────────────────────────────────────────────┘ │   │
│  │                                                             │   │
│  │  ┌──────────────────────────────────────────────────────┐ │   │
│  │  │ Main Container: task-executor                        │ │   │
│  │  │                                                       │ │   │
│  │  │  image: ubuntu:latest (or user-specified)           │ │   │
│  │  │                                                       │ │   │
│  │  │  Artifacts available at:                            │ │   │
│  │  │    /artifacts/data/data.txt                         │ │   │
│  │  │    /artifacts/model/weights.pkl                     │ │   │
│  │  │                                                       │ │   │
│  │  │  Command executes user task...                      │ │   │
│  │  │                                                       │ │   │
│  │  │  On completion:                                     │ │   │
│  │  │    Logs parsed for "[HELIOS] Cache hit/miss"       │ │   │
│  │  └──────────────────────────────────────────────────────┘ │   │
│  └────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  ┌────────────────────────────────────────────────────────────┐   │
│  │ Job Watcher (in scheduler-service)                         │   │
│  │                                                             │   │
│  │  SharedInformerFactory watches Jobs with label:           │   │
│  │    app=helios-task                                         │   │
│  │                                                             │   │
│  │  On Job completion:                                        │   │
│  │    1. Fetch pod logs                                       │   │
│  │    2. Parse for cache hit/miss                            │   │
│  │    3. Update Redis metrics                                │   │
│  │    4. Mark task as SUCCEEDED/FAILED                       │   │
│  │    5. Evaluate DAG for next tasks                         │   │
│  │    6. Finalize metrics if all tasks complete             │   │
│  └────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                        Redis Storage                             │
│                                                                   │
│  Keys Structure:                                                 │
│                                                                   │
│  dag:{dagId}:definition           → DAG YAML as JSON            │
│  dag:{dagId}:useCache             → "true" or "false"           │
│  dag:{dagId}:task:{name}:status   → "PENDING|SUCCEEDED|FAILED"  │
│  dag:{dagId}:task:{name}:logs     → JSON array of log lines     │
│  dag:{dagId}:task:{name}:attempts → attempt counter             │
│                                                                   │
│  dag:{dagId}:metrics (Hash):                                    │
│    - dagId                         → "dag-abc123"               │
│    - dagName                       → "my-workflow"              │
│    - startTime                     → "1700000000000"            │
│    - endTime                       → "1700001000000"            │
│    - durationMs                    → "1000000"                  │
│    - status                        → "SUCCEEDED|FAILED"         │
│    - cacheEnabled                  → "true|false"               │
│    - cacheHit                      → "Hit|Partial|Miss|N/A"    │
│    - taskCount                     → "5"                        │
│    - taskSucceeded                 → "5"                        │
│    - taskFailed                    → "0"                        │
│    - taskUpstreamFailed            → "0"                        │
│    - cacheHitTasks                 → "3"                        │
│    - cacheMissTasks                → "2"                        │
│    - nodes                         → "node1,node2"              │
│                                                                   │
│  dagRuns:{dagName} (List):                                      │
│    [0] dag-abc123                                                │
│    [1] dag-def456                                                │
│    [2] dag-ghi789                                                │
│    ... (up to 100 most recent)                                  │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                      MinIO (Object Storage)                      │
│                                                                   │
│  Bucket: helios-artifacts                                       │
│                                                                   │
│  dags/                                                           │
│    ├── dag-abc123/                                              │
│    │   ├── tasks/                                               │
│    │   │   ├── generate-data/                                   │
│    │   │   │   └── outputs/                                     │
│    │   │   │       └── data.txt                                 │
│    │   │   └── train-model/                                     │
│    │   │       └── outputs/                                     │
│    │   │           └── weights.pkl                              │
│    │   └── ...                                                  │
│    └── dag-def456/                                              │
│        └── ...                                                   │
└─────────────────────────────────────────────────────────────────┘
```

## Cache Flow Diagram

```
┌────────────────────────────────────────────────────────────────┐
│                    Task Execution Flow                          │
└────────────────────────────────────────────────────────────────┘

User Submits DAG
      │
      ▼
  ┌──────────┐
  │useCache? │
  └────┬─────┘
       │
   Yes │ No
       │  └──────────────────────────────────────┐
       ▼                                          ▼
┌──────────────────┐                    ┌──────────────────┐
│ Init Container   │                    │ Init Container   │
│ CACHE ENABLED    │                    │ CACHE DISABLED   │
└────────┬─────────┘                    └────────┬─────────┘
         │                                        │
         ▼                                        ▼
  ┌─────────────┐                        ┌──────────────┐
  │Check cache? │                        │ Skip cache   │
  └──────┬──────┘                        └──────┬───────┘
         │                                       │
    ┌────┴────┐                                  │
    │         │                                  │
 Exists   Not Exists                             │
    │         │                                  │
    ▼         ▼                                  ▼
┌─────┐   ┌─────────┐                   ┌──────────────┐
│ HIT │   │  MISS   │                   │Download from │
└──┬──┘   └────┬────┘                   │    MinIO     │
   │           │                         └──────┬───────┘
   │           ▼                                │
   │     ┌──────────┐                           │
   │     │Download  │                           │
   │     │from MinIO│                           │
   │     └────┬─────┘                           │
   │          │                                 │
   │          ▼                                 │
   │     ┌──────────┐                           │
   │     │Save to   │                           │
   │     │ cache    │                           │
   │     └────┬─────┘                           │
   │          │                                 │
   └────┬─────┘                                 │
        │                                       │
        ▼                                       ▼
   ┌──────────┐                          ┌──────────┐
   │Copy to   │                          │Copy to   │
   │workspace │                          │workspace │
   └────┬─────┘                          └────┬─────┘
        │                                      │
        └──────────────┬───────────────────────┘
                       │
                       ▼
              ┌─────────────────┐
              │  Main Container │
              │  executes task  │
              └────────┬────────┘
                       │
                       ▼
              ┌─────────────────┐
              │ Job completes   │
              └────────┬────────┘
                       │
                       ▼
              ┌─────────────────┐
              │ Logs parsed for │
              │ cache indicators│
              └────────┬────────┘
                       │
                       ▼
              ┌─────────────────┐
              │ Metrics updated │
              │  in Redis       │
              └─────────────────┘
```

## Metrics Flow

```
DAG Submission
      │
      ▼
Initialize Metrics
  - startTime = now
  - status = RUNNING
  - cacheEnabled = useCache
  - taskCount = tasks.length
  - taskSucceeded = 0
  - taskFailed = 0
  - cacheHitTasks = 0
  - cacheMissTasks = 0
      │
      ▼
Task Execution Loop
      │
      ├──► Task 1 completes
      │      └──► Parse logs for cache hit/miss
      │      └──► Increment cacheHitTasks or cacheMissTasks
      │      └──► Increment taskSucceeded or taskFailed
      │
      ├──► Task 2 completes
      │      └──► ... same as above
      │
      ├──► Task N completes
      │      └──► ... same as above
      │
      ▼
Check if all tasks terminal
      │
      ▼
Finalize Metrics
  - endTime = now
  - durationMs = endTime - startTime
  - status = all succeeded ? SUCCEEDED : FAILED
  - cacheHit = determine from hit/miss counts
      │
      ▼
Store in Redis
  - dag:{dagId}:metrics hash
  - dagRuns:{dagName} list
      │
      ▼
Available via API
  GET /api/v1/dags/{dagName}/metrics
      │
      ▼
Displayed in UI
  - Summary cards
  - Metrics table
```

## Component Interactions

```
Frontend                Scheduler           Kubernetes          Redis
   │                       │                    │                 │
   │ POST /api/v1/dags     │                    │                 │
   ├──────────────────────►│                    │                 │
   │                       │ Store DAG          │                 │
   │                       ├────────────────────┼────────────────►│
   │                       │                    │                 │
   │                       │ Create Job         │                 │
   │                       ├───────────────────►│                 │
   │                       │                    │                 │
   │                       │                    │ Init Container  │
   │                       │                    │ checks cache    │
   │                       │                    │                 │
   │                       │                    │ Main Container  │
   │                       │                    │ runs task       │
   │                       │                    │                 │
   │                       │ Watch Job          │                 │
   │                       │◄───────────────────┤                 │
   │                       │                    │                 │
   │                       │ Fetch logs         │                 │
   │                       ├───────────────────►│                 │
   │                       │                    │                 │
   │                       │ Update metrics     │                 │
   │                       ├────────────────────┼────────────────►│
   │                       │                    │                 │
   │ Poll /api/v1/dags/:id │                    │                 │
   │◄──────────────────────┤                    │                 │
   │                       │                    │                 │
   │ GET /api/v1/dags/     │                    │                 │
   │     :name/metrics     │                    │                 │
   ├──────────────────────►│                    │                 │
   │                       │ Fetch metrics      │                 │
   │                       ├────────────────────┼────────────────►│
   │                       │                    │                 │
   │                       │ Return metrics     │                 │
   │◄──────────────────────┤                    │                 │
   │                       │                    │                 │
   │ Display in UI         │                    │                 │
   │                       │                    │                 │
```

## Benefits of Architecture

1. **Node-Local Cache**
   - Fast access (local disk)
   - No network overhead for cached artifacts
   - Scales horizontally with nodes

2. **Smart Downloader**
   - Transparent to user tasks
   - Automatic cache management
   - Detailed logging for debugging

3. **Comprehensive Metrics**
   - Historical comparison
   - Cache effectiveness analysis
   - Performance optimization insights

4. **DaemonSet Automation**
   - No manual node configuration
   - Automatic on new nodes
   - Consistent across cluster

5. **Decoupled Design**
   - Frontend independent of backend
   - Backend independent of storage
   - Easy to extend and maintain
