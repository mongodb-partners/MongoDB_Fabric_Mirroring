# Temporary Resilience Fixes for OneLake Uploads and Change Stream Recovery

> **Status: TEMPORARY WORKAROUND — NOT A PERMANENT SOLUTION**
>
> These changes are stopgap measures to reduce the blast radius of known failure modes.
> They do NOT fix the root causes. See [Known Drawbacks](#known-drawbacks) and
> [Proper Fixes](#proper-fixes) below.

---

## What This Branch Changes

This branch (`fix/onelake-resilience-and-change-stream-recovery`) patches four files
to add defensive retry, timeout, cleanup, and circuit-breaker logic to the existing
mirroring pipeline.

### Summary of Changes

| File | Fix | What It Does |
|---|---|---|
| `push_file_to_lz.py` | Retry + TEMP cleanup + Timeouts | Wraps the 3-step OneLake upload (create → append → rename) with exponential backoff retry (up to 3 attempts). Deletes orphaned `*_TEMP` files when append or rename fails. Adds `timeout=(connect, read)` to every `requests` call. |
| `listening.py` | Circuit breaker + No token wipe | After 3 consecutive `ChangeStreamHistoryLost` events, stops the listener thread and writes a `_NEEDS_REINIT.txt` sentinel file. Stops writing `None` to the resume token file in LZ on failure. |
| `mongodb_generic_mirroring.py` | Init sync retry + always start listener | Retries `init_sync()` up to 3 times with backoff. Starts the change stream listener even if init sync fails completely. |
| `init_sync.py` | Configurable sleep | Replaces hardcoded `time.sleep(30)` between init batches with env-configurable `INIT_SYNC_BATCH_SLEEP_SEC` (default: 5). |

### New Environment Variables

All have defaults and are optional.

| Variable | Default | Purpose |
|---|---|---|
| `HTTP_CONNECT_TIMEOUT_SEC` | `10` | TCP connect timeout for OneLake calls |
| `HTTP_READ_TIMEOUT_SEC` | `60` | Read timeout for OneLake calls |
| `MAX_UPLOAD_RETRIES` | `3` | Retry count per OneLake upload |
| `UPLOAD_RETRY_BASE_DELAY_SEC` | `2` | Base delay for exponential backoff |
| `MAX_CONSECUTIVE_HISTORY_LOSS` | `3` | Circuit breaker threshold for ChangeStreamHistoryLost |
| `MAX_INIT_SYNC_RETRIES` | `3` | Init sync retry count |
| `INIT_SYNC_BATCH_SLEEP_SEC` | `5` | Sleep between init sync batches (was hardcoded 30) |

---

## Why These Errors Happen on Serverless Applications

This section explains the root causes that led to the failures observed in the CSX case
and during our live testing.

### 1. Connection Refused / Timeout on OneLake Uploads

**What happens:**

The upload pipeline performs three sequential HTTP calls per file:

```
PUT    {file}_TEMP?resource=file           → Create empty staging file
PATCH  {file}_TEMP?position=0&action=append → Write data
PUT    {file}  (x-ms-rename-source)         → Rename to final name
```

On serverless platforms (Azure App Service, AWS Lambda, etc.), outbound connections go
through a NAT gateway or load balancer. These intermediaries have connection limits,
idle timeouts, and SNAT port exhaustion thresholds.

**Why it fails:**

- **SNAT port exhaustion**: Each outbound TCP connection consumes a SNAT port. Serverless
  platforms share a pool of ports across all outbound connections. When the pool is
  exhausted, new connections are refused. This is a well-documented Azure App Service
  limitation (see [Azure SNAT exhaustion](https://learn.microsoft.com/en-us/azure/app-service/troubleshoot-intermittent-outbound-connection-errors)).

- **TCP idle timeout**: Azure's load balancer kills idle TCP connections after 4 minutes.
  If a large file upload takes longer than the idle timeout without the connection being
  marked as active, the connection is dropped mid-upload.

- **No connection reuse**: The current code creates a new TCP connection for every HTTP
  call (`requests.put`, `requests.patch`, etc.) without using a `requests.Session()` for
  connection pooling. Each file upload creates 3–4 new connections. With 47 collections
  and hundreds of control file writes, this generates hundreds of connections per minute.

- **Payload size**: The `raw_logs` collection in our test averaged 3.5 MB per document.
  At `INIT_LOAD_BATCH_SIZE=10`, each parquet file was ~35 MB. Uploading 35 MB over a
  single HTTP PATCH with no chunked transfer encoding means one long-lived connection
  that is highly susceptible to timeout.

### 2. ChangeStreamHistoryLost (Error Code 286)

**What happens:**

MongoDB change streams can only resume from a position that still exists in the oplog.
The oplog is a capped collection — old entries are overwritten as new writes come in.
If the mirroring process falls behind (due to downtime, slow processing, or upload
failures), the resume point may expire from the oplog.

**Why it cascades:**

The original code had a death spiral:

1. `ChangeStreamHistoryLost` fires because the resume token is stale
2. The error handler writes `None` to the resume token file in LZ
3. This LZ write triggers 3 more HTTP calls (create, append, rename)
4. If OneLake is also failing (connection refused), the write fails
5. The exception propagates back into the error handler
6. The loop retries immediately — no backoff, no circuit breaker
7. The same `ChangeStreamHistoryLost` fires again
8. Go to step 2

In the CSX case, this loop ran **514 times**, generating hundreds of LZ writes and
flooding the logs.

### 3. Orphaned *_TEMP Files

**What happens:**

The ADLS Gen2 staging sequence creates a `*_TEMP` file in step 1. If step 2 (append)
or step 3 (rename) fails, the `*_TEMP` file stays in OneLake forever.

**Why it matters:**

- Each orphaned `*_TEMP` file wastes storage
- The mirrored database's Landing Zone scanner may attempt to process them
- They confuse operators who see them and don't know if they're safe to delete
- They indicate incomplete uploads, which may mean data gaps

### 4. Init Sync Failure Blocks Change Stream

**What happens:**

The original code used `continue` to skip the change stream listener if init sync
failed:

```python
except Exception:
    logger.exception("init sync failed; skipping change stream listening")
    continue  # ← skips Thread(target=listening).start()
```

This means a transient OneLake failure during the last batch of a 3.2-million-document
init sync would prevent the change stream from ever starting for that collection.

---

## Known Drawbacks

These are the reasons this branch should NOT be considered a production-grade solution.

### Architecture-Level Issues

1. **Single-threaded, sequential collection processing.** Collections are processed one
   at a time in a `for` loop. If collection 1 of 47 takes 4 hours to init sync, the
   remaining 46 collections wait. There is no parallelism. This is the root cause of
   the "terminalAllocationRuleSet stuck at zero rows" symptom — it simply hadn't been
   reached yet.

2. **No connection pooling.** Every HTTP call to OneLake creates a new TCP connection.
   `requests.Session()` with an HTTP adapter that supports keep-alive and connection
   reuse would reduce SNAT port pressure by 80–90%. The current code doesn't use it.

3. **No chunked upload.** The entire parquet file is loaded into memory
   (`file.read()`) and sent as a single PATCH body. For a 35 MB file, this means
   35 MB in memory + one long-lived HTTP connection. Azure Data Lake Storage Gen2
   supports [block upload](https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update)
   with multiple append calls, which would allow chunked upload with per-chunk retry.

4. **State stored in LZ via 3 HTTP calls per write.** Every `write_to_file()` call
   (resume token, last_id, parquet file number, init status) triggers a full
   create-append-rename cycle against OneLake. During init sync of 1,514 documents
   with batch size 10, this generates ~151 batches × 3 control files × 3 HTTP calls =
   **1,359 HTTP calls just for control state**. A local-first approach with periodic
   LZ sync would reduce this by 90%.

5. **No dead letter queue or retry queue.** If an upload permanently fails after all
   retries, the batch is lost. There is no mechanism to queue the failed batch for
   later retry or manual inspection.

### Code-Level Issues

6. **Race condition in circuit breaker.** The `_NEEDS_REINIT.txt` sentinel file is
   written via `write_to_file()` which itself calls `push_file_to_lz()`. If OneLake
   is down, the sentinel write will also fail, and the exception is silently caught.
   The operator may never see the sentinel.

7. **Fix 5 (not persisting None token) has a tradeoff.** The previous behavior wrote
   `None` to force a fresh start on restart. The new behavior keeps the old token,
   which means a restart will try to resume from a position that may already be
   expired — resulting in one more `ChangeStreamHistoryLost` before the circuit
   breaker fires. This is intentional (fail fast with visibility) but means one
   guaranteed error on every restart after an oplog expiration.

8. **No idempotency for rename.** If the rename (step 3) succeeds but the response
   is lost due to a network timeout, the retry will attempt to create and rename
   again. The ADLS Gen2 rename is not idempotent — renaming to an existing file
   may overwrite it, causing duplicate data in the mirrored table.

9. **Thread-per-collection with no health monitoring.** Each collection gets a
   `Thread(target=listening).start()`. If a thread crashes (unhandled exception),
   there is no supervisor to restart it. The main thread waits on `Event().wait()`
   forever, unaware that a listener has died.

10. **No structured logging.** Log output is plain text. Parsing logs for monitoring
    (e.g., counting ChangeStreamHistoryLost events, detecting orphaned TEMP files)
    requires regex. Structured JSON logging would enable integration with Azure
    Monitor, Log Analytics, or Application Insights.

### Scalability Issues

11. **No backpressure.** If OneLake is slow, the process keeps reading from MongoDB
    and accumulating DataFrames in memory. There is no mechanism to pause reading
    when uploads are backed up.

12. **Memory usage is O(batch size × document size).** With 3.5 MB documents and
    batch size 10, each batch holds ~35 MB in a pandas DataFrame, plus the parquet
    serialization buffer. For the CSX 47-collection scenario, this could mean
    multiple GB of memory if collections are processed concurrently.

13. **Parquet file naming is a global counter.** `LAST_PARQUET_FILE_NUMBER` is a
    single counter per collection. If two connector instances run against the same
    LZ (which Mongo recommends against), they will collide on file names.

---

## Proper Fixes

These are the changes that should replace this temporary branch for a production
deployment.

### Fix 1: Use Azure SDK with Connection Pooling and Chunked Upload

Replace raw `requests` calls with the `azure-storage-file-datalake` SDK:

```python
from azure.storage.filedatalake import DataLakeServiceClient

service = DataLakeServiceClient(
    account_url="https://onelake.dfs.fabric.microsoft.com",
    credential=credential,
    # Connection pooling is built in
)
file_client = service.get_file_client(file_system, file_path)

# Chunked upload with per-chunk retry
chunk_size = 4 * 1024 * 1024  # 4 MB chunks
with open(local_path, "rb") as f:
    offset = 0
    while chunk := f.read(chunk_size):
        file_client.append_data(chunk, offset=offset, length=len(chunk))
        offset += len(chunk)
file_client.flush_data(offset)
```

Benefits:
- Connection reuse via urllib3's connection pool
- 4 MB chunks mean a retry only re-sends 4 MB, not 35 MB
- Built-in retry policies with exponential backoff
- Automatic token refresh

### Fix 2: Local-First State with Periodic LZ Sync

Store control files (resume token, last_id, parquet number) locally first.
Sync to LZ periodically (every N batches or N seconds), not on every batch:

```python
# Write locally immediately (fast, no network)
write_local_pickle(resume_token, local_path)

# Sync to LZ in background, batched
if batches_since_last_lz_sync >= 10:
    push_control_files_to_lz()
    batches_since_last_lz_sync = 0
```

### Fix 3: Parallel Collection Processing

Process collections in parallel with a bounded thread pool:

```python
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=5) as executor:
    futures = {
        executor.submit(process_collection, name): name
        for name in collection_list
    }
    for future in as_completed(futures):
        handle_result(future)
```

### Fix 4: Change Stream with Start-After and Automatic Re-init

Instead of the current error handler, implement a proper recovery flow:

```python
except OperationFailure as exc:
    if exc.code == 286:  # ChangeStreamHistoryLost
        logger.critical("Oplog expired for %s. Starting re-initialization.", name)
        # 1. Delete init_sync_status file (forces re-init)
        # 2. Delete resume token file
        # 3. Delete init_cluster_time file
        # 4. Call init_sync() again (captures new cluster_time)
        # 5. Reopen change stream with new start_at_operation_time
```

This avoids the death spiral entirely and gives the collection a clean restart.

### Fix 5: Structured Logging with Correlation IDs

```python
import structlog
logger = structlog.get_logger()

logger.info(
    "upload_completed",
    collection=collection_name,
    file=parquet_filename,
    size_bytes=file_size,
    duration_ms=upload_duration,
    attempt=attempt_number,
)
```

### Fix 6: Health Check Endpoint

Add a `/health` endpoint to `app.py` that reports:
- Status of each collection's listener thread (alive/dead)
- Last successful upload timestamp
- Circuit breaker status per collection
- Memory usage

This enables Azure App Service health probes and external monitoring.

---

## Test Results

### Unit Tests: 24/24 Passed

See `tests/test_push_file_to_lz.py`, `tests/test_listening.py`,
`tests/test_init_sync_and_mirror.py`.

### Load Tests: 8/8 Passed

See `tests/test_load.py` for simulated failure injection tests.

### Live Test: venkatesh-workspace

- **Collection**: `atlas_intelligence.raw_logs` (1,514 documents, ~3.5 MB avg)
- **Batches completed**: 97 of ~151 before init sync exhausted retries on batch 98
- **Change stream listener**: Started successfully after init sync failure (Fix 6)
- **Orphaned TEMP files in OneLake**: 0 (Fix 2 working)
- **Upload retries triggered**: 28 times, most recovered successfully
- **Process uptime**: 7+ hours in change stream listening mode
