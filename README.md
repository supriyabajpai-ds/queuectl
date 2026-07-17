# queuectl — CLI Background Job Queue

A production-grade, CLI-based background job queue in **Java**, backed by **SQLite**. It runs shell commands as jobs across multiple parallel workers, retries failures with **exponential backoff**, and parks permanently failed jobs in a **Dead Letter Queue (DLQ)** — with zero duplicate processing and full persistence across restarts.

**Demo video:** [link to CLI demo](#) <!-- upload your recording to Drive and paste the link here -->

---

## 1. Setup Instructions

**Prerequisites:** JDK 17+ (`java -version` to check). Maven is optional.

### Option A — no Maven (fastest)

```bash
git clone https://github.com/TanishkaMishraa/queuectl.git
cd queuectl
./build.sh          # downloads the SQLite JDBC jar (once) and compiles into out/
./queuectl help
```

On **Windows** (PowerShell / cmd), run `build.sh` once from Git Bash, then use the batch launcher:

```bat
queuectl.bat help
```

### Option B — Maven

```bash
mvn -q package
java -jar target/queuectl.jar help
```

All state is stored in `~/.queuectl/queuectl.db` (override with the `QUEUECTL_HOME` environment variable — the test suite uses this to isolate itself from your real queue).

---

## 2. Usage Examples

**Enqueue jobs:**

```
$ ./queuectl enqueue '{"id":"job1","command":"echo Hello World"}'
enqueued job 'job1' (max_retries=3)

$ ./queuectl enqueue '{"id":"job2","command":"sleep 2","max_retries":5,"priority":10}'
enqueued job 'job2' (max_retries=5)
```

**Start workers** (blocks in the foreground; run in its own terminal):

```
$ ./queuectl worker start --count 3
[2026-07-17T05:01:12Z] started 3 worker(s), pid 4182. Press Ctrl+C or run 'queuectl worker stop' to stop.
[2026-07-17T05:01:12Z] worker-4182-1-7a07 online
[2026-07-17T05:01:12Z] worker-4182-1-7a07 -> picked job 'job2' (attempt 1/5): sleep 2
[2026-07-17T05:01:14Z] worker-4182-1-7a07 [OK] job 'job2' completed
```

**Stop workers gracefully** (from another terminal — each worker finishes its current job first):

```
$ ./queuectl worker stop
stop requested. 3 active worker(s) will finish their current job and exit.
```

**Status, listing, DLQ:**

```
$ ./queuectl status
Jobs:
  pending     0
  processing  1
  completed   4
  failed      0
  dead        1
Active workers: 3
  worker-4182-1-7a07           pid=4182     job=job7
  ...

$ ./queuectl list --state completed
{"id": "job1", "command": "echo Hello World", "state": "completed", "attempts": 0, "max_retries": 3, ... "last_exit_code": 0, "last_output": "Hello World"}

$ ./queuectl dlq list
{"id": "bad1", "command": "no_such_cmd", "state": "dead", "attempts": 3, ... "last_exit_code": 127, "last_output": "sh: 1: no_such_cmd: not found"}

$ ./queuectl dlq retry bad1
job 'bad1' moved from DLQ back to pending (attempts reset)
```

**Configuration** (persisted; picked up by running workers without restart):

```
$ ./queuectl config set max-retries 3
$ ./queuectl config set backoff-base 2
$ ./queuectl config list
max-retries = 3
backoff-base = 2
poll-interval-ms = 500
job-timeout-seconds = 0
```

---

## 3. Architecture Overview

```
cli/Main ──────────────► parses argv, dispatches
   │
   ├── storage/JobStore ─────► jobs table (enqueue, ATOMIC CLAIM, transitions, queries)
   ├── storage/ConfigStore ──► config table (runtime-tunable settings)
   ├── storage/WorkerRegistry► workers + control tables (heartbeats, stop flag)
   │        all via storage/Database (SQLite, WAL mode, schema bootstrap)
   │
   └── worker/WorkerPool ────► N worker threads: poll → claim → execute → record
            └── worker/CommandRunner ► ProcessBuilder (sh -c / cmd /c), exit code + output
```

### Job lifecycle

```
pending ──claim──► processing ──exit 0──► completed
                        │
                    exit != 0
                        │
        attempts < max_retries ──► failed (next_run_at = now + base^attempts) ──backoff elapses──► claimed again
        attempts >= max_retries ─► dead  (Dead Letter Queue)   ──`dlq retry`──► pending (attempts reset)
```

### Persistence design

Everything lives in one SQLite database — chosen over a JSON file because SQLite gives **atomic, multi-process-safe writes** out of the box, which is precisely what the no-duplicates guarantee needs. A JSON file would require hand-built file locking and atomic-rename protocols, which is where race conditions come from.

- `jobs` — one row per job; state machine enforced by a `CHECK` constraint; ISO-8601 UTC timestamps (they sort lexicographically in time order, so backoff eligibility is a plain string comparison in SQL).
- `config` — key/value settings; nothing is hardcoded, defaults apply only until a key is set.
- `workers` — heartbeat rows; a worker is "active" if it heartbeat within the last 10s, so crashed workers age out automatically.
- `control` — the cross-process `stop_requested` flag behind `worker stop`.
- **The DLQ is not a separate table** — dead jobs are `jobs.state = 'dead'`. This makes "move to DLQ" a single atomic UPDATE instead of a cross-table transaction, and preserves the job's full history (attempts, last output, exit code) for debugging.

Pragmas: `WAL` (readers don't block on writers, so `status` works while workers churn) and `busy_timeout=5000` (contending writers wait briefly instead of erroring).

### Worker & locking logic (the important part)

Workers claim jobs with a **single conditional UPDATE**:

```sql
UPDATE jobs
SET state = 'processing', claimed_by = :worker, updated_at = :now
WHERE id = (
    SELECT id FROM jobs
    WHERE (state = 'pending' AND (next_run_at IS NULL OR next_run_at <= :now))
       OR (state = 'failed'  AND next_run_at <= :now)
    ORDER BY priority DESC, created_at ASC
    LIMIT 1
)
AND state IN ('pending', 'failed');
```

SQLite executes each statement atomically and permits exactly one writer at a time — across threads **and OS processes**. Two workers racing on this statement get serialised: the first claims the job; when the second runs, the subquery is re-evaluated against the updated table, the taken job is now `processing`, and the second worker claims a different job or matches 0 rows. "Look for a job" and "take the job" are one indivisible operation, so there is **no window** in which two workers can claim the same job. The worker checks the affected-row count: `1` = you own it, `0` = poll again.

Because the lock lives in the database and not in Java memory, you can run `worker start` in three separate terminals and they still never collide.

**Graceful shutdown:** `worker stop` sets a flag row in the DB; workers check it (and a local Ctrl+C flag) **only between jobs**, so the current job always runs to completion before the worker exits and deregisters.

**Crash recovery:** if a worker process is killed mid-job (`kill -9`), the job is stranded in `processing`. On the next `worker start` — a moment when no workers are alive, so any `processing` row must be an orphan — those jobs are reset to `pending` and run again.

### Retry & backoff

On failure, `attempts` is incremented. If `attempts >= max_retries` the job goes to the DLQ; otherwise it becomes `failed` with `next_run_at = now + backoff_base ^ attempts` seconds. Failed jobs are not held by any timer or in-memory queue — the claim query itself skips them until `next_run_at` passes, which means backoff schedules also survive restarts for free.

---

## 4. Assumptions & Trade-offs

- **Threads, not forked processes, for `worker start --count N`.** The duplicate-processing guarantee lives in the database claim, not in shared memory, so the design is process-agnostic (running `worker start` in multiple terminals is safe and works). One process with N threads is simpler to start, watch, and stop. If per-job process isolation were needed, only the launcher would change.
- **Commands run through the platform shell** (`sh -c` / `cmd /c`), so quoting, pipes and `&&` work as users expect. Trade-off: jobs can run anything the invoking user can — acceptable for a local job runner, would need sandboxing in a multi-tenant system.
- **At-least-once execution semantics.** If a worker crashes *after* a command runs but *before* recording the result, recovery will run the job again. Exactly-once is impossible without the jobs themselves being transactional; at-least-once + idempotent jobs is the standard queue contract (same as SQS/Sidekiq).
- **Polling (default 500ms) instead of push notifications.** SQLite has no LISTEN/NOTIFY; a sub-second poll of an indexed query is effectively free and keeps workers stateless. Interval is configurable.
- **`max_retries` is snapshotted onto the job at enqueue time** (config default or per-job override), so changing the global default later doesn't silently change the contract of jobs already queued. `backoff-base` is read at failure time so tuning it affects in-flight retries.
- **`dlq retry` resets attempts to 0** — the operator has presumably fixed the cause, so the job gets a fresh retry budget.
- **Hand-rolled flat JSON parser, no Gson/Jackson.** The only JSON input is the flat job object; ~150 auditable lines remove an entire dependency tree. Swapping in Jackson later is a one-class change.
- **Timestamps are ISO-8601 UTC strings**, matching the spec's JSON format exactly and sorting correctly as strings.

### Bonus features implemented

- **Job timeout handling** — `config set job-timeout-seconds N`; hung jobs are killed (exit 124, same convention as GNU `timeout`) and enter the normal retry path.
- **Job priority queues** — `"priority": 10` in the enqueue JSON; higher runs first.
- **Scheduled/delayed jobs** — `"run_at": "2026-07-17T10:30:00Z"`; the claim query skips the job until the time arrives.
- **Job output logging** — stdout+stderr captured per job (`last_output`, `last_exit_code`), visible in `list`/`dlq list`; this is what makes DLQ debugging possible.

---

## 5. Testing Instructions

```bash
./scripts/test.sh
```

The suite runs the real binary end-to-end against throwaway data directories and validates the five required scenarios:

1. **Basic completion** — job runs, reaches `completed`, output captured.
2. **Retry → backoff → DLQ** — a failing job exhausts `max_retries`, lands in the DLQ with `attempts == max_retries`, and `dlq retry` returns it to `pending`.
3. **No duplicate processing** — 20 jobs each append their id to a file; 4 concurrent workers drain the queue; the test asserts 20 lines, all unique (each job ran exactly once).
4. **Invalid commands fail gracefully** — a nonexistent binary retries then dies with its error captured; malformed JSON and missing fields are rejected with clear errors and non-zero exit codes.
5. **Persistence across restart** — a job enqueued by one process is visible to a fresh process with no workers alive, then completes normally later.

Expected final output: `PASSED: 14    FAILED: 0`.

A shell script (rather than JUnit) is deliberate: the guarantees under test are process-level — real CLI invocations, real shell commands, real restarts — which unit tests inside a single JVM cannot exercise.

Manual crash-recovery check (not automated because it needs `kill -9`):

```bash
./queuectl enqueue '{"id":"long","command":"sleep 60"}'
./queuectl worker start --count 1   # then kill -9 the java process mid-job
./queuectl list --state processing  # job is stranded
./queuectl worker start --count 1   # logs: "recovered 1 orphaned processing job(s)" and re-runs it
```

---

## Project Structure

```
queuectl/
├── build.sh / queuectl / queuectl.bat   # build + cross-platform launchers
├── pom.xml                              # optional Maven build (shaded jar)
├── scripts/test.sh                      # end-to-end test suite
└── src/main/java/com/flam/queuectl/
    ├── cli/Main.java                    # argv parsing, dispatch, help text
    ├── model/Job.java, JobState.java    # job data + lifecycle state machine
    ├── storage/Database.java            # SQLite bootstrap, pragmas, schema
    ├── storage/JobStore.java            # persistence + THE atomic claim
    ├── storage/ConfigStore.java         # runtime-tunable configuration
    ├── storage/WorkerRegistry.java      # heartbeats + cross-process stop flag
    ├── worker/WorkerPool.java           # worker threads, graceful shutdown
    ├── worker/CommandRunner.java        # ProcessBuilder execution, timeout
    └── util/Json.java                   # dependency-free flat JSON parse/write
```
