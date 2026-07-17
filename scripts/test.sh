#!/usr/bin/env bash
#
# End-to-end tests for queuectl, covering the assignment's five required
# scenarios:
#   1. Basic job completes successfully
#   2. Failed job retries with backoff and moves to DLQ
#   3. Multiple workers process jobs without overlap (no duplicates)
#   4. Invalid commands fail gracefully
#   5. Job data survives restart
#
# WHY a shell script instead of JUnit: the guarantees under test are
# *process-level* (multiple workers, real shell commands, restarts). A shell
# script exercises the exact binary a reviewer would run, CLI and all,
# whereas JUnit would test classes in one JVM and miss the cross-process
# story entirely.
#
# Each test runs against a throwaway QUEUECTL_HOME so it never touches your
# real queue, and cleans up its worker processes when done.

set -u
cd "$(dirname "$0")/.."

Q=./queuectl
PASS=0
FAIL=0

banner() { echo; echo "=== $1 ==="; }

check() { # check <description> <expression...>
    local desc="$1"; shift
    if "$@" >/dev/null 2>&1; then
        echo "  PASS: $desc"; PASS=$((PASS+1))
    else
        echo "  FAIL: $desc"; FAIL=$((FAIL+1))
    fi
}

fresh_home() {
    export QUEUECTL_HOME
    QUEUECTL_HOME=$(mktemp -d)
}

start_workers() { # start_workers <count>
    nohup $Q worker start --count "$1" > "$QUEUECTL_HOME/worker.log" 2>&1 &
    WORKER_PID=$!
    sleep 1
}

stop_workers() {
    $Q worker stop >/dev/null 2>&1
    # Give workers time to finish the current job and exit gracefully.
    for _ in $(seq 1 20); do
        kill -0 "$WORKER_PID" 2>/dev/null || return 0
        sleep 0.5
    done
    kill -9 "$WORKER_PID" 2>/dev/null
}

state_of() { # state_of <job-id>  -> prints state
    $Q list 2>/dev/null | grep "\"id\": \"$1\"" | grep -o '"state": "[a-z]*"' | cut -d'"' -f4
}

# ----------------------------------------------------------------------
banner "Test 1: basic job completes successfully"
fresh_home
$Q enqueue '{"id":"ok1","command":"echo hello"}' >/dev/null
start_workers 1
sleep 3
stop_workers
check "job reached 'completed'" test "$(state_of ok1)" = "completed"
check "output was captured"     sh -c "$Q list --state completed | grep -q '\"last_output\": \"hello\"'"

# ----------------------------------------------------------------------
banner "Test 2: failed job retries with backoff, then moves to DLQ"
fresh_home
$Q config set backoff-base 1 >/dev/null       # base 1 => 1s delays, keeps the test fast
$Q config set max-retries 3 >/dev/null
$Q enqueue '{"id":"bad1","command":"exit 1"}' >/dev/null
start_workers 1
sleep 8                                        # enough for 3 attempts with 1s backoff
stop_workers
check "job moved to DLQ (state=dead)"  test "$(state_of bad1)" = "dead"
check "attempts equals max_retries"    sh -c "$Q dlq list | grep -q '\"attempts\": 3'"
check "dlq retry brings it back"       sh -c "$Q dlq retry bad1 && [ \"\$($Q list | grep bad1 | grep -o '\"state\": \"[a-z]*\"' | cut -d'\"' -f4)\" = pending ]"

# ----------------------------------------------------------------------
banner "Test 3: multiple workers, no duplicate processing"
fresh_home
MARKER=$(mktemp)
: > "$MARKER"
N=20
for i in $(seq 1 $N); do
    $Q enqueue "{\"id\":\"j$i\",\"command\":\"echo j$i >> $MARKER\"}" >/dev/null
done
start_workers 4
sleep 8
stop_workers
TOTAL=$(wc -l < "$MARKER")
UNIQUE=$(sort "$MARKER" | uniq | wc -l)
check "all $N jobs executed"          test "$TOTAL" -eq "$N"
check "every job executed EXACTLY once" test "$TOTAL" -eq "$UNIQUE"
rm -f "$MARKER"

# ----------------------------------------------------------------------
banner "Test 4: invalid commands fail gracefully"
fresh_home
$Q config set backoff-base 1 >/dev/null
$Q enqueue '{"id":"ghost","command":"no_such_binary_xyz","max_retries":2}' >/dev/null
start_workers 1
sleep 5
stop_workers
check "nonexistent command ends in DLQ"        test "$(state_of ghost)" = "dead"
check "error output captured for debugging"    sh -c "$Q dlq list | grep -qi 'not found'"
check "malformed enqueue JSON is rejected"     sh -c "! $Q enqueue 'this is not json' 2>/dev/null"
check "missing 'command' field is rejected"    sh -c "! $Q enqueue '{\"id\":\"x\"}' 2>/dev/null"

# ----------------------------------------------------------------------
banner "Test 5: job data survives restart"
fresh_home
$Q enqueue '{"id":"survivor","command":"echo alive"}' >/dev/null
# No worker running: the ONLY thing holding this job is the SQLite file.
# Every $Q invocation below is a brand-new OS process — if state lived in
# memory, the job would be gone by the next line.
check "job visible from a fresh process"  test "$(state_of survivor)" = "pending"
start_workers 1
sleep 3
stop_workers
check "job later completes normally"      test "$(state_of survivor)" = "completed"
check "completed job persists after workers exit" sh -c "$Q list --state completed | grep -q survivor"

# ----------------------------------------------------------------------
echo
echo "======================================"
echo "  PASSED: $PASS    FAILED: $FAIL"
echo "======================================"
[ "$FAIL" -eq 0 ]
