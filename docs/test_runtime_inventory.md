# Measure the test suite for pruning

Run the collector from a clean task worktree on a Linux host with a systemd
user manager. Keep its output outside the
checkout. It uses the published `pbtest.py`, leaves file partitioning and
placement to PB, and requests one CPU, two GiB and one native thread per
action. At most eight clients are resident, so the default run has at most
eight CPUs and sixteen GiB outstanding. No GPU capacity is requested.

```bash
python3 tools/maintenance/test_runtime_inventory.py collect \
  --checkout "$PWD" --python /home/rob/venvs/pb-cpu/bin/python3.14 \
  --output /home/rob/fleet/test-inventory --repeats 3
```

`collect` starts a transient systemd user service and returns after launch.
The service survives the submitting shell or agent exiting. Its unit name and
controller PID, process start ticks and boot identity are recorded in `runs.json`
before the first submission. The command prints the unit and `controller.log`
path. A successful launch is not a completed collection. Inspect the manifest,
service and log to distinguish running, failed and completed controllers.

Each repeat attempt gets a fresh sealed environment value. PB cannot satisfy a repeat
by returning the previous repeat's cache entry. The collector records the
source revision, exact command, controller exit, report digest and fleet
status before and after each run. A refused or failed run remains evidence.
The collector does not retry it as a fix or substitute local execution.

The command runs the whole `tests/` population by default. Supply test paths
at the end for a collector smoke test. That selection is recorded, and cannot
establish full-suite coverage. Resume the same output directory to adopt
finished repeats after their report hashes and source identities verify.
Resume with the same `collect` command. A single service and output lock prevent
duplicate controllers. If an unfinished repeat has a dead producer and no report,
the collector records an abandoned attempt with its published action keys, log
digest and progress digest. It preserves those files and starts a fresh attempt
with a distinct sealed identity. Abandonment describes the collector attempt;
it does not cancel the PB actions or claim they failed. They retain their sealed
execution bounds. A live producer or unknown producer identity prevents recovery.
A final report written before controller exit is adopted after its producer dies.
Completed reports are always checked against their retained digests.

To interrupt the controller deliberately, stop the exact unit printed at launch:

```bash
systemctl --user stop pb-test-inventory-<output-directory-hash>.service
```

If the shell lacks its user-bus environment, set `XDG_RUNTIME_DIR=/run/user/$(id -u)`
and `DBUS_SESSION_BUS_ADDRESS=unix:path=$XDG_RUNTIME_DIR/bus` before using
`systemctl`. The collector discovers that normal user bus automatically.
For collector qualification, add `--priority -10`; campaign inventory uses 0.

Rebuild the rankings from the retained records without rerunning tests:

```bash
python3 tools/maintenance/test_runtime_inventory.py summarize \
  --checkout "$PWD" --output /home/rob/fleet/test-inventory
```

`inventory.json` retains every node and sample. `inventory.md` ranks source
groups and the slowest nodes, lists changed outcomes, and records coverage
gaps. Setup, call and teardown all count toward test cost. Pytest prints these
durations rounded to 0.01 seconds; sub-resolution samples are not evidence
that a test costs nothing. Session time includes collection and process
overhead and is reported separately from summed phase time.

Source groups are static leads for assigning pruning work. They use direct
imports and references, then literal fleet-tool filenames. The JSON retains
all candidates and labels files without a candidate `unattributed`. Verify
the owner before editing tests. Grouping is not runtime coverage analysis.

A pass/fail change on the same source is a flake candidate. A skip, missing
record, interrupted process or changed population cannot prove reliability.
Bound failures remain separate from ordinary failures. Review terminal
records, full stdout/stderr and CAS receipts for every action before using
the report as acceptance evidence.

Collect loaded repeats while useful admitted work is already running. The
fleet snapshots record observed load; the collector does not manufacture
load. Loaded duration ranks help find expensive tests, but they do not prove
a speedup. Use interleaved isolated `--measurement` actions under the
[execution policy](agent_execution_policy.md#measure-wall-time) for a
before/after runtime claim. Preserve exact population, interpreter, setup and
cache conditions, and report an inconclusive comparison when load or repeat
spread prevents attribution.
