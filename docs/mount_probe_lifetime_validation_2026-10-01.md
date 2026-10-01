# Completed mount-probe child ownership

## Scope

This is a source slice for #1398, not a diagnosis or resolution of the NFS
latency incident. A completed JSON payload and pipe EOF do not prove that the
probe child has exited. The completed-payload branch previously discarded a
zero return from bounded reaping, leaving a live child untracked.

The repair first extracts the existing timeout reap-or-track operation, then
uses the same operation after completed OK or error output. It retains the
original PID and start time until nonblocking `waitpid` proves exit or reports
`ECHILD`. A tracked child prevents another probe fork. There is no age-based
ownership expiry.

No signal, probe deadline, grace interval, status value, freshness rule, health
gate, or wire schema changes. A probe's completed result remains its result;
process ownership is a separate lifecycle fact.

## RED

The unchanged implementation is byte-identical to deployed generation
`3aff9642ab39-1790654284-1eed70850170`: 59,717 bytes, SHA-256
`e2727286f1f1ec0a5f563349af44e510a4b9a2df0d7d8d558de6453c6e7dcef9`.

The regression uses a real fork, private-filesystem `timed_probe`, pipe output,
EOF, and nonblocking `waitpid`. Only the child's final exit is gated after the
real result has been delivered. Both OK and error cases prove that the child
is alive and unreaped before asserting that the sampler retains its PID.
Cleanup releases and reaps only that fixture child.

PB action `9cb2f185150463e3d47d3312afc657989a7c3e32d95db8d1d5e504d8dfb5c710`
on Sparky: **2 failed, 0 skipped; 2 collected and run; 0.62 s**. Both failures
are the intended untracked-live-PID assertion, not setup failures.

## GREEN

PB action `ef7c4b6b3a233ed915fe15e71721b4ba754a8118ab4df40f4976c15174657c98`
on Sparky: **56 passed, 0 failed, 0 skipped; 9.15 s**. The new two cases and
existing mount-collector and mount-lock identity suites were reconciled.
The new cases also verify second-probe suppression and clearing ownership
after actual reaping or `ECHILD`.

Invocation, using the published PB client:

```sh
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbtest.py \
  --checkout "$PWD" --tag gb10 \
  --python /home/rob/venvs/pb-cpu/bin/python \
  --shards 1 --workers-per-shard 1 --threads-per-shard 1 \
  --cpus-per-shard 1 --mem-gb 3 --priority -10 \
  --timeout-s 300 --wait-s 1200 \
  tests/test_mount_probe_tracks_completed_unreaped_child.py \
  tests/test_mount_latency.py tests/test_mount_lock_identity.py
```

Submission scripts check `WINDOW_ACTIVE` immediately before launching the PB
client. Tests execute only inside admitted actions, with bounded native
threads and the assigned CPU affinity intact.

Terminal records, immutable attempts, log byte counts and SHA-256 values,
canonical CAS receipts/results, case counts, and released cleanup were checked.
Receipts and verification artifacts are under
`/home/rob/tmp/claude-campaign-20260926/tmp/p2p3/prismabuild/`:

- `1398-reap-red.json` and `1398-reap-red-verification.json`;
- `1398-reap-green.json` and `1398-reap-green-verification.json`.

## Review and remaining qualification

Independent source review found no issues. Active editor diagnostics are not a
clean-file claim: 56 source positions match unchanged base expressions. The
new test's unqualified import is unresolved by the static workspace, but the
actual RED and GREEN execute the intended checkout-local import. No diagnostic
suppression or unrelated cleanup was introduced.

A two-file admitted syntax compile is pending. No collector deployment or live
service mutation was performed. Passive advancing health samples still
included intermittent Sparklina timeouts. This patch does not establish the
incident's cause, lasting recovery, or full acceptance of #1398.

## Syntax result

This result supersedes the initial pending compile statement above. Admitted
two-file syntax compilation passed: action
`ac04efdb33ee187df6552d68289e44b25aada329ab96a05b35daba45fb9b1d07`,
Sparky, `COMPILE_OK 2 files`. The 19-byte result has SHA-256
`876f6f6c79127b504fa99403738f7d6b4eebf55d32c60344e21eb6ce77e4e872`;
terminal, immutable attempt, logs and CAS were verified in
`1398-reap-compile-verification.json`. This is syntax proof, not live health.
The incident-cause, deployment and recovery limitations remain unchanged.
