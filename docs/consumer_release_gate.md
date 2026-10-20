# Generation-bound TrustLadder consumer criterion

This source implements an **opt-in operator policy**, not a deployment or an
adapter port. No live configuration is installed. Existing unconfigured releases
retain their behavior. Shape waivers and `--no-canary` do not waive this criterion.
Reader/bootstrap qualifications and `FINAL_BARRIER_QUALIFICATION_GUARD` are
unchanged; barrier publication remains disabled.

## Configuration

Set `PRISMABUILD_CONSUMER_GATE_CONFIG` to an external JSON file. The exact schema
is `prismabuild.consumer_gate.config.v1`, with these required fields (unknown
fields are refused):

- `repository`: absolute path to the live consumer Git **root**.
- `fleet_root`: absolute fleet store path. The publisher's generation store must
  be `<fleet_root>/runtime-generations`.
- `pbrun`: absolute path to the installed submission client, with private client
  event support. It need not be the candidate client.
- `runtimes`: exactly two objects with unique `id` and `python`. Each has exactly
  `id`, `python`, `python_version`, `tags`, `cpus`, `mem_gb`. IDs contain only
  letters, digits, underscore and hyphen; Python paths are absolute. Tags are
  nonempty strings, sorted/deduplicated as by pbrun. CPU demand is a positive
  integer and memory a positive finite number. One route requires `x86` and
  version `3.14`, the other `gb10` and version `3.12`; no route requires both.
  Interpreter paths select the operator's pb-cpu environments, not host names.
- `accepted_breaks`: a list, initially empty; see below.

There is deliberately **no configured consumer commit**. At gate execution,
HEAD is discovered and retained after refusing tracked and untracked dirt.
Both actions must prove correspondence to that same clean pin. Rechecks do not
require that the live checkout still be at that pin.

The normalized policy, excluding `accepted_breaks`, is sealed in G's
`RUNTIME_VERSION.json`. Removing the environment variable or changing that
policy later cannot bypass a configured candidate's criterion. Accepted breaks
may be added later without rewriting G. Protect the config as operator-owned
release policy: a named approver in this trusted file is an explicit human
record, not cryptographic authentication of a human. Do not put this policy in
an untrusted consumer-generated artifact.

`PRISMABUILD_SUBMISSION_FLEET_ROOT` is the submission client's optional absolute
fleet-store override (default remains its existing installed fleet root). The
gate sets it from `fleet_root`; it does not change the installed runtime or
roster. The selected client/runtime must support this override and private
client events. Missing capabilities/evidence refuse activation, not fall back.

## Execution and lifecycle

After sealing and naming G, but **before** activation or admission closure,
the publisher submits both configured pool actions, including for `--stage-only`.
Each runs in the consumer snapshot root with
`PB_TOOLS_ROOT=<fleet_root>/runtime-generations/<G>/tools` and executes:

```
<configured python> -m pytest -q -o addopts= -p consumer_gate_capture \
  bin/test_pq_release_gate.py bin/test_pbtest_gate.py \
  bin/test_computation_runner.py bin/test_tessera_release_gate.py
```

The standalone plugin comes from G via sealed `PYTHONPATH`; it does not depend
on PrismaBuild's conftest being present in the external checkout. Native thread
limits are one. `PYTEST_ADDOPTS` and ini addopts are cleared to prevent filters,
maxfail, xdist or collect-only from reducing the population. Normal pytest plugin
autoload remains enabled. Collection modification that silently removes items
still leaves missing reports and refuses. Skips, xfails, collection errors,
deselection, early termination and empty files are not passing evidence and are
not accepted test failures for a break record.

Both submissions are attempted even when one reports failure. Each client's
private event descriptor is the write-only end of a pipe, drained concurrently
and independently of stdout/stderr, including buffered events at client exit.
Private events retain the action key and exact queue `published_unix`; stdout
labels, submitter success and arbitrary stdout JSON are never receipts. Missing
or malformed endings remain refusing diagnostics, not usable action references.
The immutable sidecar
`<fleet_root>/runtime-generations/<G>.consumer-gate.json` holds the pin,
policy, receipt digest and both action references plus diagnostic client logs.
It is outside G; G is never unsealed or rewritten. The sidecar is exclusive
first-writer, fsynced, and made read-only. An interrupted/missing/invalid record
refuses activation. Retain it, queue attempts/logs, requests, bundle inputs and
execution receipts/results for as long as G might be activated. Do not prune
these as expendable test output. If execution cannot finish, retain G for
inspection and stage a new candidate; do not edit the old record into success.

Activation verification is separate and submits **nothing**. Fresh activation,
`--activate-generation`, forward installed bootstrap reservation/activation, and
barrier arm/forward/resume re-read evidence. Restores/rollbacks retain their
existing safety behavior and do not need to rerun a broken target's tests.
Stage-only may retain an unsuccessful candidate without activating it.

## Independent verification contract

For each action, verification checks the sealed request key, standard capture
wrapper, exact command/environment/cwd, placement and demand. CAS verifies the
snapshot bundle's bytes. In a private temporary Git repository, the verifier
requires a v2 synthetic snapshot with its declared **and actual** sole parent
equal to the retained pin. Its tree must equal that pin except for the one new
pbrun closure stamp, with the exact bytes/hash declared by `code_closure`.
A synthetic snapshot commit string need not equal consumer HEAD. Verification
does not import objects or change refs in the live consumer checkout.

The exact queue publication is recovered from immutable archived attempts,
including log byte counts and hashes. A passing action must be terminal
`executed`, exit zero, and announce one execution receipt in that attempt's
stdout. The execution-specific CAS receipt/result is verified, not merely the
canonical action-key winner. A failed test action uses verified immutable
failed-attempt stdout; it is **not** treated as a successful CAS result.

One complete plugin record must agree with G and its receipt digest, the pin,
runtime ID, actual Python path/version/resolved executable/hash, running-image
correspondence, architecture, worker host, argv, plugin path/hash and exit code.
Collection is observed via `pytest_itemcollected`, before modification hooks.
Every collected node must belong to the four requested files (all four must be
nonempty), with unique, complete setup/call/teardown outcomes. Failed setup may
omit call; skips and xfails are refused. Exit zero requires all nodes passing;
exit one requires observed failed nodes. Anything partial, substituted,
unverifiable or stale refuses before the live pointer moves.

## Exact accepted breaks

An accepted break never substitutes for either original execution. For a
verified failing gate, an entry in `accepted_breaks` must have exactly:

```
{
  "accepted_by": "Rob",
  "generation": "<exact G>",
  "consumer_commit": "<tested full commit>",
  "failures": {"<runtime-id>": ["<sorted failing node ID>"]},
  "port_commit": "<full available port commit>",
  "port_actions": {
    "<runtime-id>": {"action_key": "<64 hex>", "published_unix": 123.5}
  }
}
```

`accepted_by` is exactly `Rob` or `TrustLadder lead`. Both runtime IDs must occur
in `failures` (including empty lists) and `port_actions`. Failures must exactly
match the observed sorted node IDs on each original runtime. The port must
already exist in the configured repository, descend from the tested pin, and
be reachable from local `refs/heads/trustladder-deploy`; a pending promise,
remote label or unrelated commit is insufficient.

Before recording acceptance, the operator supplies two already-completed port
actions using the same recipe/G/runtime configuration, but with
`PB_CONSUMER_COMMIT` set to that port. These must independently pass the entire
evidence contract above, and must include the original failing node IDs: deleting
or renaming those tests is not a fix. This proves the available port's
applicability against G, without porting adapters here or deploying it. The
gate does not create port actions during activation/admission closure.

Blanket reasons, another G/pin, inaccurate failures, absent port commits, missing
port action evidence, and failed port tests all refuse. A valid explicit record
plus complete original failed-attempt evidence and verified passing port evidence
allows activation; an ordinary all-passing gate requires no acceptance record.
