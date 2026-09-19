# pq-cpu312 dev-pin refusal — 2026-09-19 (#658)

`pq-cpu312` on dl380g10 is disqualified for guarded `pbtest` until it is
re-provisioned. Its `tessera-quant` is a local-directory install with no
recorded Git commit, so every shard of a checkout carrying
`tools/resolve_tessera_dev_pin.py` is refused before pytest with
`installed commit=<unknown>`. The gate is working as designed; the
environment is behind it. PrismaQuant tracks the same environment defect
as PQ #753.

## Read the live pin, not this document

The reviewed Tessera commit moves with PrismaQuant re-pins. PB #658 cites
`4c384e6049dca3eeaf503bb2c9cd1cd2778978d1`; PQ origin/main already pins
`79ddd4c6093010c65a5149eff5889f7ac8113272` (pin schema
`prismaquant.tessera_serving_runtime_pin.v2`). A re-provision must target
the commit the checkout's resolver prints at the time of the work, from a
PrismaQuant checkout at or after that pin:

```bash
python3 <pq-checkout>/tools/resolve_tessera_dev_pin.py
```

## What is owed

Re-provision `pq-cpu312` with an immutable Git requirement at the reviewed
commit using the owning project's provisioner, on dl380g10, with all users
of that venv idle, never under a running shard:

```bash
python3 <pq-checkout>/tools/provision_tessera_pin.py \
  --python /home/rob/venvs/pq-cpu312/bin/python
```

Then qualify through PB with a PrismaQuant checkout at or after the pin:

```bash
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbtest.py \
  --checkout <pq-checkout> --python /home/rob/venvs/pq-cpu312/bin/python \
  --tag x86 --priority -10 \
  tests/test_shipcard_git_provenance.py tests/test_format_registry.py
```

Qualification passes when each shard prints `pbtest dependency pin` JSON
with `installed_commit` equal to the resolver's commit. A separately
provisioned pinned environment is the alternative when the shared venv
cannot be taken idle. The caller-owned `pbrun` workaround runs tests
without the provenance check and is not qualification.

## Current-state evidence

- PB #658 records three 2026-09-19 `pbtest` submissions refused in ~1s on
  dl380g10 (receipts available on that issue).
- 2026-09-19 negative control, one shard at `--priority -10`:
  PrismaQuant checkout `/home/rob/prismaquant` at `5c3e64c523` (clean),
  resolver prints `4c384e6049dca3eeaf503bb2c9cd1cd2778978d1`. Shard refused
  before pytest with the issue's exact diagnostic
  (`distribution=tessera-quant installed commit=<unknown>`). Action
  `e410bca76f15`, failed on dl380g10 in 1s, rc 1, `NO PYTEST SUMMARY`.
  No receipt: refusal happens before any work is published.

## Scope

This record changes no gate, default, or checkout transport. The operating
guide's `pbtest` pin convention already prescribes the immutable Git
install and the idle-window rule; this file binds that prescription to the
one environment currently failing it. The 2026-09-07 CPU environment
qualification predates the pin guard and does not cover it.
