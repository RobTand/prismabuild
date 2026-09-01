# PrismaBuild's first cross-box dispatch

**Date:** 2026-08-31 · **Status:** measured, reproducible, and *not* a
quantization stage — see Limits.

Before this, PrismaBuild had 8,215 qualified lines and had **never dispatched
anything**: both shipped transports need a scheduler (`sbatch`, Dagster) that is
installed on neither Spark. `pool.py` is the transport that runs here.

## What ran

An action sealed on **sparky**, claimed and executed on **sparklina**
(`gx10-6b77`), with the receipt verified back on sparky.

    action_key   7cdfc112993f4b440fea427acc8bd9da3d727783c9a55e439dc297d5bac3337f
    sealed on    sparky
    executed on  gx10-6b77   linux-aarch64-sm121
    accelerator  nvidia cc 12.1, driver 595.84
    receipt      8ff38b7dc24d51c63e4147dbe1587cf1...   cas_receipt.v3
    result       612ab7213c8dd1c59f847229f7c53650...   21 bytes
    elapsed      0.542 s

Queue transitions recorded `published_by: sparky -> finished_host: gx10-6b77`.

## The two properties that make it usable

**1. Cross-box CAS coherence.** `cas.lookup(action)` on sparky returns the
receipt sparklina wrote, with the producing host, platform key, accelerator, and
worker-runtime digest all attested inside it. The artifact is not merely present
— it carries the evidence of where it came from.

**2. Idempotence, measured across boxes.** The same action re-queued and run on
**sparky** returned `cache_hit` in **0.041 s** against the original 0.542 s
execution on sparklina. Neither box recomputed work the other had done. This is
what makes the stale-lease requeue safe: reaping a live-but-stalled worker costs
a CAS lookup, never a corrupted or duplicated result.

The task's argv deliberately writes a **host-independent** result and sends the
hostname to stdout instead. A result whose bytes depend on which box ran it
would make the action key a lie, and the cache hit above would be hiding a
difference rather than proving equivalence.

## Reproduce

    rsync -a --exclude .git /home/rob/prismabuild/ /mnt/shared/prismabuild-fleet/repo/
    python3 tools/fleet/seal_and_publish.py          # on either box
    ssh sparklina python3 /mnt/shared/prismabuild-fleet/worker.py

## Limits — read these before citing this

- **This is a smoke, not a stage.** The action writes 21 bytes. It exercises
  seal → publish → claim → execute → receipt → cache hit, and nothing about
  quantization. No probe, no render, no KL, no artifact.
- **One action, one worker at a time.** The claim race is covered by unit tests
  (8 threads, one barrier, exactly one winner) but has **not** been exercised
  with two real boxes contending on NFS simultaneously.
- **No reservation ledger**, by design — so nothing yet prevents two GPU actions
  landing on one box. v1 admits one action per worker invocation.
- **No queue daemon.** `serve_once` runs one item and exits; there is no
  long-running unit, and pqwork's live units were left untouched.
- The shared checkout means both boxes verify the *same* code-closure bytes.
  A per-box checkout is untested and would need the closure to match.

## Next

The production-cache render is the right first real stage: its stripe/union
toolchain (`production_cache_stripes` → `build_production_cache
--include-qnames-file` → `union_production_cache`) already exists on
prismaquant's `origin/main`, its shards are disjoint, and the same split has
been measured end-to-end at **1.87x with 1.87% imbalance** on a 27B
(`prismasnap-qwen38-27b-20gb-20260825`). Splitting per-Pareto-point KL across
boxes stays blocked on cross-box numeric identity, which has never been
demonstrated.
