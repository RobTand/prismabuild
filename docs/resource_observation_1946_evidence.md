# #1946 local producer evidence and remaining populations

## Identity and scope

Working base: `f018a6039a3078395b3aa985ffecb21b1d2134ff`. These results are
admitted test-run evidence for the working changes, not a published runtime.
No publication, service mutation, canary, rollback or activation was performed.
The runner provides two admitted, no-GPU arms with two CPUs, 4 GiB and native
thread bounds. Checks use plain commands inside those actions; no nested PB
submission was made. The operation/receipt identities below are the retained
runner attribution. Separate live action/attempt, terminal and CAS records for
real kernel/GPU populations were not supplied or generated.

Final implementation/test SHA-256 identities:

| File | SHA-256 |
| --- | --- |
| src/prismabuild/resource_observation.py | edc617a65c750f84a96ec6cf7bccb56d4865725e0961951d173d1f6acdc84fdc |
| src/prismabuild/adaptive_cpu.py | 5fd34b6cbce1c5d164c4aa0e121c64aac17ecd8bd9ad733df18f732b8e26f92a |
| src/prismabuild/box_capacity.py | 1ef5df8e54d6a39d9ebef81c69ca76f2f8cff11b9102385ecc0638b058d09992 |
| tools/fleet/resource_broker.py | a7d800ba2e62f6e1c8c5cd247327f851bc858ec2910c31aa75f92b495ac7b3e2 |
| tools/fleet/worker_loop.py | 5b436dac389917bc4628f6d7db92e2ae08486074b7bcb2467f5f41b5e3a0cbc0 |
| tests/test_local_observation_producer.py | adb9638404223e7e4ce9a14392c85efa0f1e11fe29b513d61b3be318d4d9cdbf |

## Broad fixture regression

Executed on both arms:

```sh
python -m pytest tests/test_local_observation_producer.py tests/test_box_capacity.py tests/test_resource_observation_1946.py tests/test_resource_broker.py tests/test_adaptive_cpu_local_state.py tests/test_adaptive_gpu.py tests/test_gpu_memory.py tests/test_gpu_capacity.py tests/test_idle_gpu_proof_freshness.py tests/test_worker_loop_cooperative_shutdown.py tests/test_worker_loop_reloads_unversioned.py tests/test_worker_loop_bounded_queue_discovery.py tests/test_worker_loop_bounded_offer_publication.py tests/test_worker_gpu_adaptive_contract.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q --basetemp="$TMPDIR/pytest"
```

- x86/Python 3.14: **489 passed, zero skips, 2 warnings**, exit 0.
- ARM/Python 3.12: **489 passed, zero skips, 7 warnings**, exit 0.
- Warnings are the existing multithreaded-fork deprecation from bounded readers.
- This broad run precedes the final population-reread addition described below.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"598b616dfba62ca186ebc5c41e4438011f21e64fb0955bb36815d37f740a0975","size":5430}},"operation":"12ee1965b70ff8d2e998ac93d72227c03c2eb81d216af7c820e190868c17604d","receipt":"99d2f5d294191c5a79ef8c0269b5b20598190c22a5886866583e11dd5d05e50c"}]

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"3ac6b8bd48f3dfb544ff9aafbcbcdb6853e5cccb076bca4bcae3dcdc2d197b7a","size":5828}},"operation":"a71caae1d7aa062358b5a39bd51f4e36eedefaec0aabd02447868d8c36fb13d2","receipt":"6e53d745623bd90f2250d77be325a09161ea87675b9a16293d197c854337b5ad"}]

## Final population reread repair and recheck

The added cgroup-event reconciliation rereads full population under the existing
identity-field set and inode validation, retaining only advisory diagnostics.
It never releases a scope. An initial implementation incorrectly assumed an
`attempt_id` record field: six fixture failures exposed the resulting KeyError
and missed OOM handling. The cause was fixed by using the existing complete
`identity_fields` set and action-key/nonce diagnostics, without changing tests.
This is an implementation defect repaired here, not an impossible acceptance test.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"c934b91df2bd5e756937d66ebf4e5eee9ef7cfd08aaab4c9bb80111fcbac6541","size":11077}},"operation":"680c5c3920933b4da359b8694d7668d46302670e4178f229b3839c36046789ef","receipt":"0eff4e99c78a1c2c6a9a40888637ae2707159f16065bc8cadf0fa0b038dc0ffa"}]

Final affected-code check on both arms:

```sh
python -m pytest tests/test_local_observation_producer.py tests/test_resource_broker.py tests/test_resource_observation_1946.py tests/test_wire_inventory.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py tests/test_fleet_tool_flags_have_help.py -q --basetemp="$TMPDIR/pytest"
```

Both arms: **216 passed, zero skips, zero warnings**, exit 0. This includes the
additional population-hint reread/no-release regression, OOM enforcement while
a fake vendor query is blocked, degraded notification-service recovery,
host-sampler ownership/takeover, memory held-token accounting, stale gaps and
equal-capacity recovery, plus all four repository guards. The broad suite was
not rerun after this final broker-only change; its affected broker/producer
population and all guards were rerun as above. `git diff --check` passed.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"995c0eb41b9ac80bcee4d2f5008497ffc973ccd8cf62e860da865ebc2d14f1fd","size":2430}},"operation":"30e314fb255586d089faf2f1a6e8016ba0af6bb8ee5008911bb8395c4360c445","receipt":"944b7eb851a636c371efac1910c48042a1b0e7b4a5d181e8208a5c99f98a7a9a"}]

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"78533123b8185286acec629807c2b78c5759ce4de6868cd7fe7c6b18e77e3795","size":2432}},"operation":"6090285793aa2047325d4155b8578f12001638916f1465c4af497aee8298d021","receipt":"30f5411a3e109c852a8e4d4da22e5972a187ecba920de99a083350ba544126b3"}]

## Evidence boundaries and outstanding work

Ordinary-file fixtures prove pathname replacement/rearming, disappearance and
recovery, socket unlink/rebind, latching, cancellation and equal-capacity proof
renewal. A real pidfd fixture exercises identity-bound exit hints without
signalling the surviving process. Fake broker/GPU fixtures exercise OOM
attribution, full-scope safety, identity guards, stop retries and consecutive
GPU confirmations. These do **not** qualify real delegated cgroup poll/OOM,
forced PID reuse, live broker socket loss with a stale endpoint, or real GPU
telemetry. No missing external executable was encountered by the checks.

O3's disk bound remains outstanding in the existing supervisor owner:
`supervise.py` uses a default 30-second post-cycle sleep, busy/backlogged
five-second post-cycle sleep; holder-deferred statvfs has no completion bound,
and local spool offers have no age expiry. No duplicate worker disk census or
supervisor scheduler rewrite was introduced. That owner needs an independent
interest deadline/freshness contract, coordinated with adjacent supervisor work.

O5 is **unverified**, not passed: zero real delegated-cgroup OOM/empty, forced
PID-reuse, stale-endpoint loss/restart, and real GPU qualification populations
were completed. The supplied arms have no admitted GPU population. No isolated
real-broker lane or slice-0 baseline/budget evidence was supplied. Production
consumer wake integration belongs to the later consumer slice; no bell/hub or
mirror-publication conversion is claimed. Before activation, TrustLadder must
retain admitted isolated integration actions/attempts, source and published
identities, terminal outcomes, logs and CAS receipts for those populations and
confirm slice-0 evidence. Passing fixture receipts cannot substitute for them.
