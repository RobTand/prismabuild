# #2033 stuck-child attribution test repair

Scope: strengthen PR #1719's tests, not the production sampler or health gate.
The fixture uses a real forked child, SIGSTOP/SIGCONT and an empty pipe;
only the parent's timeout SIGKILL is suppressed. The real bounded reaping
still runs. An independent oracle reads `/proc/PID/status` and `wchan`
(production reads `stat`) without pinning a kernel-specific wchan name.

Coverage:
- Compare the outstanding PID's actual procfs diagnostics with the refusing
  `timed_out` sample and each `wedged` sample.
- Synchronize T -> S -> T transitions between wedged samples, detecting reuse
  of earlier diagnostics.
- Kill and reap the child immediately before the diagnostic read, for both
  statuses, then exercise the real procfs reader and require `?:?`.
- Require exact `child=state:wchan` log tokens and `stuck_child` label values.
- Pin the failing probe-state dimensions and complete one-hot numeric vector;
  legacy records lacking diagnostics must still emit `ok=0`, with no invented
  latency measurement. These are producer assertions, not a live consumer-gate
  qualification. No production health decisions or chart definitions changed.

## Admitted x86 verification

Command (plain pytest; `run_tests` supplies admission):

```sh
python -m pytest tests/test_wedged_probe_names_its_stuck_child.py tests/test_mount_latency.py tests/test_mount_probe_tracks_completed_unreaped_child.py tests/test_mount_lock_identity.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q --basetemp="$TMPDIR/pytest"
```

Observed: **156 passed**. This includes all eight strengthened/new cases and
all three repository guards present in this checkout.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"895e0ede0c126322877327e5a06d94514242edb8f745557e53a9552bdb7e0434","size":272}},"operation":"83d0a4e5c3be140bcaec904aeb094bff1c7e9d855cb7a46302ed58fbe958c2e3","receipt":"75a63ca247215c93cfc4b2228ade354ad2903d380aa94a652dc2aa5e09f76661"}]

A separate admitted `python` driver imported `mount_latency`, applied each
mutation **in memory**, then invoked `pytest.main` in a fresh subprocess with
`-q --tb=line`, a targeted `-k` selector and a distinct `$TMPDIR` basetemp.
No pytest hooks or worktree edits were used. The driver required exit 1
(assertion failures) from every mutant; its own exit was 0. Verified retained
stdout shows the intended assertions rejecting all six mutations:

| Mutation | Selector | Expected failures observed |
| --- | --- | ---: |
| `process_status` returns `('S', 'fabricated_not_from_proc')` | `sample_names` | 2 |
| Cache the first real diagnostics per PID | `wedged_sample_names` | 1 |
| Prefix the log's child diagnostic with `fabricated_` | `sample_names` | 2 |
| Prefix the `stuck_child` label's value with `fabricated_` | `sample_names` | 2 |
| Substitute `('S', 'fabricated_alive')` after disappearance | `disappearing` | 2 |
| Promote legacy records to `ok` during emission | `legacy` | 4 |

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"3144cca096047951ca0550d3a35ac379dbe4e96be8c02da2cb5f7e8503f9d733","size":8830}},"operation":"e0d7b2a522c0af0b15b4ee9c4064c85b5f610a0d4db3248e6b17e4de8a3cc0b1","receipt":"5131df080009039b7f2330eff62c409663cf898c7e8bfe1d5753946c6856191f"}]

Unavailable: `tests/test_wire_inventory.py` and
`tests/test_record_compatibility_*.py` are absent from this checkout; they were
not run and are not claimed as passing. No compatibility golden was changed.
No live-fleet, incident-recurrence or PQ #275 qualification was attempted or
claimed; those are outside this test repair.
