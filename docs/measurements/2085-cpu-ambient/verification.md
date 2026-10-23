# Verification evidence and unresolved checks

## x86 / Python 3.14

Executed in the supplied admitted action environment:

```sh
python -m pytest tests/test_cpu_ambient_acceptance_2085.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q --basetemp="$TMPDIR/pytest"
```

Observed **1 failed, 109 passed**. All four mandatory guards passed. The
unchanged #2085 acceptance test failed at line 40, before per-host disposition
checks, because its population expression compares `box_status(name, entry)`
directly with `ACTIVE`. The existing public function returns `(status, detail)`
(`src/prismabuild/fleet_roster.py`, `box_status`); the comparison is false even
for an active host. Thus the test's `expected` set is empty, whereas the honest
report includes all three configured active CPU hosts:

```text
assert set(report["hosts"]) == expected
E assert {'dl380g10', 'gx10-6b77', 'sparky'} == set()
```

This is a test defect, not a missing host disposition or an unavailable
observation. Minimal research-owned fixture fix: use
`box_status(name, entry)[0] == ACTIVE` in the population expression. Changing the
production API, emptying the report or altering roster status would violate
scope or the objective. The test was not edited and the failed attempt was not
repeated on ARM. Therefore **O5's listed acceptance check is impossible as
written**; no passing disposition test is claimed. The report remains the
permitted unqualified deliverable, not universal empirical qualification.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"e3981097ca032c74e13cba6687ed9673866ffcbe05b2b634a915038670b78e7d","size":3461}},"operation":"1ae3bec9681163445ef39252d9c176f952b4c292fb82c75732666acd7952cc09","receipt":"ccd13f766197cf66a0106896e31a14487f3a01a636ef795076ab32868fdc8a7c"}]

The retained stdout was independently read through `read_test_artifact` and
confirmed the assertion and terminal result above.

## ARM / Python 3.12

```sh
python -m pytest tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q --basetemp="$TMPDIR/pytest"
```

Observed **109 passed**. This is guard coverage only, not empirical replay.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"9fca631bcdf31ba3f14f404f2bc35b152e71536af56fc3e385f855c5ac2f81c9","size":1791}},"operation":"2b33dfb434bf12878f1cc5ff64156857bf28c7f3d254573826bd82f4a8d934b0","receipt":"02a7cd5382d9a3db02a47063bc8c6d0083316bf93379d27cc327109d2946949e"}]

## Boundaries and compatibility handoff

No W1 policy, runtime packaging or admission tests were performed by this piece.
No host observation or qualified calibration/held-out replay occurred. No
statistical qualification, proposed qualified k or deployed support is claimed.
After the runs, the inventory gained a digest binding for the retained summary
excerpt and report inventory hashes were updated; roster bytes were unchanged.

The roster changes are exactly three portable per-box selector strings; aliases,
status, roles and loop arguments are unchanged. The empty versioned policy adds
no fleet statistical constants. Wire-inventory passed without compatibility
artifact edits. No compatibility goldens or field sets were hand-edited.

Roster edits also require the release gate's prescribed record-compatibility
namespace capture/adoption lane. That lane is not specified as an available
capture facility in this piece's test environment; ordinary admitted pytest is
not a substitute. No record capture, strict adoption or record-compatibility pass
is claimed. Stop at this handoff for the PB lead's prescribed capture/adoption;
W1 owns the runtime/compatibility implementation side. Do not invent goldens or
silently weaken acceptance. No deployment/publication/workflow transition was
performed. `git diff --check` completed successfully on the final roster change.
