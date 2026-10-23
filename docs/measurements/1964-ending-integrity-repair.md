# #1964 streaming-ending integrity repair

This repair addresses the two review failures on submitted head
`c8045ecabcc21ed613dff4dc3965238e4317376d` without restoring whole-log
expansion in the consumer.

- The bounded result reader now binds the captured terminal directory to the
  terminal status before reading logs, including before a verification-budget
  exhaustion can return an incomplete result. The immutable attempt still has
  to agree with that status and terminal summary.
- Withdrawal summarization validates retained history through the existing
  metadata-only history validator, requires a final immutable outcome link,
  and checks the summary structure before accessing its fields. Malformed
  evidence follows the existing per-key refusal path rather than raising an
  indexing exception outside the isolated result reader.
- Twenty-three new regression cases cover misplaced success/failure terminals
  through both CLI and direct readers, malformed/missing retained withdrawal
  fields, and preservation of a healthy sibling action in the same response.

Core, its helper pin, compatibility goldens and all historical/control files
are unchanged by this repair. The existing golden refresh is not repeated or
hand-edited.

## Reproduction

The initial eighteen-case regression set reproduced sixteen failures and two
existing refusals, including the review's failed-in-`done/` acceptance and
empty-history `IndexError`. Five missing-field cases were added afterward.

```sh
python -m pytest tests/test_consumer_surface.py -q -k 'terminal_directory_binding_is_required or malformed_retained_withdrawal_is_isolated' --basetemp="$TMPDIR/pytest"
```

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"2368edebb3b875f51d986d0c69580080cbdb27acfbea535760fa7e5d78490f14","size":104000}},"operation":"b837fd48238bf37445a4eba1a5da645c87b4ffdc390457cbe12c1b67555af1a6","receipt":"d8aa229457d1c256445c1c247834f6469dfccda5fad7f76fd1bdb5fc42c8c942"}]

## Verification

Checks ran through `run_tests` in the provided admitted x86/Python 3.14
execution environment, with two CPUs, bounded native threads and temporary
files outside the checkout. An initial explicit `arm="x86"` tool request was
not run because this tool exposes the provided x86 environment as its default
arm; subsequent checks omitted the arm argument.

**518 passed**, including the final twenty-three new cases, oversized stream
and single-pass guards, incomplete-verification behavior, SDK compatibility,
Core, bounded observation, summary conflicts, terminal summaries, wire
inventory, fleet flag help, design references and duplication checks:

```sh
python -m pytest tests/test_consumer_surface.py tests/test_pbwait.py tests/test_core.py tests/test_verified_action_result.py tests/test_pbwait_bounded_observation.py tests/test_pbwait_summary_conflict.py tests/test_terminal_stdout_is_bounded.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_design_doc_line_references.py tests/test_duplication_baseline.py -q --basetemp="$TMPDIR/pytest"
```

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"88ae662f3c2b4e02e25a5e173bddbc90b0fd8fe68ab046205237f4fbabe789c7","size":4033}},"operation":"609ccc0299c4881451372f7e97d6513e50a4d0ff5fe41e5fba2bf48df7242efa","receipt":"b937c4cadca8f1f31636eeadd2faa5397f87c3d7dc37f33652e20c9203191799"}]

**83 passed** for withdrawal after retry, retained withdrawal evidence, archived
publication recovery and publication-ending/supersession behavior:

```sh
python -m pytest tests/test_pool_withdraw_after_a_retry.py tests/test_pool_withdrawn_retention_1533.py tests/test_pbrun_recovers_overwritten_generation.py tests/test_pool_publication_endings.py tests/test_publication_supersession.py tests/test_untyped_publication_ending_is_not_a_verdict.py -q --basetemp="$TMPDIR/pytest"
```

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"de858b98fc7510e24f73996a134fe3cc92bb715365222896aeac1c42e432a3f4","size":2947}},"operation":"1c6c9689bab726024c47bdf4330f43af4ddd549a85157ce265393126d8558c0d","receipt":"d62200fb28f283f04175a8d594a918cc1b88d3e950663afb576b45a913e02172"}]

No ARM arm is available in this task's test environment. The eight-file
archival record-compatibility suite and full release gate were not rerun.
These scoped checks do not independently authenticate the PR's earlier
external fleet/capture/adoption claims and grant no deployment credit.
