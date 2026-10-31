# Candidate A and revised recommendation

## Caller usage

```python
# Production worker ceiling retains #1707 semantics.
queue.execute(item, timeout_s=worker_ceiling_s)

# A fixture requiring an explicit payload deadline seals that declaration.
request["params"]["execution_timeout_s"] = 1e-9
item = publish_and_claim(core.seal_action(request))
outcome = queue.execute(item, containment=True, timeout_s=1e-9)
assert outcome["termination_reason"] == "execution_deadline"

# A producer fixture requiring a short advisory incumbent opportunity seals
# its finite payload budget before publication and claim.
producer_request["params"]["execution_timeout_s"] = finite_fixture_budget
```

The existing GPU and funded-export assertion bodies remain unchanged. Fixture declarations describe the finite authority those assertions require. This is proposed source repair, not a claim that the parent has already run these altered fixtures.

## Task state

1. Ground: complete, exact source and both causal paths read at `efff85c6f1c4`.
2. Sketch: complete, original Candidate A rejected below; modest alternative recommended.
3. Agree: root owns synthesis with confirmed history and independent candidate.
4. Implement: outside this read-only assignment.
5. Scrap: revisit if refactor needs broader semantics or a fixture cannot truthfully declare its intended finite behavior.

## Problem and grounded flow

`execute(timeout_s=...)` is the worker ceiling. #1707 deliberately redirects an unrequested lifetime bound toward stall allowance. A no-policy action with no sealed deadline must remain lifetime-unbounded. `_declared_run_bound` consequently returns `(stall, None)`, and `holder_bound` correctly reports it as unbounded. A workload that requires a finite completion opportunity must declare a sealed timeout. Both regressions are consistent with stale fixture declarations that relied on the former implicit worker lifetime budget.

The GPU fixture seals only demand, then expects `execute(timeout_s=1e-9)` to create a payload deadline. The producer fixture seals no finite total duration, then expects it to be a short-lived incumbent eligible to drain the withheld row. Changing production interpretation to make either assertion pass would restore implicit authority that #1707 removed.

## Original Candidate A, rejected

Candidate A split a direct caller cutoff from worker policy with a new `WorkerExecutionLimits` type and `worker_limits` keyword. It also proposed retaining lifetime-unbounded status while granting young default-stall holders an initial scheduling opportunity.

Reject it after the parent's history clarification. It expands the public execution API, requires a broad caller migration and receipt distinction, and changes scheduling semantics solely to accommodate fixture context. The young-holder scheduling exception directly undermines the correct sealed-unbounded interpretation. This is more future cost and policy churn than the evidence justifies.

## Recommended alternative B: shared request owner, truthful declarations

No new public API or domain type is needed. The existing validated action Mapping is the authoritative shape. `ExecutionBudget`, `ProgressPolicy` and `DefaultStall` retain their existing meanings and enforcement paths.

```python
def _sealed_action_request(cas_root, action_key, *, max_bytes=None):
    # existing owner: immutable read, strict decode/validation, key binding,
    # missing-request distinction


def _sealed_execution_policy(item, ceiling):
    # not implemented: read through _sealed_action_request; preserve semantics


def _declared_run_bound(item, *, max_bytes=None):
    # not implemented: same owner and bounded read; preserve semantics


def _sealed_progress_policy(item, *, read=pb.action_progress_policy):
    # not implemented: same owner; preserve semantics


def _sealed_stall_allowance(item):
    # not implemented: same owner; preserve semantics
```

The load-bearing structural change removes repeated immutable-file reads, strict decode/validation and key-binding ownership from the four supervision readers. It does not collapse execution ceilings, semantic policy and advisory lifetime classification into one answer. Preserve missing-request behavior and exception contracts exactly. Do not introduce catch-all fallback to legacy, default deadline or unbounded status on unreadable authority.

Then repair fixture declarations at their authoritative request builders. The explicit host-fault deadline fixture seals its intended deadline before publication, retaining host-fault and deadline assertions. The producer used in the funded-export withholding case seals an honestly finite fixture budget before publication and claim; all funded allowance, zero-token export, unfunded deferral, bystander and no-pass assertions remain exact. Prefer a narrow fixture parameter over changing every producer fixture's default. Do not mutate queue metadata or CAS request bytes after sealing.

## Module map

- `src/prismabuild/pool.py`: consolidate supervision readers on the existing `_sealed_action_request` owner. Keep execution and drain behavior unchanged.
- Existing GPU scope fixture/request builder: permit a sealed timeout in the explicit deadline case before `queue.publish` and claim. Preserve assertion bodies.
- Existing producer request builder or `_publish` test helper: permit an explicit sealed timeout for the withholding case. Other no-policy fixture calls retain no deadline.
- Deterministic tests: retain default activity beyond the old worker ceiling, quiet no-progress stop, explicit sealed-deadline precedence, and progress-unbounded behavior unchanged.
- Documentation: clarify fixture intent and structural ownership only. A production contract/default change would contradict this recommendation.

## Verifiable sequence

1. Parent reproduces the two exact unchanged IDs through PB and retains original signatures.
2. Refactor supervision request reading first as a separate structural commit. Run focused supervision/authority checks. The two exact stale-context failures should remain reproducible.
3. Seal finite fixture authority at request construction, preserving every assertion. No timeout widening, guard weakening or source-specific production branch.
4. Re-run the two exact IDs through PB, plus all affected fixture users and no-policy/progress/default-stall/deadline/drain coverage. Confirm explicit deadlines and funded/unfunded admission pass while default sealed holders remain unbounded.
5. Refresh scoped acceptance and obtain independent changed-head review. No runtime publication or whole-requirement promotion.

## Rubric

| Criterion | Result |
| --- | --- |
| Shared authority | Existing validated request owner becomes authoritative for all supervision readers |
| Requested behaviors | Production #1707 semantics remain; fixtures declare required finite authority |
| Minimal API | No public API or policy expansion; narrow fixture parameters only |
| Test scope | Exact IDs, affected fixture users, authority readers and existing no-policy/progress/drain guards |
| No new fallback | Unreadable or mismatched authority still refuses; no implicit lifetime bound restored |

## Red flag screen

A new standalone supervision module would be shallow here. Reuse the existing owner and delete duplicated parsing. A new public execution policy object would leak distinctions that production callers already express correctly. Keep fixture preparation in its existing request builder, not an after-publication mutation or a production special case.

## Synthesis decision

Root selects the final shape. After its confirmed-history clarification, I recommend alternative B and reject Candidate A's semantic expansion.
