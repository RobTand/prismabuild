# Shared declaration owner with truthful bounded fixtures

Status: competing design sketch. No implementation or qualification is claimed.

## Caller usage

A submitter requests a finite deadline through the sealed action:

```python
params = {"demand": demand, "execution_timeout_s": requested_s}
action = core.seal_action({... , "params": params})
```

The worker keeps its current public call:

```python
outcome = queue.execute(item, timeout_s=worker_ceiling_s, containment=True)
```

A sealed action with no requested deadline keeps activity or semantic supervision. The worker ceiling bounds quiet allowance and a requested deadline. The ceiling does not create a total deadline.

A withholding fixture that requires a finite incumbent declares that context:

```python
owner = _publish(tmp_path, queue, template, demand,
                 timeout_s=pool.WITHHOLD_CEILING_S)
```

The GPU host-fault case seals its requested deadline before publication. Its existing call, timeout value, status assertion, termination assertion, and host-fault assertion remain intact.

## Problem and grounding

The two exact Vehicle12 failures are real, and their original results stay failed. Their setups do not currently declare the bounded contexts their assertions require under #1707.

The funded-export fixture seals a producer without `execution_timeout_s`. #1707 intentionally classifies that producer as advisory `unbounded`; `docs/design.md` records the change. The original independent review and approval explicitly called out that scheduling difference. The test stops at the first foreign-refusal assertion, before it exercises funded exports.

The GPU fixture seals only `params.demand`. `execute(timeout_s=1e-9)` supplies the worker ceiling. It does not seal `execution_timeout_s`. The activity allowance is therefore clamped to `1e-9` and correctly expires as `no_progress` under the current design. This is not proof that a sealed explicit deadline lost precedence.

Production repeats the declaration read and interpretation in `_sealed_execution_policy`, `_declared_run_bound`, `progress_policy`, and `_sealed_stall_allowance`. `execute` computes the record, then `_execute_in_checkout` rereads part of the authority. `_measurement_reservation.census` independently consumes `_declared_run_bound`. A shared owner removes the chance that execution, receipts, and advisory classification interpret the same sealed declaration differently.

## Shape

Keep this change inside `pool.py`, beside the current execution declarations. Do not create a module for a handful of fields. Reuse `_sealed_action_request` as the existing validation and key-binding boundary.

```python
@dataclass(frozen=True)
class ExecutionDeclaration:
    request_present: bool
    requested_timeout_s: float | None
    requested_stall_s: float | None
    progress: Mapping[str, object] | None
    pool_contention: Mapping[str, object] | None
    awaited_batch: Mapping[str, object] | None
    profiled: bool

    def advisory_bound(self) -> tuple[str, float | None]:
        ...  # Existing legacy, progress, deadline, or stall classification.

    def budget(self, ceiling: float | None) -> ExecutionBudget:
        ...  # Existing clamp and no-default-deadline rule.

    def progress_policy(self, ceiling: float | None) -> ProgressPolicy | None:
        ...

    def default_stall(self, ceiling: float | None) -> DefaultStall | None:
        ...


def execution_declaration(
    item: Mapping[str, object], *, max_bytes: int | None = None,
) -> ExecutionDeclaration:
    ...  # Read and validate once, derive all fields from that authority.
```

The factory alone builds declarations. Existing core validation owns positive finite values, progress schema, and the incompatible progress-plus-stall combination. Preserve legacy absence as a distinct fact. Corrupt or mismatched requests still raise instead of becoming legacy.

`execute` loads the declaration, derives budget and watchers, and passes the already resolved `profiled` flag and effective deadline into `_execute_in_checkout`. The latter stops rereading and reclamping the declaration. Keep the watcher classes and stop ladder unchanged. Existing `ExecutionBudget`, `ProgressPolicy`, and `DefaultStall` remain the record owners.

Migrate the measurement census and holder classification to the same declaration in the same wave. Remove superseded parsing implementations. If narrow test adapters remain, they must exercise the shared owner and preserve any meaningful read-size or error-boundary contract.

The interface hides one body of policy knowledge: how sealed declarations determine supervision and advisory bounds. It leaves actual clocks, observations, stop precedence, and resource settlement with their existing owners. This follows separation of concerns and boundary discipline. The refactor must be behavior-preserving before fixture changes.

## Synthesis recommendation

Use this candidate as the minimal base unless new runtime evidence proves an actual production semantic error. A production structural repair is justified by repeated interpretation and repeated CAS reads. A production behavior change is not yet justified by these two failing setups.

First refactor the shared production owner, preserving all decisions. Then repair each fixture at the point where it requests its bounded context. Commit that order. Existing exact test IDs and all substantive assertions remain. Add explicit paired coverage for sealed unbounded and sealed bounded behavior so future integration cannot confuse the two contexts.

## Alternatives considered

- Change `execute(timeout_s=...)` into an explicit caller deadline and add a separate ceiling parameter. This can make the GPU case pass unchanged, but changes the public meaning used by worker loops, semantic tests, mover tests, and direct callers. Every worker-loop call currently supplies a ceiling, so all such calls need migration. It is a broad behavior change with no demonstrated need.
- Give young stall-governed holders the historical transient age presumption. This makes the export assertion pass with the existing producer setup, but reverses the documented `unbounded` classification. It can withhold work behind an active producer that has no declared end. It also requires matching measurement/backfill review.
- Patch only fixtures. This restores intended test context cheaply but leaves the duplicated production policy owner and fails the latest refactor-first instruction. Use fixture changes only after the structural refactor.

## Acceptance scope and causal wording

Retain the original Vehicle12 attribution literally as a source behavior difference. Add a correction explaining that historical test bytes relied on pre-#1707 defaults. Attribute the diagnostic signatures to absent finite declarations, not to proved loss of sealed-deadline precedence or a proved funded-export bypass failure.

Retain immutable original logs, requests, source, and failed action keys. Do not relabel those results. Compare original and corrected fixture requests to show only the intended finite declaration changed. Preserve every expected outcome and timeout number. New keys must distinguish the corrected sealed context.

Prove through PB that both exact IDs now pass, real sealed deadlines beat simultaneous stall and host-fault decoration, default active payloads outlive the old ceiling, and funded exports pass while unfunded exports retain their withhold. Preserve the existing finite-holder, unbounded-holder, and measurement episode tests. Keep staged-read requirements, deployment axes, and release state unchanged.

A source-only acceptance can cover the shared owner and corrected bounded-context fixtures. It cannot claim a repaired production precedence bug without a failing sealed-deadline control first. Independent review must assess this narrower causal account and the changed source head.

## Evidence and limits

- `fd11320dfc9bc1e609a1e4ebaa84f3be22aec433` already corrected the exact export fixture by sealing `execution_timeout_s=WITHHOLD_CEILING_S`, with assertions unchanged. That commit remains on `origin/prismabuild-1710-batch`, outside Vehicle12.
- [Original independent review](https://github.com/RobTand/prismabuild/pull/1756#issuecomment-6106500182) and [approval](https://github.com/RobTand/prismabuild/pull/1756#issuecomment-6110622227) explicitly describe sealed default holders becoming unbounded.
- At `efff85c6f1c4`, `tests/test_gpu_runtime_integration.py:161` uses the `scoped` fixture from `tests/test_pool_resource_scope.py:14`. Its sealed params contain demand and no explicit deadline.
- The current takeover requires production-owner refactoring and prohibits guard or assertion weakening. This candidate honors those constraints through structural consolidation and accurate action declarations. It does not infer authority to reverse the established default supervision contract.

## Next implementation step

Consolidate sealed execution interpretation behind `ExecutionDeclaration`, then prove the unchanged current-head behavior before changing either fixture context.
