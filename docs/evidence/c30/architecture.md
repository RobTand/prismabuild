# Explicit fleet worlds

The caller names one private root. It may compose an admission adapter, a clock,
host observations, tier supply, landed evidence and a short broker directory.
Queue transitions and records still come from production writers.

```python
world = FleetWorld(tmp_path)
world.clock.install(monkeypatch)
world.use_host(monkeypatch, "fixture-box")
world.host("fixture-box", capacity={"cpu": 2, "mem_gb": 4})
world.actions.publish(key, resources={"cpu": 1, "mem_gb": 1})
claim = world.queue.claim(capacity={"cpu": 2, "mem_gb": 4})
```

`Admission(capacity, default_demand)` composes the existing explicit simulated
admission adapter. Omitting that adapter leaves the real queue's missing-demand
and missing-capacity behavior intact. Explicit `None`, empty and zero inputs
continue to reach the production queue. No new autouse fixture installs a world.

The source trace starts at family-owned pytest queue factories. They construct
`PoolQueue`, optionally wrap it in `AdmittedQueueFixture`, then call
`RecordStore.ensure_layout`. The world keeps that order. `ActionRows.publish`
owns the repeated queue-row defaults. Its rows are state-machine inputs, not
sealed CAS execution proofs. `landed` composes the existing production fragment
and move-record writers; it likewise grants no physical execution credit.
The live-store guard remains active.

Two designs were considered. An explicit facade composes these existing
adapters and leaves unusual fault setup in the owning test. A global fixture
registry would select a profile and install all its patches automatically.
The registry hides mutable state and couples independent tests, so the explicit
facade was selected. Malformed records go through the named `corrupt` path.

The migration lever compares complete original helper bodies, preserves pytest
fixture names and decorators, removes replaced ordinary helpers, and records
every changed caller. Source assertions must remain identical after restoring
the exact helper and clock bindings. Runtime qualification covers every changed
whole file, shared-fixture importer and all eight architecture controls.

The initial source is `b26d6bc94abb3c977bbf04650ac27d0d6535277a`. C10's final
composition will change the world's single tier mint call to the tier owner.
No unfinished C10 source is imported here.

## Measurement clock ownership

The measurement fixture creates the clock before constructing its queue. It
installs the time and hostname observations first, then constructs
`FleetWorld(tmp_path, clock=clock)`. Its samplers, action publication and gang
fixture consumers all retain that exact clock object. Two independent foreign
load fixtures also use `Clock`, while retaining their existing patch targets.
The private world does not install observations implicitly.

One candidate kept the mutable one-element lists and taught `Clock` to expose
list indexing. That would keep two interfaces for the same state and obscure
which consumers had migrated. The chosen design replaces every access in this
fixture family with `clock.now`, including stores and augmented assignments.
`Clock.advance` remains available for callers that want an explicit operation.
`FleetWorld.use_host` is static because it owns no world state, so the existing
hostname-before-queue order can stay explicit.

The source lever follows the current fixture import closure, identifies all
three old clock constructors, and restores every changed module AST to its
original form after reversing the exact clock and setup bindings. This retains
all existing assertions, decorators, callback ordering and native IDs. The
whole affected importer closure must execute before this wave is accepted.
