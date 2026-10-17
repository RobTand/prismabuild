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
