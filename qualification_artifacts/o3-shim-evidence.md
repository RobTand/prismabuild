# O3 Docker shim implementation evidence

## Scope

Changed `tools/fleet/docker`; added private `src/prismabuild/_container_launch.py`.
No acceptance tests changed. Other concurrent checkout changes are not attributed
to this implementation.

The helper uses `ClassImagePolicy.evaluate` and an `InventoryCache.snapshot`
with `CLAIM_FRESHNESS_S` on every supported launch. The shim gates run/create,
container run/create, exec and container exec. Inventory verbs retain their
existing forwarding path. The gate precedes marker/resource transactions and
container workload invocation. Effective Docker endpoints are pinned by the
existing CLI resolver and restricted to the local system daemon before class
qualification.

## Required integration interface (pool/core owner)

Implement `prismabuild.core._container_class_launch_policy(*, owner: str,
marker: str) -> container_images.ClassImagePolicy | None` in the shim process.
It must fetch policy configuration from executor-produced private runtime
mediation, bound to the actual action attempt/owner/marker. It must not trust
an action-supplied satisfied flag, class policy environment JSON, or persisted
offer authority. Missing/invalid authority may return None or raise ValueError
or OSError; all refuse workload with exit 125. The shim independently reloads
fresh host-local evidence through the helper, so the provider must return
configuration, not a prior qualification verdict. Runtime packaging must include
`src/prismabuild/_container_launch.py`.

## Checks

- `git diff --check`: passed at inspection time.
- Acceptance: **38 failed, 6 passed**. Inspected retained stdout: the authorized
  first launch fails with `class_context_missing: no authoritative container
  class launch context` because the core mediation interface is not implemented
  yet. This is an integration dependency, not a completed O3 acceptance proof.
- Inventory and profiling refusal regressions: **151 passed** across
  `test_container_image_inventory.py`, `test_nsys_docker_refusal.py`, and
  `test_sample_docker_coverage.py`.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"27d202c6e5908718ee075e4cca97bfd64d81a71d14a2d06e799e27ad82c46cf0","size":62335}},"operation":"2f35999482cbec517bd732e72f5aa9a2ccb35b6cadaba011a4b249052862edad","receipt":"5348b41a409330498791d7c620188d51cbacd4e70b976dc29ae0ff6a41ef403a"}]

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"c5439135672eb8b80b0236c3071f516f61505c5ad525ec198457e4cadd28c047","size":1673}},"operation":"7ad2283db8bd985bb77eb3f95b1c873c6778a485e9f9c4de67c985138e2dedbc","receipt":"035dd2c2788ab4d7379eb2f66b19d6205c39379d020f3920b7c5e42ecf838971"}]
