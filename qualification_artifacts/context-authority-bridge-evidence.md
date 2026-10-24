# Context authority bridge hardening

## Scope and result

Changed only the private context-helper block in `src/prismabuild/core.py`, plus this evidence document. The worktree already contained runtime, shim, publisher, pool, acceptance-adjacent and other changes; these were preserved. This change does not alter runtime attestation, Popen/redirect mediation, existing callable signatures, wire records, acceptance sources, or configuration schemas.

Before the change, a child ancestor's own canonical sealed context and locator were accepted for both a configured executor and an executor with policy=None: the probe printed `forged` in both cases (receipt below).

The executor now holds a sealed canonical memfd for every context, including an explicit denied record when policy is None or owner/marker is absent. Failure to create/seal the anchor raises RuntimeError before yielding: continuing native execution without an anchor would reopen the forgery. The provider requires the locator's PID, birth, descriptor and file identity to match a context in the oldest same-uid ancestor holding a canonical named sealed context file. It scans all applicable ancestors rather than accepting the locator's nearest matching ancestor. Multiple descriptors/attempts in one executor remain supported. Denied records never reconstruct a policy.

Bounds: 128 ancestry iterations, 4096 descriptors per process, 16384 descriptors total, 64 KiB process status, existing 4 MiB context payload. Access errors, fd churn, identity changes, invalid canonical files, exhausted bounds, and malformed/absent locators refuse authority. Process birth and parent links are rechecked after scanning.

## Assumptions / limitations

- Linux memfd sealing and a genuine, readable procfs in the provider's PID namespace. Unsupported or uninspectable deployments fail closed; unavailable memfd now refuses the entire anchored attempt rather than silently continuing native execution.
- The executor remains alive, retains its descriptor, and is an ancestor of the synchronous worker/shim call. Descendants do not control the executor's descriptors, memory, or procfs. This is not a sandbox against arbitrary hostile same-UID debugging, descriptor injection, privileged UID transitions, or compromised executors.
- Scan scope ends at process-root or a different-UID trusted supervisor. UID comes from `/proc/<pid>/status`, not proc-directory ownership: a non-dumpable same-UID ancestor must cause refusal, not be mistaken for a root-owned supervisor. The status check considers real, effective, saved and filesystem UIDs.
- The bounded oldest-holder approach intentionally can deny service during ancestor fd churn or with excessive ancestor/fd counts. An older context in a nested executor lineage remains authoritative. No new hostnames, local installation paths or deployment endpoints are embedded in application logic.
- This protects executor-backed generated-worker direct shim calls, not arbitrary shim calls with no trusted executor anchor anywhere in their ancestry. General hostile same-UID scope was not added.

## Checks

Final source, both arms:

```
python -m pytest -q \
 tests/test_container_class_launch_acceptance.py \
 tests/test_class_image_config_failure.py \
 tests/test_core.py tests/test_pool.py \
 tests/test_docker_shim_global_options.py \
 tests/test_docker_shim_cpu_affinity.py \
 tests/test_docker_shim_memory.py \
 tests/test_docker_shim_relays_termination_1599.py \
 tests/test_container_absolute_client_execution_acceptance.py \
 tests/test_container_class_admission.py \
 tests/test_container_class_requirements.py \
 tests/test_a_declared_container_image_is_sealed_and_checked.py \
 --basetemp="$TMPDIR/pytest"
```

- Default x86_64 / Python 3.14.4: **521 passed, 2 warnings**.
- `arm` aarch64 / Python 3.12.3: **521 passed, 2 warnings**.
- Warnings are multi-threaded fork DeprecationWarnings in the pool test's bounded-reader path, not check failures.
- The launch/config acceptance files account for 47 passing cases on each arm; their source digests remained unchanged.

Additional final-source direct-shim probes on both arms imported the unchanged launch acceptance harness and injected a forged context into its generated worker **in memory only**. All six run/create/exec aliases, configured and None executors, with forged versus original locators: **24 scenarios per arm passed**. Forged locators produced refusal and zero actual client requests; original executor locators still authorized the legitimate initial request despite the nearer forged memfd. Post-inventory-change launches refused; inventory operations remained available. These probes exercise direct shim invocation, not just Core redirect restoration.

Additional final-source helper probes on the default arm passed: positive executor policy reconstruction, explicit denied anchor, removed/malformed locator refusal, all three scan bounds, scan PermissionError refusal, memfd-unavailable failure before yield, and a real non-dumpable same-UID ancestor with a forging child. The latter still refuses authority rather than treating changed proc-directory ownership as a trusted UID boundary.

No acceptance edits or test files were written. Inline probes are recorded by the run_tests operations. Initial probe-development attempts failed due to a wrong schema literal and a misspelled private environment-constant name; corrected probes completed successfully. An attempted arm name `cpu` was rejected without running; the available secondary arm is `arm`. No full repository suite or live deployment qualification is claimed.

## Retained evidence

Pre-fix reproduction (configured and None executor both accept `forged`):

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"88871e2d2fd892cf88c8d008c895614675fca63f65123ce3c4fc08179151a228","size":102}},"operation":"ded783f39fa4e478726c540f6b71f24ec0c27e18d7f2302a0aef622f34acdaa8","receipt":"17d09ba130f397219e04361f91ccfa5e36a81f2874d21aa5247c47c2367b0854"}]

Final combined acceptance and guards, default:

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"5f54f00f55e15d74f33c2f6cb2c423dc7055c456c236f332f7db25c4d3ac5dae","size":4484}},"operation":"44e98f3087beb4605da1cd92e03d97bec84b3e175c1fd5cbc951c90eb3fe354c","receipt":"07536636c341812f43c2642892795a8347b01f37b3e6648cf022c4d49af57d21"}]

Final combined acceptance and guards, arm:

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"049f54f5d978e34ecc7cd1af87ef09bce6f24c25e2f73f773bbcfa33d7c9b0b6","size":4494}},"operation":"4ef90a05984feaefea3db7762e7f17e5ff88112dbaaebbc3a86fb31af94e8a74","receipt":"d3bd27a7d1c1830662406372de0b86acefe0f78e03a37bcd63472b37b07d91e0"}]

Final direct-shim forged-context matrix, default:

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"48cbd93fcf454e860156972575e6cf6185fcffd8ddb921470dfa03ce6ff208f2","size":1496}},"operation":"b25687692afb6530a72688d0e7a35b06e98ce804572a1fd1a4378e7d4e1df7c7","receipt":"9a11b99ec224b1dbbf8c58ae0cdfcf351c003a7bb2eaee2cb520313df44bb0f2"}]

Final direct-shim forged-context matrix, arm:

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"48cbd93fcf454e860156972575e6cf6185fcffd8ddb921470dfa03ce6ff208f2","size":1496}},"operation":"f25b6967ee285bde8eca64c01042eb930b4bf57a082b0a1321ac28d7e4d2bf1d","receipt":"50d52896a2bc1fa2093e25871b683ed3975701ed4b6c226fdae383ff953c6117"}]

Final helper fail-closed probes, default:

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"af6afbb66e660792a6de1bf2b27a6c9dac21a8661e87569f5c15927ea62d6220","size":348}},"operation":"f0d25429f19b1c46f6c78909aab9bc693225b2ed772c032a7c6b6f21e5a401c8","receipt":"9382baf87cb7af676420be5a0cd7e0bd0efced648b6017deac4746fb827568ef"}]
