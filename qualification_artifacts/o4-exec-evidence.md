# O4 descendant executable mediation — source/test evidence

## Implemented scope

Owned `src/prismabuild/_container_exec.py`, the `run_local_action` Popen integration in `core.py`, and the corresponding execution-contract paragraph in `docs/design.md`. Preserved the concurrently supplied private class-context provider. No tests modified. No deployment, queue transition or publication performed.

Linux ptrace follows fork/vfork/clone/exec. Kernel exec stops identify the configured client's executable identity, or its kernel-inserted shebang script slot. Before client user instructions run, GETREGSET/SETREGSET and procfs memory redirect exec to the existing Docker shim. This uses the same current endpoint/class qualification and ownership/lifecycle behavior, rather than classifying only the initial command's argv. Already-mediated shim descendants are not wrapped twice. Action argv, child JSON bytes, sealed action identity and executable attestation inputs are not rewritten. The private context locator is overlaid from executor context/inherited executor environment, not from action variables.

Tracees retain the existing session/process group. TERM is forwarded and reaped; EXITKILL covers tracer death. Original action signal deaths remain signal deaths on Popen. Missing tracing/procfs/ABI/shim capability fails closed when relevant rather than silently executing a configured client unmediated. No tracing is added when the configured client path does not exist.

## Observed availability and limits

Executed on Linux x86-64, Python 3.14.4. ptrace, procfs and native launcher compilation were available: all eight direct/native-child acceptance cases passed, each with its native and inventory positive controls. Native-child argv is obtained by the launcher from sealed JSON, not inferred from initial command arguments.

AArch64 ELF registers/syscall injection are implemented using GETREGSET, but no ARM host check was run. Not a hostile-code sandbox: copied clients, raw daemon APIs, namespace manipulation and same-user ptrace attacks are outside the supported boundary. No live production Docker or deployment qualification is claimed.

## Results and remaining integration failures

- Initial acceptance run failed all eight inventory controls with SIGSEGV. Fixed by reaching execve syscall-exit stop before setting injected registers; the kernel otherwise overwrote x86 RAX.
- Acceptance rerun: **8 passed**.
- Expanded run: **188 passed, 5 failed**. Two legacy direct-shim lifecycle tests now lack authoritative class context and are refused by the concurrently changed shim. `test_worker_core_has_no_unattested_repository_imports` identifies both the context provider's `container_images.py` and this runner's `_container_exec.py` as absent from worker runtime identity. **Runtime closure/identity integration remains required before merge/deployment.** This change does not hide the import or weaken the invariant. The other two failures were SIGKILL returncode propagation (attempting to reset SIGKILL's handler); fixed.
- Expanded rerun excluding only the known runtime-import identity invariant and omitting the two legacy missing-context shim expectations: **187 passed, 1 deselected**. Includes all absolute-client acceptance, class-launch acceptance, holder/container-transaction and other core checks.
- Final source rerun after avoiding duplicate wrapping of explicit shim launches: **10 passed, 128 deselected** (eight absolute-client cases plus two signal checks).
- `git diff --check`: passed.

## Retained test evidence

Initial acceptance failure:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"de3c5115ff9e62435e759afba458207c3d942ed6352f2b0fb5271a9d5d2b0a83","size":126112}},"operation":"0dc5ecb3981bd0f188ddfff9b91f0366120a59ba8bb5cb32f41b270f3d7f47b5","receipt":"8abcace21c670525a6c1ff459dc92478b3e27c1e0f67be1f66bfb0251dc87c4b"}]

Acceptance success:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"b452bfb9998fd575e072de0797107a07cd90ef08e28ea1209cc438237f00ea62","size":1206}},"operation":"a5860813f3541454020aabe520b8255a06a1396c4272b8ec8965779822e992ce","receipt":"be94448808aa8bcf676ec83b4f07ce3ad3be8d70b8c42fad9acd10ab65069be5"}]

Expanded run with failures:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"9ed00eb6e69abb828110e1e6d75ab4bf8a81a32db4de0469a6da7dd4bdb56e7a","size":13198}},"operation":"e8ec42d266156f4bfde19007992a3ec143ebd5c80f03d7c84994524e1202a250","receipt":"67dc4598230747bc22d97c697731f9a0d6df6453e91cd878545836887999d704"}]

Expanded rerun:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"ebbd007d4814b1ece88d9b186493ba49e668bc649a718d6f2849420782fcdd41","size":2049}},"operation":"2e4e964114c0b4b66485f861c813aaec54c6c734b78d6de6dd89e412a07f9959","receipt":"15bdd3cb8408943d2594c5fc569299456629e71aa5f32cbaf2d687d3b3351e13"}]

Final source acceptance/signal check:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"48919c5d9d7b1a8eec475656a932d822f485b2f723e29a6f87cb1cedfbf43a19","size":1357}},"operation":"e76b35f853310655f7beceb985b79a9082551b3fe2933d75c42199e6d0eb94e4","receipt":"200c65abf3d6e1cc442393885d9d60dfb2d91fb1343ada8b8ae03007ee5e195c"}]
