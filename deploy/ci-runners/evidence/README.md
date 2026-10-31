# Runner deployment evidence, 2026-10-31

Implementation tested through PrismaBuild: `3e4f6518`. Evidence-only commits do not change that implementation.

All six PB slots are enabled and online in `runners.json`. Their placement and enabled state are in `host-units.json`.

| Host | Slots | CPUs | Memory each |
| --- | --- | --- | --- |
| sparky | 2 | slot 1: 0-4; slot 2: 10-14 | 8 GiB |
| sparklina | 2 | slot 1: 0-4; slot 2: 10-14 | 8 GiB |
| dl380g10 | 2 | 40-79, four-CPU quota per slot | 6 GiB |

Both Sparks had over 110 GiB available before enabling both slots. No delegation or fleet capacity was changed.

## Images

The immutable local IDs and architecture-specific manifest digests are in `images.json`.

| Host | Architecture | Manifest digest |
| --- | --- | --- |
| dl380g10 | amd64 | `sha256:d3c55aadfe526bf457ce34a5caa9cb84dbe5660e6fec4e3fd79afb35e739dfd1` |
| sparky | arm64 | `sha256:73569de293f1b6a41653a9bfd7be914025bc4a6332f888f01420072b4287efce` |
| sparklina | arm64 | `sha256:ac21bd7f1c9ff84a739bbcbec87ecb5ae996c8cc2472bb30840e408d828aa8c6` |

## PrismaBuild proof

Four red runs demonstrate missing scripts, a missing constrained SSH broker, lost affinity, and a missing systemd 255 restart-counter reset. Each failed for the named behavior. Failing actions publish no success receipt. Their terminal state, test counts, and action keys are in `prismabuild.json`.

The final 17-test file passed five fresh runs per architecture, including two runs with two xdist workers per architecture: 170 test executions, no skips. The final tautology guard also passed. All successful action receipts were verified with the published worker verifier against the retained action manifest. `prismabuild.json` records receipt paths, digests, terminal cleanup records, and verdicts.

| Run | Action key | Receipt SHA-256 |
| --- | --- | --- |
| final-x86-1 | `606031f8a50579a010893e7264c94e7a132bef3073a9a495b2295aa5638c9f42` | `8926ad53216035b11f1c49ab49edb4855d3b69a671f44d5c79f6aaefa86df657` |
| final-x86-2 | `adec4295c0e84847589df568aacbb2ec1b4339cd527fe6b7b2445098b0808f0a` | `a74394b002da223a4fe2495d10ceeb586f927a76743fcafb230fe9da231ebd05` |
| final-x86-3 | `cf8f1e3d5d6dc345f0b144d7aef2bd246a602c7954091cc0dec3e5ff87ff01e9` | `997138e857bc82f3eed5bc939f7cf16b8739bece28307dddd67682279f787f55` |
| final-x86-4 | `efb5890b78fbb0b63087343fd5bb404ffc4e913ebea80c72df5677613b51146f` | `de0c6faeabb6031a1be8f2bcf46714e0782ac39ee822cef946c5b82bbc3c1a66` |
| final-x86-5 | `1a1983ac19e2d414320a7a762b23eda4d9cbfac8fd9104d759ec25f60f8296a5` | `2dc4708f36853351bc8e564ae24777bebc36cf3fc705b7695d81e1b9e6549604` |
| final-gb10-1 | `42e42351cb2b4d7cb85349b8e36b2266cfaeeb2040eca878d67703efb7385df4` | `96f289a83bfec635b9cba800f31a16a8a41d3aa9d26c4109de274360a7ff1dfb` |
| final-gb10-2 | `1cc682c64638bcbd94b48d4e8ac2e9944e1cb1a9ecdb22825066b9440510dd78` | `a280c4e28bc072f7546aceec0bbaa3a30ef2445bc49a3a97b242c2c1a7efa99f` |
| final-gb10-3 | `55432fed5ee06f128bee1c83662f601d292372c412420513db26ce7b08296255` | `d7f3cb7810a1fa81d4196a868c612b53c9c4418d2210df0392910ef97a2cb3cf` |
| final-gb10-4 | `62a07a27949c7f032b71da1a8c0fa99406f3e1265ce30353d4b4cf38f0a063b4` | `118c5266d3ba2d45bfdab68e8b08d92be0d126d5e1124ec87c91b602deff2ca0` |
| final-gb10-5 | `3f3c3c6e5679516fb9880c24afef82d7c4725764d984a25d56a77025bf91ce23` | `0b69e208bfed4c4df7f53059956c4cda4a8fae28e48cd7962af1af635671bb35` |

The detached test worktrees and complete raw PB reports, terminal responses, verified receipts, and controller logs remain under `/home/rob/tmp/pb-lead/scratch/runners/`. The final worktree is `final-wt`. Runs reserve one CPU per pytest worker, four GiB, and one native thread per worker at priority -10. Architecture tags are used only for the required architecture qualification.

## Live canaries

Attempts 1 through 4 of run 41229105721 passed both jobs. Attempt 5 has a successful gate job on sparky slot 1; its suite job was queued behind other PB CI when captured. Complete REST run and job records and downloaded job logs are retained here. No queued job is counted as a pass.

| Attempt | Job | Runner name | Pickup seconds |
| --- | --- | --- | --- |
| 1 | [gate](https://github.com/RobTand/prismabuild/actions/runs/41229105721/job/124619346306) | `pb-sparklina-slot-1-b73048d1-be17-4c6b-b8ec-dbcf33eb95bc` | 4 |
| 1 | [suite](https://github.com/RobTand/prismabuild/actions/runs/41229105721/job/124619346416) | `pb-dl380g10-slot-1-b411b88f-4c47-4d38-9ad2-0e4ba58cec0a` | 35 |
| 2 | [suite](https://github.com/RobTand/prismabuild/actions/runs/41229105721/job/124619820365) | `pb-dl380g10-slot-1-ba637350-06b7-45a6-ba4c-e24ce2105742` | 1 |
| 2 | [gate](https://github.com/RobTand/prismabuild/actions/runs/41229105721/job/124619820630) | `pb-sparklina-slot-1-32f6dade-659a-49f3-8c75-b342c0914e9b` | 2 |
| 3 | [suite](https://github.com/RobTand/prismabuild/actions/runs/41229105721/job/124620034842) | `pb-dl380g10-slot-2-839250f5-ad72-44f8-a5b9-a57fe0eeb03b` | 1 |
| 3 | [gate](https://github.com/RobTand/prismabuild/actions/runs/41229105721/job/124620034969) | `pb-sparky-slot-2-d3b42b82-c32b-4d6c-bb12-be35146ca8ad` | 1 |
| 4 | [gate](https://github.com/RobTand/prismabuild/actions/runs/41229105721/job/124620320597) | `pb-sparklina-slot-2-a758321e-e86b-403f-b89d-8ad3f9590f23` | 72 |
| 4 | [suite](https://github.com/RobTand/prismabuild/actions/runs/41229105721/job/124620320709) | `pb-dl380g10-slot-1-4c56edec-4900-461d-b9b1-758d6f4103bd` | 2 |
| 5 | [gate](https://github.com/RobTand/prismabuild/actions/runs/41229105721/job/124620910675) | `pb-sparky-slot-1-662ce4a3-0a69-4c4f-a55c-cc61a12f219f` | 2 |

Pickup is job `created_at` to `started_at`, not workflow duration. The 72-second gate pickup followed an idle-slot placement change while GitHub still had an old runner assignment. The runner was reassigned and passed. Warm pickups were usually one to four seconds. These observations do not establish an SLA.

The job logs prove Python 3.14.10, pytest 9.1.1, pytest-xdist 3.8.0, PyYAML 6.0.3, actual CPU affinity, the cgroup memory limit, absent shared storage, and unset JIT input. Both slots on both Sparks have job evidence across attempts 1, 3, 4, and 5. Both dl380g10 slots have job evidence.

## Deployment corrections and remaining scope

The first live install rejected `userns=private` without mappings. A keep-id trial then blocked a PB child during an overlay ownership change on dl380g10. The trial PB units were stopped, the child later exited, and stale PB registrations were deleted. The final script uses the reference design's default rootless mapping. No unrelated container was removed.

The initial live gate saw CPUs 0-19 despite systemd affinity. `initial-affinity-failure.log` records that failure. The final entrypoint restores the service's mask with taskset after OCI startup. The next canaries passed.

The Sparks run systemd 255. The final slot sends `RESTART_RESET=1` and resets only its own PB unit's counter after valid registration, retaining backoff on registration failure. Current units report `NRestarts=0`.

sparklina lacked a return SSH identity. A separate PB key was generated on that host. Only its public key was installed on dl380g10, constrained by the PB-only forced-command broker. No private key or GitHub credential was read or copied.

The dl380g10 runners were already running other PB CI at handoff. Their next normal restart picks up the final host-script counter-reset change. Their image and affinity already passed. Rootless networking still reaches the LAN, and apt package resolution occurs at image-build time. These are limits of the supplied reference design. Full PB suite qualification and queue integration belong to the other readiness slices. Independent review and queue admission belong to the lead.

No shared inventory or registry file was changed. No PB worker, supervisor, broker, queue, shared deployment tree, TL3 unit, or TL3 registration was changed. No celestia slot was used.
