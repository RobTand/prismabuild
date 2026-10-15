# Physical proof test execution

`tests/proof_execution.json` declares the actual-root proof population and its
resources. Seven root files contain 104 cases. Three relevant native files
contain 78 cases, including the 40 pure identity predicates. Their family union
is 182 cases. The six process-identity cases live in the root real-writer module;
their function, parametrization and assertion syntax is pinned in the metadata.

Run actual ownership and permission observations in a private root container.
Keep ordinary permission tests in the unprivileged worker context. No test skips
itself based on UID. An incorrect execution context fails its real UID/GID or
filesystem assertions.

All test execution, collection, imports and probes use the published PB clients.
The coordinator may inspect source and stage immutable inputs. The prepare tool
never imports project/test modules, launches a test or assigns work to a host.
PB owns partition and placement.

## Inspect the declared boundary

Use an isolated frozen checkout. The source-only inspector checks every root
fixture importer, every requesting writer module, the moved test syntax, source
case axes, and the disjoint file complement using PB's `test_*.py` discovery rule.

```sh
proof_checkout=/path/to/isolated/frozen-checkout
cd "$proof_checkout"
python3 tests/prepare_proof_execution.py --checkout "$proof_checkout"
```

Counts from source are an expectation. They do not replace admitted collection,
actual call outcomes, or source/terminal/log/CAS verification.

## Prepare the root inputs

Use a new verification checkout, preserving every completed source snapshot.
Preparation reads the bundled `tests/fixtures/proof-readers.tar.gz` by default.
It verifies the archive SHA-256, exact regular member set, and every member's
length and SHA before copying. It reads members with `extractfile`, never
`extractall`. No Git objects, network fetch or Python package fallback is used.

```sh
python3 tests/prepare_proof_execution.py --checkout "$proof_checkout" --prepare
```

An explicit `--prepare-from DIR` instead reads only that supplied directory.
All six declared members must be present and match their pins. An empty, missing,
incomplete or wrong input fails without falling back to the bundled archive.

Preparation also copies 497 pure tooling files from the declared distribution
RECORDs without importing the packages. Each recorded hash and size is checked.
The default site-packages path is in the JSON; `--site-packages` can name another
source containing those exact distributions. No package install or image build
is required. Missing or changed input fails preparation.

`protocol-two-upgrader.py` always means the actual retained79 updater.
`latest-proof-upgrader.py` is the distinct actual b36 artifact. The current
installed-reader positive tests execute the frozen checkout's actual updater
bytes, so a historical fixture does not substitute for a later production fix.

## Submit the actual-root population

The tracked adapter accepts only the declared root files and defaults to all
seven. It verifies the pinned inputs and local image, preserves the admitted
affinity inside Docker, and asserts actual UID/GID 0. All writes remain in the
container-private filesystem. Source, tooling and artifacts are read-only.

```sh
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbrun.py \
  --cwd "$proof_checkout" --cpus 1 --demand mem_gb=4 \
  --container-image sha256:df18428df8a0f9189c84e7424d214b4c50092af1f9dca5842a00c41c39bda316 \
  --env OMP_NUM_THREADS=1 --env MKL_NUM_THREADS=1 --env OPENBLAS_NUM_THREADS=1 -- \
  /home/rob/venvs/pb-cpu/bin/python tests/run_proof_container.py
```

The image and absolute outer interpreter are explicit placement dependencies.
The adapter uses Python 3.12, native threads one, a 3 GiB Docker memory limit
within the aggregate 4 GiB admission, no network and standard container
CHOWN/SETUID/SETGID. It does not use privileged mode or writable host mounts.
Keep portable eligible placement and the PB-assigned affinity.

## Submit the ordinary native suite

Published `pbtest` accepts positional test files. It does not expose a container
execution option, arbitrary environment forwarding, `--snapshot-ref`, or pytest
`--ignore`. Use the explicit file complement instead of inventing those options.
The formatter prints filenames only; it neither launches jobs nor shards them.

```sh
set -euo pipefail
python3 tests/prepare_proof_execution.py --checkout "$proof_checkout" \
  --paths native-suite > "$proof_checkout/qualification-native-paths.txt"
mapfile -t proof_native_paths < "$proof_checkout/qualification-native-paths.txt"
test "${#proof_native_paths[@]}" -gt 0
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbtest.py \
  --checkout "$proof_checkout" \
  --python /home/rob/venvs/pb-cpu/bin/python \
  --cpus-per-shard 1 --threads-per-shard 1 --mem-gb 2 \
  --json "$proof_checkout/qualification-native-report.json" \
  "${proof_native_paths[@]}"
```

The file-list command must succeed before submission. An empty list must never
fall back to `pbtest`'s default full-suite selection. For a bounded 78-case family
qualification use `--paths native-family` in the formatter; the PB command stays
the same. The complete native suite contains additional unrelated cases.
Each action declares its actual resources, and PB chooses partition and eligible
workers. Native tests run as the ordinary unprivileged worker.

## Immutable retained reader sources

Native fixtures consume the same bundled archive directly, so a fresh checkout
needs no historical source staging or special `pbtest` option. The archive is
92,405 compressed bytes, containing exactly 376,155 raw source bytes / 7,692
historical source lines. It contains six unchanged earlier source files. Their
original revisions, paths, lengths and SHA values are declared in the JSON.
Those revisions describe provenance; tests do not read their Git objects.

The USTAR entries are filename-sorted regular files with mode `0444`, UID/GID
zero, empty owner/group names and mtime zero. Gzip has an empty filename, mtime
zero and compression level nine. The immutable archive's SHA-256 is
`02f96d28e3ec6f0ffeec18634868ec7fef88c895d814888415e6cae6123eef3e`.
These are historical test artifacts, not production edits encoded as data.

The existing root adapter intentionally sets `PRISMABUILD_PROOF_ARTIFACT_ROOT`
to its prepared read-only directory. If that variable is explicitly present,
the fixture provider uses only that directory and requires all six exact
members. An invalid override fails; it does not try the archive, Git, another
package or current production source. Ordinary native PB needs no environment
override and reads the bundled corpus. Keep retained79 distinct from b36 and
the actual current updater positives. Do not infer semantics from a protocol.

Qualify the retained native family in an independent single-commit source
capsule containing the frozen candidate files but none of the original five
historical commits, alternates or remotes. Record the missing historical
objects inside the admitted action before actual execution. A passing run on
the author's complete Git history alone does not establish this closure.

## Reconcile qualification

Record exact unique selected node IDs and call outcomes for both contexts. The
sets must be disjoint and their union must cover the declared family. The root
104 and native 78 subtotal must reconcile to 182. The full ordinary suite's
additional count comes from admitted collection. Record every setup/collection
failure, skip, unavailable tool and missing input. None counts as a passing call.
Check source snapshot identity, terminal return codes, log hashes, and actual
successful CAS receipts/payloads or the expected receipt absence for a frozen RED.
Never treat submission acknowledgement as qualification.
