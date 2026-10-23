# Pool CPU environment definitions

`x86_64-cp314.json` records dl380g10's 56 installed distributions.
`aarch64-cp312.json` records the Sparks' identical 49 distributions plus
jsonschema 4.26.0 and the same four dependencies as x86. Both inventories
were captured read-only through PB on 2026-10-23 for PB #2111.
The interpreter minor versions remain CPython 3.14 and 3.12 respectively.

Each package has an exact version, a purpose, and SHA-256 hashes of wheels
downloaded on its host class. `+cpu` is part of the torch version.
The definitions preserve the captured package stack, including architecture
differences. PB's runtime itself has no third-party dependencies.
These pins qualify neither GPU use nor every served project's tests.

`../pb_cpu.py --python /absolute/interpreter check` inspects installed
distribution metadata in an isolated subprocess. Names follow Python package
normalization. Versions compare exactly. JSON reports missing, extra,
different, and duplicate distributions. A match exits 0, drift exits 1, and
an inspection or build error exits 2.

`../pb_cpu.py --python /absolute/base/interpreter build /new/directory`
creates a fresh venv and installs only the declared binary wheels. Pip uses
PyPI and the PyTorch CPU index, exact pins, hashes, no dependency resolution,
and no cache. Installation output goes to stderr. The final JSON report uses
the same installed-distribution comparison as `check`. Failed builds remain
at their destination for inspection. Remove that scratch venv before retrying.
The tool refuses existing destinations and the live pb-cpu directory.

Run checks, wheel downloads, builds, and tests through the published PB
clients. Put build scratch on disk, bound native threads, and declare memory,
CPU, and filesystem growth. On dl380g10 use `TMPDIR=/home/rob/pbt17`.

For a definition change, capture the affected live inventory read-only and
review every changed pin and purpose. Download each exact wheel through PB
with `pip download --only-binary=:all: --no-deps` on the matching architecture.
Use `https://download.pytorch.org/whl/cpu` for torch and PyPI for other wheels.
Record each wheel's SHA-256 in its package entry. Build into a new directory
and qualify affected PB and project tests on both host classes. The operating
guide owns the shared environment's change procedure: re-provision only when
no pb-cpu user is running. Applying this definition to live Sparks is a
separate operations step after review.
