# Historical test inputs

`manifest.json` maps each test's revision and source path to its full commit,
byte count and SHA-256. `blobs.tar.gz` stores each distinct source blob once,
under its SHA-256, in sorted member order. Tar metadata is fixed; gzip uses an
empty filename and mtime zero. Stored inputs have no `.py` suffix.
`tests/history_inputs.py` checks the byte count and digest before returning an
input or writing it into a census subprocess's temporary tree.

From a full clone, regenerate with
`python tools/maintenance/generate_history_inputs.py --write`.
Verify every historical byte and the deterministic archive with `--check`
inside an admitted PrismaBuild action.

The census closures contain 67 files from
`8e8572ee324b8508b9a7dd6a6dcb7d3548b663a7` and 68 from
`082f75ea50b2601c8681fe070a5a342e1b042702`. They were captured by an `open`
audit installed before imports in the test's unchanged `_OBSERVE` program.
The audit records reads from forked children as well as the parent. It runs
all eight original observations against the real `build_starved` fixture.
The successful capture action was
`bab635bf992b0051871fac8b3871f967e0ed8be0ab4a9f6ba7596970dcce00cf`.
Run `--capture-census` through PrismaBuild to repeat the capture and check
the complete path lists in the generator. The converted census test exercises
both materialized closures, including from depth-1 checkouts.

`tests/test_history_inputs.py` statically scans test modules for retained Git
reads. Its only file exemptions are the five standalone maintenance tools
listed there, each with its reason. Guard examples cover all seven Git verbs,
both quote styles, `-C`, revision defaults, f-strings and concatenation.
