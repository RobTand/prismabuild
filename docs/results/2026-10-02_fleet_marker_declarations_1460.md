# PB #1460: distinguish marker declarations from marker text

This bounded additional defect was found while validating #1458's generated
command neighbors. The whole-source regex classified fixture strings/comments
as real fleet-data declarations, blocking this repository's own marker tests.
The published client refused before submitting any action. The original five
fixture tests passed through admitted action
`2660e922872d9681d16b9e81e93d62fdb5da1c71ff1e308ec0f8cba6947c0265`.

The explicit RED regression ran in CPU-only admitted action
`197d3e3d7b9ddfea289c75c2429398d69e732b57f8ddfa00811e364fe50de700`
on sparklina: 2 failures / 9 passes. Only the string/comment negative cases
failed; real markers and conservative syntax-error fallback controls passed.

After static AST recognition, action
`57495704281e3b91dbba1fe8dfb47508306cd0d07271c6aaaa6b158b530d049f`
on sparky passed 12 tests / 0 skips, including the actual test file's marker
scan. The real manifest gate, forwarding, absent flags and stage-without-
manifest refusal still pass. Parsing imports no target modules; unparseable
target syntax retains the previous conservative textual fallback.

Both runs used the published `pbrun.py`, one CPU core, 1 GiB, one native thread,
priority -10 and a 120 s deadline. This is source qualification under deployed
generation `d028dfee920b-1790960385-1d815cff72d1`. The published old pbtest
client still refuses the fixture until the fix is deployed, so qualification
uses admitted pbrun rather than changing deployed state. No GPU work or
application performance claim is involved. Terminal/log/hash evidence lives
under `/home/rob/tmp/astra-resume-20261002/pb_collection_1458/` in
`fleet-marker-red-submit.log`, `fleet-marker-final-green-submit.log`,
`receipt-metadata.json`, `claim-verification.json` and
`source-and-log-audit.json`.
