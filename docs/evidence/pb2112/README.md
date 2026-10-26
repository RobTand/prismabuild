# PB2112 frozen SDK5 CPU supplier inventory

**Terminal disposition: unresolved inventory, not a qualified CPU tuple.**

The handoff is [`../pb2112-sdk5-cpu.json`](../pb2112-sdk5-cpu.json).
Frozen source `027103d9a8417e06c7f13356e58779a313cd7088`, accepted child
`855032b1f3f2edcef0b6cc49c16078942ea1644e`, packet identity, and prior
qualification references are retained without replay. No SDK, packaging,
consumer, pin, filesystem infrastructure or runtime bytes were changed.

## What was actually read

* `pq-current-source.txt`: line-numbered excerpts of served PQ
  `dfc220bda04b8b355f45540f7f626852ec20653a`, including full-file raw hashes.
  These establish current source/configuration requirements, **not executed
  tests or deployment**: SDK5/027 pins, selected 20-file manifest, one
  `src/prismabuild` package, Core/tool origin and restoration controls,
  and selected native producer-context signature/capture assertions.
* `pb-current-controls.txt`: current PB guard and filesystem documentation
  excerpts, not a copy of frozen027 and not live safe-route qualification.
* `publication-receipt.json`, `guard-receipt.json`: original raw CAS receipt
  bytes copied through authorized direct file access.
* `publication-result.txt`, `guard-result.txt`: original result blob bytes.
  Their raw SHA256 and lengths matched the receipts: respectively
  `0182c4c4638bb2a701b38ecd6dbf38e2a42bbf1356882cdaebb35b3ddc0a6c3c`
  (427 bytes) and
  `d14677b077361eb8579773894bccd8bf5e7efa99f6a17414560d76814907fd11`
  (18600 bytes).
* `retained-linkage.json`: explicitly labelled projections from the original
  sealed requests and queue done records, with original raw digests, sizes
  and readable references. The large embedded guard helper is not copied.
  Both requests, receipts and done records have the expected action keys;
  request input descriptors equal receipt producer inputs. Each done stdout
  begins with its result blob and ends with a publication JSON record whose
  embedded receipt equals the original receipt. Both done records say
  `executed`, return code 0. These checks establish observed record linkage,
  **not independent proof of the contents reported by the commands**.

`copies.json` and the handoff list raw SHA256/size for the retained copies.
Receipt identity fields (`3a1d…`, `0ed9…`) are deliberately separate from raw
receipt-file digests. No receipt was rewritten under its original identity.
Original request/done objects remain at their supported retained references;
projections are not represented as original bytes.

## Remaining boundaries

| Constituent | Disposition and next prerequisite |
| --- | --- |
| Accepted compatibility packet | Exact `local://…#3441` URI and SHA256 retained. No supplied interface resolves its original bytes; need an authorized original readable reference. |
| Ordinary setuptools wheel | Expected `prismabuild-0.1.0-py3-none-any.whl` is an identity prediction from frozen packaging research, not a verified artifact. Need original wheel plus producing linkage and metadata/member authentication. |
| Genuine noneditable Git installation | Retained guard log reports Git027, 48 verified files and x86 Python 3.14; original `direct_url.json`, RECORD and installed owner bytes remain unauthenticated. Wheel installation is not Git provenance. |
| Source/helper archive | Publication log reports archive `94727274…`, 1195 files and 20 pinned files. Archive/tree bytes remain unauthenticated. Prior qualification-directory denial is retained, not retried. |
| Layout/native behavior | Current exact027 consumer requirements inspected; original exact-source origins, Core/package coherence, shape and selected native behavioral observations remain unresolved. No causal layout work was executed. |
| Current safe route | Current support is documented, with limitations. Need independently owned route/profile/action/request/receipt qualification from #2045–#2050 before any new missing-artifact operation. This is not the historical October 4 diagnosis. |

Each unresolved JSON constituent supplies reference, operation, reader context,
result and prerequisite. The inherited denied directory listing and
`PrismaBuildCAS.lookup` were **not repeated**. Direct file access to the
platform-readable CAS is a distinct supplied interface; successful record
reads do not remove the archive/tree boundary. No artifact loss, necessary
human privilege, permission change, alternate-account access, replacement
build or installation is inferred. There was no supplier production operation.

No full-resolution claim follows from the offline checks below. #2113 owns the
Python 3.12/aarch64/Tessera b40 pack, #2114 the guarded consumer evidence,
#2115 the campaign dossier, and #1485 the compatibility preparation.

## Offline verification

Command on each admitted arm (no build, install, historical Git dependency,
submission or prior-qualification replay):

```sh
python -m pytest tests/test_sdk5_cpu_supplier_evidence_2112.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q --basetemp="$TMPDIR/pytest"
```

* x86, Python 3.14.4: **114 passed**, exit 0 (89.68s).
* ARM, Python 3.12.3: **114 passed**, exit 0 (33.25s).
* Retained stdout was read back on both arms. Five inventory cases plus
  109 repository-wide guards pass; these do not authenticate the unavailable
  historical artifacts, qualify a route, or execute the frozen SDK tuple.
* Genuine Git-install, original wheel/archive/packet authentication, current
  route and exact027 behavioral qualification were **not completed** for the
  boundaries above. Passing inventory checks is not passing O4 qualification.

x86:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"d3d3e800985a804859e4b1eb667d71092eb57b637bd54b20d2e838efa1415e52","size":2002}},"operation":"b78b99ebfd2de03619d72d897bbb6b816408d3b103bde64b4a9a26cf3e64974d","receipt":"dad121fd93717e5fbcf0569c310a7f569e028feb4847229f919ce60fa65645a6"}]

ARM:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"af88279455509851fde66193eec1efc7b74e7dd8b3ea49e294fe6e81ab6778fb","size":2005}},"operation":"0cda1c0f73b27f314d5c47fc04dc05d57ec555425962f55dc70ffb89588cc53a","receipt":"9776032f1fc62a02690e230ec7c3afe9b46cae938773d7f63f35059a93bc3e68"}]
