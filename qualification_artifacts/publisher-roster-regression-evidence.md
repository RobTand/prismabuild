# Publisher roster regression fix

Only application edit: tools/fleet/publish_runtime.py. No tests edited.
Selected-policy validation now calls the existing `_candidate_roster` with the manifest roster digest before iterating declarations. This preserves authoritative parsing and original `SystemExit` roster diagnostics, while retaining the reduced-publication early return and generation isolation.

Baseline: focused roster suites reproduced the two reported failures (2 failed, 43 passed).
Post-fix: roster aliases, publication boundary, publisher, and dropped-option suites: 115 passed on each of default x86_64 and arm aarch64. Class-image fleet-generation and config-failure suites: 8 passed on each arm. `git diff --check` passed. An attempted arm name `x86` was refused without execution; the x86 tests were subsequently run on the default arm.

## Retained post-fix evidence

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"8dfd558b17354043a81b0897bd5b7725875338f8cba25f9cadfe9176c47a2346","size":1862}},"operation":"af4960bd16040e4beb55897960ddda5e20e5d19512e73c1779aeb2bc49dce6f9","receipt":"69f0093def691931206481c2c7d852c449bb5d13a10bd9bb3d92c61795385558"}]

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"576d1524a3e4714047cd2dcd9ac5971e1ec89b62c33ea65bd79c1d3e0fe9d113","size":1874}},"operation":"9a097c9a2fa96776fc7ac2cef174e41776ced05c889cb7ab6b026c1176891854","receipt":"9d7b9dc89df4b471d9de4ac098041247f739c4542232befe1bc8d1b7a1567fea"}]

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"57a2c20f45cf77327b667b06dd9ee4a1c04b6872fcda23b2f3182a979e88c119","size":1355}},"operation":"5e5aa04c42c2d47ee1c229a14d413cd9c7fdfeb05d66fdf29fdb3c6babc58622","receipt":"3ad5e185db078144adad881ec0e52205363560b20ac3fb6f40ae540fd34c3b21"}]

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"b049479c19cae171cf3bb5d8dcdb2849fc3bf7410f83cfdc29a3ddeed3f2cfca","size":1365}},"operation":"ea81c4665f90197cf4cbbb7ab7fe265dbe4a79eb3b654dbbf2378c2607de931a","receipt":"6717ca7989a93f089ec636a3539ebd0ab1a6ca6ef972de020b420382bcfa3b06"}]
