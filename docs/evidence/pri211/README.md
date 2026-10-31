# PR #2053 integration preservation

PRI-211 resolves one merge of main `a8e0f76abc6757e1193eec7cfabea06b70a89fbf`
into approved head `c9e297a786ecab4cdea942a16d11963ed3b7e7ef`.
The approved source remains the merge's first parent. No production logic or
compatibility policy is added by the resolution.

The archive-layout answer to interaction
`7af6cfab-8c96-469e-9ad0-0ffead9cb142` authorizes raw preservation of the two
adoption events for identical archived bytes. Main's canonical `provenance.json`
is unchanged. The approved producer's complete record is retained beside it as
`provenance-9284c8e1585709efaf4fcce8f2813464e68bf8fd.json`. Neither JSON document
was merged or rewritten. Complete current capture/golden pairs from both Git
parents are stored under `tests/compat/history-controls/merge-2053-<parent>`.
Each pair has a source and byte manifest. These files grant no new historical
or supported-generation compatibility credit.

Existing Python consumers do not read superseded-current provenance as a
capture manifest. `record_compat.retained` reads an explicit `prepared.json`
or immediate JSON descriptors, without descending into archive directories.
`refresh_compat_runtime` writes the exact canonical `provenance.json` filename
in a new archive. Its lookup and refusal behavior are unchanged. The strict
adoption wrapper also compares every pre-existing history file before and
after adoption, including the raw sidecar.

`verify_preservation.py` checks raw bytes against both original Git parents,
checks each complete pair's manifest, and exercises actual retained-generation
discovery. It runs inside an admitted PB action. The existing compatibility
replay and wire inventory guards provide the source-bound regression checks;
this integration repair adds no runtime behavior requiring a new unit test.

Evidence, source pins, captures and command records are retained in
`/mnt/shared/paperclip-runs/PRI-211/`. Fresh merged-source capture and strict adoption passed on the frozen merge
producer `79f90af7e6fd4d0aa72e8c9bfa1f8f98b91887a6`. The primary audits
bind original terminal records, full immutable logs, clean source Git objects,
CAS receipts and results, with complete cleanup. The adoption exports were
authenticated against that result before integration. Only the current
`upgrade-intent/armed` row changes, to the actual merged publisher hash
`58a160aa73215ab46226f956532cbba54bd3a22c50b384ee878bef781af72bc6`.
Its complete prior pair is archived by the strict adopter. All 63 historical
corpus files, all 20 rollout historical rows and nine unselected current rows
remain byte-exact.

The independent preservation/inventory action also passed. It exercised actual
retained discovery, verified both raw parent pairs and provenance events, and
regenerated the exact inventory from merged source, 584 to 585 rows. The
result-bound report hash is in `inventory-authentication.json`.

Final x86/ARM qualification and independent review remain acceptance gates.
Qualification uses the 45 affected portable files, including all four mandatory
guards, on both architectures, and the nine archival files on the existing
x86 lane. The source-qualification prelaunch record preserves all 69 requirement
targets and status axes by their unchanged ledger identity. It claims no
runtime publication, deployment or application acceptance.
Original PRI-67 approval and release gates remain with their source owners.
