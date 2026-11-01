# Core golden refresh for #2023

Merging main `a85f3886c3c12ce55f05ae3be66ba2c3fef3fe36` into #2023
combines main's stable-file verification with #2023's guarded log unlink.
Core's embedded immutable-file source pin must bind the merged helper.
The helper is 39,054 bytes with SHA-256
`3cf53faca654f450c4633d9664946331a5e987e46d96909d34f32ce9305a1630`.
Core remains 305,687 bytes and its new SHA-256 is
`15bbd67c1ff4f3aa54ea7f2cd6a2e7176b2ec26beb24d99df2c61b82b6dbd0e7`.
The digest owner retains main's exact bytes.

The six current Core records, receipt, attestation and runtime in both
in-process and script variants, were captured from clean merged producer
`525d7eecd6532e4317bfc2bb3e6e0c2457cdcb5a`. Capture action
`df91602a6d8876df2bd70de51b17b9b33469b08a660df5298a2a5d14b821955f`
ran `tests/compat/support/record_compat.py --generate` inside PB.
It independently reproduced six current and twelve historical rows.
Both consumer directions passed with no failures or absent participants.

Strict adoption action
`d96712d38d891f3b087e33a5fed40c712ab3dba064ce7b29ee6ad84392e2da3a`
ran `tools/maintenance/refresh_compat_runtime.py --profile runtime` inside PB.
It required the verified capture primary audit, confirmed the helper pin
and rejected changes outside the allowed runtime identity fields.
The additional adoption wrapper narrowed those changes to Core identity
fields and their dependent runtime, attestation and receipt digests.
It verified all 132 existing historical/control files against main byte
for byte. The six selected historical rows per retained generation remain
unchanged. Main's complete superseded current files are archived by the
maintenance tool, and unselected current rows remain byte exact.

Evidence is under `/mnt/shared/pbtl-probe-tmp/pb-lead-dl380/2023/`.
`repair4-runtime-primary-audit.json` verifies the capture request, Git
objects, source inputs, terminal logs, native cleanup and CAS result.
`repair4-adoption-primary-audit.json` authenticates adoption and its exported
manifest before integration. The export contains only tool-produced files.
No golden was hand-edited or textually merged.

The field-set metadata starts with main's complete version. The canonical
`refresh_compat_field_sets.py --before-source origin/main --review ... --apply`
runs inside PB after binding that worker-local ref to the supplied main SHA.
The review covers exactly #2023's eighteen additions, with no removals,
from 572 to 590 policies. It adds no policy belonging to another change.
No source policy or field-set metadata is hand-edited.

`REPAIR4_VALIDATION.md` records exact commands, action keys and native x86
and ARM results, plus compatibility replay in the qualified x86 Python 3.14
namespace lane with a 2,400-second per-action budget. The archival runtime
pins an x86 executable; this refresh claims no ARM archival replay credit.
The capture qualifies record bytes and grants no rollout or deployment credit.

## Paperclip rebase on 2026-10-25

The task-owned branch rebases the reviewed `10aa752a54` tree onto main
`d0b92d267d7beac0884e143f42ed6a8b3d469020`. Core, the immutable-file helper,
the archive and retention owners, and the action-result resolver keep their
previous bytes. The existing Core capture and adoption above remain historical
evidence. New fleet validation against the rebased head is required.

The canonical source-only field-set tool regenerated the inventory from main's
573 entries to 593. The reviewed delta contains the original eighteen #2023
entries and two already-merged #2279 local variables in
`tools/maintenance/test_runtime_inventory.py`. Those two variables parse proc
state and collect action keys. Neither constrains durable record members.
The exact review is
`docs/evidence/issue2023_rebase_field_review_2026-10-25.json`. Reproduce it with:

```bash
python3 tools/maintenance/refresh_compat_field_sets.py \
  --before-source d0b92d267d7beac0884e143f42ed6a8b3d469020 \
  --review docs/evidence/issue2023_rebase_field_review_2026-10-25.json \
  --report /tmp/issue2023-field-sets.json
```

The tool imports no product module and executes no tests. Its source-only
contract permits coordinator execution. No field-set metadata was hand-edited.
This PR still retires logs only. It does not satisfy metadata retention for
#2023 and does not authorize runtime publication or live retirement.

## Finish merge on 2026-10-31

The finish pass merges main `90332a0eb844e46920593eb61efeee0a2c63905b`.
The source-only field-set generator starts from main's 592 entries and emits
612. The exact review in
`docs/evidence/issue2023_finish_field_review_2026-10-31.json` covers the
eighteen archive and retention entries, plus two existing main expressions
in the issue-link parser and GPU placement policy. Neither of those two
expressions constrains durable record fields. The inventory is tool output.

The retained current golden already binds the merged Core exactly: 306,962
bytes and SHA-256
`96aeff415bf2a2cc4f93a2c89a20f4d6382df4d5ad00bf3ace06e993e54a95c5`.
The merged immutable helper also retains the exact pin recorded above.
No capture bytes need regeneration or adoption. Fresh reproduction action
`e6ddb020e6f5bfdbfd430b844c8b1e32a1e24e47286c611f4512e8dd8c89d925`
ran against source head `e789ae5c33fe72e3191b22f8df1303a717848427` and
exited 2. All six writer cases stopped at `bwrap: Can't mount proc on
/newroot/proc: Operation not permitted`, before reaching their production
entry points. The failed terminal and bound logs verify; no CAS receipt was
published. Fresh reproduction remains blocked on that namespace prerequisite.
Source review and tests grant no live retention, unlink, runtime publication,
rollout, or service-install authority. Issues #2023 and #2025 remain open.
