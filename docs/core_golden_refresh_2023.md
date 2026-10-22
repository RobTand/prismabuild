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
