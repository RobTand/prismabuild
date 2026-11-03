# Machine record consumer inventory

This inventory accompanies #2725. The external subprocess contract is
[machine_record_channel.md](machine_record_channel.md).

| Consumer | Source after migration |
| --- | --- |
| `tools/fleet/pool_reset.py::submission_outcome` | Private detached acknowledgement; `announced_submission` validates its existing schema and status vocabulary. Combined stdout logs remain diagnostic evidence. |
| `tools/verify.py::run_root_lane` | Private detached acknowledgement and private `prismabuild.endings.v1` document. Saved records have mode 0600. |
| `tools/fleet/pbcanary.py::submit_leg` | Private detached acknowledgement, including the authorized publisher child in `pbcanary_submission.py`. |
| `tools/fleet/pbcanary.py::wait_leg` | Private default wait rows array, selecting the one requested action. Existing stderr tails and return codes remain available. |
| `tools/fleet/pbgang.py::main` | Private detached acknowledgement for each gang member. |
| `tools/fleet/pbtest.py` | Existing typed `ClientReport` and private client events. Pool publication already runs in process. Its display callback now suppresses submission stdout without parsing schemas. |
| `tools/fleet/pbcampaign.py::submit_row` | Existing `reported_submission(ClientReport)` typed path, independent of its display callback. No CLI reader remains to migrate. |
| `src/prismabuild/submission_frontend.py::consumer_submission_display` | Writes the complete verified acknowledgement to the optional private sink before rendering the identical stdout event. |

Published CLI entrypoints for `verify` and `pbcanary` check both commands'
capabilities before relying on the new flag. In-process and same-runtime
consumers use their existing package boundary. Missing private records never
fall back to stdout.

Two external readers remain outside this repository's change:

- TrustLadder III's `workers/src/tl3_workers/pb_tests.py:326-344` and
  `pb_generators.py:170-176` parse detached acknowledgement schema and
  `action_key` from stdout. Their waits at `pb_tests.py:247` and
  `pb_generators.py:193` require `prismabuild.endings.v1` and read `endings`,
  `status`, `exit_code`, `succeeded`, and result paths. Its coordinator owns
  the private-channel migration and published-runtime capability checks.
- `src/prismabuild/installed_bootstrap.py::require_live_window` parses
  `pbstatus --json`. It reads `complete`, `timed_out_sections`,
  `unavailable_sections`, and `nodes` with `state`, `node`, and
  `runtime_commit`. It reads no endings fields. The endings-only #2718
  projection does not affect it, so this change leaves it in place.

Inventory commands:

```sh
rg -n 'pbrun|pbwait|pbstatus|PBRUN|PBWAIT|PBSTATUS' tools/ src/
rg -n 'stdout|json.loads|json.load|check_output|subprocess.run' tools/ src/
```

The remaining matches are human command suggestions, direct typed APIs,
queue or CAS file readers, launchers that do not parse command output, and
unrelated child payload JSON. This change leaves receipts, CAS, verdicts,
public projection, and the frozen `public_evidence` view unchanged.
