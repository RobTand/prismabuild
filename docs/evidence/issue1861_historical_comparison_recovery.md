# PB #1861 — bounded historical comparison recovery

**Disposition: stopped before workload execution.** The historical comparison
specification and the W1 readability prerequisite remain unestablished. This is
a retained negative recovery report, not comparison completion, W1 diagnosis,
deployment evidence, or proof of permanent artifact loss.

The original return left the worktree unchanged and kept the inventory only in
the work result. The review reported an empty change. This document retains that
inventory in the existing `docs/evidence/` collection; it introduces no runtime,
application, test-discovery, acceptance-criteria, or deployment changes.
Disposition and publication remain with TrustLadder.

## Evidence basis and bounds

Repository inspected: `38eed1488e6e1a622da295dd69ae041f63da6c08`.
This identifies the inspected checkout, **not** either historical comparison arm.

The original task brief, “Execute and verify the documented admitted concurrent
historical comparison,” supplies the prior recovery findings below. They are
attributed to that handover rather than represented as fresh retrievals or
independent verification in this repair. No raw API response, retained run
bundle, or primary search transcript is newly authenticated by this report.
The earlier implementer pass also observed an empty `/conversation_history/`
listing and a clean Git worktree. This repair rechecked the clean starting
worktree and read the policy and report conventions; it did not repeat generic
experiment searches.

Status terms apply only to the stated source and inspected scope:

- **Inspected without match:** the bounded inspection did not recover the
  specification, artifact locator, or prerequisite evidence sought.
- **Absent:** a specific listing was empty or a supplied checkout unavailable;
  this does not establish absence of historical artifacts elsewhere.
- **Inaccessible:** a concrete access attempt failed; its context matters.
- **Incomplete inspection:** coverage was bounded, interrupted, or no concrete
  artifact locator was available. This is not an exhaustive negative finding.

## Cited recovery inventory (O1)

| Source | Inspection status and result | Limit or unresolved dependency |
| --- | --- | --- |
| [Parent #1859](https://github.com/RobTand/prismabuild/issues/1859) | Handover: successful read-only GitHub API retrieval; inspected without a comparison specification or artifact locator. Names runs `ce693af19cb747d2b3b65e9dbced3b7e` and `766ae22dff0c431e9ce4476199019430`. | Runtime-readability context is not an experiment specification. No retained work-package artifact location was recovered. |
| [#1859 comments](https://api.github.com/repos/RobTand/prismabuild/issues/1859/comments) | Handover: inspected; empty list (absent comments at that endpoint). | Does not prove absence of artifacts elsewhere. |
| [#1859 sub-issues](https://api.github.com/repos/RobTand/prismabuild/issues/1859/sub_issues) and [timeline](https://api.github.com/repos/RobTand/prismabuild/issues/1859/timeline) | Handover: inspected without an artifact locator; establish #1860 as readability sibling and #1861 as execution sibling. | Issue ordering and sibling relationships do not certify W1 or identify historical artifacts. |
| [W1 / #1860](https://github.com/RobTand/prismabuild/issues/1860) and its comments | Handover: inspected without retained successful affected-context open/read evidence. [Comment 6304457079](https://github.com/RobTand/prismabuild/issues/1860#issuecomment-6304457079) repeats the operator request; [comment 6313130903](https://github.com/RobTand/prismabuild/issues/1860#issuecomment-6313130903) requires bounded diagnosis and a successful read in the affected identity/context and explicitly does not certify the prerequisite. | W1 remains unestablished. Neither permissive ACLs nor a read from another context substitutes for this evidence. W1 diagnosis/repair is outside #1861. |
| [#1861 comment 6304460867](https://github.com/RobTand/prismabuild/issues/1861#issuecomment-6304460867) and [comment 6313102802](https://github.com/RobTand/prismabuild/issues/1861#issuecomment-6313102802) | Handover: successfully retrieved; confirm missing specification and bounded-recovery/negative-result branch. Reference splitting run `888b759942c84584af3d8e069d4a9444`. | No retained splitting-run artifact location supplied. |
| Run `ce693af19cb747d2b3b65e9dbced3b7e` | Reference recovered from #1859; artifact inspection incomplete because location is unresolved. | No run-specific CAS digest, action ID, or artifact locator recovered. A run ID is not a verified retrievable artifact. Artifact absence or permission failure is not established. |
| Run `766ae22dff0c431e9ce4476199019430` | Reference recovered from #1859; artifact inspection incomplete because location is unresolved. | No run-specific CAS digest, action ID, or artifact locator recovered. Artifact absence or permission failure is not established. |
| Splitting run `888b759942c84584af3d8e069d4a9444` | Reference recovered from #1861 comments; artifact inspection incomplete because location is unresolved. | No retained work-package/run locator recovered. Not a verified retrievable artifact. |
| `/mnt/shared/prismabuild-fleet/evidence`, `incident-evidence`, `investigations`, `measurements`, `results`, and `rollout` | Handover: all six directories accessible; bounded read-only listings and targeted text/filename searches inspected without relevant run/specification/W1 matches. | Incomplete inspection: selected text files at most 2 MiB and filenames through depth six. Expanded hidden/ignored content search timed out after 30 seconds, exit 124. Not exhaustive absence evidence. |
| `/mnt/shared/prismabuild-fleet/cas` | Handover: no full CAS crawl performed; no run-specific digest/action locator recovered. | Incomplete/unperformed artifact inspection, not a claim that CAS is empty, inaccessible, or has lost the artifacts. No generic crawl repeated. |
| `/mnt/shared/prismabuild-fleet/pb-queue` and `runtime-generations` | Supplied read-only roots, not concrete retained run references; not searched in this repair. | Availability as a root is not an experiment locator. No claim about contents or accessibility from a new attempt. |
| Repository procedure search | Handover: inspected without a relevant procedure. | Search scope does not prove historical artifacts absent. No reconstruction from current source and no generic documentation search repeated. |
| `/conversation_history/` | Handover and earlier implementer pass: listing returned no files (absent entries). | No retained conversation artifact exposed there; not proof of permanent loss elsewhere. |
| Additional served-repository checkouts | Served-repository descriptions say “no checkout, since artifact retained by a continuation.” | No available additional checkout or retrievable continuation artifact locator supplied. The displayed paths are not proof that those checkouts exist. |
| `gh` executable | Handover: inaccessible, permission denied, exit 126. Direct read-only GitHub API retrieval succeeded instead. | This executable failure is not a GitHub-source absence finding and says nothing about W1's affected runtime context. |

The GitHub issue/comment URLs are concrete retrievable documentary references
according to the handover. They are **not** confirmed retrievable experiment
artifacts. No remaining run-specific work-package, CAS, or action reference is
exposed by the supplied inventory. Accordingly this repair does not repeat a
broad filesystem/documentation search or infer a CAS layout from run IDs.

## Missing execution gates (O2, O5)

Both gates must be established from retained evidence before any launch:

| Gate | Exact missing fields or evidence | Current conclusion |
| --- | --- | --- |
| Historical specification | Exact baseline revision; exact candidate revision; immutable input identities; original concurrent invocation/procedure, including its concurrency arrangement; original acceptance criteria. | Not recovered. Neither the inspected checkout nor generic current policy identifies these fields. No thresholds, replacement inputs, or commands inferred. |
| W1 readability | Retained evidence of successful opening/reading of the published entrypoint in the affected execution identity/context. | Not recovered. The #1860 requirement is not a success record. Issue ordering, ACLs, another-context read, or the `gh` error cannot satisfy it. |

**Stop before workload execution.** No W1 repair, runtime implementation change,
sequential substitute, current-source-only experiment, synthetic comparison,
or reduced workload is authorized by this negative recovery result.

Concrete dependencies for further targeted recovery would be a retained
run/work-package artifact locator (or run-specific CAS digest/action ID) carrying
the original specification, and the retained affected-context W1 success record.
None is currently confirmed retrievable. This identifies missing evidence, not
a generic request for a person. There is **no actual evidence of permanent loss
or irrecoverability**: unresolved location/accessibility is not irrecoverability.
Oracle disposition remains open; the task is not declared obsolete.

## Conditional execution and retention requirements (O3, O4)

No comparison was submitted or executed, so there are no comparison admission
settings, action/attempt IDs, workload exit statuses, stdout/stderr artifacts,
integrity records, CAS receipts, or criterion results to certify. This report
must not be counted as completion or a successful comparison.

If both gates are later established, preserve the original concurrent procedure
rather than substituting sequential arms. The relevant policy sources are
[AGENTS.md](../../AGENTS.md), [execution policy](../agent_execution_policy.md),
[operating guide](../operating_prismabuild.md),
[skill](../../skills/prismabuild/SKILL.md), and the opening architecture section
of [design.md](../design.md). Policy governs execution; it does not supply the
missing historical experiment.

- Use authorized admission for the recovered workload; the documented universal
  vLLM exemption applies if the recovered work runs vLLM, but its applicability
  to this unidentified experiment is unknown.
- Declare honest aggregate concurrent CPU/memory/GPU demand; bound native
  threads, preserve assigned affinity, and leave placement to PrismaBuild.
  Keep deployment-specific settings in existing configuration.
- The supplied check arm is already admitted, x86 CPU-only, 2 CPUs/4 GiB.
  Checks use plain commands with no nested `pbrun`, `pbtest`, or `pbcampaign`.
  Compatibility of the unknown historical demand is unestablished; if the
  recovered procedure needs more resources or GPUs, report that limitation
  rather than resize the experiment to fit this arm.
- Retain exact source/input identities, exact commands, admission/resource
  settings, action and attempt identifiers, terminal statuses, verified
  stdout/stderr references and integrity information, verified CAS receipts,
  and comparison results against **each recovered original criterion**.
- Distinguish refusal, failure, incomplete attempt, and inconclusive result from
  success. Submission acknowledgement and readability alone are not completion.

## Repair verification

The repair started at `38eed1488e6e1a622da295dd69ae041f63da6c08`:
`git status --short` was empty and `git diff --exit-code` returned 0.
The only intended delta is this retained report. No acceptance tests are added
or modified, and no test discovery or existing coverage is changed.

The brief specifies null acceptance checks: a legitimate historical comparison
check cannot be defined until its original specification and criteria are
recovered. A missing-report-file test or a synthetic experiment would manufacture
requirements. Documentation whitespace verification and final diff inspection
are reported in the work result; neither verifies the historical comparison.
