# Changes through issues and pull requests

Rob requires every change to `main` to go through a GitHub issue and pull request
(2026-09-06). Create or use an issue, work on a separate branch/worktree, link it
in the PR using `Refs #NUMBER` or `Fixes #NUMBER`, validate through PrismaBuild, review the diff and
merge the PR. Do not push commits directly to `main`, including fixes to policy
or automation. Existing dirty work must be retained until it follows this path.

The `Issue link / issue-link` workflow accepts a pull request that links an
issue in this repository either way: a `Refs #NUMBER` reference in the pull
request body (`Fixes`, `Closes` and `Resolves` also count, and close the issue
on merge), or the issue linked under **Development** in the pull request
sidebar. Use `Refs #NUMBER` when the issue must stay open after the merge. The
workflow verifies that the reference names an actual issue in this repository
rather than another pull request, and it does not execute PR code.

Rob also requires one visible canonical priority on newly filed, reopened or materially
triaged issues (2026-10-03): exactly one leading `[P0]`, `[P1]`, `[P2+]`, `[P2]` or `[P3]`
title prefix. Where the matching priority label exists, apply exactly that one and remove
all other priority labels; nonpriority labels such as `documentation` or `in-flight`
coexist independently. If the matching label is absent, the title prefix remains the
canonical priority until an authorized maintainer adds it: do not substitute `P2` for
`P2+` and do not create labels without authority. `P2+` is reserved for an important `P2`
with a documented objective urgency trigger — a named near-term acceptance or handoff
cannot obtain trustworthy evidence, evidence is at a concrete retention or deletion
deadline, or measured recurring waste threatens an admitted resource window; a generic
blocker or assertion of importance is not a trigger. Severity is separate from dependency
or decision status: `in-flight`, `blocked-external` and `needs-decision` describe state,
not severity. A reprioritization leaves an audit comment with the old → new priority, the
changed evidence, why, and the responsible reviewer, and updates the title and matching
label together. A missing or conflicting prefix, multiple priority labels, or a
title/label mismatch blocks handoff and closure until one evidence-backed classification
is established; pending that resolution, route and contain risk at the highest displayed
tier. Do not mass relabel historical issues.

Install the local direct-push guard with:

```bash
git config core.hooksPath .githooks
```

Do not replace an existing hooks configuration without preserving its hooks.
Local hooks can be bypassed and do not govern API writes or other clones.

Server-side protection should require a PR, the `issue-link` status check,
resolved review conversations, and current branch status; enforce it for admins
and disallow force pushes and branch deletion. GitHub currently returns HTTP 403
for branch protection and rulesets: private repository protection requires an
eligible paid plan. Keep this repository private. Until that account limitation
is resolved, neither the workflow nor the local hook is server-enforced branch
protection, and direct API writes remain technically possible.
