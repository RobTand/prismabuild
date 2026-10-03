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
triaged owned issues (2026-10-03): exactly one leading `[P0]`, `[P1]`, `[P2+]`, `[P2]` or
`[P3]` title prefix. Where the matching priority label exists, apply exactly that one and
remove all other priority labels; nonpriority labels such as `documentation` or
`in-flight` coexist independently. If the matching label is absent, the title prefix
remains the canonical priority until an authorized maintainer adds it: do not substitute
`P2` for `P2+` and do not create labels without authority.

Severity meanings: `[P0]` — a credible path can ship or serve wrong user-visible bytes,
corrupt an artifact, or execute or publish under the wrong runtime or identity; repair
takes precedence over routine work, and a demonstrated `P0` stays `P0`. `[P1]` — a gate
cannot catch its own relevant defect, or a wrong or underived number is consumed by a
decision; stop reliance on that gate or number and attack the causal repair. `[P2]` — a
provenance or observability defect, or a claim beyond available evidence, with no
established `P0`/`P1` consequence; repair the evidence or claim boundary without
inventing qualification. `[P3]` — cleanup, documentation currency or ergonomics with no
decision riding on it; fix bounded prose on sight.

`[P2+]` is an important `P2` with no `P0`/`P1` consequence and a documented objective
urgency trigger — name the milestone, deadline or measurement and its causal effect: an
already required, named near-term acceptance or handoff cannot obtain trustworthy
evidence, evidence is at a concrete retention or deletion deadline, or measured
recurring waste threatens an admitted resource window. It is prioritized ahead of
ordinary `P2`, never by downgrading a `P0` or `P1`; a generic blocker or assertion of
importance is not a trigger. Severity is separate from dependency and decision status;
`in-flight`, `blocked-external` and `needs-decision` are used only where their
repository label definitions match, and
otherwise stated as explicit body fields. A reprioritization leaves an audit comment
with the old → new priority, the changed evidence, why, and the responsible reviewer,
and updates the title and matching label together. A missing or conflicting prefix,
multiple priority labels, or a title/label mismatch blocks handoff and closure until one
evidence-backed classification is established; pending that resolution, route and
contain risk at the highest displayed tier. Do not retrospectively reprioritize
historical issues, including solely to adopt this taxonomy, and do not mass relabel
them.

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
