# Changes through issues and pull requests

Rob requires every change to `main` to go through a GitHub issue and pull request
(2026-09-06). Create or use an issue, work on a separate branch/worktree, link it
in the PR using `Refs #NUMBER` or `Fixes #NUMBER`, validate through PrismaBuild, review the diff and
merge the PR. Do not push commits directly to `main`, including fixes to policy
or automation. Existing dirty work must be retained until it follows this path.

The `Issue link / issue-link` workflow verifies that a reference names an
actual issue in this repository, rather than another pull request. It does not
execute PR code. Install the local direct-push guard with:

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
