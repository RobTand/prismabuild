# Create a child ticket

Use `intake.py` to create new GitHub work in `RobTand/prismaquant`,
`RobTand/prismabuild`, or `RobTand/tessera`. Authenticate `gh` before running it.
Select an open issue as the native parent. Cross-repository parents within these
three repos are supported.

1. Save the ticket body in your task worktree or evidence directory. For P0/P1,
   include a nonempty `## Reproduction` or `## Measurement` section. Inline
   `Reproduction: ...` and `Measurement: ...` fields also work. Empty sections,
   HTML template comments, and placeholder values do not count. The gate checks
   supplied content; the author and reviewer establish whether it is true.
   A new heading or unrelated `Name: value` field ends the evidence, including
   fields inside Markdown lists and task lists. List prefixes also work on
   evidence labels. Inside a
   backtick or tilde fence, headings and field labels are literal code. A fence
   outside an evidence section cannot introduce a reproduction field.
2. Run the read-only dry run:

   ```bash
   python3 tools/backlog_census/intake.py \
     --repo RobTand/prismabuild --parent 2313 --priority P2 \
     --title 'Build the intake gate for new tickets' --body-file body.md
   ```

3. If it reports `likely_duplicate`, open the listed issues and reuse the
   existing work or revise the scope. There is no duplicate override.
4. After a successful dry run, rerun the command with `--apply`. It reads live
   issues again and creates the child with its native parent in one GraphQL
   `createIssue` mutation using `parentIssueId`. Retain the returned child URL.

The command lists every page of open issues across all three repos. It excludes
pull requests and compares all priorities. A failed read stops publication.
The shared census predicate removes the leading priority, case-folds the title,
and replaces punctuation with spaces. It refuses equal nonempty titles, or
titles of at least 30 characters whose SequenceMatcher ratio is at least 0.92.
This is a likely-duplicate check, not semantic proof of equivalence.

`--priority` accepts P0, P1, P2+, P2, or P3. The command adds exactly one leading
title prefix and applies that exact priority label if it exists. It refuses a
conflicting title prefix or priority label. Pass `--label NAME` for each existing
nonpriority label you need. It never creates labels. `--parent` accepts a number,
`owner/repo#number`, or a GitHub issue URL.

Exit 0 means a successful dry run or confirmed creation. Exit 2 means a policy
refusal. Exit 1 means invalid input or a GitHub error. Argument usage errors also
exit 2. After an uncertain write result, inspect GitHub before retrying. No
mutation is automatically retried. Concurrent callers can race between the
duplicate read and creation; this command is a cooperative producer gate.

In prismaquant and tessera, use the reviewed command at
`/home/rob/paperclip-repos/prismabuild/tools/backlog_census/intake.py`. Until that
change lands, retain ticket drafts. Do not copy the implementation into consumers.

## Keep producer instructions current

The canonical Matt Pocock `to-tickets` adaptation lives in `to_tickets/SKILL.md`.
It retains plan approval and dependency-order publication. Its GitHub publishing
step requires this command and a native parent. `LICENSE` retains the upstream
license from the installed Matt Pocock skill. Repo-local copies override the
global skill in agents that discover `.agents/skills`; the `.claude/skills` link
provides the Claude Code entry point. Agents that do not discover these paths
receive the same command through `AGENTS.md`.

Regenerate these files only in task worktrees:

```bash
python3 tools/backlog_census/wire_agents.py TASK_WORKTREE --repo RobTand/prismaquant
python3 tools/backlog_census/wire_agents.py TASK_WORKTREE --repo RobTand/prismaquant --check
```

Use the matching `--repo` for each checkout. `--check` reports drift without
changing files. The generator preserves the surrounding AGENTS.md and refuses
to replace an existing unrelated Claude skill.

## Hook issuegraph when OMP restarts

The OMP fleet is stopped. This change does not restart it or claim its hook is
installed. At issuegraph's issue-creation boundary, replace direct ticket
creation with this command. Supply its planned repository, native parent,
priority, title, body file, and existing labels. Use the returned URL for its
graph node, then write native blocking relationships separately. A refusal must
leave the planned ticket unpublished and identify the duplicate or missing
evidence. An uncertain write requires reconciliation before retry. Do not treat
a body-only `Part of` link as a native parent.

## Verify the gate

Submit `python -m pytest -q -s tools/backlog_census` through the published
PrismaBuild client. Reserve one CPU, two GiB RAM, zero GPUs, and one native
thread per library. The suite uses a deterministic fake `gh`; it makes no network
requests or real GitHub writes. `test_requested_dry_run_examples` runs the actual
command and prints both required refusals. Parent creation, all-repo pagination,
read failures, priority labels, and evidence placeholders have command tests.


`test_unrelated_fields_and_fenced_labels_refuse_before_github` and
`test_fenced_shell_comments_preserve_evidence_through_apply` are the field and
fence CLI regressions. Their 12 cases fail on the pre-fix parser. To check the
original intake assertions against named mutants, submit
`python tools/backlog_census/verify_intake_mutations.py` through PrismaBuild with
the same resource limits. The script mutates private temporary source copies,
runs the corresponding test functions, and fails unless every mutant causes an
assertion failure. In each private test copy, it collects failures from every
original assertion before failing the test. An unmutated control must pass, and
every assertion site must fail under a named production mutant. Output retains
the assertion text, source line, mutant names, and full pytest report. Collection
errors and other exceptions do not count. It never changes the submitted checkout.
