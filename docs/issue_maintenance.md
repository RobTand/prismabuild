# Recurring issue maintenance

Rob authorized periodic issue triage and repair on 2026-09-06. Sparky's lingering
`rob` user manager runs `prismabuild-issues.timer` every ten minutes with up to
30 seconds of jitter. A read-only GitHub API poll starts Codex only when an open
issue is new, changed, or due for another attempt. All authors are included because
other agents also file using Rob's account. Pull requests are excluded from the
poll, but the maintenance agent inspects linked PRs and work already in progress.

The oneshot service and an advisory lock prevent overlapping maintenance runs.
An unresolved issue is retried after six hours, or sooner when GitHub reports it
changed after the completed visit's final GitHub readback. A successful visit
stores that post-run timestamp, so its own comments do not trigger another run.
External updates to a visited issue during the run are also absorbed and may
wait for the six-hour retry; the poll does not distinguish comment authors.
New issues arriving during a run remain eligible for the next check, as do
unvisited issues changed during it. Failed invocations remain eligible for retry.
A check that finds no change posts no GitHub comment: routine readbacks and
unchanged blockers belong in the local run report. Network
or authentication failures fail the check visibly; they do not imply an empty
queue. Agent sessions use the installed Codex configuration/model, existing Rob
authentication, and the authorized unrestricted, noninteractive execution mode.
The reviewed `tools/maintenance/issue_prompt.md` carries the scope and mandatory
PB test/GPU policy. No agent is started for an empty queue. Subagents are reserved
for a backlog that warrants delegation.

Installation uses stable copies rather than executing from a changing worktree:

```bash
install -d -m 700 ~/.local/lib/prismabuild-maintenance ~/.config/systemd/user
install -m 600 tools/maintenance/watch_issues.py tools/maintenance/issue_prompt.md ~/.local/lib/prismabuild-maintenance/
install -m 644 fleet/maintenance/prismabuild-issues.service fleet/maintenance/prismabuild-issues.timer ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now prismabuild-issues.timer
systemctl --user start prismabuild-issues.service
```

`systemctl --user status prismabuild-issues.timer` shows the next check;
`journalctl --user -u prismabuild-issues.service` shows polling failures and run
locations. `~/.local/state/prismabuild-maintenance/state.json` records check times,
remaining issue numbers and the latest run directory. Each private run directory
contains the prompt, Codex event log, and final summary. A successful process exit
does not mean every issue was fixed; check remaining GitHub issues and the report.
These local logs persist and should be retained as evidence or pruned deliberately.

Pause future checks with `systemctl --user disable --now prismabuild-issues.timer`;
this does not terminate an active repair. The timer depends on Sparky being up,
the user manager running, and working GitHub/Codex authentication. It catches up
after downtime. This is operator automation, separate from PB's workload queue.
