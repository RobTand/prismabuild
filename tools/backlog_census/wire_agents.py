#!/usr/bin/env python3
"""Install or check the shared intake instructions in a task worktree."""
import argparse
from pathlib import Path

from census import REPOS

START = "<!-- ticket-intake:start -->"
END = "<!-- ticket-intake:end -->"
SOURCE = Path(__file__).parent / "to_tickets"


def install(root: Path, repo: str, *, check=False) -> list[str]:
    command = ("tools/backlog_census/intake.py" if repo == "RobTand/prismabuild" else
               "/home/rob/paperclip-repos/prismabuild/tools/backlog_census/intake.py")
    section = f"""{START}
## New GitHub work

Create new work through the intake gate as a native child of an open GitHub
issue. Save the body in your task worktree or evidence directory, then run:

```bash
python3 {command} \\
  --repo {repo} --parent PARENT_NUMBER --priority P2 \\
  --title 'Ticket title' --body-file BODY_FILE
```

This is a read-only dry run. After it passes, add `--apply` to create the child.
The command checks all three repos with the census duplicate matcher. It refuses
likely duplicates and P0/P1 bodies without a nonempty Reproduction or Measurement
section. Reuse the issue named by a duplicate refusal or revise the scope.
Use existing labels only. Do not bypass the command with a direct issue write.

The repo-local `to-tickets` skill preserves Matt Pocock's plan approval step and
uses this gate to publish. Read `.agents/skills/to-tickets/SKILL.md`; Claude Code
also discovers it through `.claude/skills/to-tickets`. These instructions require
the reviewed prismabuild intake change to have landed before publication.
OMP issuegraph remains stopped; its future hook is documented in prismabuild's
`tools/backlog_census/INTAKE.md`.
{END}"""
    agents = root / "AGENTS.md"
    text = agents.read_text()
    if START in text:
        before, remainder = text.split(START, 1)
        _, after = remainder.split(END, 1)
        text = before + section + after
    else:
        heading, rest = text.split("\n", 1)
        text = heading + "\n\n" + section + "\n" + rest
    expected = {agents: text.encode()}
    for name in ("SKILL.md", "LICENSE"):
        expected[root / ".agents/skills/to-tickets" / name] = (SOURCE / name).read_bytes()
    changed = []
    for path, data in expected.items():
        if not path.exists() or path.read_bytes() != data:
            changed.append(str(path.relative_to(root)))
            if not check:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(data)
    link = root / ".claude/skills/to-tickets"
    target = "../../.agents/skills/to-tickets"
    if not link.is_symlink() or str(link.readlink()) != target:
        if link.exists() or link.is_symlink():
            raise ValueError(f"preserve existing skill at {link}")
        changed.append(str(link.relative_to(root)))
        if not check:
            link.parent.mkdir(parents=True, exist_ok=True)
            link.symlink_to(target)
    return changed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--repo", required=True, choices=REPOS)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    changed = install(args.root, args.repo, check=args.check)
    print("\n".join(changed) if changed else "intake instructions current")
    raise SystemExit(1 if args.check and changed else 0)
