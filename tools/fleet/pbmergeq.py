"""A batching merge queue that tests each batch with one ``pbtest.py`` run (#1417).

A repository whose full suite takes most of an hour cannot afford a suite run
per pull request, and a suite that only runs after merging lets ``main`` break.
This client takes the pull requests queued for a repository, merges them in
queue order onto a fresh worktree of ``origin/main``, runs the whole suite
once through the published ``pbtest.py``, and posts a commit status on every
tested head. PrismaBuild still owns sharding and placement; this file only
submits ``pbtest.py`` runs and reads their ``--json`` reports.

The verdict is a failure-set difference, by node ID, not a count. A candidate
is red when a node ID fails on it and not on its base, so a ``main`` that is
already red does not block every batch. The base is tested only on the files
the candidate failed in, which is all the difference needs; a tree whose full
results are known (a green candidate that was then merged) costs nothing.

On red, the newly failing files are re-run on the candidate. A node that
passes there is a flake: it is recorded, an issue is filed once per node ID,
and it does not block. The rest are bisected over batch prefixes, re-running
only those files, and the first prefix that fails names the culprit.

A shard that printed no pytest summary is *inconclusive*. Its files are
re-run; if they stay inconclusive the batch posts nothing and retries later.
No verdict is ever built from a shard nobody observed.

Three modes: ``dry-run`` writes what it would post and posts nothing;
``status`` posts statuses and comments and merges nothing; ``merge`` also
merges, and refuses to start unless the config's ``merge_enabled_file``
exists. Queue, batch and a ledger of every status and merge live in
``state_dir``, so a restart resumes without a double post or merge.
"""

from __future__ import annotations

import argparse
import contextlib
import dataclasses
import fcntl
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import time
from collections.abc import Callable, Iterable
from pathlib import Path

from pbmergeq_runtime import (
    RuntimeSelectionError,
    baseline_identity,
    runtime_refusal,
    select_runtime,
    validate_policy,
)
from prismabuild import core

#: GitHub refuses a commit status description longer than this.
DESCRIPTION_LIMIT = 140
#: The most often a phase may go without a log line (Rob, closed-loop rule).
HEARTBEAT_S = 60.0
MODES = ("dry-run", "status", "merge")


# --------------------------------------------------------------------------
# Configuration


@dataclasses.dataclass(frozen=True)
class Config:
    repo: str                    # owner/name
    context: str                 # commit status context, e.g. pb-tests
    state_dir: Path
    pbtest: Path                 # the PUBLISHED pbtest.py
    client_python: str           # an interpreter with pytest, to run pbtest.py
    test_python: str             # static path or declared pin template
    pbtest_args: tuple[str, ...]
    test_paths: tuple[str, ...] = ("tests",)
    exclude: tuple[str, ...] = ()
    skip_fleet_data_files: bool = False
    author: str = ""
    remote: str = ""             # fetch URL; default https://github.com/<repo>.git
    base_branch: str = "main"
    batch_cap: int = 8
    poll_s: float = 30.0
    inconclusive_retries: int = 2
    stuck_backoff_s: float = 600.0
    max_attempts: int = 3        # charged re-queues before an entry is dropped
    where: str = ""              # human pointer to a batch dir, {batch} expands
    flake_labels: tuple[str, ...] = ("P2",)
    merge_enabled_file: str = ""
    tmpdir: str = ""
    runtime_pins: dict = dataclasses.field(default_factory=dict)

    def __post_init__(self) -> None:
        # Freeze the operator's published link once, before any discovery/import
        # or submission. A new queue/config may bind a newer generation.
        object.__setattr__(self, "pbtest", self.pbtest.resolve())

    @property
    def fetch_url(self) -> str:
        return self.remote or f"https://github.com/{self.repo}.git"

    @property
    def owner(self) -> str:
        return self.repo.split("/", 1)[0]


def load_config(path: str | Path) -> Config:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    fields = {f.name: f for f in dataclasses.fields(Config)}
    unknown = sorted(set(raw) - set(fields))
    if unknown:
        raise SystemExit(f"pbmergeq: unknown config key(s) {unknown} in {path}")
    for name, value in list(raw.items()):
        if isinstance(value, list):
            raw[name] = tuple(value)
    raw["state_dir"] = Path(raw["state_dir"])
    raw["pbtest"] = Path(raw["pbtest"])
    cfg = Config(**raw)
    args = list(cfg.pbtest_args)
    # Agent self-validation runs at -10 with an explicit deadline (PB policy).
    if "--timeout-s" not in args:
        raise SystemExit("pbmergeq: pbtest_args must carry an explicit --timeout-s")
    if "--priority" not in args:
        raise SystemExit("pbmergeq: pbtest_args must carry --priority")
    for owned in ("--checkout", "--python", "--json", "--history"):
        if any(arg.split("=", 1)[0] == owned for arg in args):
            raise SystemExit(f"pbmergeq: pbtest_args may not set {owned}; the queue owns it")
    try:
        validate_policy(cfg.test_python, cfg.runtime_pins)
    except RuntimeSelectionError as exc:
        raise SystemExit(f"pbmergeq: {exc}") from exc
    return cfg


# --------------------------------------------------------------------------
# Pure verdict logic


@dataclasses.dataclass
class RunResult:
    """One ``pbtest.py`` run, reduced to what a verdict needs."""

    failed: set[str]
    inconclusive: list[str]          # files no shard observed
    files: list[str]
    receipts: list[str]              # CAS receipts; a failed shard files none
    report: str = ""                 # the --json path
    actions: list[str] = dataclasses.field(default_factory=list)  # every shard's key
    wall_s: float = 0.0
    runtime: dict = dataclasses.field(default_factory=dict)
    runtime_refusal: str = ""
    log: str = ""


def load_published_module(path: Path):
    """One owner for modules belonging to the selected published entrypoint."""
    where = str(path.parent)
    if where not in sys.path:
        sys.path.insert(0, where)
    # Normal import caching can return another generation (or the admitted
    # shard's embedded outcome recorder). Load exactly this owner's file.
    name = f"pbmergeq_published_{path.stem}:{path.parent}"
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load published module {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(name, None)
        raise
    return module


def load_outcomes(pbtest: Path):
    """``pbtest_outcomes`` from beside the published ``pbtest.py``."""
    return load_published_module(pbtest.with_name("pbtest_outcomes.py"))


def reduce_report(report: list[dict], outcomes) -> RunResult:
    """Failing node IDs and inconclusive files out of a ``pbtest --json`` report.

    A shard that ran and printed an outcome record contributes every node
    pytest counted ``failed`` or ``error`` (a collection error is the file's
    node ID), and every collected node that never ran. A shard without a
    summary or without a record contributes its files as inconclusive.
    """

    failed: set[str] = set()
    inconclusive: list[str] = []
    files: list[str] = []
    receipts: list[str] = []
    actions: list[str] = []
    refusals: list[str] = []
    for shard in report:
        files.extend(shard.get("files") or ())
        if shard.get("action_key"):
            actions.append(shard["action_key"])
        if shard.get("receipt_path"):
            receipts.append(shard["receipt_path"])
        record = outcomes.parse(shard.get("output") or "") if shard.get("ran") else None
        if record is None:
            refusal = runtime_refusal(shard.get("output") or "")
            if refusal:
                refusals.append(refusal)
            inconclusive.extend(shard.get("files") or ())
            continue
        for row in record.get("reports") or ():
            entry = dict(zip(outcomes.REPORT_FIELDS, row, strict=False))
            if entry["category"] in ("failed", "error"):
                failed.add(entry["nodeid"])
        reconciliation = shard.get("reconciliation") or {}
        failed.update(f"{nodeid} (never ran)" for nodeid in reconciliation.get("never_ran") or ())
        inconclusive.extend(reconciliation.get("missing_files") or ())
    return RunResult(failed=failed, inconclusive=sorted(set(inconclusive)),
                     files=files, receipts=receipts, actions=actions,
                     runtime_refusal="\n".join(dict.fromkeys(refusals)))


def node_file(nodeid: str) -> str:
    return nodeid.split("::", 1)[0].split(" ", 1)[0]


def files_of(nodeids: Iterable[str]) -> list[str]:
    return sorted({node_file(n) for n in nodeids})


def first_failing_prefix(count: int, fails: Callable[[int], bool]) -> int:
    """The smallest k in 1..count whose prefix fails, given that ``count`` fails."""

    low, high = 1, count
    while low < high:
        middle = (low + high) // 2
        if fails(middle):
            high = middle
        else:
            low = middle + 1
    return high


def clip(text: str, limit: int = DESCRIPTION_LIMIT) -> str:
    return text if len(text) <= limit else text[:limit - 3] + "..."


def describe(batch: str, base: str, new: list[str], shared: int, where: str) -> str:
    """A commit status description: batch, base, and the failure-set verdict."""

    if not new:
        return clip(f"{batch} base {base[:8]}: 0 new failing node IDs vs main "
                    f"(set diff; {shared} shared with main) {where}")
    return clip(f"{batch} base {base[:8]}: {len(new)} new failing vs main "
                f"(set diff): {new[0]}")


def eligibility(pr: dict, cfg: Config) -> str | None:
    """Why a pull request may not be tested on the fleet, or ``None``.

    The repository is public: only a head branch in the repository itself,
    by the configured author, is ever fetched. Never a fork.
    """

    if pr.get("state") != "OPEN":
        return f"state {pr.get('state')}"
    if pr.get("isDraft"):
        return "draft"
    if pr.get("isCrossRepository"):
        return "head is in a fork"
    owner = (pr.get("headRepositoryOwner") or {}).get("login")
    if owner != cfg.owner:
        return f"head repository owner {owner!r} is not {cfg.owner!r}"
    login = (pr.get("author") or {}).get("login")
    if cfg.author and login != cfg.author:
        return f"author {login!r} is not {cfg.author!r}"
    if pr.get("baseRefName") != cfg.base_branch:
        return f"base {pr.get('baseRefName')!r} is not {cfg.base_branch!r}"
    return None


# --------------------------------------------------------------------------
# Durable state


def now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def write_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


class Store:
    """Queue, current batch, history, ledger and logs under one directory."""

    def __init__(self, root: Path):
        self.root = root
        self.inbox = root / "inbox"
        self.batches = root / "batches"
        self.baselines = root / "baselines"
        for directory in (self.inbox, self.batches, self.baselines):
            directory.mkdir(parents=True, exist_ok=True)
        self.state_path = root / "state.json"
        self.ledger_path = root / "ledger.jsonl"
        self.events_path = root / "events.log"
        self.state = (json.loads(self.state_path.read_text(encoding="utf-8"))
                      if self.state_path.exists() else
                      {"seq": 0, "queue": [], "batch": None, "history": [],
                       "flakes": {}, "not_before": 0.0, "mode": None})

    def save(self) -> None:
        write_atomic(self.state_path, json.dumps(self.state, indent=1))

    def event(self, batch: str | None, text: str) -> None:
        line = f"{now()} {batch or '-'} {text}"
        with self.events_path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
        print(line, flush=True)

    # The ledger is the idempotency record: every status and merge, appended
    # before the batch moves on, read before anything is posted again.
    def ledger(self) -> list[dict]:
        if not self.ledger_path.exists():
            return []
        return [json.loads(line) for line in
                self.ledger_path.read_text(encoding="utf-8").splitlines() if line]

    def record(self, entry: dict) -> None:
        with self.ledger_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"at": now(), **entry}) + "\n")
            handle.flush()
            os.fsync(handle.fileno())

    def recorded(self, **match) -> bool:
        return any(all(row.get(k) == v for k, v in match.items()) for row in self.ledger())

    # The inbox is how other processes enqueue: one file each, renamed into
    # place, so nothing but the daemon ever writes state.json.
    def enqueue_request(self, pr: int, sha: str | None) -> Path:
        path = self.inbox / f"{time.time_ns()}-{pr}.json"
        write_atomic(path, json.dumps({"pr": pr, "sha": sha, "at": now()}))
        return path

    def ingest(self) -> list[dict]:
        taken = []
        for path in sorted(self.inbox.glob("*.json")):
            request = json.loads(path.read_text(encoding="utf-8"))
            self.add(int(request["pr"]), request.get("sha"), front=False)
            taken.append(request)
            path.unlink()
        if taken:
            self.save()
        return taken

    def add(self, pr: int, sha: str | None, *, front: bool, attempts: int = 0) -> None:
        queue = [entry for entry in self.state["queue"] if entry["pr"] != pr]
        entry = {"pr": pr, "sha": sha, "at": now(), "attempts": attempts}
        self.state["queue"] = [entry, *queue] if front else [*queue, entry]

    def baseline_path(self, identity: str) -> Path:
        return self.baselines / f"{identity}.json"

    def baseline(self, tree: str) -> dict[str, list[str]]:
        path = self.baseline_path(tree)
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}

    def remember_baseline(self, tree: str, files: Iterable[str], failed: Iterable[str]) -> None:
        known = self.baseline(tree)
        by_file: dict[str, list[str]] = {name: [] for name in files}
        for nodeid in failed:
            by_file.setdefault(node_file(nodeid), []).append(nodeid)
        known.update({name: sorted(nodes) for name, nodes in by_file.items()})
        write_atomic(self.baseline_path(tree), json.dumps(known, indent=1))

    def known_failure(self, identity: str) -> dict | None:
        path = self.baselines / f"known-failure-{identity}.json"
        if not path.exists():
            return None
        record = json.loads(path.read_text(encoding="utf-8"))
        if (not isinstance(record, dict)
                or record.get("schema") != "pbmergeq.known_failure.v1"
                or record.get("identity") != identity
                or not isinstance(record.get("new"), list)
                or not record["new"]):
            raise ValueError(f"invalid known-failure record: {path}")
        return record

    def remember_known_failure(self, identity: str, record: dict) -> None:
        write_atomic(self.baselines / f"known-failure-{identity}.json",
                     json.dumps({"schema": "pbmergeq.known_failure.v1",
                                 "identity": identity, **record}, indent=1))

    def status_text(self) -> str:
        state = self.state
        lines = [f"pbmergeq {self.root}  updated {now()}  mode {state.get('mode')}"]
        if state.get("not_before", 0) > time.time():
            lines.append(f"backing off until {time.strftime('%H:%M:%SZ', time.gmtime(state['not_before']))}")
        for bid, blocked in state.get("runtime_blocked", {}).items():
            lines.append(f"runtime-blocked {bid}: {blocked['reason']}")
            lines.append(f"  repair/provision the named runtime, then resume-runtime {bid}; "
                         "fresh checkout selection and worker guards still apply")
        for bid, blocked in state.get("known_failure_blocked", {}).items():
            lines.append(f"known-failure-blocked {bid}: {blocked['reason']}")
            lines.append("  enqueue a corrected full head for fresh qualification; "
                         "no passing result was reused")
        batch = state.get("batch")
        if batch:
            prs = ", ".join(f"#{p['pr']}@{p['sha'][:8]}" for p in batch.get("included", []))
            lines.append(f"current batch {batch['id']} phase {batch['phase']} since "
                         f"{batch['phase_at']} base {batch.get('base', '')[:8]} "
                         f"candidate {batch.get('candidate', '')[:8]} PRs [{prs}]")
        else:
            lines.append("current batch: none")
        lines.append(f"queue ({len(state['queue'])}):")
        lines.extend(f"  #{e['pr']} sha {(e.get('sha') or 'head')[:8]} since {e['at']}"
                     for e in state["queue"])
        lines.append("last results:")
        lines.extend(f"  {h['id']} {h['verdict']} {h['wall_s']:.0f}s {h['summary']}"
                     for h in state["history"][-10:])
        return "\n".join(lines) + "\n"

    def write_status(self) -> None:
        write_atomic(self.root / "STATUS.txt", self.status_text())


@contextlib.contextmanager
def single_instance(root: Path):
    root.mkdir(parents=True, exist_ok=True)
    with (root / "lock").open("w") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit(f"pbmergeq: another instance holds {root / 'lock'}") from None
        yield


# --------------------------------------------------------------------------
# GitHub and Git adapters


def call_tool(command: list[str], *, cwd: str | Path | None = None, check: bool = True,
        stdin: str | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(command, cwd=cwd, input=stdin, text=True,
                          capture_output=True, check=check)


class GitHub:
    """The ``gh`` calls the queue makes. Writes are idempotent by the ledger."""

    def __init__(self, cfg: Config, store: Store, *, dry: bool):
        self.cfg, self.store, self.dry = cfg, store, dry
        self.would: list[str] = []

    def pr(self, number: int) -> dict:
        fields = ("number,state,isDraft,author,headRefOid,headRefName,"
                  "isCrossRepository,headRepositoryOwner,baseRefName,title")
        out = call_tool(["gh", "pr", "view", str(number), "-R", self.cfg.repo, "--json", fields])
        return json.loads(out.stdout)

    def _write(self, what: str, command: list[str], stdin: str | None = None) -> bool:
        if self.dry:
            self.would.append(what)
            self.store.event(None, f"DRY-RUN would {what}")
            return True
        result = call_tool(command, check=False, stdin=stdin)
        if result.returncode != 0:
            self.store.event(None, f"gh refused {what}: {result.stderr.strip()[:300]}")
            return False
        return True

    def post_status(self, batch: str, sha: str, state: str, description: str) -> bool:
        if len(description) > DESCRIPTION_LIMIT:
            raise ValueError("status description over GitHub's limit")
        key = {"kind": "status", "sha": sha, "context": self.cfg.context,
               "state": state, "batch": batch}
        if not self.dry and self.store.recorded(**key):
            return True
        ok = self._write(f"post {self.cfg.context}={state} on {sha}: {description}",
                         ["gh", "api", "-X", "POST",
                          f"repos/{self.cfg.repo}/statuses/{sha}",
                          "-f", f"state={state}", "-f", f"context={self.cfg.context}",
                          "-f", f"description={description}"])
        if ok and not self.dry:
            self.store.record({**key, "description": description})
        return ok

    def comment(self, batch: str, number: int, key: str, body: str) -> bool:
        mark = {"kind": "comment", "pr": number, "batch": batch, "key": key}
        if not self.dry and self.store.recorded(**mark):
            return True
        ok = self._write(f"comment on #{number} ({key})",
                         ["gh", "pr", "comment", str(number), "-R", self.cfg.repo,
                          "--body-file", "-"], stdin=body)
        if ok and not self.dry:
            self.store.record(mark)
        return ok

    def merge(self, batch: str, number: int, sha: str) -> bool:
        if self.dry:
            raise RuntimeError("a dry run never merges")
        if self.store.recorded(kind="merge", pr=number, sha=sha):
            return True
        if self.pr(number)["state"] == "MERGED":
            self.store.record({"kind": "merge", "pr": number, "sha": sha,
                               "batch": batch, "note": "already merged"})
            return True
        ok = self._write(f"merge #{number} at {sha}",
                         ["gh", "pr", "merge", str(number), "-R", self.cfg.repo,
                          "--merge", "--match-head-commit", sha])
        if ok:
            self.store.record({"kind": "merge", "pr": number, "sha": sha, "batch": batch})
        return ok

    def file_flake(self, batch: str, nodeid: str, candidate: str) -> str | None:
        title = f"[P2] Flaky test: {nodeid}"
        found = call_tool(["gh", "issue", "list", "-R", self.cfg.repo, "--state", "all",
                     "--search", f'"{node_file(nodeid)}" in:title', "--json",
                     "number,title", "--limit", "50"], check=False)
        if found.returncode == 0:
            for issue in json.loads(found.stdout or "[]"):
                if issue["title"] == title:
                    return f"#{issue['number']}"
        body = (f"`{nodeid}` failed in merge-queue batch {batch} on candidate "
                f"{candidate} and passed when its file was re-run at once on the "
                "same candidate.\n\nP2: a flaky test can block or mislead a "
                "pre-merge verdict. The merge queue does not block on it; it "
                "records it and files this issue once per node ID.\n")
        command = ["gh", "issue", "create", "-R", self.cfg.repo, "--title", title,
                   "--body-file", "-"]
        for label in self.cfg.flake_labels:
            command += ["--label", label]
        if self.dry:
            self._write(f"file issue {title!r}", command)
            return None
        out = call_tool(command, check=False, stdin=body)
        if out.returncode != 0:
            self.store.event(batch, f"flake issue refused for {nodeid}: {out.stderr.strip()[:200]}")
            return None
        return out.stdout.strip()


class Mirror:
    """A bare clone the queue alone owns, and throwaway worktrees off it."""

    def __init__(self, cfg: Config, root: Path):
        self.cfg = cfg
        self.git_dir = root / "repo.git"
        self.worktrees = root / "wt"

    def git(self, *args: str, cwd: Path | None = None, check: bool = True):
        base = ["git"] if cwd else ["git", f"--git-dir={self.git_dir}"]
        return call_tool([*base, *args], cwd=cwd, check=check)

    def ensure(self) -> None:
        if not self.git_dir.exists():
            call_tool(["git", "init", "--bare", "-q", str(self.git_dir)])
            self.git("config", "user.name", "pbmergeq")
            self.git("config", "user.email", "pbmergeq@localhost")
        self.worktrees.mkdir(parents=True, exist_ok=True)

    def fetch(self, heads: dict[int, str]) -> str:
        """Fetch the base and GitHub-selected immutable commits, not pull refs."""

        refs = [f"+refs/heads/{self.cfg.base_branch}:refs/mq/base"]
        for number, sha in heads.items():
            if (not isinstance(sha, str) or len(sha) != 40
                    or any(c not in "0123456789abcdef" for c in sha)):
                raise ValueError(f"selected head for #{number} is not a full Git commit")
            refs.append(f"+{sha}:refs/mq/pr/{number}")
        self.git("fetch", "-q", "--no-tags", self.cfg.fetch_url, *refs)
        for number, sha in heads.items():
            fetched = self.rev(f"refs/mq/pr/{number}")
            if fetched != sha:
                raise ValueError(f"fetched head for #{number} differs from selected {sha}")
            if self.git("cat-file", "-t", fetched).stdout.strip() != "commit":
                raise ValueError(f"selected head for #{number} is not a commit")
        return self.rev("refs/mq/base")

    def remote_base(self) -> str:
        out = self.git("ls-remote", self.cfg.fetch_url, f"refs/heads/{self.cfg.base_branch}")
        return out.stdout.split()[0]

    def rev(self, ref: str) -> str:
        return self.git("rev-parse", ref).stdout.strip()

    def tree(self, commit: str) -> str:
        return self.rev(f"{commit}^{{tree}}")

    def worktree(self, name: str, commit: str) -> Path:
        path = self.worktrees / name
        if path.exists():
            self.drop(path)
        self.git("worktree", "add", "-q", "--detach", str(path), commit)
        return path

    def merge(self, path: Path, commit: str, message: str) -> bool:
        result = self.git("merge", "--no-ff", "-q", "-m", message, commit, cwd=path, check=False)
        if result.returncode != 0:
            self.git("merge", "--abort", cwd=path, check=False)
            return False
        return True

    def head(self, path: Path) -> str:
        return self.git("rev-parse", "HEAD", cwd=path).stdout.strip()

    def drop(self, path: Path) -> None:
        self.git("worktree", "remove", "--force", str(path), check=False)
        shutil.rmtree(path, ignore_errors=True)
        self.git("worktree", "prune", check=False)


# --------------------------------------------------------------------------
# pbtest runs


def checkout_source(checkout: Path) -> dict:
    source = call_tool(["git", "rev-parse", "HEAD", "HEAD^{tree}"],
                       cwd=checkout, check=False)
    return {"checkout": str(checkout),
            "source": source.stdout.splitlines() if source.returncode == 0 else []}


def checkout_file_identities(checkout: Path, tree: str) -> dict[str, str] | None:
    """Read immutable Git file modes/blobs without evaluating source code."""
    result = call_tool(["git", "ls-tree", "-r", "--full-tree", "-z", tree],
                       cwd=checkout, check=False)
    if result.returncode:
        return None
    identities = {}
    for entry in result.stdout.split("\0"):
        if entry:
            identity, name = entry.split("\t", 1)
            identities[name] = identity
    return identities


def checkout_runtime(cfg: Config, checkout: Path) -> dict:
    return {**select_runtime(checkout, cfg.test_python, cfg.runtime_pins),
            **checkout_source(checkout), "pbtest": str(cfg.pbtest)}


def baseline_key(cfg: Config, tree: str, runtime: dict) -> str:
    # Runtime evidence names the actually launched entrypoint; the config's
    # publisher link was frozen before discovery and is never resolved here.
    return baseline_identity(tree, runtime, dataclasses.asdict(cfg))


def candidate_failure_identity(cfg: Config, tree: str, base_tree: str,
                               runtime: dict, files: list[str]) -> str:
    """Hold only an identical red tree under its complete qualification domain."""
    return core.canonical_sha256({
        "schema": "pbmergeq.failed_candidate_identity.v1",
        "baseline": baseline_key(cfg, tree, runtime),
        "base_tree": base_tree, "files": files,
    })


def test_command(cfg: Config, checkout: Path, files: list[str], report: Path,
                 history: list[str], *, python: str) -> list[str]:
    """Build the submitted argv independently of interpreter selection policy."""
    return [cfg.client_python, str(cfg.pbtest),
            "--checkout", str(checkout), "--python", python,
            "--json", str(report), *cfg.pbtest_args, *history, *files]


class Runner:
    """Submits ``pbtest.py`` runs and waits for them with a heartbeat."""

    def __init__(self, cfg: Config, store: Store):
        self.cfg, self.store = cfg, store
        self.outcomes = load_outcomes(cfg.pbtest)
        self._pbtest = None

    def discover(self, checkout: Path) -> tuple[list[str], list[str]]:
        """Every test file the suite holds, and the fleet-data files left out."""

        if self._pbtest is None:
            self._pbtest = load_published_module(self.cfg.pbtest)
        files = [f for f in self._pbtest.discover(checkout, list(self.cfg.test_paths))
                 if f not in self.cfg.exclude]
        left_out: list[str] = []
        if self.cfg.skip_fleet_data_files:
            left_out = self._pbtest.fleet_data_files(checkout, files)
            files = [f for f in files if f not in left_out]
        return files, left_out

    def _history_source(self, latest: Path) -> list[str]:
        """Read the prior report's source, including existing typed v3 records."""
        source = self.store.state.get("history_source")
        def valid(value):
            return (isinstance(value, list) and len(value) == 2
                    and all(isinstance(part, str) and len(part) == 40
                            and all(c in "0123456789abcdef" for c in part) for part in value))

        if valid(source):
            return source
        # Pre-1438 typed records already bind their full runtime/source in the
        # terminal batch. Unidentified legacy reports gain no identity here.
        try:
            batch = json.loads(latest.with_name("batch.json").read_text())
            run = batch["runs"]["candidate"]
            source = run["runtime"]["source"]
            if (run["report"] == str(latest) and valid(source)
                    and source[1] == batch["candidate_tree"]):
                return source
        except (OSError, ValueError, KeyError, TypeError):
            pass
        return []

    def history(self, runtime: dict) -> list[str]:
        latest = self.store.state.get("history_report")
        source = runtime.get("source") or []
        identity = baseline_key(self.cfg, source[1], runtime) if len(source) == 2 else None
        if not latest or not Path(latest).is_file():
            return []
        if identity and identity == self.store.state.get("history_runtime"):
            return ["--history", latest]
        previous = self._history_source(Path(latest))
        if (len(source) != 2 or len(previous) != 2
                or baseline_key(self.cfg, previous[1], runtime)
                    != self.store.state.get("history_runtime")):
            return []
        # This is historical placement advice, never a baseline verdict.
        # Preserve complete original rows/receipts; a mixed changed-file row
        # is discarded rather than manufacturing a smaller outcome record.
        old_files = checkout_file_identities(Path(runtime["checkout"]), previous[1])
        new_files = checkout_file_identities(Path(runtime["checkout"]), source[1])
        if old_files is None or new_files is None:
            return []
        # Collection plugins and pytest configuration govern every module's
        # meaning, so their changes invalidate the whole optional hint set.
        def domain_file(name):
            leaf = name.rsplit("/", 1)[-1]
            return (leaf in {"conftest.py", "pytest.ini", "pyproject.toml", "setup.cfg", "tox.ini"}
                    or (leaf == "__init__.py" and "tests" in name.split("/")[:-1]))

        common = {name for name in old_files.keys() | new_files.keys() if domain_file(name)}
        if any(old_files.get(name) != new_files.get(name) for name in common):
            return []
        try:
            report = json.loads(Path(latest).read_text())
            if not isinstance(report, list):
                return []
            rows = []
            for row in report:
                if not isinstance(row, dict) or not row.get("ran"):
                    continue
                record = self.outcomes.parse(row.get("output") or "")
                durations = record.get("file_durations") if record else None
                files = row.get("files")
                if (not isinstance(durations, dict) or not durations
                        or not isinstance(files, list) or not files
                        or record.get("collect_only")):
                    continue
                names = [*files, *durations]
                if all(isinstance(name, str) and name in old_files
                       and old_files[name] == new_files.get(name) for name in names):
                    rows.append(row)
            if not rows:
                return []
            raw = core._sorted_json_bytes(rows)
            data = raw.decode("utf-8")
            digest = core.raw_sha256(raw)
            hint = self.store.baselines / f"duration-hints-{digest}.json"
            write_atomic(hint, data)
            return ["--history", str(hint)]
        except (OSError, ValueError, TypeError):
            return []

    def command(self, checkout: Path, files: list[str], report: Path) -> list[str]:
        runtime = checkout_runtime(self.cfg, checkout)
        return test_command(self.cfg, checkout, files, report, self.history(runtime),
                            python=runtime["python"])

    def run_many(self, batch: str, outdir: Path,
                 jobs: list[tuple[str, Path, list[str]]]) -> dict[str, RunResult]:
        """Run several pbtest invocations at once; retry inconclusive files."""

        results = self._wait(batch, outdir, jobs)
        for attempt in range(1, self.cfg.inconclusive_retries + 1):
            retry = [(f"{name}.retry{attempt}", checkout, results[name].inconclusive)
                     for name, checkout, _ in jobs if results[name].inconclusive
                     and not results[name].runtime_refusal]
            if not retry:
                break
            for name, _, files in retry:
                self.store.event(batch, f"run {name}: {len(files)} inconclusive file(s) re-run")
            again = self._wait(batch, outdir, retry)
            for name, _, _ in jobs:
                key = f"{name}.retry{attempt}"
                if key in again:
                    redo = set(results[name].inconclusive)
                    merged = results[name]
                    merged.failed = ({n for n in merged.failed if node_file(n) not in redo}
                                     | again[key].failed)
                    merged.inconclusive = again[key].inconclusive
                    merged.receipts += again[key].receipts
                    merged.actions += again[key].actions
                    merged.wall_s += again[key].wall_s
                    merged.runtime_refusal = again[key].runtime_refusal
                    merged.log = again[key].log
        return results

    def _wait(self, batch: str, outdir: Path,
              jobs: list[tuple[str, Path, list[str]]]) -> dict[str, RunResult]:
        outdir.mkdir(parents=True, exist_ok=True)
        env = dict(os.environ)
        if self.cfg.tmpdir:
            env["TMPDIR"] = self.cfg.tmpdir
        live, results = {}, {}
        for name, checkout, files in jobs:
            report = outdir / f"{name}.json"
            log = outdir / f"{name}.log"
            report.unlink(missing_ok=True)
            try:
                runtime = checkout_runtime(self.cfg, checkout)
            except RuntimeSelectionError as exc:
                reason = f"pbmergeq: runtime selection refused: {exc}"
                write_atomic(log, reason + "\n")
                results[name] = RunResult(set(), list(files), list(files), [],
                                         report=str(report), log=str(log),
                                         runtime={"python": self.cfg.test_python,
                                                  "pbtest": str(self.cfg.pbtest),
                                                  **checkout_source(checkout),
                                                  "policy": self.cfg.runtime_pins},
                                         runtime_refusal=reason)
                continue
            command = test_command(self.cfg, checkout, files, report, self.history(runtime),
                                   python=runtime["python"])
            handle = log.open("w", encoding="utf-8")
            process = subprocess.Popen(command, stdout=handle,
                                       stderr=subprocess.STDOUT, env=env, text=True)
            live[name] = (process, handle, report, log, runtime, time.monotonic())
            self.store.event(batch, f"run {name} started: {len(files)} file(s) on "
                                    f"{checkout.name}, pid {process.pid}, log {log}")
        last = time.monotonic()
        while any(p.poll() is None for p, *_ in live.values()):
            time.sleep(2)
            if time.monotonic() - last >= HEARTBEAT_S:
                last = time.monotonic()
                for name, (process, _, _, log, _, started) in live.items():
                    if process.poll() is None:
                        self.store.event(batch, f"run {name} waiting "
                                                f"{time.monotonic() - started:.0f}s: "
                                                f"{self._progress(log)} shard(s) reported")
                self.store.write_status()
        for name, (process, handle, report, log, runtime, started) in live.items():
            handle.close()
            wall = time.monotonic() - started
            files = next(f for n, _, f in jobs if n == name)
            result = self.read_result(report, files, log=log, returncode=process.returncode)
            result.runtime, result.log = runtime, str(log)
            result.report, result.wall_s = str(report), wall
            results[name] = result
            self.store.event(batch, f"run {name} ended rc={process.returncode} in {wall:.0f}s: "
                                    f"{len(result.failed)} failing node(s), "
                                    f"{len(result.inconclusive)} inconclusive file(s)")
        return results

    def read_result(self, report: Path, files: list[str], *, log: Path,
                    returncode: int) -> RunResult:
        if report.exists():
            # The report owns classification: only its unobserved shards can
            # name a refusal. Failed-client echoes of observed pytest cannot.
            return reduce_report(json.loads(report.read_text(encoding="utf-8")),
                                 self.outcomes)
        result = RunResult(failed=set(), inconclusive=list(files), files=list(files),
                           receipts=[])
        if returncode:
            result.runtime_refusal = runtime_refusal(log.read_text(encoding="utf-8", errors="replace"))
        return result

    @staticmethod
    def _progress(log: Path) -> int:
        try:
            text = log.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return 0
        return sum(1 for line in text.splitlines()
                   if line.startswith("shard ") and (" ok " in line or " rc=" in line))


# --------------------------------------------------------------------------
# The queue


class RuntimeBlocked(Exception):
    """No compatible reviewed runtime observed; operator repair/resume is required."""


class Inconclusive(Exception):
    """A run left files nobody observed after its retries."""


class Queue:
    def __init__(self, cfg: Config, store: Store, github: GitHub, mirror: Mirror,
                 runner: Runner, mode: str, *, once: bool = False):
        self.cfg, self.store, self.github = cfg, store, github
        self.mirror, self.runner, self.mode = mirror, runner, mode
        self.once = once      # a one-off batch never feeds the daemon's queue

    # -- batch record -------------------------------------------------------

    def phase(self, batch: dict, phase: str, note: str = "") -> None:
        batch["phase"], batch["phase_at"] = phase, now()
        batch.setdefault("timeline", []).append([phase, time.time()])
        self.store.state["batch"] = batch
        self.store.save()
        self.store.event(batch["id"], f"phase {phase}" + (f": {note}" if note else ""))
        self.store.write_status()

    def where(self, batch: str) -> str:
        return self.cfg.where.format(batch=batch) if self.cfg.where else str(
            self.store.batches / batch)

    def run(self, batch: dict, outdir: Path,
            jobs: list[tuple[str, Path, list[str]]]) -> dict[str, RunResult]:
        """Run jobs; a file absent from a job's checkout passes there untested.

        A test file a pull request adds is absent from main and from every
        prefix before that pull request. It holds no tests there, so it fails
        nothing; pbtest refuses an argument that is not a file, and passing
        it would make the whole invocation inconclusive.
        """

        absent: dict[str, list[str]] = {}
        submit = []
        for name, checkout, files in jobs:
            here = [f for f in files if (Path(checkout) / f).exists()]
            absent[name] = [f for f in files if f not in here]
            if absent[name]:
                self.store.event(batch["id"], f"run {name}: {len(absent[name])} file(s) absent "
                                              f"from this checkout pass untested: "
                                              + clip(", ".join(absent[name]), 160))
            if here:
                submit.append((name, checkout, here))
        results = self.runner.run_many(batch["id"], outdir, submit) if submit else {}
        for name, checkout, _ in jobs:
            got = results.get(name) or RunResult(failed=set(), inconclusive=[], files=[],
                                                 receipts=[])
            if not got.runtime:
                try:
                    got.runtime = checkout_runtime(self.cfg, checkout)
                except RuntimeSelectionError as exc:
                    got.runtime_refusal = f"pbmergeq: runtime selection refused: {exc}"
                    got.runtime = {"python": self.cfg.test_python, **checkout_source(checkout),
                                   "pbtest": str(self.cfg.pbtest), "policy": self.cfg.runtime_pins}
            results[name] = dataclasses.replace(got, files=[*got.files, *absent[name]])
            self.record_run(batch, name, results[name])
        return results

    def record_run(self, batch: dict, name: str, result: RunResult) -> None:
        batch.setdefault("runs", {})[name] = {
            "report": result.report, "wall_s": round(result.wall_s, 1),
            "files": len(result.files), "failed": sorted(result.failed),
            "inconclusive": result.inconclusive, "receipts": result.receipts,
            "actions": result.actions, "runtime": result.runtime,
            "runtime_refusal": result.runtime_refusal, "log": result.log}
        self.store.save()

    def check_results(self, batch: dict, results: dict[str, RunResult]) -> None:
        """Record and classify the results owned by one concurrent invocation."""
        for name, result in results.items():
            self.record_run(batch, name, result)
        # Classification belongs to the complete concurrent result set, not
        # caller iteration order. Repairable runtime blocks take precedence.
        for name, result in results.items():
            if result.runtime_refusal:
                raise RuntimeBlocked(f"{name}: {result.runtime_refusal}")
        for name, result in results.items():
            if result.inconclusive:
                raise Inconclusive(f"{name}: {len(result.inconclusive)} file(s) never observed")

    def checked(self, batch: dict, name: str, results: dict[str, RunResult]) -> RunResult:
        self.check_results(batch, results)
        return results[name]

    # -- selection and build -----------------------------------------------

    def select(self, entries: list[dict]) -> tuple[list[dict], list[dict]]:
        """Eligible entries with their current heads, and the ones skipped."""

        chosen, skipped = [], []
        for entry in entries:
            if entry.get("attempts", 0) >= self.cfg.max_attempts:
                skipped.append({**entry, "exhausted": True, "why": (
                    f"re-queued {entry['attempts']} times without a verdict it could "
                    "leave the queue on")})
                continue
            try:
                pr = self.github.pr(entry["pr"])
            except subprocess.CalledProcessError as exc:
                skipped.append({**entry, "why": f"gh pr view failed: {exc.stderr.strip()[:200]}"})
                continue
            why = eligibility(pr, self.cfg)
            if why is None and entry.get("sha") and entry["sha"] != pr["headRefOid"]:
                why = f"head {pr['headRefOid'][:8]} is not the enqueued {entry['sha'][:8]}"
            if why:
                skipped.append({**entry, "why": why})
            else:
                # ``pin`` is what the caller enqueued; a re-queue keeps it, so
                # a later push is refused rather than silently adopted.
                chosen.append({"pr": entry["pr"], "sha": pr["headRefOid"],
                               "pin": entry.get("sha"), "title": pr.get("title", ""),
                               "attempts": entry.get("attempts", 0)})
        return chosen, skipped

    def build(self, batch: dict, prs: list[dict]) -> Path:
        base = self.mirror.fetch({p["pr"]: p["sha"] for p in prs})
        batch["base"], batch["base_tree"] = base, self.mirror.tree(base)
        checkout = self.mirror.worktree(f"{batch['id']}-candidate", base)
        included, dropped = [], []
        for p in prs:
            fetched = self.mirror.rev(f"refs/mq/pr/{p['pr']}")
            if fetched != p["sha"]:
                dropped.append({**p, "why": f"fetched {fetched[:8]}, GitHub says {p['sha'][:8]}"})
                continue
            if self.mirror.merge(checkout, p["sha"], f"pbmergeq {batch['id']}: #{p['pr']}"):
                included.append(p)
                continue
            # A conflict with main itself needs the author; one with an earlier
            # batch member only needs that member merged first.
            alone = self.mirror.worktree(f"{batch['id']}-probe", base)
            clean = self.mirror.merge(alone, p["sha"], "probe")
            self.mirror.drop(alone)
            dropped.append({**p, "why": "conflicts with an earlier batch member"
                            if clean else "conflicts with main", "requeue": clean})
        batch["included"], batch["dropped"] = included, dropped
        batch["candidate"] = self.mirror.head(checkout)
        batch["candidate_tree"] = self.mirror.tree(batch["candidate"])
        return checkout

    # -- the batch ----------------------------------------------------------

    def run_batch(self, entries: list[dict]) -> dict:
        state = self.store.state
        state["seq"] += 1
        batch = {"id": f"b{state['seq']:05d}", "mode": self.mode, "created": now(),
                 "entries": entries}
        self.phase(batch, "selecting", ", ".join(f"#{e['pr']}" for e in entries))
        outdir = self.store.batches / batch["id"]
        try:
            verdict = self._run(batch, outdir)
        except RuntimeBlocked as exc:
            verdict = {"verdict": "runtime-blocked", "summary": str(exc)}
            # Keep queued pins/attempts, but skip only this batch's entries until
            # an operator requests fresh validation. Other queue entries can run.
            affected = batch.get("included", entries)
            state.setdefault("runtime_blocked", {})[batch["id"]] = {
                "reason": str(exc), "entries": affected,
                "runs": batch.get("runs", {}), "base": batch.get("base"),
                "candidate": batch.get("candidate"), "config": dataclasses.asdict(self.cfg)}
            state["runtime_blocked"][batch["id"]]["config"] = json.loads(
                json.dumps(state["runtime_blocked"][batch["id"]]["config"], default=str))
            self.requeue(affected, front=True, charge=False)
        except Inconclusive as exc:
            # Nobody observed some files: no verdict, nothing posted, retry later.
            verdict = {"verdict": "inconclusive", "summary": str(exc)}
            self.requeue(entries, front=True, charge=True)
            state["not_before"] = time.time() + self.cfg.stuck_backoff_s
        except Exception as exc:  # noqa: BLE001 -- a daemon logs and backs off
            verdict = {"verdict": "error", "summary": f"{type(exc).__name__}: {exc}"[:300]}
            self.requeue(entries, front=True, charge=True)
            state["not_before"] = time.time() + self.cfg.stuck_backoff_s
        finally:
            # A queue hold keeps only its existing clean candidate view, so
            # tick can re-observe the complete runtime/discovery domain.
            # Once has no queued hold and keeps no checkout.
            retained = self.store.state.get("known_failure_blocked", {}).get(batch["id"])
            keep = self.mirror.worktrees / f"{batch['id']}-candidate" if retained else None
            for path in list(self.mirror.worktrees.glob(f"{batch['id']}-*")):
                if path != keep:
                    self.mirror.drop(path)
        batch.update(verdict)
        batch["wall_s"] = round(time.time() - batch["timeline"][0][1], 1)
        write_atomic(outdir / "batch.json", json.dumps(batch, indent=1))
        state["history"] = [*state["history"], {
            "id": batch["id"], "verdict": batch["verdict"], "wall_s": batch["wall_s"],
            "summary": batch.get("summary", "")}][-50:]
        self.phase(batch, "done", f"{batch['verdict']} in {batch['wall_s']:.0f}s: "
                                  f"{batch.get('summary', '')}")
        state["batch"] = None
        self.store.save()
        self.store.write_status()
        return batch

    def requeue(self, prs: list[dict], *, front: bool, charge: bool) -> None:
        """Put entries back. ``charge`` counts it against them: a PR that keeps
        coming back for its own reason (refused merge, unobserved run, a
        conflict) is dropped after ``max_attempts`` instead of holding the
        queue head forever; one bumped by another PR's verdict is not."""

        if self.once:
            return
        for p in reversed(prs) if front else prs:
            self.store.add(p["pr"], p.get("pin", p.get("sha")), front=front,
                           attempts=p.get("attempts", 0) + (1 if charge else 0))
        self.store.save()

    def _run(self, batch: dict, outdir: Path) -> dict:
        chosen, skipped = self.select(batch["entries"])
        batch["skipped"] = skipped
        for s in skipped:
            self.store.event(batch["id"], f"skip #{s['pr']}: {s['why']}")
            if s.get("exhausted"):
                self.github.comment(batch["id"], s["pr"], f"exhausted-{batch['id']}",
                                    f"pbmergeq {batch['id']}: this pull request left the "
                                    f"merge queue: {s['why']}. See the batch history in "
                                    f"`{self.where(batch['id'])}` and enqueue it again.")
        if not chosen:
            return {"verdict": "empty", "summary": "nothing eligible"}
        self.phase(batch, "building")
        checkout = self.build(batch, chosen)
        for d in batch["dropped"]:
            self.store.event(batch["id"], f"drop #{d['pr']}: {d['why']}")
            if d.get("requeue"):
                self.requeue([d], front=False, charge=True)
            elif d["why"] == "conflicts with main":
                self.github.comment(batch["id"], d["pr"], f"conflict-{d['sha']}",
                                    f"pbmergeq {batch['id']}: this pull request does not "
                                    f"merge cleanly onto `{self.cfg.base_branch}` at "
                                    f"{batch['base']}. Rebase it and enqueue it again.")
        included = batch["included"]
        if not included:
            return {"verdict": "empty", "summary": "every PR dropped"}
        files, left_out = self.runner.discover(checkout)
        batch["fleet_data_left_out"] = left_out
        # A PR-number exclusion cannot remove code carried by an ordinary
        # descendant. Hold the exact already-red tree before another full run,
        # retaining its original attribution rather than blaming the descendant.
        # Runtime-selection errors still follow the existing runner refusal path.
        try:
            runtime = checkout_runtime(self.cfg, checkout)
        except RuntimeSelectionError:
            runtime = None
        identity = (candidate_failure_identity(
            self.cfg, batch["candidate_tree"], batch["base_tree"], runtime, files)
            if runtime is not None else None)
        known = self.store.known_failure(identity) if identity is not None else None
        if known is not None:
            batch["known_failure"] = known
            reason = f"same failed candidate domain as {known['batch']}; no test submitted"
            affected = [{**p, "pin": p["sha"]} for p in included]
            if not self.once:
                self.store.state.setdefault("known_failure_blocked", {})[batch["id"]] = {
                    "reason": reason, "entries": affected, "identity": identity,
                    "candidate": batch["candidate"], "candidate_tree": batch["candidate_tree"],
                }
                self.requeue(affected, front=True, charge=False)
            return {"verdict": "known-failure-blocked", "summary": reason}
        self.phase(batch, "testing", f"candidate {batch['candidate'][:8]} = base "
                   f"{batch['base'][:8]} + " + ", ".join(f"#{p['pr']}" for p in included)
                   + f"; {len(files)} files" + (f", {len(left_out)} fleet-data file(s) "
                                               "left out" if left_out else ""))
        full = self.checked(batch, "candidate", self.run(
            batch, outdir, [("candidate", checkout, files)]))
        self.store.state["history_report"] = full.report
        self.store.state["history_runtime"] = baseline_key(
            self.cfg, batch["candidate_tree"], full.runtime)
        self.store.state["history_source"] = full.runtime.get("source") or []
        if full.failed:
            new, shared, flakes = self.judge(batch, outdir, checkout, full)
        else:
            new, shared, flakes = [], [], []
        batch.update({"new": new, "shared": shared, "flakes": flakes})
        self.store.remember_baseline(baseline_key(self.cfg, batch["candidate_tree"],
                                                 full.runtime), full.files,
                                     set(full.failed) - set(flakes))
        for nodeid in flakes:
            self.flake(batch, nodeid)
        if new:
            verdict = self.culprit(batch, outdir, new)
            identity = candidate_failure_identity(
                self.cfg, batch["candidate_tree"], batch["base_tree"], full.runtime, files)
            self.store.remember_known_failure(identity, {
                "batch": batch["id"], "candidate": batch["candidate"],
                "candidate_tree": batch["candidate_tree"], "base": batch["base"],
                "base_tree": batch["base_tree"], "new": new,
                "culprit": batch["culprit"], "report": full.report,
            })
            return verdict
        return self.green(batch, shared)

    def judge(self, batch, outdir, checkout, full) -> tuple[list[str], list[str], list[str]]:
        """New, shared-with-main, and flaky node IDs among the candidate's failures."""

        failing_files = files_of(full.failed)
        base_checkout = self.mirror.worktree(f"{batch['id']}-base", batch["base"])
        try:
            base_runtime = checkout_runtime(self.cfg, base_checkout)
        except RuntimeSelectionError as exc:
            reason = f"pbmergeq: runtime selection refused: {exc}"
            self.record_run(batch, "base", RunResult(set(), failing_files, failing_files, [],
                runtime={"python": self.cfg.test_python, **checkout_source(base_checkout),
                         "pbtest": str(self.cfg.pbtest), "policy": self.cfg.runtime_pins}, runtime_refusal=reason))
            raise RuntimeBlocked(f"base: {reason}") from exc
        identity = baseline_key(self.cfg, batch["base_tree"], base_runtime)
        known = self.store.baseline(identity)
        missing = [f for f in failing_files if f not in known]
        jobs = [("rerun", checkout, failing_files)]
        if missing:
            jobs.append(("base", base_checkout, missing))
        self.phase(batch, "judging", f"{len(full.failed)} failing node(s) in "
                   f"{len(failing_files)} file(s); base needs {len(missing)} file(s), "
                   f"{len(failing_files) - len(missing)} known")
        results = self.run(batch, outdir, jobs)
        rerun = self.checked(batch, "rerun", results)
        if missing:
            base = self.checked(batch, "base", results)
            self.store.remember_baseline(identity, missing, base.failed)
            known = self.store.baseline(identity)
        base_failed = {n for nodes in known.values() for n in nodes}
        shared = sorted(full.failed & base_failed)
        candidate_new = full.failed - base_failed
        new = sorted(candidate_new & rerun.failed)
        flakes = sorted(candidate_new - rerun.failed)
        self.store.event(batch["id"], f"failure-set diff vs main: {len(candidate_new)} new, "
                         f"{len(shared)} shared; re-run keeps {len(new)}, "
                         f"{len(flakes)} flake(s)")
        return new, shared, flakes

    def flake(self, batch: dict, nodeid: str) -> None:
        flakes = self.store.state["flakes"]
        if nodeid in flakes or self.mode == "dry-run":
            if self.mode == "dry-run":
                self.store.event(batch["id"], f"DRY-RUN would record flake {nodeid}")
            return
        # A node that never ran is a missing result, not a test to file.
        issue = (self.github.file_flake(batch["id"], nodeid, batch["candidate"])
                 if not nodeid.endswith(" (never ran)") else None)
        flakes[nodeid] = {"batch": batch["id"], "at": now(), "issue": issue}
        self.store.save()
        self.store.event(batch["id"], f"flake {nodeid} recorded, issue {issue}")

    def green(self, batch: dict, shared: list[str]) -> dict:
        self.phase(batch, "posting")
        description = describe(batch["id"], batch["base"], [], len(shared),
                               self.where(batch["id"]))
        batch["description"] = description
        posted = [p for p in batch["included"]
                  if self.github.post_status(batch["id"], p["sha"], "success", description)]
        summary = ("green: " + ", ".join(f"#{p['pr']}" for p in posted)
                   + f" ({len(shared)} failure(s) shared with main)")
        if self.mode != "merge":
            return {"verdict": "green", "summary": summary}
        return self.merge(batch, summary)

    def merge(self, batch: dict, summary: str) -> dict:
        self.phase(batch, "merging")
        if self.mirror.remote_base() != batch["base"]:
            self.requeue(batch["included"], front=True, charge=False)
            return {"verdict": "green-stale", "summary": summary + "; main moved, re-testing"}
        merged = []
        for index, p in enumerate(batch["included"]):
            if not self.github.merge(batch["id"], p["pr"], p["sha"]):
                rest = batch["included"][index:]
                self.requeue(rest[1:], front=True, charge=False)
                self.requeue(rest[:1], front=True, charge=True)
                return {"verdict": "partial", "summary": f"merged {merged}; "
                        f"#{p['pr']} refused, {len(rest)} re-queued"}
            merged.append(p["pr"])
        head = self.mirror.remote_base()
        same = self.mirror.git("fetch", "-q", self.cfg.fetch_url, head, check=False)
        tree = self.mirror.tree(head) if same.returncode == 0 else ""
        note = ("main's tree equals the candidate's" if tree == batch["candidate_tree"]
                else f"main's tree {tree[:8]} differs from the candidate's")
        self.store.event(batch["id"], f"merged {merged}; {note}")
        return {"verdict": "merged", "summary": f"merged {merged}; {note}"}

    def culprit(self, batch: dict, outdir: Path, new: list[str]) -> dict:
        included = batch["included"]
        files = files_of(new)
        self.phase(batch, "bisecting", f"{len(new)} new failing node(s) in {len(files)} "
                   f"file(s) over {len(included)} PR(s)")

        def fails(k: int) -> bool:
            checkout = self.mirror.worktree(f"{batch['id']}-prefix{k}", batch["base"])
            for p in included[:k]:
                if not self.mirror.merge(checkout, p["sha"], f"prefix {k}"):
                    raise RuntimeError(f"prefix {k} no longer merges")
            name = f"prefix{k}"
            result = self.checked(batch, name, self.run(
                batch, outdir, [(name, checkout, files)]))
            self.mirror.drop(checkout)
            hit = bool(result.failed & set(new))
            self.store.event(batch["id"], f"prefix {k} ({', '.join(f'#{p['pr']}' for p in included[:k])}): "
                                          f"{'fails' if hit else 'passes'}")
            return hit

        k = first_failing_prefix(len(included), fails)
        guilty = included[k - 1]
        batch["culprit"] = guilty
        description = describe(batch["id"], batch["base"], new, 0, "")
        self.phase(batch, "posting", f"culprit #{guilty['pr']}")
        self.github.post_status(batch["id"], guilty["sha"], "failure", description)
        listed = "\n".join(f"- `{n}`" for n in new)
        self.github.comment(batch["id"], guilty["pr"], f"culprit-{guilty['sha']}",
                            f"pbmergeq {batch['id']}: merged onto `{self.cfg.base_branch}` "
                            f"at {batch['base']}, this pull request makes these node IDs "
                            "fail that do not fail on main (failure-set difference, "
                            f"confirmed by an immediate re-run and by bisecting the batch "
                            f"to prefix {k} of {len(included)}):\n\n{listed}\n\n"
                            f"Reports: `{self.where(batch['id'])}`. Fix it and enqueue it again.\n")
        innocents = [p for p in included if p["pr"] != guilty["pr"]]
        self.requeue(innocents, front=True, charge=False)
        return {"verdict": "red", "summary": f"culprit #{guilty['pr']}: {len(new)} new "
                f"failing node(s); re-queued " + ", ".join(f"#{p['pr']}" for p in innocents)}

    # -- loop ---------------------------------------------------------------

    def resume(self) -> None:
        """A batch a restart interrupted runs again from the top.

        Every verdict a batch reaches is recorded in the ledger before the
        batch moves on, so re-running posts nothing twice and merges nothing
        twice; PrismaBuild attaches identical shard actions.
        """

        batch = self.store.state.get("batch")
        if batch:
            self.store.event(batch["id"], f"restart found it at phase {batch['phase']}; "
                                          "re-queued its entries at the front")
            for entry in reversed(batch.get("entries", [])):
                self.store.add(entry["pr"], entry.get("sha"), front=True)
            self.store.state["batch"] = None
            self.store.save()

    def refresh_known_failure_blocks(self) -> set[tuple[int, str]]:
        """Suppress a held head only while its full current domain is observed.

        These views belong to Mirror's existing worktree lifecycle, not a
        second source cache. Any unobserved or changed domain removes the
        negative hint and returns the queued entries to normal selection.
        """
        blocks = self.store.state.get("known_failure_blocked", {})
        queued = {(entry["pr"], entry.get("sha")) for entry in self.store.state["queue"]}
        failed_heads = set()
        base_tree = None
        changed = False
        for bid, block in list(blocks.items()):
            if not (bid.startswith("b") and bid[1:].isdigit()):
                raise ValueError(f"invalid held batch id: {bid!r}")
            checkout = self.mirror.worktrees / f"{bid}-candidate"
            reason = ""
            owned = False
            held = {(entry["pr"], entry["sha"]) for entry in block["entries"]}
            try:
                if not checkout.exists() and not checkout.is_symlink():
                    owned = True  # No data remains; retire its Mirror registration.
                    reason = "retained candidate is missing"
                elif checkout.is_symlink():
                    reason = "retained candidate is an unowned symlink"
                else:
                    common = Path(self.mirror.git(
                        "rev-parse", "--git-common-dir", cwd=checkout).stdout.strip())
                    if not common.is_absolute():
                        common = checkout / common
                    owned = common.resolve() == self.mirror.git_dir.resolve()
                    if not owned:
                        reason = "retained candidate belongs to another repository"
                if not reason and (not held or not held <= queued):
                    reason = "held entries left or were superseded in the queue"
                elif not reason:
                    for entry in block["entries"]:
                        pr = self.github.pr(entry["pr"])
                        if (eligibility(pr, self.cfg) is not None
                                or pr.get("headRefOid") != entry["sha"]):
                            reason = "held head changed or left eligibility"
                            break
                if not reason:
                    if (self.mirror.head(checkout) != block.get("candidate")
                            or self.mirror.tree(block["candidate"]) != block.get("candidate_tree")
                            or self.mirror.git("status", "--porcelain", "--untracked-files=all",
                                               cwd=checkout).stdout.strip()):
                        reason = "retained candidate is missing, changed or not owned"
                if not reason:
                    if base_tree is None:
                        base_tree = self.mirror.tree(self.mirror.fetch({}))
                    runtime = checkout_runtime(self.cfg, checkout)
                    files, _left_out = self.runner.discover(checkout)
                    identity = candidate_failure_identity(
                        self.cfg, block["candidate_tree"], base_tree, runtime, files)
                    known = self.store.known_failure(identity)
                    if (identity != block["identity"] or known is None
                            or known.get("candidate_tree") != block["candidate_tree"]):
                        reason = "qualification domain changed"
            except (OSError, KeyError, ValueError, subprocess.CalledProcessError,
                    RuntimeSelectionError) as exc:
                reason = f"qualification domain unobserved: {type(exc).__name__}"
            if reason:
                if owned:
                    self.mirror.drop(checkout)
                else:
                    self.store.event(bid, f"unowned view retained for recovery: {checkout}")
                del blocks[bid]
                changed = True
                self.store.event(bid, f"known-failure hold released: {reason}; fresh selection")
            else:
                failed_heads.update(held)
        if changed:
            self.store.save()
            self.store.write_status()
        return failed_heads

    def tick(self) -> dict | None:
        if self.store.ingest():
            self.store.event(None, f"queue: {[e['pr'] for e in self.store.state['queue']]}")
        failed_heads = self.refresh_known_failure_blocks()
        if not self.store.state["queue"] or self.store.state["not_before"] > time.time():
            return None
        blocked_prs = {entry["pr"] for block in self.store.state.get("runtime_blocked", {}).values()
                       for entry in block["entries"]}
        entries = [entry for entry in self.store.state["queue"]
                   if entry["pr"] not in blocked_prs
                   and (entry["pr"], entry.get("sha")) not in failed_heads][:self.cfg.batch_cap]
        if not entries:
            return None
        selected = {entry["pr"] for entry in entries}
        self.store.state["queue"] = [entry for entry in self.store.state["queue"]
                                     if entry["pr"] not in selected]
        self.store.save()
        return self.run_batch(entries)


# --------------------------------------------------------------------------
# Command line


def open_queue(cfg: Config, mode: str, *, once: bool = False) -> Queue:
    store = Store(cfg.state_dir)
    store.state["mode"] = mode
    store.save()
    mirror = Mirror(cfg, cfg.state_dir)
    mirror.ensure()
    github = GitHub(cfg, store, dry=mode == "dry-run")
    return Queue(cfg, store, github, mirror, Runner(cfg, store), mode, once=once)


def check_mode(cfg: Config, mode: str) -> None:
    if mode == "merge" and not (cfg.merge_enabled_file and Path(cfg.merge_enabled_file).exists()):
        raise SystemExit(f"pbmergeq: merge mode needs {cfg.merge_enabled_file or 'merge_enabled_file'}"
                         " to exist (the coordinator's go)")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("--config", required=True,
                        help="the repository's queue config (JSON)")
    sub = parser.add_subparsers(dest="command", required=True)
    enqueue = sub.add_parser("enqueue", help="queue a pull request (any process)")
    enqueue.add_argument("pr", type=int, help="pull request number")
    enqueue.add_argument("sha", nargs="?",
                         help="head SHA the caller approved; a later push is refused")
    daemon = sub.add_parser("daemon", help="run the queue until stopped")
    daemon.add_argument("--mode", choices=MODES, required=True,
                        help="dry-run posts nothing; status posts; merge also merges")
    once = sub.add_parser("once", help="test one batch of named PRs, outside the queue")
    once.add_argument("--mode", choices=MODES, required=True,
                      help="dry-run posts nothing; status posts; merge also merges")
    once.add_argument("prs", type=int, nargs="+", help="pull request numbers, in merge order")
    resume = sub.add_parser("resume-runtime", help="request fresh validation of a repaired runtime")
    resume.add_argument("batch", help="runtime-blocked batch ID from STATUS.txt")
    sub.add_parser("status", help="print the status file")
    args = parser.parse_args(argv)
    cfg = load_config(args.config)

    if args.command == "enqueue":
        store = Store(cfg.state_dir)
        store.enqueue_request(args.pr, args.sha)
        print(f"queued #{args.pr} mode={store.state.get('mode')}")
        return 0
    if args.command == "status":
        path = cfg.state_dir / "STATUS.txt"
        print(path.read_text(encoding="utf-8") if path.exists() else "no status yet")
        return 0
    if args.command == "resume-runtime":
        with single_instance(cfg.state_dir):
            store = Store(cfg.state_dir)
            blocked = store.state.get("runtime_blocked", {})
            if args.batch not in blocked:
                raise SystemExit(f"pbmergeq: {args.batch} is not runtime-blocked")
            del blocked[args.batch]
            store.save()
            store.event(args.batch, "operator requested fresh runtime validation; worker guards remain on")
            store.write_status()
        return 0
    check_mode(cfg, args.mode)
    with single_instance(cfg.state_dir):
        queue = open_queue(cfg, args.mode, once=args.command == "once")
        queue.resume()
        if args.command == "once":
            batch = queue.run_batch([{"pr": n, "sha": None, "at": now()} for n in args.prs])
            print(json.dumps({k: batch.get(k) for k in (
                "id", "verdict", "summary", "base", "candidate", "included", "dropped",
                "new", "shared", "flakes", "description", "wall_s")}, indent=1))
            if queue.github.would:
                print("would:\n  " + "\n  ".join(queue.github.would))
            return 2 if batch["verdict"] in {"runtime-blocked", "known-failure-blocked"} else 0
        queue.store.event(None, f"daemon started in {args.mode} mode, pid {os.getpid()}")
        while True:
            queue.tick()
            queue.store.write_status()
            time.sleep(cfg.poll_s)


if __name__ == "__main__":
    raise SystemExit(main())
