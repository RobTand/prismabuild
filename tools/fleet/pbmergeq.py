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
import importlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from typing import Callable, Iterable

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
    test_python: str             # the interpreter on the target box
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
    where: str = ""              # human pointer to a batch dir, {batch} expands
    flake_labels: tuple[str, ...] = ("P2",)
    merge_enabled_file: str = ""
    tmpdir: str = ""

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
        if owned in args:
            raise SystemExit(f"pbmergeq: pbtest_args may not set {owned}; the queue owns it")
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


def load_outcomes(pbtest: Path):
    """``pbtest_outcomes`` from beside the published ``pbtest.py``."""

    where = str(pbtest.parent)
    if where not in sys.path:
        sys.path.insert(0, where)
    return importlib.import_module("pbtest_outcomes")


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
    for shard in report:
        files.extend(shard.get("files") or ())
        if shard.get("action_key"):
            actions.append(shard["action_key"])
        if shard.get("receipt_path"):
            receipts.append(shard["receipt_path"])
        record = outcomes.parse(shard.get("output") or "") if shard.get("ran") else None
        if record is None:
            inconclusive.extend(shard.get("files") or ())
            continue
        for row in record.get("reports") or ():
            entry = dict(zip(outcomes.REPORT_FIELDS, row))
            if entry["category"] in ("failed", "error"):
                failed.add(entry["nodeid"])
        reconciliation = shard.get("reconciliation") or {}
        failed.update(f"{nodeid} (never ran)" for nodeid in reconciliation.get("never_ran") or ())
        inconclusive.extend(reconciliation.get("missing_files") or ())
    return RunResult(failed=failed, inconclusive=sorted(set(inconclusive)),
                     files=files, receipts=receipts, actions=actions)


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

    def add(self, pr: int, sha: str | None, *, front: bool) -> None:
        queue = [entry for entry in self.state["queue"] if entry["pr"] != pr]
        entry = {"pr": pr, "sha": sha, "at": now()}
        self.state["queue"] = [entry, *queue] if front else [*queue, entry]

    def baseline(self, tree: str) -> dict[str, list[str]]:
        path = self.baselines / f"{tree}.json"
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}

    def remember_baseline(self, tree: str, files: Iterable[str], failed: Iterable[str]) -> None:
        known = self.baseline(tree)
        by_file: dict[str, list[str]] = {name: [] for name in files}
        for nodeid in failed:
            by_file.setdefault(node_file(nodeid), []).append(nodeid)
        known.update({name: sorted(nodes) for name, nodes in by_file.items()})
        write_atomic(self.baselines / f"{tree}.json", json.dumps(known, indent=1))

    def status_text(self) -> str:
        state = self.state
        lines = [f"pbmergeq {self.root}  updated {now()}  mode {state.get('mode')}"]
        if state.get("not_before", 0) > time.time():
            lines.append(f"backing off until {time.strftime('%H:%M:%SZ', time.gmtime(state['not_before']))}")
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
            raise SystemExit(f"pbmergeq: another instance holds {root / 'lock'}")
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

    def fetch(self, prs: Iterable[int]) -> str:
        """Fetch the base branch and each PR head; return the base SHA."""

        refs = [f"+refs/heads/{self.cfg.base_branch}:refs/mq/base"]
        refs += [f"+refs/pull/{n}/head:refs/mq/pr/{n}" for n in prs]
        self.git("fetch", "-q", "--no-tags", self.cfg.fetch_url, *refs)
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


class Runner:
    """Submits ``pbtest.py`` runs and waits for them with a heartbeat."""

    def __init__(self, cfg: Config, store: Store):
        self.cfg, self.store = cfg, store
        self.outcomes = load_outcomes(cfg.pbtest)
        self._pbtest = None

    def discover(self, checkout: Path) -> tuple[list[str], list[str]]:
        """Every test file the suite holds, and the fleet-data files left out."""

        if self._pbtest is None:
            load_outcomes(self.cfg.pbtest)          # puts pbtest's dir on sys.path
            self._pbtest = importlib.import_module("pbtest")
        files = [f for f in self._pbtest.discover(checkout, list(self.cfg.test_paths))
                 if f not in self.cfg.exclude]
        left_out: list[str] = []
        if self.cfg.skip_fleet_data_files:
            left_out = self._pbtest.fleet_data_files(checkout, files)
            files = [f for f in files if f not in left_out]
        return files, left_out

    def history(self) -> list[str]:
        latest = self.store.state.get("history_report")
        return ["--history", latest] if latest and Path(latest).is_file() else []

    def command(self, checkout: Path, files: list[str], report: Path) -> list[str]:
        return [self.cfg.client_python, str(self.cfg.pbtest),
                "--checkout", str(checkout), "--python", self.cfg.test_python,
                "--json", str(report), *self.cfg.pbtest_args, *self.history(), *files]

    def run_many(self, batch: str, outdir: Path,
                 jobs: list[tuple[str, Path, list[str]]]) -> dict[str, RunResult]:
        """Run several pbtest invocations at once; retry inconclusive files."""

        results = self._wait(batch, outdir, jobs)
        for attempt in range(1, self.cfg.inconclusive_retries + 1):
            retry = [(f"{name}.retry{attempt}", checkout, results[name].inconclusive)
                     for name, checkout, _ in jobs if results[name].inconclusive]
            if not retry:
                break
            for name, _, files in retry:
                self.store.event(batch, f"run {name}: {len(files)} inconclusive file(s) re-run")
            again = self._wait(batch, outdir, retry)
            for name, checkout, _ in jobs:
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
        return results

    def _wait(self, batch: str, outdir: Path,
              jobs: list[tuple[str, Path, list[str]]]) -> dict[str, RunResult]:
        outdir.mkdir(parents=True, exist_ok=True)
        env = dict(os.environ)
        if self.cfg.tmpdir:
            env["TMPDIR"] = self.cfg.tmpdir
        live = {}
        for name, checkout, files in jobs:
            report = outdir / f"{name}.json"
            log = outdir / f"{name}.log"
            report.unlink(missing_ok=True)
            handle = log.open("w", encoding="utf-8")
            process = subprocess.Popen(self.command(checkout, files, report), stdout=handle,
                                       stderr=subprocess.STDOUT, env=env, text=True)
            live[name] = (process, handle, report, log, len(files), time.monotonic())
            self.store.event(batch, f"run {name} started: {len(files)} file(s) on "
                                    f"{checkout.name}, pid {process.pid}, log {log}")
        last = time.monotonic()
        while any(p.poll() is None for p, *_ in live.values()):
            time.sleep(2)
            if time.monotonic() - last >= HEARTBEAT_S:
                last = time.monotonic()
                for name, (process, _, _, log, count, started) in live.items():
                    if process.poll() is None:
                        self.store.event(batch, f"run {name} waiting "
                                                f"{time.monotonic() - started:.0f}s: "
                                                f"{self._progress(log)} shard(s) reported")
                self.store.write_status()
        results = {}
        for name, (process, handle, report, log, count, started) in live.items():
            handle.close()
            wall = time.monotonic() - started
            if report.exists():
                result = reduce_report(json.loads(report.read_text(encoding="utf-8")),
                                       self.outcomes)
            else:
                files = next(f for n, _, f in jobs if n == name)
                result = RunResult(failed=set(), inconclusive=list(files), files=list(files),
                                   receipts=[])
            result.report, result.wall_s = str(report), wall
            results[name] = result
            self.store.event(batch, f"run {name} ended rc={process.returncode} in {wall:.0f}s: "
                                    f"{len(result.failed)} failing node(s), "
                                    f"{len(result.inconclusive)} inconclusive file(s)")
        return results

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

    def checked(self, batch: dict, name: str, results: dict[str, RunResult]) -> RunResult:
        result = results[name]
        batch.setdefault("runs", {})[name] = {
            "report": result.report, "wall_s": round(result.wall_s, 1),
            "files": len(result.files), "failed": sorted(result.failed),
            "inconclusive": result.inconclusive, "receipts": result.receipts,
            "actions": result.actions}
        self.store.save()
        if result.inconclusive:
            raise Inconclusive(f"{name}: {len(result.inconclusive)} file(s) never observed")
        return result

    # -- selection and build -----------------------------------------------

    def select(self, entries: list[dict]) -> tuple[list[dict], list[dict]]:
        """Eligible entries with their current heads, and the ones skipped."""

        chosen, skipped = [], []
        for entry in entries:
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
                               "pin": entry.get("sha"), "title": pr.get("title", "")})
        return chosen, skipped

    def build(self, batch: dict, prs: list[dict]) -> Path:
        base = self.mirror.fetch([p["pr"] for p in prs])
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
        except Inconclusive as exc:
            # Nobody observed some files: no verdict, nothing posted, retry later.
            verdict = {"verdict": "inconclusive", "summary": str(exc)}
            self.requeue(entries, front=True)
            state["not_before"] = time.time() + self.cfg.stuck_backoff_s
        except Exception as exc:  # noqa: BLE001 -- a daemon logs and backs off
            verdict = {"verdict": "error", "summary": f"{type(exc).__name__}: {exc}"[:300]}
            self.requeue(entries, front=True)
            state["not_before"] = time.time() + self.cfg.stuck_backoff_s
        finally:
            for path in list(self.mirror.worktrees.glob(f"{batch['id']}-*")):
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

    def requeue(self, prs: list[dict], *, front: bool) -> None:
        if self.once:
            return
        for p in reversed(prs) if front else prs:
            self.store.add(p["pr"], p.get("pin", p.get("sha")), front=front)
        self.store.save()

    def _run(self, batch: dict, outdir: Path) -> dict:
        chosen, skipped = self.select(batch["entries"])
        batch["skipped"] = skipped
        for s in skipped:
            self.store.event(batch["id"], f"skip #{s['pr']}: {s['why']}")
        if not chosen:
            return {"verdict": "empty", "summary": "nothing eligible"}
        self.phase(batch, "building")
        checkout = self.build(batch, chosen)
        for d in batch["dropped"]:
            self.store.event(batch["id"], f"drop #{d['pr']}: {d['why']}")
            if d.get("requeue"):
                self.requeue([d], front=False)
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
        self.phase(batch, "testing", f"candidate {batch['candidate'][:8]} = base "
                   f"{batch['base'][:8]} + " + ", ".join(f"#{p['pr']}" for p in included)
                   + f"; {len(files)} files" + (f", {len(left_out)} fleet-data file(s) "
                                               "left out" if left_out else ""))
        full = self.checked(batch, "candidate", self.runner.run_many(
            batch["id"], outdir, [("candidate", checkout, files)]))
        self.store.state["history_report"] = full.report
        if full.failed:
            new, shared, flakes = self.judge(batch, outdir, checkout, full)
        else:
            new, shared, flakes = [], [], []
        batch.update({"new": new, "shared": shared, "flakes": flakes})
        self.store.remember_baseline(batch["candidate_tree"], full.files,
                                     set(full.failed) - set(flakes))
        for nodeid in flakes:
            self.flake(batch, nodeid)
        if new:
            return self.culprit(batch, outdir, new)
        return self.green(batch, shared)

    def judge(self, batch, outdir, checkout, full) -> tuple[list[str], list[str], list[str]]:
        """New, shared-with-main, and flaky node IDs among the candidate's failures."""

        failing_files = files_of(full.failed)
        known = self.store.baseline(batch["base_tree"])
        missing = [f for f in failing_files if f not in known]
        jobs = [("rerun", checkout, failing_files)]
        if missing:
            base_checkout = self.mirror.worktree(f"{batch['id']}-base", batch["base"])
            jobs.append(("base", base_checkout, missing))
        self.phase(batch, "judging", f"{len(full.failed)} failing node(s) in "
                   f"{len(failing_files)} file(s); base needs {len(missing)} file(s), "
                   f"{len(failing_files) - len(missing)} known")
        results = self.runner.run_many(batch["id"], outdir, jobs)
        rerun = self.checked(batch, "rerun", results)
        if missing:
            base = self.checked(batch, "base", results)
            self.store.remember_baseline(batch["base_tree"], missing, base.failed)
            known = self.store.baseline(batch["base_tree"])
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
        summary = (f"green: " + ", ".join(f"#{p['pr']}" for p in posted)
                   + f" ({len(shared)} failure(s) shared with main)")
        if self.mode != "merge":
            return {"verdict": "green", "summary": summary}
        return self.merge(batch, summary)

    def merge(self, batch: dict, summary: str) -> dict:
        self.phase(batch, "merging")
        if self.mirror.remote_base() != batch["base"]:
            self.requeue(batch["included"], front=True)
            return {"verdict": "green-stale", "summary": summary + "; main moved, re-testing"}
        merged = []
        for index, p in enumerate(batch["included"]):
            if not self.github.merge(batch["id"], p["pr"], p["sha"]):
                rest = batch["included"][index:]
                self.requeue(rest, front=True)
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
            result = self.checked(batch, name, self.runner.run_many(
                batch["id"], outdir, [(name, checkout, files)]))
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
        self.requeue(innocents, front=True)
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

    def tick(self) -> dict | None:
        if self.store.ingest():
            self.store.event(None, f"queue: {[e['pr'] for e in self.store.state['queue']]}")
        if not self.store.state["queue"] or self.store.state["not_before"] > time.time():
            return None
        entries = self.store.state["queue"][:self.cfg.batch_cap]
        self.store.state["queue"] = self.store.state["queue"][self.cfg.batch_cap:]
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
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
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
            return 0
        queue.store.event(None, f"daemon started in {args.mode} mode, pid {os.getpid()}")
        while True:
            queue.tick()
            queue.store.write_status()
            time.sleep(cfg.poll_s)


if __name__ == "__main__":
    raise SystemExit(main())
