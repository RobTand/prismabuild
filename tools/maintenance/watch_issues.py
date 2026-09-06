#!/usr/bin/env python3
"""Poll GitHub cheaply; run one non-overlapping Codex repair session as needed."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import subprocess
import time


REPOSITORY = "RobTand/prismabuild"


def issues():
    result = subprocess.run(
        ["gh", "api", "--paginate", "--slurp",
         f"repos/{REPOSITORY}/issues?state=open&per_page=100"],
        check=True, capture_output=True, text=True, timeout=60,
    )
    return {str(row["number"]): row["updated_at"]
            for page in json.loads(result.stdout) for row in page
            if "pull_request" not in row}


def save(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def run_once(checkout, state_dir, prompt_file, retry_seconds=21600):
    state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (state_dir / "lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("An issue maintenance session is already active.", flush=True)
            return 0
        path = state_dir / "state.json"
        state = json.loads(path.read_text()) if path.exists() else {}
        current = issues()  # API/auth failure must not mean an empty queue.
        now = time.time()
        previous = state.get("attempted", {})
        eligible = [number for number, updated in current.items()
                    if previous.get(number, {}).get("updated_at") != updated
                    or now - previous.get(number, {}).get("finished_at", 0) >= retry_seconds]
        state.update(checked_at=now, open_issues=sorted(current, key=int))
        save(path, state)
        if not eligible:
            print(f"Open issues: {len(current)}; no new or due work.", flush=True)
            return 0
        run_dir = state_dir / "runs" / f"{time.time_ns()}"
        run_dir.mkdir(parents=True)
        prompt = prompt_file.read_text() + "\n\nIssues due for triage: " + ", ".join(
            "#" + number for number in sorted(eligible, key=int)) + ".\n"
        (run_dir / "prompt.txt").write_text(prompt)
        state.update(started_at=now, run_dir=str(run_dir))
        save(path, state)
        print(f"Starting issue maintenance for {eligible}; logs: {run_dir}", flush=True)
        with (run_dir / "events.jsonl").open("w") as events:
            result = subprocess.run(
                ["codex", "exec", "--cd", str(checkout),
                 "--sandbox", "danger-full-access", "-c", 'approval_policy="never"',
                 "--json", "--output-last-message", str(run_dir / "summary.md"), "-"],
                input=prompt, text=True, stdout=events, stderr=subprocess.STDOUT,
            )
        # Agent exit 0 is not an issue resolution verdict: re-read GitHub.
        remaining = issues()
        finished = time.time()
        attempted = {number: record for number, record in previous.items() if number in remaining}
        for number in eligible:
            if number in remaining and result.returncode == 0:
                # Preserve updates made during the run for the next poll. A
                # failed agent invocation also remains eligible for retry.
                attempted[number] = {"updated_at": current[number], "finished_at": finished}
        state.update(attempted=attempted, finished_at=finished,
                     exit_code=result.returncode, open_issues=sorted(remaining, key=int))
        save(path, state)
        print(f"Maintenance exit {result.returncode}; open issues: {state['open_issues']}", flush=True)
        return result.returncode


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkout", type=Path, required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--prompt", type=Path, default=Path(__file__).with_name("issue_prompt.md"))
    args = parser.parse_args()
    os.umask(0o077)
    return run_once(args.checkout, args.state_dir, args.prompt)


if __name__ == "__main__":
    raise SystemExit(main())
