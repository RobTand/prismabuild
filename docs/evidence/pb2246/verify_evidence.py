#!/usr/bin/env python3
"""Read retained PrismaBuild evidence without exposing execution credentials."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess


def calls(requests):
    messages = [
        {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2024-11-05", "capabilities": {},
            "clientInfo": {"name": "PRI-19-evidence", "version": "1"}}},
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
    ]
    for i, (name, arguments) in enumerate(requests, 2):
        messages.append({"jsonrpc": "2.0", "id": i, "method": "tools/call",
                         "params": {"name": name, "arguments": arguments}})
    process = subprocess.run([
        "/usr/bin/python3", "/mnt/shared/prismabuild-fleet/repo/tools/fleet/pbmcp.py",
        "--deadline-s", "10", "--log-tail-bytes", str(2 * 1024 * 1024)],
        input="".join(json.dumps(m) + "\n" for m in messages),
        capture_output=True, text=True, timeout=20 + 15 * len(requests), check=True)
    replies = {}
    for line in process.stdout.splitlines():
        reply = json.loads(line)
        if reply.get("id", 0) < 2:
            continue
        if "error" in reply:
            raise RuntimeError(reply["error"])
        replies[reply["id"]] = json.loads(reply["result"]["content"][0]["text"])
    return [replies[i] for i in range(2, 2 + len(requests))]


def inspect_action(key, expected, expected_tree):
    action, = calls([("pb_action", {"key_prefix": key})])
    log, = calls([("pb_log", {"key_prefix": key, "tail_lines": 2 * 1024 * 1024})])
    lines = (log.get("log") or {}).get("lines", [])
    runtime = []
    claims = []
    mutants = []
    for line in lines:
        if line.startswith("prismabuild-test-runtime: "):
            runtime.append(json.loads(line.split(": ", 1)[1]))
        if line.startswith("{"):
            try:
                parsed = json.loads(line)
                claim = parsed.get("local_result_claim_sha256")
                if "mutant" in parsed:
                    mutants.append(parsed)
            except ValueError:
                continue
            if claim:
                claims.append(claim)
    mismatches = []
    source_count = 0
    for record in runtime:
        for name, digest in record.get("selected_source_sha256", {}).items():
            source_count += 1
            if name in expected and digest != expected[name]:
                mismatches.append(name)
    summary = {
        "action_key": key, "state": action.get("state"),
        "complete": action.get("complete"), "timed_out": action.get("timed_out"),
        "unavailable": action.get("unavailable"),
        "log_complete": log.get("complete"),
        "log_bytes_match": (log.get("log") or {}).get("bytes_match"),
        "runtime_exitstatus": [r.get("exitstatus") for r in runtime],
        "declared_load": [json.loads(line.split(": ", 1)[1]) for line in lines
                          if line.startswith("declared-load: ")],
        "log_truncated_head": (log.get("log") or {}).get("truncated_head"),
        "pytest_summaries": [line for line in lines if re.search(
            r"\b(?:passed|failed|error|errors|skipped)\b.*\bin [0-9.]+s", line)],
        "receipt": action.get("receipt"),
        "source_hashes_observed": source_count,
        "source_mismatches_vs_candidate": sorted(set(mismatches)),
        "mutants": mutants,
        "snapshot": action.get("sealed", {}).get("checkout_snapshot"),
        "demand": action.get("sealed", {}).get("resources"),
        "attempts": [],
    }
    for attempt in action.get("attempts_detail", []):
        detail = attempt.get("detail", {})
        profile = detail.get("resource_profile", {})
        summary["attempts"].append({
            "host": attempt.get("claimed_host"), "status": attempt.get("status"),
            "returncode": detail.get("returncode"),
            "action_returncode": detail.get("action_returncode"),
            "elapsed_s": detail.get("elapsed_s"),
            "cpu_load": profile.get("box_window", {}).get("cpu"),
            "scope": profile.get("scope"), "logs": attempt.get("logs"),
        })
    summary["result_claim_checks"] = calls([
        ("pb_verify_claim", {"sha256": claim, "hash_payload": True}) for claim in claims
    ]) if claims else []
    if summary["receipt"] and summary["receipt"].get("present"):
        verify = subprocess.run(["/usr/bin/python3", "-c", """
import json, re, subprocess, sys, tempfile
sys.path.insert(0, '/mnt/shared/prismabuild-fleet/repo/src')
from prismabuild.core import PrismaBuildCAS
cas = PrismaBuildCAS('/mnt/shared/prismabuild-fleet/cas')
receipt = cas.lookup(cas.read_action_request(sys.argv[1]))
if receipt is None:
    raise RuntimeError('receipt disappeared')
snapshot = json.loads(sys.argv[2])
bundle = cas.input_path(snapshot['input'])
with tempfile.TemporaryDirectory(prefix='pri19-source-proof-') as directory:
    git = ['/usr/bin/git', '-C', directory]
    subprocess.run(git + ['init', '--bare', '-q'], check=True)
    subprocess.run(git + ['fetch', '-q', str(bundle), snapshot['commit']], check=True)
    source_tree = subprocess.check_output(
        git + ['rev-parse', snapshot['parent'] + '^{tree}'], text=True).strip()
    if source_tree != sys.argv[3]:
        raise RuntimeError('sealed checkout parent differs from candidate tree')
    changes = subprocess.check_output(git + [
        'diff', '--name-status', snapshot['parent'], snapshot['commit']], text=True).splitlines()
    if any(not re.fullmatch(r'A\t[.]pbrun-closure[.][0-9a-f]{16}[.]json', line)
           for line in changes):
        raise RuntimeError('sealed checkout changes candidate source: ' + repr(changes))
print(json.dumps({'action_key': receipt['action_key'],
                  'receipt_sha256': receipt['receipt_sha256'],
                  'result': receipt['result'],
                  'source_snapshot': {'bundle_sha256': snapshot['input']['sha256'],
                                      'candidate_tree': source_tree,
                                      'snapshot_changes': changes,
                                      'verified': True}}))
""", key, json.dumps(summary['snapshot']), expected_tree],
            capture_output=True, text=True, timeout=60, check=True)
        summary["cas_content_verified"] = json.loads(verify.stdout)
    return summary


def require_passing(result, expected_passes):
    assert result["state"] == "done", result
    assert result["complete"] is True and not result["timed_out"], result
    assert not result["unavailable"], result
    assert result["log_complete"] is True and result["log_bytes_match"] is True, result
    assert not result["log_truncated_head"], result
    assert result.get("cas_content_verified", {}).get(
        "source_snapshot", {}).get("verified") is True, result
    assert not result["source_mismatches_vs_candidate"], result
    assert result["runtime_exitstatus"] and all(
        status == 0 for status in result["runtime_exitstatus"]), result
    assert any(re.search(rf"\b{expected_passes} passed\b", line)
               for line in result["pytest_summaries"]), result
    assert all(not re.search(r"\b(?:failed|skipped|error|errors|xfailed|xpassed)\b", line)
               for line in result["pytest_summaries"]), result
    assert result["result_claim_checks"] and all(
        check.get("complete") is True and not check.get("timed_out")
        and not check.get("unavailable") and check.get("checks_passed") is True
        for check in result["result_claim_checks"]), result
    assert result.get("cas_content_verified", {}).get("action_key") == result["action_key"], result
    assert result["attempts"] and result["attempts"][-1]["returncode"] == 0, result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--checkout", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expected-passes", type=int, required=True)
    parser.add_argument("keys", nargs="+")
    args = parser.parse_args()
    expected = {str(path.relative_to(args.checkout)): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in (args.checkout / "tests").glob("*.py")}
    expected_tree = subprocess.check_output([
        '/usr/bin/git', '-C', str(args.checkout), 'rev-parse', 'HEAD^{tree}'], text=True).strip()
    results = [inspect_action(key, expected, expected_tree) for key in args.keys]
    args.output.write_text(json.dumps(results, indent=2) + "\n")
    for result in results:
        require_passing(result, args.expected_passes)
        print(json.dumps({name: result[name] for name in [
            "action_key", "state", "complete", "pytest_summaries",
            "source_mismatches_vs_candidate"]}))


if __name__ == "__main__":
    main()
