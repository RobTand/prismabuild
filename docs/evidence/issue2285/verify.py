"""Recheck retained test evidence bytes without submitting or executing work."""
import hashlib
import json
from pathlib import Path


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    rows = [json.loads(line) for line in Path(__file__).with_name("actions.jsonl").read_text().splitlines()]
    for row in rows:
        key = row["action_key"]
        for kind in ("terminal", "stdout"):
            assert digest(row[f"{kind}_path"]) == row[f"{kind}_sha256"], (key, kind)
        terminal = json.loads(Path(row["terminal_path"]).read_text())
        assert terminal["action_key"] == key
        assert terminal["detail"]["returncode"] == row["expected_returncode"]
        stdout = Path(row["stdout_path"]).read_text()
        assert "pbtest-completion:" in stdout and row["summary"] in stdout
        if row["state"] == "done":
            assert digest(row["receipt_path"]) == row["receipt_file_sha256"], key
            receipt = json.loads(Path(row["receipt_path"]).read_text())
            assert receipt["action_key"] == key
            assert receipt["result"]["sha256"] == row["payload_sha256"]
            assert digest(row["payload_path"]) == row["payload_sha256"], key
            assert row["summary"].encode() in Path(row["payload_path"]).read_bytes()
            claims = []
            for line in stdout.splitlines():
                try:
                    value = json.loads(line)
                except ValueError:
                    continue
                if isinstance(value, dict) and "local_result_claim_sha256" in value:
                    claims.append(value["local_result_claim_sha256"])
            assert row["claim_sha256"] in claims, key
    print(f"Rechecked {len(rows)} terminal records and logs; "
          f"{sum(row['state'] == 'done' for row in rows)} receipt and payload hashes. "
          "Worker attestations are not checked by this script.")


if __name__ == "__main__":
    main()
