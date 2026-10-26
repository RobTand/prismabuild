#!/usr/bin/env python3
"""Save resumable Hindsight recalls; source review remains a separate step."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import sys
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from prismabuild.digest_primitives import raw_sha256
from census_json import census_json_bytes as json_bytes

ENDPOINT = "http://127.0.0.1:8888/v1/default/banks/omp-history/memories/recall"
TOKEN_PATH = Path("/home/rob/.omp/agent/hindsight-api-token")
SCHEMA = "backlog-census-hindsight/v1"


def save(path, value):
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(json_bytes(value))
    temporary.replace(path)


def recall(query):
    payload = {"query": query, "types": ["world", "experience"], "budget": "mid", "max_tokens": 1500}
    token = TOKEN_PATH.read_text().strip()
    request = Request(ENDPOINT, data=json_bytes(payload), method="POST",
                      headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    with urlopen(request, timeout=60) as response:
        return json.load(response)


def collect(census, snapshot, output, request=recall):
    output = Path(output)
    identity = raw_sha256(json_bytes([census, snapshot]))
    titles = {f"{issue['repo']}#{issue['number']}": issue["title"] for issue in snapshot["issues"]}
    planned = {}
    subjects = census["candidates"] + [
        {"id": row["id"], "kind": row["action"], "tickets": [row["issue"]]}
        for row in census.get("write_list", []) if not row.get("candidate_id")]
    for candidate in subjects:
        candidate_id = candidate["id"]
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", candidate_id) or candidate_id in planned:
            raise ValueError("Candidate IDs must be unique safe filenames")
        tickets = "; ".join(f"{key}: {titles[key]}" for key in candidate["tickets"])
        query = f"Backlog census {candidate['kind']}. Check prior facts and source evidence for {tickets}."
        planned[candidate_id] = {"input_hash": identity, "query": query,
                                 "receipt": f"{candidate_id}.json", "sha256": None, "sources_reviewed": False}
    output.mkdir(parents=True, exist_ok=True)
    index_path = output / "index.json"
    if index_path.exists():
        index = json.loads(index_path.read_text())
        if index.get("_schema") != SCHEMA or index.get("_input_hash") != identity:
            raise ValueError("Saved recall inputs differ; choose a new output directory")
        if {key for key in index if not key.startswith("_")} != set(planned):
            raise ValueError("Saved candidate identities differ")
        for key, expected in planned.items():
            if any(index[key].get(field) != expected[field] for field in ("query", "receipt", "input_hash")):
                raise ValueError(f"Saved recall identity differs for {key}")
    else:
        if any(output.iterdir()):
            raise ValueError("Output directory has unbound evidence; choose a new directory")
        index = {"_schema": SCHEMA, "_input_hash": identity, **planned}
        save(index_path, index)
    for key in planned:
        record = index[key]
        path = output / record["receipt"]
        if path.exists():
            payload = path.read_bytes()
            response = json.loads(payload)
            if not isinstance(response, dict) or not isinstance(response.get("results"), list):
                raise ValueError(f"Saved recall omitted results for {key}")
            observed = raw_sha256(payload)
            if record["sha256"] is not None and record["sha256"] != observed:
                raise ValueError(f"Saved recall digest differs for {key}")
        else:
            if record["sha256"] is not None:
                raise ValueError(f"Saved recall receipt is missing for {key}")
            response = request(record["query"])
            if not isinstance(response, dict) or not isinstance(response.get("results"), list):
                raise ValueError(f"Recall omitted results for {key}")
            save(path, response)
            observed = raw_sha256(path.read_bytes())
        record["sha256"] = observed
        save(index_path, index)
    return index


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--census", required=True, type=Path)
    parser.add_argument("--snapshot", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    index = collect(json.loads(args.census.read_text()), json.loads(args.snapshot.read_text()), args.output)
    print(f"Saved {len(index) - 2} recalls to {args.output / 'index.json'}; source review remains pending")


if __name__ == "__main__":
    main()
