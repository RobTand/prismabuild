"""Capture repeatable floor golden bytes through the isolated production writers."""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
from pathlib import Path
import tempfile

from record_compat import ROOT, accepted, invoke
from prismabuild.record_kinds import KINDS


def capture(output: Path):
    rows = []
    proofs = []
    with tempfile.TemporaryDirectory(prefix="floor-captures-") as temporary:
        for kind in KINDS:
            if not kind.name.startswith("floor-"):
                continue
            for variant in kind.variants:
                case = kind.name + "/" + variant
                observations = [invoke(ROOT, case, Path(temporary) / f"{kind.name}-{variant}-{index}")
                                for index in range(2)]
                writes = [accepted(observation)["raw"] for observation in observations]
                assert writes[0] == writes[1], case
                raw = base64.b64decode(writes[0])
                rows.append(dict(case=case, raw=writes[0], sha256=hashlib.sha256(raw).hexdigest()))
                proofs.append(dict(case=case, observations=observations))
                print(case, rows[-1]["sha256"], flush=True)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows))
    output.with_suffix(".proofs.json").write_text(json.dumps(proofs, sort_keys=True) + "\n")
    print("captured", len(rows), "repeatable production writes", hashlib.sha256(output.read_bytes()).hexdigest())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    capture(parser.parse_args().output)
