"""Census presentation over the existing sorted JSON owner."""

import json

from prismabuild.digest_primitives import sorted_json


def census_json_bytes(value: object) -> bytes:
    ordered = json.loads(sorted_json(value))
    return (json.dumps(ordered, indent=2) + "\n").encode("utf-8")
