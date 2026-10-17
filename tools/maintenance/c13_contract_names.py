#!/usr/bin/env python3
"""C13 owner symbol migration; AST/source text only, no product imports.

--apply moves actual definitions and their qualified callers together.
Without --apply, report any remaining retired qualified references.
"""
from __future__ import annotations
import argparse
import io
import json
from pathlib import Path
import re
import subprocess
import tokenize

C13_LEAF_NAMES = {
    "_abs_norm": "absolute_output_path",
    "_nonneg_int": "nonnegative_output_integer",
    "_strict_int": "strict_output_integer",
    "_require_bound_contract": "bound_output_contract",
    "_checked_origin_ref": "checked_origin_ref",
    "_hex32": "output_nonce", "_HEX": "OUTPUT_KEY_HEX",
    "_HEX32": "NONCE_HEX_LENGTH", "_check_class_bytes": "output_class_bytes",
    "_ENDED_ATTEMPT_STATES": "ENDED_ATTEMPT_STATES",
    "_TERMINAL_CONSUMER_STATES": "TERMINAL_CONSUMER_STATES",
    "_RESOLVED_CONSUMER_STATES": "RESOLVED_CONSUMER_STATES",
    "_STALLED_CONSUMER_STATES": "STALLED_CONSUMER_STATES",
}
C13_OWNER_NAMES = {
    "_origin_verdict_unlocked": "verify_origins_unlocked",
    "_load_origin_batch": "resolve_origin_batch",
    "_origin_ref_scope": "origin_scope",
    "_declared_consumers": "declared_consumers",
    "_consumer_release": "read_consumer_release",
    "_consumer_state": "consumer_state",
    "_portable_identity_of": "portable_origin_identity",
    "_held_by_unknown_event": "unknown_holder_report",
    "_unknown_path_holders": "unknown_path_holders",
}


def c13_rename_owner_tokens(source, names):
    tokens = []
    for token in tokenize.generate_tokens(io.StringIO(source).readline):
        if token.type == tokenize.NAME and token.string in names:
            token = token._replace(string=names[token.string])
        tokens.append(token)
    return tokenize.untokenize(tokens)


def c13_contract_migrate(apply):
    paths = subprocess.check_output(["git", "ls-files", "*.py"], text=True).splitlines()
    changed = []
    for name in paths:
        path = Path(name)
        before = path.read_text()
        after = before
        if name == "src/prismabuild/output_contract.py":
            after = c13_rename_owner_tokens(after, C13_LEAF_NAMES)
        if name == "src/prismabuild/batch_ledger.py":
            after = c13_rename_owner_tokens(after, C13_OWNER_NAMES)
        for module, aliases, names in (
                ("output_contract", ("oc", "output_contract"), C13_LEAF_NAMES),
                ("batch_ledger", ("bl", "batch_ledger"), C13_OWNER_NAMES)):
            for old, new in names.items():
                for alias in aliases:
                    after = re.sub(r"\b" + alias + r"\." + re.escape(old) + r"\b",
                                   alias + "." + new, after)
                for separator in (":", "."):
                    after = after.replace("prismabuild." + module + separator + old,
                                          "prismabuild." + module + separator + new)
        if before != after:
            changed.append(name)
            if apply:
                path.write_text(after)
    if apply:
        path = Path("tools/maintenance/c13_batch_migration.json")
        manifest = json.loads(path.read_text())
        manifest["leaf_names"].update(C13_LEAF_NAMES)
        manifest["renames"].update(C13_OWNER_NAMES)
        path.write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({"applied": apply, "paths": changed}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    c13_contract_migrate(parser.parse_args().apply)
