# Private machine records

`pbrun.py` and `pbwait.py` accept `--machine-record-fd N` for subprocess
callers. This channel carries complete continuation records independently of
public stdout. An acknowledgement identifies submitted work. It does not
prove that the work passed.

## Capability check

Run each published command with `--machine-record-capability` before using
the channel:

```sh
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbrun.py --machine-record-capability
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbwait.py --machine-record-capability
```

Each command prints exactly one JSON line and exits 0:

```json
{"schema":"prismabuild.machine_record_capability.v1","machine_record_fd":1}
```

The check performs no submission, observation, queue access, or CAS access.
It requires no command, action key, or descriptor. An older runtime rejects
the unknown option with argparse exit 2. Callers require exit 0, the exact
schema, and an integer `machine_record_fd` version of at least 1. Otherwise,
they report a typed unsupported-runtime error and refuse. They never fall
back to parsing public stdout. An incompatible channel change increments
the integer version and requires callers to check compatibility.

## Descriptor and framing

The caller supplies an initially empty, seekable regular file owned by its
current UID, opened writable with permissions exactly 0600. The descriptor
must be greater than 2 and must not alias stdout or stderr. Pass it explicitly
with `subprocess`'s `pass_fds`. The command validates it before starting work
and sets close-on-exec so execution subprocesses do not inherit it.

The command serializes one complete UTF-8 JSON document, writes it at offset
0, and writes one LF last. There are no progress messages or extra documents
on this channel. The caller reads only after the child exits, with no other
writer or reader sharing the file during execution. There is no atomicity
guarantee for reads while the child runs. The final LF marks a completed
write. An empty file, missing LF, malformed JSON, or multiple documents is a
channel failure. Consumers reject that failure without consulting stdout.

Use an unnamed temporary file to avoid path races and pipe capacity limits:

```python
import json
import subprocess
import tempfile

with tempfile.TemporaryFile(mode="w+b") as record:
	completed = subprocess.run(
		[python, pbrun, "--machine-record-fd", str(record.fileno()),
		 "--detach", "--", "/bin/true"],
		pass_fds=(record.fileno(),), capture_output=True, text=True)
	record.seek(0)
	raw = record.read()
	if not raw or not raw.endswith(b"\n"):
		raise RuntimeError("missing or incomplete private machine record")
	value = json.loads(raw)
```

Check the command's return code and validate the expected record schema and
fields before using `value`. Keep the file private if retaining it as evidence.

## Records

| Command | Private document |
| --- | --- |
| `pbrun --detach` | The exact acknowledgement currently rendered on stdout, after verification and adaptation. |
| `pbwait` | The exact ordered legacy rows array produced by `pbwait --json`. The human table stays unchanged. |
| `pbwait --json` | The same legacy rows array. |
| `pbwait --endings` | The exact `prismabuild.endings.v1` object currently rendered on stdout. |

A normal detached acknowledgement has schema `prismabuild.submission.v1`.
It retains `action_key`, `transport`, `status`, `published_unix`, `phases`,
`job_id`, `submission`, `done`, `failed`, `withdrawn`, `request_sha256`, and
`publication`. Status vocabulary remains `submitted`, `published`, `attached`,
or `cache_hit` where the existing producer emits it. Legacy or deferred
`prismaquant.prismabuild.pbrun_detach.v1` acknowledgements retain their schema
and complete fields, including `pending_id` and `release` where present.
The private channel does not reconstruct documents from client events.

The endings object retains `schema`, `complete`, and `endings` in argument-key
order. Each ending retains every existing field, including `action_key`,
`status`, `succeeded`, `exit_code`, `signal`, `host`, `attempt`, `publication`,
`started_unix`, `ended_unix`, `reason`, `result`, `diagnostics`, `phases`, and
`superseded` where present. In particular, result paths remain available to
private callers. Legacy rows retain their existing fields without a wrapper.

## Completion and errors

The producer writes the same document used for display before public
rendering. Detached acknowledgements are written after verification and
adaptation. Wait records are written after the requested observations finish.
Existing `--client-events-fd` events remain separate and unchanged.

An invalid descriptor or unsupported mode is a CLI refusal before work.
`pbrun` supports the flag only for detached submissions. `pbwait` supports it
for ordinary waits, including `--json` and `--endings`, not reconciliation.
An ordinary submission refusal before acknowledgement leaves the file empty.
Waiting, unreadable, and failed wait results still produce complete documents.
All command verdicts, return codes, and public output retain their existing
semantics, including wait exits 74, 75, and 1.

A write failure emits a diagnostic without record contents on stderr and
leaves an empty or incomplete channel. It does not change the underlying
command's verdict or return code. Callers require a valid private document
in addition to the command's existing success conditions. A killed command
may leave an incomplete document and must be treated the same way.
