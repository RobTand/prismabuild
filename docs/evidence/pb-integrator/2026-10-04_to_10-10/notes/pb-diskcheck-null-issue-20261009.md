## What breaks
`bin/fleet-diskcheck` reports a host as failed with a raw `TypeError` when a Netdata `disk_space` chart returns `null` values. The verdict is correct to fail closed, but the message does not say what is wrong, so an operator cannot tell a full disk from a dead collector.

## Evidence (2026-10-09)
- From 07:10:10Z to about 07:57Z, sparklina's Netdata returned `{"labels":["time","avail","used","reserved for root"],"data":[[1791529810,null,null,null]]}` for `disk_space./`, on its local API as well. The service was active, other charts kept updating, and `df` showed 111 GB free (88% used).
- `fleet-diskcheck --need-gb 2 --hosts sparky,sparklina --paths /,/mnt/shared` then returned `pass: false` with, for sparklina: `{"pass": false, "error": "TypeError(\"unsupported operand type(s) for +: 'NoneType' and 'NoneType'\")"}`.
- The cause is in `bin/fleet-diskcheck` lines 70 to 72. `dict(zip(d["labels"], d["data"][0]))` keeps keys whose value is `null`. `v.get("avail", 0)` returns `None` (the key exists), not `0`, so `avail + used + rsv` raises.
- The restart of Netdata on sparklina at 07:57:37Z restored the data. The tool passed both Sparks afterwards (08:01:58Z, CEO).
- The D44 D1 gate (production service on port 22449) calls this tool, so the unclear failure reached the native-admission gate.

## Required fix
Report a clear, named error for a chart with no data. Keep the verdict fail-closed. Do not add cached, default or synthetic values.

## Acceptance criteria
1. A `disk_space` chart sample with any `null` value for `avail`, `used` or `reserved for root` gives that host `pass: false` and an `error` that names the host, the chart and "no data". The text contains no `TypeError`. The tool exits 1.
2. A missing chart, an empty `data` list and an unreachable Netdata also give `pass: false` with a named error for that host.
3. One host with no data does not change the other host's entry. `passing_hosts` still lists the host that passed.
4. No fallback is added: no cached sample, no default, no direct `df`. Null data stays a failure.
5. A test with a stubbed `get` covers criteria 1 to 3, and fails on the current tool.

## Not in scope
The cause of the Netdata stall on sparklina (unknown; the daemon was restarted). Changing the D1 thresholds.
