"""What the box was doing for the seconds one action ran.

An in-process counter says where an action spent itself; it cannot say whether
the machine around it was loaded, and no in-process tool can. This module reads
that second view from the recorders the boxes already run, summarises it over
one action's window, and names the source of every field it reports.

Two recorders, because neither covers the whole box:

*   ``pqteld`` is a 2 Hz flight recorder on the GB10 boxes. Its CSV carries GPU
    power and utilisation, the unified memory pool, and the memory and I/O
    pressure stalls -- and no CPU busy figure and no CPU pressure at all.
*   Netdata runs on every box, including ``dl380g10``, which has no ``pqteld``.
    ``system.cpu`` and ``system.cpu_some_pressure`` are where the CPU fields
    come from, on every host rather than only the one without a recorder, so a
    field means the same thing wherever it is read.

So the summary is grouped by what produced it and each group names its own
source. A field the recorders do not record is absent; a cell they left empty
was not measured and is skipped rather than counted as a zero. When no recorder
answers at all, the window is ``unavailable`` with the reason, because a
receipt that says nothing was measured is worth more than one that implies an
idle box.

Reading uses a cooperative deadline and never raises into a caller. Expiry
preserves collected samples with a diagnostic, or produces ``unavailable``
when nothing was measured. It cannot interrupt a filesystem read in progress.
"""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
import re
import time
import urllib.error
import urllib.request

BOX_WINDOW_SCHEMA_V1 = "prismabuild.box_window.v1"

#: Where ``pqteld`` writes, matching the ``--csv-dir`` its user unit passes.
DEFAULT_CSV_DIR = Path.home() / "pqtel" / "csv"
DEFAULT_NETDATA_URL = "http://127.0.0.1:19999"
#: The whole read, both recorders together.  Two seconds is the budget the
#: finish path can afford; what does not fit is reported as not measured.
DEFAULT_DEADLINE_S = 2.0
#: Target chart rows before transfer (Netdata rounds to whole time buckets).
#: The byte cap still protects against unexpectedly large server responses.
NETDATA_POINTS = 4096
#: Read recent recorder rows first without loading the daily file into memory.
CSV_READ_BLOCK_BYTES = 64 * 1024
#: A window wider than this is not one action's window, and walking a month of
#: daily CSVs to summarise it would cost more than the deadline allows.
MAX_WINDOW_DAYS = 3
KIB = 1024
_ROOT = Path(__file__).resolve().parents[2]
_FLEET_CONFIG_PATHS = (_ROOT / "tools" / "fleet_boxes.json",
                       _ROOT / "tools" / "fleet" / "fleet_boxes.json")

#: pqteld column -> the name this module reports it under.  Every output is
#: named after the column that produced it: ``psi_mem_full_avg10`` stays
#: ``full_avg10`` rather than being rounded off to "PSI", because a reader
#: comparing it with ``/proc/pressure`` needs to know which of the two numbers
#: there it is.
_GPU_COLUMNS = ("power_draw_w", "gpu_util", "uvm_residual_kb", "temp_gpu_c")
_MEMORY_COLUMNS = ("MemTotal", "MemAvailable", "psi_mem_full_avg10",
                   "psi_io_full_avg10")


class _Series:
    """Count, sum, min and max of the cells that were actually measured."""

    __slots__ = ("count", "total", "low", "high", "last")

    def __init__(self) -> None:
        self.count = 0
        self.total = 0.0
        self.low: float | None = None
        self.high: float | None = None
        self.last: float | None = None

    def add(self, value: float) -> None:
        self.count += 1
        self.total += value
        self.low = value if self.low is None else min(self.low, value)
        self.high = value if self.high is None else max(self.high, value)
        self.last = value

    @property
    def mean(self) -> float | None:
        return self.total / self.count if self.count else None


def _number(cell: str) -> float | None:
    """A measured value, or ``None`` for a cell the recorder left empty."""

    if not cell:
        return None
    try:
        value = float(cell)
    except ValueError:
        return None
    return value if math.isfinite(value) else None


def _days(start_unix: float, end_unix: float) -> list[str]:
    """The local days the window touches, in the recorder's own file naming."""

    last = time.strftime("%Y%m%d", time.localtime(end_unix))
    days: list[str] = []
    moment = start_unix
    while len(days) <= MAX_WINDOW_DAYS:
        stamp = time.strftime("%Y%m%d", time.localtime(moment))
        if stamp not in days:
            days.append(stamp)
        if stamp == last:
            break
        moment += 86400
    return days


def _recorder_hosts(host: str) -> list[str]:
    """Only names explicitly declared to be this machine in this generation.

    The recorder captures its hostname at startup, so an OS rename can leave
    it appending under the former name. Reuse the supervisor's fleet `_alias`
    declaration; unrelated CSVs in the directory are never identity evidence.
    """

    names = {host}
    for path in _FLEET_CONFIG_PATHS:
        try:
            config = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        boxes = config.get("boxes") if isinstance(config, dict) else None
        if not isinstance(boxes, dict):
            continue
        groups = []
        for name, shape in boxes.items():
            group = {name}
            if isinstance(shape, dict) and shape.get("_alias") is not None:
                alias = shape["_alias"]
                if not isinstance(alias, str):
                    if name == host:
                        raise ValueError(f"invalid recorder hostname alias for {host}")
                    continue
                group.add(alias)
            groups.append(group)
        matches = [group for group in groups if host in group]
        if matches:
            names = matches[0]
            if sum(bool(names & group) for group in groups) != 1:
                raise ValueError(f"ambiguous fleet hostname alias {host}")
        break
    if any(not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", name) for name in names):
        raise ValueError("invalid recorder hostname")
    return sorted(names)


def _csv_files(csv_dir: Path, host: str, start_unix: float,
               end_unix: float) -> list[Path]:
    """Every recorder file that can hold a row in the window.

    The recorder rotates daily and puts its schema version in the name, so a
    window that crosses midnight or a schema bump spans more than one file.
    Explicit fleet aliases cover a recorder that predates a hostname change.
    Paths are returned in name order within each day, without assuming that
    wall-clock timestamps are ordered across files or rows.
    """

    found: list[Path] = []
    hosts = _recorder_hosts(host)
    for day in _days(start_unix, end_unix):
        found.extend(sorted(path for name in hosts
                            for path in csv_dir.glob(f"pqteld-{name}-{day}.s*.csv")))
    return found


def _reverse_csv_rows(handle, start: int, expired):
    """Rows after the header, newest append first, from a fixed EOF.

    Timestamps need not be ordered. Exhausting the budget should lose old
    day rows before the recent action's rows, but an out-of-window timestamp
    is never a reason to stop. Memory is one block plus an unfinished row.
    """
    if expired():
        return
    position = handle.seek(0, os.SEEK_END)
    pending: list[bytes] = []
    while position > start:
        if expired():
            return
        size = min(CSV_READ_BLOCK_BYTES, position - start)
        position -= size
        handle.seek(position)
        if expired():
            return
        chunk = handle.read(size)
        if len(chunk) != size:
            raise OSError('recorder file changed during reverse read')
        parts = chunk.split(b'\n')
        ready: list[bytes] = []
        if len(parts) > 1:
            ready.append(parts[-1] + b''.join(reversed(pending)))
            ready.extend(reversed(parts[1:-1]))
            pending = [parts[0]]
        else:
            pending.append(parts[0])
        if position == start:
            ready.append(b''.join(reversed(pending)))
            pending = []
        first = True
        for row in ready:
            if not row:
                continue
            # As with a forward readline, retain the first row from a read
            # that spent the budget; do not start processing another one.
            if not first and expired():
                return
            first = False
            yield row


def _pqteld_series(csv_dir: Path, host: str, start_unix: float, end_unix: float,
                   *, expires: float) -> tuple[dict[str, _Series], list[str]]:
    """Accumulate the window's rows, one series per column, or say why not."""

    wanted = (*_GPU_COLUMNS, *_MEMORY_COLUMNS)
    series = {name: _Series() for name in wanted}
    errors: list[str] = []

    def expired() -> bool:
        if time.monotonic() >= expires:
            errors.append("deadline reached while reading pqteld")
            return True
        return False

    first_ms, last_ms = start_unix * 1000.0, end_unix * 1000.0
    if expired():
        return series, errors
    files = _csv_files(Path(csv_dir), host, start_unix, end_unix)
    if not files:
        return {}, [f"no pqteld CSV for {host} covering the window"]
    for path in reversed(files):
        if expired():
            return series, errors
        try:
            with path.open("rb") as handle:
                if expired():
                    return series, errors
                header = handle.readline().decode('utf-8', errors='replace').rstrip("\r\n").split(",")
                index = {name: header.index(name) for name in wanted
                         if name in header}
                if "epoch_ms" not in header:
                    errors.append(f"{path.name} has no epoch_ms column")
                    continue
                stamp = header.index("epoch_ms")
                width = len(header)
                for line in _reverse_csv_rows(handle, handle.tell(), expired):
                    cells = line.decode('utf-8', errors='replace').rstrip("\r\n").split(",")
                    if len(cells) != width:
                        continue  # a torn final row, not a schema change
                    when = _number(cells[stamp])
                    if when is None or not first_ms <= when <= last_ms:
                        continue
                    for name, column in index.items():
                        value = _number(cells[column])
                        if value is not None:
                            last = series[name].last
                            series[name].add(value)
                            if last is not None:
                                # First encountered is last in append/file
                                # order, even after a wall-clock correction.
                                series[name].last = last
        except OSError as exc:
            errors.append(f"{path.name}: {type(exc).__name__}")
    return series, errors


def _pqteld_groups(series, reference) -> dict[str, dict]:
    """The GPU and memory groups pqteld can answer for, and only those."""

    groups: dict[str, dict] = {}
    power, util = series.get("power_draw_w"), series.get("gpu_util")
    residual, temperature = series.get("uvm_residual_kb"), series.get("temp_gpu_c")
    if power is not None and power.count:
        gpu: dict[str, object] = {
            "source": "pqteld", "samples": power.count,
            "power_w_mean": power.mean, "power_w_peak": power.high,
        }
        if isinstance(reference, dict):
            envelope = reference.get("power_reference_w")
            gpu["power_reference_w"] = envelope
            gpu["power_reference_scope"] = reference.get("power_reference_scope")
            gpu["power_reference_source"] = reference.get("power_reference_source")
            if isinstance(envelope, (int, float)) and envelope > 0:
                gpu["power_peak_fraction_of_reference"] = power.high / envelope
        if util is not None and util.count:
            gpu["utilization_percent_mean"] = util.mean
            gpu["utilization_percent_peak"] = util.high
            gpu["utilization_samples"] = util.count
        if residual is not None and residual.count:
            gpu["uvm_residual_bytes_peak"] = int(residual.high * KIB)
            gpu["uvm_residual_samples"] = residual.count
        if temperature is not None and temperature.count:
            gpu["temperature_c_peak"] = temperature.high
        groups["gpu"] = gpu

    total, available = series.get("MemTotal"), series.get("MemAvailable")
    stall_mem, stall_io = (series.get("psi_mem_full_avg10"),
                           series.get("psi_io_full_avg10"))
    memory: dict[str, object] = {}
    if available is not None and available.count:
        memory["samples"] = available.count
        memory["unified_available_bytes_min"] = int(available.low * KIB)
        if total is not None and total.count:
            memory["unified_total_bytes"] = int(total.last * KIB)
            # On a GB10 the GPU and the host share this pool, so the peak the
            # action ran against is the peak of the whole box, not of a
            # framebuffer. Named for the pool rather than for either consumer.
            memory["unified_used_bytes_peak"] = int(
                (total.last - available.low) * KIB)
    if stall_mem is not None and stall_mem.count:
        memory["psi_mem_full_avg10_max"] = stall_mem.high
    if stall_io is not None and stall_io.count:
        memory["psi_io_full_avg10_max"] = stall_io.high
    if memory:
        memory["source"] = "pqteld"
        groups["memory"] = memory
    return groups


def _netdata_chart(url: str, chart: str, after: int, before: int,
                   timeout_s: float):
    """One bounded chart read, or ``None`` when Netdata does not answer."""

    query = (f"{url.rstrip('/')}/api/v1/data?chart={chart}"
             f"&after={after}&before={before}&format=json"
             f"&points={NETDATA_POINTS}&group=average&options=jsonwrap")
    try:
        with urllib.request.urlopen(query, timeout=timeout_s) as response:
            if response.status != 200:
                return None
            payload = json.loads(response.read(4 * 1024 * 1024).decode("utf-8"))
    except (urllib.error.URLError, OSError, ValueError, TimeoutError):
        return None
    if not isinstance(payload, dict) or not isinstance(payload.get("result"), dict):
        return None
    # Plain JSON has only labels/data. The wrapper supplies the returned view's
    # interval; update_every alone is the database's raw collection interval.
    return {**payload["result"], "view_update_every": payload.get("view_update_every")}


def _netdata_group(url: str, start_unix: float, end_unix: float,
                   *, expires: float, chart_reader) -> tuple[dict | None, list[str]]:
    """CPU busy and CPU pressure, which no pqteld column carries."""

    after, before = int(start_unix), int(math.ceil(end_unix))
    remaining = expires - time.monotonic()
    if remaining <= 0:
        return None, ["deadline reached before netdata system.cpu"]
    busy = chart_reader(url, "system.cpu", after, before, min(remaining, 1.0))
    if not isinstance(busy, dict) or not isinstance(busy.get("data"), list):
        return None, ["netdata system.cpu unavailable"]
    labels = busy.get("labels") or []
    # Netdata's ``system.cpu`` publishes every state except idle, so their sum
    # is the busy percentage. Reading it that way rather than as ``100 - idle``
    # keeps the figure defined on a chart that has no idle dimension.
    dimensions = [i for i, name in enumerate(labels) if i and name != "time"]
    series = _Series()
    for row in busy["data"]:
        if not isinstance(row, list) or len(row) != len(labels):
            continue
        values = [row[i] for i in dimensions]
        if any(not isinstance(v, (int, float)) or isinstance(v, bool)
               for v in values):
            continue  # a collection gap, which is not a busy figure of zero
        series.add(float(sum(values)))
    if not series.count:
        return None, ["netdata system.cpu returned no measured rows"]
    group: dict[str, object] = {
        "source": "netdata", "samples": series.count,
        "busy_percent_mean": series.mean, "busy_percent_peak": series.high,
        # Maxima describe returned average buckets, not raw-sample peaks.
        "time_group": "average",
    }
    interval = busy.get("view_update_every")
    if (isinstance(interval, (int, float)) and not isinstance(interval, bool)
            and math.isfinite(interval) and interval > 0):
        group["update_every_s"] = interval
    errors: list[str] = []
    for chart, field in (("system.cpu_some_pressure", "psi_some_avg10_max"),
                         ("system.cpu_full_pressure", "psi_full_avg10_max")):
        remaining = expires - time.monotonic()
        if remaining <= 0:
            errors.append(f"deadline reached before {chart}")
            break
        payload = chart_reader(url, chart, after, before, min(remaining, 1.0))
        if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
            continue
        names = payload.get("labels") or []
        try:
            column = list(names).index("some 10" if "some" in chart else "full 10")
        except ValueError:
            continue
        stalls = _Series()
        for row in payload["data"]:
            if (isinstance(row, list) and len(row) == len(names)
                    and isinstance(row[column], (int, float))
                    and not isinstance(row[column], bool)):
                stalls.add(float(row[column]))
        if stalls.count:
            group[field] = stalls.high
            group[field.replace("_max", "_samples")] = stalls.count
            interval = payload.get("view_update_every")
            if (isinstance(interval, (int, float)) and not isinstance(interval, bool)
                    and math.isfinite(interval) and interval > 0):
                group[field.replace("_max", "_update_every_s")] = interval
    return group, errors


def read_window(start_unix: float, end_unix: float, *, host: str,
                csv_dir=None, netdata_url: str | None = DEFAULT_NETDATA_URL,
                gpu_reference=None,
                deadline_s: float = DEFAULT_DEADLINE_S,
                chart_reader=None) -> dict[str, object]:
    """Summarise the box over ``[start_unix, end_unix]`` on ``host``.

    Never raises: an unreadable recorder, an unreachable Netdata and a window
    nobody has rows for all produce ``{"source": "unavailable", "reason": ...}``.
    The caller is a finish path, and a finish path that can fail on telemetry
    is a finish path that loses actions to its own instrumentation.

    ``gpu_reference`` accepts a keyword-only ``timeout_s`` for the remaining
    query budget; it is not called once the window's deadline has elapsed.
    """

    expires = time.monotonic() + max(0.05, float(deadline_s))
    window: dict[str, object] = {
        "schema": BOX_WINDOW_SCHEMA_V1, "host": host,
        "start_unix": start_unix, "end_unix": end_unix,
    }
    errors: list[str] = []
    if not (math.isfinite(start_unix) and math.isfinite(end_unix)
            and end_unix >= start_unix):
        return {**window, "source": "unavailable",
                "reason": "the action's window is not an interval"}

    groups: dict[str, dict] = {}
    try:
        series, csv_errors = _pqteld_series(
            Path(csv_dir) if csv_dir is not None else DEFAULT_CSV_DIR,
            host, start_unix, end_unix, expires=expires)
        errors.extend(csv_errors)
        if series:
            reference = None
            measured_power = series.get("power_draw_w")
            if (gpu_reference is not None and measured_power is not None
                    and measured_power.count):
                remaining = expires - time.monotonic()
                if remaining <= 0:
                    errors.append("deadline reached before GPU power reference")
                else:
                    try:
                        reference = gpu_reference(timeout_s=min(remaining, 1.0))
                    except Exception as exc:                   # noqa: BLE001
                        errors.append(f"GPU power reference unavailable: {exc}")
            produced = _pqteld_groups(series, reference)
            if not produced:
                # The files were there and nothing in them fell in the window.
                # Saying so is the difference between a box with no recorder
                # and an action shorter than the recorder's 2 Hz period.
                errors.append("pqteld recorded no rows inside the window")
            groups.update(produced)
    except Exception as exc:                                   # noqa: BLE001
        errors.append(f"pqteld unreadable: {type(exc).__name__}: {exc}")

    if netdata_url and time.monotonic() < expires:
        try:
            cpu, netdata_errors = _netdata_group(
                netdata_url, start_unix, end_unix, expires=expires,
                chart_reader=chart_reader or _netdata_chart)
            errors.extend(netdata_errors)
            if cpu is not None:
                groups["cpu"] = cpu
        except Exception as exc:                               # noqa: BLE001
            errors.append(f"netdata unreadable: {type(exc).__name__}: {exc}")
    elif netdata_url:
        errors.append("deadline reached before netdata")

    if not groups:
        return {**window, "source": "unavailable",
                "reason": "; ".join(errors) or "no recorder answered"}
    sources = sorted({str(group["source"]) for group in groups.values()})
    window["source"] = "+".join(sources)
    window.update(groups)
    if errors:
        window["errors"] = errors
    return window


def gpu_power_reference(*, timeout_s: float = 1.0) -> dict[str, object] | None:
    """The device's own power reference, so no envelope is ever hardcoded.

    ``gpu_capacity`` already reads it from the driver and already records that
    a GB10 has no programmable limit, so its published SoC TDP is a reference
    and not a measured GPU saturation point. That distinction has to travel
    with the fraction, or the fraction reads as a claim it is not.
    """

    from . import gpu_capacity

    found, _ = gpu_capacity.devices(timeout_s=timeout_s)
    best = None
    for device in found:
        reference = device.get("power_reference_w")
        if isinstance(reference, (int, float)) and reference > 0:
            if best is None or reference > best["power_reference_w"]:
                best = {
                    "power_reference_w": float(reference),
                    "power_reference_scope": device.get("power_reference_scope"),
                    "power_reference_source": device.get("power_reference_source"),
                }
    return best


def default_csv_dir() -> Path:
    """Where to look for the recorder, with an override for a moved store."""

    return Path(os.environ.get("PRISMABUILD_PQTELD_CSV_DIR")
                or DEFAULT_CSV_DIR)
