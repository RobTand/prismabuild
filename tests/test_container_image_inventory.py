"""Container-image references, and the bounded local inventory behind them.

The inventory is the evidence a claim refuses on (#714), so the two things
that matter are that a short or failed read is *unknown* rather than empty,
and that the two reference forms stay distinct: a ``sha256:`` image ID and a
``repository@sha256:`` manifest digest are different identities even when
their hex collides.  The cache hardening is the review's other half: a
corrupt, future-dated, foreign-owned or symlinked record must never become a
positive admission.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import container_images as ci  # noqa: E402

ID_A = "sha256:" + "a" * 64
ID_B = "sha256:" + "b" * 64
REF_A = "ghcr.io/example/stage-a@sha256:" + "a" * 64


class _Probe:
    """A fake bounded probe: bytes on success, ``None`` on any failure."""

    def __init__(self, data=b"", absent=False):
        self.data = data
        self.absent = absent
        self.calls = []

    def __call__(self, argv, **kwargs):
        self.calls.append((argv, kwargs))
        return None if self.absent else self.data


def _line(*refs) -> bytes:
    rows = []
    for ref in refs:
        if ref.startswith("sha256:"):
            rows.append(f"{ref}\t<none>\t<none>")
        else:
            repository, _, digest = ref.partition("@")
            rows.append(f"{ID_A}\t{repository}\t{digest}")
    return ("\n".join(rows) + "\n").encode()


def _cache(tmp_path, *, probe=None, ttl_s=30.0, clock=None):
    return ci.InventoryCache(
        root=tmp_path / "inventory", ttl_s=ttl_s,
        probe=probe if probe is not None else _Probe(),
        clock=clock or (lambda: 1000.0),
    )


def _write_record(cache, *, schema=ci.INVENTORY_SCHEMA, observed=1000.0,
                  entries=None, raw=None):
    cache.root.mkdir(parents=True, exist_ok=True)
    os.chmod(cache.root, 0o700)
    payload = raw if raw is not None else json.dumps({
        "schema": schema, "observed_unix": observed,
        "entries": entries if entries is not None else [ID_A],
    }).encode()
    (cache.root / cache.record_name).write_bytes(payload)


# --------------------------------------------------------------------------
# Declarations
# --------------------------------------------------------------------------

def test_both_immutable_reference_forms_are_accepted_and_normalized():
    # Sorted strings, so a repository-qualified digest precedes the IDs.
    assert ci.normalize_refs([REF_A, ID_A, ID_A, ID_B]) == (REF_A, ID_A, ID_B)
    assert ci.validate_ref(ID_A) == ID_A
    assert ci.validate_ref("library/ubuntu@sha256:" + "0" * 64)


@pytest.mark.parametrize("bad", [
    "stage-a:replicate",               # mutable tag
    "ghcr.io/example/stage-a:latest",  # mutable tag with a registry
    "sha256:" + "A" * 64,              # uppercase hex is not a digest
    "sha256:" + "a" * 63,              # short
    "sha256:" + "a" * 65,              # long
    "",
    None,
])
def test_a_mutable_or_malformed_reference_is_refused(bad):
    with pytest.raises(ValueError):
        ci.validate_ref(bad)
    with pytest.raises(ValueError):
        ci.normalize_refs([bad])


def test_a_bare_id_and_a_repo_digest_with_the_same_hex_are_not_aliases():
    assert ci.missing([ID_A], [REF_A]) == (ID_A,)
    assert ci.missing([REF_A], [ID_A]) == (REF_A,)
    assert ci.missing([ID_A, REF_A], [ID_A, REF_A]) == ()


def test_a_snapshot_declares_what_the_box_must_check_for():
    snapshot = [
        {"container_images": [REF_A, ID_B]},
        {"container_images": [ID_B, ID_A]},
        {"tags": ["gb10"]},
        {"container_images": "not-a-list"},
    ]
    assert ci.required_from_items(snapshot) == (REF_A, ID_A, ID_B)


# --------------------------------------------------------------------------
# The listing
# --------------------------------------------------------------------------

def test_the_listing_yields_bare_ids_and_exact_repo_digests():
    entries = ci.parse_inventory(
        f"{ID_A}\tghcr.io/example/stage-a\t{ID_A}\n"
        f"{ID_A}\texample/stage-a\t<none>\n"
        f"{ID_B}\t<none>\t<none>\n")
    assert entries == frozenset({ID_A, ID_B, REF_A})
    assert ci.missing([REF_A], entries) == ()
    assert ci.missing([ID_A], entries) == ()


@pytest.mark.parametrize("text", [
    "not-an-id\trepo\tsha256:" + "a" * 64,       # ID column is foreign
    ID_A + "\trepo",                             # row shape changed
    ID_A + "\trepo\tsha256:" + "A" * 64,         # foreign digest form
])
def test_a_listing_shape_this_does_not_understand_is_unknown(text):
    with pytest.raises(ValueError):
        ci.parse_inventory(text)


def test_observe_pins_the_local_daemon_and_scrubs_remote_endpoints(monkeypatch):
    monkeypatch.setenv("DOCKER_HOST", "tcp://remote.invalid:2375")
    monkeypatch.setenv("DOCKER_CONTEXT", "remote")
    probe = _Probe(_line(REF_A))
    assert ci.observe(probe=probe) == frozenset({ID_A, REF_A})
    argv, kwargs = probe.calls[0]
    assert argv[1:3] == ["--host", ci.LOCAL_ENDPOINT]
    assert "tcp://remote.invalid:2375" not in argv
    assert "DOCKER_HOST" not in kwargs["env"]
    assert "DOCKER_CONTEXT" not in kwargs["env"]
    assert kwargs["limit"] == ci.MAX_INVENTORY_BYTES


@pytest.mark.parametrize("probe", [
    _Probe(absent=True),
    _Probe(b"garbage\n"),
    _Probe(b"\xff\xfe"),                       # not UTF-8
    lambda argv, **kwargs: "not bytes",
])
def test_any_failed_read_is_unknown_never_empty(probe):
    assert ci.observe(probe=probe) is None


def test_a_real_probe_read_is_capped_while_reading_not_after():
    # The cap is enforced on the stream: an answer that runs past it is
    # refused without first being held in memory.
    overflowing = ["/usr/bin/python3", "-c",
                   "print('x' * 200000)"]
    result = ci._run_bounded(
        overflowing, env=dict(os.environ), timeout_s=10.0, limit=4096)
    assert result is None


def test_a_real_probe_read_returns_output_under_the_cap():
    argv = ["/usr/bin/python3", "-c", "import sys; sys.stdout.write('ok')"]
    assert ci._run_bounded(
        argv, env=dict(os.environ), timeout_s=10.0, limit=4096) == b"ok"


@pytest.mark.skipif(not Path("/var/run/docker.sock").exists(),
                    reason="this worker has no local Docker daemon")
def test_a_real_daemon_listing_parses_or_is_unknown():
    # Against the real listing shape: a box with a daemon answers a set of
    # well-formed references, and a box without one answers unknown.  Never a
    # malformed partial set.
    observed = ci.observe(timeout_s=20.0)
    if observed is None:
        return
    for entry in observed:
        assert entry.startswith("sha256:") or "@sha256:" in entry, entry


def test_a_real_probe_that_overruns_its_deadline_is_killed():
    argv = ["/usr/bin/python3", "-c", "import time; time.sleep(30)"]
    started = time.monotonic()
    assert ci._run_bounded(
        argv, env=dict(os.environ), timeout_s=0.5, limit=4096) is None
    assert time.monotonic() - started < 15.0


# --------------------------------------------------------------------------
# The cache
# --------------------------------------------------------------------------

def test_one_probe_answers_every_loop_on_the_box_for_a_ttl(tmp_path):
    probe = _Probe(_line(REF_A))
    clock = [1000.0]
    cache = _cache(tmp_path, probe=probe, clock=lambda: clock[0])
    # A second loop sharing the directory reads the record; its own probe
    # deliberately fails the test if it is called.
    other_loop = _cache(
        tmp_path, clock=lambda: clock[0],
        probe=lambda *args, **kwargs: pytest.fail(
            "a fresh shared record must not be probed again"))
    assert cache.get() == frozenset({ID_A, REF_A})
    assert other_loop.get() == frozenset({ID_A, REF_A})
    assert len(probe.calls) == 1
    clock[0] += 1.0
    assert cache.get() == frozenset({ID_A, REF_A})
    assert len(probe.calls) == 1


def test_an_installed_image_appears_after_the_ttl_and_a_removed_one_goes_away(tmp_path):
    probe = _Probe(_line(REF_A))
    clock = [1000.0]
    cache = _cache(tmp_path, probe=probe, clock=lambda: clock[0])
    assert ci.missing([REF_A], cache.get() or ()) == ()
    probe.data = _line(ID_B)
    clock[0] += 31.0
    observed = cache.get() or ()
    assert ci.missing([REF_A], observed) == (REF_A,)     # removed after a cache
    assert ci.missing([ID_B], observed) == ()             # and the new one seen


def test_a_claim_can_demand_fresher_evidence_than_the_offer(tmp_path):
    probe = _Probe(_line(REF_A))
    clock = [1000.0]
    cache = _cache(tmp_path, probe=probe, clock=lambda: clock[0])
    assert cache.get() is not None                        # offer TTL record
    assert len(probe.calls) == 1
    clock[0] += ci.CLAIM_FRESHNESS_S + 1.0
    probe.data = b""                                      # the image was removed
    assert cache.get(max_age_s=ci.CLAIM_FRESHNESS_S) == frozenset()
    assert len(probe.calls) == 2


def test_a_failed_probe_is_cached_as_unknown_and_never_as_empty(tmp_path):
    probe = _Probe(absent=True)
    clock = [1000.0]
    cache = _cache(tmp_path, probe=probe, clock=lambda: clock[0])
    assert cache.get() is None
    assert cache.get() is None
    assert len(probe.calls) == 1
    probe.absent = False
    probe.data = _line(ID_A)
    clock[0] += 31.0
    assert ci.missing([ID_A], cache.get() or ()) == ()


@pytest.mark.parametrize("record", [
    {"schema": "something-else", "observed_unix": 1000.0, "entries": [ID_A]},
    {"schema": ci.INVENTORY_SCHEMA, "observed_unix": True, "entries": [ID_A]},
    {"schema": ci.INVENTORY_SCHEMA, "observed_unix": float("nan"), "entries": [ID_A]},
    {"schema": ci.INVENTORY_SCHEMA, "observed_unix": float("inf"), "entries": [ID_A]},
    {"schema": ci.INVENTORY_SCHEMA, "observed_unix": 2000.0, "entries": [ID_A]},   # future
    {"schema": ci.INVENTORY_SCHEMA, "observed_unix": 1000.0, "entries": "bare"},
    {"schema": ci.INVENTORY_SCHEMA, "observed_unix": 1000.0, "entries": ["stage-a:latest"]},
    {"schema": ci.INVENTORY_SCHEMA, "observed_unix": 1000.0, "entries": [7]},
    {"schema": ci.INVENTORY_SCHEMA, "observed_unix": 1000.0,
     "entries": [ID_A] * (ci.MAX_INVENTORY_ENTRIES + 1)},
])
def test_a_foreign_or_corrupt_record_is_unknown_and_refreshed(tmp_path, record):
    probe = _Probe(_line(ID_A))
    cache = _cache(tmp_path, probe=probe)
    _write_record(cache, raw=json.dumps(record).encode())
    assert cache.get() == frozenset({ID_A})
    assert len(probe.calls) == 1


def test_a_stale_record_beyond_the_ttl_is_not_evidence_and_is_refreshed(tmp_path):
    cache = _cache(tmp_path, probe=_Probe(_line(ID_A)))
    _write_record(cache, observed=1000.0 - ci.INVENTORY_TTL_S - 1.0)
    assert cache.get() == frozenset({ID_A})


def test_a_malformed_record_is_ignored_and_refreshed(tmp_path):
    cache = _cache(tmp_path, probe=_Probe(_line(ID_A)))
    _write_record(cache, raw=b"{not json")
    assert cache.get() == frozenset({ID_A})
    assert json.loads(
        (cache.root / cache.record_name).read_text())["schema"] == ci.INVENTORY_SCHEMA


def test_a_symlinked_record_is_not_followed(tmp_path):
    cache = _cache(tmp_path, probe=_Probe(_line(ID_A)))
    cache.root.mkdir(parents=True)
    os.chmod(cache.root, 0o700)
    outside = tmp_path / "outside.json"
    outside.write_text(json.dumps({
        "schema": ci.INVENTORY_SCHEMA, "observed_unix": 1000.0,
        "entries": [ID_A]}), encoding="utf-8")
    (cache.root / cache.record_name).symlink_to(outside)
    # The link records a positive; it must not be read as one.
    assert cache.get() == frozenset({ID_A})
    assert not (cache.root / cache.record_name).is_symlink()


def test_a_world_writable_cache_directory_is_not_trusted(tmp_path):
    cache = _cache(tmp_path, probe=_Probe(_line(ID_A)))
    _write_record(cache, observed=1000.0)
    os.chmod(cache.root, 0o777)
    assert cache.get() is None


def test_a_sibling_holding_the_refresh_lock_leaves_the_caller_unknown(tmp_path):
    cache = _cache(tmp_path, probe=_Probe(_line(ID_A)))
    cache.root.mkdir(parents=True)
    os.chmod(cache.root, 0o700)
    with open(cache.root / "refresh.lock", "a+", encoding="utf-8") as lock:
        import fcntl
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # The sibling's refresh is in flight; this loop must not probe, and
        # must not turn the absence into "no images".
        assert cache.get() is None
