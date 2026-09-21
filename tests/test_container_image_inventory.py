"""Container-image references, and the bounded local inventory behind them.

The inventory is the evidence a claim refuses on (#714), so the two things
that matter are that a short or failed read is *unknown* rather than empty,
and that the reference forms stay distinct: a ``sha256:`` image ID, a
``repository@sha256:`` manifest digest and a ``content:sha256:`` content
reference are different identities even when their hex collides.  The cache
hardening is the review's other half: a corrupt, future-dated, foreign-owned
or symlinked record must never become a positive admission.

#805 is the third form's reason, and the fixtures under
``fixtures/container_images/`` are the real answers both Sparks gave on
2026-09-21: sparky runs Docker's containerd image store and sparklina runs
the classic one, so one image has two IDs and an action sealed with either
could never be claimed by the other box.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys
import time
import tracemalloc

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import container_images as ci  # noqa: E402

ID_A = "sha256:" + "a" * 64
ID_B = "sha256:" + "b" * 64
REF_A = "ghcr.io/example/stage-a@sha256:" + "a" * 64
CONTENT_A = "content:sha256:" + "a" * 64

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "container_images"

#: The image the GLM Stage A campaign runs, held by both boxes (#805).
GLM_TAG = "prismaquant-glm-derivative:causal-exp-v1-20260908"
#: One name, two different images: each Spark built its own.
DIVERGENT_TAG = "prismabuild-slurm-smoke:25.11"


def _layer(seed: str) -> str:
    return "sha256:" + hashlib.sha256(seed.encode()).hexdigest()


def _inspect_row(image_id: str) -> str:
    """A minimal, well-formed inspect row for a synthetic image ID."""

    return json.dumps({
        "Id": image_id,
        "RepoTags": [], "RepoDigests": [], "Size": 1,
        "Created": "2026-09-21T00:00:00.000000000-04:00",
        "Architecture": "arm64", "Os": "linux",
        "Config": {"Entrypoint": ["/bin/sh"]},
        "RootFS": {"Type": "layers", "Layers": [_layer(image_id)]},
    })


class _Probe:
    """A fake bounded probe: bytes on success, ``None`` on any failure.

    ``observe`` takes two reads -- one listing, then one inspect of exactly
    the IDs it named -- so the fake answers on which one it was handed.  With
    no explicit inspect payload it synthesizes a row per requested ID, which
    is what a daemon holding the listed images would answer.
    """

    def __init__(self, data=b"", absent=False, inspect=None,
                 inspect_absent=False):
        self.data = data
        self.absent = absent
        self.inspect = inspect
        self.inspect_absent = inspect_absent
        self.calls = []

    @property
    def listings(self) -> int:
        """How many refreshes happened, counted at the listing read."""

        return sum(1 for argv, _ in self.calls if "ls" in argv)

    @property
    def inspects(self) -> list:
        return [argv for argv, _ in self.calls if "inspect" in argv]

    def __call__(self, argv, **kwargs):
        self.calls.append((argv, kwargs))
        if self.absent:
            return None
        if "inspect" in argv:
            if self.inspect_absent:
                return None
            if self.inspect is not None:
                return self.inspect
            ids = [item for item in argv if item.startswith("sha256:")]
            return ("\n".join(_inspect_row(item) for item in ids)
                    + "\n").encode() if ids else b""
        return self.data


def _line(*refs) -> bytes:
    rows = []
    for ref in refs:
        if ref.startswith("sha256:"):
            rows.append(f"{ref}\t<none>\t<none>")
        else:
            repository, _, digest = ref.partition("@")
            rows.append(f"{ID_A}\t{repository}\t{digest}")
    return ("\n".join(rows) + "\n").encode()


def _synthetic_content(*ids) -> set:
    """The content references ``_Probe``'s synthesized rows publish."""

    return {ci.content_ref(json.loads(_inspect_row(item))) for item in ids}


def _fixture(box: str, name: str) -> bytes:
    return (FIXTURES / f"{box}_image_{name}").read_bytes()


def _box_inventory(box: str):
    """One box's real inventory, read through the real ``observe`` path."""

    probe = _Probe(data=_fixture(box, "ls.txt"),
                   inspect=_fixture(box, "inspect.jsonl"))
    return ci.observe(probe=probe), probe


def _fixture_row(box: str, tag: str) -> dict:
    for line in _fixture(box, "inspect.jsonl").decode().splitlines():
        if not line:
            continue
        row = json.loads(line)
        if tag in (row.get("RepoTags") or []):
            return row
    raise AssertionError(f"{tag} is not in the {box} fixture")


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

def test_every_immutable_reference_form_is_accepted_and_normalized():
    # Sorted strings, so a repository-qualified digest precedes the IDs.
    assert ci.normalize_refs([REF_A, ID_A, ID_A, ID_B]) == (REF_A, ID_A, ID_B)
    assert ci.validate_ref(ID_A) == ID_A
    assert ci.validate_ref("library/ubuntu@sha256:" + "0" * 64)
    assert ci.validate_ref(CONTENT_A) == CONTENT_A
    assert ci.normalize_refs([CONTENT_A, ID_A]) == (CONTENT_A, ID_A)


@pytest.mark.parametrize("bad", [
    "stage-a:replicate",               # mutable tag
    "ghcr.io/example/stage-a:latest",  # mutable tag with a registry
    "sha256:" + "A" * 64,              # uppercase hex is not a digest
    "sha256:" + "a" * 63,              # short
    "sha256:" + "a" * 65,              # long
    "content:" + "a" * 64,             # a content ref names its algorithm
    "content:sha256:" + "A" * 64,      # uppercase hex again
    "content:sha256:" + "a" * 63,      # short
    "repo@content:sha256:" + "a" * 64,  # not a repository-qualified form
    "",
    None,
])
def test_a_mutable_or_malformed_reference_is_refused(bad):
    with pytest.raises(ValueError):
        ci.validate_ref(bad)
    with pytest.raises(ValueError):
        ci.normalize_refs([bad])


def test_the_three_forms_with_the_same_hex_are_not_aliases():
    assert ci.missing([ID_A], [REF_A]) == (ID_A,)
    assert ci.missing([REF_A], [ID_A]) == (REF_A,)
    assert ci.missing([CONTENT_A], [ID_A, REF_A]) == (CONTENT_A,)
    assert ci.missing([ID_A], [CONTENT_A]) == (ID_A,)
    assert ci.missing([ID_A, REF_A, CONTENT_A], [ID_A, REF_A, CONTENT_A]) == ()


# --------------------------------------------------------------------------
# The content reference (#805), against what both Sparks really answered
# --------------------------------------------------------------------------

def test_one_image_on_two_image_stores_has_one_content_reference():
    """The measured #805 fact, and the fix, in one place.

    The campaign image is on both boxes.  sparky's containerd store and
    sparklina's classic store call it two different things, so neither box's
    ID can be sealed into work the other may claim; the content reference is
    the same string on both.
    """

    sparky = _fixture_row("sparky", GLM_TAG)
    sparklina = _fixture_row("sparklina", GLM_TAG)

    assert sparky["Id"] != sparklina["Id"]
    assert sparky["RootFS"]["Layers"] == sparklina["RootFS"]["Layers"]
    # And the older form cannot rescue it: a locally built image has no
    # RepoDigest on the classic store at all.
    assert sparklina["RepoDigests"] == []

    assert ci.content_ref(sparky) == ci.content_ref(sparklina)


def test_two_images_under_one_tag_keep_two_content_references():
    """Each Spark built its own ``prismabuild-slurm-smoke:25.11``.

    Same name, different layers.  The content reference must tell them
    apart, or the new form would admit a box holding a different image.
    """

    sparky = _fixture_row("sparky", DIVERGENT_TAG)
    sparklina = _fixture_row("sparklina", DIVERGENT_TAG)

    assert sparky["RootFS"]["Layers"] != sparklina["RootFS"]["Layers"]
    assert ci.content_ref(sparky) != ci.content_ref(sparklina)


@pytest.mark.parametrize("field, value", [
    ("Id", "sha256:" + "c" * 64),
    ("RepoTags", ["something/else:latest"]),
    ("RepoDigests", []),
    ("Size", 1),
    ("Metadata", {"LastTagTime": "2026-01-01T00:00:00Z"}),
    ("GraphDriver", {"Name": "overlay2", "Data": {"LowerDir": "/elsewhere"}}),
    ("Descriptor", {"mediaType": "application/vnd.oci.image.index.v1+json"}),
    ("Identity", {"Pull": [{"Repository": "docker.io/library/other"}]}),
    ("Parent", "sha256:" + "d" * 64),
    ("DockerVersion", "26.1.3"),
    # Rendered metadata the container never sees.  These four agreed on all
    # 17 shared images, and are still excluded: they are client-formatted
    # strings, so a formatting difference between two daemons would refuse a
    # box that holds the image, and they cannot buy safety in exchange.
    ("Created", "2020-01-01T00:00:00Z"),
    ("Author", "somebody"),
    ("Comment", "rebuilt"),
    ("Variant", "v8"),
])
def test_store_bookkeeping_does_not_enter_the_content_reference(field, value):
    row = _fixture_row("sparky", GLM_TAG)
    before = ci.content_ref(row)
    assert ci.content_ref({**row, field: value}) == before


def _with_config(row, **changes):
    return {**row, "Config": {**row["Config"], **changes}}


def test_anything_the_container_executes_changes_the_content_reference():
    row = _fixture_row("sparky", GLM_TAG)
    before = ci.content_ref(row)
    layers = list(row["RootFS"]["Layers"])

    swapped = list(layers)
    swapped[0], swapped[1] = swapped[1], swapped[0]
    variants = [
        {**row, "RootFS": {"Type": "layers", "Layers": swapped}},
        {**row, "RootFS": {"Type": "layers", "Layers": layers[:-1]}},
        {**row, "RootFS": {"Type": "layers",
                           "Layers": layers + ["sha256:" + "e" * 64]}},
        {**row, "Architecture": "amd64"},
        {**row, "Os": "windows"},
        _with_config(row, Env=list(row["Config"]["Env"]) + ["EXTRA=1"]),
        _with_config(row, Entrypoint=["/bin/sh"]),
        _with_config(row, User="nobody"),
        _with_config(row, WorkingDir="/elsewhere"),
        _with_config(row, Cmd=["--served-model-name", "other"]),
        _with_config(row, ExposedPorts={"8000/tcp": {}}),
        _with_config(row, Volumes={"/data": {}}),
        _with_config(row, Labels={**row["Config"]["Labels"], "new": "label"}),
        _with_config(row, StopSignal="SIGKILL"),
        _with_config(row, Healthcheck={"Test": ["CMD", "true"], "Retries": 3}),
    ]
    for variant in variants:
        assert ci.content_ref(variant) != before, variant.get("Config", {})

    # Layer order is content: the same digests in another order are another
    # filesystem, because the order whiteouts apply in matters.
    assert (ci.content_ref({**row, "RootFS": {"Type": "layers",
                                              "Layers": swapped}})
            != ci.content_ref(row))


def test_a_zero_valued_config_key_says_what_an_absent_one_says():
    """Docker writes the config with ``omitempty``.

    ``"Cmd": null``, ``"Cmd": []`` and no ``Cmd`` key are one statement, and
    two daemons that render one of them differently must still agree.
    """

    row = _fixture_row("sparky", GLM_TAG)
    bare = {**row, "Config": {key: value
                              for key, value in row["Config"].items()
                              if key != "Cmd"}}
    assert ci.content_ref(_with_config(bare, Cmd=None)) == ci.content_ref(bare)
    assert ci.content_ref(_with_config(bare, Cmd=[])) == ci.content_ref(bare)
    assert ci.content_ref(_with_config(bare, User="")) == ci.content_ref(bare)
    assert ci.content_ref(_with_config(bare, ArgsEscaped=False)) == \
        ci.content_ref(bare)
    # A container-config key a daemon pads the object with, at its zero
    # value, says nothing either -- and must not refuse the image.
    assert ci.content_ref(_with_config(bare, Hostname="", OnBuild=None,
                                       AttachStdin=False)) == ci.content_ref(bare)


def test_a_label_with_an_empty_value_is_still_a_label():
    # 10 of sparky's 27 images and 12 of sparklina's 32 carry one, so the
    # zero rule stops at the top of ``Config`` and never enters ``Labels``.
    row = _fixture_row("sparky", GLM_TAG)
    without = {key: value for key, value in row["Config"]["Labels"].items()
               if value != ""}
    assert without != row["Config"]["Labels"], "fixture has no empty label"
    assert ci.content_ref(_with_config(row, Labels=without)) != \
        ci.content_ref(row)


def test_a_config_key_this_does_not_cover_is_refused_when_it_carries_a_value():
    """Refused, never ignored.

    A key outside the covered set that holds a value is a daemon saying
    something this identity cannot price, and pricing it wrong would let one
    box claim work for an image another box does not have.
    """

    row = _fixture_row("sparky", GLM_TAG)
    with pytest.raises(ValueError, match="does not cover"):
        ci.content_ref(_with_config(row, Hostname="build-host"))
    with pytest.raises(ValueError, match="does not cover"):
        ci.content_ref(_with_config(
            row, Healthcheck={"Test": ["CMD", "true"], "Unknown": "x"}))


@pytest.mark.parametrize("row", [
    "not-an-object",
    {"Config": {}, "Architecture": "arm64", "Os": "linux"},          # no rootfs
    {"Config": {}, "Architecture": "arm64", "Os": "linux",
     "RootFS": {"Type": "snapshot", "Layers": ["sha256:" + "a" * 64]}},
    {"Config": {}, "Architecture": "arm64", "Os": "linux",
     "RootFS": {"Type": "layers", "Layers": []}},                    # no layers
    {"Config": {}, "Architecture": "arm64", "Os": "linux",
     "RootFS": {"Type": "layers", "Layers": ["not-a-digest"]}},
    {"Config": "bare", "Architecture": "arm64", "Os": "linux",
     "RootFS": {"Type": "layers", "Layers": ["sha256:" + "a" * 64]}},
    {"Config": {}, "Os": "linux",                                    # no arch
     "RootFS": {"Type": "layers", "Layers": ["sha256:" + "a" * 64]}},
    {"Config": {}, "Architecture": "arm64",                          # no os
     "RootFS": {"Type": "layers", "Layers": ["sha256:" + "a" * 64]}},
    {"Config": {"Env": [7]}, "Architecture": "arm64", "Os": "linux",
     "RootFS": {"Type": "layers", "Layers": ["sha256:" + "a" * 64]}},
    {"Config": {"Labels": {"a": 7}}, "Architecture": "arm64", "Os": "linux",
     "RootFS": {"Type": "layers", "Layers": ["sha256:" + "a" * 64]}},
])
def test_an_inspect_row_this_cannot_read_is_unknown(row):
    with pytest.raises(ValueError):
        ci.content_ref(row)


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
    assert ci.observe(probe=probe) == (
        frozenset({ID_A, REF_A}) | _synthetic_content(ID_A))
    # Both reads: the listing, then the inspect of what it named.  Each one
    # is pinned and scrubbed, because either could answer for another box.
    assert len(probe.calls) == 2
    for argv, kwargs in probe.calls:
        assert argv[1:3] == ["--host", ci.LOCAL_ENDPOINT]
        assert "tcp://remote.invalid:2375" not in argv
        assert "DOCKER_HOST" not in kwargs["env"]
        assert "DOCKER_CONTEXT" not in kwargs["env"]
        assert kwargs["limit"] == ci.MAX_INVENTORY_BYTES


def test_the_inspect_asks_for_exactly_the_ids_the_listing_named():
    # One image is listed once per tag; it is inspected once.
    probe = _Probe((f"{ID_A}\trepo-one\t<none>\n"
                    f"{ID_A}\trepo-two\t<none>\n"
                    f"{ID_B}\t<none>\t<none>\n").encode())
    assert ci.observe(probe=probe) is not None
    argv = probe.inspects[0]
    assert [item for item in argv if item.startswith("sha256:")] == [ID_A, ID_B]


def test_a_listing_with_no_images_needs_no_inspect():
    probe = _Probe(b"")
    assert ci.observe(probe=probe) == frozenset()
    assert probe.inspects == []


def test_a_failed_inspect_makes_the_whole_inventory_unknown():
    """Never the listing alone.

    An inventory carrying IDs but no content references would answer a
    content-form requirement with ``container_image_absent`` -- the
    misleading "the image is missing" refusal #805 is about -- on a box that
    holds the image.  Unknown refuses every form and says so.
    """

    assert ci.observe(probe=_Probe(_line(ID_A), inspect_absent=True)) is None


@pytest.mark.parametrize("inspect", [
    b"not json\n",
    b'{"Id": "sha256:' + b"a" * 64 + b'"}\n',            # no rootfs
    b'[{"RootFS": {"Type": "layers", "Layers": []}}]\n',  # an array, not rows
])
def test_an_inspect_answer_this_cannot_read_is_unknown(inspect):
    assert ci.observe(probe=_Probe(_line(ID_A), inspect=inspect)) is None


def test_the_two_reads_share_one_timeout_budget():
    # A slow listing must not let the pair run to twice the ceiling.
    probe = _Probe(_line(ID_A))
    assert ci.observe(probe=probe, timeout_s=4.0) is not None
    budgets = [kwargs["timeout_s"] for _, kwargs in probe.calls]
    assert budgets[0] <= 4.0
    assert budgets[1] < budgets[0]
    assert sum(budgets) < 8.0


def test_a_listing_naming_more_images_than_a_record_holds_is_unknown():
    many = b"".join(
        (f"sha256:{index:064x}\t<none>\t<none>\n").encode()
        for index in range(ci.MAX_INVENTORY_ENTRIES + 1))
    probe = _Probe(many)
    assert ci.observe(probe=probe) is None
    assert probe.inspects == []


# --------------------------------------------------------------------------
# Both stores, against what the boxes really answered (#805)
# --------------------------------------------------------------------------

def test_an_action_sealed_with_the_content_form_is_admitted_by_either_store():
    sparky, _ = _box_inventory("sparky")
    sparklina, _ = _box_inventory("sparklina")
    assert sparky is not None and sparklina is not None

    required = ci.content_ref(_fixture_row("sparky", GLM_TAG))
    assert ci.missing([required], sparky) == ()
    assert ci.missing([required], sparklina) == ()

    # The old forms still behave as #805 reported, which is why the new one
    # exists: neither box's ID is claimable from the other.
    sparky_id = _fixture_row("sparky", GLM_TAG)["Id"]
    sparklina_id = _fixture_row("sparklina", GLM_TAG)["Id"]
    assert ci.missing([sparky_id], sparklina) == (sparky_id,)
    assert ci.missing([sparklina_id], sparky) == (sparklina_id,)
    # And the repository-qualified form is absent on the classic store for a
    # locally built image, so it is not the fix either.
    assert not any(entry.startswith("prismaquant-glm-derivative@")
                   for entry in sparklina)

    # Each box still shows its own ID and its own inventory is a superset of
    # nothing it does not hold.
    assert ci.missing([sparky_id], sparky) == ()
    assert ci.missing([sparklina_id], sparklina) == ()


def test_a_box_holding_a_different_image_of_the_same_name_is_still_refused():
    sparky, _ = _box_inventory("sparky")
    sparklina, _ = _box_inventory("sparklina")
    required = ci.content_ref(_fixture_row("sparky", DIVERGENT_TAG))
    assert ci.missing([required], sparky) == ()
    assert ci.missing([required], sparklina) == (required,)


def test_a_record_of_a_real_inventory_survives_its_own_validation(tmp_path):
    # Every entry an observation publishes must be a reference the cache
    # will accept back, or a box would refresh into a record it then reads
    # as corrupt and answers unknown from forever.
    probe = _Probe(data=_fixture("sparky", "ls.txt"),
                   inspect=_fixture("sparky", "inspect.jsonl"))
    cache = _cache(tmp_path, probe=probe)
    observed = cache.get()
    assert observed == ci.observe(probe=_Probe(
        data=_fixture("sparky", "ls.txt"),
        inspect=_fixture("sparky", "inspect.jsonl")))
    assert any(entry.startswith("content:") for entry in observed)


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


def test_a_single_read_never_allocates_past_the_chunk_bound():
    """A sized read, not ``BufferedReader.read()``.

    A nonblocking buffered stream can satisfy ``read()`` with no size by
    draining a continuously-fed pipe into one arbitrarily large object, so
    checking the length afterwards is not the bound.  The fake below models
    exactly that drain; the tracked peak shows whether the reader ever asked
    for it.
    """

    read_fd, write_fd = os.pipe()
    try:
        os.write(write_fd, b"x" * 32)
        os.close(write_fd)
        write_fd = -1

        class _Draining:
            def fileno(self):
                return read_fd

            def read(self, size=-1):
                # What ``read()`` without a size may return: one huge object,
                # allocated inside the call, allocated once.
                return b"y" * (8 << 20)

        tracemalloc.start()
        try:
            result = ci._read_capped(
                _Draining(), limit=4096, deadline=time.monotonic() + 5.0)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        assert result is None or len(result) <= 4096
        assert peak < (1 << 20), f"one read allocated {peak} bytes"
    finally:
        os.close(read_fd)
        if write_fd != -1:
            os.close(write_fd)


def test_a_probe_descendant_does_not_survive_the_deadline(tmp_path):
    """The leader can exit while its child still holds the stdout pipe.

    A deadline failure must signal the exact process group the probe was
    spawned under, not only when the leader has not been reaped: polling the
    leader first retires its pid, and a descendant that inherited the pipe
    would otherwise outlive the read (#714 review).
    """

    child_script = tmp_path / "child.py"
    child_script.write_text(
        "import sys, time\n"
        "with open(sys.argv[1], 'a') as stream:\n"
        "    while True:\n"
        "        stream.write('tick\\n')\n"
        "        stream.flush()\n"
        "        time.sleep(0.05)\n",
        encoding="utf-8",
    )
    leader_script = tmp_path / "leader.py"
    leader_script.write_text(
        "import os, subprocess, sys\n"
        "child = subprocess.Popen([sys.executable, sys.argv[1], sys.argv[2]])\n"
        "with open(sys.argv[3], 'w') as stream:\n"
        "    stream.write(str(child.pid))\n"
        "os._exit(0)\n",
        encoding="utf-8",
    )
    marker = tmp_path / "child.log"
    pid_file = tmp_path / "child.pid"

    assert ci._run_bounded(
        [sys.executable, str(leader_script), str(child_script), str(marker),
         str(pid_file)],
        env=dict(os.environ), timeout_s=0.6, limit=4096) is None

    deadline = time.monotonic() + 5.0
    child_pid = None
    while time.monotonic() < deadline and child_pid is None:
        if pid_file.exists():
            child_pid = int(pid_file.read_text(encoding="utf-8").strip())
        else:
            time.sleep(0.05)
    assert child_pid is not None, "the leader never named its child"

    def marker_size() -> int:
        return marker.stat().st_size if marker.exists() else 0

    time.sleep(0.5)
    settled = marker_size()
    time.sleep(1.0)
    assert marker_size() == settled, "the orphaned probe child kept running"

    status = Path(f"/proc/{child_pid}/stat")
    if status.exists():
        text = status.read_text(encoding="utf-8")
        state = text[text.rindex(")") + 2:].split()[0]
        assert state == "Z", f"probe child still running in state {state!r}"


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
        assert (entry.startswith("sha256:") or "@sha256:" in entry
                or entry.startswith("content:")), entry


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
    held = frozenset({ID_A, REF_A}) | _synthetic_content(ID_A)
    assert cache.get() == held
    assert other_loop.get() == held
    assert probe.listings == 1
    clock[0] += 1.0
    assert cache.get() == held
    assert probe.listings == 1


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
    assert probe.listings == 1
    clock[0] += ci.CLAIM_FRESHNESS_S + 1.0
    probe.data = b""                                      # the image was removed
    assert cache.get(max_age_s=ci.CLAIM_FRESHNESS_S) == frozenset()
    assert probe.listings == 2


def test_a_failed_probe_is_cached_as_unknown_and_never_as_empty(tmp_path):
    probe = _Probe(absent=True)
    clock = [1000.0]
    cache = _cache(tmp_path, probe=probe, clock=lambda: clock[0])
    assert cache.get() is None
    assert cache.get() is None
    assert probe.listings == 1
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
    assert cache.get() == frozenset({ID_A}) | _synthetic_content(ID_A)
    assert probe.listings == 1


def test_a_stale_record_beyond_the_ttl_is_not_evidence_and_is_refreshed(tmp_path):
    cache = _cache(tmp_path, probe=_Probe(_line(ID_A)))
    _write_record(cache, observed=1000.0 - ci.INVENTORY_TTL_S - 1.0)
    assert cache.get() == frozenset({ID_A}) | _synthetic_content(ID_A)


def test_a_malformed_record_is_ignored_and_refreshed(tmp_path):
    cache = _cache(tmp_path, probe=_Probe(_line(ID_A)))
    _write_record(cache, raw=b"{not json")
    assert cache.get() == frozenset({ID_A}) | _synthetic_content(ID_A)
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
    assert cache.get() == frozenset({ID_A}) | _synthetic_content(ID_A)
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
