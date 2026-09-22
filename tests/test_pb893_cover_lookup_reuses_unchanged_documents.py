"""PB #893: a repeat cover lookup must not re-read unchanged documents.

``covers_for_keys`` is the stage-fed reader's selection step. PrismaQuant
calls it two or three times for every staged entry, each time with a fresh
``context`` (PQ ``staged_lease._call_context``), so the only cache it had
never hit: every call re-read and re-validated every mover's material
sidecar and fragment. On R11 that is 27 movers, 19 MB of JSON and 13,956
fragment entries per call, and the GPU idled about 75% of the time.

The documents did not change between those calls. A lookup that finds a
file with the same identity as the one it validated last time must reuse
the validated document, not validate it again, and must answer exactly what
the pre-#893 lookup answers (``pb893_reference``, frozen from ef997618e133)
for every document state and every republish between calls, including the
same-generation incremental republish of #823.

Every republish in these fixtures changes the file's size, so the identity
change the cache relies on never depends on the test filesystem's timestamp
granularity. The production writers rename a new inode into place on every
publish. Run via published pbtest at -10; fixtures use tmp_path roots only.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import random
import sys
import threading

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import residency_map, reader_lease  # noqa: E402
import pb893_reference as reference  # noqa: E402

CONSUMER = "c" * 64
TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:dl380g10"
MANIFEST = "a" * 64
OTHER_MANIFEST = "9" * 64


def _mover(index: int) -> str:
    return f"{index:064x}"


@pytest.fixture(autouse=True)
def _fresh_cache():
    reader_lease.clear_cover_docs_cache()
    yield
    reader_lease.clear_cover_docs_cache()


def _publish(root: Path, stage: Path, mover: str, keys: dict[str, str],
             generation: str) -> None:
    """One publication: fragment first, then the sidecar that dates it."""

    staged = {}
    dated = {}
    for key, digest in keys.items():
        path = stage / mover[-8:] / key.split("/")[-1]
        staged[key] = {"stage_path": str(path), "bytes": 512,
                       "sha256": digest, "offset": 0}
        dated[key] = {"stage_path": str(path), "bytes": 512,
                      "sha256": digest,
                      "file_id": {"ino": 7, "size": 512, "mtime_ns": 1,
                                  "ctime_ns": 1}}
    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": MANIFEST, "entries": staged})
    reader_lease.write_material(
        root, consumer_action_key=CONSUMER, mover_action_key=mover,
        tier_id=TIER, stage_root=str(stage), manifest_sha256=MANIFEST,
        generation=generation, entries=dated)


@pytest.fixture()
def counted(monkeypatch):
    """Count every material and fragment validation the lookup performs."""

    counts = {"material": 0, "fragment": 0}
    real_material = reader_lease.validate_material
    real_fragment = residency_map.validate_fragment

    def material(value):
        counts["material"] += 1
        return real_material(value)

    def fragment(value):
        counts["fragment"] += 1
        return real_fragment(value)

    monkeypatch.setattr(reader_lease, "validate_material", material)
    monkeypatch.setattr(residency_map, "validate_fragment", fragment)
    return counts


def test_a_repeat_lookup_with_a_fresh_context_revalidates_nothing(
        tmp_path: Path, counted) -> None:
    """PQ's call pattern: fresh context every call, documents unchanged."""

    root = tmp_path / "residency"
    stage = tmp_path / "stage"
    keys = {f"0:/mnt/shared/pb893/shard-{index:05d}.bin": f"{index + 1:064x}"
            for index in range(6)}
    movers = [_mover(index + 1) for index in range(3)]
    for number, mover in enumerate(movers):
        owned = dict(list(keys.items())[number * 2:number * 2 + 2])
        _publish(root, stage, mover, owned, reader_lease.mint_generation())

    def lookup(key: str) -> dict:
        return reader_lease.covers_for_keys(
            root, CONSUMER, [key], tier_id=TIER, manifest_sha256=MANIFEST,
            epoch="", context={})

    # Publishing validates too; count only what the lookups do.
    counted.update(material=0, fragment=0)
    first_key = next(iter(keys))
    first = lookup(first_key)
    assert first["ok"], first
    assert first["covers"] == [{"mover_action_key": movers[0],
                                "manifest_sha256": MANIFEST}]
    seen = dict(counted)
    assert seen == {"material": 3, "fragment": 3}

    # The same documents, asked again and for every other key: nothing on
    # disk changed, so nothing may be read and validated again.
    for key in keys:
        answer = lookup(key)
        assert answer["ok"], answer
        assert answer["expected"] == {key: {"bytes": 512,
                                            "sha256": keys[key]}}
    assert counted == seen


# --------------------------------------------------------------------------
# #823: a republish between two calls is seen, atomic or in place
# --------------------------------------------------------------------------

def _rewrite_in_place(path: Path, document: dict) -> None:
    """Rewrite ``path`` through its existing inode (no rename)."""

    before = os.stat(path).st_ino
    with open(path, "r+") as stream:
        stream.seek(0)
        stream.truncate()
        json.dump(document, stream, sort_keys=True)
        stream.write("\n")
    assert os.stat(path).st_ino == before


@pytest.mark.parametrize("persistent", [False, True],
                         ids=["fresh-context", "persistent-context"])
@pytest.mark.parametrize("how", ["atomic", "in-place"])
def test_a_same_generation_republish_between_calls_is_seen(
        tmp_path: Path, how: str, persistent: bool) -> None:
    root = tmp_path / "residency"
    stage = tmp_path / "stage"
    mover = _mover(1)
    key_a = "0:/mnt/shared/pb893/a.bin"
    key_b = "0:/mnt/shared/pb893/b.bin"
    generation = reader_lease.mint_generation()
    shared: dict = {}

    def lookup(key: str) -> dict:
        return reader_lease.covers_for_keys(
            root, CONSUMER, [key], tier_id=TIER, manifest_sha256=MANIFEST,
            epoch="", context=shared if persistent else {})

    _publish(root, stage, mover, {key_a: "1" * 64}, generation)
    assert lookup(key_a)["ok"]
    assert lookup(key_b) == {"ok": False, "refusal": "unpublished"}

    if how == "atomic":
        _publish(root, stage, mover, {key_a: "1" * 64, key_b: "2" * 64},
                 generation)
    else:
        # Build the next documents with the real writers elsewhere, then
        # copy them into the live files through the same inodes.
        scratch = tmp_path / "scratch"
        _publish(scratch, stage, mover, {key_a: "1" * 64, key_b: "2" * 64},
                 generation)
        for name in (residency_map.fragment_path(scratch, CONSUMER, mover),
                     reader_lease.material_path(scratch, CONSUMER, mover)):
            live = root / Path(name).relative_to(scratch)
            _rewrite_in_place(live, json.loads(Path(name).read_text()))

    after = lookup(key_b)
    assert after["ok"], after
    assert after["expected"] == {key_b: {"bytes": 512, "sha256": "2" * 64}}
    assert after["covers"] == [{"mover_action_key": mover,
                                "manifest_sha256": MANIFEST}]


def test_absence_and_malformation_are_never_cached(tmp_path: Path) -> None:
    root = tmp_path / "residency"
    stage = tmp_path / "stage"
    mover = _mover(1)
    key = "0:/mnt/shared/pb893/a.bin"
    generation = reader_lease.mint_generation()

    def lookup() -> dict:
        return reader_lease.covers_for_keys(
            root, CONSUMER, [key], tier_id=TIER, manifest_sha256=MANIFEST,
            epoch="", context={})

    _publish(root, stage, mover, {key: "1" * 64}, generation)
    assert lookup()["ok"]
    slot = (str(root), CONSUMER, mover)
    assert slot in reader_lease._COVER_DOCS

    material = reader_lease.material_path(root, CONSUMER, mover)
    good = material.read_text()
    material.write_text("{not json" + " " * len(good))
    assert lookup() == {"ok": False, "refusal": "unpublished"}
    assert slot not in reader_lease._COVER_DOCS

    material.write_text(good)
    assert lookup()["ok"]
    fragment = residency_map.fragment_path(root, CONSUMER, mover)
    fragment.unlink()
    assert lookup() == {"ok": False, "refusal": "unpublished"}
    assert slot not in reader_lease._COVER_DOCS

    _publish(root, stage, mover, {key: "1" * 64}, generation)
    assert lookup()["ok"]
    # A mover whose sidecar leaves the directory leaves the cache too.
    material.unlink()
    assert lookup() == {"ok": False, "refusal": "unpublished"}
    assert slot not in reader_lease._COVER_DOCS


def test_the_cache_is_bounded_and_answers_stay_exact(
        tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "residency"
    stage = tmp_path / "stage"
    monkeypatch.setattr(reader_lease, "COVER_DOCS_CACHE_PAIRS", 3)
    keys = {}
    for index in range(1, 8):
        key = f"0:/mnt/shared/pb893/bounded-{index}.bin"
        keys[key] = _mover(index)
        _publish(root, stage, _mover(index), {key: f"{index:064x}"},
                 reader_lease.mint_generation())
    for key in list(keys) * 2:
        answer = reader_lease.covers_for_keys(
            root, CONSUMER, [key], tier_id=TIER, manifest_sha256=MANIFEST,
            epoch="", context={})
        with reference.old_hex_checks():
            expected = reference.covers_for_keys(
                root, CONSUMER, [key], tier_id=TIER,
                manifest_sha256=MANIFEST, epoch="", context={})
        assert answer == expected
        assert answer["covers"] == [{"mover_action_key": keys[key],
                                     "manifest_sha256": MANIFEST}]
        assert len(reader_lease._COVER_DOCS) <= 3


# --------------------------------------------------------------------------
# Equivalence with the frozen pre-#893 lookup over generated document states
# --------------------------------------------------------------------------

STAGE_ROOT = "/stage/pb893"
POOL = [f"{offset}:/mnt/shared/pb893/src-{index:02d}.bin"
        for index in range(10) for offset in (0, 4096)][:14]
QUERY_TIERS = [(TIER, ""), (RAM_TIER, "E1"), (RAM_TIER, "E0"),
               (RAM_TIER, "")]


def _digest_for(rng: random.Random) -> str:
    return rng.choice(["1", "2", "3"]) * 64


class Fleet:
    """One consumer's material and fragment files, written as raw JSON.

    Raw, not through the writers, so the states the writers refuse
    (malformed JSON, invalid digests, a staged epoch) can be laid down too.
    Every write of a path changes its size (trailing whitespace is valid
    JSON), so every republish changes the file's identity on any filesystem.
    """

    def __init__(self, root: Path, rng: random.Random) -> None:
        self.root = root
        self.rng = rng
        self.movers: dict[str, dict] = {}
        self.sizes: dict[Path, int] = {}

    # -- document state ---------------------------------------------------

    def new_state(self) -> dict:
        rng = self.rng
        tier = rng.choice([TIER, TIER, RAM_TIER])
        owned = rng.sample(POOL, rng.randint(0, 5))
        entries = {key: (rng.choice([100, 200]), _digest_for(rng))
                   for key in owned}
        state = {
            "tier": tier,
            "material_epoch": (rng.choice(["E1", "E1", "E0"])
                               if tier == RAM_TIER
                               else rng.choice([None] * 4 + ["E1"])),
            "fragment_epoch": None,
            "manifest": rng.choice([MANIFEST] * 5 + [OTHER_MANIFEST]),
            "fragment_manifest": None,
            "fragment_tier": None,
            "entries": entries,
            "fragment_entries": dict(entries),
            "generation": f"{rng.getrandbits(128):032x}",
            "material": rng.choice(["ok"] * 8 + ["missing", "json",
                                                 "invalid"]),
            "fragment": rng.choice(["ok"] * 8 + ["missing", "json",
                                                 "invalid"]),
        }
        state["fragment_epoch"] = state["material_epoch"]
        if tier == RAM_TIER and rng.random() < 0.2:
            state["fragment_epoch"] = rng.choice(["E1", "E0"])
        state["fragment_manifest"] = (
            state["manifest"] if rng.random() < 0.9 else OTHER_MANIFEST)
        state["fragment_tier"] = tier if rng.random() < 0.9 else (
            RAM_TIER if tier == TIER else TIER)
        self._disagree(state)
        return state

    def _disagree(self, state: dict) -> None:
        """Sometimes the fragment vouches different bytes or omits a key."""

        rng = self.rng
        for key in list(state["fragment_entries"]):
            roll = rng.random()
            if roll < 0.1:
                size, _ = state["fragment_entries"][key]
                state["fragment_entries"][key] = (size, _digest_for(rng))
            elif roll < 0.15:
                del state["fragment_entries"][key]

    def material_doc(self, mover: str, state: dict) -> object:
        document: dict[str, object] = {
            "schema": reader_lease.MATERIAL_SCHEMA_V1,
            "consumer_action_key": CONSUMER, "mover_action_key": mover,
            "tier_id": state["tier"], "stage_root": STAGE_ROOT,
            "manifest_sha256": state["manifest"],
            "generation": state["generation"],
            "entries": {key: {
                "stage_path": f"{STAGE_ROOT}/{mover[-6:]}/{index}",
                "bytes": size, "sha256": digest,
                "file_id": {"ino": index + 1, "size": size,
                            "mtime_ns": 5, "ctime_ns": 5}}
                for index, (key, (size, digest))
                in enumerate(sorted(state["entries"].items()))},
        }
        if state["material_epoch"] is not None:
            document["epoch"] = state["material_epoch"]
        return document

    def fragment_doc(self, mover: str, state: dict) -> object:
        positions = {key: index for index, key
                     in enumerate(sorted(state["entries"]))}
        document: dict[str, object] = {
            "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
            "consumer_action_key": CONSUMER, "mover_action_key": mover,
            "tier_id": state["fragment_tier"], "stage_root": STAGE_ROOT,
            "manifest_sha256": state["fragment_manifest"],
            "entries": {key: {
                "stage_path": f"{STAGE_ROOT}/{mover[-6:]}/"
                              f"{positions.get(key, 99)}",
                "bytes": size, "offset": int(key.split(":")[0]),
                "sha256": digest}
                for key, (size, digest)
                in sorted(state["fragment_entries"].items())},
        }
        epoch = state["fragment_epoch"]
        if state["fragment_tier"] == RAM_TIER and epoch is None:
            epoch = "E1"
        if epoch is not None:
            document["epoch"] = epoch
        return document

    # -- files --------------------------------------------------------------

    def material_path(self, mover: str) -> Path:
        return reader_lease.material_path(self.root, CONSUMER, mover)

    def fragment_path(self, mover: str) -> Path:
        return residency_map.fragment_path(self.root, CONSUMER, mover)

    def _write(self, path: Path, how: str, document: object,
               state: str) -> None:
        if state == "missing":
            path.unlink(missing_ok=True)
            self.sizes.pop(path, None)
            return
        if state == "json":
            text = '{"schema": '
        else:
            if state == "invalid":
                document = json.loads(json.dumps(document))
                entries = document.get("entries") or {}
                if entries:
                    first = sorted(entries)[0]
                    entries[first]["sha256"] = "Z" * 64
                else:
                    document["manifest_sha256"] = "not-hex"
            text = json.dumps(document, sort_keys=True) + "\n"
        previous = self.sizes.get(path)
        while previous is not None and len(text.encode()) == previous:
            text += " "
        path.parent.mkdir(parents=True, exist_ok=True)
        if how == "in-place" and path.exists():
            with open(path, "r+") as stream:
                stream.seek(0)
                stream.truncate()
                stream.write(text)
        else:
            temporary = path.with_name(f".{path.name}.tmp")
            temporary.write_text(text)
            os.replace(temporary, path)
        self.sizes[path] = len(text.encode())

    def publish(self, mover: str, how: str, *, sidecar: bool = True) -> None:
        state = self.movers[mover]
        self._write(self.fragment_path(mover), how,
                    self.fragment_doc(mover, state), state["fragment"])
        if sidecar:
            self._write(self.material_path(mover), how,
                        self.material_doc(mover, state), state["material"])

    # -- mutations ----------------------------------------------------------

    def step(self) -> str:
        rng = self.rng
        how = rng.choice(["atomic", "in-place"])
        movers = sorted(self.movers)
        roll = rng.random()
        if not movers or roll < 0.15:
            mover = _mover(rng.randint(1, 9))
            self.movers[mover] = self.new_state()
            self.publish(mover, how)
            return f"new {mover[-2:]} {how}"
        mover = rng.choice(movers)
        state = self.movers[mover]
        if roll < 0.45:
            # Incremental republish under the same generation (#823).
            key = rng.choice(POOL)
            entry = (rng.choice([100, 200]), _digest_for(rng))
            state["entries"][key] = entry
            state["fragment_entries"][key] = entry
            self.publish(mover, how)
            return f"land {mover[-2:]} {how}"
        if roll < 0.55:
            # A mover caught between its fragment and its sidecar.
            key = rng.choice(POOL)
            entry = (rng.choice([100, 200]), _digest_for(rng))
            state["entries"][key] = entry
            state["fragment_entries"][key] = entry
            self.publish(mover, how, sidecar=False)
            return f"fragment-only {mover[-2:]} {how}"
        if roll < 0.7:
            self.movers[mover] = self.new_state()
            self.publish(mover, how)
            return f"retry {mover[-2:]} {how}"
        if roll < 0.8:
            for field in ("material", "fragment"):
                state[field] = rng.choice(["ok", "ok", "missing", "json",
                                           "invalid"])
            self.publish(mover, how)
            return f"damage {mover[-2:]} {how}"
        if roll < 0.87:
            del self.movers[mover]
            for path in (self.material_path(mover), self.fragment_path(mover)):
                path.unlink(missing_ok=True)
                self.sizes.pop(path, None)
            return f"retire {mover[-2:]}"
        if roll < 0.93:
            junk = self.root / "material" / CONSUMER
            junk.mkdir(parents=True, exist_ok=True)
            choice = rng.choice(["notes.txt", "ZZ.json", "dir.json"])
            if choice == "dir.json":
                (junk / choice).mkdir(exist_ok=True)
            else:
                (junk / choice).write_text("junk")
            return f"junk {choice}"
        self._disagree(state)
        self.publish(mover, how)
        return f"disagree {mover[-2:]} {how}"


def _queries(rng: random.Random) -> list[list[str]]:
    queries = [[key] for key in POOL]
    queries.append(list(POOL))
    queries.append([])
    queries.append(["0:/mnt/shared/pb893/nobody.bin"])
    for _ in range(4):
        picked = rng.sample(POOL, rng.randint(2, 6))
        picked.append(rng.choice(picked))  # a duplicate key
        queries.append(picked)
    return queries


def _assert_cache_matches_disk(root: Path) -> None:
    """A cached document is the validated file whenever identities agree."""

    with reader_lease._COVER_DOCS_LOCK:
        held = dict(reader_lease._COVER_DOCS)
    for (slot_root, consumer, mover), slots in held.items():
        if slot_root != str(root):
            continue
        paths = (reader_lease.material_path(root, consumer, mover),
                 residency_map.fragment_path(root, consumer, mover))
        validators = (reader_lease.validate_material,
                      residency_map.validate_fragment)
        for slot, path, validate in zip(slots, paths, validators):
            if slot is None:
                continue
            try:
                with open(path) as stream:
                    info = os.fstat(stream.fileno())
                    identity = (info.st_dev, info.st_ino, info.st_size,
                                info.st_mtime_ns, info.st_ctime_ns)
                    if identity != slot[0]:
                        continue
                    assert slot[1] == validate(json.load(stream))
            except FileNotFoundError:
                continue


def test_covers_for_keys_answers_exactly_what_the_pre_893_lookup_answers(
        tmp_path: Path) -> None:
    compared = 0
    outcomes: set[str] = set()
    for seed in (893, 823, 2026):
        compared += _compare_sequence(tmp_path / str(seed), seed, outcomes)
    # The generated states must actually reach every answer the lookup has,
    # or the comparison proved less than it claims.
    assert outcomes >= {"ok", "unpublished", "source-coverage-gap",
                        "ownership-uncertain: contradictory covers",
                        "ownership-uncertain: sidecar/fragment disagree",
                        "ownership-uncertain: staged epoch set"}, outcomes
    assert compared > 30_000, compared


def _compare_sequence(base: Path, seed: int, outcomes: set[str]) -> int:
    """One seeded sequence of republishes, each followed by every query.

    Each query runs four ways: the new and the frozen lookup with a fresh
    context per call (PQ's pattern), and each again with its own context
    kept across the whole sequence.  Returns the number of comparisons.
    """

    rng = random.Random(seed)
    root = base / "residency"
    fleet = Fleet(root, rng)
    persistent_new: dict = {}
    persistent_old: dict = {}
    compared = 0
    for step in range(45):
        event = fleet.step()
        if step % 15 == 14:
            # The whole material directory vanishes, then comes back.
            directory = root / "material" / CONSUMER
            parked = root / "material" / "parked"
            if directory.exists():
                directory.rename(parked)
                for tier_id, epoch in QUERY_TIERS[:2]:
                    for keys in _queries(rng)[:3]:
                        new = reader_lease.covers_for_keys(
                            root, CONSUMER, keys, tier_id=tier_id,
                            manifest_sha256=MANIFEST, epoch=epoch,
                            context={})
                        with reference.old_hex_checks():
                            old = reference.covers_for_keys(
                                root, CONSUMER, keys, tier_id=tier_id,
                                manifest_sha256=MANIFEST, epoch=epoch,
                                context={})
                        assert new == old == {"ok": False,
                                              "refusal": "unpublished"}
                parked.rename(directory)
        for tier_id, epoch in QUERY_TIERS:
            for manifest in (MANIFEST, OTHER_MANIFEST):
                for keys in _queries(rng):
                    arguments = dict(tier_id=tier_id,
                                     manifest_sha256=manifest, epoch=epoch)
                    new = reader_lease.covers_for_keys(
                        root, CONSUMER, keys, context={}, **arguments)
                    with reference.old_hex_checks():
                        old = reference.covers_for_keys(
                            root, CONSUMER, keys, context={}, **arguments)
                    assert new == old, (seed, step, event, keys, arguments)
                    new = reader_lease.covers_for_keys(
                        root, CONSUMER, keys, context=persistent_new,
                        **arguments)
                    with reference.old_hex_checks():
                        old = reference.covers_for_keys(
                            root, CONSUMER, keys, context=persistent_old,
                            **arguments)
                    assert new == old, (seed, step, event, keys, arguments,
                                        "persistent")
                    compared += 2
                    outcomes.add(str(new.get("refusal") or "ok"))
        _assert_cache_matches_disk(root)
    return compared


def test_concurrent_lookups_agree_with_serial_answers(tmp_path: Path) -> None:
    rng = random.Random(4242)
    root = tmp_path / "residency"
    fleet = Fleet(root, rng)
    for _ in range(12):
        fleet.step()
    queries = [(keys, tier_id, epoch)
               for keys in _queries(rng) for tier_id, epoch in QUERY_TIERS]
    with reference.old_hex_checks():
        serial = [reference.covers_for_keys(
            root, CONSUMER, keys, tier_id=tier_id, manifest_sha256=MANIFEST,
            epoch=epoch, context={}) for keys, tier_id, epoch in queries]
    failures: list[object] = []
    start = threading.Barrier(8)

    def worker(offset: int) -> None:
        try:
            start.wait()
            for turn in range(3):
                for index in range(len(queries)):
                    position = (index + offset * 7 + turn) % len(queries)
                    keys, tier_id, epoch = queries[position]
                    answer = reader_lease.covers_for_keys(
                        root, CONSUMER, keys, tier_id=tier_id,
                        manifest_sha256=MANIFEST, epoch=epoch, context={})
                    if answer != serial[position]:
                        failures.append((position, answer, serial[position]))
        except Exception as exc:  # pragma: no cover - reported below
            failures.append(exc)

    threads = [threading.Thread(target=worker, args=(offset,))
               for offset in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert failures == []


def test_lookups_under_a_concurrent_republisher_see_its_final_state(
        tmp_path: Path) -> None:
    root = tmp_path / "residency"
    stage = tmp_path / "stage"
    mover = _mover(1)
    generation = reader_lease.mint_generation()
    landed: dict[str, str] = {}
    keys = [f"0:/mnt/shared/pb893/landing-{index:03d}.bin"
            for index in range(40)]
    _publish(root, stage, mover, {keys[0]: "1" * 64}, generation)
    landed[keys[0]] = "1" * 64
    errors: list[BaseException] = []
    done = threading.Event()

    def reader() -> None:
        try:
            while not done.is_set():
                for key in keys[:5]:
                    answer = reader_lease.covers_for_keys(
                        root, CONSUMER, [key], tier_id=TIER,
                        manifest_sha256=MANIFEST, epoch="", context={})
                    assert answer.get("ok") or answer.get("refusal") in {
                        "source-coverage-gap", "unpublished"}, answer
        except BaseException as exc:  # pragma: no cover - reported below
            errors.append(exc)

    readers = [threading.Thread(target=reader) for _ in range(4)]
    for thread in readers:
        thread.start()
    try:
        for key in keys[1:]:
            landed[key] = "2" * 64
            _publish(root, stage, mover, dict(landed), generation)
    finally:
        done.set()
        for thread in readers:
            thread.join()
    assert errors == []
    for key in keys:
        answer = reader_lease.covers_for_keys(
            root, CONSUMER, [key], tier_id=TIER, manifest_sha256=MANIFEST,
            epoch="", context={})
        assert answer["ok"], (key, answer)
        assert answer["expected"] == {key: {"bytes": 512,
                                            "sha256": landed[key]}}


# --------------------------------------------------------------------------
# The C-speed hex checks accept and refuse exactly what the old ones did
# --------------------------------------------------------------------------

class _Str(str):
    pass


HEX_CORPUS: list[object] = [
    "a" * 64, "0123456789abcdef" * 4, "f" * 63, "f" * 65, "", "A" * 64,
    "a" * 63 + "G", "a" * 63 + "\n", "a" * 64 + "\n", "\n" + "a" * 63,
    "a" * 63 + " ", "a" * 63 + "٠", "a" * 63 + "ａ",
    "a" * 63 + "é", "a" * 32, "b" * 32, "B" * 32, "g" * 32,
    _Str("c" * 64), _Str("c" * 32), _Str("C" * 64), None, 0, 1.5,
    b"a" * 64, ["a"] * 64, {"a": 1}, True,
]


def _outcome(function, value, *args, **kwargs):
    try:
        return ("value", function(value, *args, **kwargs))
    except Exception as exc:  # the type and message are the contract
        return ("error", type(exc), str(exc))


@pytest.mark.parametrize("value", HEX_CORPUS, ids=repr)
def test_hex_checks_keep_their_accept_set_and_error_text(value) -> None:
    assert (_outcome(residency_map._digest, value, where="w")
            == _outcome(reference._digest, value, where="w"))
    assert (_outcome(residency_map._action_key, value, where="w")
            == _outcome(reference._action_key, value, where="w"))
    for length in (32, 64):
        assert (_outcome(reader_lease._hex, value, length, where="w")
                == _outcome(reference._hex, value, length, where="w"))
