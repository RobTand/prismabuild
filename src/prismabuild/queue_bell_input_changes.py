"""Compare authoritative observations after names-only hints.

Readers supply policy meaning, proof state and evidence deadlines. The bell
does not interpret records, and this comparison never grants admission. A
caller remembers the result of its own evaluation before consuming its writes.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import math

from .denial_records import diagnostic_stamp
from .digest_primitives import sorted_json
from .queue_bell_topics import TOPICS


@dataclass(frozen=True)
class InputObservation:
    meaning: object
    state: str
    deadlines: tuple[float, ...] = ()


class InputChanges:
    def __init__(self):
        self._observed = {}
        self._deadlines = {}
        self.changed = 0
        self.coalesced = 0
        self.expired = 0

    def observe(self, topic, identity, observation: InputObservation, *, own_write=False):
        """Record a confirmed read, including unknown/absent/freshness states.

        Deadlines are absolute in the caller's clock. A timestamp-only renewal
        may move an evidence deadline without changing meaning. Freshness and
        proof maturity belong in state/meaning even when rounded capacity agrees.
        """
        if topic not in TOPICS or not TOPICS[topic].accepts_id(identity):
            raise ValueError("invalid input observation identifier")
        key = topic, identity
        # Detach mutable reader results so an in-place change cannot also edit
        # the comparison baseline. Unknown schema fields remain significant.
        meaning = sorted_json([observation.state, observation.meaning], allow_nan=False)
        changed = self._observed.get(key) != meaning
        self._observed[key] = meaning
        self._deadlines[key] = observation.deadlines
        if own_write or not changed:
            self.coalesced += 1
            return False
        self.changed += 1
        return True

    @property
    def next_deadline(self):
        return min((deadline for deadlines in self._deadlines.values()
                    for deadline in deadlines), default=None)

    def due(self, now):
        """Consume evidence expiries once; policy starts are never invented here."""
        due = {key for key, deadlines in self._deadlines.items()
               if any(deadline <= now for deadline in deadlines)}
        for key in due:
            self._deadlines[key] = tuple(deadline for deadline in self._deadlines[key]
                                         if deadline > now)
        self.expired += len(due)
        return due

    def discard(self, topic, identity):
        self._observed.pop((topic, identity), None)
        self._deadlines.pop((topic, identity), None)


def claim_denial_meaning(document):
    """The latest per-publication refusals, without diagnostic pass summaries."""
    records = document.get("records")
    if not isinstance(records, Mapping):
        return {}
    return {identity: {name: value for name, value in record.items()
                       if name != "denied_unix"}
            for identity, record in records.items() if isinstance(record, Mapping)}


def pass_meaning(document):
    """Retain real pass counts, episode starts and protected policy metadata."""
    return {name: value for name, value in document.items() if name != "updated_unix"}


def adaptive_meaning(documents):
    """Remove mirror/counter sample stamps, retaining learned and proof state.

    Consumers separately report freshness and arm peer_evidence_deadlines.
    Counter histories retain their timestamps because baseline maturity can
    depend on elapsed history even when rounded capacity did not change.
    """
    ignored = {"_snapshot", "sampled_unix", "sample_id"}
    meaning = {}
    for name, document in documents.items():
        if name in ("publisher-result.json", "claim-denials.json"):
            continue
        value = {key: field for key, field in document.items() if key not in ignored}
        if name == "cpu-sample.json" and isinstance(value.get("observation"), dict):
            value["observation"] = {key: field for key, field in value["observation"].items()
                                    if key not in ignored}
        meaning[name] = value
    return meaning


def denial_transition_meaning(document):
    """Keep refusal history and latest reason while dropping repeat counters."""
    transitions = [record for record in document.get("transitions", [])
                   if isinstance(record, Mapping)]
    latest = {}
    for record in transitions:
        if (not isinstance(record.get("host"), str)
                or not isinstance(record.get("published_unix"), (int, float))):
            continue
        identity = record["host"], record["published_unix"]
        stamp = diagnostic_stamp(record, "last_unix" if "last_unix" in record else "unix")
        previous = latest.get(identity)
        if previous is None or stamp > previous[0]:
            latest[identity] = stamp, record
    return {"action_key": document.get("action_key"),
            "transitions": [{name: value for name, value in record.items()
                             if name not in ("last_unix", "count")}
                            for record in transitions],
            "latest": sorted((host, publication, record.get("reason"), record.get("decision_reason"))
                             for (host, publication), (_, record) in latest.items())}


def peer_evidence_deadlines(*, now, announced_unix=None, cpu_sampled_unix=None,
                            gpu_sampled_unix=None):
    """Wall-clock expiry of already validated stamps, using policy horizons.

    Policy accepts age equal to the horizon, so wake at the first representable
    later instant. Consumers convert once to their wait clock when arming.
    """
    from .adaptive_cpu import MAX_SAMPLE_AGE_S as CPU_AGE
    from .adaptive_gpu import MAX_SAMPLE_AGE_S as GPU_AGE
    from .pool import OFFER_TIMEOUT_S

    return tuple(math.nextafter(stamp + horizon, math.inf)
                 for stamp, horizon in ((announced_unix, OFFER_TIMEOUT_S),
                     (cpu_sampled_unix, CPU_AGE), (gpu_sampled_unix, GPU_AGE))
                 if stamp is not None and math.nextafter(stamp + horizon, math.inf) > now)
