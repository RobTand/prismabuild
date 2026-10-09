"""The demand-based gang fence, as a rule over declared demands (#1579).

``gang_blocking`` is a pure function of the census, the row, the host, the time
and the host ledger's ``held`` and ``capacity``, and of whether the caller found
the row to be a movement node this host trusts (``exempt``).  A gang that has
waited past ``GANG_RESERVE_AFTER_S``, on a host that holds the protected copy of
its runtime, reserves its elected member's declared demand; a new equal-priority
row is admitted only if the reservation survives it.  On a host without the copy
the gang keeps only the fence against strictly lower priority.
"""
import random

import pytest

from prismabuild import _measurement_reservation as reservation, movement_actions

FIRST = 1000.0
CAPACITY = {"cpu": 20, "gpu": 1, "mem_gb": 120}
MEMBER = {"cpu": 2, "gpu": 1, "mem_gb": 100}
BOUND = getattr(reservation, "GANG_RESERVE_AFTER_S", 600.0)  # absent before #1579
YOUNG = FIRST + BOUND - 1
OLD = FIRST + BOUND + 1


def _census(priority=-10, demand=MEMBER, host="sparky", other=None):
    elections = {"g" * 63 + "1": {
        "group": "g" * 32, "index": 0, "action_key": "g" * 63 + "1", "host": host,
        "priority": priority, "rank": [-priority, FIRST, "g" * 32], "demand": demand}}
    if other is not None:
        elections.update(other)
    return {"gang_elections": elections}


def _row(**over):
    row = {"action_key": "a" * 64, "priority": -10, "published_unix": FIRST + 5,
           "needs_gpu": False, "resources": {"cpu": 1, "mem_gb": 1}}
    row.update(over)
    return row


def _blocked(row, *, now=OLD, held=None, capacity=CAPACITY, census=None, exempt=False,
             authority=True, **kw):
    census = _census(**kw) if census is None else census
    return reservation.gang_blocking(census, row, host="sparky", group=None, now=now,
                                     held={} if held is None else held, capacity=capacity,
                                     exempt=exempt, authority=authority)


# --- the rule table -----------------------------------------------------------------

def test_a_young_gang_reserves_nothing():
    assert _blocked(_row(needs_gpu=True, resources={"cpu": 1, "gpu": 1, "mem_gb": 1}), now=YOUNG) is None


def test_a_gpu_single_is_held_by_the_gpu_the_member_needs():
    held = _blocked(_row(needs_gpu=True, resources={"cpu": 1, "gpu": 1, "mem_gb": 8}))
    assert held is not None and held["reservation"] == {"gpu": 1}


def test_a_large_cpu_row_is_held_by_the_memory_the_member_needs():
    held = _blocked(_row(resources={"cpu": 8, "mem_gb": 64}))
    assert held is not None and held["reservation"] == {"mem_gb": 44}



def test_a_running_member_reserves_nothing():
    """A CLAIMED member already holds its demand in the ledger (#1659)."""
    census = _census()
    for election in census["gang_elections"].values():
        election["member_state"] = "claimed"
    assert _blocked(_row(resources={"cpu": 1, "mem_gb": 1}), census=census) is None
    assert reservation.reservation_priority_on(census, host="sparky", now=OLD, authority=True) is None


def test_a_terminal_member_reserves_nothing_not_the_whole_host():
    """A finished member holds nothing: no whole-host reservation (#1659)."""
    census = _census(demand=None)
    for election in census["gang_elections"].values():
        election["member_state"] = "terminal"
    assert _blocked(_row(resources={"cpu": 1, "mem_gb": 1}), census=census) is None
    assert reservation.reservation_priority_on(census, host="sparky", now=OLD, authority=True) is None

def test_a_small_row_that_fits_beside_the_member_is_admitted():
    assert _blocked(_row(), held={"cpu": 2, "mem_gb": 10}) is None


def test_a_small_row_is_held_once_running_work_has_used_the_slack():
    held = _blocked(_row(), held={"cpu": 2, "mem_gb": 20})
    assert held is not None and held["reservation"] == {"mem_gb": 1}


def test_an_exempt_movement_node_is_admitted_even_with_the_slack_gone():
    assert _blocked(_row(), held={"cpu": 2, "mem_gb": 119}, exempt=True) is None


@pytest.mark.parametrize("value", [False, "true", 1, None, {}])
def test_only_a_true_exemption_exempts(value):
    assert _blocked(_row(), held={"cpu": 2, "mem_gb": 119}, exempt=value) is not None


@pytest.mark.parametrize("role", ["returns_capacity", "serves_residency"])
def test_a_role_mark_on_the_row_alone_exempts_nothing(role):
    """The rule never reads the mark: the caller decides it against its own host's copy."""
    held = _blocked(_row(**{role: True}), held={"cpu": 2, "mem_gb": 119})
    assert held is not None and held["reservation"]


@pytest.mark.parametrize("resources", [
    {"mem_gb": 1},                      # no cpu: unknown CPU, taken as the whole host
    {"cpu": 1},                         # no mem_gb
    {"cpu": "1", "mem_gb": 1},          # unreadable
    {"cpu": -1, "mem_gb": 1}, {"cpu": True, "mem_gb": 1}, None, "x", []])
def test_unknown_or_unreadable_demand_is_consuming_never_exempt(resources):
    assert _blocked(_row(resources=resources)) is not None


def test_a_row_without_a_gpu_key_demands_no_gpu_but_a_gpu_row_without_one_is_unknown():
    assert _blocked(_row(resources={"cpu": 1, "mem_gb": 1})) is None
    assert _blocked(_row(needs_gpu=True, resources={"cpu": 1, "mem_gb": 1})) is not None


def test_an_unknown_member_demand_reserves_the_whole_host():
    for demand in (None, "x", {"cpu": "2"}, {"cpu": -1}):
        held = _blocked(_row(), demand=demand)
        # the whole host is reserved, so a 1 cpu / 1 GB row overruns cpu and memory
        assert held is not None and set(held["reservation"]) == {"cpu", "mem_gb"}, demand


def test_a_member_that_demands_nothing_on_this_host_reserves_nothing():
    assert _blocked(_row(needs_gpu=True, resources={"cpu": 1, "gpu": 1, "mem_gb": 8}),
                    demand={"stage_gib@t:h": 4}) is None


def test_an_unreadable_host_ledger_fails_safe():
    for held, capacity in ((None, CAPACITY), ({}, None), ("x", CAPACITY)):
        blocked = reservation.gang_blocking(
            _census(), _row(), host="sparky", group=None, now=OLD, held=held, capacity=capacity,
            authority=True)
        assert blocked is not None and "unknown" in blocked["reservation"]


def test_a_malformed_held_count_is_consuming():
    assert _blocked(_row(), held={"cpu": "x", "mem_gb": 0}) is not None


def test_strictly_lower_priority_is_fenced_from_election_without_waiting():
    blocked = _blocked(_row(priority=-20), now=FIRST + 1)
    assert blocked is not None and "reservation" not in blocked
    # and an exempt returner of lower priority is still fenced: only equal priority reserves
    assert _blocked(_row(priority=-20), now=FIRST + 1, exempt=True) is not None


def test_higher_priority_own_members_other_hosts_and_canary_are_never_held():
    big = {"cpu": 8, "gpu": 1, "mem_gb": 64}
    assert _blocked(_row(priority=0, resources=big)) is None
    assert reservation.gang_blocking(_census(), _row(resources=big), host="sparky", group="g" * 32,
                                     now=OLD, held={}, capacity=CAPACITY, authority=True) is None
    assert reservation.gang_blocking(_census(), _row(resources=big, gang={"group": "h" * 32}),
                                     host="sparky", group="h" * 32, now=OLD, held={},
                                     capacity=CAPACITY, authority=True) is None
    assert reservation.gang_blocking(_census(), _row(resources=big), host="sparklina", group=None,
                                     now=OLD, held={}, capacity=CAPACITY, authority=True) is None
    canary = _row(needs_gpu=True, resources=big,
                  publication_canary={"host": "sparky", "generation": "g", "run_id": "r"})
    assert _blocked(canary) is None


def test_a_missing_rank_does_not_reserve():
    census = _census()
    next(iter(census["gang_elections"].values()))["rank"] = None
    assert _blocked(_row(resources={"cpu": 8, "mem_gb": 64}), census=census) is None


# --- measurement precedence ---------------------------------------------------------

def test_the_reservation_priority_is_the_gangs_and_only_past_the_bound_on_its_host():
    census = _census()
    assert reservation.reservation_priority_on(census, host="sparky", now=YOUNG, authority=True) is None
    assert reservation.reservation_priority_on(census, host="sparky", now=OLD, authority=True) == -10
    assert reservation.reservation_priority_on(census, host="sparklina", now=OLD, authority=True) is None
    assert reservation.reservation_priority_on({}, host="sparky", now=OLD, authority=True) is None


# --- the fallback: no protected copy on this host ---------------------------------

@pytest.mark.parametrize("authority", [False, None, "yes", 1])
def test_a_host_without_the_protected_copy_reserves_nothing_for_equal_priority(authority):
    """Main's behaviour: equal priority work is never held, whatever it demands."""
    big = {"cpu": 8, "gpu": 1, "mem_gb": 64}
    for now in (YOUNG, OLD, OLD + 10 * BOUND):
        assert _blocked(_row(needs_gpu=True, resources=big), now=now, authority=authority) is None
    assert reservation.reservation_priority_on(
        _census(), host="sparky", now=OLD, authority=authority) is None


def test_a_caller_that_says_nothing_about_authority_reserves_nothing():
    """The default is main's behaviour: a reservation needs the caller to vouch for the host."""
    assert reservation.gang_blocking(_census(), _row(resources={"cpu": 8, "mem_gb": 64}),
                                     host="sparky", group=None, now=OLD, held={},
                                     capacity=CAPACITY) is None


def test_a_host_without_the_protected_copy_still_fences_strictly_lower_priority():
    """The fence of #1517 does not depend on the copy."""
    blocked = _blocked(_row(priority=-20), now=FIRST + 1, authority=False)
    assert blocked is not None and "reservation" not in blocked


@pytest.mark.parametrize("first, now, authority, expected", [
    (FIRST, FIRST + BOUND + 1, True, True),
    (FIRST, FIRST + BOUND, True, False),          # the bound is exclusive
    (FIRST, FIRST + BOUND + 1, False, False),
    (FIRST, FIRST + BOUND + 1, None, False),
    (None, FIRST + BOUND + 1, True, False),
    (True, FIRST + BOUND + 1, True, False),       # a bool is no publication time
    ("1000", FIRST + BOUND + 1, True, False),
])
def test_a_gang_reserves_when_it_has_waited_and_the_host_holds_the_copy(first, now, authority, expected):
    assert reservation.reserves_after(first, now, authority=authority) is expected


# --- the timeline -------------------------------------------------------------------

CLASSES = [
    ("gpu single", {"cpu": 2, "gpu": 1, "mem_gb": 8}, True, False, 1800),
    ("cpu single", {"cpu": 8, "mem_gb": 32}, False, False, 1800),
    ("small cpu", {"cpu": 1, "mem_gb": 2}, False, False, 1500),
    ("egress", {"cpu": 1, "mem_gb": 1}, False, True, 120),
    ("unknown cpu", {"mem_gb": 1}, False, False, 600),
]


def _fits(held, row, capacity):
    return all(held.get(kind, 0) + row.get(kind, 0) <= capacity[kind] for kind in capacity)


@pytest.mark.parametrize("seed", range(60))
def test_the_gang_is_admissible_within_the_bound_plus_the_longest_running_job(seed):
    """Seeded timeline over the real rule.

    Running jobs hold the host at the moment the reservation starts; arrivals of
    every row class keep coming every 20 s.  An arrival is admitted iff the real
    rule lets it through and it fits the free ledger.  The member is admissible
    once ``held + member <= capacity``.  That must happen within the longest
    remaining running job plus one returner's run (the declared returners the
    rule never holds, each at most two minutes, arriving once a minute).
    """
    rng = random.Random(seed)
    census = _census()
    running = []     # (end, resources)
    held = {"cpu": 0, "gpu": 0, "mem_gb": 0}

    def add(resources, end):
        running.append((end, resources))
        for kind, count in resources.items():
            held[kind] = held.get(kind, 0) + count

    # Work admitted before the reservation: fill the host to a random level.
    for _ in range(rng.randrange(1, 6)):
        _, resources, _, _, run = rng.choice(CLASSES[:3])
        if _fits(held, resources, CAPACITY):
            add(resources, OLD + rng.uniform(60, run))
    longest = max([end for end, _ in running], default=OLD) - OLD
    admissible_at = None
    now = OLD
    while now < OLD + 3 * 1800 and admissible_at is None:
        for end, resources in [r for r in running if r[0] <= now]:
            running.remove((end, resources))
            for kind, count in resources.items():
                held[kind] -= count
        if all(held.get(kind, 0) + MEMBER.get(kind, 0) <= CAPACITY[kind] for kind in MEMBER):
            admissible_at = now
            break
        for _ in range(rng.randrange(1, 4)):
            name, resources, gpu, returns, run = rng.choice(CLASSES)
            if returns and int(now) % 60:
                continue
            row = _row(resources=dict(resources), needs_gpu=gpu)
            if _blocked(row, now=now, held=dict(held), census=census, exempt=returns) is None and _fits(
                    held, resources, CAPACITY):
                add(resources, now + rng.uniform(30, run))
        now += 20
    assert admissible_at is not None, f"seed {seed}: the gang never became admissible"
    assert admissible_at - OLD <= longest + 120 + 20, (seed, admissible_at - OLD, longest)


# --- malformed sealed actions are nothing (the role cases themselves are in the claims file) ---

def test_a_malformed_sealed_action_has_no_role():
    """Shape only: a definition that is not a movement node derives nothing, on any host."""
    small = {"cpu": 1, "mem_gb": 1}
    for action in ({}, {"params": {}}, {"params": {"command": ["/usr/bin/python3", "x.py"]}},
                   {"params": {"command": None}, "task": {}},
                   {"params": {"command": ["/usr/bin/python3", 5]}, "task": {"argv": []}},
                   {"params": {"command": "stage_release.py"}, "task": {"result_path": "r"}},
                   None, "stage_release.py"):
        assert movement_actions.capacity_role(action if isinstance(action, dict) else {}, small,
                                              residency=None) is None
    assert movement_actions.capacity_role({"params": {"command": ["/p/python3", "/x/stage_release.py"]},
                                           "task": {"result_path": "r", "argv": []}},
                                          {"cpu": 1, "mem_gb": 1, "gpu": 1}, residency=None) is None
