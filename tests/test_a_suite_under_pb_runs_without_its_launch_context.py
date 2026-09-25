"""A test sees none of the launch context of the action running the suite.

The suite runs as an admitted PrismaBuild action, so the pool launcher's
context -- the action key, the residency map and, since #961, the queue root
(``PRISMABUILD_QUEUE_ROOT``, the live queue) -- is in the environment of every
test.  ``reader_lease.launch_queue_root()`` reads ``os.environ`` by default,
so a test that asks it which queue launched it would answer the fleet's live
queue.  A standalone box has none of these, and every test must pass there;
under PB each test must see the same.
"""

from __future__ import annotations

import os

from prismabuild import core as pb
from prismabuild import reader_lease


def test_no_launch_context_name_reaches_a_test() -> None:
    present = sorted(name for name in pb.ACTION_RESIDENCY_ENV
                     if name in os.environ)
    assert present == []


def test_a_test_is_not_told_the_live_queue_launched_it() -> None:
    assert reader_lease.launch_queue_root() is None
