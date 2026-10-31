"""Hosted nightly planning and artifact adapters around the pure reporter."""
from pathlib import Path


def cpu_plan(root, lanes, sha):
    raise NotImplementedError


def combine_architectures(rows, architectures):
    raise NotImplementedError


def collect_shards(directory, manifest):
    raise NotImplementedError
