#!/usr/bin/env python3
"""Classify repeated hosted main test results without retrying test failures."""
from __future__ import annotations

import importlib.util
from pathlib import Path

SPEC = importlib.util.spec_from_file_location('census_nightly', Path(__file__).with_name('full_suite_workflow.py'))
nightly = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(nightly)
report = nightly.report
ARCHITECTURES = {'x64': 'ubuntu-24.04', 'arm64': 'ubuntu-24.04-arm'}


def classify_samples(samples, repeats, quarantine):
    return {}


def census_issue_plans(result, issues):
    return []


def file_new_issues(api, plans):
    return []


def census_manifest(root, sha, repeats, shard_count):
    return {}


def collect_census(directory, manifest, run_url):
    return []
