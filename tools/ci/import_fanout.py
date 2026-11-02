#!/usr/bin/env python3
"""Compatibility exports for PrismaBuild's Bazel graph import restrictions."""
from tools.ci.bazel_imports import bazel_imports, bazel_regressions

__all__ = ['bazel_imports', 'bazel_regressions']
