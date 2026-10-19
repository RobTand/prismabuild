"""Captured runtime receipts, selected member integrity and source declarations.

Filesystem custody, read budgets and publication authority belong to callers.
"""
from __future__ import annotations

import ast
from dataclasses import dataclass
import json
from pathlib import Path
import re
from typing import Literal

if __package__:
    from .digest_primitives import raw_sha256, stream_sha256
else:
    from digest_primitives import raw_sha256, stream_sha256


RUNTIME_SCHEMA = "prismaquant.prismabuild.runtime_version.v1"
_MISSING_DECLARATION = object()


@dataclass(frozen=True)
class GenerationReceipt:
    """One decoded observation; construction and questions never reread it."""

    document: object

    @classmethod
    def capture(cls, raw: bytes | str) -> GenerationReceipt:
        return cls(json.loads(raw))

    @classmethod
    def read(cls, path: Path) -> GenerationReceipt:
        return cls.capture(path.read_text(encoding="utf-8"))

    def require_generation(self, generation: str) -> dict:
        value = self.document
        if (not isinstance(value, dict) or value.get("schema") != RUNTIME_SCHEMA
                or value.get("generation") != generation
                or not isinstance(value.get("files"), dict)):
            raise ValueError("invalid generation receipt")
        return value

    def require_commit(self) -> object:
        value = self.document
        if (not isinstance(value, dict)
                or re.fullmatch(r"[0-9a-f]{40}", str(value.get("commit", ""))) is None):
            raise ValueError("invalid generation receipt")
        return value["commit"]

    def require_identity(self, generation: str) -> dict:
        value = self.document
        if value.get("schema") != RUNTIME_SCHEMA or value.get("generation") != generation:
            raise ValueError("invalid generation receipt")
        self.require_commit()
        return value

    @property
    def files(self) -> dict:
        return self.document["files"]

    @staticmethod
    def member_path(root: Path, relative: str) -> Path:
        path = Path(relative)
        if not path.parts or path.is_absolute() or any(part in (".", "..") for part in path.parts):
            raise ValueError("unsafe generation member path")
        return root / relative

    def member_digest(self, relative: str) -> str:
        expected = self.files.get(relative)
        if not isinstance(expected, str) or re.fullmatch(r"[0-9a-f]{64}", expected) is None:
            raise ValueError(f"generation receipt covers no sha256 for {relative}")
        return expected

    def matches_bytes(self, relative: str, data: bytes) -> bool:
        return raw_sha256(data) == self.files.get(relative)

    def matches_file(self, relative: str, path: Path) -> bool:
        return stream_sha256(path) == self.files.get(relative)

    def source_declarations(self, relative: str, data: bytes) -> SourceDeclarations:
        if not self.matches_bytes(relative, data):
            raise ValueError(f"generation source differs from its receipt: {relative}")
        return SourceDeclarations.capture(data)


@dataclass(frozen=True)
class SourceDeclarations:
    """Literal declarations from captured source, without executing target code."""

    tree: ast.Module

    @classmethod
    def capture(cls, source: bytes) -> SourceDeclarations:
        try:
            return cls(ast.parse(source))
        except (SyntaxError, ValueError, UnicodeError) as exc:
            raise ValueError("invalid capability source") from exc

    def _literal(self, name: str) -> object:
        bindings = [node for node in ast.walk(self.tree)
                    if isinstance(node, ast.Name) and node.id == name
                    and isinstance(node.ctx, (ast.Store, ast.Del))]
        declarations = [node for node in self.tree.body
                        if isinstance(node, (ast.Assign, ast.AnnAssign))
                        and ((isinstance(node, ast.Assign) and len(node.targets) == 1
                              and isinstance(node.targets[0], ast.Name) and node.targets[0].id == name)
                             or (isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
                                 and node.target.id == name))]
        other_bindings = [node for node in ast.walk(self.tree)
                          if (isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
                              and node.name == name)
                          or (isinstance(node, ast.arg) and node.arg == name)
                          or (isinstance(node, ast.alias) and (node.asname or node.name) == name)
                          or (isinstance(node, ast.ExceptHandler) and node.name == name)]
        if not bindings and not other_bindings:
            return _MISSING_DECLARATION
        if len(bindings) != 1 or len(declarations) != 1 or other_bindings:
            raise ValueError(f"ambiguous capability declaration: {name}")
        try:
            return ast.literal_eval(declarations[0].value)
        except (ValueError, TypeError, SyntaxError) as exc:
            raise ValueError(f"nonliteral capability declaration: {name}") from exc

    def protocol(self, name: str) -> int | None:
        value = self._literal(name)
        if value is _MISSING_DECLARATION:
            return None
        if type(value) is not int or value < 1:
            raise ValueError(f"invalid capability declaration: {name}")
        return value

    def members(self, name: str) -> dict[str, str] | None:
        value = self._literal(name)
        if value is _MISSING_DECLARATION:
            return None
        if (not isinstance(value, dict) or any(not isinstance(key, str) or not isinstance(path, str)
                                             for key, path in value.items())):
            raise ValueError(f"invalid member declaration: {name}")
        return value


@dataclass(frozen=True)
class RuntimeIdentity:
    commit: str
    generation: str


@dataclass(frozen=True)
class ReceiptUnavailable:
    reason: Literal["missing", "unreadable", "malformed", "incomplete"]


ReceiptObservation = RuntimeIdentity | ReceiptUnavailable


@dataclass(frozen=True)
class Match:
    identity: RuntimeIdentity


@dataclass(frozen=True)
class Drift:
    loaded: RuntimeIdentity
    published: RuntimeIdentity


@dataclass(frozen=True)
class Unknown:
    loaded: ReceiptObservation
    published: ReceiptObservation


ReceiptDecision = Match | Drift | Unknown


def observe(path: Path) -> ReceiptObservation:
    try:
        value = GenerationReceipt.read(path).document
    except FileNotFoundError:
        return ReceiptUnavailable("missing")
    except OSError:
        return ReceiptUnavailable("unreadable")
    except ValueError:
        return ReceiptUnavailable("malformed")
    if not isinstance(value, dict):
        return ReceiptUnavailable("malformed")
    commit = value.get("commit")
    generation = value.get("generation")
    if not isinstance(commit, str) or not commit or not isinstance(generation, str) or not generation:
        return ReceiptUnavailable("incomplete")
    return RuntimeIdentity(commit, generation)


def compare(loaded: ReceiptObservation, published: ReceiptObservation) -> ReceiptDecision:
    if isinstance(loaded, ReceiptUnavailable) or isinstance(published, ReceiptUnavailable):
        return Unknown(loaded, published)
    return Match(loaded) if loaded == published else Drift(loaded, published)
