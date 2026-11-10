"""A small tracer: timed, nested spans for every step of a run, written as JSON lines.

Enable it with ``--trace trace.jsonl`` on the CLI, ``SQL2ICEBERG_TRACE=trace.jsonl`` in the environment, or in code:

    from sql2iceberg.tracing import Tracer, set_tracer
    set_tracer(Tracer("trace.jsonl"))

With no tracer configured every call is a no-op. Attribute values whose key looks secret (password, token,
key, secret, credential) and anything that looks like a bearer/access token are replaced with ``***`` before
they are stored, so a trace file is safe to attach to a GitHub issue after a quick read.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

_SECRET_KEY = re.compile(r"pass(word)?|token|secret|credential|private|(^|_)key($|_)", re.IGNORECASE)
_SECRET_VALUE = re.compile(r"(ya29\.[\w.-]+|AKIA[0-9A-Z]{16}|-----BEGIN [A-Z ]*PRIVATE KEY-----|eyJ[\w-]{20,}\.[\w-]+\.[\w-]+)")


def redact(key: str, value):
    if _SECRET_KEY.search(key):
        return "***"
    if isinstance(value, str):
        return _SECRET_VALUE.sub("***", value)[:500]
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return _SECRET_VALUE.sub("***", str(value))[:500]


@dataclass
class Span:
    id: str
    parent: str | None
    name: str
    start: float
    attrs: dict = field(default_factory=dict)
    duration_ms: float = 0.0
    status: str = "ok"
    error: str = ""

    def set(self, **attrs) -> None:
        self.attrs.update({k: redact(k, v) for k, v in attrs.items()})

    def to_dict(self) -> dict:
        d = {"id": self.id, "parent": self.parent, "name": self.name, "start": round(self.start, 3),
             "duration_ms": round(self.duration_ms, 1), "status": self.status, "attrs": self.attrs}
        if self.error:
            d["error"] = self.error
        return d


class Tracer:
    def __init__(self, path: str | Path | None = None, run_id: str | None = None) -> None:
        self.path = Path(path) if path else None
        self.run_id = run_id or uuid.uuid4().hex[:12]
        self.spans: list[Span] = []
        self._local = threading.local()
        self._lock = threading.Lock()
        if self.path:
            self.path.parent.mkdir(parents=True, exist_ok=True)

    def _stack(self) -> list[Span]:
        if not hasattr(self._local, "stack"):
            self._local.stack = []
        return self._local.stack

    @contextlib.contextmanager
    def span(self, name: str, **attrs):
        stack = self._stack()
        sp = Span(uuid.uuid4().hex[:8], stack[-1].id if stack else None, name, time.time())
        sp.set(**attrs)
        stack.append(sp)
        t0 = time.perf_counter()
        try:
            yield sp
        except BaseException as exc:
            sp.status, sp.error = "error", f"{type(exc).__name__}: {exc}"[:500]
            raise
        finally:
            sp.duration_ms = (time.perf_counter() - t0) * 1000
            stack.pop()
            with self._lock:
                self.spans.append(sp)
                if self.path:
                    with self.path.open("a", encoding="utf-8") as f:
                        f.write(json.dumps({"run": self.run_id, **sp.to_dict()}) + "\n")

    def summary(self) -> str:
        rows = ["| step | status | ms |", "|---|---|---|"]
        for sp in sorted(self.spans, key=lambda s: s.start):
            depth = 0
            p = sp.parent
            ids = {s.id: s for s in self.spans}
            while p and p in ids:
                depth += 1
                p = ids[p].parent
            rows.append(f"| {'&nbsp;&nbsp;' * depth}{sp.name} | {sp.status} | {sp.duration_ms:.0f} |")
        return f"Trace {self.run_id}\n\n" + "\n".join(rows) + "\n"


class _Noop:
    @contextlib.contextmanager
    def span(self, name: str, **attrs):
        yield Span("", None, name, time.time())


_current: Tracer | _Noop | None = None


def set_tracer(tracer: Tracer | None) -> None:
    global _current
    _current = tracer


def get_tracer() -> Tracer | _Noop:
    global _current
    if _current is None:
        path = os.environ.get("SQL2ICEBERG_TRACE")
        _current = Tracer(path) if path else _Noop()
    return _current


def span(name: str, **attrs):
    """``with span("step", table="orders") as sp: ... sp.set(rows=3)``"""
    return get_tracer().span(name, **attrs)
