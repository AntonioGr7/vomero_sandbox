"""Shared timing helper for the examples.

Wraps an example's ``main()`` to report the wall-clock time from sandbox setup to
the end of the run — useful for comparing cold-start + work across examples (and
for seeing how much warming the pool actually costs). Most of the elapsed time is
pod scheduling + image pull on first run, not the snippets themselves.

Import-safe: when you run ``python examples/<name>.py`` the examples directory is
on ``sys.path``, so ``from _timing import timed`` resolves with no install.
"""

from __future__ import annotations

import functools
import os
import sys
import time
from contextlib import contextmanager


def _script_label() -> str:
    return os.path.basename(sys.argv[0]) or "example"


@contextmanager
def timeit(label: str | None = None):
    """Time the enclosed block and print the elapsed wall-clock seconds."""
    label = label or _script_label()
    start = time.monotonic()
    try:
        yield
    finally:
        print(f"\n⏱  {label}: {time.monotonic() - start:.2f}s (sandbox setup → done)")


def timed(fn):
    """Decorator form of :func:`timeit` — wrap an example's ``main()`` to time it
    from the first line (where the sandbox is defined) through to the end."""
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        with timeit():
            return fn(*args, **kwargs)
    return wrapper
