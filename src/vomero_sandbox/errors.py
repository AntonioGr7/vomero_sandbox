"""Exceptions raised by vomero_sandbox.

The split that matters to callers: a run that *executes* but exits non-zero or
times out is NOT an exception — it returns a RunResult you inspect (the user's
code failing is a normal outcome). Exceptions are reserved for infrastructure
problems: the cluster is unreachable, a worker won't start, the pool is closed.
"""

from __future__ import annotations


class SandboxError(Exception):
    """Base class for all vomero_sandbox errors (infrastructure failures)."""


class SandboxConfigError(SandboxError):
    """The SandboxConfig is invalid (bad values, conflicting options)."""


class SandboxStartupError(SandboxError):
    """A worker pod failed to reach the Running state within its budget
    (image pull failure, scheduling failure, rejected by admission, etc.)."""


class SandboxPoolClosed(SandboxError):
    """run() was called after the pool was closed."""


class SandboxConfigWarning(UserWarning):
    """A SandboxConfig is valid but likely not what you want (e.g. a resource
    request far below its limit, which overcommits shared nodes). Emitted via
    ``warnings.warn`` — silence or escalate it with the standard ``warnings``
    filters if you've made the choice deliberately."""
