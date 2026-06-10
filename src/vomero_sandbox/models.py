"""Result types returned to callers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import NamedTuple


class StreamChunk(NamedTuple):
    """One piece of output yielded by a streaming run, as it is produced.

    ``stream`` is ``"stdout"`` or ``"stderr"``; ``data`` is the text chunk.
    Chunks are NOT aligned to line or token boundaries — they are whatever the
    websocket delivered — so a consumer that needs whole lines should reassemble
    them. Unpackable as a 2-tuple (``for stream, data in run:``) or accessed by
    attribute (``chunk.stream`` / ``chunk.data``).
    """

    stream: str
    data: str


@dataclass(frozen=True)
class RunResult:
    """The outcome of running one snippet in a sandbox.

    A non-zero ``exit_code`` or ``timed_out=True`` is a normal result, not an
    error — it means the *user's code* failed or ran too long, which is exactly
    what a sandbox is for. Inspect these fields rather than relying on
    exceptions for that case.
    """

    stdout: str
    stderr: str
    exit_code: int | None      # 0 = clean; non-zero = the code failed; None = killed before exit
    timed_out: bool            # True if the wall-clock timeout fired (worker was retired)
    duration_s: float          # wall-clock seconds from dispatch to result
    sandbox_id: str            # the worker pod that served this run (for tracing/logs)
    # Output files pulled back from the worker's working dir, when run(...) is
    # called with collect=. Maps relative path -> file bytes. Empty unless asked.
    files: dict[str, bytes] = field(default_factory=dict)
    # Paths skipped because the total would exceed max_collect_bytes (if any).
    files_truncated: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        """True iff the code ran to completion and exited 0."""
        return self.exit_code == 0 and not self.timed_out


@dataclass(frozen=True)
class PoolStartReport:
    """The outcome of warming the pool — returned by ``SandboxPool.start()``.

    On shared, possibly-full nodes the pool may come up SMALLER than requested.
    Every worker counted in ``placed`` was genuinely scheduled by Kubernetes and
    reached Running; ``placed < requested`` means the scheduler (or the namespace
    quota) had no room for the rest, and ``reasons`` records why. Inspect
    ``complete`` to decide whether to proceed, alert, or retry later.
    """

    requested: int             # workers asked for (config.pool_size)
    placed: int                # workers actually scheduled and Running
    reasons: tuple[str, ...] = ()   # scheduler/quota verdicts for the unplaced workers

    @property
    def complete(self) -> bool:
        """True iff the full requested pool was placed."""
        return self.placed >= self.requested

    @property
    def shortfall(self) -> int:
        """How many requested workers could not be placed."""
        return max(0, self.requested - self.placed)
