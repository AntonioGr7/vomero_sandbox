"""Filesystem state across runs, and how to control it.

The working dir (/scratch) lives for the WORKER's lifetime, with two
consequences you must design around:

  - State persists across runs on the SAME worker (until it's retired after
    max_uses / max_age_s, or the pool closes).
  - Routing is NOT sticky: consecutive runs may land on different workers.

So you can neither rely on state carrying over nor assume a clean slate — unless
you configure for it. Two knobs decide the model:

  - fresh_workdir_per_run (default True): wipe the workspace before each run, so
    a run never sees a prior run's files even on a reused worker. Per-run
    filesystem isolation while keeping warm-pool reuse.
  - max_uses=1: retire a worker after a single run — the strongest isolation
    (a brand-new pod per run), at the cost of warm-pool speed.

Needs a cluster (see examples/README.md).
"""

from vomero_sandbox import SandboxPool, SandboxConfig

from _timing import timed

WRITE = "open('state.txt', 'a').write('x'); print('wrote')"
READ = ("import os; "
        "print('len', len(open('state.txt').read()) if os.path.exists('state.txt') else 'MISSING')")


def demo(title: str, cfg: SandboxConfig) -> None:
    print(f"\n=== {title} ===")
    # pool_size=1 forces every run onto the same worker, so the only thing
    # varying is the isolation policy — not which worker we happened to hit.
    with SandboxPool(cfg) as pool:
        for i in range(3):
            pool.run(WRITE)
            r = pool.run(READ)
            print(f"  run {i}: {r.stdout.strip()}")


@timed
def main() -> None:
    # Default: each run starts with an empty workspace -> always 'len 1'.
    demo("fresh_workdir_per_run=True (default)",
         SandboxConfig(pool_size=1, fresh_workdir_per_run=True))

    # Persist across runs on the worker -> the file grows: len 2, 4, 6...
    demo("fresh_workdir_per_run=False (state accumulates)",
         SandboxConfig(pool_size=1, fresh_workdir_per_run=False))

    # max_uses=1 retires the worker after every run -> brand-new pod each time,
    # the strongest isolation for untrusted, multi-tenant input.
    demo("max_uses=1 (a fresh pod per run)",
         SandboxConfig(pool_size=1, max_uses=1, fresh_workdir_per_run=False))


if __name__ == "__main__":
    main()
