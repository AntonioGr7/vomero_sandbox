"""Not leaking worker pods when the controlling process dies.

Workers are pods on the CLUSTER, not children of your process. If the process
that owns a pool exits without calling close(), those pods keep running. There
are three guards (see the README's "Cleanup & leaked workers"); this script
shows each and how to verify it.

Throughout, watch the cluster in another terminal to SEE the effect:

    kubectl get pods -n sandbox -l app=vomero-sandbox -w

Needs a cluster (see examples/README.md).
"""

import time

from vomero_sandbox import SandboxConfig, SandboxPool

from _timing import timed


def graceful_with_block() -> None:
    """The baseline: `with` (or try/finally) closes the pool on exception or exit.
    Covers everything except a hard kill (SIGKILL) of the process."""
    print("=== 1. `with` block — workers deleted on exit, even on error ===")
    try:
        with SandboxPool(SandboxConfig(pool_size=2)) as pool:
            print("   workers warm:", pool.stats()["placed"])
            raise RuntimeError("boom — simulating a mid-run crash")
    except RuntimeError as e:
        print("   caught:", e)
    print("   -> on the way out of `with`, close() ran; pods are being deleted")


def auto_cleanup_on_signal() -> None:
    """auto_cleanup (default True) installs atexit + SIGTERM handlers, so even
    without a `with` block a normal exit or `kill <pid>` still closes the pool.

    To see the SIGTERM path: while this function's sleep is running, from another
    terminal run `kill <pid>` (NOT -9) against this process — the handler closes
    the pool before the interpreter exits. A plain process exit triggers the same
    cleanup via atexit."""
    print("\n=== 2. auto_cleanup — atexit + SIGTERM close the pool ===")
    pool = SandboxPool(SandboxConfig(pool_size=1))   # no `with`
    pool.start()
    print(f"   started without `with`; auto_cleanup={pool.config.auto_cleanup}")
    print("   try `kill <this_pid>` now to watch SIGTERM clean up; otherwise it")
    print("   cleans up via atexit when this script ends.")
    time.sleep(2)
    # Not calling close() on purpose — atexit/SIGTERM is the safety net here.


def idle_self_termination() -> None:
    """The only CLUSTER-SIDE guard: each worker self-terminates after it sits idle
    for idle_shutdown_s, so a SIGKILL'd or vanished controller still can't leak
    pods forever. Here we set a tiny TTL to watch it happen quickly.

    Normally leave idle_shutdown_s at its 30-min default — this short value is
    just so the demo doesn't take half an hour."""
    print("\n=== 3. idle_shutdown_s — workers self-terminate when abandoned ===")
    cfg = SandboxConfig(pool_size=1, idle_shutdown_s=20)   # tiny TTL for the demo
    pool = SandboxPool(cfg)
    pool.start()
    name = pool.stats()
    print(f"   worker warm with a {cfg.idle_shutdown_s}s idle TTL: {name['placed']} placed")
    print("   imagine this process is now SIGKILL'd (no cleanup possible).")
    print("   watch `kubectl get pods` — with no runs touching it, the worker")
    print(f"   will exit (-> Completed/Succeeded) ~{cfg.idle_shutdown_s}s after the last activity.")
    # We *do* close() here so the demo cleans up promptly; in a real SIGKILL the
    # watchdog is what reaps the pod.
    pool.close()


def reclaim_orphans() -> None:
    """reclaim_on_start: a fresh pool deletes pre-existing workers with the same
    app_label before warming, sweeping up orphans a previous crashed run left.
    Enable ONLY when an app_label maps to a single pool (else it deletes live
    peers' workers)."""
    print("\n=== 4. reclaim_on_start — sweep a crashed run's orphans at startup ===")
    cfg = SandboxConfig(pool_size=1, reclaim_on_start=True)
    with SandboxPool(cfg) as pool:
        print("   start() first deleted any stale app=vomero-sandbox workers,")
        print("   then warmed a clean pool:", pool.stats()["placed"], "worker(s)")


@timed
def main() -> None:
    graceful_with_block()
    auto_cleanup_on_signal()
    idle_self_termination()
    reclaim_orphans()
    print("\nManual sweep any time:")
    print("   kubectl delete pod -n sandbox -l app=vomero-sandbox,role=worker")


if __name__ == "__main__":
    main()
