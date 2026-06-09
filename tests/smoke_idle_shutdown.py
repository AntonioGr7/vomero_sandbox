"""End-to-end smoke test for the idle self-termination watchdog (issue: leaked
workers when the controller dies).

It exercises the ONE guard that runs cluster-side — the in-pod watchdog from
`idle_shutdown_s` — against a real cluster, by reading pod phase straight from the
Kubernetes API:

  1. self-termination: warm a worker with a short idle TTL, touch nothing, and
     assert the pod reaches **Succeeded** (the watchdog exited 0) within the TTL.
  2. activity resets the timer: keep running commands faster than the TTL and
     assert the worker stays **Running** the whole time (never self-terminates
     under load).

This needs a real cluster (kind/minikube — see examples/README.md). It uses a
dedicated app_label so it can never touch your real workers. Run it directly:

    python tests/smoke_idle_shutdown.py          # exits 0 on pass, 1 on failure

It also works under pytest (`pytest tests/smoke_idle_shutdown.py`); the test
functions raise AssertionError on failure. Both will error out if no cluster is
reachable — that's expected for an integration test.
"""

from __future__ import annotations

import sys
import time

from kubernetes import client, config as kube_config

from vomero_sandbox import SandboxConfig, SandboxPool

# Short TTL so the test runs in well under a minute, not the 30-min default.
IDLE_TTL_S = 15
# A label only this test uses — keeps the reclaim/delete blast radius off your
# real workers if both run in the same namespace.
SMOKE_LABEL = "vomero-smoketest"
NAMESPACE = "sandbox"


def _api() -> client.CoreV1Api:
    try:
        kube_config.load_incluster_config()
    except kube_config.ConfigException:
        kube_config.load_kube_config()
    return client.CoreV1Api()


def _worker_phase(api: client.CoreV1Api) -> str:
    """Phase of the single smoke-test worker, or 'Gone' if no pod matches."""
    sel = f"app={SMOKE_LABEL},role=worker"      # role label is _pod.WORKER_ROLE
    pods = api.list_namespaced_pod(NAMESPACE, label_selector=sel).items
    if not pods:
        return "Gone"
    return pods[0].status.phase or "Unknown"


def _wait_for_phase(api: client.CoreV1Api, target: str, budget_s: float) -> bool:
    """Poll until the worker reaches `target` phase, logging each transition.
    Returns True if reached within budget_s."""
    deadline = time.monotonic() + budget_s
    last = None
    while time.monotonic() < deadline:
        phase = _worker_phase(api)
        if phase != last:
            print(f"    [{time.monotonic() - (deadline - budget_s):5.1f}s] phase = {phase}")
            last = phase
        if phase == target:
            return True
        time.sleep(2)
    return False


def test_idle_self_termination() -> None:
    """An untouched worker self-terminates (-> Succeeded) within ~IDLE_TTL_S."""
    print(f"\n=== self-termination: worker idle TTL = {IDLE_TTL_S}s ===")
    api = _api()
    cfg = SandboxConfig(pool_size=1, idle_shutdown_s=IDLE_TTL_S,
                        app_label=SMOKE_LABEL, namespace=NAMESPACE)
    pool = SandboxPool(cfg)
    pool.start()
    try:
        assert _worker_phase(api) == "Running", "worker should be Running right after start()"
        print("    worker is Running; now leaving it idle (simulating a dead controller)")

        # Watchdog polls every ~TTL/4; allow the TTL plus generous slack.
        reached = _wait_for_phase(api, "Succeeded", budget_s=IDLE_TTL_S + 45)
        assert reached, (
            f"worker did not self-terminate within {IDLE_TTL_S + 45}s — the idle "
            "watchdog is not reaping abandoned workers")
        print("    PASS: idle worker self-terminated (Succeeded)")
    finally:
        pool.close()


def test_activity_resets_idle_timer() -> None:
    """A worker under steady load stays Running well past the idle TTL."""
    print(f"\n=== activity resets the timer: pinging every {IDLE_TTL_S // 2}s ===")
    api = _api()
    cfg = SandboxConfig(pool_size=1, idle_shutdown_s=IDLE_TTL_S,
                        app_label=SMOKE_LABEL, namespace=NAMESPACE)
    pool = SandboxPool(cfg)
    pool.start()
    try:
        interval = max(2, IDLE_TTL_S // 2)
        run_for = IDLE_TTL_S * 2 + 5          # cross the TTL boundary several times
        deadline = time.monotonic() + run_for
        pings = 0
        while time.monotonic() < deadline:
            r = pool.run("print('ping')")
            assert r.ok, f"ping run failed: {r.stderr}"
            pings += 1
            phase = _worker_phase(api)
            assert phase == "Running", (
                f"worker left Running ({phase}) under load — the watchdog is reaping "
                "an actively-used worker")
            time.sleep(interval)
        print(f"    PASS: worker stayed Running across {pings} pings over ~{run_for}s")
    finally:
        pool.close()


def main() -> int:
    print(f"Idle-watchdog smoke test (TTL={IDLE_TTL_S}s, label={SMOKE_LABEL}, ns={NAMESPACE})")
    try:
        test_idle_self_termination()
        test_activity_resets_idle_timer()
    except AssertionError as e:
        print(f"\nFAIL: {e}")
        return 1
    except Exception as e:                    # cluster unreachable, RBAC, etc.
        print(f"\nERROR (is a cluster reachable?): {type(e).__name__}: {e}")
        return 1
    print("\nAll idle-watchdog smoke checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
