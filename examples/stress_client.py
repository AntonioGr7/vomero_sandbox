"""Load generator for stress_server.py — change the parameters here to stress the
pool from the outside.

Fires ``--jobs`` requests at ``--concurrency`` in flight, each a job for one of
the registered packages with a synthetic input you size with ``--input-size``.
Measures end-to-end latency per request and reports throughput + percentiles, so
you can sweep parameters (concurrency vs. pool_size, payload size, timeout) and
see how the server holds up. The server runs unchanged across your sweeps.

    # against `python stress_server.py --pool-size 5`:
    python examples/stress_client.py --jobs 200 --concurrency 20
    python examples/stress_client.py --jobs 200 --concurrency 50 --package wordcount
    python examples/stress_client.py --jobs 100 --concurrency 10 --input-size 5000

Watch what happens when --concurrency exceeds the server's --pool-size: extra
requests block server-side waiting for a worker, so throughput plateaus at the
pool's capacity and latency climbs — the back-pressure made visible.

Stdlib only (urllib). The server must be running first.
"""

import argparse
import json
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed


def make_payload(package: str, size: int, index: int) -> dict:
    """Build one job's request body. ``size`` scales the synthetic input so you
    can stress-test payload size; ``mix`` alternates packages by request index."""
    if package == "mix":
        package = ("stats", "wordcount")[index % 2]
    if package == "stats":
        return {"package": "stats",
                "files": {"input.json": json.dumps(list(range(size)))}}
    return {"package": "wordcount",
            "files": {"input.txt": " ".join(f"w{i % 64}" for i in range(size))},
            "env": {"RUN_TAG": "stress"}}


def fire_one(url: str, payload: dict, timeout: float) -> tuple[bool, float, str]:
    """POST one job, return (ok, latency_s, note). ``ok`` reflects the sandbox
    run's success; transport/HTTP errors are counted as failures too."""
    body = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=body, method="POST",
                                 headers={"Content-Type": "application/json"})
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read())
        dt = time.monotonic() - t0
        return bool(data.get("ok")), dt, data.get("sandbox_id", "")
    except urllib.error.HTTPError as e:
        return False, time.monotonic() - t0, f"http {e.code}"
    except Exception as e:
        return False, time.monotonic() - t0, f"{type(e).__name__}"


def percentile(sorted_vals: list, q: float) -> float:
    if not sorted_vals:
        return 0.0
    i = min(len(sorted_vals) - 1, int(q * len(sorted_vals)))
    return sorted_vals[i]


def main() -> None:
    p = argparse.ArgumentParser(description="Stress-test the sandbox job server.")
    p.add_argument("--url", default="http://127.0.0.1:8099")
    p.add_argument("--jobs", type=int, default=100, help="total requests to send")
    p.add_argument("--concurrency", type=int, default=10, help="requests in flight at once")
    p.add_argument("--package", choices=("stats", "wordcount", "mix"), default="mix")
    p.add_argument("--input-size", type=int, default=100, help="synthetic input size (scales payload)")
    p.add_argument("--timeout", type=float, default=30.0, help="per-request client timeout (s)")
    args = p.parse_args()

    run_url = args.url.rstrip("/") + "/run"
    payloads = [make_payload(args.package, args.input_size, i) for i in range(args.jobs)]

    print(f"firing {args.jobs} jobs at concurrency {args.concurrency} "
          f"(package={args.package}, input-size={args.input_size}) -> {run_url}")

    oks = fails = 0
    latencies: list = []
    wall0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futures = [ex.submit(fire_one, run_url, pl, args.timeout) for pl in payloads]
        for fut in as_completed(futures):
            ok, dt, _note = fut.result()
            latencies.append(dt)
            oks, fails = (oks + 1, fails) if ok else (oks, fails + 1)
    wall = time.monotonic() - wall0

    latencies.sort()
    print(f"\ndone in {wall:.2f}s")
    print(f"  ok={oks}  failed={fails}  throughput={args.jobs / wall:.1f} jobs/s")
    print(f"  latency  p50={percentile(latencies, 0.50):.3f}s  "
          f"p95={percentile(latencies, 0.95):.3f}s  "
          f"p99={percentile(latencies, 0.99):.3f}s  "
          f"max={latencies[-1] if latencies else 0:.3f}s")

    # Pull the server-side pool state for context (how many workers, retirements).
    try:
        with urllib.request.urlopen(args.url.rstrip("/") + "/stats", timeout=5) as resp:
            print("  server stats:", json.loads(resp.read()))
    except Exception as e:
        print("  (could not fetch /stats:", e, ")")


if __name__ == "__main__":
    main()
