"""Queue-driven service: warm the pool once, consume jobs that arrive OVER TIME.

This is the realistic shape of a worker service. At boot you create and warm a
SandboxPool. Then a consumer pulls jobs off a message broker (RabbitMQ, SQS, ...)
and runs each one in the sandbox AS IT ARRIVES — not as a batch.

The contrast with use_in_service.py is the point: that example fires a fixed set
of requests all at once. Here jobs land at *different moments* (a trickle off the
queue), and each one kicks off its own run independently. The pool's ``pool_size``
is the concurrency ceiling — when more jobs are in flight than there are workers,
the extra consumers simply block in run_project() until a worker frees, which is
exactly the back-pressure you want.

What we simulate vs. what's real:
  - The BROKER is a stdlib ``queue.Queue`` so the example is self-contained (no
    RabbitMQ to stand up). The consumer loop below is the same shape you'd write
    against pika/aio-pika: block for a message, run it, ack, repeat — swap the
    queue for a channel and the structure is unchanged.
  - A job's PAYLOAD is just a package NAME plus the INPUT to run it on — what you
    would actually put on the wire. The package itself is "saved somewhere": here,
    on disk under examples/package_registry/<name>/ (in production: object
    storage, an artifact registry, a git ref). The service resolves the name to
    that directory and ships it into the sandbox with run_project().

Needs a cluster (see examples/README.md).
"""

import json
import os
import queue
import threading
import time
from dataclasses import dataclass, field

from vomero_sandbox import SandboxPool, SandboxConfig

from _timing import timed

HERE = os.path.dirname(os.path.abspath(__file__))
REGISTRY = os.path.join(HERE, "package_registry")   # where packages are "saved"

POOL_SIZE = 3
N_CONSUMERS = 3          # == pool_size here; raise it to watch back-pressure kick in

# Warm one pool for the whole service lifetime (not per job).
pool = SandboxPool(SandboxConfig(pool_size=POOL_SIZE, max_uses=25, default_timeout_s=15))

_START = time.monotonic()
_STOP = object()         # sentinel put on the queue to stop a consumer


def _t() -> str:
    """Elapsed wall-clock, so the trickle of arrivals is visible in the output."""
    return f"+{time.monotonic() - _START:4.1f}s"


@dataclass
class Job:
    """One message off the queue: a package name, its per-run input files, and any
    env. The input files overlay the package tree in the sandbox working dir."""
    job_id: str
    package: str                            # -> package_registry/<package>/
    input_files: dict                       # {relpath: bytes}, dropped beside the code
    env: dict = field(default_factory=dict)


def publish_jobs(broker: "queue.Queue") -> None:
    """Stand-in for a RabbitMQ producer: enqueue jobs that ARRIVE OVER TIME.

    In production this is your broker delivering messages whenever upstream
    produces them; here we sleep between puts so the consumers see a realistic
    trickle instead of a batch that all lands at t=0.
    """
    jobs = [
        Job("job-1", "stats",     {"input.json": json.dumps([3, 1, 4, 1, 5, 9, 2, 6]).encode()}),
        Job("job-2", "wordcount", {"input.txt": b"the quick brown fox the lazy dog the end"}),
        Job("job-3", "stats",     {"input.json": json.dumps([10, 20, 30]).encode()}),
        Job("job-4", "wordcount", {"input.txt": b"alpha beta beta gamma gamma gamma"},
            {"RUN_TAG": "batch-7"}),
        Job("job-5", "stats",     {"input.json": json.dumps(list(range(101))).encode()}),
    ]
    for job in jobs:
        time.sleep(0.7)                     # messages don't all arrive at once
        print(f"[{_t()}] broker -> {job.job_id} ({job.package})")
        broker.put(job)


def handle(consumer: str, job: Job) -> None:
    """Run one job in the sandbox: resolve its package name to the on-disk tree,
    ship it in with the per-job input overlaid, and collect the artifact."""
    pkg_dir = os.path.join(REGISTRY, job.package)
    result = pool.run_project(
        pkg_dir,
        ["python", "."],                    # convention: each package has a __main__.py
        extra_files=job.input_files,        # the per-job input lands beside the code
        collect=["out.json"],               # just the produced artifact, not the sources
        env=job.env,
        timeout_s=10,
    )
    artifact = result.files.get("out.json", b"").decode().strip() or "—"
    print(f"[{_t()}] {consumer} ran {job.job_id} {job.package:9} "
          f"ok={result.ok} on={result.sandbox_id} artifact={artifact}")


def consume(consumer: str, broker: "queue.Queue") -> None:
    """A consumer loop — the same shape as a pika ``basic_consume`` callback:
    block for a message, run it in the sandbox, 'ack' by moving on. Stops on the
    _STOP sentinel. Concurrency across consumers is bounded by pool_size."""
    while True:
        job = broker.get()
        try:
            if job is _STOP:
                return
            handle(consumer, job)
        except Exception as e:             # a broker callback must not die on one bad job
            print(f"[{_t()}] {consumer} job failed: {e!r}")
        finally:
            broker.task_done()             # 'ack'


@timed
def main() -> None:
    report = pool.start()                  # provision namespace/policy + warm workers, once
    print(f"pool warm: {report.placed}/{report.requested} workers\n")
    broker: "queue.Queue" = queue.Queue()
    try:
        # Start the consumers first — they sit idle, blocked on an empty queue...
        consumers = [
            threading.Thread(target=consume, args=(f"consumer-{i+1}", broker), daemon=True)
            for i in range(N_CONSUMERS)
        ]
        for c in consumers:
            c.start()

        # ...then the producer trickles jobs in over time.
        producer = threading.Thread(target=publish_jobs, args=(broker,))
        producer.start()
        producer.join()                    # all jobs enqueued

        broker.join()                      # all enqueued jobs processed (every task_done in)
        for _ in consumers:                # tell the consumers to stop, and wait for them
            broker.put(_STOP)
        for c in consumers:
            c.join()

        print("\npool stats:", pool.stats())
    finally:
        pool.close()                       # delete all worker pods
        print("pool closed")


if __name__ == "__main__":
    main()
