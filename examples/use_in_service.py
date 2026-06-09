"""The long-lived-service lifecycle: warm once, exec per request, tear down once.

In a real service you don't want the context-manager's warm-up/tear-down on
every request. Instead: start() the pool once at boot, run() from your request
handlers (it's thread-safe — pool_size is your concurrency ceiling), and close()
on shutdown.

This sketches it with a Flask-style handler, but the pattern is framework-
agnostic. Needs a cluster (see examples/README.md).
"""

from concurrent.futures import ThreadPoolExecutor

from vomero_sandbox import SandboxPool, SandboxConfig

# Build the pool at import/boot time and warm it once.
pool = SandboxPool(SandboxConfig(pool_size=5, max_uses=25, default_timeout_s=15))


def boot() -> None:
    report = pool.start()                 # provisions namespace/policy + warms workers
    print(f"pool warm: placed {report.placed}/{report.requested} workers")
    if not report.complete:               # shared cluster may give you fewer
        print("  shortfall reasons:", report.reasons)


def handle_request(user_code: str, user_token: str) -> dict:
    """What a request handler does: run the user's code, return a JSON-able dict.

    Thread-safe — call it concurrently up to pool_size; extra calls block until a
    worker frees up. A failing run is a normal result, surfaced to the caller.
    """
    result = pool.run(user_code, env={"USER_TOKEN": user_token}, timeout_s=10)
    return {
        "ok": result.ok,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "exit_code": result.exit_code,
        "timed_out": result.timed_out,
        "duration_s": result.duration_s,
        "sandbox_id": result.sandbox_id,   # which worker served it (for tracing)
    }


def shutdown() -> None:
    pool.close()                          # delete all worker pods
    print("pool closed")


def main() -> None:
    boot()
    try:
        # Simulate a few concurrent requests hitting the handler.
        snippets = [
            ("print(2 ** 10)", "tok_a"),
            ("print('hello')", "tok_b"),
            ("raise SystemExit(3)", "tok_c"),     # a clean non-zero exit
        ]
        with ThreadPoolExecutor(max_workers=3) as ex:
            for req, res in zip(snippets, ex.map(lambda a: handle_request(*a), snippets)):
                print(f"{req[0]!r:30} -> ok={res['ok']} exit={res['exit_code']} "
                      f"out={res['stdout'].strip()!r}")
        print("\npool stats:", pool.stats())
    finally:
        shutdown()


if __name__ == "__main__":
    main()
