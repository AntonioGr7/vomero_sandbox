"""A client/server version of the queue scenario, for stress testing.

queue_consumer.py kept everything in one process. Here the pool lives behind an
HTTP server (the long-lived service that owns the warmed SandboxPool), and a
separate client (stress_client.py) drives load against it — so you can vary the
parameters from the client and watch how the pool behaves under different
pressure, without touching the server.

Same job model as queue_consumer.py: a request names a PACKAGE (resolved on disk
under examples/package_registry/) and carries its INPUT; the server ships it into
the sandbox with run_project() and returns the result as JSON. The only new thing
is the transport — an HTTP front instead of an in-process queue.

Endpoints:
  POST /run    body: {"package": "stats", "files": {"input.json": "[1,2,3]"},
                      "env": {...}, "timeout_s": 10}  -> RunResult as JSON
  GET  /stats  -> pool.stats()
  GET  /healthz

Concurrency is bounded by the pool, NOT the server: the server is threaded and
accepts every connection, but a handler blocks inside run_project() until a
worker frees, so pool_size requests run at once and the rest queue — exactly the
back-pressure you want to observe under load.

Stdlib only (http.server) so the example stays self-contained. Needs a cluster
(see examples/README.md).

    python examples/stress_server.py --pool-size 5 --port 8099
    # then, in another shell:
    python examples/stress_client.py --jobs 200 --concurrency 20
"""

import argparse
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from vomero_sandbox import SandboxPool, SandboxConfig

HERE = os.path.dirname(os.path.abspath(__file__))
REGISTRY = os.path.join(HERE, "package_registry")   # where packages are "saved"


def _package_dir(name: str) -> str:
    """Resolve a job's package NAME to its on-disk tree, refusing anything that
    isn't a plain registry entry (no path traversal from a client-supplied name)."""
    if name != os.path.basename(name) or name in ("", ".", ".."):
        raise ValueError(f"invalid package name: {name!r}")
    path = os.path.join(REGISTRY, name)
    if not os.path.isdir(path):
        raise ValueError(f"unknown package: {name!r}")
    return path


class JobRunner:
    """Turns a decoded request into a sandbox run. Kept separate from the HTTP
    handler so the transport and the work are independent (and so a test can swap
    in a fake runner without a cluster)."""

    def __init__(self, pool: SandboxPool):
        self.pool = pool

    def run(self, package: str, files: dict, env: dict, timeout_s: float) -> dict:
        pkg_dir = _package_dir(package)
        # JSON can't carry raw bytes; the client sends text, we encode it. (For
        # binary inputs you'd base64 on the wire and b64decode here.)
        extra = {rel: (v.encode() if isinstance(v, str) else bytes(v))
                 for rel, v in (files or {}).items()}
        r = self.pool.run_project(
            pkg_dir, ["python", "."],
            extra_files=extra, collect=["out.json"],
            env=env or {}, timeout_s=timeout_s,
        )
        return {
            "ok": r.ok,
            "exit_code": r.exit_code,
            "timed_out": r.timed_out,
            "duration_s": r.duration_s,
            "sandbox_id": r.sandbox_id,
            "stdout": r.stdout.strip(),
            "artifact": r.files.get("out.json", b"").decode(),
        }


def _log(msg: str) -> None:
    """Print a server-side log line immediately (flush, since stdout is block-
    buffered when piped/redirected)."""
    print(msg, flush=True)


class Handler(BaseHTTPRequestHandler):
    # The runner and pool are attached to the server instance (see make_server).
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):
        pass                                   # silence the default per-request HTTP log

    def _send(self, code: int, payload: dict) -> None:
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/healthz":
            self._send(200, {"status": "ok"})
        elif self.path == "/stats":
            self._send(200, self.server.pool.stats())
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self) -> None:
        if self.path != "/run":
            self._send(404, {"error": "not found"})
            return
        seq = active = None
        try:
            length = int(self.headers.get("Content-Length", 0))
            req = json.loads(self.rfile.read(length) or b"{}")
            pkg = req["package"]
            # Bump the in-flight counters and log arrival. `active` is how many
            # requests are inside run_project() right now — it caps at pool_size
            # while the rest queue, so this number makes the back-pressure visible.
            with self.server.lock:
                self.server.seq += 1
                self.server.active += 1
                seq, active = self.server.seq, self.server.active
            _log(f"→ #{seq:<4} {pkg:9} received   (in-flight={active})")
            result = self.server.runner.run(
                package=pkg,
                files=req.get("files", {}),
                env=req.get("env", {}),
                timeout_s=req.get("timeout_s", 10),
            )
            _log(f"← #{seq:<4} {pkg:9} ok={result['ok']!s:5} on={result['sandbox_id']} "
                 f"{result['duration_s']}s")
            self._send(200, result)
        except (KeyError, ValueError) as e:    # bad request from the client
            _log(f"✗ #{seq} bad request: {e}")
            self._send(400, {"error": str(e)})
        except Exception as e:                 # infra error: surface it, keep serving
            _log(f"✗ #{seq} error: {type(e).__name__}: {e}")
            self._send(500, {"error": f"{type(e).__name__}: {e}"})
        finally:
            if active is not None:             # we incremented; decrement on the way out
                with self.server.lock:
                    self.server.active -= 1


def make_server(host: str, port: int, runner: JobRunner, pool: SandboxPool) -> ThreadingHTTPServer:
    """Build a threaded HTTP server with the runner/pool attached. Factored out so
    tests can construct one with a fake runner and no cluster."""
    server = ThreadingHTTPServer((host, port), Handler)
    server.runner = runner
    server.pool = pool
    server.lock = threading.Lock()   # guards the in-flight counters below
    server.seq = 0                   # monotonic request id (for the logs)
    server.active = 0                # requests currently inside run_project()
    return server


def main() -> None:
    p = argparse.ArgumentParser(description="Sandbox job server (stress-test target).")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8099)
    p.add_argument("--pool-size", type=int, default=5, help="warm workers = concurrency ceiling")
    p.add_argument("--max-uses", type=int, default=50, help="retire a worker after N runs")
    p.add_argument("--default-timeout", type=float, default=15.0)
    args = p.parse_args()

    pool = SandboxPool(SandboxConfig(
        pool_size=args.pool_size, max_uses=args.max_uses,
        default_timeout_s=args.default_timeout,
    ))
    report = pool.start()
    print(f"pool warm: {report.placed}/{report.requested} workers")

    server = make_server(args.host, args.port, JobRunner(pool), pool)
    print(f"serving on http://{args.host}:{args.port}  (POST /run, GET /stats)  — Ctrl-C to stop")
    # Serve on a background thread so Ctrl-C in the main thread shuts down cleanly.
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        t.join()
    except KeyboardInterrupt:
        print("\nshutting down...")
    finally:
        server.shutdown()
        pool.close()
        print("pool closed")


if __name__ == "__main__":
    main()
