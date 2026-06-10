"""Internal: exec a snippet into a running worker and capture stdout/stderr/exit.

Capturing the exit code from an exec stream is the fiddly part: stdout and
stderr arrive on their own channels, and the exit status arrives as a JSON
object on a separate "error" channel (channel 3) — it is NOT the program's
stderr. We pump stdout/stderr while the stream is open, then read the exit
status at the end. A wall-clock deadline guards against hung code (the warm-pool
model has no pod-level activeDeadlineSeconds to fall back on).
"""

from __future__ import annotations

import json
import time

from kubernetes import client
from kubernetes.stream import stream
from kubernetes.stream.ws_client import ERROR_CHANNEL


class _ExecTimeout(Exception):
    """Raised internally when a run exceeds its wall-clock budget."""


class _UploadError(Exception):
    """Raised internally when writing input files into a worker fails."""


# Runs inside the worker: reads a length-prefixed JSON {relpath: base64} payload
# from stdin and writes each file into the working dir. A 16-char decimal length
# header lets the reader take exactly the right number of bytes without needing a
# stdin half-close (which the exec websocket doesn't cleanly support).
_UPLOADER = """\
import sys, os, json, base64
root = {root!r}
def readn(n):
    buf = ""
    while len(buf) < n:
        c = sys.stdin.read(n - len(buf))
        if not c:
            break
        buf += c
    return buf
data = json.loads(readn(int(readn(16))))
for rel, b64 in data.items():
    p = os.path.join(root, rel)
    d = os.path.dirname(p)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(p, "wb") as f:
        f.write(base64.b64decode(b64))
print("UPLOAD_OK", len(data))
"""


def upload_files(api_client: client.ApiClient, pod: str, namespace: str,
                 scratch_dir: str, files: dict[str, bytes], timeout_s: float) -> None:
    """Write ``files`` ({relpath: bytes}) into the worker's working dir via the
    exec stdin channel. Binary-safe and not bounded by argv/URL length limits.
    Raises _UploadError on failure.

    Takes a caller-owned ``api_client`` (one per worker — see the threading note
    in :func:`exec_stream`); it is NOT closed here, so it can be reused across the
    worker's sequential calls."""
    import base64

    payload = json.dumps({rel: base64.b64encode(b).decode() for rel, b in files.items()})
    framed = f"{len(payload):016d}{payload}"   # all ASCII -> char count == byte count

    api = client.CoreV1Api(api_client)
    code = _UPLOADER.format(root=scratch_dir)
    resp = stream(
        api.connect_get_namespaced_pod_exec, pod, namespace,
        command=["python", "-c", code],
        stdin=True, stdout=True, stderr=True, tty=False,
        _preload_content=False,
    )
    out, err = [], []
    deadline = time.monotonic() + timeout_s
    try:
        resp.write_stdin(framed)
        while resp.is_open():
            if time.monotonic() > deadline:
                raise _UploadError(f"upload exceeded {timeout_s}s")
            resp.update(timeout=1)
            if resp.peek_stdout():
                out.append(resp.read_stdout())
            if resp.peek_stderr():
                err.append(resp.read_stderr())
            if "UPLOAD_OK" in "".join(out):
                return
        if "UPLOAD_OK" not in "".join(out):
            raise _UploadError("upload failed: " + ("".join(err).strip()[:200] or "no confirmation"))
    finally:
        try:
            resp.close()
        except Exception:
            pass


def exec_stream(api_client: client.ApiClient, pod: str, namespace: str,
                argv: list[str], timeout_s: float):
    """Run ``argv`` in ``pod`` and YIELD ('stdout'|'stderr', chunk) pairs as the
    bytes arrive, returning the exit code (via ``StopIteration.value``) once the
    process exits.

    This is the streaming core: the exec websocket already delivers stdout/stderr
    incrementally, so a caller that wants real-time output (e.g. forwarding an
    LLM's token stream to its own client) consumes the chunks here instead of
    waiting for the whole run. The exit status only arrives on the error channel
    at close, so it is the generator's RETURN value, not a yielded chunk.

    Threading contract: ``api_client`` must NOT be shared across concurrent execs.
    ``stream()`` monkeypatches ``ApiClient.request`` for the duration of the call
    and restores it in a ``finally``, so SEQUENTIAL reuse on one client is safe —
    but two threads streaming on the same client would race that patch. The pool
    gives each worker its own client and leases the worker exclusively, so the
    worker's sequential calls (clean/upload/exec/collect) all reuse one client
    while staying isolated from the CRUD clients and from other workers. The
    client is owned by the caller and is NOT closed here.

    Raises _ExecTimeout if it doesn't finish within timeout_s (the caller is
    expected to retire the worker, since a runaway process may still be live).
    The ``finally`` closes the websocket even if the consumer abandons the
    generator early (``break`` / ``.close()``)."""
    api = client.CoreV1Api(api_client)
    resp = stream(
        api.connect_get_namespaced_pod_exec,
        pod, namespace,
        command=argv,
        stderr=True, stdout=True, stdin=False, tty=False,
        _preload_content=False,
    )
    deadline = time.monotonic() + timeout_s
    try:
        while resp.is_open():
            if time.monotonic() > deadline:
                raise _ExecTimeout()
            resp.update(timeout=1)
            if resp.peek_stdout():
                yield ("stdout", resp.read_stdout())
            if resp.peek_stderr():
                yield ("stderr", resp.read_stderr())
        return _exit_code(resp.read_channel(ERROR_CHANNEL))
    finally:
        try:
            resp.close()
        except Exception:
            pass


def exec_capture(api_client: client.ApiClient, pod: str, namespace: str,
                 argv: list[str], timeout_s: float) -> tuple[str, str, int]:
    """Run ``argv`` in ``pod`` and return (stdout, stderr, exit_code).

    The buffered counterpart to :func:`exec_stream`: it drives the same generator
    and joins the chunks, so behavior is identical for every existing caller. Use
    this when you want the whole result at once; use :func:`exec_stream` when you
    want the output as it is produced.

    Raises _ExecTimeout if it doesn't finish within timeout_s (the caller is
    expected to retire the worker, since a runaway process may still be live).
    """
    out: list[str] = []
    err: list[str] = []
    gen = exec_stream(api_client, pod, namespace, argv, timeout_s)
    try:
        while True:
            kind, data = next(gen)
            (out if kind == "stdout" else err).append(data)
    except StopIteration as stop:
        return "".join(out), "".join(err), stop.value
    finally:
        gen.close()


def _exit_code(error_channel: str) -> int:
    """Parse the v1.Status JSON on the exec error channel into an exit code."""
    if not error_channel:
        return 0
    try:
        status = json.loads(error_channel)
    except ValueError:
        return 0
    if status.get("status") == "Success":
        return 0
    for cause in status.get("details", {}).get("causes", []):
        if cause.get("reason") == "ExitCode":
            try:
                return int(cause.get("message", 1))
            except (TypeError, ValueError):
                return 1
    return 1
