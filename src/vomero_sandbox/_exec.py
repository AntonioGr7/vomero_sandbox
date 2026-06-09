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


def upload_files(configuration: client.Configuration, pod: str, namespace: str,
                 scratch_dir: str, files: dict[str, bytes], timeout_s: float) -> None:
    """Write ``files`` ({relpath: bytes}) into the worker's working dir via the
    exec stdin channel. Binary-safe and not bounded by argv/URL length limits.
    Raises _UploadError on failure. Uses a dedicated ApiClient (thread-safe)."""
    import base64

    payload = json.dumps({rel: base64.b64encode(b).decode() for rel, b in files.items()})
    framed = f"{len(payload):016d}{payload}"   # all ASCII -> char count == byte count

    api_client = client.ApiClient(configuration)
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
        try:
            api_client.close()
        except Exception:
            pass


def exec_capture(configuration: client.Configuration, pod: str, namespace: str,
                 argv: list[str], timeout_s: float) -> tuple[str, str, int]:
    """Run ``argv`` in ``pod`` and return (stdout, stderr, exit_code).

    Uses a DEDICATED, per-call ApiClient. This is essential, not incidental:
    kubernetes' ``stream()`` monkeypatches ``ApiClient.request`` to do websockets
    for the duration of the exec, and that patch is NOT thread-safe. If exec
    shared one ApiClient with the pool's CRUD calls (or with other concurrent
    execs), a plain GET/DELETE on another thread would get routed through the
    websocket path and fail. A fresh client per exec isolates the patch.

    Raises _ExecTimeout if it doesn't finish within timeout_s (the caller is
    expected to retire the worker, since a runaway process may still be live).
    """
    api_client = client.ApiClient(configuration)
    api = client.CoreV1Api(api_client)
    resp = stream(
        api.connect_get_namespaced_pod_exec,
        pod, namespace,
        command=argv,
        stderr=True, stdout=True, stdin=False, tty=False,
        _preload_content=False,
    )
    out: list[str] = []
    err: list[str] = []
    deadline = time.monotonic() + timeout_s
    try:
        while resp.is_open():
            if time.monotonic() > deadline:
                raise _ExecTimeout()
            resp.update(timeout=1)
            if resp.peek_stdout():
                out.append(resp.read_stdout())
            if resp.peek_stderr():
                err.append(resp.read_stderr())
        exit_code = _exit_code(resp.read_channel(ERROR_CHANNEL))
        return "".join(out), "".join(err), exit_code
    finally:
        try:
            resp.close()
        except Exception:
            pass
        try:
            api_client.close()
        except Exception:
            pass


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
