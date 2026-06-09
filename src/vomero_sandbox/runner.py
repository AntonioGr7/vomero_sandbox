"""The sandbox pool: warm, hardened workers you exec untrusted code into.

Typical use inside a service:

    from vomero_sandbox import SandboxPool, SandboxConfig

    pool = SandboxPool(SandboxConfig(pool_size=5, runtime_class="gvisor"))
    pool.start()
    try:
        result = pool.run("print(sum(range(100)))", timeout_s=10)
        if result.ok:
            handle(result.stdout)
    finally:
        pool.close()

or as a context manager:

    with SandboxPool(SandboxConfig()) as pool:
        result = pool.run(user_code)
"""

from __future__ import annotations

import base64
import fnmatch
import gzip
import io
import json
import os
import posixpath
import queue
import tarfile
import threading
import time
from dataclasses import dataclass

from kubernetes import client, config as kube_config, watch

from ._exec import _ExecTimeout, _UploadError, exec_capture, upload_files
from ._pod import WORKER_ROLE, build_worker_pod
from .config import SandboxConfig
from .errors import SandboxError, SandboxPoolClosed, SandboxStartupError
from .models import PoolStartReport, RunResult

# Runs inside the worker (which has python) to read produced files out of the
# working dir and emit them base64-encoded as JSON on stdout. Binary-safe; size-
# capped so a giant artifact can't blow up the caller.
_COLLECTOR = """\
import os, sys, json, base64
root, want, maxb = {root!r}, {want!r}, {maxb}
out, trunc, total = {{}}, [], 0
def add(rel):
    global total
    try:
        with open(os.path.join(root, rel), 'rb') as f:
            d = f.read()
    except OSError:
        return
    if total + len(d) > maxb:
        trunc.append(rel); return
    total += len(d); out[rel] = base64.b64encode(d).decode()
if want is None:
    for dp, _, fs in os.walk(root):
        for f in fs:
            add(os.path.relpath(os.path.join(dp, f), root))
else:
    for rel in want:
        add(rel)
sys.stdout.write(json.dumps({{"files": out, "trunc": trunc}}))
"""


# Runs inside the worker to empty the working dir before a fresh run (keeps the
# dir itself, removes its contents). python is always present in worker images.
_CLEANER = """\
import os, shutil
root = {root!r}
for name in os.listdir(root):
    p = os.path.join(root, name)
    if os.path.isdir(p) and not os.path.islink(p):
        shutil.rmtree(p, ignore_errors=True)
    else:
        try:
            os.remove(p)
        except OSError:
            pass
print("CLEAN_OK")
"""


# Names/globs pruned from an uploaded project tree by default: caches, VCS
# metadata, virtualenvs, editor cruft. They bloat the upload and never belong in
# the sandbox. Matched against each path component (so a dir name prunes the
# whole subtree) with fnmatch, so globs like "*.pyc" work.
_DEFAULT_PROJECT_EXCLUDES = (
    "__pycache__", "*.pyc", "*.pyo", "*.egg-info",
    ".git", ".hg", ".svn",
    ".venv", "venv", "env", "node_modules",
    ".mypy_cache", ".pytest_cache", ".ruff_cache", ".tox",
    ".DS_Store", ".ipynb_checkpoints",
)


def _excluded(name: str, patterns: tuple[str, ...]) -> bool:
    return any(fnmatch.fnmatch(name, p) for p in patterns)


def _read_project_tree(project_dir: str, exclude: tuple[str, ...],
                       max_bytes: int, follow_symlinks: bool) -> dict[str, bytes]:
    """Walk a host directory into an ``input_files`` map ({posix-relpath: bytes}).

    Prunes excluded names (dirs are pruned whole), skips symlinks unless
    ``follow_symlinks``, and fails early with a clear message if the total would
    exceed ``max_bytes`` — before anything is sent to the cluster."""
    root = os.path.abspath(project_dir)
    if not os.path.isdir(root):
        raise ValueError(f"project_dir is not a directory: {project_dir}")
    files: dict[str, bytes] = {}
    total = 0
    for dirpath, dirnames, filenames in os.walk(root, followlinks=follow_symlinks):
        dirnames[:] = sorted(d for d in dirnames if not _excluded(d, exclude))  # prune + deterministic order
        for fn in sorted(filenames):
            if _excluded(fn, exclude):
                continue
            full = os.path.join(dirpath, fn)
            if os.path.islink(full) and not follow_symlinks:
                continue
            if not os.path.isfile(full):
                continue
            with open(full, "rb") as f:
                data = f.read()
            total += len(data)
            rel = os.path.relpath(full, root).replace(os.sep, "/")
            if total > max_bytes:
                raise ValueError(
                    f"project tree exceeds max_upload_bytes={max_bytes} (reached "
                    f"{total} bytes at {rel!r}); vendor large data via object storage "
                    "or a mounted volume instead of uploading it in-band")
            files[rel] = data
    if not files:
        raise ValueError(f"no files to upload from {project_dir} (everything excluded?)")
    return files


def _safe_member_name(name: str) -> bool:
    """Reject snapshot entries that would escape the working dir (absolute paths
    or any '..' component) — a tampered blob shouldn't be able to aim writes
    outside /scratch."""
    if name.startswith("/"):
        return False
    return not any(part in ("..", "") for part in name.split("/") if part != ".")


def _pack_snapshot(files: dict[str, bytes]) -> bytes:
    """Serialize a {relpath: bytes} working-dir capture into a portable, gzip'd
    tar blob. Deterministic (sorted, fixed mtime) so equal state -> equal bytes."""
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w") as tar:   # uncompressed tar first
        for rel in sorted(files):
            info = tarfile.TarInfo(name=rel)
            info.size = len(files[rel])
            info.mtime = 0
            tar.addfile(info, io.BytesIO(files[rel]))
    out = io.BytesIO()
    with gzip.GzipFile(fileobj=out, mode="wb", mtime=0) as gz:   # mtime=0 -> deterministic
        gz.write(raw.getvalue())
    return out.getvalue()


def _unpack_snapshot(blob: bytes) -> dict[str, bytes]:
    """Inverse of :func:`_pack_snapshot`. Skips non-files and rejects unsafe
    member names (path traversal)."""
    out: dict[str, bytes] = {}
    with tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz") as tar:
        for m in tar.getmembers():
            if not m.isfile():
                continue
            name = posixpath.normpath(m.name)
            if not _safe_member_name(name):
                raise SandboxError(f"refusing unsafe path in snapshot: {m.name!r}")
            f = tar.extractfile(m)
            if f is not None:
                out[name] = f.read()
    return out


def _with_env(argv: list[str], env: dict[str, str] | None) -> list[str]:
    """Prefix argv with the coreutils ``env`` tool to set per-run environment
    variables for just this process: ``env K1=V1 K2=V2 <argv>``. Values are
    separate argv elements, so no shell re-parsing — spaces/special chars in
    values are safe. Keys must be valid (no '=' or NUL)."""
    if not env:
        return argv
    pairs = []
    for k, v in env.items():
        if not k or "=" in k or "\x00" in k or "\x00" in str(v):
            raise ValueError(f"invalid environment variable name: {k!r}")
        pairs.append(f"{k}={v}")
    return ["env", *pairs, *argv]


@dataclass
class _Worker:
    name: str
    created_at: float
    uses: int = 0

    def spent(self, max_uses: int, max_age_s: float) -> bool:
        return self.uses >= max_uses or (time.monotonic() - self.created_at) >= max_age_s


class SandboxPool:
    """A pool of warm, hardened worker pods. Thread-safe.

    Construct with a :class:`SandboxConfig` (hardened by default), call
    :meth:`start` once, then :meth:`run` concurrently from your request handlers.
    """

    def __init__(self, config: SandboxConfig | None = None):
        self.config = config or SandboxConfig()
        self.config.validate()
        try:
            kube_config.load_incluster_config()
        except kube_config.ConfigException:
            kube_config.load_kube_config(context=self.config.kube_context)
        self._api = client.CoreV1Api()
        self._net = client.NetworkingV1Api()
        # exec uses its own ApiClient per call (built from this configuration) so
        # stream()'s non-thread-safe monkeypatch can't corrupt these CRUD clients.
        self._configuration = self._api.api_client.configuration
        self._free: queue.Queue[_Worker] = queue.Queue()
        self._lock = threading.Lock()
        self._serial = 0
        self._spawned = 0
        self._retired = 0
        self._started = False
        self._closed = False
        self._start_report: PoolStartReport | None = None

    # --- lifecycle --------------------------------------------------------

    def start(self) -> PoolStartReport:
        """Provision namespace/policy (if managed) and warm the pool. Idempotent.

        Returns a :class:`PoolStartReport`. With ``min_pool_size`` unset (default)
        the pool is all-or-nothing: any worker that can't be placed raises
        ``SandboxStartupError``. With ``min_pool_size`` set, startup is best-effort
        on shared nodes — it places as many workers as the scheduler will accept,
        stops at the first that won't fit, and succeeds as long as at least
        ``min_pool_size`` landed (raising only below that floor). The returned
        report tells you how many of the requested workers were actually placed.
        """
        if self._started:
            return self._start_report
        if self.config.manage_namespace:
            self._ensure_namespace()
        if self.config.manage_network_policy:
            self._ensure_network_policy()

        requested = self.config.pool_size
        floor = self.config.min_pool_size or requested   # None => strict (== requested)
        placed: list[_Worker] = []
        reasons: list[str] = []
        for _ in range(requested):
            worker, reason = self._try_spawn()
            if worker is None:
                # All workers are identical, so the first one the cluster won't
                # accept means there's no room for any more — stop probing.
                reasons.append(reason)
                break
            placed.append(worker)

        report = PoolStartReport(requested=requested, placed=len(placed), reasons=tuple(reasons))
        if len(placed) < floor:
            # Couldn't even meet the floor: tear down the partial pool and fail loud.
            for w in placed:
                self._delete(w.name)
            raise SandboxStartupError(
                f"placed {len(placed)}/{requested} workers, below min_pool_size={floor}: "
                + ("; ".join(reasons) or "unknown")
            )
        for w in placed:
            self._free.put(w)
        self._start_report = report
        self._started = True
        return report

    def close(self) -> None:
        """Delete all worker pods owned by this pool. Safe to call more than once."""
        self._closed = True
        sel = f"app={self.config.app_label},role={WORKER_ROLE}"
        try:
            for p in self._api.list_namespaced_pod(self.config.namespace, label_selector=sel).items:
                self._delete(p.metadata.name)
        except client.ApiException:
            pass

    def __enter__(self) -> "SandboxPool":
        self.start()   # report is available via .start_report / .stats() afterwards
        return self

    @property
    def start_report(self) -> PoolStartReport | None:
        """The PoolStartReport from the last successful start() (None if not started)."""
        return self._start_report

    def __exit__(self, *exc) -> None:
        self.close()

    # --- the main API -----------------------------------------------------

    def run(self, code: str, timeout_s: float | None = None,
            collect: bool | list[str] = False,
            env: dict[str, str] | None = None,
            input_files: dict[str, bytes] | None = None) -> RunResult:
        """Run a code snippet in the configured interpreter (Python by default).

        ``run("print(2+2)")`` becomes ``python -c "print(2+2)"`` inside the
        sandbox. Set ``config.interpreter`` (and ``config.image``) for another
        language. For shell commands or arbitrary tools, use :meth:`shell` or
        :meth:`exec`.

        ``collect`` pulls produced files back; ``env`` injects per-run env vars;
        ``input_files`` places files in the workspace before running — see
        :meth:`exec`.
        """
        return self._dispatch([*self.config.interpreter, code], timeout_s, collect, env, input_files)

    def shell(self, command: str, timeout_s: float | None = None,
              collect: bool | list[str] = False,
              env: dict[str, str] | None = None,
              input_files: dict[str, bytes] | None = None) -> RunResult:
        """Run a shell command string via ``sh -c`` (pipes, globs, redirects work).

        ``shell("grep -rn TODO . | head")`` runs the whole pipeline in the
        sandbox. Convenient for agents; the usual shell-quoting caveats apply, but
        there's no privilege to escalate to — the sandbox itself is the boundary.
        """
        return self._dispatch(["sh", "-c", command], timeout_s, collect, env, input_files)

    def exec(self, argv: list[str], timeout_s: float | None = None,
             collect: bool | list[str] = False,
             env: dict[str, str] | None = None,
             input_files: dict[str, bytes] | None = None) -> RunResult:
        """Run an explicit command vector — no shell, no interpreter.

        ``exec(["grep", "-rn", "TODO", "."])`` runs grep directly. This is the
        safest, most precise primitive: the arguments are passed verbatim, so
        nothing in them is re-parsed by a shell. The command must exist in the
        worker's image (the default ``python:3.13-slim`` has python + coreutils +
        grep + sh; use a richer image for more tools).

        ``input_files`` writes files INTO the working directory before the run, so
        the code can read them as local files: ``input_files={"data.csv": b"...",
        "cfg/opts.json": b"..."}`` (relative paths; nested dirs are created). The
        end-user's selected platform files go here. Total size is capped by
        ``config.max_upload_bytes``. Requires ``python`` in the image.

        ``collect`` retrieves files the run wrote to the working directory:
          - ``collect=["out.png", "sub/report.csv"]`` — those paths (relative to
            the working dir).
          - ``collect=True`` — every file under the working dir.
        Retrieved files land in ``result.files`` ({relpath: bytes}); anything
        over ``config.max_collect_bytes`` is skipped and listed in
        ``result.files_truncated``. Collection requires ``python`` in the image
        (the default has it) and is skipped on timeout.

        ``env`` injects environment variables for THIS run only (e.g. values the
        end-user supplied with their code). They're scoped to the single process
        — they do NOT persist on the reused worker or leak to other runs — so
        per-user values are safe here. Requires ``env`` in the image (coreutils,
        present by default).
        """
        if not argv:
            raise ValueError("argv must be a non-empty list")
        return self._dispatch(list(argv), timeout_s, collect, env, input_files)

    def run_project(self, project_dir: str, entrypoint: list[str],
                    timeout_s: float | None = None,
                    collect: bool | list[str] = False,
                    env: dict[str, str] | None = None,
                    exclude: tuple[str, ...] = _DEFAULT_PROJECT_EXCLUDES,
                    follow_symlinks: bool = False,
                    extra_files: dict[str, bytes] | None = None) -> RunResult:
        """Upload a whole host directory (a multi-file package) into the sandbox
        working dir and run ``entrypoint`` against it.

        This is the multi-file counterpart to :meth:`run`: instead of one snippet,
        you ship a tree — packages with sub-folders, local ``import`` of sibling
        modules, data files — and run it like a real project. The directory's
        contents land under the working dir (``config.scratch_dir``), which is the
        cwd and on ``sys.path``, so ``python -m app`` / ``python main.py`` and
        relative imports resolve.

            pool.run_project("./submission", ["python", "-m", "app"],
                             collect=["out/report.csv"])

        ``entrypoint`` is an explicit argv (no shell — same safety as :meth:`exec`).
        ``exclude`` prunes names/globs from the upload (defaults drop caches, VCS,
        venvs, ``node_modules``). ``extra_files`` injects extra ``{relpath: bytes}``
        on top of the tree (e.g. a generated config) — they overlay same-named
        files. The tree is size-capped by ``config.max_upload_bytes``.

        NOTE on ``collect``: ``collect=True`` returns EVERY file under the working
        dir — including the sources you just uploaded. To get only produced
        artifacts, pass explicit paths (``collect=["out/report.csv"]``). For large
        outputs, write to object storage from inside the sandbox instead.

        Third-party dependencies are NOT installed by this call: the package can
        import the standard library, its own modules, and whatever is baked into
        ``config.image``. Code that needs external libraries needs them in the
        worker image (or vendored into the tree / installed via an egress proxy) —
        the read-only rootfs and default-deny egress otherwise block a runtime
        ``pip install``. See DEPLOYMENT.md.
        """
        if not entrypoint:
            raise ValueError("entrypoint must be a non-empty argv list")
        files = _read_project_tree(project_dir, exclude, self.config.max_upload_bytes, follow_symlinks)
        if extra_files:
            files.update(extra_files)
        return self._dispatch(list(entrypoint), timeout_s, collect, env, files)

    def _dispatch(self, argv: list[str], timeout_s: float | None,
                  collect: bool | list[str] = False,
                  env: dict[str, str] | None = None,
                  input_files: dict[str, bytes] | None = None) -> RunResult:
        """Acquire a worker, optionally upload input files, exec ``argv``, capture
        the result (and optionally collect outputs) before releasing/retiring."""
        if self._closed:
            raise SandboxPoolClosed("run() called on a closed pool")
        if not self._started:
            self.start()
        timeout = self.config.default_timeout_s if timeout_s is None else timeout_s
        worker = self._lease()              # acquire (blocks until one is free)
        t0 = time.monotonic()
        retire = False
        try:
            try:
                # A one-shot run wipes the workspace first iff configured.
                return self._exec_on_worker(worker.name, argv, timeout, collect, env,
                                            input_files, clean=self.config.fresh_workdir_per_run)
            except _ExecTimeout:
                # A runaway process may still be live in this worker -> retire it.
                retire = True
                return RunResult("", "", None, True,
                                 round(time.monotonic() - t0, 3), worker.name)
            except _UploadError as e:
                # Failing to stage inputs is an infra problem; retire and surface
                # it as a public error (the run never started).
                retire = True
                raise SandboxError(f"failed to upload input files: {e}") from e
            except Exception:
                retire = True               # infra/exec error -> treat worker as unhealthy
                raise
        finally:
            self._release(worker, retire)

    def _exec_on_worker(self, worker_name: str, argv: list[str], timeout: float,
                        collect: bool | list[str], env: dict[str, str] | None,
                        input_files: dict[str, bytes] | None, clean: bool) -> RunResult:
        """Run one command on an ALREADY-HELD worker: optional wipe, optional input
        upload, exec, optional collect. Shared by one-shot dispatch (clean=True per
        config) and sessions (clean=False — the workspace persists across calls).
        Raises _ExecTimeout / _UploadError / other on failure; the caller decides
        whether to retire the worker."""
        if input_files:
            total = sum(len(v) for v in input_files.values())
            if total > self.config.max_upload_bytes:
                raise ValueError(
                    f"input_files total {total} bytes exceeds max_upload_bytes="
                    f"{self.config.max_upload_bytes}")
        argv = _with_env(argv, env)         # per-run env, scoped to this process
        t0 = time.monotonic()
        if clean:
            self._clean_workspace(worker_name, timeout)
        if input_files:
            upload_files(self._configuration, worker_name, self.config.namespace,
                         self.config.scratch_dir, input_files, timeout)
        stdout, stderr, exit_code = exec_capture(
            self._configuration, worker_name, self.config.namespace, argv, timeout
        )
        files, truncated = ({}, ())
        if collect:
            files, truncated = self._collect(worker_name, collect, timeout)
        return RunResult(stdout, stderr, exit_code, False,
                         round(time.monotonic() - t0, 3), worker_name,
                         files=files, files_truncated=truncated)

    def _lease(self) -> _Worker:
        """Take a worker out of the free pool (blocks until one is available)."""
        return self._free.get()

    def _release(self, worker: _Worker, retire: bool) -> None:
        """Return a worker to the pool, or retire+replace it if it's spent/unhealthy."""
        worker.uses += 1
        if retire or worker.spent(self.config.max_uses, self.config.max_age_s):
            self._retire(worker)
        else:
            self._free.put(worker)

    def _retire(self, worker: _Worker) -> None:
        """Delete a worker and spawn its replacement, off the caller's thread."""
        threading.Thread(target=self._replace, args=(worker,), daemon=True).start()

    # --- sessions: multi-call work that shares a workspace -----------------

    def session(self) -> "SandboxSession":
        """Lease one worker for a sequence of calls that SHARE a working directory
        — no per-call wipe — so files written by one call are visible to the next.
        This is the right primitive for multi-step work within a turn or burst:

            with pool.session() as s:
                s.run_project("./repo", ["python", "-m", "build"])
                s.shell("pytest -q")          # sees the build output (same /scratch)
                blob = s.checkpoint()         # capture state to continue later

        State is FILESYSTEM only (a fresh interpreter per call). To continue across
        a turn boundary / process / replica, ``checkpoint()`` the session to a blob,
        store it in your conversation store, and ``resume()`` it later. A session
        holds a worker for the whole ``with`` block, so concurrent sessions are
        bounded by ``pool_size``. Use as a context manager.
        """
        return SandboxSession(self, restore=None)

    def resume(self, snapshot: bytes) -> "SandboxSession":
        """Lease a worker, restore a :meth:`SandboxSession.checkpoint` blob into its
        working dir, and return a session continuing from that filesystem state:

            with pool.resume(stored_blob) as s:
                s.run("...continue the work...")
                new_blob = s.checkpoint()     # persist the new state for next turn

        Works on ANY worker, in any process or replica — the session lives in the
        snapshot you persisted, not in a pinned pod. Restored size is bounded by
        ``config.max_upload_bytes``. Use as a context manager.
        """
        files = _unpack_snapshot(snapshot)
        total = sum(len(v) for v in files.values())
        if total > self.config.max_upload_bytes:
            raise ValueError(
                f"snapshot restores {total} bytes, exceeding max_upload_bytes="
                f"{self.config.max_upload_bytes}")
        return SandboxSession(self, restore=files)

    def stats(self) -> dict:
        """Snapshot of pool state (eventually-consistent during replacement)."""
        r = self._start_report
        return {
            "pool_size": self.config.pool_size,
            "requested": r.requested if r else self.config.pool_size,
            "placed": r.placed if r else 0,
            "shortfall": r.shortfall if r else 0,
            "free": self._free.qsize(),
            "spawned_total": self._spawned,
            "retired_total": self._retired,
            "started": self._started,
            "closed": self._closed,
        }

    # --- internals --------------------------------------------------------

    def _clean_workspace(self, worker_name: str, timeout_s: float) -> None:
        """Empty the worker's working dir before a fresh run. Cleanup failure is
        an isolation failure, so we surface it (retire + raise) rather than run in
        a dirty workspace."""
        code = _CLEANER.format(root=self.config.scratch_dir)
        try:
            stdout, _stderr, ec = exec_capture(
                self._configuration, worker_name, self.config.namespace,
                ["python", "-c", code], max(timeout_s, 15.0),
            )
        except _ExecTimeout as e:
            raise SandboxError("workspace cleanup timed out") from e
        if ec != 0 or "CLEAN_OK" not in stdout:
            raise SandboxError("workspace cleanup failed")

    def _collect(self, worker_name: str, collect: bool | list[str],
                 timeout_s: float) -> tuple[dict[str, bytes], tuple[str, ...]]:
        """Read produced files out of the worker's working dir. Best-effort: if
        the worker has no python or the collector fails, returns empty rather
        than failing an otherwise-successful run."""
        want = None if collect is True else [str(p) for p in collect]
        code = _COLLECTOR.format(root=self.config.scratch_dir, want=want,
                                 maxb=self.config.max_collect_bytes)
        try:
            stdout, _stderr, ec = exec_capture(
                self._configuration, worker_name, self.config.namespace,
                ["python", "-c", code], max(timeout_s, 15.0),
            )
        except _ExecTimeout:
            return {}, ()
        if ec != 0 or not stdout.strip():
            return {}, ()
        try:
            data = json.loads(stdout)
            files = {k: base64.b64decode(v) for k, v in data.get("files", {}).items()}
            return files, tuple(data.get("trunc", []))
        except (ValueError, TypeError):
            return {}, ()

    def _spawn(self) -> _Worker:
        """Create one worker and block until Running. Raises if it can't be placed
        (used by the warm-pool replacement path, which wants strict behavior)."""
        worker, reason = self._try_spawn()
        if worker is None:
            raise SandboxStartupError(f"worker could not be placed: {reason}")
        return worker

    def _try_spawn(self) -> tuple[_Worker | None, str | None]:
        """Create one worker and let the cluster decide if it fits.

        Returns ``(worker, None)`` once it reaches Running, or ``(None, reason)``
        if Kubernetes won't place it — either the namespace ResourceQuota rejects
        the create (admission 403) or the scheduler declares it Unschedulable
        (no node has room for its resource *requests*). In the reject case any
        Pending pod is deleted so it can't linger holding quota or schedule later.
        Other failures (Failed, startup timeout, API errors) still raise.
        """
        with self._lock:
            self._serial += 1
            self._spawned += 1
            name = f"{self.config.app_label}-{self._serial}"
        self._delete(name)                  # clear any stale pod with this name
        try:
            self._api.create_namespaced_pod(self.config.namespace, build_worker_pod(self.config, name))
        except client.ApiException as e:
            if e.status == 403:             # ResourceQuota / admission rejected the create
                return None, f"admission rejected (quota exhausted): {e.reason}"
            raise
        try:
            placed = self._wait_placed(name)
        except Exception:
            self._delete(name)              # don't leak a pod we're abandoning
            raise
        if not placed:
            self._delete(name)              # drop the Pending pod (frees quota; avoids late scheduling)
            return None, "unschedulable: no node has capacity for the worker's resource requests"
        return _Worker(name=name, created_at=time.monotonic()), None

    def _replace(self, old: _Worker) -> None:
        self._delete(old.name)
        with self._lock:
            self._retired += 1
        try:
            self._free.put(self._spawn())
        except Exception:
            # If a replacement can't be spawned (e.g. pool closing), don't crash
            # the daemon thread; the pool simply runs one worker short.
            pass

    def _wait_placed(self, name: str) -> bool:
        """Block until the scheduler's verdict is in: return True once the worker
        is Running, False once the scheduler declares it Unschedulable. Raise on
        Failed or startup timeout.

        We trust the scheduler as the authority on capacity rather than computing
        free space ourselves — it alone accounts for other tenants on these shared
        nodes, taints/affinity, LimitRange, and quota, atomically and race-free.
        """
        w = watch.Watch()
        try:
            for ev in w.stream(self._api.list_namespaced_pod,
                               namespace=self.config.namespace,
                               field_selector=f"metadata.name={name}",
                               timeout_seconds=int(self.config.startup_timeout_s)):
                pod = ev["object"]
                phase = pod.status.phase
                if phase == "Running":
                    return True
                if phase == "Failed":
                    raise SandboxStartupError(
                        f"worker {name} failed to start: {pod.status.reason}")
                if self._is_unschedulable(pod):
                    return False
        finally:
            w.stop()
        raise SandboxStartupError(
            f"worker {name} did not reach Running within {self.config.startup_timeout_s}s")

    @staticmethod
    def _is_unschedulable(pod) -> bool:
        """True if the scheduler has given up placing this (still-Pending) pod —
        the PodScheduled condition is False with reason Unschedulable."""
        for c in (pod.status.conditions or []):
            if c.type == "PodScheduled" and c.status == "False" and c.reason == "Unschedulable":
                return True
        return False

    def _delete(self, name: str, timeout_s: float = 60.0) -> None:
        """Delete a pod and wait until it's really gone. Polls for a 404 rather
        than watching for a DELETED event: a watch started after a fast delete
        never sees the already-past event and would block the full timeout."""
        try:
            self._api.delete_namespaced_pod(name, self.config.namespace, grace_period_seconds=0)
        except client.ApiException as e:
            if e.status == 404:
                return
            raise
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                self._api.read_namespaced_pod(name, self.config.namespace)
            except client.ApiException as e:
                if e.status == 404:
                    return
                raise
            time.sleep(0.2)

    def _ensure_namespace(self) -> None:
        names = {ns.metadata.name for ns in self._api.list_namespace().items}
        if self.config.namespace not in names:
            self._api.create_namespace(
                client.V1Namespace(metadata=client.V1ObjectMeta(name=self.config.namespace)))

    def _ensure_network_policy(self) -> None:
        """Lock sandbox egress: DNS only, plus the egress proxy if configured.
        Anything else outbound (internet, metadata endpoint, other namespaces)
        is denied. Idempotent."""
        egress = [client.V1NetworkPolicyEgressRule(ports=[
            client.V1NetworkPolicyPort(protocol="UDP", port=53),
            client.V1NetworkPolicyPort(protocol="TCP", port=53),
        ])]
        if self.config.egress_proxy:
            egress.append(client.V1NetworkPolicyEgressRule(
                to=[client.V1NetworkPolicyPeer(pod_selector=client.V1LabelSelector(
                    match_labels={"app": self.config.egress_proxy_label}))],
                ports=[client.V1NetworkPolicyPort(protocol="TCP", port=3128)],
            ))
        name = f"{self.config.app_label}-deny-egress"
        policy = client.V1NetworkPolicy(
            metadata=client.V1ObjectMeta(name=name, namespace=self.config.namespace),
            spec=client.V1NetworkPolicySpec(
                pod_selector=client.V1LabelSelector(match_labels={"app": self.config.app_label}),
                policy_types=["Egress"],
                egress=egress,
            ),
        )
        try:
            self._net.create_namespaced_network_policy(self.config.namespace, policy)
        except client.ApiException as e:
            if e.status == 409:
                self._net.replace_namespaced_network_policy(name, self.config.namespace, policy)
            else:
                raise


class SandboxSession:
    """A worker leased for a sequence of calls that share a working directory.

    Created by :meth:`SandboxPool.session` (fresh workspace) or
    :meth:`SandboxPool.resume` (workspace restored from a checkpoint). Within the
    ``with`` block, ``run`` / ``shell`` / ``exec`` / ``run_project`` all hit the
    SAME worker WITHOUT wiping between calls, so files persist across calls.

    State is filesystem-only: each call still runs a fresh process (no in-memory
    state carries over). To continue the work across a turn boundary, a different
    process, or after a long human pause, call :meth:`checkpoint` to get a portable
    blob, persist it in your own conversation store, and :meth:`SandboxPool.resume`
    it later — the session lives in that blob, not in a held-open pod.

    Not thread-safe: drive one session from one thread at a time. A timeout or
    infrastructure error retires the worker and ends the session (its in-progress
    filesystem state is lost unless you checkpointed earlier), so checkpoint before
    risky steps if durability matters.
    """

    def __init__(self, pool: "SandboxPool", restore: dict[str, bytes] | None):
        self._pool = pool
        self._restore = restore        # files to seed the workspace with (resume), else None
        self._worker: _Worker | None = None
        self._dead = False

    @property
    def sandbox_id(self) -> str | None:
        """The worker pod backing this session (None once closed/retired)."""
        return self._worker.name if self._worker else None

    def __enter__(self) -> "SandboxSession":
        p = self._pool
        if p._closed:
            raise SandboxPoolClosed("session() called on a closed pool")
        if not p._started:
            p.start()
        prep_timeout = max(p.config.default_timeout_s, 30.0)
        worker = p._lease()
        self._worker = worker
        try:
            # Start from a clean, isolated workspace, then restore the snapshot
            # (if resuming) so the session sees exactly the checkpointed state.
            p._clean_workspace(worker.name, prep_timeout)
            if self._restore:
                upload_files(p._configuration, worker.name, p.config.namespace,
                             p.config.scratch_dir, self._restore, prep_timeout)
        except Exception:
            self._retire_now()
            raise
        self._restore = None
        return self

    def __exit__(self, *exc) -> None:
        self._close()

    # --- the same surface as the pool, but pinned to this session's worker ---

    def run(self, code: str, timeout_s: float | None = None,
            collect: bool | list[str] = False, env: dict[str, str] | None = None,
            input_files: dict[str, bytes] | None = None) -> RunResult:
        """Run a snippet in the configured interpreter on this session's worker."""
        return self._call([*self._pool.config.interpreter, code], timeout_s, collect, env, input_files)

    def shell(self, command: str, timeout_s: float | None = None,
              collect: bool | list[str] = False, env: dict[str, str] | None = None,
              input_files: dict[str, bytes] | None = None) -> RunResult:
        """Run a shell command (``sh -c``) on this session's worker."""
        return self._call(["sh", "-c", command], timeout_s, collect, env, input_files)

    def exec(self, argv: list[str], timeout_s: float | None = None,
             collect: bool | list[str] = False, env: dict[str, str] | None = None,
             input_files: dict[str, bytes] | None = None) -> RunResult:
        """Run an explicit argv (no shell) on this session's worker."""
        if not argv:
            raise ValueError("argv must be a non-empty list")
        return self._call(list(argv), timeout_s, collect, env, input_files)

    def run_project(self, project_dir: str, entrypoint: list[str],
                    timeout_s: float | None = None, collect: bool | list[str] = False,
                    env: dict[str, str] | None = None,
                    exclude: tuple[str, ...] = _DEFAULT_PROJECT_EXCLUDES,
                    follow_symlinks: bool = False,
                    extra_files: dict[str, bytes] | None = None) -> RunResult:
        """Upload a host directory into this session's (persisting) workspace and
        run ``entrypoint`` — the multi-file form of :meth:`run`. Files added here
        stay for later calls in the session."""
        if not entrypoint:
            raise ValueError("entrypoint must be a non-empty argv list")
        files = _read_project_tree(project_dir, exclude, self._pool.config.max_upload_bytes, follow_symlinks)
        if extra_files:
            files.update(extra_files)
        return self._call(list(entrypoint), timeout_s, collect, env, files)

    def checkpoint(self, timeout_s: float | None = None) -> bytes:
        """Snapshot the session's working dir into a portable blob to persist and
        later hand to :meth:`SandboxPool.resume`. Filesystem state only.

        Raises if the workspace exceeds ``config.max_collect_bytes`` (a partial
        snapshot would silently lose state) — write large data to object storage
        from inside the sandbox instead of carrying it in the checkpoint.
        """
        self._require_live()
        timeout = max(self._pool.config.default_timeout_s if timeout_s is None else timeout_s, 15.0)
        files, truncated = self._pool._collect(self._worker.name, True, timeout)
        if truncated:
            raise SandboxError(
                f"checkpoint exceeds max_collect_bytes={self._pool.config.max_collect_bytes}: "
                f"{len(truncated)} file(s) would be dropped, so the snapshot is incomplete. "
                "Keep large artifacts in object storage rather than the session workspace.")
        return _pack_snapshot(files)

    # --- internals --------------------------------------------------------

    def _call(self, argv: list[str], timeout_s: float | None,
              collect: bool | list[str], env: dict[str, str] | None,
              input_files: dict[str, bytes] | None) -> RunResult:
        self._require_live()
        timeout = self._pool.config.default_timeout_s if timeout_s is None else timeout_s
        t0 = time.monotonic()
        try:
            # clean=False: the whole point of a session is a persistent workspace.
            return self._pool._exec_on_worker(self._worker.name, argv, timeout, collect,
                                              env, input_files, clean=False)
        except _ExecTimeout:
            name = self._worker.name
            self._retire_now()   # a runaway process may still be live; the session is over
            return RunResult("", "", None, True, round(time.monotonic() - t0, 3), name)
        except _UploadError as e:
            self._retire_now()
            raise SandboxError(f"failed to upload input files: {e}") from e
        except Exception:
            self._retire_now()
            raise

    def _require_live(self) -> None:
        if self._dead or self._worker is None:
            raise SandboxError(
                "session is no longer usable (the worker was retired after a timeout or "
                "error, or the session was closed); start a new session, resuming from "
                "your last checkpoint")

    def _retire_now(self) -> None:
        w, self._worker, self._dead = self._worker, None, True
        if w is not None:
            self._pool._retire(w)

    def _close(self) -> None:
        w, self._worker = self._worker, None
        if w is None:
            return
        if self._dead:
            self._pool._retire(w)
            return
        # Healthy: wipe the workspace (no state lingers for the next tenant) and
        # return the worker to the pool; retire it if the reset itself fails.
        try:
            self._pool._clean_workspace(w.name, max(self._pool.config.default_timeout_s, 15.0))
            self._pool._release(w, retire=False)
        except Exception:
            self._pool._retire(w)
