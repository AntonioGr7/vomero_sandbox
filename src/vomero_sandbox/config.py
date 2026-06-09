"""Configuration for the sandbox pool — one dataclass, secure by default.

Every security control is ON out of the box. A bare ``SandboxConfig()`` already
gives you: non-root, no service-account token, read-only root filesystem, all
Linux capabilities dropped, seccomp RuntimeDefault, CPU/memory limits, and
default-deny egress. You opt *out* of protections explicitly (and visibly),
never into them by forgetting.

gVisor (``runtime_class``) and the egress proxy (``egress_proxy``) are the two
controls left OFF by default, because they require cluster-side setup that this
library can't provision (a RuntimeClass + node binaries; a deployed proxy). Turn
them on once that infrastructure exists.
"""

from __future__ import annotations

import re
import warnings
from dataclasses import dataclass, field

from .errors import SandboxConfigError, SandboxConfigWarning

# Warn when a limit exceeds its request by more than this factor: the pod is
# heavily overcommitted (Burstable, far over its reserved floor), so the
# scheduler reserves little but the worker can balloon — risking node memory
# pressure and eviction on shared nodes. Above the library's own defaults
# (cpu 5x, memory 4x), which are intentional, so a bare SandboxConfig() is quiet.
_OVERCOMMIT_WARN_RATIO = 8.0

# Kubernetes resource-quantity suffixes -> multiplier (binary + decimal + milli).
_QUANTITY_SUFFIXES = {
    "": 1.0, "m": 1e-3,
    "k": 1e3, "M": 1e6, "G": 1e9, "T": 1e12, "P": 1e15, "E": 1e18,
    "Ki": 2 ** 10, "Mi": 2 ** 20, "Gi": 2 ** 30,
    "Ti": 2 ** 40, "Pi": 2 ** 50, "Ei": 2 ** 60,
}
_QUANTITY_RE = re.compile(r"^\s*([+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s*([A-Za-z]*)\s*$")


def _parse_quantity(value: str) -> float:
    """Parse a Kubernetes resource quantity ('100m', '256Mi', '1Gi', '2') into a
    float in canonical units (CPU cores, or bytes). Raises on malformed input."""
    m = _QUANTITY_RE.match(str(value))
    if not m or m.group(2) not in _QUANTITY_SUFFIXES:
        raise SandboxConfigError(f"invalid resource quantity: {value!r}")
    return float(m.group(1)) * _QUANTITY_SUFFIXES[m.group(2)]


@dataclass
class SandboxConfig:
    # --- cluster connection ------------------------------------------------
    namespace: str = "sandbox"
    kube_context: str | None = None       # None = default kubeconfig / in-cluster

    # --- image & interpreter ----------------------------------------------
    image: str = "python:3.13-slim"
    # argv prefix the snippet is appended to. Default runs Python with no shell,
    # so user code can't break out via quoting. Change image+interpreter together
    # for other languages.
    interpreter: list[str] = field(default_factory=lambda: ["python", "-c"])
    image_pull_policy: str = "IfNotPresent"

    # --- pool sizing & lifecycle ------------------------------------------
    pool_size: int = 3                    # warm workers; also the concurrency ceiling
    # Floor for best-effort startup on SHARED nodes. None (default) = strict:
    # start() must place all pool_size workers or it raises (all-or-nothing).
    # Set lower to allow a partial pool: start() places as many as the cluster
    # scheduler will actually accept (stopping at the first that won't fit),
    # succeeds as long as >= min_pool_size landed, and reports the shortfall in
    # its PoolStartReport. It raises only if fewer than min_pool_size could be
    # placed. Every placed worker is guaranteed to have reached Running.
    min_pool_size: int | None = None
    max_uses: int = 25                    # retire a worker after this many runs
    max_age_s: float = 600.0              # retire a worker older than this
    default_timeout_s: float = 30.0       # per-run wall-clock limit if run() omits one
    startup_timeout_s: float = 90.0       # how long to wait for a worker to reach Running

    # --- output file collection (run(..., collect=...)) -------------------
    # Cap on total bytes pulled back from the worker in one collect, so a huge
    # artifact can't blow up the caller's memory. Files past the cap are skipped
    # and reported in RunResult.files_truncated. For large outputs, write to
    # object storage from inside the sandbox instead of collecting in-band.
    max_collect_bytes: int = 32 * 1024 * 1024   # 32 MiB
    # Cap on total bytes of input files written into a worker in one run (same
    # rationale; large inputs should come from a mounted volume / object storage).
    max_upload_bytes: int = 32 * 1024 * 1024     # 32 MiB

    # --- resource limits (cgroup-enforced) --------------------------------
    cpu_request: str = "100m"
    cpu_limit: str = "500m"
    memory_request: str = "64Mi"
    memory_limit: str = "256Mi"

    # --- security context (all hardened by default) -----------------------
    run_as_user: int = 1000
    run_as_group: int = 1000
    drop_all_capabilities: bool = True
    read_only_root_filesystem: bool = True
    automount_service_account_token: bool = False
    seccomp_runtime_default: bool = True
    # Writable emptyDir mounted here AND used as the working directory, so any
    # command (python, grep, a compiler) has a writable cwd even with a read-only
    # root. It persists for the worker's lifetime (state carries across runs on
    # the same worker; wiped when the worker is retired). See README "Filesystem
    # & state": set max_uses=1 for a guaranteed-clean workspace per run.
    scratch_dir: str = "/scratch"
    # When True (default), each run starts with an EMPTY working directory even on
    # a reused worker: the workspace is wiped before the run. This gives per-run
    # filesystem isolation (no file leakage between runs / tenants on the same
    # worker) while keeping warm-pool reuse. Set False to let files persist across
    # runs on a worker (e.g. a single trusted caller building up state). When both
    # are set, input_files are written AFTER the wipe, so they survive into the run.
    fresh_workdir_per_run: bool = True

    # --- kernel isolation (opt-in: needs the RuntimeClass installed) ------
    runtime_class: str | None = None      # e.g. "gvisor"

    # --- network egress ---------------------------------------------------
    # None  -> default-deny egress (DNS only) when manage_network_policy=True.
    # set    -> route user-code HTTP(S) through this proxy URL and allow egress
    #           only to it (your allowlisting proxy decides what leaves).
    egress_proxy: str | None = None
    egress_proxy_label: str = "egress-proxy"   # pod label the egress NetworkPolicy targets

    # --- what the library is allowed to create in the cluster -------------
    # Set these False if your platform provisions the namespace / policies via
    # GitOps and the library should only create pods.
    manage_namespace: bool = True
    manage_network_policy: bool = True

    # --- leak protection: cleaning up orphaned workers --------------------
    # Workers are pods on the cluster, not children of your process — so if the
    # controlling process dies WITHOUT calling close() (a crash, SIGKILL, a lost
    # node), they would otherwise run forever. Three layers guard against that;
    # the first survives even a hard kill.
    #
    # Idle self-termination (cluster-side backstop): a worker's main process is a
    # watchdog that exits, letting the pod stop, if no run touches it for this
    # many seconds. Set None to disable (workers then run until explicitly
    # deleted). Keep it comfortably above your inter-run gap: an idle pool that
    # exceeds it sheds workers, and the next run pays for a replacement.
    idle_shutdown_s: float | None = 1800.0     # 30 min; None to disable
    # In-process hooks (graceful exits): when True, the pool registers an atexit
    # handler and a SIGTERM handler so an unhandled exception, normal exit, or
    # `kill <pid>` still calls close(). Does NOT cover SIGKILL / crashes — that's
    # what idle_shutdown_s is for. Set False to manage signals yourself.
    auto_cleanup: bool = True
    # Startup reclaim: when True, start() deletes any pre-existing workers
    # carrying this app_label in the namespace before warming, sweeping up
    # orphans a previous (crashed) run left behind. Leave False if several pools
    # / replicas SHARE an app_label in one namespace — reclaim can't tell live
    # peers' workers from orphans and would delete them. Safe when each app_label
    # maps to a single pool/controller.
    reclaim_on_start: bool = False

    # --- labels -----------------------------------------------------------
    app_label: str = "vomero-sandbox"    # all pods carry app=<this>; policies target it

    def validate(self) -> None:
        if self.pool_size < 1:
            raise SandboxConfigError("pool_size must be >= 1")
        if self.min_pool_size is not None and not (1 <= self.min_pool_size <= self.pool_size):
            raise SandboxConfigError("min_pool_size must be between 1 and pool_size")
        if self.max_uses < 1:
            raise SandboxConfigError("max_uses must be >= 1")
        if self.default_timeout_s <= 0:
            raise SandboxConfigError("default_timeout_s must be > 0")
        if self.idle_shutdown_s is not None and self.idle_shutdown_s <= 0:
            raise SandboxConfigError("idle_shutdown_s must be > 0 (or None to disable)")
        if not self.interpreter:
            raise SandboxConfigError("interpreter must be a non-empty argv list")
        if self.read_only_root_filesystem and not self.scratch_dir:
            raise SandboxConfigError(
                "read_only_root_filesystem requires a scratch_dir for the run to write to"
            )
        self._check_resources()

    def _check_resources(self) -> None:
        """Validate CPU/memory requests against limits. A request ABOVE its limit
        is invalid (Kubernetes would reject the pod) -> error. A request far BELOW
        its limit is legal but overcommits shared nodes -> warn, but proceed: the
        caller may want Burstable on purpose."""
        for kind, req_s, lim_s in (
            ("cpu", self.cpu_request, self.cpu_limit),
            ("memory", self.memory_request, self.memory_limit),
        ):
            req, lim = _parse_quantity(req_s), _parse_quantity(lim_s)
            if req <= 0 or lim <= 0:
                raise SandboxConfigError(f"{kind} request and limit must be > 0")
            if req > lim:
                raise SandboxConfigError(
                    f"{kind}_request ({req_s}) exceeds {kind}_limit ({lim_s}): a request "
                    "above its limit can never be scheduled"
                )
            if lim > req * _OVERCOMMIT_WARN_RATIO:
                warnings.warn(
                    f"{kind}_limit ({lim_s}) is {lim / req:.0f}x {kind}_request ({req_s}). "
                    "The scheduler only reserves the request, so on shared nodes this "
                    f"overcommits and the worker can be evicted under {kind} pressure. "
                    f"Raise {kind}_request toward the limit for predictable placement, "
                    "or ignore this if Burstable is intended.",
                    SandboxConfigWarning,
                    stacklevel=3,
                )
