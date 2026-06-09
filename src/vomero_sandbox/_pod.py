"""Internal: translate a SandboxConfig into a hardened worker Pod spec.

A worker's main process keeps the pod Running so we can exec snippets into it
(the warm-pool model). By default that process is an idle watchdog that exits —
letting the pod terminate on its own — if no run touches it for
``config.idle_shutdown_s``. This is the cluster-side backstop against leaked
workers: if the controlling process dies without calling ``close()`` (a crash,
SIGKILL, a lost node), the orphaned workers self-terminate instead of running
forever. With ``idle_shutdown_s=None`` the process is a plain ``sleep infinity``
and workers live until explicitly deleted. Every security control from the
config is applied here.
"""

from __future__ import annotations

from kubernetes import client

from .config import SandboxConfig

WORKER_ROLE = "worker"

# Idle watchdog, run as the container's PID 1 (so it sees the whole PID
# namespace). Each exec the pool runs shows up as another PID; whenever one
# exists the idle timer resets, and once nothing but the watchdog itself has run
# for ttl seconds it exits 0 — the container stops and the pod becomes Succeeded
# (restartPolicy Never), freeing its CPU/memory. No heartbeat file, no extra exec
# per run: activity is inferred straight from /proc. python is always present in
# worker images (the collector/cleaner rely on it too).
_IDLE_WATCHDOG = """\
import os, time
ttl, poll = {ttl}, {poll}
last = time.monotonic()
while True:
    active = any(e.isdigit() and e != '1' for e in os.listdir('/proc'))
    now = time.monotonic()
    if active:
        last = now
    elif now - last >= ttl:
        break
    time.sleep(poll)
"""


def _worker_command(cfg: SandboxConfig) -> list[str]:
    """The container's main process: the idle watchdog, or a bare sleep if idle
    self-termination is disabled."""
    if cfg.idle_shutdown_s is None:
        return ["sleep", "infinity"]
    poll = max(1.0, min(30.0, cfg.idle_shutdown_s / 4))
    return ["python", "-c", _IDLE_WATCHDOG.format(ttl=cfg.idle_shutdown_s, poll=poll)]


def build_worker_pod(cfg: SandboxConfig, name: str) -> client.V1Pod:
    container_sc = client.V1SecurityContext(
        run_as_non_root=True,
        run_as_user=cfg.run_as_user,
        run_as_group=cfg.run_as_group,
        allow_privilege_escalation=False,
        read_only_root_filesystem=cfg.read_only_root_filesystem,
        capabilities=client.V1Capabilities(drop=["ALL"]) if cfg.drop_all_capabilities else None,
    )

    pod_sc = None
    if cfg.seccomp_runtime_default:
        pod_sc = client.V1PodSecurityContext(
            seccomp_profile=client.V1SeccompProfile(type="RuntimeDefault")
        )

    resources = client.V1ResourceRequirements(
        requests={"cpu": cfg.cpu_request, "memory": cfg.memory_request},
        limits={"cpu": cfg.cpu_limit, "memory": cfg.memory_limit},
    )

    # A writable scratch volume, always mounted and used as the working
    # directory. This makes the workspace writable for ANY command (python, grep,
    # a compiler, ...) even when the root filesystem is read-only, and gives every
    # run a predictable cwd.
    volumes = [client.V1Volume(name="scratch", empty_dir=client.V1EmptyDirVolumeSource())]
    mounts = [client.V1VolumeMount(name="scratch", mount_path=cfg.scratch_dir)]

    env = [client.V1EnvVar(name="TMPDIR", value=cfg.scratch_dir)]
    if cfg.egress_proxy:
        env += [
            client.V1EnvVar(name="HTTPS_PROXY", value=cfg.egress_proxy),
            client.V1EnvVar(name="HTTP_PROXY", value=cfg.egress_proxy),
            client.V1EnvVar(name="NO_PROXY", value="localhost,127.0.0.1,.svc,.cluster.local"),
        ]

    container = client.V1Container(
        name="runner",
        image=cfg.image,
        command=_worker_command(cfg),
        image_pull_policy=cfg.image_pull_policy,
        working_dir=cfg.scratch_dir,      # cwd is the writable scratch workspace
        security_context=container_sc,
        resources=resources,
        volume_mounts=mounts,
        env=env,
    )

    return client.V1Pod(
        metadata=client.V1ObjectMeta(
            name=name,
            labels={"app": cfg.app_label, "role": WORKER_ROLE},
        ),
        spec=client.V1PodSpec(
            restart_policy="Never",
            termination_grace_period_seconds=2,   # hung pods die fast after SIGTERM
            automount_service_account_token=cfg.automount_service_account_token,
            runtime_class_name=cfg.runtime_class,
            security_context=pod_sc,
            volumes=volumes or None,
            containers=[container],
        ),
    )
