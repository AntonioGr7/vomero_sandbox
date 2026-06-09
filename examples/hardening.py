"""The security model: what's on by default, what you opt into, and how to verify.

A bare SandboxConfig() is already hardened: non-root, no service-account token,
read-only root filesystem, all Linux capabilities dropped, seccomp
RuntimeDefault, CPU/memory limits, and default-deny egress. You opt OUT of
protections explicitly; you never forget to opt in.

Two controls are OFF by default because they need cluster-side infrastructure
this library can't provision:

  - gVisor (runtime_class="gvisor") — a real kernel boundary against host-kernel
    exploits. Needs runsc + a matching RuntimeClass installed on your nodes.
  - egress proxy (egress_proxy=...) — narrows egress to an allowlisting forward
    proxy you deploy (Squid/Envoy). The library then wires HTTPS_PROXY into
    workers and the NetworkPolicy to allow only the proxy.

This example probes the live defaults, then shows the hardened config you'd use
once that infrastructure exists. Needs a cluster (see examples/README.md).
"""

from vomero_sandbox import SandboxPool, SandboxConfig

from _timing import timed


def probe_defaults() -> None:
    """Demonstrate three of the on-by-default controls from inside the sandbox."""
    with SandboxPool(SandboxConfig(pool_size=1)) as pool:
        print("=== default hardening (observed from inside the sandbox) ===")

        r = pool.run("import os; print('uid', os.getuid())")
        print(" non-root:        ", r.stdout.strip(), "(uid != 0)")

        # Root filesystem is read-only: writing outside /scratch fails...
        r = pool.run("open('/etc/pwned', 'w').write('x')")
        print(" read-only root:  ", "write to /etc denied" if not r.ok else "WRITABLE?!")

        # ...but the /scratch working dir is writable, so real work still runs.
        r = pool.run("open('ok.txt', 'w').write('x'); print('scratch writable')")
        print(" scratch dir:     ", r.stdout.strip())

        # The service-account token is not mounted -> no pod -> API escalation.
        r = pool.run(
            "import os; "
            "p='/var/run/secrets/kubernetes.io/serviceaccount/token'; "
            "print('token mounted' if os.path.exists(p) else 'no SA token')"
        )
        print(" no SA token:     ", r.stdout.strip())


def hardened_config() -> SandboxConfig:
    """The config you'd run once gVisor + an egress proxy exist in the cluster.

    Constructing it here does NOT require the infrastructure; actually start()ing
    a pool with it does (gVisor needs the RuntimeClass; the proxy needs to be
    deployed and reachable). See the README's "Defense in depth" section.
    """
    return SandboxConfig(
        pool_size=5,
        # opt-in kernel isolation: needs a RuntimeClass named "gvisor" on nodes
        runtime_class="gvisor",
        # opt-in allowlisting egress: route user-code HTTP(S) through this proxy
        # and deny everything else outbound
        egress_proxy="http://egress-proxy.sandbox.svc:3128",
        # for untrusted, multi-tenant input prefer a clean pod per run
        max_uses=1,
        # tighter wall-clock budget for adversarial code
        default_timeout_s=10,
    )


@timed
def main() -> None:
    probe_defaults()
    cfg = hardened_config()
    print("\n=== hardened config (needs gVisor + proxy deployed to start) ===")
    print(" runtime_class =", cfg.runtime_class)
    print(" egress_proxy  =", cfg.egress_proxy)
    print(" max_uses      =", cfg.max_uses)
    print("\nValidates fine without the infrastructure; start() is what needs it.")
    cfg.validate()


if __name__ == "__main__":
    main()
