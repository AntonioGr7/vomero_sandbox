"""vomero_sandbox — run untrusted code in hardened, isolated Kubernetes sandboxes.

A small library that maintains a warm pool of locked-down worker pods and execs
code snippets into them. Hardened by default (non-root, no API token, read-only
root, dropped capabilities, resource limits, default-deny egress); optional
gVisor kernel isolation and allowlisting egress proxy.

    from vomero_sandbox import SandboxPool, SandboxConfig

    with SandboxPool(SandboxConfig(pool_size=5)) as pool:
        result = pool.run("print(2 + 2)", timeout_s=10)
        print(result.stdout, result.exit_code)
"""

from .config import SandboxConfig
from .errors import (
    SandboxConfigError,
    SandboxConfigWarning,
    SandboxError,
    SandboxPoolClosed,
    SandboxStartupError,
)
from .models import PoolStartReport, RunResult, StreamChunk
from .runner import SandboxPool, SandboxSession, SandboxStream

__version__ = "0.1.0"

__all__ = [
    "SandboxPool",
    "SandboxSession",
    "SandboxStream",
    "SandboxConfig",
    "RunResult",
    "StreamChunk",
    "PoolStartReport",
    "SandboxError",
    "SandboxConfigError",
    "SandboxConfigWarning",
    "SandboxStartupError",
    "SandboxPoolClosed",
]
