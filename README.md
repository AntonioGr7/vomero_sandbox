# vomero_sandbox

Run untrusted code in hardened, isolated Kubernetes sandboxes — a warm pool of
locked-down worker pods you exec snippets into. A single, embeddable library.

**Hardened by default.** A bare `SandboxConfig()` already runs workers non-root,
with no service-account token, a read-only root filesystem, all Linux
capabilities dropped, seccomp `RuntimeDefault`, CPU/memory limits, and
default-deny egress. You opt *out* of protections explicitly; you never forget to
opt in.

## Install

```bash
pip install -e ./vomero_sandbox      # or build a wheel; depends only on `kubernetes`
```

Needs a kubeconfig (or in-cluster service account) pointing at a cluster, and
RBAC to manage pods (+ namespaces / networkpolicies if you let the library
create them).

## Testing locally (set up a Kubernetes cluster)

The library talks to whatever cluster your current kubeconfig context points at,
so a one-node local cluster is enough to run everything in [`examples/`](examples/).
Pick the path that matches what you want to exercise. All you need on the host is
`kubectl` plus **one** of `kind` or `minikube` (and Docker).

### Option A — kind (fastest, for everything except gVisor)

```bash
kind create cluster --name sandbox          # kubectl context becomes "kind-sandbox"

# Custom-image example only: build it and load it into the cluster's nodes,
# so IfNotPresent finds it without a registry.
docker build -t vomero-sandbox-runtime:1.0 examples/sandbox-image
kind load docker-image vomero-sandbox-runtime:1.0 --name sandbox

pip install -e .
python examples/quickstart.py               # run/shell/exec
python examples/sessions_and_checkpoints.py # sessions + checkpoint/resume
# ...and the rest of examples/

kind delete cluster --name sandbox          # tear down when done
```

`run_as_user=1000`, read-only root, dropped caps, no SA token, and resource
limits all work out of the box on kind — so the core API, sessions, and
checkpoints are fully testable here.

### Option B — minikube with gVisor (to test `runtime_class="gvisor"`)

gVisor needs `runsc` on the nodes and a matching `RuntimeClass` — infrastructure
the library can't provision. minikube ships an addon that sets both up for you,
which is by far the easiest local path:

```bash
minikube start --container-runtime=containerd   # gVisor requires containerd
minikube addons enable gvisor                    # installs runsc + a RuntimeClass named "gvisor"

# wait until the gvisor pod is Running, then it's ready:
kubectl get pod -n kube-system -l kubernetes.io/minikube-addons=gvisor

minikube image load vomero-sandbox-runtime:1.0   # if using the custom image

pip install -e .
```

The addon creates a `RuntimeClass` named **`gvisor`**, which is exactly what
`SandboxConfig(runtime_class="gvisor")` references — no extra wiring:

```python
from vomero_sandbox import SandboxPool, SandboxConfig

with SandboxPool(SandboxConfig(runtime_class="gvisor", pool_size=1)) as pool:
    r = pool.run("import platform; print(platform.uname().release)")
    print(r.stdout)   # gVisor reports its own synthetic kernel version, not the host's
```

`minikube delete` tears it down. (gVisor on **kind** is possible but fiddly —
you must install `runsc` into the node container and register it as a containerd
runtime by hand; see the [gVisor containerd docs](https://gvisor.dev/docs/user_guide/containerd/quick_start/).
Use minikube for the gVisor path.)

### Verifying the egress lock (needs an enforcing CNI)

The default-deny egress NetworkPolicy is *created* on any cluster, but only
**enforced** by a CNI that implements NetworkPolicy. kind's default `kindnet` and
minikube's default bridge do **not** enforce it — so the policy is silently a
no-op there, and untrusted code could still reach the network. For a faithful
test of the egress controls, start the cluster with Calico:

```bash
minikube start --cni=calico                  # Calico enforces NetworkPolicy
# (on kind: create the cluster with the default CNI disabled, then install
#  Calico or Cilium — see their kind quickstarts.)
```

Then a sandboxed attempt to reach the internet should fail (DNS-only egress),
while functional examples that don't touch the network keep working. If you only
care about the run/exec/session behavior and not the egress boundary, the default
CNI is fine — or set `manage_network_policy=False` to skip the policy entirely
while testing.

## Use

Three ways to say *what* to run — all return the same `RunResult`, all run in a
hardened worker with a writable working directory:

```python
from vomero_sandbox import SandboxPool, SandboxConfig

with SandboxPool(SandboxConfig(pool_size=5)) as pool:
    # 1. run(code) — the configured interpreter (Python by default)
    pool.run("print(sum(range(100)))")                 # -> "4950\n"

    # 2. shell(command) — a shell string via `sh -c` (pipes, globs, redirects)
    pool.shell("grep -rn TODO . | head")

    # 3. exec(argv) — an explicit command vector, no shell (safest/most precise)
    pool.exec(["python", "script.py", "--flag"])
    pool.exec(["grep", "-c", "ERROR", "log.txt"])
```

Which to use:

| Method | Runs | Use when |
|--------|------|----------|
| `run(code)` | `python -c <code>` (or your `interpreter`) | evaluating a code snippet |
| `shell(cmd)` | `sh -c <cmd>` | you want shell features (pipes/globs); agent shell tools |
| `exec(argv)` | `argv` verbatim | you have an exact command; avoids shell re-parsing |
| `run_project(dir, argv)` | uploads a directory tree, runs `argv` in it | a multi-file **package** (folders, local imports, data files) |

A failing run is a **result, not an exception** (the user's code failing is the
normal case):

```python
r = pool.run("raise ValueError('boom')")
r.ok          # False
r.exit_code   # 1
r.stderr      # "...ValueError: boom\n"
r.timed_out   # False
```

The command must exist in the worker's image. The default `python:3.13-slim` has
`python` + coreutils + `grep` + `sh`; use a richer image (`config.image`) for
more tools.

### Per-run environment variables

Pass `env=` to inject environment variables for a single run — e.g. values the
end-user supplied alongside their code:

```python
pool.run(user_code, env={"MODEL": "gpt-x", "USER_TOKEN": user_supplied_token})
```

They're scoped to that one process: they do **not** persist on the reused worker
or leak into the next run (verified), so per-user values are safe to pass here.
Values are passed verbatim (no shell parsing — `"$x; rm -rf /"` is just a
string). Keys must be valid env names. (Needs `env` from coreutils, present by
default.) Works on `run`, `shell`, and `exec`.

> This is for the *caller's* values. Don't hand your platform's own secrets to
> untrusted code — prefer the safe patterns: pre-fetch with `input_files=`, or
> pass a per-run scoped, short-lived token rather than a long-lived secret.

### Providing input files

To run code *against files the user selected*, pass
`input_files=` — they're written into the working directory before the run, so
the code reads them as ordinary local files:

```python
result = pool.run(
    "import csv; rows = list(csv.DictReader(open('data.csv'))); print(len(rows))",
    input_files={
        "data.csv": csv_bytes,            # fetched from your storage/DB
        "cfg/opts.json": b'{"mode":"fast"}',   # nested paths are created
    },
    collect=["report.pdf"],               # …and get results back in the same call
)
```

- Keys are relative paths under the working dir (subdirs auto-created); values
  are `bytes`. Binary-safe (sent over the exec stdin stream, not argv — no length
  limits, no shell parsing).
- Combine `input_files=` + `collect=` for a complete **inputs → process →
  outputs** cycle in one call — the natural shape for a "Quick Action."
- Total input size is capped by `config.max_upload_bytes` (32 MiB default); for
  large inputs, mount a volume or read from object storage inside the sandbox.
- Files land in the workspace, which persists on a reused worker (see "Filesystem
  & state"). For a guaranteed-clean input set per call, use `max_uses=1`.
- Requires `python` in the image (the default has it).

### Getting output files back

When a run *produces files* (a chart, a CSV, a built artifact) rather than just
text, pass `collect=` to pull them out of the working directory into
`result.files` (`{relative_path: bytes}`):

```python
r = pool.run(
    "import matplotlib; matplotlib.use('Agg')\n"
    "import matplotlib.pyplot as plt\n"
    "plt.plot([1,2,3]); plt.savefig('chart.png')",
    collect=["chart.png"],            # specific files…
)
png_bytes = r.files["chart.png"]      # exact bytes, binary-safe

r = pool.shell("make build", collect=True)   # …or collect=True for everything produced
for path, data in r.files.items():
    save(path, data)
```

- **Produce and collect in the *same* call.** Files live on the worker that ran
  the command, and the pool doesn't guarantee the next call hits the same worker.
  So write the file and `collect=` it in one `run`/`shell`/`exec` — don't write in
  one call and collect in another.
- `collect=True` returns *everything* under the working dir on that worker —
  which, on a reused worker, can include files left by earlier runs (see
  "Filesystem & state"). Prefer an explicit list when you know the names.
- Total bytes are capped at `config.max_collect_bytes` (32 MiB default); anything
  over is skipped and listed in `result.files_truncated`. For large outputs,
  write to object storage (S3/blob) from inside the sandbox instead.
- Collection needs `python` in the image (the default has it) and is skipped on
  timeout.

### Running a whole package (`run_project`)

When the unit of work isn't a snippet but a **project** — a package with
sub-folders, local `import`s of sibling modules, data files — use `run_project`.
It uploads a host directory into the working directory and runs an entrypoint
against it. The tree lands under `config.scratch_dir`, which is the cwd and on
`sys.path`, so `python -m app` / `python main.py` and relative imports resolve
just as they do locally:

```python
result = pool.run_project(
    "./user_submission",          # a host directory: the package to run
    ["python", "-m", "app"],      # entrypoint argv (no shell — like exec())
    collect=["out/report.csv"],   # pull back just the produced artifacts
    env={"RUN_ID": "123"},
)
```

For a layout like:

```
user_submission/
  app/__init__.py
  app/__main__.py        # `python -m app` runs this
  app/utils.py           # `from app.utils import …` works
  data/config.json
```

- **Excludes by default**: caches, VCS, virtualenvs, `node_modules` and friends
  (`__pycache__`, `*.pyc`, `.git`, `.venv`, …) are pruned so they never inflate
  the upload. Override with `exclude=(...)`. Symlinks are skipped unless
  `follow_symlinks=True`.
- **`extra_files={relpath: bytes}`** overlays generated files on top of the tree
  (e.g. a config you produced at request time).
- **Size**: the whole tree is uploaded in-band, capped by `config.max_upload_bytes`
  (32 MiB); it fails fast with a clear message if exceeded. For large data, read
  it from object storage inside the sandbox instead of shipping it.
- **`collect`**: prefer an **explicit list** — `collect=True` returns *everything*
  under the working dir, which includes the sources you just uploaded.
- **Dependencies are not installed by this call** — see the next section. The
  package gets the standard library, its own modules, and whatever is baked into
  `config.image`; it can't `pip install` at runtime (read-only rootfs +
  default-deny egress).

`run_project` is sugar over `exec(argv, input_files=…)`: anything it does, you can
do by building the `input_files` map yourself.

### Installing libraries (use a custom image)

The default `python:3.13-slim` has the standard library only. To make `numpy`,
`matplotlib`, `requests`, etc. available to sandboxed code, **bake them into a
worker image** and point `config.image` at it — don't `pip install` at runtime.

Why baked, not runtime-installed: at run time egress is denied (so pip can't
reach PyPI), the root filesystem is read-only (so pip can't write site-packages),
and per-run installs are slow and non-reproducible. Pre-installed = warm pods
ready instantly, offline, identical every time.

```dockerfile
# examples/sandbox-image/Dockerfile
FROM python:3.13-slim
RUN pip install --no-cache-dir numpy==2.2.1 matplotlib==3.10.0 requests==2.32.3
```

```bash
docker build -t vomero-sandbox-runtime:1.0 examples/sandbox-image
kind load docker-image vomero-sandbox-runtime:1.0 --name sandbox   # local kind
# production: push to your registry (ACR on AKS) — see DEPLOYMENT.md
```

```python
cfg = SandboxConfig(image="vomero-sandbox-runtime:1.0")
pool.run("import numpy, matplotlib; print('libs available')")
```

The pre-installed packages live in system site-packages (world-readable), so the
non-root sandbox user imports them fine, and importing is read-only so the
read-only root filesystem is no obstacle. A full worked example (matplotlib chart
→ collect the PNG) is in [`examples/runtime_example.py`](examples/runtime_example.py).

> Some libraries want a writable cache/config dir at import time (e.g. matplotlib
> → `MPLCONFIGDIR`). Point them at the writable workspace from inside your code:
> `os.environ["MPLCONFIGDIR"] = "/scratch"` before importing.

> Runtime `pip install` *is* possible but only if you deliberately open egress to
> PyPI (via the egress proxy, allowlisting `pypi.org` + `files.pythonhosted.org`)
> and install to a writable path (`pip install --target=/scratch/libs` +
> `PYTHONPATH`). It's slow and fights the hardening — prefer a baked image.

Or manage the lifecycle yourself (e.g. in a long-lived service — see
[`examples/use_in_service.py`](examples/use_in_service.py)):

```python
pool = SandboxPool(SandboxConfig(pool_size=5))
pool.start()            # warm once at startup
...
pool.run(code)          # fast exec per request, from any thread
...
pool.close()            # delete workers at shutdown
```

### What `run()` returns

A `RunResult` — and a failing run is a **result, not an exception** (the user's
code failing is the normal case):

| field | meaning |
|-------|---------|
| `stdout`, `stderr` | captured output |
| `exit_code` | `0` clean · non-zero the code raised · `None` killed before exit |
| `timed_out` | the wall-clock limit fired (the worker was retired) |
| `duration_s` | wall-clock seconds |
| `sandbox_id` | worker pod that served it (for tracing) |
| `files` | `{relpath: bytes}` collected outputs (empty unless `collect=`) |
| `files_truncated` | paths skipped for exceeding `max_collect_bytes` |
| `.ok` | `True` iff exit 0 and not timed out |

Only **infrastructure** failures raise (`SandboxStartupError`, `SandboxError`, …).

## Using it from async code

The pool is **synchronous and thread-safe**, and there's no async API — on
purpose. Your concurrency ceiling is `pool_size` (the number of pods), so a
handful of blocking calls on threads is all you ever need; an event loop buys no
extra throughput when the bottleneck is the cluster, not the I/O wait. So from
`asyncio`, bridge with a thread instead of blocking the loop:

```python
import asyncio

result = await asyncio.to_thread(pool.run, code, timeout_s=10)
```

For many concurrent jobs, bound them to the pool with a dedicated executor sized
to `pool_size` (so a `run()` always has a thread to block in, and surplus work
queues instead of spawning unbounded threads):

```python
import asyncio, functools
from concurrent.futures import ThreadPoolExecutor

POOL_SIZE = 5
sandbox_exec = ThreadPoolExecutor(max_workers=POOL_SIZE)

async def run_in_sandbox(code: str, **kw):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(sandbox_exec, functools.partial(pool.run, code, **kw))

results = await asyncio.gather(*(run_in_sandbox(c) for c in snippets))
```

### A queue consumer (aio-pika)

The same shape works for an async broker consumer. Set the prefetch to
`pool_size` for backpressure (the broker holds surplus work instead of your
process), run each job on the sized executor, and `ack`/`nack` right in the
coroutine — no thread-safe marshaling needed, since aio-pika's callbacks already
run on the loop:

```python
import asyncio, functools, json
from concurrent.futures import ThreadPoolExecutor
import aio_pika
from vomero_sandbox import SandboxPool, SandboxConfig, SandboxError

POOL_SIZE = 5
pool = SandboxPool(SandboxConfig(pool_size=POOL_SIZE, max_uses=1, default_timeout_s=30))
sandbox_exec = ThreadPoolExecutor(max_workers=POOL_SIZE)


async def on_message(message: aio_pika.abc.AbstractIncomingMessage) -> None:
    loop = asyncio.get_running_loop()
    try:
        req = json.loads(message.body)
        # In reality: download the code + inputs from blob storage (also off-loop).
        code = await loop.run_in_executor(sandbox_exec, download_blob, req["code_path"])
        result = await loop.run_in_executor(sandbox_exec, functools.partial(
            pool.run, code.decode(), input_files={"main.py": code},
            timeout_s=req.get("timeout_s", 30), collect=req.get("outputs")))
        await loop.run_in_executor(sandbox_exec, persist_result, req["id"], result)
        await message.ack()                      # user-code failure is a *result*, still ack
    except SandboxError:
        await message.nack(requeue=False)        # infra failure → dead-letter queue
    except Exception:
        await message.nack(requeue=False)


async def main() -> None:
    await asyncio.to_thread(pool.start)          # warm once (the warm-up is blocking)
    conn = await aio_pika.connect_robust("amqp://rabbitmq/")
    try:
        channel = await conn.channel()
        await channel.set_qos(prefetch_count=POOL_SIZE)   # ← backpressure to pool capacity
        queue = await channel.declare_queue("jobs", durable=True)
        await queue.consume(on_message)          # manual ack (no_ack defaults False)
        await asyncio.Future()                   # run until cancelled
    finally:
        await conn.close()
        await asyncio.to_thread(pool.close)      # delete all worker pods
```

`prefetch_count == pool_size == executor workers` gives a clean 1:1: at most
`pool_size` jobs are ever in flight, the rest wait at the broker. Same rules as
the threaded version — bound timeouts, prefer `max_uses=1` for untrusted
multi-tenant input, and dead-letter infra failures rather than blind-requeueing
a poison message. (A native `kubernetes_asyncio` rewrite would double the
maintenance for no throughput gain, so the thread bridge is the recommended
pattern, not a stopgap.)

## Configuration

Everything is on `SandboxConfig`; the security-relevant defaults are the strict
ones. Common knobs:

```python
SandboxConfig(
    namespace="sandbox",
    image="python:3.13-slim",
    pool_size=3,                # warm workers = your concurrency ceiling
    max_uses=25,                # retire a worker after N runs (bounds state leakage)
    max_age_s=600,              # retire a worker older than this
    default_timeout_s=30,       # per-run wall-clock limit

    # leak protection (see "Cleanup & leaked workers"):
    idle_shutdown_s=1800,       # workers self-terminate after this much idle time
    auto_cleanup=True,          # close() on atexit / SIGTERM
    reclaim_on_start=False,     # sweep orphaned workers at startup (single-pool only)

    # opt-in, require cluster-side setup:
    runtime_class="gvisor",                              # kernel isolation
    egress_proxy="http://egress-proxy.sandbox.svc:3128", # allowlisting egress

    # let the library create cluster objects, or False if GitOps owns them:
    manage_namespace=True,
    manage_network_policy=True,
)
```

### Defense in depth (what's on, and what each stops)

| Control | Default | Stops |
|---------|:-------:|-------|
| resource limits | on | node-level DoS (memory/CPU exhaustion) |
| no SA token | on | pod → Kubernetes API → cluster escalation |
| non-root + drop caps + no-priv-esc | on | privileged ops, packet forging, escalation |
| read-only root + scratch volume | on | tampering with the image (writes confined to `/scratch`) |
| seccomp RuntimeDefault | on | a swath of the kernel syscall surface |
| default-deny egress | on | exfiltration, the cloud metadata endpoint |
| **gVisor** (`runtime_class`) | **off** | **host-kernel exploits** (a real kernel boundary) |
| **egress allowlist** (`egress_proxy`) | **off** | reaching only vetted external APIs |

The two `off` controls need infrastructure this library can't provision:

- **gVisor** — install `runsc` + a `RuntimeClass` named to match `runtime_class`
  on your nodes. (See your distro's gVisor docs / GKE Sandbox.)
- **Egress proxy** — deploy an allowlisting forward proxy (Squid/Envoy) and point
  `egress_proxy` at it. The library then wires `HTTPS_PROXY` into workers and
  narrows egress to the proxy.

> **Secrets:** never bake your own API keys into code you run here — untrusted
> code can read and exfiltrate them, and the egress proxy's one allowed hop is a
> perfect exfil channel. Use per-caller keys or a key-injecting broker.

## Cleanup & leaked workers

Workers are pods on the **cluster**, not children of your process — so if the
controlling process dies without calling `close()`, they keep running and
consuming resources. Always prefer the `with` form (or `try/finally: pool.close()`),
but that only covers *graceful* exits. Three layers guard the rest, in order of
how much they survive:

| Layer | Covers | Misses |
|-------|--------|--------|
| `with` / `try-finally` | exceptions, `Ctrl-C` | SIGTERM, SIGKILL, crashes |
| **`auto_cleanup`** (atexit + SIGTERM handler) | unhandled-exception exit, normal exit, `kill <pid>` | SIGKILL, segfault, OOM, node loss |
| **`idle_shutdown_s`** (in-pod watchdog) | *everything*, including a hard-killed or vanished controller | — |
| **`reclaim_on_start`** (startup sweep) | orphans left by a previous crashed run | live peers' workers if they share an `app_label` |

Only the **idle watchdog is cluster-side**, so it's the one that survives a
`kill -9`, a panic, or a dead node: each worker's main process watches for
activity and, if no run touches it for `idle_shutdown_s` (default 30 min), exits
on its own — the pod stops and frees its CPU/memory. An actively-used warm pool
keeps resetting that timer, so it never fires in normal operation; it's purely a
backstop. Set `idle_shutdown_s=None` to disable (workers then live until deleted).

`auto_cleanup` (on by default) registers an `atexit` hook and a SIGTERM handler
so graceful shutdowns close the pool immediately rather than waiting out the idle
timer. It chains any SIGTERM handler you installed earlier, and silently does
nothing if the pool is constructed off the main thread (where signals can't be
set) — the watchdog still covers that case.

`reclaim_on_start` (off by default) makes `start()` delete any pre-existing
workers carrying this pool's `app_label` before warming, sweeping up orphans from
a prior crash. Enable it **only** when an `app_label` maps to a single
pool/controller — if several pools or replicas share one, reclaim can't tell a
live peer's workers from orphans and will delete them. To clean up by hand at any
time: `kubectl delete pod -n <ns> -l app=<app_label>,role=worker`.

> One trade-off: if a *live* pool sits idle longer than `idle_shutdown_s`, its
> workers self-terminate and the next run pays for a replacement (and may surface
> one error as the dead worker is detected and retired). Keep `idle_shutdown_s`
> comfortably above your expected inter-run gap.

A runnable demonstration is in [`examples/cleanup_and_leaks.py`](examples/cleanup_and_leaks.py).

## Filesystem & state (read this for agent use)

The working directory is a writable `/scratch` volume (an `emptyDir`). It lives
for the **worker's** lifetime, which has two consequences you must design around:

- **State persists across runs on the same worker.** If two calls happen to land
  on the same worker, the second sees files the first wrote. A worker is wiped
  only when it's **retired** (after `max_uses` runs or `max_age_s`) or the pool
  closes.
- **Routing is not sticky.** Consecutive `run`/`shell`/`exec` calls are *not*
  guaranteed the same worker (the pool hands out whichever is free). So you can
  neither rely on state carrying over **nor** assume a clean slate.

Pick the model you need:

| You want | Do this |
|----------|---------|
| Independent, isolated calls (multi-tenant, untrusted) | `max_uses=1` — one run per worker, then retired. Loses warm-pool speed but guarantees a clean workspace and no cross-call leakage. |
| Fast reuse, calls are same-trust (e.g. one agent's own steps) | keep `max_uses` high; tolerate shared `/scratch`, bounded by retirement. |
| A stateful multi-step **session** (write a file, run it, grep output) | `with pool.session() as s:` — one worker, shared `/scratch` across calls, no per-call wipe. Checkpoint it to a blob to continue across turns/processes. See "Sessions & checkpoints" below. |

> For untrusted, multi-tenant input, prefer `max_uses=1` (or a per-tenant pool).
> Warm reuse trades isolation for latency; that trade is yours to make explicitly.

## Sessions & checkpoints (stateful multi-step work)

When the work is a *sequence* of dependent steps — write a file, run it, grep the
output, build then test — open a **session**. It leases one worker for the whole
`with` block and does **not** wipe `/scratch` between calls, so each call sees the
files the previous one wrote:

```python
with pool.session() as s:
    s.run_project("./repo", ["python", "-m", "build"])
    r = s.shell("pytest -q")          # sees the build output — same /scratch
    print(r.ok)
```

A session pins a worker for its lifetime, so concurrent sessions are bounded by
`pool_size`. State is **filesystem-only** — each call still runs a fresh process,
so nothing in memory (variables, imports) carries between calls; persist what you
need to disk. On exit a healthy session's workspace is wiped and the worker
returns to the pool; a timeout or error retires the worker and ends the session.

**Checkpoints** let a session outlive the `with` block — across a turn boundary, a
different process, or a different replica. `checkpoint()` packs the working dir
into a portable blob you store yourself (e.g. in your conversation store);
`pool.resume(blob)` restores it into a fresh worker and hands you a session
continuing from exactly that filesystem state:

```python
# turn 1 — do some work, then persist the workspace
with pool.session() as s:
    s.run("open('progress.txt', 'w').write('step 1 done\\n')")
    blob = s.checkpoint()             # bytes — store wherever you keep session state
save_to_store(conversation_id, blob)

# turn 2 — later, possibly on another replica — pick up where it left off
with pool.resume(load_from_store(conversation_id)) as s:
    s.run("print(open('progress.txt').read())")   # -> "step 1 done"
    new_blob = s.checkpoint()         # persist the advanced state again
```

The session lives in the blob, not in a held-open pod — so nothing is wasted
between turns. Notes:

- **Filesystem only.** A checkpoint captures files, not process/memory state.
- **Checkpoint before risky steps** if durability matters: a timeout/crash loses
  the live workspace, but a stored checkpoint is safe.
- **Size**: a checkpoint is bounded by `config.max_collect_bytes` and a resume by
  `config.max_upload_bytes` (32 MiB each). `checkpoint()` *raises* rather than
  silently dropping files if the workspace is too big — keep large artifacts in
  object storage, not the session workspace.
- The blob is a gzip'd tar; `resume()` rejects unsafe member paths (no traversal
  out of `/scratch`).

A full runnable walkthrough is in
[`examples/sessions_and_checkpoints.py`](examples/sessions_and_checkpoints.py).

## Roadmap

- **stdin** — stream stdin into a run.
- **microVM isolation** (Firecracker/Kata) — a stronger boundary than gVisor;
  swappable via `runtime_class`.

## Notes & limits

- **Language:** ships with the Python interpreter for `run()` (`["python","-c"]`).
  For another language set `image` + `interpreter`; or just use `shell()` /
  `exec()` for any tool present in the image.
- **Concurrency:** `pool_size` is the parallelism ceiling; extra concurrent
  calls block until a worker frees up. Thread-safe.
- **Timeouts** are enforced client-side (warm pods have no pod-level deadline); a
  timed-out worker is retired, not reused.
- **NetworkPolicy needs an enforcing CNI** (Calico, Cilium, modern kindnet). With
  a non-enforcing CNI the egress lock is silently a no-op — verify it.
