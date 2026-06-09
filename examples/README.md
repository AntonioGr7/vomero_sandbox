# Examples

Runnable examples for `vomero_sandbox`, one per capability. They progress from
the basics to lifecycle and hardening.

**All of these need a real Kubernetes cluster** reachable via your kubeconfig (or
an in-cluster service account), with RBAC to manage pods — and namespaces /
networkpolicies if you let the library create them (the default). A local
[kind](https://kind.sigs.k8s.io/) or [minikube](https://minikube.sigs.k8s.io/)
cluster is plenty: see **[Testing locally](../README.md#testing-locally-set-up-a-kubernetes-cluster)**
in the main README for one-command setup (including the gVisor path via minikube).

```bash
# after creating a local cluster (see the link above):
pip install -e ..        # install vomero_sandbox from the repo root
python quickstart.py     # then run any example
```

| File | Shows |
|------|-------|
| [quickstart.py](quickstart.py) | `run` / `shell` / `exec`, and that a failing run is a *result*, not an exception |
| [env_vars.py](env_vars.py) | per-run environment variables (`env=`), scoped to one process |
| [input_files.py](input_files.py) | staging files into the workspace before a run (`input_files=`) |
| [output_files.py](output_files.py) | pulling produced files back out (`collect=`) |
| [input_output_cycle.py](input_output_cycle.py) | a full inputs → process → outputs round-trip in one call |
| [run_project.py](run_project.py) | uploading and running a whole multi-file package (`run_project`) |
| [runtime_example.py](runtime_example.py) | a custom worker image with `numpy`/`matplotlib`, charting → collect the PNG |
| [use_in_service.py](use_in_service.py) | the long-lived-service lifecycle: `start()` once, `run()` per request, `close()` at shutdown |
| [isolation_and_state.py](isolation_and_state.py) | filesystem state across runs, and the `max_uses` / `fresh_workdir_per_run` knobs |
| [sessions_and_checkpoints.py](sessions_and_checkpoints.py) | multi-step `pool.session()` with a shared workspace; `checkpoint()` / `resume()` to continue across turns/processes |
| [hardening.py](hardening.py) | the security defaults, opt-in gVisor + egress proxy, and partial-pool startup |

Supporting assets:

- [sample_project/](sample_project/) — the package uploaded by `run_project.py`.
- [sandbox-image/Dockerfile](sandbox-image/Dockerfile) — the custom worker image for `runtime_example.py`.
