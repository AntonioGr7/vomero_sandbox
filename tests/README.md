# Tests

Integration smoke tests. **They need a real Kubernetes cluster** (kind/minikube —
see [../README.md#testing-locally](../README.md#testing-locally-set-up-a-kubernetes-cluster))
because they assert on actual pod lifecycle, not mocks. They run against a
dedicated `app_label` (`vomero-smoketest`) so they never touch real workers.

| File | Validates |
|------|-----------|
| [smoke_idle_shutdown.py](smoke_idle_shutdown.py) | the cluster-side leak guard: an idle worker self-terminates (→ `Succeeded`) within `idle_shutdown_s`, and a busy worker stays `Running`. |

```bash
pip install -e ..
python tests/smoke_idle_shutdown.py     # exits 0 on pass, 1 on failure
# or:  pytest tests/smoke_idle_shutdown.py
```

`smoke_idle_shutdown.py` takes roughly a minute (it uses a short 15s idle TTL).
Watch it happen live in another terminal:

```bash
kubectl get pods -n sandbox -l app=vomero-smoketest -w
```
