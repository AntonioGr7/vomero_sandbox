"""Carrying in-memory VARIABLES across turns — so the agent finds them by NAME.

A sandbox run is a fresh process: when it exits, RAM is gone. Files survive
(checkpoint/resume restores the workspace), but the variables your agent built up
do not. And an agent — especially an LLM writing code across turns — expects to
find its variables again *by name* (`last_answer`, `memo`), exactly as it left
them, like a notebook kernel that kept running. It does NOT want to dig them out
of a `state` dict.

So instead of making the agent read/write an explicit container, a platform-owned
HARNESS wraps the agent's code: it restores the saved variables INTO THE GLOBAL
NAMESPACE before the code runs, and captures them again after. The agent author
writes plain code with bare variables — no pickle plumbing, no `state[...]`. The
persistence happens around their code, not inside it.

Two harnesses, side by side, with different fidelity:

  • STDLIB (pickle): restores DATA variables by name (dicts, lists, numbers,
    strings). No image dependency. Cannot carry functions/lambdas/classes the
    agent defined (a __main__ function doesn't survive into a fresh process), and
    it also drags along any stray data globals — whole-namespace capture keeps
    junk too. Robust for plain data.

  • DILL (dill.dump_session/load_session): restores the WHOLE namespace —
    including functions, closures, classes — so the agent finds *everything* as it
    left it. The fullest "persistent REPL" experience, but needs `dill` in the
    worker image and is more fragile (breaks on unpicklable objects: open handles,
    threads, live generators).

Either way the captured file rides the existing checkpoint()/resume() machinery
(see the docstrings in runner.py) to cross turns/pods. The blob is trusted-only:
unpickling executes code, so sign it (HMAC) if it's ever user-reachable, and only
ever unpickle inside the sandbox. Keep big objects on disk/object storage; persist
only light state.

Needs a cluster (see examples/README.md).
"""

from vomero_sandbox import SandboxPool, SandboxConfig

from _timing import timed

# ---------------------------------------------------------------------------
# The AGENT's own code. Note what's NOT here: no pickle, no load/save, no state
# dict. Just bare variables. Turn 1 creates them; turn 2 USES them by name (with
# no initialization) — so if the harness didn't restore the namespace, turn 2
# would NameError. This is what an LLM writing follow-up code naturally expects.
# ---------------------------------------------------------------------------

AGENT_TURN_1 = r'''
memo = {}                                  # a cache of (expensive) sub-results
history = []
bump = lambda x: x + 100                   # a closure the agent defined this turn

nums = [3, 1, 4, 1, 5]
for n in nums:
    memo[n] = sum(range(n + 1))            # pretend each was expensive -> memoize
last_answer = sum(nums)
history.append(("sum " + " ".join(map(str, nums)), last_answer))

print(f"[turn 1] answer={last_answer}  (defined memo, history, bump())")
'''

AGENT_TURN_2 = r'''
# A follow-up. The agent references variables it created LAST turn, by name, with
# no setup — it expects them to still be in scope.
print(f"[turn 2] found by name: last_answer={last_answer}  memo_entries={len(memo)}  "
      f"history_len={len(history)}")

if "bump" in dir():                         # did the FUNCTION survive too?
    answer = bump(last_answer)
    print(f"[turn 2] bump() survived (dill) -> {answer}")
else:
    answer = last_answer + 100              # stdlib harness carried data but not the function
    print(f"[turn 2] bump() was NOT restored (data-only stdlib harness); recomputed -> {answer}")

last_answer = answer
history.append(("add 100", answer))
'''


# ---------------------------------------------------------------------------
# The HARNESSES. The platform wraps the agent code with one of these; the agent
# author never writes this part. Each is a prelude (restore namespace) + the
# agent code + an epilogue (capture namespace).
# ---------------------------------------------------------------------------

_STDLIB_PRELUDE = r'''
import pickle as _pickle, types as _types
_NS = "ns.pkl"
try:
    with open(_NS, "rb") as _f:
        globals().update(_pickle.load(_f))      # prior turn's DATA variables, bound by name
except FileNotFoundError:
    pass
'''

_STDLIB_EPILOGUE = r'''
_keep = {}
for _k, _v in list(globals().items()):
    if _k.startswith("_"):
        continue                                 # harness internals
    if isinstance(_v, _types.ModuleType) or callable(_v):
        continue                                 # modules + functions/classes don't round-trip via stdlib pickle
    try:
        _pickle.dumps(_v)                         # keep only what actually pickles
    except Exception:
        continue
    _keep[_k] = _v
with open(_NS, "wb") as _f:
    _pickle.dump(_keep, _f)
'''

_DILL_PRELUDE = r'''
import dill as _dill
try:
    _dill.load_session("session.pkl")            # restore the WHOLE namespace (incl. functions)
except FileNotFoundError:
    pass
'''

_DILL_EPILOGUE = r'''
_dill.dump_session("session.pkl")                # capture the WHOLE namespace
'''


def with_stdlib_harness(agent_code: str) -> str:
    """Wrap agent code so its DATA variables persist by name (stdlib only)."""
    return _STDLIB_PRELUDE + "\n" + agent_code + "\n" + _STDLIB_EPILOGUE


def with_dill_harness(agent_code: str) -> str:
    """Wrap agent code so its WHOLE namespace persists by name (needs dill in the image)."""
    return _DILL_PRELUDE + "\n" + agent_code + "\n" + _DILL_EPILOGUE


# Default to the stdlib harness so this runs on the stock python:3.13-slim image.
# Swap to with_dill_harness once your worker image has `dill` (then bump() survives).
wrap = with_stdlib_harness


@timed
def main() -> None:
    with SandboxPool(SandboxConfig(pool_size=1)) as pool:
        # ---- Turn 1: a fresh conversation ----
        print("=== turn 1 ===")
        with pool.session() as s:
            r1 = s.run(wrap(AGENT_TURN_1), env={})
            print(r1.stdout.strip())
            blob = s.checkpoint()              # packs the workspace (ns.pkl) into a blob

        store = blob                           # in reality: save to your conversation store
        print(f"  [stored checkpoint: {len(store)} bytes]\n")

        # ... the user comes back with a follow-up; the original pod is long gone ...

        # ---- Turn 2: resume; the agent finds its variables by name ----
        print("=== turn 2 (follow-up) ===")
        with pool.resume(store) as s:
            r2 = s.run(wrap(AGENT_TURN_2), env={})
            print(r2.stdout.strip())
            blob2 = s.checkpoint()             # persist the new state for a turn 3
        print(f"\n  [stored updated checkpoint: {len(blob2)} bytes]")


if __name__ == "__main__":
    main()
