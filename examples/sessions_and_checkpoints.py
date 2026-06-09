"""Stateful multi-step work: sessions, and checkpoints that survive turn boundaries.

A SESSION leases one worker for a sequence of dependent calls that SHARE a
working directory — no per-call wipe — so a file written by one call is visible
to the next. It's the right primitive for "write a file, run it, grep the
output" or "build, then test" flows.

A CHECKPOINT lets that session outlive its `with` block. checkpoint() packs the
working dir into a portable blob you store yourself; pool.resume(blob) restores
it into a fresh worker and continues from exactly that filesystem state — in a
different process, replica, or turn. The session lives in the blob, not in a
held-open pod.

Two things to remember:
  - State is FILESYSTEM only — each call runs a fresh process, so persist to disk
    what you need to carry forward (variables/imports do NOT survive).
  - checkpoint before risky steps if durability matters: a timeout/crash loses
    the live workspace, but a stored checkpoint is safe.

Needs a cluster (see examples/README.md).
"""

from vomero_sandbox import SandboxPool, SandboxConfig

from _timing import timed


def shared_workspace(pool: SandboxPool) -> None:
    """Within a session, later calls see files earlier calls wrote."""
    print("=== a session shares /scratch across calls ===")
    with pool.session() as s:
        print(" worker:", s.sandbox_id)

        # Step 1: write a module to disk.
        s.run("open('greet.py', 'w').write('def hello(): return \"hi from step 1\"\\n')")

        # Step 2: a FRESH process — but the file is still there, so import works.
        r = s.run("import greet; print(greet.hello())")
        print(" step 2 ->", r.stdout.strip())          # hi from step 1

        # Step 3: shell tools see it too (same workspace).
        r = s.shell("wc -l < greet.py")
        print(" step 3 ->", r.stdout.strip(), "line(s) in greet.py")

        # In-memory state does NOT carry over (fresh interpreter each call):
        s.run("x = 42")
        r = s.run("print('x' in dir())")
        print(" memory carries over?", r.stdout.strip())   # False


def checkpoint_across_turns(pool: SandboxPool) -> bytes:
    """Turn 1: build up some state, then persist it to a blob you store."""
    print("\n=== turn 1: work, then checkpoint() to a portable blob ===")
    with pool.session() as s:
        s.run("open('progress.txt', 'w').write('step 1 done\\n')")
        s.run("open('progress.txt', 'a').write('step 2 done\\n')")
        blob = s.checkpoint()                  # bytes: store wherever you keep session state
    print(f" checkpoint blob: {len(blob)} bytes (gzip'd tar of the workspace)")
    return blob


def resume_from_checkpoint(pool: SandboxPool, blob: bytes) -> None:
    """Turn 2 (possibly another process/replica): resume and continue the work."""
    print("\n=== turn 2: resume() the blob on a fresh worker and continue ===")
    with pool.resume(blob) as s:
        print(" resumed on worker:", s.sandbox_id)
        r = s.run("print(open('progress.txt').read().strip())")
        print(" restored state ->", r.stdout.strip().replace("\n", " | "))

        # Advance the work and re-checkpoint for the next turn.
        s.run("open('progress.txt', 'a').write('step 3 done\\n')")
        new_blob = s.checkpoint()
        print(f" re-checkpointed: {len(new_blob)} bytes")


@timed
def main() -> None:
    # pool_size=1 keeps the demo deterministic; sessions work with any size
    # (each session holds one worker, so pool_size bounds concurrent sessions).
    with SandboxPool(SandboxConfig(pool_size=1)) as pool:
        shared_workspace(pool)
        blob = checkpoint_across_turns(pool)
        # ... in reality you'd persist `blob` to your store and reload it later ...
        resume_from_checkpoint(pool, blob)


if __name__ == "__main__":
    main()
