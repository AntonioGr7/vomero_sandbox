"""The three ways to say *what* to run — and what a failing run looks like.

run(code)   -> the configured interpreter (python -c <code>)
shell(cmd)  -> a shell string via `sh -c` (pipes, globs, redirects)
exec(argv)  -> an explicit command vector, no shell (safest / most precise)

All three return the same RunResult. A non-zero exit or a timeout is a normal
*result* you inspect — only infrastructure failures raise.

Needs a cluster (see examples/README.md).
"""

from vomero_sandbox import SandboxPool, SandboxConfig


def main() -> None:
    # The context manager warms the pool on enter and deletes the workers on exit.
    with SandboxPool(SandboxConfig(pool_size=2)) as pool:

        # 1. run(code): your snippet is appended to config.interpreter (python -c).
        r = pool.run("print(sum(range(100)))")
        print("run   ->", r.stdout.strip())          # 4950

        # 2. shell(command): full shell features — here a pipe into head.
        r = pool.shell("seq 1 100 | tail -3")
        print("shell ->", r.stdout.replace("\n", " ").strip())   # 98 99 100

        # 3. exec(argv): the command vector runs verbatim, no shell re-parsing.
        r = pool.exec(["python", "-c", "import sys; print(sys.version.split()[0])"])
        print("exec  ->", r.stdout.strip())           # e.g. 3.13.x

        # A failing run is a RESULT, not an exception. Inspect the fields.
        r = pool.run("raise ValueError('boom')")
        print("\nfailing run:")
        print("  ok        =", r.ok)                  # False
        print("  exit_code =", r.exit_code)           # 1
        print("  stderr    =", r.stderr.strip().splitlines()[-1])   # ValueError: boom
        print("  timed_out =", r.timed_out)           # False

        # A per-run timeout is enforced client-side; the worker is retired.
        r = pool.run("import time; time.sleep(10)", timeout_s=1)
        print("\ntimed-out run: timed_out =", r.timed_out, " exit_code =", r.exit_code)


if __name__ == "__main__":
    main()
