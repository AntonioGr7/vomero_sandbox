"""Per-run environment variables.

Pass env= to inject variables for a SINGLE run — e.g. values the end-user
supplied alongside their code. They are scoped to that one process: they do NOT
persist on the reused worker, and they do NOT leak into the next run. Values are
passed verbatim (no shell parsing), so even "$x; rm -rf /" is just a string.

Works on run(), shell(), and exec(). Needs a cluster (see examples/README.md).
"""

from vomero_sandbox import SandboxPool, SandboxConfig

from _timing import timed


@timed
def main() -> None:
    with SandboxPool(SandboxConfig(pool_size=1)) as pool:
        # Inject two variables for this run only.
        r = pool.run(
            "import os; print(os.environ['MODEL'], os.environ['USER_TOKEN'])",
            env={"MODEL": "gpt-x", "USER_TOKEN": "tok_abc123"},
        )
        print("with env ->", r.stdout.strip())        # gpt-x tok_abc123

        # The next run on the same worker does NOT see them — env is per-process.
        r = pool.run("import os; print('MODEL' in os.environ)")
        print("leaked?  ->", r.stdout.strip())        # False

        # Values are passed as argv elements, never through a shell, so shell
        # metacharacters in a value are inert — this prints the literal string.
        r = pool.run(
            "import os; print(repr(os.environ['DANGEROUS']))",
            env={"DANGEROUS": "$(whoami); rm -rf /"},
        )
        print("verbatim ->", r.stdout.strip())

        # env= composes with shell() and exec() too.
        r = pool.shell('echo "region=$REGION"', env={"REGION": "eu-south-1"})
        print("shell    ->", r.stdout.strip())        # region=eu-south-1


if __name__ == "__main__":
    main()
