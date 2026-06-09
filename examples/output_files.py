"""Pulling produced files back out of the sandbox.

When a run *produces files* (a chart, a CSV, a built artifact) rather than just
text, pass collect= to retrieve them into result.files ({relpath: bytes}):

    collect=["a.png", "out/b.csv"]   -> just those paths
    collect=True                     -> every file under the working dir

IMPORTANT: produce and collect in the SAME call. Files live on the worker that
ran the command, and the pool does not guarantee the next call hits the same
worker — so write the file and collect it together. Total bytes are capped by
config.max_collect_bytes; anything over is skipped and listed in
result.files_truncated.

Needs a cluster (see examples/README.md).
"""

from vomero_sandbox import SandboxPool, SandboxConfig

from _timing import timed


@timed
def main() -> None:
    with SandboxPool(SandboxConfig(pool_size=1)) as pool:
        # Write two files, then collect them by name in the same run.
        r = pool.run(
            "open('report.txt', 'w').write('hello from the sandbox\\n')\n"
            "open('data.bin', 'wb').write(bytes(range(256)))",
            collect=["report.txt", "data.bin"],
        )
        print("collected:", sorted(r.files))                      # ['data.bin', 'report.txt']
        print("report.txt ->", r.files["report.txt"].decode().strip())
        print("data.bin   ->", len(r.files["data.bin"]), "bytes")  # 256, binary-safe

        # collect=True grabs everything under the working dir. Prefer an explicit
        # list when you know the names — on a reused worker, "everything" can
        # include files left by earlier runs.
        r = pool.shell("mkdir -p out && echo built > out/artifact.txt", collect=True)
        print("\ncollect=True ->", sorted(r.files))

        if r.files_truncated:
            print("skipped (too big):", r.files_truncated)


if __name__ == "__main__":
    main()
