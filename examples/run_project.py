"""Running a whole multi-file package with run_project().

When the unit of work isn't a snippet but a *project* — a package with
sub-folders, local imports of sibling modules, data files — upload the whole
host directory and run an entrypoint against it. The tree lands under the
working dir (which is the cwd and on sys.path), so `python -m app`, relative
imports, and reading bundled data files all resolve just as they do locally.

This runs examples/sample_project/ (see its layout below) and collects the one
artifact it writes. Needs a cluster (see examples/README.md).

    sample_project/
      app/__init__.py
      app/__main__.py     # `python -m app` runs this
      app/stats.py        # imported as `from app.stats import ...`
      data/numbers.txt    # a bundled data file the code reads
"""

import os

from vomero_sandbox import SandboxPool, SandboxConfig

from _timing import timed

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.join(HERE, "sample_project")


@timed
def main() -> None:
    with SandboxPool(SandboxConfig(pool_size=1)) as pool:
        result = pool.run_project(
            PROJECT,
            ["python", "-m", "app"],          # entrypoint argv (no shell, like exec())
            collect=["out/result.json"],      # explicit list: just the produced artifact
            env={"RUN_ID": "demo-123"},
            # caches/VCS/venvs are excluded by default; override with exclude=(...)
            # overlay generated files on top of the tree with extra_files={...}
        )

        print(result.stdout.strip())
        if result.ok and "out/result.json" in result.files:
            print("artifact ->", result.files["out/result.json"].decode().strip())


if __name__ == "__main__":
    main()
