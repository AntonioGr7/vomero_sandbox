"""A registered package, run inside the sandbox by examples/queue_consumer.py.

Convention shared by every package in the registry: each is a directory with a
``__main__.py``, run with ``python .`` (so ``sys.path[0]`` is the working dir and
sibling modules import cleanly). The per-job INPUT is dropped in beside the code
as ``input.json`` (via run_project(extra_files=...)); the produced artifact is
written to ``out.json`` for the service to collect.
"""

import json

from compute import summarize   # sibling module, resolves from the working dir


def main() -> None:
    with open("input.json") as f:
        numbers = json.load(f)

    summary = summarize(numbers)
    with open("out.json", "w") as f:
        json.dump(summary, f)

    print(f"stats: n={summary['count']} sum={summary['sum']} mean={summary['mean']}")


if __name__ == "__main__":
    main()
