"""Entrypoint for `python -m app`, run inside the sandbox.

Demonstrates the three things run_project() makes work out of the box:
  - a local import of a sibling module (app.stats),
  - reading a bundled data file by relative path (data/numbers.txt),
  - writing an artifact the caller collects (out/result.json).
"""

import json
import os

from app.stats import summarize


def main() -> None:
    # The whole tree was uploaded into the working dir, which is the cwd — so the
    # bundled data file is reachable by its relative path.
    with open("data/numbers.txt") as f:
        numbers = [int(line) for line in f if line.strip()]

    summary = summarize(numbers)
    summary["run_id"] = os.environ.get("RUN_ID", "unknown")   # passed via env=

    os.makedirs("out", exist_ok=True)
    with open("out/result.json", "w") as f:
        json.dump(summary, f)

    print(f"processed {summary['count']} numbers (mean={summary['mean']}) "
          f"for run {summary['run_id']}")


if __name__ == "__main__":
    main()
