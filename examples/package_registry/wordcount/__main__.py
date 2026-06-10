"""A second registered package (run with ``python .``), to show jobs naming
DIFFERENT packages off the same queue. Reads its per-job input from ``input.txt``
and writes a small summary to ``out.json`` for the service to collect.
"""

import json
import os
from collections import Counter


def main() -> None:
    with open("input.txt") as f:
        words = f.read().split()

    counts = Counter(words)
    result = {
        "total_words": sum(counts.values()),
        "distinct": len(counts),
        "top": counts.most_common(3),
        "run_tag": os.environ.get("RUN_TAG", ""),   # passed per job via env=
    }
    with open("out.json", "w") as f:
        json.dump(result, f)

    print(f"wordcount: {result['total_words']} words, top={result['top']}")


if __name__ == "__main__":
    main()
