"""Sibling module imported by stats/__main__.py — proves local imports resolve
when a registered package is uploaded and run as a tree."""


def summarize(numbers):
    """Return count / sum / mean for a list of numbers."""
    n = len(numbers)
    total = sum(numbers)
    return {
        "count": n,
        "sum": total,
        "mean": round(total / n, 2) if n else 0,
    }
