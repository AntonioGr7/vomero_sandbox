"""A sibling module, imported by __main__ to prove local imports resolve."""


def summarize(numbers):
    """Return count / sum / mean / min / max for a list of numbers."""
    n = len(numbers)
    total = sum(numbers)
    return {
        "count": n,
        "sum": total,
        "mean": round(total / n, 2) if n else 0,
        "min": min(numbers) if numbers else None,
        "max": max(numbers) if numbers else None,
    }
