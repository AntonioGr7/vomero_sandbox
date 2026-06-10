"""Stream output to the caller as the sandboxed code produces it.

The exec websocket already delivers stdout/stderr in chunks, so a run that emits
results incrementally — say, code that calls an LLM through the egress proxy and
relays tokens — can be forwarded to YOUR client in real time instead of waiting
for the whole run to finish.

run_stream / shell_stream / exec_stream mirror run / shell / exec, but instead of
a RunResult they return a SandboxStream context manager: iterate it for live
StreamChunk pairs, then read `.result` for the exit code / timeout / collected
files once the run completes.

Streaming runs default PYTHONUNBUFFERED=1 so Python flushes each line as it is
written rather than block-buffering it all into one chunk at exit.

Needs a cluster (see examples/README.md).
"""

import time

from vomero_sandbox import SandboxPool, SandboxConfig

from _timing import timed


# Stand-in for "real" streaming work: print one line every 200ms, flushing as we
# go. Swap this for an LLM SDK call that prints tokens as they arrive.
_PRODUCER = r"""
import sys, time
for i in range(1, 6):
    print(f"token {i}", flush=True)
    time.sleep(0.2)
print("done")
"""


@timed
def main() -> None:
    with SandboxPool(SandboxConfig(pool_size=2)) as pool:

        # 1. Stream chunks as they arrive. The `for` loop unblocks on each flush,
        #    not at the end — note the wall-clock gaps between lines.
        print("streaming run:")
        with pool.run_stream(_PRODUCER, timeout_s=30) as run:
            for chunk in run:                       # StreamChunk(stream, data)
                tag = "ERR" if chunk.stream == "stderr" else "out"
                print(f"  [{tag} @ +{time.monotonic() % 60:5.1f}s] {chunk.data!r}")
            result = run.result                     # populated once the run ends

        # The streamed bytes are not re-accumulated (you already got them), so
        # stdout/stderr are empty — result carries the verdict + any collected files.
        print("  ->", "ok" if result.ok else "failed",
              "exit_code =", result.exit_code, "timed_out =", result.timed_out)

        # 2. A timeout is still a normal RESULT, surfaced after the chunks you did
        #    receive — the worker is retired, just like the buffered path.
        print("\nstreaming run that times out:")
        with pool.run_stream("import time\nwhile True:\n print('tick', flush=True); time.sleep(0.3)",
                             timeout_s=1) as run:
            for chunk in run:
                print("  got:", chunk.data.strip())
            print("  -> timed_out =", run.result.timed_out)


if __name__ == "__main__":
    main()