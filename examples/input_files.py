"""Staging input files into the sandbox before a run.

Pass input_files={relpath: bytes} to write files into the working directory
*before* the command runs, so the code reads them as ordinary local files. This
is how you run code against files the user selected on your platform: fetch the
bytes from your storage/DB, hand them in here.

Keys are relative paths under the working dir (nested dirs are created). Values
are bytes — binary-safe, sent over the exec stdin stream (no argv length limits,
no shell parsing). Total size is capped by config.max_upload_bytes (32 MiB).

Needs a cluster (see examples/README.md).
"""

from vomero_sandbox import SandboxPool, SandboxConfig


def main() -> None:
    # Pretend these came from your object store / database.
    csv_bytes = b"name,score\nada,90\ngrace,95\nlin,88\n"
    opts_bytes = b'{"mode": "fast"}'

    with SandboxPool(SandboxConfig(pool_size=1)) as pool:
        r = pool.run(
            "import csv, json\n"
            "rows = list(csv.DictReader(open('data.csv')))\n"
            "opts = json.load(open('cfg/opts.json'))\n"
            "best = max(rows, key=lambda x: int(x['score']))\n"
            "print(f\"{len(rows)} rows, mode={opts['mode']}, top={best['name']}\")",
            input_files={
                "data.csv": csv_bytes,           # lands at <workdir>/data.csv
                "cfg/opts.json": opts_bytes,     # nested path -> cfg/ is created
            },
        )
        print(r.stdout.strip())                  # 3 rows, mode=fast, top=grace


if __name__ == "__main__":
    main()
