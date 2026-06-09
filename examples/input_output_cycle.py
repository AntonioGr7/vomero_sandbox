"""A full inputs -> process -> outputs round-trip in ONE call.

Combining input_files= and collect= gives you the natural shape of a platform
"Quick Action": hand in the user's files, run their code against them, and get
the produced artifacts back — all without the file ever touching your own host
or a second round-trip to a (possibly different) worker.

Needs a cluster (see examples/README.md).
"""

import csv
import io

from vomero_sandbox import SandboxPool, SandboxConfig

# A CSV the user "uploaded" on your platform.
SALES_CSV = b"region,amount\nnorth,120\nsouth,90\nnorth,30\nsouth,60\n"

# The transform we run against it in the sandbox: total per region -> summary.csv.
CODE = """\
import csv
from collections import defaultdict

totals = defaultdict(int)
for row in csv.DictReader(open('sales.csv')):
    totals[row['region']] += int(row['amount'])

with open('summary.csv', 'w', newline='') as f:
    w = csv.writer(f)
    w.writerow(['region', 'total'])
    for region, total in sorted(totals.items()):
        w.writerow([region, total])

print(f'summarized {len(totals)} regions')
"""


def main() -> None:
    with SandboxPool(SandboxConfig(pool_size=1)) as pool:
        result = pool.run(
            CODE,
            input_files={"sales.csv": SALES_CSV},   # in
            collect=["summary.csv"],                 # ...and out, same call
        )

        print(result.stdout.strip())                 # summarized 2 regions
        if not result.ok:
            print("run failed:", result.stderr)
            return

        # The produced artifact came back as bytes; parse it on the host.
        summary = result.files["summary.csv"].decode()
        for row in csv.reader(io.StringIO(summary)):
            print("  ", row)                          # ['region','total'], ['north','150'], ['south','150']


if __name__ == "__main__":
    main()
