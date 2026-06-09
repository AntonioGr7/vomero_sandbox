"""Using a custom worker image with libraries baked in — chart -> collect the PNG.

The default python:3.13-slim has the standard library only. To make numpy /
matplotlib / requests available to sandboxed code, bake them into a worker image
(see sandbox-image/Dockerfile) and point config.image at it. Don't pip install
at run time: egress is denied and the root filesystem is read-only.

This example draws a chart with matplotlib and collects the produced PNG.

Prereq — build and load the image first:

    docker build -t vomero-sandbox-runtime:1.0 sandbox-image
    kind load docker-image vomero-sandbox-runtime:1.0 --name sandbox   # local kind

Then run it (needs a cluster — see examples/README.md).
"""

from vomero_sandbox import SandboxPool, SandboxConfig

from _timing import timed

# matplotlib wants a writable cache/config dir at import time; point it at the
# writable workspace. Use the non-interactive 'Agg' backend (no display).
CHART_CODE = """\
import os
os.environ['MPLCONFIGDIR'] = '/scratch'
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

x = np.linspace(0, 2 * np.pi, 200)
plt.figure(figsize=(6, 3))
plt.plot(x, np.sin(x), label='sin')
plt.plot(x, np.cos(x), label='cos')
plt.legend(); plt.tight_layout()
plt.savefig('chart.png', dpi=120)
print('wrote chart.png')
"""


@timed
def main() -> None:
    cfg = SandboxConfig(image="vomero-sandbox-runtime:1.0", pool_size=1)
    with SandboxPool(cfg) as pool:
        # Sanity check that the baked libraries import.
        r = pool.run("import numpy, matplotlib; print('libs available')")
        print(r.stdout.strip() if r.ok else r.stderr)

        # Produce the chart and collect it in the same call.
        r = pool.run(CHART_CODE, collect=["chart.png"])
        if not r.ok:
            print("chart run failed:", r.stderr)
            return

        png = r.files["chart.png"]
        with open("chart.png", "wb") as f:
            f.write(png)
        print(f"saved chart.png locally ({len(png)} bytes)")


if __name__ == "__main__":
    main()
