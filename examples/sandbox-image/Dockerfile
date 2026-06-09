# A custom worker image with third-party libraries baked in.
#
# Why baked, not pip-installed at run time: in the running sandbox egress is
# denied (pip can't reach PyPI), the root filesystem is read-only (pip can't
# write site-packages), and per-run installs are slow and non-reproducible.
# Pre-installing means warm pods are ready instantly, offline, and identical
# every time. The packages land in system site-packages (world-readable), so the
# non-root sandbox user imports them fine under the read-only root filesystem.
#
# Build and make it available to the cluster, then point config.image at it:
#
#   docker build -t vomero-sandbox-runtime:1.0 .
#   kind load docker-image vomero-sandbox-runtime:1.0 --name sandbox   # local kind
#   # production: push to your registry instead

FROM python:3.13-slim

RUN pip install --no-cache-dir \
    numpy==2.2.1 \
    matplotlib==3.10.0 \
    requests==2.32.3
