# ferryman Dockerfile - Multi-stage build
# One image for both the agent (root: FUSE, bind mounts) and the coordinator
# (drops to PUID:PGID). See contrib/docker/docker-compose.yml for how to run each.

# ============================================================
# Stage 1: Builder
# ============================================================
FROM python:3.13-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.13.5 /uv /bin/uv

# Install into /opt/venv from the lock file, compiled, no dev dependencies,
# using the base image's Python (never a downloaded one)
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /build

# Dependencies first, so a source change doesn't re-install them
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

# Then the package itself (proper install, not editable)
COPY src/ ./src/
RUN uv sync --frozen --no-dev --no-editable

# ============================================================
# Stage 2: rclone
# ============================================================
# Pinned upstream binary: ferryman needs >= 1.75, distribution packages are
# far older. Checked against the release's SHA256SUMS.
FROM debian:trixie-slim AS rclone

ARG RCLONE_VERSION=1.75.2
ARG TARGETARCH

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl unzip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp/rclone
RUN set -eu; \
    zip="rclone-v${RCLONE_VERSION}-linux-${TARGETARCH}.zip"; \
    base="https://downloads.rclone.org/v${RCLONE_VERSION}"; \
    curl -fsSLO "$base/$zip"; \
    curl -fsSL "$base/SHA256SUMS" | grep " $zip\$" | sha256sum -c -; \
    unzip -q "$zip"; \
    install -m 0755 "rclone-v${RCLONE_VERSION}-linux-${TARGETARCH}/rclone" /usr/local/bin/rclone

# ============================================================
# Stage 3: Runtime
# ============================================================
FROM python:3.13-slim

# fuse3: fusermount3 for mounts; tzdata: cron schedules and time
# placeholders use local time (set TZ); mount/umount/stat ship in the base
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    fuse3 \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder, rclone from its stage
COPY --from=builder /opt/venv /opt/venv
COPY --from=rclone /usr/local/bin/rclone /usr/local/bin/rclone
ENV PATH="/opt/venv/bin:$PATH"

# contrib/ (example config, systemd units) for reference; the real config
# is mounted at /etc/ferryman
RUN mkdir -p /etc/ferryman
COPY contrib/ /usr/share/ferryman/

# Non-root user for the coordinator (the agent stays root)
RUN useradd --system --create-home --shell /usr/sbin/nologin --uid 1000 ferryman

# The agent starts and stays root: FUSE mounts, kernel read-only binds and
# mount directories need it. The coordinator starts as root only to give
# its state to PUID:PGID (default 1000:1000), then drops to that user --
# see docker/ferryman.sh. The wrapper shadows the real ferryman on PATH so
# `docker exec <c> ferryman status` goes through it too.
# Its own directory, not /usr/local/bin: putting that first on PATH would
# also make `python` the base image's interpreter instead of the venv's.
COPY docker/ferryman.sh /opt/ferryman/bin/ferryman
RUN chmod 0755 /opt/ferryman/bin/ferryman
ENV PATH="/opt/ferryman/bin:$PATH"

# Default config locations
ENV FERRYMAN_CONFIG=/etc/ferryman/config.yaml \
    FERRYMAN_COORDINATOR_CONFIG=/etc/ferryman/coordinator.yaml

# Health check: the agent answers on its API socket (the coordinator
# service overrides this in contrib/docker/docker-compose.yml)
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD ferryman status > /dev/null || exit 1

# Coordinator port (the agent's servers listen wherever their addr says)
EXPOSE 8470

# Config, state, cache and logs
VOLUME ["/etc/ferryman", "/var/lib/ferryman", "/var/cache/ferryman", "/var/log/ferryman"]

# Default command: run the agent
ENTRYPOINT ["ferryman"]
CMD ["agent"]
