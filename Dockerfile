# Multi-stage build for smaller image. Pin the reviewed multi-architecture OCI
# index so rebuilding the same commit cannot silently change the Python base.
FROM python:3.11-slim-trixie@sha256:1042b61448fef4ba92d16a8c7eb4996d027568ce64792a7877fd88511e0af7c6 AS builder

# Build arguments for version tracking
ARG GIT_COMMIT=unknown
ARG GIT_BRANCH=unknown
ARG BUILD_TIME=unknown

# Install the build frontend at a reviewed version and OCI index digest.
COPY --from=ghcr.io/astral-sh/uv:0.9.27@sha256:143b40f4ab56a780f43377604702107b5a35f83a4453daf1e4be691358718a6a /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency files and source for build
COPY pyproject.toml uv.lock README.md ./
COPY src/ ./src/

# A production image must match the reviewed lockfile exactly.
RUN uv sync --frozen --no-dev

# Runtime stage
FROM python:3.11-slim-trixie@sha256:1042b61448fef4ba92d16a8c7eb4996d027568ce64792a7877fd88511e0af7c6 AS common-runtime

# Inherit build arguments for version tracking
ARG GIT_COMMIT=unknown
ARG GIT_BRANCH=unknown
ARG BUILD_TIME=unknown

# Set version info as environment variables
ENV GIT_COMMIT=${GIT_COMMIT}
ENV GIT_BRANCH=${GIT_BRANCH}
ENV BUILD_TIME=${BUILD_TIME}

# Add Debian oldstable for legacy libidn11 required by iFinD SDK
RUN echo "deb http://deb.debian.org/debian bullseye main" > /etc/apt/sources.list.d/bullseye.list

# Install system dependencies required by iFinD SDK and LightGBM (cache bust: 2026-03-27)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libstdc++6 \
    libgcc-s1 \
    libc6 \
    libssl3t64 \
    libcurl4t64 \
    libgomp1 \
    zlib1g \
    libidn11 \
    && rm -rf /var/lib/apt/lists/* \
    && rm /etc/apt/sources.list.d/bullseye.list

WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder /app/.venv /app/.venv

# V20's reviewed runtime contract hashes dependency metadata as well as code.
# Keep the exact build inputs in both the platform and dedicated V20 targets.
COPY pyproject.toml uv.lock ./

# Copy application code
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY config/ ./config/
# V20 validates the frozen G manifest and its referenced files at startup.
# Keep the migration beside the runtime as the operator/audit copy of the DDL.
COPY docs/strategy-v20-artifacts/ ./docs/strategy-v20-artifacts/
COPY migrations/ ./migrations/
# Static data files — placed outside /app/data which is a volume mount
COPY data/sectors.json data/board_constituents.json data/board_relevance_cache.json ./bundled_data/

# Copy LGBRank model files
COPY models/ ./models/

# Copy and install iFinD SDK
COPY vendor/ ./vendor/

# Install iFinD SDK
# Extract SDK to /opt/ths_sdk
RUN mkdir -p /opt/ths_sdk && \
    tar -xzf vendor/THSDataInterface_Linux_*.tar.gz -C /opt/ths_sdk && \
    # Setup library path
    echo "/opt/ths_sdk/bin64" > /etc/ld.so.conf.d/ths_sdk.conf && \
    ldconfig && \
    # Install Python module
    /app/.venv/bin/python /opt/ths_sdk/bin64/installiFinDPy.py /opt/ths_sdk

# Set PATH to use virtual environment
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app:/opt/ths_sdk/bin64"
ENV PYTHONUNBUFFERED=1

# iFinD SDK library path
ENV LD_LIBRARY_PATH="/opt/ths_sdk/bin64:$LD_LIBRARY_PATH"

# iFinD credentials (override at runtime via docker run -e or docker-compose)
# These are placeholders - MUST be set at runtime
ENV IFIND_USERNAME=""
ENV IFIND_PASSWORD=""

# Web UI configuration
ENV WEB_ENABLED=true
ENV WEB_HOST=0.0.0.0
ENV WEB_PORT=8000
ENV INTERACTION_MODE=web

# Expose Web UI port
EXPOSE 8000

# Build with `--target v20` for the production V20 boundary. This target never
# invokes the platform SystemManager or mounts its trading/iQuant routes.
FROM common-runtime AS v20
CMD ["python", "scripts/v20_main.py"]

# Keep the default (last) target compatible with the existing platform/V16
# deployment. Formal V20 is rejected by this process at startup.
FROM common-runtime AS runtime
CMD ["python", "scripts/main.py"]
