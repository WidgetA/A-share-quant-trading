# Multi-stage build for smaller image
FROM python:3.11-slim AS builder

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency files and source for build
COPY pyproject.toml uv.lock* README.md ./
COPY src/ ./src/

# Install dependencies
RUN uv sync --frozen --no-dev || uv sync --no-dev

# Runtime stage
FROM python:3.11-slim AS runtime

# Add Debian oldstable for legacy libidn11 required by iFinD SDK
RUN echo "deb http://deb.debian.org/debian bullseye main" > /etc/apt/sources.list.d/bullseye.list

# Install system dependencies required by iFinD SDK
RUN apt-get update && apt-get install -y --no-install-recommends \
    libstdc++6 \
    libgcc-s1 \
    libc6 \
    libssl3t64 \
    libcurl4t64 \
    zlib1g \
    libidn11 \
    && rm -rf /var/lib/apt/lists/* \
    && rm /etc/apt/sources.list.d/bullseye.list

WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder /app/.venv /app/.venv

# Copy application code
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY config/ ./config/

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

# Run the service
CMD ["python", "scripts/main.py"]
