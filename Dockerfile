# Multi-stage build for smaller image
FROM python:3.11-slim AS builder

# Build arguments for version tracking
ARG GIT_COMMIT=unknown
ARG GIT_BRANCH=unknown
ARG BUILD_TIME=unknown

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

# Inherit build arguments for version tracking
ARG GIT_COMMIT=unknown
ARG GIT_BRANCH=unknown
ARG BUILD_TIME=unknown

# Set version info as environment variables
ENV GIT_COMMIT=${GIT_COMMIT}
ENV GIT_BRANCH=${GIT_BRANCH}
ENV BUILD_TIME=${BUILD_TIME}

WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder /app/.venv /app/.venv

# Copy application code
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY config/ ./config/
# Static data files — placed outside /app/data which is a volume mount
COPY data/sectors.json data/board_constituents.json data/board_relevance_cache.json ./bundled_data/

# Set PATH to use virtual environment
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app"
ENV PYTHONUNBUFFERED=1

# Web UI configuration
ENV WEB_ENABLED=true
ENV WEB_HOST=0.0.0.0
ENV WEB_PORT=8000
ENV INTERACTION_MODE=web

# Expose Web UI port
EXPOSE 8000

# Run the web service
CMD ["python", "-m", "src.web.app"]
