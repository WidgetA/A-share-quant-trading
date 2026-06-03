# Multi-stage build for smaller image
# Python 3.13: required by kimi-cli (path B server-side verify needs it on PATH).
FROM python:3.13-slim AS builder

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

# Build-time gate: kimi-cli MUST be installed + runnable in this (slim) env.
# Path B's server-side verification depends on the `kimi` binary; if the
# install is broken or a system lib is missing, fail the BUILD here so a
# dead-on-arrival image never ships. (CI's docker build runs this.)
RUN /app/.venv/bin/kimi --version

# Runtime stage
FROM python:3.13-slim AS runtime

# Inherit build arguments for version tracking
ARG GIT_COMMIT=unknown
ARG GIT_BRANCH=unknown
ARG BUILD_TIME=unknown

# Set version info as environment variables
ENV GIT_COMMIT=${GIT_COMMIT}
ENV GIT_BRANCH=${GIT_BRANCH}
ENV BUILD_TIME=${BUILD_TIME}

# Runtime system libs required by ML wheels (sklearn/xgboost/lightgbm need libgomp)
# libgomp1: lightgbm runtime. curl: kimi-cli's Shell-fallback tool — when its
# SearchWeb fails, kimi reasons its way to `curl`-ing financial sites (verified
# manually for 920039); without curl that fallback is unavailable in-container.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder /app/.venv /app/.venv

# Copy application code
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY config/ ./config/
# Static data files — placed outside /app/data which is a volume mount
COPY data/sectors.json data/board_constituents.json data/board_relevance_cache.json ./bundled_data/

# kimi-cli config is generated at RUNTIME, not baked here: app startup calls
# kimi_config.ensure_kimi_config_from_env(), which writes ~/.kimi/config.toml
# from the KIMI_API_KEY env var (a static Kimi-Code API key — no OAuth, no
# 15-min token, no interactive login). The key is NEVER in the image; it comes
# from the deploy env (gitignored docker-compose.yml). See CLAUDE.md §12.

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
CMD ["uvicorn", "src.web.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
