FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PATH="/root/.local/bin:/workspace/.venv/bin:${PATH}" \
    UV_LINK_MODE=copy

# check uv container

RUN --mount=type=cache,target=/var/cache/apt \
    set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      curl make bash git ca-certificates procps \
    && rm -rf /var/lib/apt/lists/*

# uv package manager
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

WORKDIR /workspace

COPY pyproject.toml uv.lock ./

# Create venv and install ONLY dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    uv venv && uv sync --locked --no-install-project --no-dev

COPY . .

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

# Ensure venv comes first
#ENV PATH="/workspace/.venv/bin:${PATH}"

ENV PYSPARK_PYTHON=/workspace/.venv/bin/python \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# [OPTIONAL] This line is for auto-activation in interactive shells
RUN echo "source /workspace/.venv/bin/activate" >> /root/.bashrc

ENTRYPOINT []
