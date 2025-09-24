FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PATH="/root/.local/bin:/workspace/.venv/bin:${PATH}" \
    UV_LINK_MODE=copy  \
    UV_PIP_FLAGS="--only-binary=:all:"

# check uv container

RUN --mount=type=cache,target=/var/cache/apt \
    set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      curl bash git ca-certificates procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYSPARK_PYTHON=/workspace/.venv/bin/python

# uv package manager
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

WORKDIR /workspace

COPY pyproject.toml uv.lock ./

# 1) Create venv and install ONLY dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    uv venv && uv sync --locked --no-install-project --no-dev

COPY . .

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

# Ensure venv comes first
ENV PATH="/workspace/.venv/bin:${PATH}"

ENTRYPOINT []
