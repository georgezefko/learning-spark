# ./Dockerfile
# FROM python:3.12-slim

# # Base tools (no Java via apt)
# RUN set -eux; \
#     apt-get update; \
#     apt-get install -y --no-install-recommends curl bash git ca-certificates procps; \
#     rm -rf /var/lib/apt/lists/*

# # Temurin JRE 17 (Adoptium)
# ARG JRE_URL="https://api.adoptium.net/v3/binary/latest/17/ga/linux/x64/jre/hotspot/normal/eclipse"
# RUN curl -fsSL "$JRE_URL" -o /tmp/jre17.tar.gz \
#  && mkdir -p /opt/jre \
#  && tar -xzf /tmp/jre17.tar.gz -C /opt/jre --strip-components=1 \
#  && rm /tmp/jre17.tar.gz
# ENV JAVA_HOME=/opt/jre
# ENV PATH="$JAVA_HOME/bin:${PATH}"

# # uv + Spark 4
# RUN curl -LsSf https://astral.sh/uv/install.sh | sh
# ENV PATH="/root/.local/bin:${PATH}"

# ARG SPARK_VERSION=4.0.1
# ARG HADOOP_PROFILE=hadoop3
# RUN curl -L -o /tmp/spark.tgz https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}.tgz \
#  && tar -xzf /tmp/spark.tgz -C /opt \
#  && ln -s /opt/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE} /opt/spark \
#  && rm /tmp/spark.tgz
# ENV SPARK_HOME=/opt/spark
# ENV PATH="$SPARK_HOME/bin:${PATH}"

# # Spark will pick up configs from /opt/spark/conf (mounted via compose)
# ENV SPARK_CONF_DIR=/opt/spark/conf

# WORKDIR /workspace


# ./Dockerfile  (for the "dev" service)
# syntax=docker/dockerfile:1.7
FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive
RUN --mount=type=cache,target=/var/cache/apt \
    set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl bash git ca-certificates procps; \
    rm -rf /var/lib/apt/lists/*

# uv package manager
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /workspace

# Only re-install when requirements.txt changes
COPY requirements.txt /tmp/requirements.txt
RUN --mount=type=cache,target=/root/.cache/uv \
    uv venv && . .venv/bin/activate && uv pip install -r /tmp/requirements.txt

# Make venv binaries easily available
ENV PATH="/workspace/.venv/bin:${PATH}"
