# Minimal Spark Connect image with Python and requests for foreachBatch
FROM apache/spark:4.0.1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 python3-pip && \
    pip3 install --no-cache-dir \
      pandas>=2.0.0 \
      pyarrow \
      grpcio>=1.74.0 \
      grpcio-status>=1.74.0 \
      requests && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Tell Spark which Python to use for workers
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3
