## Learning Spark – Local Dev Environment with Dev Container, Spark Connect, Kafka, MinIO, and Superset

This repository provides a ready-to-run local Spark development lab using a Dev Container, Docker Compose, and Spark Connect. It includes Kafka, Schema Registry, MinIO (S3-compatible storage), Spark Connect, and optional StarRocks + Superset for analytics and visualization.

### Prerequisites
- **Docker** and **Docker Compose** (Docker Desktop on macOS/Windows)
- Option A (recommended): **VS Code** + **Dev Containers** extension
- Option B: Any editor + terminal (use `docker compose` directly)

No host-level Python/Java installs are required. The dev container includes Python (with `uv`) and Spark tooling.

### Clone the repository
```bash
git clone https://github.com/<your-org>/learning_spark.git
cd learning_spark
```

### Environment variables
Create a `.env` file in the project root with the following variables (used by `docker-compose.yml`):

```env
# MinIO
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin

# Inside-container endpoints (used by the dev container)
KAFKA_BROKERS=kafka:9093
KAFKA_SCHEMA_REGISTRY=http://schema-registry:8081
```

Notes:
- From the host, Kafka is exposed on `localhost:9092`. Inside containers, use `kafka:9093` (as preconfigured).
- Schema Registry is on `http://localhost:8081` from the host; inside containers use `http://schema-registry:8081`.

---

## Quick start

### Option A: Open in Dev Container (recommended)
1. Open the repo in VS Code.
2. Run: “Reopen in Container”. VS Code will build `.devcontainer/Dockerfile` and bring up services.
3. If services don’t start automatically, run:
   ```bash
   docker compose up -d
   ```
4. Test spark connect by running:
   ```
   make run_spark
   or
   python src/main.py data/mnm_dataset.csv
   ```

### Option B: Local-first with uv (no Docker/Dev Containers)
If you prefer a minimal local setup without Docker:

**Prerequisites:**
- Python 3.9+
- `uv` package manager
- Local Apache Spark (optional if you only run basic PySpark without external connectors)

**Install uv:**
```bash
# macOS (Homebrew)
brew install uv

# Or universal installer
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**Setup:**
```bash
# Install all Python dependencies (including pyspark)
uv sync

# Add a package later
uv add some-package

# Upgrade all packages
uv lock --upgrade && uv sync
```

**Local Spark install (recommended):**
```bash
# macOS (Homebrew)
brew install apache-spark

# Or manual install:
# 1) Download from https://spark.apache.org/downloads.html
# 2) Extract and set environment variables:
export SPARK_HOME="/path/to/spark"
export PATH="$SPARK_HOME/bin:$PATH"
```

**Run examples:**
```bash
# Start optional data services only if you need them
make up

# Run local scripts
uv run python src/main.py data/mnm_dataset.py
```

**Note**: `uv` automatically manages the virtual environment - no need to manually activate/deactivate!

### Option C: Run with Docker Compose only
```bash
docker compose up -d
```

If you want an interactive dev shell with project files mounted and `uv` available:
```bash
docker compose run --rm dev bash
uv sync
```

---

## Services and URLs

- **Spark Connect**: `sc://spark-connect:15002` (inside containers)
  - Spark UI (host): `http://localhost:4040` (may increment to 4041-4045 for multiple apps)
- **Kafka**: `localhost:9092` (host), `kafka:9093` (inside containers)
- **Schema Registry**: `http://localhost:8081` (host), `http://schema-registry:8081` (inside containers)
- **MinIO**: Console `http://localhost:9001`, API `http://localhost:9000`
  - Credentials from `.env` (default: `minioadmin` / `minioadmin`)
- **Superset**: `http://localhost:8088` (user/pass created at container start: `admin` / `admin`)
- **StarRocks** (optional):
  - FE HTTP: `http://localhost:8030`  | FE query port: `9030`
  - BE Web: `http://localhost:8040`

Spark has S3/MinIO access preconfigured via `spark-connect/conf/core-site.xml` and `spark-connect/conf/spark-defaults.conf`.

---

## Common tasks

Using the provided `Makefile` targets:

```bash
# Start all containers in the background
make up

# Stop all containers
make down

# Rebuild images and restart
make rebuild

# Tail logs
make logs

# Open Spark UI in browser
make browse
```

---

## Data and connectors

- Local sample data: `data/mm_dataset.csv`
- JARs loaded by Spark Connect via `--packages` (see `docker-compose.yml`):
  - Kafka: `org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1`, `kafka-clients`
  - S3/MinIO: `org.apache.hadoop:hadoop-aws:3.3.6`, `aws-java-sdk-bundle`
  - JDBC: `com.mysql:mysql-connector-j:8.0.33`

---

## Troubleshooting

- "Class not found" for connectors: ensure Docker downloaded images successfully and the Spark Connect container is running. The `--packages` list is set in `docker-compose.yml` under the `spark-connect` service.
- S3/MinIO auth errors: verify `.env` credentials and that `spark.hadoop.fs.s3a.*` settings are present (see `spark-connect/conf/*`).
- Kafka connectivity: from the host use `localhost:9092`; from containers use `kafka:9093`.
- Spark UI not visible: confirm ports `4040-4045` are exposed and no other app is using them.

---

## Project structure (selected)

```
├─ .devcontainer/
│  ├─ devcontainer.json
│  └─ Dockerfile
├─ docker-compose.yml
├─ spark-connect/
│  ├─ Dockerfile
│  └─ conf/
│     ├─ core-site.xml
│     └─ spark-defaults.conf
├─ src/
│  ├─ main.py
├─ data/mm_dataset.csv
└─ Makefile
```
