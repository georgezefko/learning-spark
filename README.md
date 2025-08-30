## Learning Spark (local-first, uv-managed)

A minimal Spark learning project using a local PySpark install and `uv` for Python dependency management. Optional Docker services (MinIO, Kafka, PostgreSQL, Redis) are available separately for when you need them.

### Prerequisites
- Python 3.9+
- `uv` package manager (fast, modern)
- Local Apache Spark (optional if you only run basic PySpark without external connectors)

Install `uv` quickly:
```bash
# macOS (Homebrew)
brew install uv

# Or universal installer
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Project setup (uv)
This project was created with `uv` and declares dependencies in `pyproject.toml`.
```bash
# Create a new project (if starting fresh)
uv init learning_spark
cd learning_spark

# Install all Python dependencies (including pyspark)
uv sync

# Add a package later
uv add some-package

# Upgrade all packages
uv lock --upgrade && uv sync
```

**Note**: `uv` automatically manages the virtual environment - no need to manually activate/deactivate like traditional `venv`!

### Local PySpark (Spark runtime) install
If you want Spark running locally (recommended):

- macOS (Homebrew):
```bash
brew install apache-spark
```
- Manual (all OS):
1) Download Apache Spark prebuilt package from `https://spark.apache.org/downloads.html`
2) Extract and set environment variables in your shell profile:
```bash
export SPARK_HOME="/path/to/spark"
export PATH="$SPARK_HOME/bin:$PATH"
```

Verify:
```bash
spark-shell --version
pyspark --version
```

### Run examples
```bash
# Start optional data services only if you need them (see below)
# make start

# Run local scripts
uv run python main.py
uv run python streaming.py
```
<!-- 
### Optional: Docker services (MinIO/Kafka/Postgres/Redis)
If you want S3-like storage, Kafka streams, or a SQL database locally, use the provided Docker Compose stack and helper Make targets. See `README-Docker.md` for details.

Common commands:
```bash
make start     # start services
make status    # check status
make logs      # tail logs
make stop      # stop services
make clean     # remove containers + volumes
```

### Repo layout
- `main.py` – sample batch/ETL style code
- `streaming.py` – sample streaming code
- `README-Docker.md` – optional services reference
- `docker-compose.yml` – optional services stack

### Notes
- PySpark library is managed by `uv` (see `pyproject.toml`).
- If your code needs connectors (e.g., Kafka source), ensure your local Spark has the right packages via `--packages` or by placing JARs in `$SPARK_HOME/jars`.
- Keep day-to-day commands simple with the Makefile; use Docker only when needed. -->
