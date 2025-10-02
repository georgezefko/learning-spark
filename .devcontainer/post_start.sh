#!/usr/bin/env bash
set -euo pipefail

SQL_FILE="src/kappa_project/schemas/db_schema.sql"

if [[ ! -f "$SQL_FILE" ]]; then
  echo "⚠️  $SQL_FILE not found, skipping StarRocks init."
  exit 0
fi

echo "⏳ Waiting for StarRocks FE (starrocks-fe-0:9030)…"
for i in {1..60}; do
  if mysql -h starrocks-fe-0 -P9030 -uroot -e "SELECT 1" >/dev/null 2>&1; then
    echo "StarRocks FE is ready."
    break
  fi
  sleep 2
  if [[ $i -eq 60 ]]; then
    echo "Could not connect to StarRocks FE on 9030."
    exit 1
  fi
done

echo "Applying schema: $SQL_FILE"
mysql -h starrocks-fe-0 -P9030 -uroot < "$SQL_FILE"
echo "StarRocks schema applied."
