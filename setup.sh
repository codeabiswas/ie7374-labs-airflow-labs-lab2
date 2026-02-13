#!/bin/bash
set -e

echo "=== Airflow Lab 2 — Local Setup ==="

rm -rf ./logs ./plugins

docker compose down -v 2>/dev/null || true

mkdir -p ./logs ./plugins ./config


if ! grep -q "AIRFLOW_UID" .env 2>/dev/null; then
    echo "AIRFLOW_UID=$(id -u)" >> .env
fi

echo ""
echo "=== Building custom Airflow image ==="
docker compose build

echo ""
echo "=== Initialising Airflow database ==="
docker compose up airflow-init

echo ""
echo "=== Starting Airflow + Report Server ==="
echo "  Airflow UI       → http://localhost:8080  (user: airflow / pass: airflow)"
echo "  Pipeline Report  → http://localhost:5555  (updated after each pipeline run)"
echo ""
docker compose up -d

echo ""
echo "=== Done! Run 'docker compose logs -f' to follow logs ==="
