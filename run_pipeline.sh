#!/usr/bin/env bash
set -a
[ -f .env ] && . ./.env
set +a

set -euo pipefail

echo "[1/7] Build image ingester"
docker compose build ingester

echo "[2/7] Pull images"
docker compose pull

echo "[3/7] Démarrage Mongo"
docker compose up -d mongodb

echo "[3b/7] Attente disponibilité Mongo"
for i in {1..30}; do
  if docker exec -i mongodb mongosh "mongodb://mongodb:27017/admin" --eval 'db.runCommand({ping:1})' >/dev/null 2>&1; then
    echo "Mongo prêt"; break
  fi
  echo "Mongo non prêt, tentative $i/30"; sleep 2
  if [ "$i" -eq 30 ]; then echo "Timeout Mongo"; exit 1; fi
done

echo "[4/7] Injection du CSV brut"
docker compose run --rm ingester "mkdir -p /app/raw"
docker compose run --rm ingester "cat > /app/raw/healthcare_dataset.csv" < data/healthcare_dataset.csv
docker compose run --rm ingester "ls -lh /app/raw/healthcare_dataset.csv"

echo "[5/7] Préparation des données"
docker compose run --rm ingester "python scripts/prepare_clean_data.py"
docker compose run --rm ingester "ls -lh /app/data/healthcare_cleaned.csv /app/data/healthcare_rejects.csv"

echo "[6/7] Ingestion MongoDB"
docker compose run --rm ingester "python scripts/ingest.py"

echo "[7/7] Contrôle en base"
docker compose run --rm ingester "python scripts/verify_migration.py"