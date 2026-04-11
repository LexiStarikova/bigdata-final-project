#!/bin/bash
# Stage 0 — download NYC Yellow Taxi 2025 Parquet into ./data (run from repo root).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
mkdir -p data
BASE="https://d37ci6vzurychx.cloudfront.net/trip-data"
for m in 01 02 03 04 05 06 07 08 09 10 11 12; do
  URL="${BASE}/yellow_tripdata_2025-${m}.parquet"
  DEST="data/yellow_tripdata_2025-${m}.parquet"
  if [[ -s "$DEST" ]]; then
    echo "Skip existing $DEST"
    continue
  fi
  echo "Downloading $DEST"
  curl -fL --retry 3 --retry-delay 5 -o "$DEST" "$URL"
done
echo "Preprocess done."
