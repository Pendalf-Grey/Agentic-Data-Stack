#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")/.."

mkdir -p data/openmetrics
rm -rf data/prometheus
mkdir -p data/prometheus

python3 scripts/generate_openmetrics.py

docker run --rm \
  --entrypoint promtool \
  -v "$PWD/data/openmetrics:/input:ro" \
  -v "$PWD/data/prometheus:/prometheus" \
  prom/prometheus:v2.54.1 \
  tsdb create-blocks-from openmetrics /input/synthetic.openmetrics /prometheus

echo "Historical synthetic blocks were written to ./data/prometheus"
echo "Start Prometheus with: docker compose up -d"
