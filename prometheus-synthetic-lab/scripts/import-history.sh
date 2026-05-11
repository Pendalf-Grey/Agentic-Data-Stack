#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")/.."

mkdir -p data/openmetrics data/prometheus

node scripts/generate-openmetrics.mjs

docker run --rm \
  -v "$PWD/data/openmetrics:/input:ro" \
  -v "$PWD/data/prometheus:/prometheus" \
  prom/prometheus:v2.54.1 \
  promtool tsdb create-blocks-from openmetrics /input/synthetic.openmetrics /prometheus

echo "Historical synthetic blocks were written to ./data/prometheus"
echo "Start Prometheus with: docker compose up -d"
