#!/bin/sh
set -eu

python3 tools/map_queue_worker.py "$@"
