#!/bin/sh
set -eu

sh tools/run_sql.sh sql/00_create_investigation.sql
sh tools/compress_raw_logs.sh
sh tools/map_queue_worker.sh --create-schema --enqueue --max-iterations 1 --claim-size 10 "${@}"
sh tools/run_sql.sh sql/30_reduce_ai.sql
sh tools/run_sql.sh sql/40_final_reduce_sql.sql
