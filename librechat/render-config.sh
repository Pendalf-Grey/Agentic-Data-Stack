#!/bin/sh
set -eu

models="${LIBRECHAT_MODELS:-${MODEL:-qwen2.5:7b}}"
title_model="${LIBRECHAT_TITLE_MODEL:-${MODEL:-$(printf '%s' "$models" | cut -d, -f1)}}"
summary_model="${LIBRECHAT_SUMMARY_MODEL:-$title_model}"
model_yaml=""

old_ifs="$IFS"
IFS=','
for model in $models; do
  trimmed="$(printf '%s' "$model" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  if [ -n "$trimmed" ]; then
    model_yaml="${model_yaml}          - \"${trimmed}\"
"
  fi
done
IFS="$old_ifs"

if [ -z "$model_yaml" ]; then
  model_yaml='          - "qwen2.5:7b"
'
fi

export LLM_GATEWAY_API_KEY="${LLM_GATEWAY_API_KEY:-local-dev-key}"
export LLM_GATEWAY_BASE_URL="${LLM_GATEWAY_BASE_URL:-http://llm-gateway:3344/v1}"
export LIBRECHAT_TITLE_MODEL="$title_model"
export LIBRECHAT_SUMMARY_MODEL="$summary_model"
export LIBRECHAT_MODEL_LIST_YAML="$model_yaml"

python3 - <<'PY'
from pathlib import Path
import os

template = Path('/app/librechat.yaml.template').read_text()
for key in [
    'LLM_GATEWAY_API_KEY',
    'LLM_GATEWAY_BASE_URL',
    'LIBRECHAT_TITLE_MODEL',
    'LIBRECHAT_SUMMARY_MODEL',
    'LIBRECHAT_MODEL_LIST_YAML',
]:
    template = template.replace('${' + key + '}', os.environ.get(key, ''))
Path('/app/librechat.yaml').write_text(template)
PY

exec npm run backend
