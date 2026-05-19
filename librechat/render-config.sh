#!/bin/sh
set -eu

# Этот скрипт запускается внутри готового контейнера LibreChat перед стартом backend.
# Он нужен, потому что librechat.yaml.template содержит placeholders вида ${...},
# а LibreChat сам не подставляет все нужные переменные в YAML так, как нам нужно.

# LIBRECHAT_MODELS - список моделей через запятую для UI LibreChat.
# MODEL - fallback на одну основную модель, если список не задан.
models="${LIBRECHAT_MODELS:-${MODEL:-qwen2.5:7b}}"

# Модель для автоматического названия чатов.
title_model="${LIBRECHAT_TITLE_MODEL:-${MODEL:-$(printf '%s' "$models" | cut -d, -f1)}}"

# Модель для summary. Сейчас summarize выключен в YAML, но переменная оставлена для явной настройки.
summary_model="${LIBRECHAT_SUMMARY_MODEL:-$title_model}"
model_yaml=""

# LibreChat YAML ожидает список моделей в YAML-формате, а .env хранит их одной строкой через запятую.
# Этот цикл превращает "a,b,c" в:
#   - "a"
#   - "b"
#   - "c"
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

# Если список оказался пустым, оставляем рабочую локальную модель по умолчанию.
if [ -z "$model_yaml" ]; then
  model_yaml='          - "qwen2.5:7b"
'
fi

# Эти значения попадут в librechat.yaml и заставят LibreChat ходить не напрямую в model backend,
# а через наш llm-gateway.
export LLM_GATEWAY_API_KEY="${LLM_GATEWAY_API_KEY:-local-dev-key}"
export LLM_GATEWAY_BASE_URL="${LLM_GATEWAY_BASE_URL:-http://llm-gateway:3344/v1}"
export LIBRECHAT_TITLE_MODEL="$title_model"
export LIBRECHAT_SUMMARY_MODEL="$summary_model"
export LIBRECHAT_MODEL_LIST_YAML="$model_yaml"

# Маленький Python-блок делает безопасную текстовую подстановку placeholders в YAML-template.
# На выходе создается /app/librechat.yaml, который затем читает LibreChat backend.
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

# Передаем управление стандартному backend-старту LibreChat из готового образа.
exec npm run backend
