#!/bin/sh
set -eu

# Этот скрипт запускается внутри готового контейнера LibreChat перед стартом backend.
# Он нужен, потому что librechat.yaml.template содержит placeholders вида ${...},
# а LibreChat сам не подставляет все нужные переменные в YAML так, как нам нужно.

# LOCAL_OLLAMA_MODELS - список локальных Ollama-моделей для endpoint "Local Ollama".
# Для обратной совместимости, если LOCAL_OLLAMA_MODELS не задан, используем LIBRECHAT_MODELS.
local_models="${LOCAL_OLLAMA_MODELS:-${LIBRECHAT_MODELS:-${MODEL:-qwen2.5:7b}}}"

# Модель для автоматического названия чатов.
title_model="${LIBRECHAT_TITLE_MODEL:-${MODEL:-$(printf '%s' "$local_models" | cut -d, -f1)}}"

# Модель для summary. Сейчас summarize выключен в YAML, но переменная оставлена для явной настройки.
summary_model="${LIBRECHAT_SUMMARY_MODEL:-$title_model}"

yaml_model_list() {
  model_list="$1"
  yaml=""
  old_ifs="$IFS"
  IFS=','
  for model in $model_list; do
    trimmed="$(printf '%s' "$model" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    if [ -n "$trimmed" ]; then
      yaml="${yaml}          - \"${trimmed}\"
"
    fi
  done
  IFS="$old_ifs"
  printf '%s' "$yaml"
}

# LibreChat YAML ожидает список моделей в YAML-формате, а .env хранит их одной строкой через запятую.
# Эта функция превращает "a,b,c" в:
#   - "a"
#   - "b"
#   - "c"
local_model_yaml="$(yaml_model_list "$local_models")"

# Если список оказался пустым, оставляем рабочую локальную модель по умолчанию.
if [ -z "$local_model_yaml" ]; then
  local_model_yaml='          - "qwen2.5:7b"
'
fi

cloud_endpoint_yaml=""
if [ "${CLOUD_MODEL_ENABLED:-false}" = "true" ]; then
  cloud_model_yaml="$(yaml_model_list "${CLOUD_MODEL_MODELS:-}")"
  if [ -z "$cloud_model_yaml" ]; then
    cloud_model_yaml='          - "cloud-model"
'
  fi
  cloud_endpoint_yaml="
    - name: \"Cloud Model\"
      apiKey: \"${CLOUD_MODEL_API_KEY:-}\"
      baseURL: \"${CLOUD_MODEL_BASE_URL:-}\"
      models:
        default:
${cloud_model_yaml}
        fetch: false
      titleConvo: true
      titleModel: \"${CLOUD_MODEL_TITLE_MODEL:-${LIBRECHAT_TITLE_MODEL:-$title_model}}\"
      summarize: false
      summaryModel: \"${CLOUD_MODEL_SUMMARY_MODEL:-${LIBRECHAT_SUMMARY_MODEL:-$summary_model}}\"
"
fi

kimi_model="${KIMI_MODEL:-kimi-k2.6}"
kimi_models="${KIMI_MODELS:-$kimi_model}"
kimi_model_yaml="$(yaml_model_list "$kimi_models")"
if [ -z "$kimi_model_yaml" ]; then
  kimi_model_yaml='          - "kimi-k2.6"
'
fi

model_endpoints_yaml="    - name: \"Moonshot\"
      apiKey: \"\${KIMI_API_KEY}\"
      baseURL: \"\${KIMI_BASE_URL}\"
      models:
        default:
${kimi_model_yaml}
        fetch: false
      titleConvo: true
      titleModel: \"${KIMI_TITLE_MODEL:-$kimi_model}\"
      summarize: false
      summaryModel: \"${KIMI_SUMMARY_MODEL:-$kimi_model}\"
"

# LibreChat ходит к Kimi/Moonshot напрямую. MCP tools подключаются самим LibreChat.
export LIBRECHAT_TITLE_MODEL="$title_model"
export LIBRECHAT_SUMMARY_MODEL="$summary_model"
export LOCAL_OLLAMA_MODEL_LIST_YAML="$local_model_yaml"
export CLOUD_MODEL_ENDPOINT_YAML="$cloud_endpoint_yaml"
export MODEL_ENDPOINTS_YAML="$model_endpoints_yaml"
export LANGFUSE_MCP_BASIC_TOKEN="${LANGFUSE_MCP_BASIC_TOKEN:-}"

# Маленький Python-блок делает безопасную текстовую подстановку placeholders в YAML-template.
# На выходе создается /app/librechat.yaml, который затем читает LibreChat backend.
python3 - <<'PY'
from pathlib import Path
import os

template = Path('/app/librechat.yaml.template').read_text()
for key in [
    'LIBRECHAT_TITLE_MODEL',
    'LIBRECHAT_SUMMARY_MODEL',
    'LOCAL_OLLAMA_MODEL_LIST_YAML',
    'CLOUD_MODEL_ENDPOINT_YAML',
    'MODEL_ENDPOINTS_YAML',
    'LANGFUSE_MCP_BASIC_TOKEN',
]:
    template = template.replace('${' + key + '}', os.environ.get(key, ''))
Path('/app/librechat.yaml').write_text(template)
PY

# Передаем управление стандартному backend-старту LibreChat из готового образа.
exec npm run backend
