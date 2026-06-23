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

openmodel_model_spec_yaml=""
case "${ANTHROPIC_REVERSE_PROXY:-}" in
  *api.openmodel.ai*)
    openmodel_model="$(printf '%s' "${ANTHROPIC_MODELS:-deepseek-v4-flash}" | cut -d, -f1 | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    if [ -n "${ANTHROPIC_API_KEY:-}" ] && [ -n "$openmodel_model" ]; then
      openmodel_model_spec_yaml="    - name: \"openmodel-${openmodel_model}\"
      label: \"DeepSeek V4 Flash · OpenModel\"
      description: \"OpenModel cloud: ${openmodel_model}. MCP tools are opt-in to preserve the model context window.\"
      preset:
        endpoint: \"anthropic\"
        model: \"${openmodel_model}\"
"
    fi
    ;;
esac
kimi_reasoning_params_enabled="${KIMI_REASONING_PARAMS_ENABLED:-auto}"
kimi_reasoning_params_yaml=""
case "$kimi_reasoning_params_enabled:${KIMI_BASE_URL:-https://api.moonshot.ai/v1}" in
  true:*|auto:*openrouter.ai*)
    kimi_reasoning_effort="${KIMI_REASONING_EFFORT:-none}"
    kimi_include_reasoning="${KIMI_INCLUDE_REASONING:-false}"
    kimi_reasoning_exclude="${KIMI_REASONING_EXCLUDE:-true}"
    kimi_reasoning_params_yaml="        include_reasoning: ${kimi_include_reasoning}
        reasoning_effort: \"${kimi_reasoning_effort}\"
        reasoning:
          effort: \"${kimi_reasoning_effort}\"
          exclude: ${kimi_reasoning_exclude}
"
    ;;
esac

model_endpoints_yaml="    - name: \"Moonshot\"
      apiKey: \"\${KIMI_API_KEY}\"
      baseURL: \"\${KIMI_BASE_URL}\"
      models:
        default:
${kimi_model_yaml}
        fetch: false
      addParams:
        thinking:
          type: \"${KIMI_THINKING_TYPE:-disabled}\"
${kimi_reasoning_params_yaml}
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
export OPENMODEL_MODEL_SPEC_YAML="$openmodel_model_spec_yaml"
export LANGFUSE_MCP_BASIC_TOKEN="${LANGFUSE_MCP_BASIC_TOKEN:-}"
export GRAFANA_BASE_URL="${GRAFANA_BASE_URL:-http://localhost:3001}"
export GRAFANA_MCP_TIMEOUT_MS="${GRAFANA_MCP_TIMEOUT_MS:-300000}"
export KIMI_API_KEY="${KIMI_API_KEY:-${MOONSHOT_API_KEY:-}}"
export KIMI_BASE_URL="${KIMI_BASE_URL:-https://api.moonshot.ai/v1}"
export KIMI_MODEL="${KIMI_MODEL:-kimi-k2.6}"
export ADS_ANALYTICS_DATABASE="${ADS_ANALYTICS_DATABASE:-${CLICKHOUSE_DB:-analytics}}"
ads_llm_result_database="${ADS_LLM_LOG_RESULT_DATABASE:-${LLM_LOG_RESULT_DATABASE:-${CLICKHOUSE_DB:-analytics}}}"
ads_llm_investigations_table="${ADS_LLM_LOG_INVESTIGATIONS_TABLE:-${LLM_LOG_INVESTIGATIONS_TABLE:-llm_log_investigations}}"
ads_llm_chunk_reports_table="${ADS_LLM_LOG_CHUNK_REPORTS_TABLE:-${LLM_LOG_CHUNK_REPORTS_TABLE:-llm_log_chunk_reports}}"
ads_llm_refined_sql_table="${ADS_LLM_LOG_REFINED_SQL_TABLE:-${LLM_LOG_REFINED_SQL_TABLE:-llm_log_refined_sql}}"
export ADS_LLM_LOG_INVESTIGATIONS_FQN="${ads_llm_result_database}.${ads_llm_investigations_table}"
export ADS_LLM_LOG_CHUNK_REPORTS_FQN="${ads_llm_result_database}.${ads_llm_chunk_reports_table}"
export ADS_LLM_LOG_REFINED_SQL_FQN="${ads_llm_result_database}.${ads_llm_refined_sql_table}"
export ADS_LLM_LOG_REFINEMENT_DAG_ID="${ADS_LLM_LOG_REFINEMENT_DAG_ID:-${AIRFLOW_LLM_SQL_REFINEMENT_DAG_ID:-llm_guided_log_sql_refinement}}"
export LIBRECHAT_AGENTS_RECURSION_LIMIT="${LIBRECHAT_AGENTS_RECURSION_LIMIT:-80}"
export LIBRECHAT_AGENTS_MAX_RECURSION_LIMIT="${LIBRECHAT_AGENTS_MAX_RECURSION_LIMIT:-120}"

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
    'OPENMODEL_MODEL_SPEC_YAML',
    'KIMI_API_KEY',
    'KIMI_BASE_URL',
    'LANGFUSE_MCP_BASIC_TOKEN',
    'GRAFANA_BASE_URL',
    'GRAFANA_MCP_TIMEOUT_MS',
    'KIMI_MODEL',
    'ADS_ANALYTICS_DATABASE',
    'ADS_LLM_LOG_INVESTIGATIONS_FQN',
    'ADS_LLM_LOG_CHUNK_REPORTS_FQN',
    'ADS_LLM_LOG_REFINED_SQL_FQN',
    'ADS_LLM_LOG_REFINEMENT_DAG_ID',
    'LIBRECHAT_AGENTS_RECURSION_LIMIT',
    'LIBRECHAT_AGENTS_MAX_RECURSION_LIMIT',
]:
    template = template.replace('${' + key + '}', os.environ.get(key, ''))
Path('/app/librechat.yaml').write_text(template)
PY

# Передаем управление стандартному backend-старту LibreChat из готового образа.
exec npm run backend
