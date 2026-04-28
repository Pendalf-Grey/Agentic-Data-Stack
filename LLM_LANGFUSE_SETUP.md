# LLM и LangFuse setup

Эта инструкция описывает, какие данные нужны для подключения локальных и облачных LLM providers, где искать локальные модели на macOS и как подключать LangFuse tracing.

## 1. Текущая ревизия моделей пользователя

### Ollama models

Найдены модели:

```text
x/flux2-klein:latest       image/generation-like, safetensors, 5.7 GB
llama3.2-vision:latest     vision/chat, 10.7B Q4_K_M, 7.8 GB
llava:latest               vision/chat, 7B Q4_0, 4.7 GB
glm-ocr:latest             OCR, 1.1B F16, 2.2 GB
llava:13b                  vision/chat, 13B Q4_0, 8.0 GB
llava:7b                   vision/chat, 7B Q4_0, 4.7 GB
nomic-embed-text:latest    embeddings, 137M F16, 274 MB
qwen2.5:14b                chat/instruct, 14.8B Q4_K_M, 9.0 GB
qwen2.5:7b                 chat/instruct, 7.6B Q4_K_M, 4.7 GB
```

Рекомендованная раскладка:

```text
Default/Fast chat model: qwen2.5:7b
Smart/deeper analysis model: qwen2.5:14b
Vision model: llama3.2-vision:latest
Embedding model: nomic-embed-text:latest
OCR-specific model: glm-ocr:latest
```

Текущий `.env` настроен под Ollama:

```env
AGENT_PROXY_API_KEY=local-dev-key
AGENT_PROXY_BASE_URL=http://agent-proxy:3344/v1
UPSTREAM_OPENAI_API_KEY=local-dev-key
UPSTREAM_OPENAI_BASE_URL=http://host.docker.internal:11434/v1
OPENAI_MODEL=qwen2.5:7b
OPENAI_MODEL_FAST=qwen2.5:7b
OPENAI_MODEL_SMART=qwen2.5:14b
OPENAI_MODEL_VISION=llama3.2-vision:latest
OPENAI_EMBEDDING_MODEL=nomic-embed-text:latest
LIBRECHAT_MODELS=qwen2.5:7b,qwen2.5:14b,llama3.2-vision:latest
LIBRECHAT_TITLE_MODEL=qwen2.5:7b
LIBRECHAT_SUMMARY_MODEL=qwen2.5:7b
```

Для других разработчиков эти значения должны быть заменены на их локальные модели. Достаточно поменять `.env`, например:

```env
LIBRECHAT_MODELS=llama3.1:8b,mistral:7b,my-local-model:latest
LIBRECHAT_TITLE_MODEL=llama3.1:8b
LIBRECHAT_SUMMARY_MODEL=llama3.1:8b
OPENAI_MODEL=llama3.1:8b
```

Файл `librechat/librechat.yaml` не нужно редактировать вручную. При старте контейнера LibreChat запускается `librechat/render-config.sh`, который берёт `librechat/librechat.yaml.template`, подставляет значения из `.env` и генерирует `/app/librechat.yaml` внутри контейнера.

### HuggingFace cache

Найдены локальные HuggingFace модели:

```text
Qwen/Qwen2.5-7B-Instruct
Qwen/Qwen2.5-14B-Instruct
mlx-community/Qwen2.5-VL-7B-Instruct-8bit
intfloat/multilingual-e5-small
sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2
deepset/xlm-roberta-base-squad2
google/mt5-small
nvidia/NVIDIA-Nemotron-Parse-v1.1
nvidia/C-RADIOv2-H
PaddlePaddle OCR/layout/table/formula models
```

Для LibreChat напрямую эти HuggingFace cache-модели не подключаются сами по себе. Их нужно поднять через runtime/server:

```text
vLLM
TGI
llama.cpp server
LM Studio
Ollama import
MLX server
custom FastAPI OpenAI-compatible wrapper
```

Для MacBook Air наиболее простой путь:

```text
Ollama для chat/vision
Grafana для графиков ClickHouse
MCP server для запросов в ClickHouse
LangFuse через реализованный agent-proxy слой
```

## 2. Что нужно предоставить для дальнейшей ревизии LLM

### Ollama

Нужны:

- список установленных моделей;
- endpoint Ollama;
- какие модели использовать для chat/completion;
- нужна ли embedding model;
- доступен ли Ollama с Docker containers.

Команды на macOS:

```bash
ollama list
ollama ps
curl http://localhost:11434/api/tags
```

Стандартный OpenAI-compatible endpoint Ollama:

```text
http://localhost:11434/v1
```

Из Docker container к Ollama на Mac обычно обращаться так:

```text
http://host.docker.internal:11434/v1
```

В `.env` это сейчас задано через `agent-proxy` так:

```env
AGENT_PROXY_BASE_URL=http://agent-proxy:3344/v1
UPSTREAM_OPENAI_BASE_URL=http://host.docker.internal:11434/v1
OPENAI_MODEL=qwen2.5:7b
LIBRECHAT_MODELS=qwen2.5:7b,qwen2.5:14b,llama3.2-vision:latest
```

Если модель называется иначе, поменяй `OPENAI_MODEL`, `LIBRECHAT_MODELS`, `LIBRECHAT_TITLE_MODEL` и `LIBRECHAT_SUMMARY_MODEL` на имена из `ollama list`.

### Где Ollama хранит модели на macOS

Обычно:

```text
~/.ollama/models
```

Проверить размер:

```bash
du -sh ~/.ollama/models
```

Посмотреть manifests:

```bash
find ~/.ollama/models/manifests -type f | head
```

### Hugging Face локальные модели

Нужны:

- путь к модели;
- формат модели: Transformers, GGUF, safetensors;
- чем она запускается: transformers server, text-generation-inference, vLLM, llama.cpp, LM Studio, Ollama import;
- есть ли OpenAI-compatible HTTP endpoint.

Частые пути cache на macOS:

```text
~/.cache/huggingface/hub
~/.cache/huggingface/transformers
~/Library/Caches/huggingface/hub
```

Команды поиска:

```bash
ls -lah ~/.cache/huggingface/hub
ls -lah ~/Library/Caches/huggingface/hub
find ~/.cache/huggingface -maxdepth 3 -type d -name 'models--*' 2>/dev/null | head -50
```

### LM Studio, если используешь

Частый endpoint:

```text
http://localhost:1234/v1
```

Из Docker:

```text
http://host.docker.internal:1234/v1
```

### vLLM, если используешь

Частый endpoint:

```text
http://localhost:8000/v1
```

Из Docker:

```text
http://host.docker.internal:8000/v1
```

### OpenCode / облачные модели

Нужны:

- base URL;
- API key;
- список model IDs;
- OpenAI-compatible ли endpoint;
- есть ли отдельные endpoints для chat/completions/embeddings.

В `.env` нужно будет указать:

```env
AGENT_PROXY_API_KEY=<local-or-provider-key-for-librechat-to-proxy>
AGENT_PROXY_BASE_URL=http://agent-proxy:3344/v1
UPSTREAM_OPENAI_API_KEY=<opencode-or-provider-key>
UPSTREAM_OPENAI_BASE_URL=<provider-openai-compatible-base-url>
OPENAI_MODEL=<model-id>
LIBRECHAT_MODELS=<model-id>,<another-model-id>
```

## 3. LibreChat подключение к локальным моделям

LibreChat config генерируется при старте контейнера из:

```text
librechat/librechat.yaml.template
librechat/render-config.sh
```

Итоговый файл появляется внутри контейнера:

```text
/app/librechat.yaml
```

Проверить:

```bash
docker compose exec librechat sh -lc "sed -n '10,30p' /app/librechat.yaml"
```

После изменения `.env` перезапустить LibreChat:

```bash
docker compose up -d --force-recreate librechat
```

Проверить logs:

```bash
docker compose logs --tail=120 librechat
```

## 4. LangFuse: что уже подготовлено

В `.env` добавлены переменные:

```env
LANGFUSE_HOST=http://localhost:3000
LANGFUSE_PUBLIC_KEY=
LANGFUSE_SECRET_KEY=
```

Они передаются в:

- `librechat`;
- `mcp-server`.
- `agent-proxy`.

Traces создаёт `agent-proxy`, потому что именно он выполняет LLM-вызовы к Ollama/OpenAI-compatible upstream.

## 5. Как фактически подключено LangFuse tracing

### Вариант A: через отдельный agent service

Реализованный в проекте вариант:

```text
LibreChat -> agent-proxy -> Ollama OpenAI-compatible API
                       -> LangFuse traces
LibreChat -> MCP server -> ClickHouse tools
```

Сервис:

```text
agent-proxy
```

доступен:

```text
http://localhost:3344
```

Внутри Docker:

```text
http://agent-proxy:3344/v1
```

Он реализует:

```text
GET  /health
GET  /v1/models
POST /v1/chat/completions
```

LibreChat теперь ходит не напрямую в Ollama, а в:

```env
AGENT_PROXY_BASE_URL=http://agent-proxy:3344/v1
```

А сам proxy ходит в Ollama:

```env
UPSTREAM_OPENAI_BASE_URL=http://host.docker.internal:11434/v1
```

Проверка:

```bash
curl http://localhost:3344/health
```

Ожидаемо:

```json
{"ok":true,"upstreamBaseUrl":"http://host.docker.internal:11434/v1","langfuseEnabled":true}
```

Тестовый LLM-вызов:

```bash
curl -fsS http://localhost:3344/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer local-dev-key' \
  --data '{"model":"qwen2.5:7b","messages":[{"role":"user","content":"Ответь одним словом: OK"}],"stream":false}'
```

Streaming тоже поддерживается. Proxy пропускает SSE chunks в LibreChat, накапливает финальный текст ответа и после завершения stream отправляет trace/generation в LangFuse.

### Вариант B: через MCP server

MCP server сейчас не делает LLM-вызовы, он только выполняет ClickHouse queries. Поэтому он может логировать tool calls в LangFuse, но не полноценные LLM generations.

Что можно добавить позже:

- trace на каждый MCP tool call;
- observation для SQL query;
- metadata: query, duration, row count, error.

### Вариант C: если LibreChat поддержит LangFuse напрямую

Нужно проверить конкретную версию LibreChat и её поддержку LangFuse/OpenTelemetry callbacks. Если поддержка есть, достаточно будет env/config. Если нет — нужен proxy/agent layer.

## 6. Что нужно от разработчика для точного подключения

Скопируй выводы:

```bash
ollama list
curl http://localhost:11434/api/tags
```

Если есть HuggingFace models:

```bash
find ~/.cache/huggingface -maxdepth 3 -type d -name 'models--*' 2>/dev/null | head -50
find ~/Library/Caches/huggingface -maxdepth 3 -type d -name 'models--*' 2>/dev/null | head -50
```

Если используешь LM Studio/vLLM/TGI/OpenCode:

```text
base URL
model IDs
нужен ли API key
OpenAI-compatible или нет
```

После этого можно точно выбрать `UPSTREAM_OPENAI_BASE_URL`, `OPENAI_MODEL`, `LIBRECHAT_MODELS` и проверить LangFuse tracing через `agent-proxy`.
