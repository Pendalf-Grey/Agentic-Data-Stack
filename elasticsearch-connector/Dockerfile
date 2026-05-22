FROM python:3.12-alpine

# Контейнер намеренно минимальный: только Python runtime и наш server.py.
# Внешние библиотеки не нужны, потому что Elasticsearch и ClickHouse вызываются по HTTP.
WORKDIR /app

# Кладем исходный код коннектора внутрь контейнера.
COPY src ./src

# По умолчанию запускается HTTP server для batch-режима.
# Для stream-режима docker-compose передает ELASTICSEARCH_CONNECTOR_MODE=stream.
CMD ["python", "src/server.py"]
