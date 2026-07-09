FROM python:3.12-alpine

# Контейнер намеренно минимальный: только Python runtime и наш server.py.
# Внешние библиотеки не нужны, потому что Elasticsearch и ClickHouse вызываются по HTTP.
WORKDIR /app

# Кладем исходный код коннектора внутрь контейнера.
COPY src ./src

# Запускается HTTP server с единственным demo-режимом: batch import.
CMD ["python", "src/server.py"]
