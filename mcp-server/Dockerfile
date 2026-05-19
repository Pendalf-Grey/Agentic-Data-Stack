FROM python:3.12-alpine

WORKDIR /app

# MCP server теперь написан на Python и работает только на стандартной библиотеке.
# LibreChat отправляет сюда MCP JSON-RPC, а сервер дальше ходит в ClickHouse/Grafana.
COPY src ./src

EXPOSE 3333
CMD ["python", "src/server.py"]
