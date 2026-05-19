FROM node:22-alpine

WORKDIR /app

# Устанавливаем зависимости MCP server. Этот контейнер принимает MCP-запросы от LibreChat.
COPY package*.json ./
RUN npm install

# Копируем основной код MCP server: ClickHouse tools, Grafana tools и loader generated-коннекторов.
COPY src ./src

# Копируем стартовую папку generated-коннекторов. В docker-compose она перекрывается bind mount,
# чтобы созданные моделью JS-файлы сохранялись на хосте и были доступны для ручной правки.
COPY generated-connectors ./generated-connectors

EXPOSE 3333
CMD ["npm", "start"]
