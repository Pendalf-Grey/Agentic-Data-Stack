FROM node:22-alpine

WORKDIR /app

# Устанавливаем зависимости MCP server. Этот контейнер принимает MCP-запросы от LibreChat.
COPY package*.json ./
RUN npm install

# Копируем основной код MCP server: ClickHouse tools, Grafana tools и loader generated-коннекторов.
COPY src ./src

EXPOSE 3333
CMD ["npm", "start"]
