FROM node:22-alpine

WORKDIR /app
COPY package*.json ./
RUN npm install
COPY src ./src
COPY generated-connectors ./generated-connectors
EXPOSE 3333
CMD ["npm", "start"]
