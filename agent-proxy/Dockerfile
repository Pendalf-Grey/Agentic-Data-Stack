FROM node:22-alpine

WORKDIR /app
COPY package*.json ./
RUN npm install
COPY src ./src
EXPOSE 3344
CMD ["npm", "start"]
