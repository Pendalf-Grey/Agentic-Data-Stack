FROM node:22-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY proto ./proto
COPY src ./src
EXPOSE 3355
CMD ["npm", "start"]
