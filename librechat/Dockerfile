FROM ghcr.io/danny-avila/librechat@sha256:a46254938507971e0d4f7ed3f9d116bd9b118f4810b5b75eb716baf575645068 AS ui-builder

ENV NODE_OPTIONS=--max-old-space-size=4096

COPY patches/client/src/components/Chat/Messages/Content/ToolCall.tsx /app/client/src/components/Chat/Messages/Content/ToolCall.tsx
RUN npm ci --include=dev --fetch-retries=5 --fetch-retry-factor=2 --fetch-retry-mintimeout=1000 --fetch-retry-maxtimeout=30000
RUN npm run build:client

FROM ghcr.io/danny-avila/librechat@sha256:a46254938507971e0d4f7ed3f9d116bd9b118f4810b5b75eb716baf575645068

COPY patches/api/server/middleware/abortMiddleware.js /app/api/server/middleware/abortMiddleware.js
COPY patches/api/server/routes/agents/index.js /app/api/server/routes/agents/index.js
COPY patches/apply-ads-progress-bridge.cjs /tmp/apply-ads-progress-bridge.cjs
COPY --from=ui-builder /app/client/dist /app/client/dist
RUN node /tmp/apply-ads-progress-bridge.cjs
