FROM ghcr.io/danny-avila/librechat@sha256:a46254938507971e0d4f7ed3f9d116bd9b118f4810b5b75eb716baf575645068

COPY patches/api/server/middleware/abortMiddleware.js /app/api/server/middleware/abortMiddleware.js
COPY patches/api/server/routes/agents/index.js /app/api/server/routes/agents/index.js
COPY patches/api/server/services/cancelAirflowRunsForAbortedMessage.js /app/api/server/services/cancelAirflowRunsForAbortedMessage.js
