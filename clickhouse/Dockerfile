ARG CLICKHOUSE_VERSION=26.6.1.1193-alpine
FROM clickhouse/clickhouse-server:${CLICKHOUSE_VERSION}

RUN apk add --no-cache python3

COPY users.d/*.xml /etc/clickhouse-server/users.d/
