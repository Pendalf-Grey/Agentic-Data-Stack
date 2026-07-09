-- Minimal demo schema for the Elasticsearch-only ADS-2 branch.
-- ClickHouse is the analytical workspace. Elasticsearch stays outside the
-- compose stack and is imported into ClickHouse by elasticsearch-connector.

CREATE DATABASE IF NOT EXISTS analytics;
