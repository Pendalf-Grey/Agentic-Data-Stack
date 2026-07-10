DROP VIEW IF EXISTS analytics.v_prometheus_targets;
DROP VIEW IF EXISTS analytics.v_prometheus_metric_summary;
DROP VIEW IF EXISTS analytics.v_event_summary;
DROP VIEW IF EXISTS analytics.v_elasticsearch_index_summary;
DROP VIEW IF EXISTS analytics.v_elasticsearch_event_timeline;
DROP VIEW IF EXISTS analytics.v_car_inventory_summary;

DROP TABLE IF EXISTS analytics.prometheus_samples;
DROP TABLE IF EXISTS analytics.kimi_raw_demo_payloads;
DROP TABLE IF EXISTS analytics.kimi_raw_demo_chunks;
DROP TABLE IF EXISTS analytics.kimi_raw_demo_runs;
DROP TABLE IF EXISTS analytics.ingestion_offsets;
DROP TABLE IF EXISTS analytics.elasticsearch_events_raw;
DROP TABLE IF EXISTS analytics.car_inventory_raw;
DROP TABLE IF EXISTS analytics.app_events_raw;
