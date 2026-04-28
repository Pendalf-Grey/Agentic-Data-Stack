CREATE TABLE IF NOT EXISTS app_events (
  id BIGSERIAL PRIMARY KEY,
  event_time TIMESTAMPTZ NOT NULL DEFAULT now(),
  user_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  route TEXT,
  status_code INTEGER,
  latency_ms INTEGER,
  model_name TEXT,
  prompt_tokens INTEGER,
  completion_tokens INTEGER,
  total_cost_usd NUMERIC(12, 6),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_app_events_event_time ON app_events(event_time);
CREATE INDEX IF NOT EXISTS idx_app_events_user_id ON app_events(user_id);
CREATE INDEX IF NOT EXISTS idx_app_events_event_type ON app_events(event_type);

ALTER TABLE app_events REPLICA IDENTITY FULL;

INSERT INTO app_events (
  event_time,
  user_id,
  session_id,
  event_type,
  route,
  status_code,
  latency_ms,
  model_name,
  prompt_tokens,
  completion_tokens,
  total_cost_usd,
  metadata
)
SELECT
  now() - (gs || ' minutes')::interval,
  'user_' || ((gs % 25) + 1),
  'session_' || ((gs % 80) + 1),
  (ARRAY['page_view', 'chat_message', 'tool_call', 'model_completion', 'error'])[1 + (gs % 5)],
  (ARRAY['/', '/chat', '/agents', '/settings', '/api/completions'])[1 + (gs % 5)],
  CASE WHEN gs % 17 = 0 THEN 500 WHEN gs % 11 = 0 THEN 429 ELSE 200 END,
  50 + (random() * 2500)::int,
  (ARRAY['gpt-4o-mini', 'claude-3-5-sonnet', 'local-llama', 'gemini-1.5-pro'])[1 + (gs % 4)],
  50 + (random() * 1500)::int,
  20 + (random() * 1200)::int,
  round((random() * 0.08)::numeric, 6),
  jsonb_build_object(
    'source', 'seed',
    'environment', 'local',
    'request_id', gen_random_uuid()::text
  )
FROM generate_series(1, 1000) AS gs;
