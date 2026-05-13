import http from 'node:http';
import { randomUUID } from 'node:crypto';

import { createClient } from '@clickhouse/client';

const port = Number(process.env.PORT || 3333);
const publicBaseUrl = (process.env.PUBLIC_BASE_URL || `http://localhost:${port}`).replace(/\/$/, '');
const grafanaBaseUrl = (process.env.GRAFANA_BASE_URL || 'http://localhost:3001').replace(/\/$/, '');
const grafanaApiUrl = (process.env.GRAFANA_API_URL || 'http://grafana:3000').replace(/\/$/, '');
const grafanaUser = process.env.GRAFANA_USER || 'admin';
const grafanaPassword = process.env.GRAFANA_PASSWORD || 'admin';
const chartStore = new Map();

const clickhouse = createClient({
  url: process.env.CLICKHOUSE_HOST || 'http://localhost:8123',
  username: process.env.CLICKHOUSE_USER || 'analytics',
  password: process.env.CLICKHOUSE_PASSWORD || 'analytics_password',
  database: process.env.CLICKHOUSE_DATABASE || 'analytics',
});

const tools = [
  {
    name: 'describe_analytics_schema',
    description: 'Describe ClickHouse analytics tables and views available for analysis.',
    inputSchema: {
      type: 'object',
      properties: {},
    },
  },
  {
    name: 'list_analytics_tables',
    description: 'List real tables and views in the ClickHouse analytics database, including row and size estimates. This returns table names only, not Prometheus metric_name values.',
    inputSchema: {
      type: 'object',
      properties: {
        include_empty: {
          type: 'boolean',
          description: 'Include empty analytics tables.',
          default: true,
        },
      },
    },
  },
  {
    name: 'list_non_empty_analytics_tables',
    description: 'Live authoritative list of real non-empty tables in the ClickHouse analytics database. Use this every time the user asks which ClickHouse tables are non-empty. Return only the rows from this tool; do not answer from memory and do not use Prometheus metric tools for table inventory.',
    inputSchema: {
      type: 'object',
      properties: {},
    },
  },
  {
    name: 'sample_app_events',
    description: 'Return recent rows migrated from PostgreSQL to ClickHouse by Debezium.',
    inputSchema: {
      type: 'object',
      properties: {
        limit: {
          type: 'number',
          description: 'Maximum number of rows to return.',
          default: 10,
        },
      },
    },
  },
  {
    name: 'event_summary',
    description: 'Return hourly event metrics from ClickHouse analytics.v_event_summary.',
    inputSchema: {
      type: 'object',
      properties: {
        limit: {
          type: 'number',
          description: 'Maximum number of rows to return.',
          default: 50,
        },
      },
    },
  },
  {
    name: 'route_performance',
    description: 'Analyze request volume, users, error rate, average latency, and p95 latency by route.',
    inputSchema: {
      type: 'object',
      properties: {
        limit: {
          type: 'number',
          description: 'Maximum number of routes to return.',
          default: 20,
        },
      },
    },
  },
  {
    name: 'model_usage',
    description: 'Analyze model usage, token usage, completions, and total cost by model_name.',
    inputSchema: {
      type: 'object',
      properties: {
        limit: {
          type: 'number',
          description: 'Maximum number of models to return.',
          default: 20,
        },
      },
    },
  },
  {
    name: 'prometheus_metric_summary',
    description: 'Analyze Prometheus metrics stored in ClickHouse by metric_name over recent time buckets.',
    inputSchema: {
      type: 'object',
      properties: {
        limit: {
          type: 'number',
          description: 'Maximum number of rows to return.',
          default: 50,
        },
      },
    },
  },
  {
    name: 'prometheus_targets',
    description: 'Return Prometheus up target health from ClickHouse, sorted with problematic targets first.',
    inputSchema: {
      type: 'object',
      properties: {
        limit: {
          type: 'number',
          description: 'Maximum number of targets to return.',
          default: 50,
        },
      },
    },
  },
  {
    name: 'sample_prometheus_metrics',
    description: 'Return recent Prometheus samples for one metric_name from ClickHouse.',
    inputSchema: {
      type: 'object',
      properties: {
        metric_name: {
          type: 'string',
          description: 'Prometheus metric name, for example up or http_requests_total.',
        },
        limit: {
          type: 'number',
          description: 'Maximum number of samples to return.',
          default: 50,
        },
      },
      required: ['metric_name'],
    },
  },
  {
    name: 'prometheus_label_values',
    description: 'Return frequent label values for a Prometheus metric stored in ClickHouse.',
    inputSchema: {
      type: 'object',
      properties: {
        metric_name: {
          type: 'string',
          description: 'Prometheus metric name.',
        },
        label: {
          type: 'string',
          description: 'Label name, for example job, instance, route, service, pod, namespace.',
        },
        limit: {
          type: 'number',
          description: 'Maximum number of values to return.',
          default: 50,
        },
      },
      required: ['metric_name', 'label'],
    },
  },
  {
    name: 'error_trends',
    description: 'Analyze hourly errors by route and status code from migrated ClickHouse events.',
    inputSchema: {
      type: 'object',
      properties: {
        limit: {
          type: 'number',
          description: 'Maximum number of rows to return.',
          default: 50,
        },
      },
    },
  },
  {
    name: 'visualize_event_volume',
    description: 'Render an SVG line chart of migrated log/event volume over time grouped by event_type.',
    inputSchema: {
      type: 'object',
      properties: {
        hours: {
          type: 'number',
          description: 'Number of latest hourly buckets to visualize.',
          default: 24,
        },
      },
    },
  },
  {
    name: 'visualize_route_performance',
    description: 'Render an SVG bar chart for route events, error_rate, avg_latency_ms, or p95_latency_ms.',
    inputSchema: {
      type: 'object',
      properties: {
        metric: {
          type: 'string',
          enum: ['events', 'error_rate', 'avg_latency_ms', 'p95_latency_ms'],
          description: 'Route performance metric to visualize.',
          default: 'error_rate',
        },
        limit: {
          type: 'number',
          description: 'Maximum number of routes to visualize.',
          default: 10,
        },
      },
    },
  },
  {
    name: 'visualize_model_usage',
    description: 'Render an SVG bar chart for model usage, token usage, cost, or latency.',
    inputSchema: {
      type: 'object',
      properties: {
        metric: {
          type: 'string',
          enum: ['events', 'total_tokens', 'total_cost_usd', 'avg_latency_ms'],
          description: 'Model usage metric to visualize.',
          default: 'total_cost_usd',
        },
        limit: {
          type: 'number',
          description: 'Maximum number of models to visualize.',
          default: 10,
        },
      },
    },
  },
  {
    name: 'run_readonly_query',
    description: 'Run a read-only SELECT query against ClickHouse analytics database.',
    inputSchema: {
      type: 'object',
      properties: {
        query: {
          type: 'string',
          description: 'SELECT query to execute.',
        },
      },
      required: ['query'],
    },
  },
];

function jsonRpc(id, result) {
  return JSON.stringify({ jsonrpc: '2.0', id, result });
}

function jsonRpcError(id, code, message) {
  return JSON.stringify({ jsonrpc: '2.0', id, error: { code, message } });
}

function boundedLimit(value, fallback = 50, maximum = 500) {
  const parsed = Number(value || fallback);
  if (!Number.isFinite(parsed) || parsed < 1) {
    return fallback;
  }
  return Math.min(Math.floor(parsed), maximum);
}

function safeChoice(value, allowed, fallback) {
  return allowed.includes(value) ? value : fallback;
}

function safeIdentifier(value, fallback = '') {
  const text = String(value || fallback);
  if (!/^[A-Za-z_:][A-Za-z0-9_:]*$/.test(text)) {
    throw new Error(`Unsafe identifier-like value: ${text}`);
  }
  return text;
}

function safeLabelName(value) {
  const text = String(value || '');
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(text)) {
    throw new Error(`Unsafe Prometheus label name: ${text}`);
  }
  return text;
}

function numberValue(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function escapeXml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&apos;');
}

function axisTicks(maxValue, count = 4) {
  if (maxValue <= 0) {
    return [0];
  }
  return Array.from({ length: count + 1 }, (_, index) => (maxValue * index) / count);
}

function renderBarChart({ title, subtitle, rows, labelKey, valueKey, valueLabel }) {
  const width = 920;
  const height = 520;
  const margin = { top: 72, right: 48, bottom: 130, left: 86 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const values = rows.map(row => numberValue(row[valueKey]));
  const maxValue = Math.max(...values, 1);
  const barGap = 14;
  const barWidth = Math.max(18, (plotWidth - barGap * Math.max(rows.length - 1, 0)) / Math.max(rows.length, 1));
  const ticks = axisTicks(maxValue);
  const palette = ['#2563eb', '#0f766e', '#7c3aed', '#dc2626', '#ca8a04', '#0891b2'];

  const grid = ticks.map(tick => {
    const y = margin.top + plotHeight - (tick / maxValue) * plotHeight;
    return `
      <line x1="${margin.left}" y1="${y}" x2="${width - margin.right}" y2="${y}" stroke="#e5e7eb" />
      <text x="${margin.left - 12}" y="${y + 4}" text-anchor="end" font-size="12" fill="#64748b">${escapeXml(tick.toFixed(tick < 10 ? 2 : 0))}</text>`;
  }).join('');

  const bars = rows.map((row, index) => {
    const value = numberValue(row[valueKey]);
    const x = margin.left + index * (barWidth + barGap);
    const barHeight = (value / maxValue) * plotHeight;
    const y = margin.top + plotHeight - barHeight;
    const color = palette[index % palette.length];
    return `
      <rect x="${x}" y="${y}" width="${barWidth}" height="${barHeight}" rx="5" fill="${color}" />
      <text x="${x + barWidth / 2}" y="${y - 8}" text-anchor="middle" font-size="12" fill="#334155">${escapeXml(value.toFixed(value < 10 ? 3 : 0))}</text>
      <text x="${x + barWidth / 2}" y="${height - margin.bottom + 36}" transform="rotate(-42 ${x + barWidth / 2} ${height - margin.bottom + 36})" text-anchor="end" font-size="12" fill="#334155">${escapeXml(row[labelKey])}</text>`;
  }).join('');

  return `
<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">
  <rect width="100%" height="100%" fill="#ffffff" />
  <text x="${margin.left}" y="34" font-family="Inter, Arial, sans-serif" font-size="24" font-weight="700" fill="#0f172a">${escapeXml(title)}</text>
  <text x="${margin.left}" y="58" font-family="Inter, Arial, sans-serif" font-size="13" fill="#475569">${escapeXml(subtitle)}</text>
  <text x="${width - margin.right}" y="58" text-anchor="end" font-family="Inter, Arial, sans-serif" font-size="12" fill="#64748b">${escapeXml(valueLabel)}</text>
  <g font-family="Inter, Arial, sans-serif">
    ${grid}
    <line x1="${margin.left}" y1="${margin.top}" x2="${margin.left}" y2="${margin.top + plotHeight}" stroke="#cbd5e1" />
    <line x1="${margin.left}" y1="${margin.top + plotHeight}" x2="${width - margin.right}" y2="${margin.top + plotHeight}" stroke="#cbd5e1" />
    ${bars}
  </g>
</svg>`;
}

function renderLineChart({ title, subtitle, rows }) {
  const width = 960;
  const height = 540;
  const margin = { top: 76, right: 180, bottom: 92, left: 78 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const hours = [...new Set(rows.map(row => row.hour))].sort();
  const seriesNames = [...new Set(rows.map(row => row.event_type))].sort();
  const maxValue = Math.max(...rows.map(row => numberValue(row.events)), 1);
  const palette = ['#2563eb', '#dc2626', '#0f766e', '#7c3aed', '#ca8a04', '#0891b2', '#be123c'];
  const xFor = hour => {
    const index = Math.max(hours.indexOf(hour), 0);
    return margin.left + (hours.length <= 1 ? plotWidth / 2 : (index / (hours.length - 1)) * plotWidth);
  };
  const yFor = value => margin.top + plotHeight - (numberValue(value) / maxValue) * plotHeight;
  const ticks = axisTicks(maxValue);
  const bySeries = new Map();
  for (const row of rows) {
    const key = row.event_type;
    if (!bySeries.has(key)) {
      bySeries.set(key, new Map());
    }
    bySeries.get(key).set(row.hour, numberValue(row.events));
  }

  const grid = ticks.map(tick => {
    const y = yFor(tick);
    return `
      <line x1="${margin.left}" y1="${y}" x2="${width - margin.right}" y2="${y}" stroke="#e5e7eb" />
      <text x="${margin.left - 12}" y="${y + 4}" text-anchor="end" font-size="12" fill="#64748b">${escapeXml(tick.toFixed(0))}</text>`;
  }).join('');

  const lines = seriesNames.map((name, index) => {
    const color = palette[index % palette.length];
    const points = hours.map(hour => `${xFor(hour)},${yFor(bySeries.get(name)?.get(hour) || 0)}`).join(' ');
    const circles = hours.map(hour => {
      const value = bySeries.get(name)?.get(hour) || 0;
      return `<circle cx="${xFor(hour)}" cy="${yFor(value)}" r="3.5" fill="${color}" />`;
    }).join('');
    const legendY = margin.top + index * 24;
    return `
      <polyline points="${points}" fill="none" stroke="${color}" stroke-width="3" stroke-linejoin="round" stroke-linecap="round" />
      ${circles}
      <rect x="${width - margin.right + 30}" y="${legendY - 10}" width="12" height="12" rx="2" fill="${color}" />
      <text x="${width - margin.right + 48}" y="${legendY}" font-size="13" fill="#334155">${escapeXml(name)}</text>`;
  }).join('');

  const labels = hours.map((hour, index) => {
    if (hours.length > 10 && index % Math.ceil(hours.length / 8) !== 0) {
      return '';
    }
    const label = hour.slice(5, 16);
    const x = xFor(hour);
    return `<text x="${x}" y="${height - margin.bottom + 28}" transform="rotate(-35 ${x} ${height - margin.bottom + 28})" text-anchor="end" font-size="12" fill="#334155">${escapeXml(label)}</text>`;
  }).join('');

  return `
<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">
  <rect width="100%" height="100%" fill="#ffffff" />
  <text x="${margin.left}" y="34" font-family="Inter, Arial, sans-serif" font-size="24" font-weight="700" fill="#0f172a">${escapeXml(title)}</text>
  <text x="${margin.left}" y="58" font-family="Inter, Arial, sans-serif" font-size="13" fill="#475569">${escapeXml(subtitle)}</text>
  <g font-family="Inter, Arial, sans-serif">
    ${grid}
    <line x1="${margin.left}" y1="${margin.top}" x2="${margin.left}" y2="${margin.top + plotHeight}" stroke="#cbd5e1" />
    <line x1="${margin.left}" y1="${margin.top + plotHeight}" x2="${width - margin.right}" y2="${margin.top + plotHeight}" stroke="#cbd5e1" />
    ${lines}
    ${labels}
  </g>
</svg>`;
}

function chartResponse(id, svg, rows, metadata) {
  const chartId = `${randomUUID()}.svg`;
  chartStore.set(chartId, {
    svg,
    createdAt: Date.now(),
  });

  const chartUrl = `${publicBaseUrl}/charts/${chartId}`;
  return jsonRpc(id, {
    content: [
      {
        type: 'text',
        text: [
          `Chart URL: ${chartUrl}`,
          '',
          `Markdown image: ![${metadata.title || 'ClickHouse chart'}](${chartUrl})`,
          '',
          JSON.stringify({ metadata: { ...metadata, chartUrl, markdownImage: `![${metadata.title || 'ClickHouse chart'}](${chartUrl})` }, rows }, null, 2),
        ].join('\n'),
      },
    ],
  });
}

function grafanaPanelUrl(panelId, from = 'now-24h', to = 'now') {
  const params = new URLSearchParams({
    orgId: '1',
    from,
    to,
    viewPanel: String(panelId),
  });
  return `${grafanaBaseUrl}/d/agentic-data-stack-events/agentic-data-stack-events?${params.toString()}`;
}

function grafanaRelativePanelPath(panelId, from = 'now-24h', to = 'now') {
  const params = new URLSearchParams({
    orgId: '1',
    from,
    to,
    viewPanel: String(panelId),
  });
  return `d/agentic-data-stack-events/agentic-data-stack-events?${params.toString()}`;
}

async function createGrafanaShortUrl(panelId, from = 'now-24h', to = 'now') {
  const path = grafanaRelativePanelPath(panelId, from, to);
  const auth = Buffer.from(`${grafanaUser}:${grafanaPassword}`).toString('base64');
  const response = await fetch(`${grafanaApiUrl}/api/short-urls`, {
    method: 'POST',
    headers: {
      Authorization: `Basic ${auth}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ path }),
  });

  if (!response.ok) {
    const message = await response.text();
    throw new Error(`Grafana short URL failed: ${response.status} ${message}`);
  }

  const payload = await response.json();
  return `${grafanaBaseUrl}/goto/${payload.uid}?orgId=1`;
}

async function grafanaResponse(id, rows, metadata) {
  const grafanaUrl = grafanaPanelUrl(metadata.panelId, metadata.from, metadata.to);
  let grafanaShortUrl = grafanaUrl;
  try {
    grafanaShortUrl = await createGrafanaShortUrl(metadata.panelId, metadata.from, metadata.to);
  } catch (error) {
    console.error(error.message);
  }

  return jsonRpc(id, {
    content: [
      {
        type: 'text',
        text: [
          `Browser-ready Grafana short URL (copy exactly, do not rewrite host or port): ${grafanaShortUrl}`,
          `Full Grafana panel URL (fallback): ${grafanaUrl}`,
          `Important: use localhost:3001 for the user's browser. Do not use grafana:3000, grafana-server:3000, or port 3000.`,
          '',
          JSON.stringify({
            metadata: {
              ...metadata,
              grafanaUrl,
              grafanaShortUrl,
              browserUrl: grafanaUrl,
              browserShortUrl: grafanaShortUrl,
              urlInstruction: 'Return browserShortUrl exactly as-is. Never rewrite it to port 3000 and never invent d-solo URLs.',
            },
            rows,
          }, null, 2),
        ].join('\n'),
      },
    ],
  });
}

async function runQuery(query) {
  const normalized = query.trim().replace(/;+$/, '');
  if (!/^select\b/i.test(normalized)) {
    throw new Error('Only SELECT queries are allowed.');
  }
  if (/\b(insert|update|delete|alter|drop|truncate|create|grant|revoke|optimize)\b/i.test(normalized)) {
    throw new Error('Only read-only SELECT queries are allowed.');
  }

  const result = await clickhouse.query({
    query: normalized,
    format: 'JSONEachRow',
  });
  return result.json();
}

async function runUserQuery(query) {
  const normalized = query.trim().replace(/;+$/, '');
  if (/\b(system|information_schema|INFORMATION_SCHEMA|langfuse)\s*\./i.test(normalized)) {
    throw new Error('Generic queries are limited to the analytics database. Use analytics tables/views or a purpose-built tool.');
  }
  if (/\bFROM\s+(?!analytics\.|\()/i.test(normalized) || /\bJOIN\s+(?!analytics\.|\()/i.test(normalized)) {
    throw new Error('Use fully qualified analytics.* tables/views in run_readonly_query.');
  }
  return runQuery(normalized);
}

async function handleRpc(payload) {
  const { id, method, params } = payload;

  if (method === 'initialize') {
    return jsonRpc(id, {
      protocolVersion: '2024-11-05',
      capabilities: { tools: {} },
      serverInfo: { name: 'clickhouse-analytics-mcp', version: '0.1.0' },
    });
  }

  if (method === 'tools/list') {
    return jsonRpc(id, { tools });
  }

  if (method === 'tools/call') {
    const name = params?.name;
    const args = params?.arguments || {};

    if (name === 'describe_analytics_schema') {
      const rows = await runQuery(`
        SELECT
          table,
          name,
          type,
          default_kind,
          default_expression
        FROM system.columns
        WHERE database = 'analytics'
          AND table IN (
            'app_events_raw',
            'v_event_summary',
            'car_inventory_raw',
            'v_car_inventory_summary',
            'prometheus_samples',
            'v_prometheus_metric_summary',
            'v_prometheus_targets'
          )
        ORDER BY table, position
      `);
      return jsonRpc(id, {
        content: [{ type: 'text', text: JSON.stringify(rows, null, 2) }],
      });
    }

    if (name === 'list_analytics_tables') {
      const includeEmpty = args.include_empty !== false;
      const rows = await runQuery(`
        SELECT
          database,
          name AS table,
          engine,
          total_rows AS rows,
          formatReadableSize(total_bytes) AS bytes
        FROM system.tables
        WHERE database = 'analytics'
          ${includeEmpty ? '' : 'AND ifNull(total_rows, 0) > 0'}
        ORDER BY database, name
      `);
      return jsonRpc(id, {
        content: [{ type: 'text', text: JSON.stringify(rows, null, 2) }],
      });
    }

    if (name === 'list_non_empty_analytics_tables') {
      const rows = await runQuery(`
        SELECT
          database,
          name AS table,
          engine,
          total_rows AS rows,
          formatReadableSize(total_bytes) AS bytes
        FROM system.tables
        WHERE database = 'analytics'
          AND engine NOT LIKE '%View'
          AND ifNull(total_rows, 0) > 0
        ORDER BY database, name
      `);
      return jsonRpc(id, {
        content: [{ type: 'text', text: JSON.stringify(rows, null, 2) }],
      });
    }

    if (name === 'sample_app_events') {
      const limit = boundedLimit(args.limit, 10, 100);
      const rows = await runQuery(`
        SELECT
          id,
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
        FROM analytics.app_events_raw
        ORDER BY event_time DESC, id DESC
        LIMIT ${limit}
      `);
      return jsonRpc(id, {
        content: [{ type: 'text', text: JSON.stringify(rows, null, 2) }],
      });
    }

    if (name === 'event_summary') {
      const limit = boundedLimit(args.limit, 50, 500);
      const rows = await runQuery(`SELECT * FROM analytics.v_event_summary LIMIT ${limit}`);
      return jsonRpc(id, {
        content: [{ type: 'text', text: JSON.stringify(rows, null, 2) }],
      });
    }

    if (name === 'route_performance') {
      const limit = boundedLimit(args.limit, 20, 100);
      const rows = await runQuery(`
        SELECT
          route,
          count() AS events,
          uniqExact(user_id) AS users,
          countIf(status_code >= 400) AS errors,
          round(errors / events, 4) AS error_rate,
          round(avgOrNull(latency_ms), 2) AS avg_latency_ms,
          quantileOrNull(0.95)(latency_ms) AS p95_latency_ms
        FROM analytics.app_events_raw
        WHERE route IS NOT NULL
        GROUP BY route
        ORDER BY events DESC, errors DESC
        LIMIT ${limit}
      `);
      return jsonRpc(id, {
        content: [{ type: 'text', text: JSON.stringify(rows, null, 2) }],
      });
    }

    if (name === 'model_usage') {
      const limit = boundedLimit(args.limit, 20, 100);
      const rows = await runQuery(`
        SELECT
          model_name,
          count() AS events,
          countIf(event_type = 'model_completion') AS completions,
          sumOrNull(prompt_tokens) AS total_prompt_tokens,
          sumOrNull(completion_tokens) AS total_completion_tokens,
          sum(ifNull(prompt_tokens, 0) + ifNull(completion_tokens, 0)) AS total_tokens,
          sumOrNull(total_cost_usd) AS total_cost_usd,
          round(avgOrNull(latency_ms), 2) AS avg_latency_ms
        FROM analytics.app_events_raw
        WHERE model_name IS NOT NULL
        GROUP BY model_name
        ORDER BY total_cost_usd DESC, total_tokens DESC
        LIMIT ${limit}
      `);
      return jsonRpc(id, {
        content: [{ type: 'text', text: JSON.stringify(rows, null, 2) }],
      });
    }

    if (name === 'prometheus_metric_summary') {
      const limit = boundedLimit(args.limit, 50, 500);
      const rows = await runQuery(`
        SELECT
          minute,
          metric_name,
          samples,
          min_value,
          max_value,
          round(avg_value, 4) AS avg_value,
          round(p95_value, 4) AS p95_value
        FROM analytics.v_prometheus_metric_summary
        ORDER BY minute DESC, metric_name ASC
        LIMIT ${limit}
      `);
      return jsonRpc(id, {
        content: [{ type: 'text', text: JSON.stringify(rows, null, 2) }],
      });
    }

    if (name === 'prometheus_targets') {
      const limit = boundedLimit(args.limit, 50, 500);
      const rows = await runQuery(`
        SELECT
          job,
          instance,
          last_sample_time,
          last_up,
          min_up,
          round(avg_up, 4) AS avg_up
        FROM analytics.v_prometheus_targets
        ORDER BY last_up ASC, min_up ASC, job ASC, instance ASC
        LIMIT ${limit}
      `);
      return jsonRpc(id, {
        content: [{ type: 'text', text: JSON.stringify(rows, null, 2) }],
      });
    }

    if (name === 'sample_prometheus_metrics') {
      const metricName = safeIdentifier(args.metric_name);
      const limit = boundedLimit(args.limit, 50, 500);
      const rows = await runQuery(`
        SELECT
          metric_name,
          labels_json,
          sample_time,
          value,
          source,
          ingest_mode,
          ingest_time
        FROM analytics.prometheus_samples
        WHERE metric_name = '${metricName}'
        ORDER BY sample_time DESC
        LIMIT ${limit}
      `);
      return jsonRpc(id, {
        content: [{ type: 'text', text: JSON.stringify(rows, null, 2) }],
      });
    }

    if (name === 'prometheus_label_values') {
      const metricName = safeIdentifier(args.metric_name);
      const label = safeLabelName(args.label);
      const limit = boundedLimit(args.limit, 50, 500);
      const rows = await runQuery(`
        SELECT
          JSONExtractString(labels_json, '${label}') AS label_value,
          count() AS samples,
          max(sample_time) AS last_sample_time
        FROM analytics.prometheus_samples
        WHERE metric_name = '${metricName}'
          AND label_value != ''
        GROUP BY label_value
        ORDER BY samples DESC, label_value ASC
        LIMIT ${limit}
      `);
      return jsonRpc(id, {
        content: [{ type: 'text', text: JSON.stringify(rows, null, 2) }],
      });
    }

    if (name === 'error_trends') {
      const limit = boundedLimit(args.limit, 50, 500);
      const rows = await runQuery(`
        SELECT
          toStartOfHour(parseDateTimeBestEffortOrNull(event_time)) AS hour,
          route,
          status_code,
          count() AS errors,
          uniqExact(user_id) AS affected_users,
          round(avgOrNull(latency_ms), 2) AS avg_latency_ms
        FROM analytics.app_events_raw
        WHERE status_code >= 400
        GROUP BY hour, route, status_code
        ORDER BY hour DESC, errors DESC
        LIMIT ${limit}
      `);
      return jsonRpc(id, {
        content: [{ type: 'text', text: JSON.stringify(rows, null, 2) }],
      });
    }

    if (name === 'visualize_event_volume') {
      const hours = boundedLimit(args.hours, 24, 168);
      const rows = await runQuery(`
        SELECT
          toString(hour) AS hour,
          event_type,
          sum(events) AS events
        FROM analytics.v_event_summary
        WHERE hour IS NOT NULL
        GROUP BY hour, event_type
        ORDER BY hour ASC, event_type ASC
        LIMIT ${hours * 8}
      `);
      const latestHours = [...new Set(rows.map(row => row.hour))].sort().slice(-hours);
      const filteredRows = rows.filter(row => latestHours.includes(row.hour));
      return grafanaResponse(id, filteredRows, {
        title: 'Event Volume Over Time',
        chart: 'grafana-timeseries',
        panelId: 3,
        from: 'now-24h',
        to: 'now',
        metric: 'events',
        groupBy: 'event_type',
        source: 'analytics.v_event_summary',
      });
    }

    if (name === 'visualize_route_performance') {
      const metric = safeChoice(args.metric, ['events', 'error_rate', 'avg_latency_ms', 'p95_latency_ms'], 'error_rate');
      const limit = boundedLimit(args.limit, 10, 30);
      const rows = await runQuery(`
        SELECT
          route,
          count() AS events,
          uniqExact(user_id) AS users,
          countIf(status_code >= 400) AS errors,
          round(errors / events, 4) AS error_rate,
          round(avgOrNull(latency_ms), 2) AS avg_latency_ms,
          quantileOrNull(0.95)(latency_ms) AS p95_latency_ms
        FROM analytics.app_events_raw
        WHERE route IS NOT NULL
        GROUP BY route
        ORDER BY ${metric === 'error_rate' ? 'error_rate ASC, p95_latency_ms ASC' : `${metric} DESC`}
        LIMIT ${limit}
      `);
      return grafanaResponse(id, rows, {
        title: 'Route Performance',
        chart: 'grafana-barchart',
        panelId: metric === 'error_rate' ? 6 : 7,
        from: 'now-24h',
        to: 'now',
        metric,
        groupBy: 'route',
        source: 'analytics.app_events_raw',
      });
    }

    if (name === 'visualize_model_usage') {
      const metric = safeChoice(args.metric, ['events', 'total_tokens', 'total_cost_usd', 'avg_latency_ms'], 'total_cost_usd');
      const limit = boundedLimit(args.limit, 10, 30);
      const rows = await runQuery(`
        SELECT
          model_name,
          count() AS events,
          countIf(event_type = 'model_completion') AS completions,
          sumOrNull(prompt_tokens) AS total_prompt_tokens,
          sumOrNull(completion_tokens) AS total_completion_tokens,
          sum(ifNull(prompt_tokens, 0) + ifNull(completion_tokens, 0)) AS total_tokens,
          sumOrNull(total_cost_usd) AS total_cost_usd,
          round(avgOrNull(latency_ms), 2) AS avg_latency_ms
        FROM analytics.app_events_raw
        WHERE model_name IS NOT NULL
        GROUP BY model_name
        ORDER BY ${metric} DESC
        LIMIT ${limit}
      `);
      return grafanaResponse(id, rows, {
        title: 'Model Usage',
        chart: 'grafana-barchart',
        panelId: 8,
        from: 'now-24h',
        to: 'now',
        metric,
        groupBy: 'model_name',
        source: 'analytics.app_events_raw',
      });
    }

    if (name === 'run_readonly_query') {
      const rows = await runUserQuery(args.query);
      return jsonRpc(id, {
        content: [{ type: 'text', text: JSON.stringify(rows, null, 2) }],
      });
    }

    return jsonRpcError(id, -32602, `Unknown tool: ${name}`);
  }

  if (method === 'notifications/initialized') {
    return '';
  }

  return jsonRpcError(id, -32601, `Unknown method: ${method}`);
}

const server = http.createServer(async (req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  if ((req.method === 'GET' || req.method === 'HEAD') && req.url?.startsWith('/charts/')) {
    const chartId = decodeURIComponent(req.url.slice('/charts/'.length));
    const chart = chartStore.get(chartId);
    if (!chart) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Chart not found' }));
      return;
    }

    res.writeHead(200, {
      'Content-Type': 'image/svg+xml; charset=utf-8',
      'Cache-Control': 'no-store',
      'Access-Control-Allow-Origin': '*',
    });
    if (req.method === 'HEAD') {
      res.end();
      return;
    }
    res.end(chart.svg);
    return;
  }

  if (req.url !== '/mcp' || req.method !== 'POST') {
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Not found' }));
    return;
  }

  let body = '';
  req.on('data', chunk => {
    body += chunk;
  });

  req.on('end', async () => {
    try {
      const payload = JSON.parse(body || '{}');
      const response = await handleRpc(payload);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(response);
    } catch (error) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(jsonRpcError(null, -32000, error.message));
    }
  });
});

server.listen(port, '0.0.0.0', () => {
  console.log(`ClickHouse MCP server listening on ${port}`);
});
