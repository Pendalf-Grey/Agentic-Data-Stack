import http from 'node:http';
import { createHash } from 'node:crypto';

import { createClient } from '@clickhouse/client';
import protobuf from 'protobufjs';
import snappy from 'snappyjs';

const port = Number(process.env.PORT || 3355);
const prometheusBaseUrl = (process.env.PROMETHEUS_BASE_URL || 'http://prometheus:9090').replace(/\/$/, '');
const prometheusBearerToken = process.env.PROMETHEUS_BEARER_TOKEN || '';
const prometheusBasicUser = process.env.PROMETHEUS_BASIC_USER || '';
const prometheusBasicPassword = process.env.PROMETHEUS_BASIC_PASSWORD || '';
const defaultBackfillStep = process.env.PROMETHEUS_BACKFILL_STEP || '60s';
const defaultBackfillQuery = process.env.PROMETHEUS_BACKFILL_QUERY || 'up';
const sourceName = process.env.PROMETHEUS_SOURCE_NAME || 'prometheus';
const debugJsonEnabled = (process.env.PROMETHEUS_DEBUG_JSON_ENABLED || 'true').toLowerCase() === 'true';

const clickhouse = createClient({
  url: process.env.CLICKHOUSE_HOST || 'http://clickhouse:8123',
  username: process.env.CLICKHOUSE_USER || 'analytics',
  password: process.env.CLICKHOUSE_PASSWORD || 'analytics_password',
  database: process.env.CLICKHOUSE_DATABASE || 'analytics',
});

const protoRoot = await protobuf.load('/app/proto/remote_write.proto');
const WriteRequest = protoRoot.lookupType('prometheus.WriteRequest');

function sendJson(res, status, data) {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(data));
}

function readRawBody(req, limitBytes = 50 * 1024 * 1024) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let total = 0;
    req.on('data', chunk => {
      total += chunk.length;
      if (total > limitBytes) {
        reject(new Error(`Request body exceeds ${limitBytes} bytes`));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

async function readJsonBody(req) {
  const raw = await readRawBody(req);
  if (raw.length === 0) {
    return {};
  }
  return JSON.parse(raw.toString('utf8'));
}

function labelsToObject(labels = []) {
  const object = {};
  for (const label of labels) {
    object[label.name] = label.value;
  }
  return object;
}

function stableLabelsJson(labelsObject) {
  const sorted = Object.keys(labelsObject)
    .sort()
    .reduce((acc, key) => {
      acc[key] = labelsObject[key];
      return acc;
    }, {});
  return JSON.stringify(sorted);
}

function fingerprint(labelsJson) {
  return createHash('sha256').update(labelsJson).digest('hex');
}

function timestampToDateTime64(timestampMs) {
  return new Date(Number(timestampMs)).toISOString().replace('T', ' ').replace('Z', '');
}

function rowsFromTimeSeries(timeSeries, ingestMode) {
  const rows = [];
  for (const series of timeSeries) {
    const labelsObject = labelsToObject(series.labels || []);
    const metricName = labelsObject.__name__ || 'unknown_metric';
    const labelsJson = stableLabelsJson(labelsObject);
    const labelsFingerprint = fingerprint(labelsJson);

    for (const sample of series.samples || []) {
      rows.push({
        metric_name: metricName,
        labels_json: labelsJson,
        fingerprint: labelsFingerprint,
        sample_time: timestampToDateTime64(sample.timestamp),
        value: Number(sample.value),
        source: sourceName,
        ingest_mode: ingestMode,
      });
    }
  }
  return rows;
}

async function insertRows(rows) {
  if (rows.length === 0) {
    return { inserted: 0 };
  }

  await clickhouse.insert({
    table: 'prometheus_samples',
    values: rows,
    format: 'JSONEachRow',
  });
  return { inserted: rows.length };
}

function prometheusHeaders() {
  const headers = { Accept: 'application/json' };
  if (prometheusBearerToken) {
    headers.Authorization = `Bearer ${prometheusBearerToken}`;
  } else if (prometheusBasicUser || prometheusBasicPassword) {
    headers.Authorization = `Basic ${Buffer.from(`${prometheusBasicUser}:${prometheusBasicPassword}`).toString('base64')}`;
  }
  return headers;
}

function unixSeconds(value) {
  if (typeof value === 'number') {
    return String(value);
  }
  if (/^\d+(\.\d+)?$/.test(String(value))) {
    return String(value);
  }
  return String(Date.parse(value) / 1000);
}

async function queryRange({ query, start, end, step }) {
  const params = new URLSearchParams({
    query,
    start: unixSeconds(start),
    end: unixSeconds(end),
    step: step || defaultBackfillStep,
  });

  const response = await fetch(`${prometheusBaseUrl}/api/v1/query_range?${params.toString()}`, {
    headers: prometheusHeaders(),
  });
  const payload = await response.json();

  if (!response.ok || payload.status !== 'success') {
    throw new Error(`Prometheus query_range failed: HTTP ${response.status} ${JSON.stringify(payload)}`);
  }

  return payload.data?.result || [];
}

function rowsFromQueryRangeResult(result, query) {
  const rows = [];
  for (const series of result) {
    const labelsObject = { ...series.metric };
    const metricName = labelsObject.__name__ || query;
    if (!labelsObject.__name__) {
      labelsObject.__name__ = query;
    }
    const labelsJson = stableLabelsJson(labelsObject);
    const labelsFingerprint = fingerprint(labelsJson);

    for (const [timestampSeconds, value] of series.values || []) {
      rows.push({
        metric_name: metricName,
        labels_json: labelsJson,
        fingerprint: labelsFingerprint,
        sample_time: timestampToDateTime64(Number(timestampSeconds) * 1000),
        value: Number(value),
        source: sourceName,
        ingest_mode: 'backfill',
      });
    }
  }
  return rows;
}

async function handleRemoteWrite(req, res) {
  const raw = await readRawBody(req);
  const decompressed = Buffer.from(snappy.uncompress(raw));
  const decoded = WriteRequest.decode(decompressed);
  const object = WriteRequest.toObject(decoded, {
    longs: Number,
    defaults: false,
    arrays: true,
  });
  const rows = rowsFromTimeSeries(object.timeseries || [], 'remote_write');
  const result = await insertRows(rows);
  res.writeHead(204, {
    'X-Prometheus-Remote-Write-Version': '0.1.0',
  });
  res.end();
  console.log(JSON.stringify({ event: 'remote_write_ingested', ...result }));
}

async function handleBackfill(req, res) {
  const body = await readJsonBody(req);
  const queries = Array.isArray(body.queries) && body.queries.length > 0
    ? body.queries
    : [body.query || defaultBackfillQuery];
  const end = body.end || new Date().toISOString();
  const start = body.start || new Date(Date.now() - 60 * 60 * 1000).toISOString();
  const step = body.step || defaultBackfillStep;
  let inserted = 0;
  const details = [];

  for (const query of queries) {
    const result = await queryRange({ query, start, end, step });
    const rows = rowsFromQueryRangeResult(result, query);
    const insertResult = await insertRows(rows);
    inserted += insertResult.inserted;
    details.push({ query, series: result.length, inserted: insertResult.inserted });
  }

  sendJson(res, 200, { ok: true, mode: 'backfill', start, end, step, inserted, details });
}

async function handleDebugJson(req, res) {
  if (!debugJsonEnabled) {
    sendJson(res, 404, { error: 'Not found' });
    return;
  }
  const body = await readJsonBody(req);
  const rows = rowsFromTimeSeries(body.timeseries || [], body.ingest_mode || 'debug_json');
  const result = await insertRows(rows);
  sendJson(res, 200, { ok: true, ...result });
}

const server = http.createServer(async (req, res) => {
  try {
    if (req.method === 'GET' && req.url === '/health') {
      sendJson(res, 200, {
        ok: true,
        prometheusBaseUrl,
        clickhouseDatabase: process.env.CLICKHOUSE_DATABASE || 'analytics',
        remoteWritePath: '/api/v1/write',
        backfillPath: '/backfill',
      });
      return;
    }

    if (req.method === 'POST' && req.url === '/api/v1/write') {
      await handleRemoteWrite(req, res);
      return;
    }

    if (req.method === 'POST' && req.url === '/backfill') {
      await handleBackfill(req, res);
      return;
    }

    if (req.method === 'POST' && req.url === '/debug/write-json') {
      await handleDebugJson(req, res);
      return;
    }

    sendJson(res, 404, { error: 'Not found' });
  } catch (error) {
    console.error(error);
    sendJson(res, 500, { error: error.message });
  }
});

server.listen(port, '0.0.0.0', () => {
  console.log(`prometheus-connector listening on 0.0.0.0:${port}`);
  console.log(`prometheus base URL: ${prometheusBaseUrl}`);
});
