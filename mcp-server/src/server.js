import http from 'node:http';

import { createClient } from '@clickhouse/client';

const port = Number(process.env.PORT || 3333);

const clickhouse = createClient({
  url: process.env.CLICKHOUSE_HOST || 'http://localhost:8123',
  username: process.env.CLICKHOUSE_USER || 'analytics',
  password: process.env.CLICKHOUSE_PASSWORD || 'analytics_password',
  database: process.env.CLICKHOUSE_DATABASE || 'analytics',
});

const tools = [
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

async function runQuery(query) {
  const normalized = query.trim().replace(/;+$/, '');
  if (!/^select\b/i.test(normalized)) {
    throw new Error('Only SELECT queries are allowed.');
  }

  const result = await clickhouse.query({
    query: normalized,
    format: 'JSONEachRow',
  });
  return result.json();
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

    if (name === 'event_summary') {
      const limit = Math.min(Number(args.limit || 50), 500);
      const rows = await runQuery(`SELECT * FROM analytics.v_event_summary LIMIT ${limit}`);
      return jsonRpc(id, {
        content: [{ type: 'text', text: JSON.stringify(rows, null, 2) }],
      });
    }

    if (name === 'run_readonly_query') {
      const rows = await runQuery(args.query);
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
