import http from 'node:http';
import { randomUUID } from 'node:crypto';

const port = Number(process.env.PORT || 3344);
const upstreamBaseUrl = (process.env.UPSTREAM_OPENAI_BASE_URL || 'http://host.docker.internal:11434/v1').replace(/\/$/, '');
const upstreamApiKey = process.env.UPSTREAM_OPENAI_API_KEY || 'local-dev-key';
const clickhouseUrl = (process.env.CLICKHOUSE_HOST || 'http://clickhouse:8123').replace(/\/$/, '');
const clickhouseUser = process.env.CLICKHOUSE_USER || 'analytics';
const clickhousePassword = process.env.CLICKHOUSE_PASSWORD || 'analytics_password';
const langfuseEnabled = (process.env.LANGFUSE_ENABLED || 'false').toLowerCase() === 'true';
const langfuseBaseUrl = (process.env.LANGFUSE_BASE_URL || '').replace(/\/$/, '');
const langfusePublicKey = process.env.LANGFUSE_PUBLIC_KEY || '';
const langfuseSecretKey = process.env.LANGFUSE_SECRET_KEY || '';
const langfuseEnvironment = process.env.LANGFUSE_ENVIRONMENT || 'local';

function sendJson(res, status, data) {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(data));
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', () => {
      if (chunks.length === 0) {
        resolve({});
        return;
      }

      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      } catch (error) {
        reject(error);
      }
    });
    req.on('error', reject);
  });
}

async function proxyGetModels(res) {
  const upstream = await fetch(`${upstreamBaseUrl}/models`, {
    headers: { Authorization: `Bearer ${upstreamApiKey}` },
  });
  const data = await upstream.json();
  sendJson(res, upstream.status, data);
}

function usageFromCompletion(data) {
  if (!data?.usage) {
    return undefined;
  }

  return {
    input: data.usage.prompt_tokens,
    output: data.usage.completion_tokens,
    total: data.usage.total_tokens,
    unit: 'TOKENS',
  };
}

function extractCompletionText(data) {
  return data?.choices
    ?.map((choice) => choice?.message?.content || choice?.delta?.content || '')
    .filter(Boolean)
    .join('\n');
}

function parseStreamingContent(chunk) {
  return chunk
    .split('\n')
    .filter((line) => line.startsWith('data: '))
    .map((line) => line.slice(6).trim())
    .filter((line) => line && line !== '[DONE]')
    .map((line) => {
      try {
        return JSON.parse(line)?.choices?.[0]?.delta?.content || '';
      } catch {
        return '';
      }
    })
    .join('');
}

function messageText(message) {
  const content = message?.content;
  if (typeof content === 'string') {
    return content;
  }
  if (Array.isArray(content)) {
    return content
      .map((part) => (typeof part?.text === 'string' ? part.text : ''))
      .filter(Boolean)
      .join('\n');
  }
  return '';
}

function latestUserText(messages = []) {
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    if (messages[index]?.role === 'user') {
      return messageText(messages[index]);
    }
  }
  return '';
}

function isClickHouseQuestion(text) {
  const normalized = text.toLowerCase();
  return /clickhouse|кликхаус|кликхауз|клик хаус/.test(normalized);
}

function asksForTables(text) {
  const normalized = text.toLowerCase();
  const mentionsTables = /таблиц|table/.test(normalized);
  const asksDataInventory = /что\s+(есть|лежит)|какие\s+(есть\s+)?данн|что\s+в\s+(бд|базе)/.test(normalized);
  return mentionsTables || asksDataInventory;
}

function asksForNonEmptyTables(text) {
  const normalized = text.toLowerCase();
  const mentionsNonEmpty = /не\s*пуст|непуст|non[-\s]?empty|with\s+data|с\s+данн/.test(normalized);
  return asksForTables(text) && mentionsNonEmpty;
}

function asksForSchema(text) {
  const normalized = text.toLowerCase();
  return /схем|колон|столбц|column|schema|describe/.test(normalized);
}

async function queryClickHouseRows(query) {
  const response = await fetch(clickhouseUrl, {
    method: 'POST',
    headers: {
      Authorization: `Basic ${Buffer.from(`${clickhouseUser}:${clickhousePassword}`).toString('base64')}`,
      'Content-Type': 'text/plain; charset=utf-8',
    },
    body: query,
  });

  const text = await response.text();
  if (!response.ok) {
    throw new Error(`ClickHouse returned HTTP ${response.status}: ${text}`);
  }

  return text
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

async function clickHouseTables(includeEmpty) {
  return queryClickHouseRows(`
    SELECT
      database,
      name AS table,
      engine,
      total_rows AS rows,
      formatReadableSize(total_bytes) AS bytes
    FROM system.tables
    WHERE database = 'analytics'
      AND engine NOT LIKE '%View'
      ${includeEmpty ? '' : 'AND ifNull(total_rows, 0) > 0'}
    ORDER BY database, name
    FORMAT JSONEachRow
  `);
}

async function clickHouseSchema() {
  return queryClickHouseRows(`
    SELECT
      table,
      name,
      type
    FROM system.columns
    WHERE database = 'analytics'
      AND table IN (
        'app_events_raw',
        'car_inventory_raw',
        'prometheus_samples',
        'v_event_summary',
        'v_car_inventory_summary',
        'v_prometheus_metric_summary',
        'v_prometheus_targets'
      )
    ORDER BY table, position
    FORMAT JSONEachRow
  `);
}

function formatTableRows(title, rows) {
  if (rows.length === 0) {
    return `${title}\n\nНет таблиц.`;
  }
  return [
    title,
    '',
    ...rows.map((row) => `- \`${row.database}.${row.table}\` — ${row.rows} строк, ${row.bytes}, engine: ${row.engine}`),
  ].join('\n');
}

function tablePurpose(table) {
  return {
    app_events_raw: 'старые демо-события приложения из Debezium/PostgreSQL',
    car_inventory_raw: 'демо-инвентарь автомобилей из PostgreSQL: склады, города, бренды, модели, цены, статусы',
    prometheus_samples: 'Prometheus samples; metric_name хранится внутри строк, это не отдельные таблицы',
  }[table] || 'таблица analytics';
}

async function clickHouseInventoryAnswer({ nonEmptyOnly }) {
  const rows = await clickHouseTables(!nonEmptyOnly);
  const title = nonEmptyOnly
    ? 'Непустые таблицы в ClickHouse database `analytics`:'
    : 'Таблицы в ClickHouse database `analytics`:';
  const body = formatTableRows(title, rows);
  return [
    body,
    '',
    'Что есть что:',
    ...rows.map((row) => `- \`${row.table}\`: ${tablePurpose(row.table)}`),
    '',
    'Это live-ответ из ClickHouse через agent-proxy. Модель не строила догадки.',
  ].join('\n');
}

async function clickHouseSchemaAnswer() {
  const rows = await clickHouseSchema();
  const grouped = new Map();
  for (const row of rows) {
    if (!grouped.has(row.table)) {
      grouped.set(row.table, []);
    }
    grouped.get(row.table).push(`${row.name} ${row.type}`);
  }
  return [
    'Схема объектов ClickHouse `analytics`:',
    '',
    ...Array.from(grouped, ([table, columns]) => `- \`${table}\`: ${columns.join(', ')}`),
    '',
    'Это live-ответ из ClickHouse через agent-proxy. Модель не строила догадки.',
  ].join('\n');
}

async function clickHouseGuardedAnswer(text) {
  if (!isClickHouseQuestion(text)) {
    return null;
  }
  if (asksForSchema(text)) {
    return clickHouseSchemaAnswer();
  }
  if (asksForNonEmptyTables(text)) {
    return clickHouseInventoryAnswer({ nonEmptyOnly: true });
  }
  if (asksForTables(text)) {
    return clickHouseInventoryAnswer({ nonEmptyOnly: false });
  }
  return [
    'Я не буду строить догадки по ClickHouse.',
    '',
    'Для этого вопроса нужен live-запрос к данным. Сейчас agent-proxy гарантированно отвечает без догадок на:',
    '- какие таблицы есть в ClickHouse',
    '- какие таблицы непустые',
    '- какая схема/колонки у объектов analytics',
    '',
    'Сформулируйте вопрос в одном из этих видов, и ответ будет получен напрямую из ClickHouse.',
  ].join('\n');
}

function completionResponse(body, content) {
  return {
    id: `chatcmpl-${randomUUID()}`,
    object: 'chat.completion',
    created: Math.floor(Date.now() / 1000),
    model: body.model || 'agentic-data-stack-direct-clickhouse',
    choices: [
      {
        index: 0,
        message: { role: 'assistant', content },
        finish_reason: 'stop',
      },
    ],
    usage: {
      prompt_tokens: 0,
      completion_tokens: 0,
      total_tokens: 0,
    },
  };
}

function sendStreamingCompletion(res, body, content) {
  const id = `chatcmpl-${randomUUID()}`;
  const created = Math.floor(Date.now() / 1000);
  const model = body.model || 'agentic-data-stack-direct-clickhouse';
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });
  res.write(`data: ${JSON.stringify({ id, object: 'chat.completion.chunk', created, model, choices: [{ index: 0, delta: { role: 'assistant' }, finish_reason: null }] })}\n\n`);
  res.write(`data: ${JSON.stringify({ id, object: 'chat.completion.chunk', created, model, choices: [{ index: 0, delta: { content }, finish_reason: null }] })}\n\n`);
  res.write(`data: ${JSON.stringify({ id, object: 'chat.completion.chunk', created, model, choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] })}\n\n`);
  res.write('data: [DONE]\n\n');
  res.end();
}

async function sendLangfuseTrace({ body, output, usage, startedAt, endedAt, status }) {
  if (!langfuseEnabled || !langfuseBaseUrl || !langfusePublicKey || !langfuseSecretKey) {
    return;
  }

  const traceId = randomUUID();
  const generationId = randomUUID();
  const now = new Date().toISOString();
  const auth = Buffer.from(`${langfusePublicKey}:${langfuseSecretKey}`).toString('base64');
  const model = body.model || 'unknown-model';

  const batch = [
    {
      id: randomUUID(),
      type: 'trace-create',
      timestamp: now,
      body: {
        id: traceId,
        timestamp: startedAt,
        name: 'librechat.llm.request',
        input: body.messages || body,
        output,
        environment: langfuseEnvironment,
        tags: ['agentic-data-stack', 'librechat', 'agent-proxy'],
        metadata: {
          upstreamBaseUrl,
          stream: Boolean(body.stream),
          status,
        },
      },
    },
    {
      id: randomUUID(),
      type: 'generation-create',
      timestamp: now,
      body: {
        id: generationId,
        traceId,
        name: 'chat.completions',
        startTime: startedAt,
        endTime: endedAt,
        model,
        modelParameters: {
          temperature: body.temperature,
          top_p: body.top_p,
          max_tokens: body.max_tokens,
          stream: Boolean(body.stream),
        },
        input: body.messages || body,
        output,
        usage,
        level: status >= 400 ? 'ERROR' : 'DEFAULT',
        statusMessage: status >= 400 ? `Upstream returned HTTP ${status}` : undefined,
        metadata: {
          endpoint: '/v1/chat/completions',
        },
      },
    },
  ];

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 3000);
    const response = await fetch(`${langfuseBaseUrl}/api/public/ingestion`, {
      method: 'POST',
      headers: {
        Authorization: `Basic ${auth}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        batch,
        metadata: {
          source: 'agentic-data-stack-agent-proxy',
        },
      }),
      signal: controller.signal,
    });
    clearTimeout(timeout);

    if (!response.ok && response.status !== 207) {
      console.warn(`Langfuse ingestion returned HTTP ${response.status}`);
    }
  } catch (error) {
    console.warn(`Langfuse ingestion skipped: ${error.message}`);
  }
}

async function proxyChatCompletions(req, res) {
  const body = await readBody(req);
  const startedAt = new Date().toISOString();

  const guardedClickHouseAnswer = await clickHouseGuardedAnswer(latestUserText(body.messages));
  if (guardedClickHouseAnswer) {
    const output = guardedClickHouseAnswer;
    if (body.stream) {
      sendStreamingCompletion(res, body, output);
    } else {
      sendJson(res, 200, completionResponse(body, output));
    }
    void sendLangfuseTrace({
      body,
      output,
      usage: { input: 0, output: 0, total: 0, unit: 'TOKENS' },
      startedAt,
      endedAt: new Date().toISOString(),
      status: 200,
    });
    return;
  }

  const upstream = await fetch(`${upstreamBaseUrl}/chat/completions`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${upstreamApiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  });

  if (!body.stream) {
    const data = await upstream.json();
    sendJson(res, upstream.status, data);
    void sendLangfuseTrace({
      body,
      output: extractCompletionText(data) || data,
      usage: usageFromCompletion(data),
      startedAt,
      endedAt: new Date().toISOString(),
      status: upstream.status,
    });
    return;
  }

  res.writeHead(upstream.status, {
    'Content-Type': upstream.headers.get('content-type') || 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });

  const reader = upstream.body.getReader();
  const decoder = new TextDecoder();
  let streamedOutput = '';

  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }

    const chunk = decoder.decode(value, { stream: true });
    streamedOutput += parseStreamingContent(chunk);
    res.write(chunk);
  }

  res.end();

  void sendLangfuseTrace({
    body,
    output: streamedOutput,
    usage: undefined,
    startedAt,
    endedAt: new Date().toISOString(),
    status: upstream.status,
  });
}

const server = http.createServer(async (req, res) => {
  try {
    if (req.method === 'GET' && req.url === '/health') {
      sendJson(res, 200, { ok: true, upstreamBaseUrl });
      return;
    }

    if (req.method === 'GET' && req.url === '/v1/models') {
      await proxyGetModels(res);
      return;
    }

    if (req.method === 'POST' && req.url === '/v1/chat/completions') {
      await proxyChatCompletions(req, res);
      return;
    }

    sendJson(res, 404, { error: 'Not found' });
  } catch (error) {
    console.error(error);
    sendJson(res, 500, { error: { message: error.message, type: 'agent_proxy_error' } });
  }
});

server.listen(port, '0.0.0.0', () => {
  console.log(`agent-proxy listening on 0.0.0.0:${port}`);
  console.log(`upstream OpenAI-compatible base URL: ${upstreamBaseUrl}`);
  console.log(`langfuse tracing: ${langfuseEnabled ? 'enabled' : 'disabled'}`);
});
