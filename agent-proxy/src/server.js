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

function stripCodeFence(text) {
  return text
    .replace(/^```(?:json|sql)?\s*/i, '')
    .replace(/\s*```$/i, '')
    .trim();
}

function parseJsonObject(text) {
  const cleaned = stripCodeFence(text);
  try {
    return JSON.parse(cleaned);
  } catch {
    try {
      return JSON.parse(cleaned.replace(/,\s*}/g, '}').replace(/,\s*]/g, ']'));
    } catch {
      // Continue to extracting the first object below.
    }
    const start = cleaned.indexOf('{');
    const end = cleaned.lastIndexOf('}');
    if (start >= 0 && end > start) {
      const objectText = cleaned.slice(start, end + 1).replace(/,\s*}/g, '}').replace(/,\s*]/g, ']');
      return JSON.parse(objectText);
    }
    throw new Error(`Model did not return a JSON object: ${text}`);
  }
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

function normalizeSql(query) {
  return query
    .trim()
    .replace(/;+$/g, '')
    .replace(/\s+FORMAT\s+\w+$/i, '')
    .trim();
}

function validateReadOnlyAnalyticsQuery(query) {
  const normalized = normalizeSql(query);
  if (!/^select\b/i.test(normalized)) {
    throw new Error('Only SELECT queries are allowed for ClickHouse questions.');
  }
  if (/\b(insert|update|delete|alter|drop|truncate|create|grant|revoke|optimize|attach|detach)\b/i.test(normalized)) {
    throw new Error('Only read-only SELECT queries are allowed for ClickHouse questions.');
  }
  if (/\b(information_schema|INFORMATION_SCHEMA|langfuse)\s*\./i.test(normalized)) {
    throw new Error('ClickHouse questions are limited to analytics data and analytics metadata.');
  }

  const referencesSystem = /\bsystem\s*\./i.test(normalized);
  if (referencesSystem) {
    if (/\bsystem\s*\.(?!tables\b|columns\b)/i.test(normalized)) {
      throw new Error('Only system.tables and system.columns metadata are allowed.');
    }
    if (!/\bdatabase\s*=\s*'analytics'/i.test(normalized)) {
      throw new Error("Metadata queries must include database = 'analytics'.");
    }
  }

  if (/\bFROM\s+(?!analytics\.|system\.(?:tables|columns)\b|\()/i.test(normalized)
    || /\bJOIN\s+(?!analytics\.|system\.(?:tables|columns)\b|\()/i.test(normalized)) {
    throw new Error('ClickHouse queries must use fully qualified analytics.* tables/views or allowed system metadata.');
  }
  return normalized;
}

async function callUpstreamJson(body) {
  const response = await fetch(`${upstreamBaseUrl}/chat/completions`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${upstreamApiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      model: body.model,
      stream: false,
      temperature: 0,
      messages: body.messages,
    }),
  });
  const data = await response.json();
  if (!response.ok) {
    throw new Error(`Upstream returned HTTP ${response.status}: ${JSON.stringify(data)}`);
  }
  return data;
}

async function clickHouseSchema() {
  return queryClickHouseRows(`
    SELECT
      table,
      name,
      type
    FROM system.columns
    WHERE database = 'analytics'
    ORDER BY table, position
    FORMAT JSONEachRow
  `);
}

function schemaPrompt(rows) {
  const grouped = new Map();
  for (const row of rows) {
    if (!grouped.has(row.table)) {
      grouped.set(row.table, []);
    }
    grouped.get(row.table).push(`${row.name} ${row.type}`);
  }
  return Array.from(grouped, ([table, columns]) => `analytics.${table}(${columns.join(', ')})`).join('\n');
}

async function planClickHouseQuery(body, userText, schemaRows, repair = null) {
  const repairPrompt = repair
    ? [
      '',
      'Previous query failed. Build a corrected query using the schema.',
      `Failed query: ${repair.query}`,
      `ClickHouse error: ${repair.error}`,
    ].join('\n')
    : '';
  const messages = [
    {
      role: 'system',
      content: [
        'You translate user questions into one safe ClickHouse SQL SELECT.',
        'Return only compact valid JSON with keys: query, intent.',
        'Do not use Markdown. Do not add comments. Do not use trailing commas.',
        'Use analytics.* tables and views shown in the schema for data questions.',
        "For inventory/schema questions, use only system.tables or system.columns and always include database = 'analytics'.",
        "For view/list of views questions, use system.tables with database = 'analytics' and engine LIKE '%View%'.",
        'Use fully qualified table names.',
        'Do not use INSERT, UPDATE, DELETE, ALTER, DROP, TRUNCATE, CREATE, information_schema, langfuse, or multiple statements.',
        'For exploratory content questions, include a sensible LIMIT unless the query is aggregate or DISTINCT.',
        'Examples:',
        'User: "что содержится в ClickHouse" -> {"query":"SELECT database, name AS table, engine, total_rows AS rows, formatReadableSize(total_bytes) AS bytes FROM system.tables WHERE database = \'analytics\' ORDER BY name","intent":"analytics inventory"}',
        'User: "какие непустые таблицы в кликхаус" -> {"query":"SELECT database, name AS table, engine, total_rows AS rows, formatReadableSize(total_bytes) AS bytes FROM system.tables WHERE database = \'analytics\' AND engine NOT LIKE \'%View\' AND ifNull(total_rows, 0) > 0 ORDER BY name","intent":"non-empty analytics tables"}',
        'User: "какие представления есть в ClickHouse" -> {"query":"SELECT database, name AS view, engine FROM system.tables WHERE database = \'analytics\' AND engine LIKE \'%View%\' ORDER BY name","intent":"analytics views"}',
        'User: "какая схема в ClickHouse" -> {"query":"SELECT table, name, type FROM system.columns WHERE database = \'analytics\' ORDER BY table, position","intent":"analytics schema"}',
        'User: "найди все уникальные марки машин" -> {"query":"SELECT DISTINCT brand FROM analytics.car_inventory_raw ORDER BY brand","intent":"unique car brands"}',
        'User: "сколько машин по городам" -> {"query":"SELECT city, count() AS cars FROM analytics.car_inventory_raw GROUP BY city ORDER BY city","intent":"cars by city"}',
        'User: "сколько доступных машин по брендам" -> {"query":"SELECT brand, sum(available_cars) AS available_cars FROM analytics.v_car_inventory_summary GROUP BY brand ORDER BY brand","intent":"available cars by brand"}',
        'User: "сколько машин сейчас в maintenance" -> {"query":"SELECT count() AS maintenance_cars FROM analytics.car_inventory_raw WHERE stock_status = \'maintenance\'","intent":"maintenance cars"}',
        'User: "сколько Toyota в Tokyo" -> {"query":"SELECT count() AS cars FROM analytics.car_inventory_raw WHERE brand = \'Toyota\' AND city = \'Tokyo\'","intent":"Toyota cars in Tokyo"}',
      ].join('\n'),
    },
    {
      role: 'user',
      content: [
        'ClickHouse analytics schema:',
        schemaPrompt(schemaRows),
        '',
        `User question: ${userText}`,
        repairPrompt,
      ].join('\n'),
    },
  ];

  for (let attempt = 0; attempt < 2; attempt += 1) {
    const data = await callUpstreamJson({
      model: body.model,
      messages,
    });

    const content = extractCompletionText(data);
    try {
      const plan = parseJsonObject(content);
      if (!plan?.query || typeof plan.query !== 'string') {
        throw new Error(`Model did not return a query: ${content}`);
      }
      return {
        query: validateReadOnlyAnalyticsQuery(plan.query),
        intent: typeof plan.intent === 'string' ? plan.intent : 'ClickHouse data question',
      };
    } catch (error) {
      if (attempt === 1) {
        throw error;
      }
      messages.push({ role: 'assistant', content });
      messages.push({
        role: 'user',
        content: 'Your previous response was not valid compact JSON. Return only valid JSON with exactly keys "query" and "intent".',
      });
    }
  }

  throw new Error('Model did not return a valid ClickHouse query plan.');
}

async function answerFromClickHouseRows(body, userText, plan, rows) {
  const data = await callUpstreamJson({
    model: body.model,
    messages: [
      {
        role: 'system',
        content: [
          'Answer the user in Russian.',
          'Use only Russian language except SQL/table/column names and literal values from the query result.',
          'Use only the provided ClickHouse query result rows.',
          'Do not add facts that are not present in the rows.',
          'Do not mention that you cannot access the database; the query has already been executed.',
          'Keep the answer concise and include the executed SQL at the end under "SQL:".',
        ].join('\n'),
      },
      {
        role: 'user',
        content: [
          `Original user question: ${userText}`,
          `Executed ClickHouse SQL: ${plan.query}`,
          `Query intent: ${plan.intent}`,
          'Rows JSON:',
          JSON.stringify(rows, null, 2),
        ].join('\n'),
      },
    ],
  });
  return extractCompletionText(data).trim();
}

async function clickHouseQuestionAnswer(body, userText) {
  try {
    const schemaRows = await clickHouseSchema();
    let plan = await planClickHouseQuery(body, userText, schemaRows);
    let rows;
    try {
      rows = await queryClickHouseRows(`${plan.query}\nFORMAT JSONEachRow`);
    } catch (error) {
      plan = await planClickHouseQuery(body, userText, schemaRows, {
        query: plan.query,
        error: error.message,
      });
      rows = await queryClickHouseRows(`${plan.query}\nFORMAT JSONEachRow`);
    }
    const answer = await answerFromClickHouseRows(body, userText, plan, rows);
    return answer || [
      'ClickHouse вернул результат:',
      '',
      JSON.stringify(rows, null, 2),
      '',
      `SQL: ${plan.query}`,
    ].join('\n');
  } catch (error) {
    return [
      'Не буду строить догадки по ClickHouse.',
      '',
      `Не удалось построить и выполнить безопасный live-запрос: ${error.message}`,
      '',
      'Можно спросить про таблицы, схему, строки, агрегаты или значения колонок в `analytics.*`; agent-proxy выполнит только read-only SELECT.',
    ].join('\n');
  }
}

async function clickHouseGuardedAnswer(body, text) {
  if (!isClickHouseQuestion(text)) {
    return null;
  }
  return clickHouseQuestionAnswer(body, text);
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

  const guardedClickHouseAnswer = await clickHouseGuardedAnswer(body, latestUserText(body.messages));
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
