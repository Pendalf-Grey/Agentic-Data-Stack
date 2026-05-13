import http from 'node:http';
import { randomUUID } from 'node:crypto';

const port = Number(process.env.PORT || 3344);
const upstreamBaseUrl = (process.env.UPSTREAM_OPENAI_BASE_URL || 'http://host.docker.internal:11434/v1').replace(/\/$/, '');
const upstreamApiKey = process.env.UPSTREAM_OPENAI_API_KEY || 'local-dev-key';
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
