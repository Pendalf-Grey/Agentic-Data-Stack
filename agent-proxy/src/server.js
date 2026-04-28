import http from 'node:http';
import { randomUUID } from 'node:crypto';

const port = Number(process.env.PORT || 3344);
const upstreamBaseUrl = (process.env.UPSTREAM_OPENAI_BASE_URL || 'http://host.docker.internal:11434/v1').replace(/\/$/, '');
const upstreamApiKey = process.env.UPSTREAM_OPENAI_API_KEY || 'local-dev-key';
const langfuseHost = (process.env.LANGFUSE_HOST || '').replace(/\/$/, '');
const langfusePublicKey = process.env.LANGFUSE_PUBLIC_KEY || '';
const langfuseSecretKey = process.env.LANGFUSE_SECRET_KEY || '';

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

function authHeader() {
  return `Basic ${Buffer.from(`${langfusePublicKey}:${langfuseSecretKey}`).toString('base64')}`;
}

async function sendLangfuseEvent(event) {
  if (!langfuseHost || !langfusePublicKey || !langfuseSecretKey) {
    return;
  }

  try {
    await fetch(`${langfuseHost}/api/public/ingestion`, {
      method: 'POST',
      headers: {
        Authorization: authHeader(),
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        batch: [event],
        metadata: { source: 'agent-proxy' },
      }),
    });
  } catch (error) {
    console.error('Failed to send LangFuse event:', error.message);
  }
}

async function traceChatCompletion(requestBody, responseBody, startedAt, endedAt) {
  const traceId = randomUUID();
  const generationId = randomUUID();
  const timestamp = new Date(startedAt).toISOString();
  const endTime = new Date(endedAt).toISOString();
  const model = requestBody.model || responseBody.model || 'unknown';
  const output = responseBody.choices?.map((choice) => choice.message).filter(Boolean) || responseBody;

  await sendLangfuseEvent({
    id: randomUUID(),
    type: 'trace-create',
    timestamp,
    body: {
      id: traceId,
      name: 'librechat-chat-completion',
      input: requestBody.messages || requestBody,
      output,
      metadata: {
        source: 'agent-proxy',
        model,
      },
    },
  });

  await sendLangfuseEvent({
    id: randomUUID(),
    type: 'generation-create',
    timestamp,
    body: {
      id: generationId,
      traceId,
      name: 'ollama-chat-completion',
      model,
      input: requestBody.messages || requestBody,
      output,
      startTime: timestamp,
      endTime,
      usage: responseBody.usage,
      metadata: {
        upstreamBaseUrl,
        source: 'agent-proxy',
      },
    },
  });
}

function extractStreamingContent(buffer) {
  const chunks = [];
  for (const line of buffer.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed.startsWith('data:')) {
      continue;
    }

    const payload = trimmed.slice(5).trim();
    if (!payload || payload === '[DONE]') {
      continue;
    }

    try {
      const parsed = JSON.parse(payload);
      const content = parsed.choices?.[0]?.delta?.content;
      if (content) {
        chunks.push(content);
      }
    } catch {
      continue;
    }
  }
  return chunks.join('');
}

async function proxyGetModels(res) {
  const upstream = await fetch(`${upstreamBaseUrl}/models`, {
    headers: { Authorization: `Bearer ${upstreamApiKey}` },
  });
  const data = await upstream.json();
  sendJson(res, upstream.status, data);
}

async function proxyChatCompletions(req, res) {
  const body = await readBody(req);
  const startedAt = Date.now();
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
    const endedAt = Date.now();
    if (upstream.ok) {
      await traceChatCompletion(body, data, startedAt, endedAt);
    }
    sendJson(res, upstream.status, data);
    return;
  }

  res.writeHead(upstream.status, {
    'Content-Type': upstream.headers.get('content-type') || 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });

  const reader = upstream.body.getReader();
  const decoder = new TextDecoder();
  let streamBuffer = '';

  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }

    const chunk = decoder.decode(value, { stream: true });
    streamBuffer += chunk;
    res.write(chunk);
  }

  res.end();

  if (upstream.ok) {
    const content = extractStreamingContent(streamBuffer);
    const data = {
      model: body.model,
      choices: [{ message: { role: 'assistant', content } }],
    };
    const endedAt = Date.now();
    await traceChatCompletion(body, data, startedAt, endedAt);
  }
}

const server = http.createServer(async (req, res) => {
  try {
    if (req.method === 'GET' && req.url === '/health') {
      sendJson(res, 200, { ok: true, upstreamBaseUrl, langfuseEnabled: Boolean(langfuseHost && langfusePublicKey && langfuseSecretKey) });
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
  console.log(`LangFuse enabled: ${Boolean(langfuseHost && langfusePublicKey && langfuseSecretKey)}`);
});
