import http from 'node:http';

const port = Number(process.env.PORT || 3344);
const upstreamBaseUrl = (process.env.UPSTREAM_OPENAI_BASE_URL || 'http://host.docker.internal:11434/v1').replace(/\/$/, '');
const upstreamApiKey = process.env.UPSTREAM_OPENAI_API_KEY || 'local-dev-key';

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

async function proxyChatCompletions(req, res) {
  const body = await readBody(req);
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
    return;
  }

  res.writeHead(upstream.status, {
    'Content-Type': upstream.headers.get('content-type') || 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });

  const reader = upstream.body.getReader();
  const decoder = new TextDecoder();

  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }

    const chunk = decoder.decode(value, { stream: true });
    res.write(chunk);
  }

  res.end();
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
});
