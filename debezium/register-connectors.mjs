import fs from 'node:fs/promises';
import net from 'node:net';

const connectUrl = (process.env.CONNECT_URL || 'http://debezium:8083').replace(/\/$/, '');
const sourceMode = (process.env.SOURCE_MODE || 'external').trim().toLowerCase();
const activeSource = (process.env.ACTIVE_SOURCE_DB || 'postgres').trim().toLowerCase();
const connectorsDir = '/connectors';
const sourceTemplate = `${connectorsDir}/${activeSource}-source.json`;
const sinkTemplate = `${connectorsDir}/clickhouse-sink.json`;
const activePrefix = `${activeSource.toUpperCase()}_SOURCE`;
process.env.ACTIVE_SOURCE_TOPIC = process.env[`${activePrefix}_TOPIC`] || '';

function requiredEnv(name) {
  const value = process.env[name];
  if (value === undefined || value === '') {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function renderTemplate(text) {
  return text.replace(/\$\{([A-Z0-9_]+)\}/g, (_, name) => requiredEnv(name));
}

async function readConnector(path) {
  const rendered = renderTemplate(await fs.readFile(path, 'utf8'));
  return JSON.parse(rendered);
}

async function readConnectorName(path) {
  return JSON.parse(await fs.readFile(path, 'utf8')).name;
}

async function request(path, options = {}) {
  const response = await fetch(`${connectUrl}${path}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...(options.headers || {}),
    },
  });

  const text = await response.text();
  if (!response.ok) {
    throw new Error(`${options.method || 'GET'} ${path} failed with ${response.status}: ${text}`);
  }

  return text ? JSON.parse(text) : null;
}

async function waitForConnect() {
  console.log(`Waiting for Kafka Connect at ${connectUrl}...`);
  for (let attempt = 1; attempt <= 60; attempt += 1) {
    try {
      await request('/connectors');
      return;
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }
  throw new Error(`Kafka Connect did not become ready at ${connectUrl}`);
}

function waitForPort(host, port) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ host, port: Number(port) });
    socket.setTimeout(3000);
    socket.once('connect', () => {
      socket.end();
      resolve();
    });
    socket.once('timeout', () => {
      socket.destroy();
      reject(new Error('timeout'));
    });
    socket.once('error', reject);
  });
}

async function waitForSource() {
  const host = process.env[`${activePrefix}_HOST`];
  const port = process.env[`${activePrefix}_PORT`];

  if (!host || !port) {
    return;
  }

  console.log(`Waiting for active source ${activeSource} at ${host}:${port}...`);
  for (let attempt = 1; attempt <= 60; attempt += 1) {
    try {
      await waitForPort(host, port);
      return;
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 2000));
    }
  }
  throw new Error(`Source ${activeSource} did not become reachable at ${host}:${port}`);
}

async function upsertConnector(connector) {
  try {
    await request(`/connectors/${connector.name}`);
    console.log(`Updating connector ${connector.name}`);
    await request(`/connectors/${connector.name}/config`, {
      method: 'PUT',
      body: JSON.stringify(connector.config),
    });
  } catch (error) {
    if (!error.message.includes('404')) {
      throw error;
    }

    console.log(`Registering connector ${connector.name}`);
    await request('/connectors', {
      method: 'POST',
      body: JSON.stringify(connector),
    });
  }
}

async function deleteConnector(name) {
  try {
    await request(`/connectors/${name}`, { method: 'DELETE' });
    console.log(`Deleted inactive source connector ${name}`);
  } catch (error) {
    if (!error.message.includes('404')) {
      throw error;
    }
  }
}

async function listSourceTemplates() {
  const files = await fs.readdir(connectorsDir);
  return files.filter((file) => file.endsWith('-source.json'));
}

async function main() {
  if (!['external', 'demo'].includes(sourceMode)) {
    throw new Error(`SOURCE_MODE must be "external" or "demo", got "${sourceMode}"`);
  }

  console.log(`Source mode is ${sourceMode}; active source is ${activeSource}`);
  await fs.access(sourceTemplate);

  await waitForSource();
  await waitForConnect();

  const activeConnector = await readConnector(sourceTemplate);
  const sinkConnector = await readConnector(sinkTemplate);

  for (const file of await listSourceTemplates()) {
    if (file === `${activeSource}-source.json`) {
      continue;
    }

    await deleteConnector(await readConnectorName(`${connectorsDir}/${file}`));
  }

  await upsertConnector(activeConnector);
  await upsertConnector(sinkConnector);

  console.log(`Debezium source is ${activeSource}; connectors are ready`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
