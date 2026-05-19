import http from 'node:http';
import { randomUUID } from 'node:crypto';
import { mkdir, readFile, readdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

import { createClient } from '@clickhouse/client';

const port = Number(process.env.PORT || 3333);
const publicBaseUrl = (process.env.PUBLIC_BASE_URL || `http://localhost:${port}`).replace(/\/$/, '');
const grafanaBaseUrl = (process.env.GRAFANA_BASE_URL || 'http://localhost:3001').replace(/\/$/, '');
const grafanaApiUrl = (process.env.GRAFANA_API_URL || 'http://grafana:3000').replace(/\/$/, '');
const grafanaUser = process.env.GRAFANA_USER || 'admin';
const grafanaPassword = process.env.GRAFANA_PASSWORD || 'admin';
const chartStore = new Map();

// Папка, куда MCP server сохраняет JS-коннекторы, созданные моделью под конкретный вопрос пользователя.
// В docker-compose эта директория примонтирована наружу, поэтому файлы можно открыть и поправить вручную.
const generatedConnectorsDir = path.resolve(
  process.env.GENERATED_CONNECTORS_DIR || path.join(process.cwd(), 'generated-connectors'),
);
const generatedConnectorsPublicDir = (
  process.env.GENERATED_CONNECTORS_PUBLIC_DIR || generatedConnectorsDir
).replace(/\/$/, '');

// Кэш уже загруженных generated-коннекторов. При update/create конкретный коннектор сбрасывается из кэша.
const generatedConnectorCache = new Map();

const clickhouse = createClient({
  url: process.env.CLICKHOUSE_HOST || 'http://localhost:8123',
  username: process.env.CLICKHOUSE_USER || 'analytics',
  password: process.env.CLICKHOUSE_PASSWORD || 'analytics_password',
  database: process.env.CLICKHOUSE_DATABASE || 'analytics',
});

// Базовые MCP tools. К ним на лету добавляются JS-коннекторы из mcp-server/generated-connectors.
const baseTools = [
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
    name: 'describe_analytics_table',
    description: 'Describe one analytics table or view by name. Use this before answering questions about columns or table contents.',
    inputSchema: {
      type: 'object',
      properties: {
        table: {
          type: 'string',
          description: 'Table or view name in the analytics database, without database prefix.',
        },
        table_name: {
          type: 'string',
          description: 'Compatibility alias for table; the handler forwards this to the same ClickHouse lookup.',
        },
      },
    },
  },
  {
    name: 'sample_analytics_table',
    description: 'Return live sample rows from any analytics table or view. Use this when the user asks what data is inside a table.',
    inputSchema: {
      type: 'object',
      properties: {
        table: {
          type: 'string',
          description: 'Table or view name in the analytics database, without database prefix.',
        },
        table_name: {
          type: 'string',
          description: 'Compatibility alias for table; the handler forwards this to the same ClickHouse lookup.',
        },
        limit: {
          type: 'number',
          description: 'Maximum number of rows to return.',
          default: 10,
        },
      },
    },
  },
  {
    name: 'profile_analytics_table',
    description: 'Profile any analytics table or view: metadata, columns, row count, and sample rows. Use this to explain what kind of data a table contains.',
    inputSchema: {
      type: 'object',
      properties: {
        table: {
          type: 'string',
          description: 'Table or view name in the analytics database, without database prefix.',
        },
        table_name: {
          type: 'string',
          description: 'Compatibility alias for table; the handler forwards this to the same ClickHouse lookup.',
        },
        sample_limit: {
          type: 'number',
          description: 'Maximum number of sample rows to return.',
          default: 5,
        },
      },
    },
  },
  {
    name: 'distinct_analytics_values',
    description: 'Return distinct values from one column in any analytics table or view. Use this for questions like unique brands, cities, statuses, metric names, or labels.',
    inputSchema: {
      type: 'object',
      properties: {
        table: {
          type: 'string',
          description: 'Table or view name in the analytics database, without database prefix.',
        },
        table_name: {
          type: 'string',
          description: 'Compatibility alias for table; the handler forwards this to the same ClickHouse lookup.',
        },
        column: {
          type: 'string',
          description: 'Column name.',
        },
        limit: {
          type: 'number',
          description: 'Maximum number of values to return.',
          default: 100,
        },
      },
      required: ['column'],
    },
  },
  {
    name: 'count_analytics_by',
    description: 'Count rows in any analytics table grouped by one to three columns, with optional equality and comparison filters. Use this for distribution and "how many by ..." questions, including conditions such as mileage_km > 20000.',
    inputSchema: {
      type: 'object',
      properties: {
        table: {
          type: 'string',
          description: 'Table or view name in the analytics database, without database prefix.',
        },
        table_name: {
          type: 'string',
          description: 'Compatibility alias for table; the handler forwards this to the same ClickHouse lookup.',
        },
        dimensions: {
          type: 'array',
          items: { type: 'string' },
          description: 'One to three column names to group by.',
        },
        filters: {
          type: 'object',
          description: 'Optional equality filters by column name.',
          additionalProperties: {
            type: ['string', 'number', 'boolean'],
          },
        },
        filter_conditions: {
          type: 'array',
          description: 'Optional comparison filters. Operators: =, !=, >, >=, <, <=.',
          items: {
            type: 'object',
            properties: {
              column: { type: 'string', description: 'Column name.' },
              operator: {
                type: 'string',
                enum: ['=', '!=', '>', '>=', '<', '<=', 'eq', 'ne', 'gt', 'gte', 'lt', 'lte'],
                description: 'Comparison operator.',
              },
              value: {
                type: ['string', 'number', 'boolean'],
                description: 'Comparison value.',
              },
            },
            required: ['column', 'operator', 'value'],
          },
        },
        limit: {
          type: 'number',
          description: 'Maximum number of grouped rows to return.',
          default: 100,
        },
      },
      required: ['dimensions'],
    },
  },
  {
    name: 'create_car_inventory_dashboard',
    description: 'Create a Grafana dashboard for car inventory data stored in analytics.car_inventory_raw and return a browser-ready dashboard link. Use this when the user asks to create, build, draw, or show a Grafana dashboard for warehouses, cars, cities, brands, stock status, prices, mileage, or vehicle inventory migrated from PostgreSQL.',
    inputSchema: {
      type: 'object',
      properties: {
        title: {
          type: 'string',
          description: 'Optional dashboard title.',
          default: 'Car Inventory Dashboard',
        },
        min_mileage_km: {
          type: 'number',
          description: 'Optional minimum mileage filter, inclusive.',
        },
        max_mileage_km: {
          type: 'number',
          description: 'Optional maximum mileage filter, inclusive.',
        },
        city: {
          type: 'string',
          description: 'Optional city filter.',
        },
        brand: {
          type: 'string',
          description: 'Optional brand filter.',
        },
        stock_status: {
          type: 'string',
          description: 'Optional stock status filter, for example available, reserved, or maintenance.',
        },
      },
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
    name: 'create_prometheus_availability_dashboard',
    description: 'Create a rich Grafana dashboard for Prometheus synthetic availability, incidents, HTTP health, and DB health stored in ClickHouse. Use this for up/down, instance health, incidents, service availability, and operational overview questions.',
    inputSchema: {
      type: 'object',
      properties: {
        hours: {
          type: 'number',
          description: 'How many recent hours to include.',
          default: 24,
        },
        bucket_minutes: {
          type: 'number',
          description: 'Bucket size in minutes for time series panels.',
          default: 1,
        },
        title: {
          type: 'string',
          description: 'Optional dashboard title.',
        },
      },
    },
  },
  {
    name: 'create_prometheus_metric_dashboard',
    description: 'Create a Grafana dashboard for one Prometheus metric stored in ClickHouse and return a browser-ready dashboard link. Use this when the user asks to create, build, draw, or show a Grafana dashboard for Prometheus data.',
    inputSchema: {
      type: 'object',
      properties: {
        metric_name: {
          type: 'string',
          description: 'Prometheus metric name stored in analytics.prometheus_samples, for example up, synthetic_http_request_duration_ms, or synthetic_log_events_total.',
        },
        group_by_label: {
          type: 'string',
          description: 'Optional Prometheus label to split series by, for example job, instance, service, route, or city.',
          default: 'job',
        },
        aggregation: {
          type: 'string',
          enum: ['avg', 'min', 'max', 'p95', 'sum', 'count', 'last'],
          description: 'Aggregation for samples inside each time bucket.',
          default: 'avg',
        },
        hours: {
          type: 'number',
          description: 'How many recent hours to include.',
          default: 24,
        },
        bucket_minutes: {
          type: 'number',
          description: 'Bucket size in minutes.',
          default: 1,
        },
        title: {
          type: 'string',
          description: 'Optional dashboard title.',
        },
      },
      required: ['metric_name'],
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
    name: 'list_generated_connectors',
    description: 'List JavaScript MCP connectors generated for previous user database tasks. Use this when the user asks which saved generated connector can be reused.',
    inputSchema: {
      type: 'object',
      properties: {},
    },
  },
  {
    name: 'describe_generated_connector',
    description: 'Describe one saved generated MCP connector by name, including file path and exported schema.',
    inputSchema: {
      type: 'object',
      properties: {
        connector_name: {
          type: 'string',
          description: 'Generated connector name without .js suffix.',
        },
      },
      required: ['connector_name'],
    },
  },
  {
    name: 'create_generated_connector',
    description: 'Create a JavaScript MCP connector file for a specific user database question. Use this only after inspecting live ClickHouse schema. The final chat answer must mention only connector name, saved path, and query result; do not paste source code unless asked.',
    inputSchema: {
      type: 'object',
      properties: {
        connector_name: {
          type: 'string',
          description: 'Safe connector/tool name, for example clickhouse_cars_by_city_mileage.',
        },
        source_code: {
          type: 'string',
          description: 'Complete JavaScript module source. It must export default { name, description, inputSchema, handler }.',
        },
        overwrite: {
          type: 'boolean',
          description: 'Allow replacing an existing connector file.',
          default: false,
        },
      },
      required: ['connector_name', 'source_code'],
    },
  },
  {
    name: 'update_generated_connector',
    description: 'Update an existing generated JavaScript MCP connector file by name. Use this when the user explicitly asks to modify a saved connector.',
    inputSchema: {
      type: 'object',
      properties: {
        connector_name: {
          type: 'string',
          description: 'Existing connector/tool name without .js suffix.',
        },
        source_code: {
          type: 'string',
          description: 'Complete replacement JavaScript module source.',
        },
      },
      required: ['connector_name', 'source_code'],
    },
  },
  {
    name: 'run_generated_connector',
    description: 'Run a saved generated MCP connector by name and return live ClickHouse data. Use this when the user explicitly names a connector or after creating/updating one.',
    inputSchema: {
      type: 'object',
      properties: {
        connector_name: {
          type: 'string',
          description: 'Generated connector/tool name without .js suffix.',
        },
        arguments: {
          type: 'object',
          description: 'Arguments passed to the generated connector handler.',
          default: {},
        },
      },
      required: ['connector_name'],
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

function safeSqlIdentifier(value, fallback = '') {
  const text = String(value || fallback);
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(text)) {
    throw new Error(`Unsafe SQL identifier: ${text}`);
  }
  return text;
}

function safeLabelName(value, fallback = 'job') {
  const text = String(value || fallback);
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(text)) {
    throw new Error(`Unsafe Prometheus label name: ${text}`);
  }
  return text;
}

function safeDashboardTitle(value, fallback) {
  const text = String(value || fallback).trim();
  return text.replace(/[^\w\s:().,\-/]/g, '').slice(0, 90) || fallback;
}

function quoteIdent(value) {
  return `\`${String(value).replaceAll('`', '``')}\``;
}

// Проверяет имя generated-коннектора до записи файла и до динамического import.
// Это защищает путь сохранения и одновременно задает понятный namespace для model-created tools.
function safeGeneratedConnectorName(value) {
  const text = String(value || '').trim();
  if (!/^[a-z][a-z0-9_]{2,80}$/.test(text)) {
    throw new Error(`Unsafe generated connector name: ${text}`);
  }
  if (!text.startsWith('clickhouse_')) {
    throw new Error('Generated connector names must start with clickhouse_.');
  }
  return text;
}

// Нормализует имя таблицы для старых и новых схем tool-call.
// LibreChat иногда присылает table_name вместо table; здесь мы принимаем оба варианта,
// чтобы запрос дошел до ClickHouse, а не падал на уровне JSON schema.
function analyticsTableArgument(args) {
  const table = args?.table ?? args?.table_name;
  if (!table) {
    throw new Error('Table name is required. Use argument "table" or "table_name".');
  }
  return table;
}

// Возвращает абсолютный путь к JS-файлу generated-коннектора внутри отдельной папки этого коннектора.
function generatedConnectorPath(connectorName) {
  const safeName = safeGeneratedConnectorName(connectorName);
  return path.join(generatedConnectorsDir, safeName, 'connector.js');
}

// Возвращает человекочитаемый путь, который модель должна показывать пользователю в чате.
function publicGeneratedConnectorPath(connectorName) {
  return `${generatedConnectorsPublicDir}/${safeGeneratedConnectorName(connectorName)}/connector.js`;
}

// Валидирует JS-код, который модель пытается сохранить как MCP-коннектор.
// Здесь мы не выполняем код, а отсекаем опасные/бесполезные варианты до записи на диск.
function validateGeneratedConnectorSource(connectorName, sourceCode) {
  const safeName = safeGeneratedConnectorName(connectorName);
  const source = String(sourceCode || '').trim();
  if (!source) {
    throw new Error('Generated connector source_code is required.');
  }
  if (source.length > 30000) {
    throw new Error('Generated connector source_code is too large.');
  }
  if (!source.includes('export default')) {
    throw new Error('Generated connector must use export default.');
  }
  const expectedNamePattern = new RegExp(`["']?name["']?\\s*:\\s*["']${safeName}["']`);
  if (!expectedNamePattern.test(source)) {
    throw new Error(`Generated connector source must export name ${safeName}.`);
  }
  if (source.includes('analytics.') && !source.includes('analyticsColumnExists') && !source.includes('analyticsColumns')) {
    throw new Error('Generated connectors that query analytics.* must validate referenced columns with helpers.analyticsColumnExists or helpers.analyticsColumns before runQuery.');
  }
  const forbiddenPatterns = [
    /\bimport\s+/,
    /\brequire\s*\(/,
    /\bprocess\b/,
    /\bchild_process\b/,
    /\bnode:/,
    /\bfs\b/,
    /\beval\s*\(/,
    /\bFunction\s*\(/,
    /\bfetch\s*\(/,
    /\bXMLHttpRequest\b/,
  ];
  for (const pattern of forbiddenPatterns) {
    if (pattern.test(source)) {
      throw new Error(`Generated connector source contains forbidden pattern: ${pattern}`);
    }
  }
  return `${source}\n`;
}

// Проверяет runtime-контракт уже импортированного generated-коннектора.
// MCP server регистрирует только объекты с name/description/inputSchema/handler.
function validateGeneratedConnectorModule(connector, expectedName = '') {
  if (!connector || typeof connector !== 'object') {
    throw new Error('Generated connector must export an object.');
  }
  const connectorName = safeGeneratedConnectorName(connector.name);
  if (expectedName && connectorName !== safeGeneratedConnectorName(expectedName)) {
    throw new Error(`Generated connector name mismatch: expected ${expectedName}, got ${connectorName}.`);
  }
  if (typeof connector.description !== 'string' || connector.description.trim().length < 10) {
    throw new Error(`Generated connector ${connectorName} must have a useful description.`);
  }
  if (!connector.inputSchema || connector.inputSchema.type !== 'object') {
    throw new Error(`Generated connector ${connectorName} must expose an object inputSchema.`);
  }
  if (typeof connector.handler !== 'function') {
    throw new Error(`Generated connector ${connectorName} must expose an async handler function.`);
  }
  return connectorName;
}

// Создает директорию для generated-коннекторов, если она еще не была создана на хосте или внутри контейнера.
async function ensureGeneratedConnectorsDir() {
  await mkdir(generatedConnectorsDir, { recursive: true });
}

async function ensureGeneratedConnectorDir(connectorName) {
  await mkdir(path.dirname(generatedConnectorPath(connectorName)), { recursive: true });
}

// Загружает JS-коннектор из файла и кладет его в кэш. force=true нужен после create/update,
// чтобы модель сразу могла вызвать новую версию без перезапуска контейнера.
async function loadGeneratedConnector(connectorName, { force = false } = {}) {
  const safeName = safeGeneratedConnectorName(connectorName);
  await ensureGeneratedConnectorsDir();
  const filePath = generatedConnectorPath(safeName);
  if (!force && generatedConnectorCache.has(safeName)) {
    return generatedConnectorCache.get(safeName);
  }
  const moduleUrl = `${pathToFileURL(filePath).href}?v=${Date.now()}`;
  const module = await import(moduleUrl);
  const connector = module.default;
  validateGeneratedConnectorModule(connector, safeName);
  const loaded = {
    ...connector,
    filePath,
    publicPath: publicGeneratedConnectorPath(safeName),
  };
  generatedConnectorCache.set(safeName, loaded);
  return loaded;
}

// Сканирует внешнюю папку generated-коннекторов и возвращает имена подпапок, где есть connector.js.
async function listGeneratedConnectorFiles() {
  await ensureGeneratedConnectorsDir();
  const entries = await readdir(generatedConnectorsDir, { withFileTypes: true });
  const connectorNames = [];
  for (const entry of entries) {
    if (!entry.isDirectory()) {
      continue;
    }
    const name = entry.name;
    try {
      safeGeneratedConnectorName(name);
      await readFile(path.join(generatedConnectorsDir, name, 'connector.js'), 'utf8');
      connectorNames.push(name);
    } catch {
      // Папки без валидного connector.js не публикуются как MCP tools.
    }
  }
  return connectorNames
    .filter(name => {
      try {
        safeGeneratedConnectorName(name);
        return true;
      } catch {
        return false;
      }
    })
    .sort();
}

// Преобразует сохраненные JS-коннекторы в MCP tool definitions, которые LibreChat видит в tools/list.
async function loadGeneratedConnectorTools() {
  const connectorNames = await listGeneratedConnectorFiles();
  const loaded = [];
  for (const connectorName of connectorNames) {
    try {
      const connector = await loadGeneratedConnector(connectorName);
      loaded.push({
        name: connector.name,
        description: `${connector.description} Generated connector saved at ${connector.publicPath}.`,
        inputSchema: connector.inputSchema,
      });
    } catch (error) {
      console.error(`Failed to load generated connector ${connectorName}: ${error.message}`);
    }
  }
  return loaded;
}

// Единая точка запуска generated-коннектора.
// В handler передаются read-only ClickHouse-клиент, runQuery и helpers для проверки схемы/колонок.
async function callGeneratedConnector(connectorName, args) {
  const connector = await loadGeneratedConnector(connectorName, { force: true });
  const result = await connector.handler({
    args: args || {},
    clickhouse,
    runQuery,
    helpers: {
      analyticsColumns,
      analyticsColumnExists,
      analyticsTableExists,
      boundedLimit,
      normalizeFilterOperator,
      quoteIdent,
      quoteString,
      safeSqlIdentifier,
      sqlLiteral,
    },
  });
  return {
    connector_name: connector.name,
    saved_path: connector.publicPath,
    result,
  };
}

function quoteString(value) {
  return `'${String(value).replaceAll('\\', '\\\\').replaceAll("'", "\\'")}'`;
}

function sqlLiteral(value) {
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new Error('Only finite numeric filter values are allowed.');
    }
    return String(value);
  }
  if (typeof value === 'boolean') {
    return value ? '1' : '0';
  }
  return quoteString(value);
}

function normalizeFilterOperator(operator) {
  const normalized = String(operator || '').trim().toLowerCase();
  const operators = {
    '=': '=',
    eq: '=',
    '!=': '!=',
    ne: '!=',
    '>': '>',
    gt: '>',
    '>=': '>=',
    gte: '>=',
    '<': '<',
    lt: '<',
    '<=': '<=',
    lte: '<=',
  };
  const sqlOperator = operators[normalized];
  if (!sqlOperator) {
    throw new Error(`Unsupported filter operator: ${operator}`);
  }
  return sqlOperator;
}

function optionalStringFilter(column, value) {
  const text = String(value || '').trim();
  if (!text) {
    return '';
  }
  return `${quoteIdent(column)} = ${quoteString(text)}`;
}

function optionalNumericFilter(column, operator, value) {
  if (value === undefined || value === null || value === '') {
    return '';
  }
  return `${quoteIdent(column)} ${operator} ${sqlLiteral(Number(value))}`;
}

async function analyticsTableExists(table) {
  const tableName = safeSqlIdentifier(table);
  const rows = await runQuery(`
    SELECT name
    FROM system.tables
    WHERE database = 'analytics'
      AND name = ${quoteString(tableName)}
    LIMIT 1
  `);
  if (rows.length === 0) {
    throw new Error(`Unknown analytics table or view: ${tableName}`);
  }
  return tableName;
}

async function analyticsColumns(table) {
  const tableName = await analyticsTableExists(table);
  const rows = await runQuery(`
    SELECT
      name,
      type,
      position
    FROM system.columns
    WHERE database = 'analytics'
      AND table = ${quoteString(tableName)}
    ORDER BY position
  `);
  return { tableName, columns: rows };
}

async function analyticsColumnExists(table, column) {
  const columnName = safeSqlIdentifier(column);
  const { tableName, columns } = await analyticsColumns(table);
  if (!columns.some(row => row.name === columnName)) {
    throw new Error(`Unknown column ${columnName} in analytics.${tableName}`);
  }
  return { tableName, columnName, columns };
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
  return createGrafanaShortUrlForPath(grafanaRelativePanelPath(panelId, from, to));
}

async function createGrafanaShortUrlForPath(path) {
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

async function createGrafanaDashboard(dashboard) {
  const auth = Buffer.from(`${grafanaUser}:${grafanaPassword}`).toString('base64');
  const response = await fetch(`${grafanaApiUrl}/api/dashboards/db`, {
    method: 'POST',
    headers: {
      Authorization: `Basic ${auth}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      dashboard,
      folderId: 0,
      overwrite: true,
      message: 'Created by Agentic Data Stack MCP',
    }),
  });

  if (!response.ok) {
    const message = await response.text();
    throw new Error(`Grafana dashboard create failed: ${response.status} ${message}`);
  }

  return response.json();
}

function formatToolRows(rows, limit = 20) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return 'No rows returned.';
  }

  const limitedRows = rows.slice(0, limit);
  const columns = [...new Set(limitedRows.flatMap((row) => Object.keys(row)))];
  const header = `| ${columns.join(' | ')} |`;
  const separator = `| ${columns.map(() => '---').join(' | ')} |`;
  const body = limitedRows.map((row) => {
    const cells = columns.map((column) => {
      const value = row[column];
      if (value === null || value === undefined || value === '') {
        return '';
      }
      return String(value).replaceAll('|', '\\|').replace(/\s+/g, ' ').slice(0, 120);
    });
    return `| ${cells.join(' | ')} |`;
  });
  const suffix = rows.length > limit ? `\n\n... ${rows.length - limit} more rows omitted.` : '';
  return [header, separator, ...body].join('\n') + suffix;
}

async function grafanaDashboardResponse(id, rows, metadata) {
  const grafanaUrl = `${grafanaBaseUrl}${metadata.dashboardPath}`;
  const {
    dashboardUid,
    dashboardPath,
    ...publicMetadata
  } = metadata;
  const safeMetadata = Object.fromEntries(
    Object.entries(publicMetadata).filter(([key]) => key !== 'sql' && !key.endsWith('Sql')),
  );
  let grafanaShortUrl = '';
  try {
    grafanaShortUrl = await createGrafanaShortUrlForPath(dashboardPath.replace(/^\//, ''));
  } catch (error) {
    console.error(error.message);
  }

  return jsonRpc(id, {
    content: [
      {
        type: 'text',
        text: [
          `Dashboard URL for the final answer: ${grafanaUrl}`,
          grafanaShortUrl ? `Secondary short URL: ${grafanaShortUrl}` : 'Secondary short URL: not available',
          `Final answer instruction: return only the dashboard URL above and a short summary from the rows below. Do not paste raw tool output, JSON, SQL, metadata, UID, path, or debug text. Do not rewrite the URL host or port.`,
          '',
          `Dashboard title: ${safeMetadata.title || 'Grafana dashboard'}`,
          safeMetadata.note ? `Note: ${safeMetadata.note}` : '',
          '',
          'Rows for summary:',
          formatToolRows(rows),
        ].join('\n'),
      },
    ],
  });
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

function clickhouseGrafanaTarget(rawSql, refId = 'A', format = 1) {
  return {
    datasource: {
      type: 'grafana-clickhouse-datasource',
      uid: 'clickhouse-analytics',
    },
    format,
    rawSql,
    refId,
  };
}

// Преобразует время из ClickHouse в формат, который Grafana принимает в dashboard.time.
function grafanaIsoTime(value) {
  const text = String(value || '').trim();
  if (!text) {
    return 'now';
  }
  return `${text.replace(' ', 'T')}Z`;
}

// Строит временное окно Prometheus не от системного now(), а от max(sample_time) в данных.
// Это важно для синтетического demo-набора: данные могут быть загружены в прошлом, но dashboards все равно должны быть непустыми.
async function prometheusTimeWindow(hours, metricName = '') {
  const metricFilter = metricName ? `WHERE metric_name = ${quoteString(metricName)}` : '';
  const rows = await runQuery(`
    SELECT
      max(sample_time) - INTERVAL ${hours} HOUR AS from_time,
      max(sample_time) AS to_time
    FROM analytics.prometheus_samples
    ${metricFilter}
  `);
  const row = rows[0] || {};
  if (!row.from_time || !row.to_time) {
    const suffix = metricName ? ` for metric ${metricName}` : '';
    throw new Error(`No Prometheus samples found${suffix}.`);
  }
  return {
    fromTime: row.from_time,
    toTime: row.to_time,
    grafanaFrom: grafanaIsoTime(row.from_time),
    grafanaTo: grafanaIsoTime(row.to_time),
    sqlFilter: `sample_time >= parseDateTime64BestEffort(${quoteString(row.from_time)}) AND sample_time <= parseDateTime64BestEffort(${quoteString(row.to_time)})`,
  };
}

// Создает большой Grafana dashboard по доступности сервисов/БД из Prometheus samples в ClickHouse.
// На выход MCP отдает URL dashboard и компактные строки, по которым модель формирует ответ пользователю.
async function createPrometheusAvailabilityDashboardResponse(id, args = {}) {
  const hours = boundedLimit(args.hours, 24, 24 * 30);
  const bucketMinutes = boundedLimit(args.bucket_minutes, 1, 60);
  const title = safeDashboardTitle(args.title, 'Prometheus Availability Overview');
  const uid = `prom-avail-${randomUUID().replaceAll('-', '').slice(0, 14)}`;
  const datasource = {
    type: 'grafana-clickhouse-datasource',
    uid: 'clickhouse-analytics',
  };
  const timeWindow = await prometheusTimeWindow(hours);
  const timeFilter = timeWindow.sqlFilter;
  const bucket = `toStartOfInterval(sample_time, INTERVAL ${bucketMinutes} MINUTE)`;
  const serviceLabel = "JSONExtractString(labels_json, 'service')";
  const instanceLabel = "JSONExtractString(labels_json, 'instance')";
  const severityLabel = "JSONExtractString(labels_json, 'severity')";
  const incidentLabel = "JSONExtractString(labels_json, 'incident')";
  const realTargetFilter = `${instanceLabel} != 'synthetic-exporter:9201'`;

  const targetCountSql = `
    SELECT uniqExact(${instanceLabel}) AS targets
    FROM analytics.prometheus_samples
    WHERE metric_name = 'synthetic_service_up'
      AND ${timeFilter}
      AND ${realTargetFilter}
  `.trim();
  const downNowSql = `
    SELECT countIf(last_value = 0) AS down_targets
    FROM (
      SELECT
        ${instanceLabel} AS instance,
        argMax(value, sample_time) AS last_value
      FROM analytics.prometheus_samples
      WHERE metric_name = 'synthetic_service_up'
        AND ${timeFilter}
        AND ${realTargetFilter}
      GROUP BY instance
    )
  `.trim();
  const activeIncidentsSql = `
    SELECT countIf(last_value = 1) AS active_incidents
    FROM (
      SELECT
        ${incidentLabel} AS incident,
        ${serviceLabel} AS service,
        argMax(value, sample_time) AS last_value
      FROM analytics.prometheus_samples
      WHERE metric_name = 'synthetic_incident_active'
        AND ${timeFilter}
      GROUP BY incident, service
    )
  `.trim();
  const exporterUpSql = `
    SELECT min(value) AS scrape_up_min
    FROM analytics.prometheus_samples
    WHERE metric_name = 'up'
      AND ${timeFilter}
  `.trim();
  const availabilityTimelineSql = `
    SELECT
      ${bucket} AS time,
      concat(if(${serviceLabel} = '', 'unknown-service', ${serviceLabel}), ' / ', if(${instanceLabel} = '', 'unknown-instance', ${instanceLabel})) AS series,
      min(value) AS value
    FROM analytics.prometheus_samples
    WHERE metric_name = 'synthetic_service_up'
      AND ${timeFilter}
      AND ${realTargetFilter}
    GROUP BY time, series
    ORDER BY time ASC, series ASC
  `.trim();
  const downWindowsSql = `
    SELECT
      if(${serviceLabel} = '', 'unknown-service', ${serviceLabel}) AS service,
      if(${instanceLabel} = '', 'unknown-instance', ${instanceLabel}) AS instance,
      min(sample_time) AS first_seen_down,
      max(sample_time) AS last_seen_down,
      count() AS down_samples
    FROM analytics.prometheus_samples
    WHERE metric_name = 'synthetic_service_up'
      AND value = 0
      AND ${timeFilter}
      AND ${realTargetFilter}
    GROUP BY service, instance
    ORDER BY last_seen_down DESC, down_samples DESC
    LIMIT 100
  `.trim();
  const uptimeSql = `
    SELECT
      if(${serviceLabel} = '', 'unknown-service', ${serviceLabel}) AS service,
      if(${instanceLabel} = '', 'unknown-instance', ${instanceLabel}) AS instance,
      round(100 * avg(value), 2) AS uptime_percent,
      countIf(value = 0) AS down_samples,
      count() AS samples
    FROM analytics.prometheus_samples
    WHERE metric_name = 'synthetic_service_up'
      AND ${timeFilter}
      AND ${realTargetFilter}
    GROUP BY service, instance
    ORDER BY uptime_percent ASC, down_samples DESC, service ASC
    LIMIT 100
  `.trim();
  const incidentTimelineSql = `
    SELECT
      ${bucket} AS time,
      concat(if(${severityLabel} = '', 'unknown', ${severityLabel}), ': ', if(${incidentLabel} = '', 'incident', ${incidentLabel})) AS series,
      max(value) AS value
    FROM analytics.prometheus_samples
    WHERE metric_name = 'synthetic_incident_active'
      AND ${timeFilter}
    GROUP BY time, series
    ORDER BY time ASC, series ASC
  `.trim();
  const httpLatencySql = `
    SELECT
      ${bucket} AS time,
      if(${serviceLabel} = '', 'unknown-service', ${serviceLabel}) AS series,
      quantile(0.95)(value) AS value
    FROM analytics.prometheus_samples
    WHERE metric_name = 'synthetic_http_request_duration_seconds_p95'
      AND ${timeFilter}
    GROUP BY time, series
    ORDER BY time ASC, series ASC
  `.trim();
  const httpTrafficSql = `
    SELECT
      toStartOfInterval(sample_time, INTERVAL 5 MINUTE) AS time,
      concat(if(${serviceLabel} = '', 'unknown-service', ${serviceLabel}), ' ', if(JSONExtractString(labels_json, 'status_class') = '', 'status', JSONExtractString(labels_json, 'status_class'))) AS series,
      greatest(max(value) - min(value), 0) AS value
    FROM analytics.prometheus_samples
    WHERE metric_name = 'synthetic_http_requests_total'
      AND ${timeFilter}
    GROUP BY time, series
    ORDER BY time ASC, series ASC
  `.trim();
  const dbDiskSql = `
    SELECT
      ${bucket} AS time,
      if(${serviceLabel} = '', 'unknown-db', ${serviceLabel}) AS series,
      max(value) AS value
    FROM analytics.prometheus_samples
    WHERE metric_name = 'synthetic_db_disk_usage_ratio'
      AND ${timeFilter}
    GROUP BY time, series
    ORDER BY time ASC, series ASC
  `.trim();
  const dbLagSql = `
    SELECT
      ${bucket} AS time,
      if(${serviceLabel} = '', 'unknown-db', ${serviceLabel}) AS series,
      max(value) AS value
    FROM analytics.prometheus_samples
    WHERE metric_name = 'synthetic_db_replication_lag_seconds'
      AND ${timeFilter}
    GROUP BY time, series
    ORDER BY time ASC, series ASC
  `.trim();
  const dbQuerySql = `
    SELECT
      ${bucket} AS time,
      if(${serviceLabel} = '', 'unknown-db', ${serviceLabel}) AS series,
      quantile(0.95)(value) AS value
    FROM analytics.prometheus_samples
    WHERE metric_name = 'synthetic_db_query_duration_seconds_p95'
      AND ${timeFilter}
    GROUP BY time, series
    ORDER BY time ASC, series ASC
  `.trim();

  const previewRows = await runQuery(`
    SELECT
      if(${serviceLabel} = '', 'unknown-service', ${serviceLabel}) AS service,
      if(${instanceLabel} = '', 'unknown-instance', ${instanceLabel}) AS instance,
      round(100 * avg(value), 2) AS uptime_percent,
      countIf(value = 0) AS down_samples,
      if(countIf(value = 0) = 0, '', toString(minIf(sample_time, value = 0))) AS first_seen_down,
      if(countIf(value = 0) = 0, '', toString(maxIf(sample_time, value = 0))) AS last_seen_down,
      argMax(value, sample_time) AS current_value
    FROM analytics.prometheus_samples
    WHERE metric_name = 'synthetic_service_up'
      AND ${timeFilter}
      AND ${realTargetFilter}
    GROUP BY service, instance
    ORDER BY current_value ASC, uptime_percent ASC, down_samples DESC
    LIMIT 50
  `);

  const stateMappings = [
    {
      type: 'value',
      options: {
        0: { text: 'DOWN', color: 'red' },
        1: { text: 'UP', color: 'green' },
      },
    },
  ];
  const redGreenThresholds = {
    mode: 'absolute',
    steps: [
      { color: 'red', value: null },
      { color: 'green', value: 1 },
    ],
  };
  const incidentMappings = [
    {
      type: 'value',
      options: {
        0: { text: 'OK', color: 'green' },
        1: { text: 'ACTIVE', color: 'red' },
      },
    },
  ];
  const incidentThresholds = {
    mode: 'absolute',
    steps: [
      { color: 'green', value: null },
      { color: 'red', value: 1 },
    ],
  };
  const dashboard = {
    id: null,
    uid,
    title,
    description: 'Synthetic Prometheus operational dashboard generated from ClickHouse metrics. Raw metric up shows exporter scrape health; synthetic_service_up shows monitored service and database availability.',
    tags: ['agentic-data-stack', 'prometheus', 'availability', 'clickhouse'],
    timezone: 'browser',
    schemaVersion: 39,
    version: 0,
    refresh: '30s',
    time: {
      from: timeWindow.grafanaFrom,
      to: timeWindow.grafanaTo,
    },
    panels: [
      {
        id: 1,
        title: 'Monitored targets',
        type: 'stat',
        datasource,
        gridPos: { h: 4, w: 6, x: 0, y: 0 },
        fieldConfig: { defaults: { color: { mode: 'thresholds' }, thresholds: redGreenThresholds }, overrides: [] },
        options: { colorMode: 'background', graphMode: 'none', justifyMode: 'center', reduceOptions: { calcs: ['lastNotNull'], fields: '', values: false }, textMode: 'auto' },
        targets: [clickhouseGrafanaTarget(targetCountSql, 'A', 1)],
      },
      {
        id: 2,
        title: 'Down now',
        type: 'stat',
        datasource,
        gridPos: { h: 4, w: 6, x: 6, y: 0 },
        fieldConfig: {
          defaults: {
            color: { mode: 'thresholds' },
            thresholds: { mode: 'absolute', steps: [{ color: 'green', value: null }, { color: 'red', value: 1 }] },
          },
          overrides: [],
        },
        options: { colorMode: 'background', graphMode: 'none', justifyMode: 'center', reduceOptions: { calcs: ['lastNotNull'], fields: '', values: false }, textMode: 'auto' },
        targets: [clickhouseGrafanaTarget(downNowSql, 'A', 1)],
      },
      {
        id: 3,
        title: 'Active incidents',
        type: 'stat',
        datasource,
        gridPos: { h: 4, w: 6, x: 12, y: 0 },
        fieldConfig: {
          defaults: {
            color: { mode: 'thresholds' },
            thresholds: { mode: 'absolute', steps: [{ color: 'green', value: null }, { color: 'orange', value: 1 }, { color: 'red', value: 3 }] },
          },
          overrides: [],
        },
        options: { colorMode: 'background', graphMode: 'none', justifyMode: 'center', reduceOptions: { calcs: ['lastNotNull'], fields: '', values: false }, textMode: 'auto' },
        targets: [clickhouseGrafanaTarget(activeIncidentsSql, 'A', 1)],
      },
      {
        id: 4,
        title: 'Prometheus scrape health',
        type: 'stat',
        datasource,
        gridPos: { h: 4, w: 6, x: 18, y: 0 },
        fieldConfig: { defaults: { color: { mode: 'thresholds' }, mappings: stateMappings, thresholds: redGreenThresholds }, overrides: [] },
        options: { colorMode: 'background', graphMode: 'none', justifyMode: 'center', reduceOptions: { calcs: ['lastNotNull'], fields: '', values: false }, textMode: 'auto' },
        targets: [clickhouseGrafanaTarget(exporterUpSql, 'A', 1)],
      },
      {
        id: 5,
        title: 'Service availability timeline',
        type: 'state-timeline',
        datasource,
        gridPos: { h: 9, w: 24, x: 0, y: 4 },
        fieldConfig: { defaults: { color: { mode: 'thresholds' }, mappings: stateMappings, thresholds: redGreenThresholds }, overrides: [] },
        options: { alignValue: 'center', legend: { displayMode: 'list', placement: 'bottom', showLegend: true }, mergeValues: true, showValue: 'auto', tooltip: { mode: 'multi', sort: 'none' } },
        transformations: [
          {
            id: 'renameByRegex',
            options: { regex: '^value (.*)$', renamePattern: '$1' },
          },
        ],
        targets: [clickhouseGrafanaTarget(availabilityTimelineSql, 'A', 0)],
      },
      {
        id: 6,
        title: 'Down windows',
        type: 'table',
        datasource,
        gridPos: { h: 8, w: 12, x: 0, y: 13 },
        targets: [clickhouseGrafanaTarget(downWindowsSql, 'A', 1)],
      },
      {
        id: 7,
        title: 'Uptime by service',
        type: 'table',
        datasource,
        gridPos: { h: 8, w: 12, x: 12, y: 13 },
        fieldConfig: {
          defaults: { custom: { align: 'auto', cellOptions: { type: 'auto' } } },
          overrides: [
            {
              matcher: { id: 'byName', options: 'uptime_percent' },
              properties: [
                { id: 'unit', value: 'percent' },
                { id: 'custom.cellOptions', value: { type: 'color-background' } },
                { id: 'thresholds', value: { mode: 'absolute', steps: [{ color: 'red', value: null }, { color: 'orange', value: 95 }, { color: 'green', value: 99 }] } },
              ],
            },
          ],
        },
        targets: [clickhouseGrafanaTarget(uptimeSql, 'A', 1)],
      },
      {
        id: 8,
        title: 'Incident timeline',
        type: 'state-timeline',
        datasource,
        gridPos: { h: 7, w: 24, x: 0, y: 21 },
        fieldConfig: { defaults: { color: { mode: 'thresholds' }, mappings: incidentMappings, thresholds: incidentThresholds }, overrides: [] },
        options: { alignValue: 'center', legend: { displayMode: 'list', placement: 'bottom', showLegend: true }, mergeValues: true, showValue: 'auto', tooltip: { mode: 'multi', sort: 'none' } },
        transformations: [
          {
            id: 'renameByRegex',
            options: { regex: '^value (.*)$', renamePattern: '$1' },
          },
        ],
        targets: [clickhouseGrafanaTarget(incidentTimelineSql, 'A', 0)],
      },
      {
        id: 9,
        title: 'HTTP p95 latency by service',
        type: 'timeseries',
        datasource,
        gridPos: { h: 8, w: 12, x: 0, y: 28 },
        fieldConfig: { defaults: { color: { mode: 'palette-classic' }, unit: 's', custom: { drawStyle: 'line', fillOpacity: 10, lineWidth: 2, showPoints: 'never', spanNulls: false } }, overrides: [] },
        options: { legend: { displayMode: 'list', placement: 'bottom', showLegend: true }, tooltip: { mode: 'multi', sort: 'desc' } },
        transformations: [
          {
            id: 'renameByRegex',
            options: { regex: '^value (.*)$', renamePattern: '$1' },
          },
        ],
        targets: [clickhouseGrafanaTarget(httpLatencySql, 'A', 0)],
      },
      {
        id: 10,
        title: 'HTTP requests per 5 min by status class',
        type: 'timeseries',
        datasource,
        gridPos: { h: 8, w: 12, x: 12, y: 28 },
        fieldConfig: { defaults: { color: { mode: 'palette-classic' }, unit: 'short', custom: { drawStyle: 'bars', fillOpacity: 35, lineWidth: 1, showPoints: 'never', stacking: { mode: 'normal', group: 'A' } } }, overrides: [] },
        options: { legend: { displayMode: 'list', placement: 'bottom', showLegend: true }, tooltip: { mode: 'multi', sort: 'desc' } },
        transformations: [
          {
            id: 'renameByRegex',
            options: { regex: '^value (.*)$', renamePattern: '$1' },
          },
        ],
        targets: [clickhouseGrafanaTarget(httpTrafficSql, 'A', 0)],
      },
      {
        id: 11,
        title: 'DB disk usage ratio',
        type: 'timeseries',
        datasource,
        gridPos: { h: 8, w: 8, x: 0, y: 36 },
        fieldConfig: { defaults: { color: { mode: 'palette-classic' }, unit: 'percentunit', max: 1, min: 0, custom: { drawStyle: 'line', fillOpacity: 15, lineWidth: 2, showPoints: 'never' } }, overrides: [] },
        options: { legend: { displayMode: 'list', placement: 'bottom', showLegend: true }, tooltip: { mode: 'multi', sort: 'desc' } },
        transformations: [
          {
            id: 'renameByRegex',
            options: { regex: '^value (.*)$', renamePattern: '$1' },
          },
        ],
        targets: [clickhouseGrafanaTarget(dbDiskSql, 'A', 0)],
      },
      {
        id: 12,
        title: 'DB replication lag',
        type: 'timeseries',
        datasource,
        gridPos: { h: 8, w: 8, x: 8, y: 36 },
        fieldConfig: { defaults: { color: { mode: 'palette-classic' }, unit: 's', custom: { drawStyle: 'line', fillOpacity: 15, lineWidth: 2, showPoints: 'never' } }, overrides: [] },
        options: { legend: { displayMode: 'list', placement: 'bottom', showLegend: true }, tooltip: { mode: 'multi', sort: 'desc' } },
        transformations: [
          {
            id: 'renameByRegex',
            options: { regex: '^value (.*)$', renamePattern: '$1' },
          },
        ],
        targets: [clickhouseGrafanaTarget(dbLagSql, 'A', 0)],
      },
      {
        id: 13,
        title: 'DB query p95 latency',
        type: 'timeseries',
        datasource,
        gridPos: { h: 8, w: 8, x: 16, y: 36 },
        fieldConfig: { defaults: { color: { mode: 'palette-classic' }, unit: 's', custom: { drawStyle: 'line', fillOpacity: 15, lineWidth: 2, showPoints: 'never' } }, overrides: [] },
        options: { legend: { displayMode: 'list', placement: 'bottom', showLegend: true }, tooltip: { mode: 'multi', sort: 'desc' } },
        transformations: [
          {
            id: 'renameByRegex',
            options: { regex: '^value (.*)$', renamePattern: '$1' },
          },
        ],
        targets: [clickhouseGrafanaTarget(dbQuerySql, 'A', 0)],
      },
    ],
  };
  const created = await createGrafanaDashboard(dashboard);
  return grafanaDashboardResponse(id, previewRows, {
    title,
    dashboardUid: created.uid || uid,
    dashboardPath: created.url || `/d/${uid}/${uid}`,
    purpose: 'prometheus_availability_overview',
    primaryMetric: 'synthetic_service_up',
    note: 'Use synthetic_service_up for monitored service/database availability. Raw Prometheus up is only scrape health for the exporter target.',
    hours,
    fromTime: timeWindow.fromTime,
    toTime: timeWindow.toTime,
    bucketMinutes,
    datasourceUid: 'clickhouse-analytics',
    sql: {
      targetCountSql,
      downNowSql,
      activeIncidentsSql,
      exporterUpSql,
      availabilityTimelineSql,
      downWindowsSql,
      uptimeSql,
      incidentTimelineSql,
      httpLatencySql,
      httpTrafficSql,
      dbDiskSql,
      dbLagSql,
      dbQuerySql,
    },
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
    // LibreChat запрашивает tools/list перед выбором инструмента.
    // Здесь мы объединяем статические tools и JS-коннекторы, созданные моделью ранее.
    const generatedTools = await loadGeneratedConnectorTools();
    return jsonRpc(id, { tools: [...baseTools, ...generatedTools] });
  }

  if (method === 'tools/call') {
    const name = params?.name;
    const args = params?.arguments || {};

    if (name === 'list_generated_connectors') {
      // Показывает пользователю и модели, какие generated-коннекторы уже сохранены в репозитории.
      const connectorNames = await listGeneratedConnectorFiles();
      const rows = [];
      for (const connectorName of connectorNames) {
        try {
          const connector = await loadGeneratedConnector(connectorName, { force: true });
          rows.push({
            name: connector.name,
            description: connector.description,
            saved_path: connector.publicPath,
          });
        } catch (error) {
          rows.push({
            name: connectorName,
            description: '',
            saved_path: publicGeneratedConnectorPath(connectorName),
            error: error.message,
          });
        }
      }
      return jsonRpc(id, {
        content: [{ type: 'text', text: JSON.stringify(rows, null, 2) }],
      });
    }

    if (name === 'describe_generated_connector') {
      // Возвращает контракт конкретного generated-коннектора без раскрытия исходного кода.
      const connector = await loadGeneratedConnector(args.connector_name, { force: true });
      return jsonRpc(id, {
        content: [{
          type: 'text',
          text: JSON.stringify({
            name: connector.name,
            description: connector.description,
            inputSchema: connector.inputSchema,
            saved_path: connector.publicPath,
          }, null, 2),
        }],
      });
    }

    if (name === 'create_generated_connector' || name === 'update_generated_connector') {
      // Сохраняет JS-код, который модель написала под вопрос пользователя, затем сразу валидирует import/export.
      // После успешного ответа этот файл доступен как обычный MCP tool.
      const connectorName = safeGeneratedConnectorName(args.connector_name);
      const filePath = generatedConnectorPath(connectorName);
      const overwrite = name === 'update_generated_connector' || args.overwrite === true;
      const source = validateGeneratedConnectorSource(connectorName, args.source_code);
      await ensureGeneratedConnectorDir(connectorName);
      if (!overwrite) {
        try {
          await readFile(filePath, 'utf8');
          throw new Error(`Generated connector already exists: ${publicGeneratedConnectorPath(connectorName)}. Use update_generated_connector or overwrite=true.`);
        } catch (error) {
          if (error.code !== 'ENOENT') {
            throw error;
          }
        }
      }
      await writeFile(filePath, source, 'utf8');
      generatedConnectorCache.delete(connectorName);
      const connector = await loadGeneratedConnector(connectorName, { force: true });
      return jsonRpc(id, {
        content: [{
          type: 'text',
          text: JSON.stringify({
            connector_name: connector.name,
            saved_path: connector.publicPath,
            status: name === 'create_generated_connector' ? 'created' : 'updated',
            instruction: 'Now call run_generated_connector, or call the connector tool by its name, to return ClickHouse data to the user.',
          }, null, 2),
        }],
      });
    }

    if (name === 'run_generated_connector') {
      // Явный запуск saved-коннектора по имени. Данные идут из ClickHouse обратно в модель, а затем в чат.
      const payload = await callGeneratedConnector(args.connector_name, args.arguments || {});
      return jsonRpc(id, {
        content: [{ type: 'text', text: JSON.stringify(payload, null, 2) }],
      });
    }

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

    if (name === 'describe_analytics_table') {
      const { tableName, columns } = await analyticsColumns(analyticsTableArgument(args));
      const metadata = await runQuery(`
        SELECT
          database,
          name AS table,
          engine,
          total_rows AS rows,
          formatReadableSize(total_bytes) AS bytes
        FROM system.tables
        WHERE database = 'analytics'
          AND name = ${quoteString(tableName)}
        LIMIT 1
      `);
      return jsonRpc(id, {
        content: [{ type: 'text', text: JSON.stringify({ metadata: metadata[0] || null, columns }, null, 2) }],
      });
    }

    if (name === 'sample_analytics_table') {
      const tableName = await analyticsTableExists(analyticsTableArgument(args));
      const limit = boundedLimit(args.limit, 10, 100);
      const rows = await runQuery(`
        SELECT *
        FROM analytics.${quoteIdent(tableName)}
        LIMIT ${limit}
      `);
      return jsonRpc(id, {
        content: [{ type: 'text', text: JSON.stringify(rows, null, 2) }],
      });
    }

    if (name === 'profile_analytics_table') {
      const { tableName, columns } = await analyticsColumns(analyticsTableArgument(args));
      const sampleLimit = boundedLimit(args.sample_limit, 5, 50);
      const metadata = await runQuery(`
        SELECT
          database,
          name AS table,
          engine,
          total_rows AS rows,
          formatReadableSize(total_bytes) AS bytes
        FROM system.tables
        WHERE database = 'analytics'
          AND name = ${quoteString(tableName)}
        LIMIT 1
      `);
      const sampleRows = await runQuery(`
        SELECT *
        FROM analytics.${quoteIdent(tableName)}
        LIMIT ${sampleLimit}
      `);
      return jsonRpc(id, {
        content: [{ type: 'text', text: JSON.stringify({ metadata: metadata[0] || null, columns, sampleRows }, null, 2) }],
      });
    }

    if (name === 'distinct_analytics_values') {
      const { tableName, columnName } = await analyticsColumnExists(analyticsTableArgument(args), args.column);
      const limit = boundedLimit(args.limit, 100, 500);
      const rows = await runQuery(`
        SELECT
          ${quoteIdent(columnName)} AS value,
          count() AS rows
        FROM analytics.${quoteIdent(tableName)}
        GROUP BY value
        ORDER BY rows DESC, value ASC
        LIMIT ${limit}
      `);
      return jsonRpc(id, {
        content: [{ type: 'text', text: JSON.stringify(rows, null, 2) }],
      });
    }

    if (name === 'count_analytics_by') {
      const tableName = await analyticsTableExists(analyticsTableArgument(args));
      const dimensions = Array.isArray(args.dimensions) ? args.dimensions.slice(0, 3) : [];
      if (dimensions.length === 0) {
        throw new Error('count_analytics_by requires at least one dimension.');
      }
      const validatedDimensions = [];
      for (const dimension of dimensions) {
        const { columnName } = await analyticsColumnExists(tableName, dimension);
        validatedDimensions.push(columnName);
      }
      const filters = args.filters && typeof args.filters === 'object' ? args.filters : {};
      const whereParts = [];
      for (const [column, value] of Object.entries(filters)) {
        const { columnName } = await analyticsColumnExists(tableName, column);
        whereParts.push(`${quoteIdent(columnName)} = ${sqlLiteral(value)}`);
      }
      const filterConditions = Array.isArray(args.filter_conditions) ? args.filter_conditions : [];
      for (const condition of filterConditions) {
        const { columnName } = await analyticsColumnExists(tableName, condition?.column);
        const operator = normalizeFilterOperator(condition?.operator);
        whereParts.push(`${quoteIdent(columnName)} ${operator} ${sqlLiteral(condition?.value)}`);
      }
      const limit = boundedLimit(args.limit, 100, 500);
      const groupBy = validatedDimensions.map(quoteIdent).join(', ');
      const whereClause = whereParts.length > 0 ? `WHERE ${whereParts.join(' AND ')}` : '';
      const rows = await runQuery(`
        SELECT
          ${groupBy},
          count() AS rows
        FROM analytics.${quoteIdent(tableName)}
        ${whereClause}
        GROUP BY ${groupBy}
        ORDER BY rows DESC
        LIMIT ${limit}
      `);
      return jsonRpc(id, {
        content: [{ type: 'text', text: JSON.stringify(rows, null, 2) }],
      });
    }

    if (name === 'create_car_inventory_dashboard') {
      await analyticsTableExists('car_inventory_raw');
      const title = safeDashboardTitle(args.title, 'Car Inventory Dashboard');
      const uid = `cars-${randomUUID().replaceAll('-', '').slice(0, 18)}`;
      const filters = [
        optionalStringFilter('city', args.city),
        optionalStringFilter('brand', args.brand),
        optionalStringFilter('stock_status', args.stock_status),
        optionalNumericFilter('mileage_km', '>=', args.min_mileage_km),
        optionalNumericFilter('mileage_km', '<=', args.max_mileage_km),
      ].filter(Boolean);
      const whereClause = filters.length > 0 ? `WHERE ${filters.join(' AND ')}` : '';
      const carsByCityBrandSql = `
        SELECT
          city,
          brand,
          count() AS cars
        FROM analytics.car_inventory_raw
        ${whereClause}
        GROUP BY city, brand
        ORDER BY city ASC, cars DESC, brand ASC
      `.trim();
      const carsByWarehouseSql = `
        SELECT
          city,
          warehouse_name,
          brand,
          count() AS cars,
          countIf(stock_status = 'available') AS available_cars,
          countIf(stock_status = 'reserved') AS reserved_cars,
          countIf(stock_status = 'maintenance') AS maintenance_cars,
          round(avg(price_usd), 2) AS avg_price_usd,
          round(avg(mileage_km), 0) AS avg_mileage_km
        FROM analytics.car_inventory_raw
        ${whereClause}
        GROUP BY city, warehouse_name, brand
        ORDER BY city ASC, warehouse_name ASC, cars DESC, brand ASC
      `.trim();
      const stockStatusSql = `
        SELECT
          city,
          stock_status,
          count() AS cars
        FROM analytics.car_inventory_raw
        ${whereClause}
        GROUP BY city, stock_status
        ORDER BY city ASC, cars DESC, stock_status ASC
      `.trim();
      const priceMileageSql = `
        SELECT
          city,
          brand,
          round(avg(price_usd), 2) AS avg_price_usd,
          round(avg(mileage_km), 0) AS avg_mileage_km,
          count() AS cars
        FROM analytics.car_inventory_raw
        ${whereClause}
        GROUP BY city, brand
        ORDER BY city ASC, cars DESC, brand ASC
      `.trim();
      const previewRows = await runQuery(`${carsByCityBrandSql} LIMIT 50`);
      const dashboard = {
        id: null,
        uid,
        title,
        tags: ['agentic-data-stack', 'postgres', 'car-inventory', 'clickhouse'],
        timezone: 'browser',
        schemaVersion: 39,
        version: 0,
        refresh: '',
        time: {
          from: 'now-30d',
          to: 'now',
        },
        panels: [
          {
            id: 1,
            title: 'Cars by city and brand',
            type: 'barchart',
            datasource: {
              type: 'grafana-clickhouse-datasource',
              uid: 'clickhouse-analytics',
            },
            gridPos: { h: 10, w: 24, x: 0, y: 0 },
            fieldConfig: {
              defaults: {
                color: { mode: 'palette-classic' },
                custom: {
                  axisPlacement: 'auto',
                  fillOpacity: 70,
                  lineWidth: 1,
                },
              },
              overrides: [],
            },
            options: {
              legend: { displayMode: 'list', placement: 'bottom', showLegend: true },
              tooltip: { mode: 'multi', sort: 'none' },
              orientation: 'auto',
              stacking: 'none',
            },
            targets: [
              {
                datasource: {
                  type: 'grafana-clickhouse-datasource',
                  uid: 'clickhouse-analytics',
                },
                format: 1,
                rawSql: carsByCityBrandSql,
                refId: 'A',
              },
            ],
          },
          {
            id: 2,
            title: 'Cars by city and stock status',
            type: 'barchart',
            datasource: {
              type: 'grafana-clickhouse-datasource',
              uid: 'clickhouse-analytics',
            },
            gridPos: { h: 8, w: 12, x: 0, y: 10 },
            targets: [
              {
                datasource: {
                  type: 'grafana-clickhouse-datasource',
                  uid: 'clickhouse-analytics',
                },
                format: 1,
                rawSql: stockStatusSql,
                refId: 'A',
              },
            ],
          },
          {
            id: 3,
            title: 'Average price and mileage by city and brand',
            type: 'table',
            datasource: {
              type: 'grafana-clickhouse-datasource',
              uid: 'clickhouse-analytics',
            },
            gridPos: { h: 8, w: 12, x: 12, y: 10 },
            targets: [
              {
                datasource: {
                  type: 'grafana-clickhouse-datasource',
                  uid: 'clickhouse-analytics',
                },
                format: 1,
                rawSql: priceMileageSql,
                refId: 'A',
              },
            ],
          },
          {
            id: 4,
            title: 'Warehouse inventory detail',
            type: 'table',
            datasource: {
              type: 'grafana-clickhouse-datasource',
              uid: 'clickhouse-analytics',
            },
            gridPos: { h: 11, w: 24, x: 0, y: 18 },
            targets: [
              {
                datasource: {
                  type: 'grafana-clickhouse-datasource',
                  uid: 'clickhouse-analytics',
                },
                format: 1,
                rawSql: carsByWarehouseSql,
                refId: 'A',
              },
            ],
          },
        ],
      };
      const created = await createGrafanaDashboard(dashboard);
      return grafanaDashboardResponse(id, previewRows, {
        title,
        dashboardUid: created.uid || uid,
        dashboardPath: created.url || `/d/${uid}/${uid}`,
        sourceTable: 'analytics.car_inventory_raw',
        filters: {
          city: args.city || null,
          brand: args.brand || null,
          stockStatus: args.stock_status || null,
          minMileageKm: args.min_mileage_km ?? null,
          maxMileageKm: args.max_mileage_km ?? null,
        },
        datasourceUid: 'clickhouse-analytics',
        carsByCityBrandSql,
        carsByWarehouseSql,
        stockStatusSql,
        priceMileageSql,
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

    if (name === 'create_prometheus_availability_dashboard') {
      return createPrometheusAvailabilityDashboardResponse(id, args);
    }

    if (name === 'create_prometheus_metric_dashboard') {
      const metricName = safeIdentifier(args.metric_name);
      const groupByLabel = safeLabelName(args.group_by_label, 'job');
      const aggregation = safeChoice(args.aggregation, ['avg', 'min', 'max', 'p95', 'sum', 'count', 'last'], 'avg');
      const hours = boundedLimit(args.hours, 24, 24 * 30);
      const bucketMinutes = boundedLimit(args.bucket_minutes, 1, 60);
      const title = safeDashboardTitle(args.title, `Prometheus ${metricName}`);
      if (metricName === 'up' && ['job', 'instance'].includes(groupByLabel)) {
        return createPrometheusAvailabilityDashboardResponse(id, {
          ...args,
          hours,
          bucket_minutes: bucketMinutes,
          title: title === `Prometheus ${metricName}` ? 'Prometheus Availability Overview' : title,
        });
      }
      const uid = `prom-${randomUUID().replaceAll('-', '').slice(0, 18)}`;
      const timeWindow = await prometheusTimeWindow(hours, metricName);
      const labelExpression = `JSONExtractString(labels_json, ${quoteString(groupByLabel)})`;
      const valueExpressionByAggregation = {
        avg: 'avg(value)',
        min: 'min(value)',
        max: 'max(value)',
        p95: 'quantile(0.95)(value)',
        sum: 'sum(value)',
        count: 'count()',
        last: 'argMax(value, sample_time)',
      };
      const valueExpression = valueExpressionByAggregation[aggregation];
      const timeseriesSql = `
        SELECT
          toStartOfInterval(sample_time, INTERVAL ${bucketMinutes} MINUTE) AS time,
          if(${labelExpression} = '', 'unknown', ${labelExpression}) AS series,
          ${valueExpression} AS value
        FROM analytics.prometheus_samples
        WHERE metric_name = ${quoteString(metricName)}
          AND ${timeWindow.sqlFilter}
        GROUP BY time, series
        ORDER BY time ASC, series ASC
      `.trim();
      const latestSql = `
        SELECT
          if(${labelExpression} = '', 'unknown', ${labelExpression}) AS series,
          max(sample_time) AS last_sample_time,
          argMax(value, sample_time) AS last_value,
          count() AS samples
        FROM analytics.prometheus_samples
        WHERE metric_name = ${quoteString(metricName)}
          AND ${timeWindow.sqlFilter}
        GROUP BY series
        ORDER BY series ASC
      `.trim();
      const previewRows = await runQuery(`
        SELECT
          if(${labelExpression} = '', 'unknown', ${labelExpression}) AS series,
          count() AS samples,
          min(value) AS min_value,
          max(value) AS max_value,
          avg(value) AS avg_value,
          argMax(value, sample_time) AS last_value
        FROM analytics.prometheus_samples
        WHERE metric_name = ${quoteString(metricName)}
          AND ${timeWindow.sqlFilter}
        GROUP BY series
        ORDER BY samples DESC, series ASC
        LIMIT 50
      `);
      const dashboard = {
        id: null,
        uid,
        title,
        tags: ['agentic-data-stack', 'prometheus', 'clickhouse', metricName],
        timezone: 'browser',
        schemaVersion: 39,
        version: 0,
        refresh: '30s',
        time: {
          from: timeWindow.grafanaFrom,
          to: timeWindow.grafanaTo,
        },
        panels: [
          {
            id: 1,
            title: `${metricName} by ${groupByLabel}`,
            type: 'timeseries',
            datasource: {
              type: 'grafana-clickhouse-datasource',
              uid: 'clickhouse-analytics',
            },
            gridPos: { h: 14, w: 24, x: 0, y: 0 },
            fieldConfig: {
              defaults: {
                color: { mode: 'palette-classic' },
                custom: {
                  drawStyle: 'line',
                  lineInterpolation: 'linear',
                  lineWidth: 2,
                  fillOpacity: 10,
                  showPoints: 'never',
                  spanNulls: false,
                },
              },
              overrides: [],
            },
            options: {
              legend: { displayMode: 'list', placement: 'bottom', showLegend: true },
              tooltip: { mode: 'multi', sort: 'none' },
            },
            targets: [
              {
                datasource: {
                  type: 'grafana-clickhouse-datasource',
                  uid: 'clickhouse-analytics',
                },
                format: 0,
                rawSql: timeseriesSql,
                refId: 'A',
              },
            ],
          },
          {
            id: 2,
            title: `Latest ${metricName} values`,
            type: 'table',
            datasource: {
              type: 'grafana-clickhouse-datasource',
              uid: 'clickhouse-analytics',
            },
            gridPos: { h: 9, w: 24, x: 0, y: 14 },
            targets: [
              {
                datasource: {
                  type: 'grafana-clickhouse-datasource',
                  uid: 'clickhouse-analytics',
                },
                format: 1,
                rawSql: latestSql,
                refId: 'A',
              },
            ],
          },
        ],
      };
      const created = await createGrafanaDashboard(dashboard);
      return grafanaDashboardResponse(id, previewRows, {
        title,
        dashboardUid: created.uid || uid,
        dashboardPath: created.url || `/d/${uid}/${uid}`,
        metricName,
        groupByLabel,
        aggregation,
        hours,
        fromTime: timeWindow.fromTime,
        toTime: timeWindow.toTime,
        bucketMinutes,
        datasourceUid: 'clickhouse-analytics',
        timeseriesSql,
        latestSql,
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

    if (typeof name === 'string' && name.startsWith('clickhouse_')) {
      // Прямой вызов generated-коннектора как обычного MCP tool, без обертки run_generated_connector.
      const payload = await callGeneratedConnector(name, args);
      return jsonRpc(id, {
        content: [{ type: 'text', text: JSON.stringify(payload, null, 2) }],
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
