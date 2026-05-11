import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';

import { renderMetrics } from '../synthetic-exporter/src/metrics.js';

const hours = Number(process.env.HISTORY_HOURS || 72);
const stepSeconds = Number(process.env.HISTORY_STEP_SECONDS || 60);
const output = resolve(process.env.OUTPUT || 'data/openmetrics/synthetic.openmetrics');
const end = process.env.HISTORY_END ? new Date(process.env.HISTORY_END) : new Date();
const start = new Date(end.getTime() - hours * 60 * 60 * 1000);

mkdirSync(dirname(output), { recursive: true });

const lines = [];
const metricHeader = new Set();

for (let ts = start.getTime(); ts <= end.getTime(); ts += stepSeconds * 1000) {
  const rendered = renderMetrics(new Date(ts), true);
  for (const line of rendered.trim().split('\n')) {
    if (line.startsWith('# HELP') || line.startsWith('# TYPE')) {
      if (!metricHeader.has(line)) {
        metricHeader.add(line);
        lines.push(line);
      }
      continue;
    }
    lines.push(line);
  }
}

lines.push('# EOF');
writeFileSync(output, `${lines.join('\n')}\n`);
console.log(JSON.stringify({
  output,
  hours,
  stepSeconds,
  start: start.toISOString(),
  end: end.toISOString(),
  lines: lines.length,
}, null, 2));
