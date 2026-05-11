import {
  activeIncidentFor,
  dbState,
  incidents,
  serviceErrorRatio,
  serviceLatencyP95,
  serviceRate,
  targets,
} from './scenario.js';

const routesByService = {
  'api-gateway': ['/api/v1/search', '/api/v1/profile', '/api/v1/checkout'],
  'auth-service': ['/login', '/refresh', '/oauth/callback'],
  'payment-service': ['/charge', '/refund', '/webhook/provider'],
  'orders-service': ['/orders', '/orders/{id}', '/cart/checkout'],
  'notification-service': ['/send', '/templates/render', '/delivery-status'],
};

function labels(object) {
  return Object.entries(object)
    .filter(([, value]) => value !== undefined && value !== '')
    .map(([key, value]) => `${key}="${String(value).replaceAll('\\', '\\\\').replaceAll('"', '\\"')}"`)
    .join(',');
}

function sample(name, labelObject, value, timestampMs) {
  const renderedLabels = labels(labelObject);
  return `${name}{${renderedLabels}} ${Number(value).toFixed(6)} ${timestampMs}`;
}

function counterValue(baseRatePerMinute, date, salt = 0) {
  const minutes = Math.floor(date.getTime() / 60000);
  return Math.max(0, minutes * baseRatePerMinute + salt);
}

export function renderMetrics(date = new Date(), includeTimestamp = true) {
  const ts = includeTimestamp ? date.getTime() : undefined;
  const lines = [
    '# HELP synthetic_service_up Whether the monitored target is up.',
    '# TYPE synthetic_service_up gauge',
    '# HELP synthetic_incident_active Whether a synthetic incident is active.',
    '# TYPE synthetic_incident_active gauge',
    '# HELP synthetic_log_events_total Synthetic log events by level and event type.',
    '# TYPE synthetic_log_events_total counter',
    '# HELP synthetic_http_requests_total Synthetic HTTP requests by service, route, and status class.',
    '# TYPE synthetic_http_requests_total counter',
    '# HELP synthetic_http_request_duration_seconds_p95 Synthetic HTTP request p95 latency in seconds.',
    '# TYPE synthetic_http_request_duration_seconds_p95 gauge',
    '# HELP synthetic_db_connections Active database connections.',
    '# TYPE synthetic_db_connections gauge',
    '# HELP synthetic_db_query_duration_seconds_p95 Synthetic database query p95 latency in seconds.',
    '# TYPE synthetic_db_query_duration_seconds_p95 gauge',
    '# HELP synthetic_db_replication_lag_seconds Synthetic database replication lag in seconds.',
    '# TYPE synthetic_db_replication_lag_seconds gauge',
    '# HELP synthetic_db_disk_usage_ratio Synthetic database disk usage ratio.',
    '# TYPE synthetic_db_disk_usage_ratio gauge',
    '# HELP synthetic_process_restarts_total Synthetic process restart counter.',
    '# TYPE synthetic_process_restarts_total counter',
  ];

  for (const target of targets) {
    const common = {
      job: target.job,
      instance: target.instance,
      service: target.service,
      team: target.team,
      tier: target.tier,
      environment: process.env.ENVIRONMENT || 'synthetic-prod',
    };

    const incident = activeIncidentFor(target.service, date);
    const up = target.kind === 'db' ? dbState(target, date).up : 1;
    lines.push(sample('synthetic_service_up', { ...common, component_kind: target.kind }, up, ts));

    for (const item of incidents) {
      if (item.service === target.service) {
        lines.push(sample('synthetic_incident_active', { ...common, incident: item.name, severity: item.severity }, incident?.name === item.name ? 1 : 0, ts));
      }
    }

    if (target.kind === 'db') {
      const state = dbState(target, date);
      lines.push(sample('synthetic_db_connections', { ...common, db_engine: target.engine, state: 'active' }, state.connections, ts));
      lines.push(sample('synthetic_db_query_duration_seconds_p95', { ...common, db_engine: target.engine }, state.queryP95, ts));
      lines.push(sample('synthetic_db_replication_lag_seconds', { ...common, db_engine: target.engine }, state.replicationLag, ts));
      lines.push(sample('synthetic_db_disk_usage_ratio', { ...common, db_engine: target.engine, mount: '/var/lib/data' }, state.diskUsage, ts));
      lines.push(sample('synthetic_process_restarts_total', { ...common, reason: 'crash_loop' }, counterValue(state.restarts / 60, date, target.service.length), ts));
      const errorRate = state.up === 0 ? 20 : state.queryP95 > 0.2 ? 3 : 0.2;
      lines.push(sample('synthetic_log_events_total', { ...common, level: 'error', event: state.up === 0 ? 'database_unavailable' : 'slow_query' }, counterValue(errorRate, date, target.service.length * 11), ts));
      lines.push(sample('synthetic_log_events_total', { ...common, level: 'info', event: 'healthcheck_ok' }, counterValue(state.up === 0 ? 0 : 12, date, target.service.length * 13), ts));
      continue;
    }

    const routes = routesByService[target.service] || ['/'];
    const rpm = serviceRate(target.service, date);
    const err = serviceErrorRatio(target.service, date);
    for (const route of routes) {
      const routeShare = 1 / routes.length;
      const routeRpm = rpm * routeShare;
      lines.push(sample('synthetic_http_requests_total', { ...common, route, status_class: '2xx' }, counterValue(routeRpm * (1 - err), date, route.length), ts));
      lines.push(sample('synthetic_http_requests_total', { ...common, route, status_class: '5xx' }, counterValue(routeRpm * err, date, route.length * 7), ts));
      lines.push(sample('synthetic_http_request_duration_seconds_p95', { ...common, route }, serviceLatencyP95(target.service, route, date), ts));
    }
    lines.push(sample('synthetic_log_events_total', { ...common, level: 'error', event: incident ? 'request_failed' : 'handled_exception' }, counterValue(rpm * err, date, target.service.length * 19), ts));
    lines.push(sample('synthetic_log_events_total', { ...common, level: 'warn', event: incident ? 'degraded_dependency' : 'retry' }, counterValue(incident ? rpm * 0.07 : rpm * 0.003, date, target.service.length * 23), ts));
    lines.push(sample('synthetic_log_events_total', { ...common, level: 'info', event: 'request_completed' }, counterValue(rpm * 0.96, date, target.service.length * 29), ts));
    lines.push(sample('synthetic_process_restarts_total', { ...common, reason: incident ? 'oom_or_probe_failure' : 'rolling_deploy' }, counterValue(incident ? 0.08 : 0.0002, date, target.service.length * 31), ts));
  }

  return lines.join('\n') + '\n';
}
