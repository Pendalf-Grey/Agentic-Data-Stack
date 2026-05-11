export const targets = [
  { kind: 'db', engine: 'postgresql', service: 'db-postgres-primary', job: 'postgresql', instance: 'postgresql-primary:5432', team: 'platform', tier: 'data' },
  { kind: 'db', engine: 'mysql', service: 'db-mysql-orders', job: 'mysql', instance: 'mysql-orders-1:3306', team: 'orders', tier: 'data' },
  { kind: 'db', engine: 'mysql', service: 'db-mysql-billing', job: 'mysql', instance: 'mysql-billing-1:3306', team: 'billing', tier: 'data' },
  { kind: 'db', engine: 'mongodb', service: 'db-mongodb-users', job: 'mongodb', instance: 'mongodb-users-1:27017', team: 'identity', tier: 'data' },
  { kind: 'db', engine: 'mongodb', service: 'db-mongodb-events', job: 'mongodb', instance: 'mongodb-events-1:27017', team: 'analytics', tier: 'data' },
  { kind: 'service', service: 'api-gateway', job: 'service', instance: 'api-gateway-1:8080', team: 'platform', tier: 'edge' },
  { kind: 'service', service: 'auth-service', job: 'service', instance: 'auth-service-1:8080', team: 'identity', tier: 'app' },
  { kind: 'service', service: 'payment-service', job: 'service', instance: 'payment-service-1:8080', team: 'billing', tier: 'app' },
  { kind: 'service', service: 'orders-service', job: 'service', instance: 'orders-service-1:8080', team: 'orders', tier: 'app' },
  { kind: 'service', service: 'notification-service', job: 'service', instance: 'notification-service-1:8080', team: 'messaging', tier: 'app' },
];

export const incidents = [
  {
    name: 'payment_gateway_degradation',
    service: 'payment-service',
    severity: 'critical',
    startMinute: 9 * 60 + 30,
    endMinute: 10 * 60 + 25,
    description: 'Payment provider timeout storm, high 5xx and latency.',
  },
  {
    name: 'mysql_billing_crash_loop',
    service: 'db-mysql-billing',
    severity: 'critical',
    startMinute: 13 * 60 + 5,
    endMinute: 13 * 60 + 28,
    description: 'MySQL billing primary restarts after disk flush stalls.',
  },
  {
    name: 'mongodb_events_disk_pressure',
    service: 'db-mongodb-events',
    severity: 'warning',
    startMinute: 17 * 60,
    endMinute: 19 * 60 + 15,
    description: 'MongoDB events disk usage and page faults grow during batch import.',
  },
  {
    name: 'postgres_checkpoint_saturation',
    service: 'db-postgres-primary',
    severity: 'warning',
    startMinute: 2 * 60 + 15,
    endMinute: 3 * 60 + 10,
    description: 'PostgreSQL checkpoint saturation, replication lag and slow queries.',
  },
  {
    name: 'notification_queue_backlog',
    service: 'notification-service',
    severity: 'warning',
    startMinute: 21 * 60,
    endMinute: 22 * 60 + 20,
    description: 'Notification queue backlog after SMS provider throttling.',
  },
];

export function minuteOfDay(date) {
  return date.getUTCHours() * 60 + date.getUTCMinutes();
}

export function dailyWave(date, periodMinutes = 1440, phase = 0) {
  const minute = minuteOfDay(date);
  return (Math.sin(((minute + phase) / periodMinutes) * Math.PI * 2) + 1) / 2;
}

export function isIncidentActive(incident, date) {
  const minute = minuteOfDay(date);
  return minute >= incident.startMinute && minute <= incident.endMinute;
}

export function activeIncidentFor(service, date) {
  return incidents.find(incident => incident.service === service && isIncidentActive(incident, date));
}

export function hashNumber(text) {
  let hash = 0;
  for (const char of text) {
    hash = ((hash << 5) - hash + char.charCodeAt(0)) | 0;
  }
  return Math.abs(hash);
}

export function jitter(text, date, amplitude = 1) {
  const bucket = Math.floor(date.getTime() / 60000);
  const value = Math.sin((hashNumber(text) + bucket * 17) * 0.013);
  return value * amplitude;
}

export function serviceRate(service, date) {
  const base = {
    'api-gateway': 420,
    'auth-service': 95,
    'payment-service': 55,
    'orders-service': 120,
    'notification-service': 38,
  }[service] || 15;
  const wave = 0.65 + dailyWave(date, 1440, -360) * 0.7;
  const incident = activeIncidentFor(service, date);
  const multiplier = incident?.name === 'payment_gateway_degradation' ? 1.45 : 1;
  return Math.max(1, base * wave * multiplier + jitter(service, date, base * 0.04));
}

export function serviceLatencyP95(service, route, date) {
  const base = {
    'api-gateway': 0.18,
    'auth-service': 0.11,
    'payment-service': 0.28,
    'orders-service': 0.22,
    'notification-service': 0.35,
  }[service] || 0.2;
  const routeMultiplier = route === '/checkout' ? 1.7 : route === '/login' ? 1.2 : route === '/send' ? 1.6 : 1;
  const incident = activeIncidentFor(service, date);
  const incidentMultiplier = incident?.name === 'payment_gateway_degradation' ? 8 : incident?.name === 'notification_queue_backlog' ? 3.2 : 1;
  return Math.max(0.01, base * routeMultiplier * incidentMultiplier + jitter(`${service}:${route}:lat`, date, 0.025));
}

export function serviceErrorRatio(service, date) {
  const base = {
    'api-gateway': 0.002,
    'auth-service': 0.0015,
    'payment-service': 0.004,
    'orders-service': 0.0025,
    'notification-service': 0.003,
  }[service] || 0.002;
  const incident = activeIncidentFor(service, date);
  if (incident?.name === 'payment_gateway_degradation') return 0.18 + Math.abs(jitter(service, date, 0.025));
  if (incident?.name === 'notification_queue_backlog') return 0.045 + Math.abs(jitter(service, date, 0.012));
  return Math.max(0, base + Math.abs(jitter(`${service}:err`, date, base)));
}

export function dbState(target, date) {
  const incident = activeIncidentFor(target.service, date);
  const down = incident?.name === 'mysql_billing_crash_loop';
  const pressure = incident?.name === 'mongodb_events_disk_pressure';
  const checkpoint = incident?.name === 'postgres_checkpoint_saturation';
  return {
    up: down ? 0 : 1,
    connections: down ? 0 : Math.round((target.engine === 'postgresql' ? 82 : target.engine === 'mysql' ? 64 : 48) + dailyWave(date, 1440, -240) * 26 + jitter(`${target.service}:conn`, date, 6)),
    queryP95: down ? 0 : Math.max(0.005, (target.engine === 'postgresql' ? 0.045 : target.engine === 'mysql' ? 0.035 : 0.055) * (checkpoint ? 6 : pressure ? 3.8 : 1) + jitter(`${target.service}:query`, date, 0.008)),
    replicationLag: down ? 180 : Math.max(0, (checkpoint ? 45 : pressure ? 18 : 1.5) + Math.abs(jitter(`${target.service}:lag`, date, 3))),
    diskUsage: Math.min(0.98, (target.engine === 'mongodb' ? 0.62 : 0.54) + (pressure ? 0.28 : 0) + dailyWave(date, 1440, 180) * 0.05 + Math.abs(jitter(`${target.service}:disk`, date, 0.015))),
    restarts: down ? 1 : 0,
  };
}
