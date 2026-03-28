/**
 * FlycommC2 — Agent Entrypoint
 *
 * The SOC Dashboard IS the agent. It:
 *   - Queries ClickHouse every 30s (auto-refresh from browser)
 *   - Runs the full rule engine + statistical detection on each scan
 *   - Tracks detections in-memory with alerting (webhook, console)
 *   - Serves the real-time threat dashboard at http://localhost:3000
 *
 * Usage:  node start.js
 *
 * There is no separate "agent loop" — the dashboard handles everything.
 * ClickHouse access is READ-ONLY; all state is held in server memory.
 */

const { startServer } = require('./server');

console.log('╔══════════════════════════════════════════╗');
console.log('║   FlycommC2 — RF Threat Detection Agent  ║');
console.log('╚══════════════════════════════════════════╝');

startServer();
