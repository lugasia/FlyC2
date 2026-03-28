const config = require('./config');
const db = require('./db');
const { CRITICAL, HIGH, MEDIUM } = config.severity;

/**
 * Format a threat event into a human-readable alert string.
 */
function formatAlert(threat) {
  const ts = new Date().toISOString();
  return [
    `[${ts}] THREAT DETECTED`,
    `  Type:       ${threat.threat_type}`,
    `  Severity:   ${threat.severity}`,
    `  Cell ID:    ${threat.cell_id}`,
    `  Cell ECGI:  ${threat.cell_ecgi}`,
    `  Location:   (${threat.location_lat}, ${threat.location_lng})`,
    `  Score:      ${threat.score}`,
    `  Confidence: ${threat.confidence}`,
    `  Reasoning:  ${threat.reasoning}`,
    `  Sample ID:  ${threat.sample_id}`,
  ].join('\n');
}

/**
 * Dispatch a single threat to the appropriate alert channel(s).
 */
async function dispatchOne(threat) {
  const formatted = formatAlert(threat);
  const severity = threat.severity || 'MEDIUM';

  // Console output — color-coded by severity
  if (severity === CRITICAL || severity === HIGH) {
    console.error(`\n🚨 ${formatted}\n`);
  } else if (severity === MEDIUM) {
    console.warn(`\n⚠️  ${formatted}\n`);
  } else {
    console.log(`\nℹ️  ${formatted}\n`);
  }

  // Structured JSON log (machine-readable)
  const payload = JSON.stringify({
    timestamp: new Date().toISOString(),
    threat_type: threat.threat_type,
    severity,
    cell_id: threat.cell_id,
    location: { lat: threat.location_lat, lng: threat.location_lng },
    confidence: threat.confidence,
    reasoning: threat.reasoning,
    sample_id: threat.sample_id,
  });

  // Write to alert_log in ClickHouse
  try {
    await db.writeAlertLog({
      threat_event_id: threat.sample_id || '',
      channel: 'console',
      payload,
    });
  } catch (err) {
    console.error('[ALERTS] Failed to write alert log:', err.message);
  }

  // Webhook dispatch (if configured)
  if (config.alert.webhookUrl) {
    try {
      const response = await fetch(config.alert.webhookUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: payload,
      });
      if (!response.ok) {
        console.error(`[ALERTS] Webhook returned ${response.status}`);
      }
    } catch (err) {
      console.error('[ALERTS] Webhook dispatch failed:', err.message);
    }
  }
}

/**
 * Dispatch an array of confirmed threats.
 *
 * @param {Array} threats - confirmed threat events from AI escalation
 */
async function dispatch(threats) {
  if (!threats || threats.length === 0) return;

  console.log(`[ALERTS] Dispatching ${threats.length} threat alert(s)`);

  for (const threat of threats) {
    await dispatchOne(threat);
  }
}

module.exports = { dispatch };
