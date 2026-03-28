const config = require('./config');
const db = require('./db');
const { runRules } = require('./rules');
const { runStatistics } = require('./stats');
const { escalateToAI } = require('./ai');
const { dispatch } = require('./alerts');
const { loadWatermark, saveWatermark } = require('./state');

let isRunning = false;

/**
 * Merge rule flags and stat flags by sample_id.
 * Sums scores (capped at 1.0) and collects all raw flag names.
 */
function mergeFlags(ruleFlags, statFlags) {
  const merged = {};

  for (const flag of [...ruleFlags, ...statFlags]) {
    const sid = flag.sample_id;
    if (!merged[sid]) {
      merged[sid] = {
        sample_id: sid,
        cell_id: flag.cell_id || '',
        cell_ecgi: flag.cell_ecgi || '',
        location_lat: flag.location_lat || 0,
        location_lng: flag.location_lng || 0,
        severity: flag.severity || 'MEDIUM',
        score: 0,
        raw_flags: [],
        details: [],
      };
    }

    merged[sid].score = Math.min(1.0, merged[sid].score + flag.score);
    merged[sid].raw_flags.push(flag.rule || flag.stat_check || 'UNKNOWN');
    merged[sid].details.push(flag.details);

    // Escalate severity if any individual flag is higher
    const severityRank = { CRITICAL: 4, HIGH: 3, MEDIUM: 2, LOW: 1 };
    if ((severityRank[flag.severity] || 0) > (severityRank[merged[sid].severity] || 0)) {
      merged[sid].severity = flag.severity;
    }
  }

  return Object.values(merged);
}

/**
 * Main agent loop — single iteration.
 */
async function agentLoop() {
  if (isRunning) {
    console.log('[AGENT] Previous loop still running, skipping...');
    return;
  }

  isRunning = true;
  const startTime = Date.now();

  try {
    // 1. Load watermark
    const sinceTs = await loadWatermark();
    console.log(`[AGENT] Polling for measurements since ${sinceTs}`);

    // 2. Fetch new measurements
    const measurements = await db.getNewMeasurements(sinceTs, config.agent.batchSize);

    if (measurements.length === 0) {
      console.log('[AGENT] No new data, sleeping...');
      return;
    }

    console.log(`[AGENT] Processing ${measurements.length} measurements`);

    // 3. Get known cells
    const knownCells = await db.getKnownCells();
    console.log(`[AGENT] Loaded ${knownCells.length} known cell sites`);

    // 4. Run rule-based detection (includes bad_measurements check)
    const ruleFlags = await runRules(measurements, knownCells);
    console.log(`[AGENT] Rule engine flagged ${ruleFlags.length} events`);

    // 5. Get cell baselines for statistical analysis
    const uniqueCellIds = [...new Set(measurements.map((m) => String(m.cell_pci)).filter((id) => id && id !== 'undefined' && id !== 'null'))];
    const baselines = await db.getCellBaselines(uniqueCellIds);
    console.log(`[AGENT] Loaded baselines for ${Object.keys(baselines).length} cells`);

    // 6. Run statistical anomaly detection
    const statFlags = await runStatistics(measurements, baselines);
    console.log(`[AGENT] Statistical engine flagged ${statFlags.length} events`);

    // 7. Merge and deduplicate
    const combined = mergeFlags(ruleFlags, statFlags);
    const highSuspicion = combined.filter((e) => e.score > config.agent.suspicionThreshold);
    console.log(`[AGENT] ${highSuspicion.length} events above suspicion threshold (${config.agent.suspicionThreshold})`);

    // 8. AI escalation for high-suspicion events
    if (highSuspicion.length > 0) {
      const confirmedThreats = await escalateToAI(highSuspicion);
      console.log(`[AGENT] AI confirmed ${confirmedThreats.length} threats`);

      if (confirmedThreats.length > 0) {
        // 9. Write to threat_events table
        try {
          await db.writeThreatEvents(confirmedThreats);
        } catch (writeErr) {
          console.warn('[AGENT] Could not write threat_events (read-only user?):', writeErr.message);
        }

        // 10. Dispatch alerts
        await dispatch(confirmedThreats);
      }
    }

    // 11. Update watermark to last measurement timestamp
    const lastTs = measurements[measurements.length - 1].timestamp;
    try {
      await saveWatermark(lastTs);
    } catch (wmErr) {
      console.warn('[AGENT] Could not save watermark (read-only user?):', wmErr.message);
    }

    // Summary
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(2);
    console.log(
      `[AGENT] Cycle complete: processed=${measurements.length}, ` +
      `ruleFlagged=${ruleFlags.length}, statFlagged=${statFlags.length}, ` +
      `highSuspicion=${highSuspicion.length}, elapsed=${elapsed}s`
    );
  } catch (err) {
    console.error('[AGENT] Loop error:', err.message);
    console.error(err.stack);
  } finally {
    isRunning = false;
  }
}

/**
 * Boot the agent.
 */
async function main() {
  console.log('╔══════════════════════════════════════════╗');
  console.log('║   FlycommC2 — RF Threat Detection Agent  ║');
  console.log('╚══════════════════════════════════════════╝');
  console.log(`[AGENT] Poll interval: ${config.agent.pollIntervalMs}ms`);
  console.log(`[AGENT] Batch size:    ${config.agent.batchSize}`);
  console.log(`[AGENT] Threshold:     ${config.agent.suspicionThreshold}`);
  console.log(`[AGENT] AI model:      ${config.anthropic.model}`);

  // Health check
  try {
    await db.healthCheck();
    console.log('[AGENT] ClickHouse connection OK');
  } catch (err) {
    console.error('[AGENT] ClickHouse connection FAILED:', err.message);
    console.error('[AGENT] Check your .env configuration and ensure ClickHouse is running');
    process.exit(1);
  }

  // Run immediately, then on interval
  await agentLoop();
  setInterval(agentLoop, config.agent.pollIntervalMs);

  console.log('[AGENT] Agent loop started. Press Ctrl+C to stop.');
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n[AGENT] Shutting down...');
  await db.close();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('\n[AGENT] Shutting down...');
  await db.close();
  process.exit(0);
});

// Auto-run when called directly or when required by start.js
main().catch((err) => {
  console.error('[AGENT] Fatal error:', err);
  process.exit(1);
});
