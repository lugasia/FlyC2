const express = require('express');
const path = require('path');
const config = require('./config');
const db = require('./db');
const { runRules } = require('./rules');
const { runStatistics } = require('./stats');

const app = express();
app.use(express.json());

// ---------------------------------------------------------------------------
// Connection state tracking
// ---------------------------------------------------------------------------
let lastHealthCheckOk = false;

// ---------------------------------------------------------------------------
// API: Platform status — used by auth gate to decide setup vs dashboard
// ---------------------------------------------------------------------------
app.get('/api/status', (req, res) => {
  res.json({
    configured: config.isConfigured(),
    connected: lastHealthCheckOk,
  });
});

// ---------------------------------------------------------------------------
// IN-MEMORY ALERT HISTORY — since we can't write to ClickHouse
// Persists across dashboard refreshes for the lifetime of the server process
// ---------------------------------------------------------------------------
const alertHistory = [];           // Array of { id, timestamp, rule, severity, cell_id, ... }
const MAX_ALERT_HISTORY = 5000;    // Keep last 5000 alerts in memory
const knownAlertKeys = new Set();  // Dedup key: "rule|cell_id|sample_id"
let alertStats = { total: 0, critical: 0, high: 0, medium: 0, low: 0, lastAlertTime: null };

// SSE (Server-Sent Events) clients for real-time push to dashboard
const sseClients = new Set();

function recordAlert(flag, measurement) {
  const dedupKey = `${flag.rule}|${flag.cell_id || ''}|${measurement.sample_id || ''}`;
  if (knownAlertKeys.has(dedupKey)) return null; // Already seen
  knownAlertKeys.add(dedupKey);

  // Trim dedup set if it gets too large
  if (knownAlertKeys.size > 50000) {
    const arr = [...knownAlertKeys];
    arr.splice(0, 25000);
    knownAlertKeys.clear();
    arr.forEach(k => knownAlertKeys.add(k));
  }

  const alert = {
    id: alertHistory.length + 1,
    timestamp: new Date().toISOString(),
    measurement_time: measurement.timestamp || null,
    rule: flag.rule || flag.stat_check || 'UNKNOWN',
    severity: flag.severity || 'MEDIUM',
    score: flag.score || 0,
    cell_id: flag.cell_id || measurement.cell_pci || '--',
    cell_eci: measurement.cell_eci || null,
    cell_enb: measurement.cell_enb || null,
    location_lat: measurement.location_lat_rounded || null,
    location_lng: measurement.location_lng_rounded || null,
    network_mcc: measurement.network_mcc || null,
    network_plmn: measurement.network_PLMN || null,
    network_operator: measurement.network_operator || null,
    device_id: measurement.deviceInfo_deviceId || null,
    details: flag.details || '',
    sample_id: measurement.sample_id || null,
    sample_count: flag.sample_count || 1,
    device_count: flag.device_count || 1,
    known_site_lat: flag.known_site_lat || null,
    known_site_lng: flag.known_site_lng || null,
    known_site_id: flag.known_site_id || null,
    distance_km: flag.distance_km || null,
  };

  alertHistory.push(alert);
  if (alertHistory.length > MAX_ALERT_HISTORY) {
    alertHistory.splice(0, alertHistory.length - MAX_ALERT_HISTORY);
  }

  // Update stats
  alertStats.total++;
  const sev = (alert.severity || '').toUpperCase();
  if (sev === 'CRITICAL') alertStats.critical++;
  else if (sev === 'HIGH') alertStats.high++;
  else if (sev === 'MEDIUM') alertStats.medium++;
  else alertStats.low++;
  alertStats.lastAlertTime = alert.timestamp;

  return alert;
}

async function fireWebhook(alerts) {
  const webhookUrl = config.alert.webhookUrl;
  if (!webhookUrl || alerts.length === 0) return;

  const payload = JSON.stringify({
    source: 'FlycommC2',
    timestamp: new Date().toISOString(),
    alert_count: alerts.length,
    alerts: alerts.map(a => ({
      rule: a.rule,
      severity: a.severity,
      cell_id: a.cell_id,
      location: { lat: a.location_lat, lng: a.location_lng },
      details: a.details,
      distance_km: a.distance_km,
    })),
  });

  try {
    const response = await fetch(webhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: payload,
    });
    if (!response.ok) {
      console.error(`[WEBHOOK] POST ${webhookUrl} returned ${response.status}`);
    } else {
      console.log(`[WEBHOOK] Sent ${alerts.length} alerts to webhook`);
    }
  } catch (err) {
    console.error(`[WEBHOOK] Failed:`, err.message);
  }
}

function pushSSE(alerts) {
  if (sseClients.size === 0 || alerts.length === 0) return;
  const data = JSON.stringify(alerts);
  for (const client of sseClients) {
    try {
      client.write(`data: ${data}\n\n`);
    } catch (_) {
      sseClients.delete(client);
    }
  }
}

function logAlertsToConsole(alerts) {
  for (const a of alerts) {
    const icon = a.severity === 'CRITICAL' ? '🚨' : a.severity === 'HIGH' ? '⚠️ ' : 'ℹ️ ';
    const distInfo = a.distance_km ? ` (${a.distance_km}km from known site)` : '';
    console.log(`${icon} [ALERT] ${a.severity} ${a.rule} — Cell ${a.cell_id} at (${a.location_lat},${a.location_lng})${distInfo}`);
  }
}

// Helper: safe query wrapper — always returns 200 with empty data on error
function safeRoute(handler) {
  return async (req, res) => {
    try {
      await handler(req, res);
    } catch (err) {
      // Client disconnected or transient DB error — silently ignore
      if (err.message === 'aborted' || err.code === 'ECONNRESET' || req.destroyed ||
          (err.message && err.message.includes('Timeout'))) return;
      console.error(`[API] ${req.path} error:`, err.message);
      if (!res.headersSent) res.json({ ok: false, data: [], error: err.message });
    }
  };
}

// ---------------------------------------------------------------------------
// Serve SOC Dashboard
// ---------------------------------------------------------------------------
app.use(express.static(path.join(__dirname, 'dashboard')));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'dashboard', 'index.html'));
});

// ---------------------------------------------------------------------------
// API: Threat events — served from in-memory alert history (no ClickHouse writes)
// ---------------------------------------------------------------------------
app.get('/api/threats', safeRoute(async (req, res) => {
  const limit = parseInt(req.query.limit, 10) || 100;
  const recent = alertHistory.slice(-limit).reverse();
  res.json({ ok: true, data: recent });
}));

// ---------------------------------------------------------------------------
// API: Threat stats — from in-memory alert counters
// ---------------------------------------------------------------------------
app.get('/api/stats', safeRoute(async (req, res) => {
  res.json({
    ok: true,
    data: {
      severity_breakdown: [
        { severity: 'CRITICAL', count: alertStats.critical },
        { severity: 'HIGH', count: alertStats.high },
        { severity: 'MEDIUM', count: alertStats.medium },
        { severity: 'LOW', count: alertStats.low },
      ].filter(r => r.count > 0),
      total_threats_24h: alertStats.total,
      last_alert_time: alertStats.lastAlertTime,
    },
  });
}));

// (Health check moved to after /api/alerts — uses in-memory stats now)

// ---------------------------------------------------------------------------
// API: Bad measurements
// ---------------------------------------------------------------------------
app.get('/api/bad-measurements', safeRoute(async (req, res) => {
  const stats = await db.getBadMeasurementStats();
  res.json({ ok: true, data: stats });
}));

app.get('/api/bad-measurements/recent', safeRoute(async (req, res) => {
  const limit = parseInt(req.query.limit, 10) || 50;
  const recent = await db.getRecentBadMeasurementsForDashboard(limit);
  res.json({ ok: true, data: recent });
}));

// ---------------------------------------------------------------------------
// API: Measurements (live data from existing tables)
// ---------------------------------------------------------------------------
app.get('/api/measurements/stats', safeRoute(async (req, res) => {
  const [overall, last24h, ratDist] = await Promise.all([
    db.getMeasurementStats(),
    db.getMeasurementStats24h(),
    db.getRatDistribution(),
  ]);
  res.json({ ok: true, data: { overall, last24h, rat_distribution: ratDist } });
}));

app.get('/api/measurements/recent', safeRoute(async (req, res) => {
  const limit = parseInt(req.query.limit, 10) || 50;
  const regionCode = req.query.region;
  let measurements;

  if (regionCode && regionCode !== 'GLOBAL') {
    const region = config.regions[regionCode] || config.regions.GLOBAL;
    measurements = await db.getRecentMeasurementsFiltered(region.bbox, limit);
  } else {
    measurements = await db.getRecentMeasurements(limit);
  }

  res.json({ ok: true, data: measurements });
}));

// ---------------------------------------------------------------------------
// API: Diagnostic search — find a measurement by lat/lng in both tables
// Usage: /api/search?lat=31.7448&lng=34.3057&radius=0.01&hours=48
// ---------------------------------------------------------------------------
app.get('/api/search', safeRoute(async (req, res) => {
  const lat = parseFloat(req.query.lat);
  const lng = parseFloat(req.query.lng);
  const radius = parseFloat(req.query.radius) || 0.01;
  const hours = parseInt(req.query.hours, 10) || 48;

  if (isNaN(lat) || isNaN(lng)) {
    return res.json({ ok: false, error: 'lat and lng query params required' });
  }

  console.log(`[SEARCH] Looking for measurements near (${lat}, ${lng}) radius=${radius}° hours=${hours}`);
  const results = await db.searchMeasurementByLocation(lat, lng, radius, hours);

  console.log(`[SEARCH] Found: ${results.measurements.length} in measurements, ${results.bad_measurements.length} in bad_measurements`);

  res.json({ ok: true, data: results });
}));

// ---------------------------------------------------------------------------
// API: Real-time anomaly scan — runs rules on recent measurements on the fly
// This works even without the agent loop or threat_events table!
// ---------------------------------------------------------------------------
app.get('/api/scan/live', safeRoute(async (req, res) => {
  let regionCode = req.query.region || config.agent.region;
  let region = config.regions[regionCode] || config.regions.GLOBAL;
  const limit = parseInt(req.query.limit, 10) || 1000;
  const hours = parseInt(req.query.hours, 10) || 24;

  // Custom bbox from query string = user-drawn polygon (overrides region bbox)
  let bbox = null;
  let isCustomBbox = false;
  if (req.query.bbox) {
    const parts = req.query.bbox.split(',').map(parseFloat);
    if (parts.length === 4 && parts.every(p => !isNaN(p))) {
      bbox = { latMin: parts[0], lngMin: parts[1], latMax: parts[2], lngMax: parts[3] };
      isCustomBbox = true;

      // Auto-detect region from bbox center — find which region's bbox contains it
      const centerLat = (bbox.latMin + bbox.latMax) / 2;
      const centerLng = (bbox.lngMin + bbox.lngMax) / 2;
      let matchedRegion = null;
      for (const [code, r] of Object.entries(config.regions)) {
        if (code === 'GLOBAL' || !r.bbox) continue;
        if (centerLat >= r.bbox.latMin && centerLat <= r.bbox.latMax &&
            centerLng >= r.bbox.lngMin && centerLng <= r.bbox.lngMax) {
          matchedRegion = code;
          break;
        }
      }
      if (matchedRegion) {
        regionCode = matchedRegion;
        region = config.regions[regionCode];
      } else {
        // User polygon is outside all known regions — use GLOBAL (no MCC expectations)
        regionCode = 'GLOBAL';
        region = config.regions.GLOBAL;
      }
    }
  }

  // Fall back to region bbox if no custom bbox provided
  if (!bbox) bbox = region.bbox;

  const regionLabel = isCustomBbox ? `Custom (${regionCode})` : region.name;

  console.log(`[SCAN] Scan: ${regionLabel}, limit=${limit}, hours=${hours}, bbox=${bbox ? `${bbox.latMin.toFixed(2)},${bbox.lngMin.toFixed(2)},${bbox.latMax.toFixed(2)},${bbox.lngMax.toFixed(2)}` : 'none'}`);

  // 1. Fetch recent measurements + targeted anomaly queries in parallel
  //    All queries use the user-selected time window
  // All queries sequential — ClickHouse Cloud serverless needs warm-up
  console.log(`[SCAN] Query 1/6: recent measurements...`);
  const recentMeasurements = bbox
    ? await db.getRecentMeasurementsFiltered(bbox, limit, hours)
    : await db.getRecentMeasurements(limit);
  console.log(`[SCAN] Query 1/6: got ${recentMeasurements.length}`);

  let taZeros = [], mccAnomalies = [], downgrades = [];

  if (recentMeasurements.length > 0) {
    console.log(`[SCAN] Query 2/4: TA=0...`);
    taZeros = await db.getTAZeroMeasurements(bbox, 500, hours);
    console.log(`[SCAN] Query 2/4: got ${taZeros.length}`);

    if (region.expectedMCC && region.expectedMCC.length > 0 && bbox) {
      console.log(`[SCAN] Query 3/4: MCC anomalies...`);
      mccAnomalies = await db.getMCCAnomalyMeasurements(region.expectedMCC, bbox, 100, hours);
      console.log(`[SCAN] Query 3/4: got ${mccAnomalies.length}`);
    }

    console.log(`[SCAN] Query 4/4: downgrades...`);
    downgrades = await db.getDowngradeMeasurements(bbox, 100, hours);
    console.log(`[SCAN] Query 4/4: got ${downgrades.length}`);
  } else {
    console.log(`[SCAN] No measurements in this area — skipping`);
  }

  // bad_measurements and test_networks queries removed — too slow, low detection value

  // 2. Merge and deduplicate by sample_id (targeted queries may overlap with recent)
  const seenSampleIds = new Set();
  const measurements = [];

  // Add recent measurements first
  for (const m of recentMeasurements) {
    if (!seenSampleIds.has(m.sample_id)) {
      seenSampleIds.add(m.sample_id);
      measurements.push(m);
    }
  }

  // Add targeted anomaly measurements (these are specifically interesting)
  let injectedCount = 0;
  for (const m of [...mccAnomalies, ...taZeros, ...downgrades]) {
    if (!seenSampleIds.has(m.sample_id)) {
      seenSampleIds.add(m.sample_id);
      measurements.push(m);
      injectedCount++;
    }
  }

  console.log(`[SCAN] ${recentMeasurements.length} recent + ${mccAnomalies.length} MCC + ${taZeros.length} TA=0 + ${downgrades.length} downgrades → ${measurements.length} unique (${injectedCount} injected)`);

  // RSU mode: filter to source='modem' only — RSU hardware data
  const currentMode = deploymentMode;
  if (currentMode === 'RSU') {
    const before = measurements.length;
    const filtered = measurements.filter(m => m.source === 'modem' || m._source === 'bad_measurements');
    console.log(`[SCAN] RSU mode: filtered ${before} → ${filtered.length} (source=modem)`);
    measurements.length = 0;
    measurements.push(...filtered);
  }

  if (measurements.length === 0) {
    return res.json({ ok: true, data: { flags: [], measurements: [], total_scanned: 0, summary: {}, mode: currentMode } });
  }

  // 3. Load known cells for rule engine — filtered by region bbox to exclude
  //    bogus site entries on other continents (Israeli PLMNs at Singapore coords, etc.)
  let knownCells = [];
  try {
    knownCells = await db.getKnownCells(bbox);
  } catch (sitesErr) {
    console.warn(`[SCAN] Sites lookup skipped: ${sitesErr.message}`);
  }

  // 4. Run rule-based detection
  const ruleFlags = await runRules(measurements, knownCells, region.expectedMCC || [], bbox, ruleThresholds);

  // 5. Run statistical detection (optional — skip on DB errors to keep scan working)
  let statFlags = [];
  try {
    const uniqueCellIds = [...new Set(
      measurements.map((m) => String(m.cell_pci)).filter((id) => id && id !== 'undefined' && id !== 'null')
    )];
    const baselines = await db.getCellBaselines(uniqueCellIds);
    statFlags = await runStatistics(measurements, baselines);
  } catch (statsErr) {
    console.warn(`[SCAN] Statistical detection skipped: ${statsErr.message}`);
  }

  // 6. Combine all flags
  const allFlags = [...ruleFlags, ...statFlags];

  // 7. Build detection aggregation: group by (rule + cell_id) to get
  //    sample count, unique devices, and time range per detection cluster
  const detectionAgg = {};  // key: "rule|cell_id" → { samples: Set, devices: Set, timestamps: [] }

  // Build a quick sample_id → measurement lookup for device/timestamp info
  const measBySample = {};
  for (const m of measurements) {
    if (m.sample_id) measBySample[m.sample_id] = m;
  }

  for (const f of allFlags) {
    const rule = f.rule || f.stat_check || 'UNKNOWN';
    const cellKey = f.cell_id || 'unknown';
    const aggKey = `${rule}|${cellKey}`;
    if (!detectionAgg[aggKey]) {
      detectionAgg[aggKey] = { rule, cell_id: cellKey, samples: new Set(), devices: new Set(), timestamps: [] };
    }
    const agg = detectionAgg[aggKey];
    agg.samples.add(f.sample_id);
    const srcMeas = measBySample[f.sample_id];
    if (srcMeas) {
      if (srcMeas.deviceInfo_deviceId) agg.devices.add(srcMeas.deviceInfo_deviceId);
      if (srcMeas.timestamp) agg.timestamps.push(new Date(srcMeas.timestamp).getTime());
    }
  }

  // Convert to serialisable form: { aggKey → { sample_count, device_count, first_seen, last_seen } }
  const detectionStats = {};
  for (const [key, agg] of Object.entries(detectionAgg)) {
    const ts = agg.timestamps.filter(t => !isNaN(t));
    ts.sort((a, b) => a - b);
    detectionStats[key] = {
      sample_count: agg.samples.size,
      device_count: agg.devices.size,
      first_seen: ts.length > 0 ? new Date(ts[0]).toISOString() : null,
      last_seen: ts.length > 0 ? new Date(ts[ts.length - 1]).toISOString() : null,
    };
  }

  // 8. Build per-sample flag map and summary
  const flagsBySample = {};
  const summary = {};
  for (const f of allFlags) {
    const key = f.rule || f.stat_check || 'UNKNOWN';
    summary[key] = (summary[key] || 0) + 1;
    if (!flagsBySample[f.sample_id]) flagsBySample[f.sample_id] = [];

    const cellKey = f.cell_id || 'unknown';
    const aggKey = `${key}|${cellKey}`;
    const stats = detectionStats[aggKey] || {};

    const flagEntry = {
      rule: key,
      severity: f.severity,
      score: f.score,
      details: f.details,
      // Detection cluster stats
      sample_count: stats.sample_count || 1,
      device_count: stats.device_count || 1,
      first_seen: stats.first_seen || null,
      last_seen: stats.last_seen || null,
    };
    // Pass through known-site coordinates for map visualization (GPS spoofing lines)
    if (f.known_site_lat !== undefined) {
      flagEntry.known_site_lat = f.known_site_lat;
      flagEntry.known_site_lng = f.known_site_lng;
      flagEntry.known_site_id = f.known_site_id;
      flagEntry.distance_km = f.distance_km;
    }
    flagsBySample[f.sample_id].push(flagEntry);
  }

  // 9. Enrich measurements with flags + known site data
  const sevRank = { CRITICAL: 4, HIGH: 3, MEDIUM: 2, LOW: 1 };

  // Build site lookup for enrichment (if knownCells loaded)
  let siteLookupFn = null;
  if (knownCells.length > 0) {
    // Use the rules.js buildSiteIndex indirectly — just do a simple eNB→site map
    const siteByEnb = {};
    for (const site of knownCells) {
      if (site.site_id) siteByEnb[site.site_id] = site;
    }
    siteLookupFn = (enbId) => siteByEnb[enbId] || siteByEnb[String(enbId)] || null;
  }

  const enriched = measurements.map((m) => {
    const flags = flagsBySample[m.sample_id] || [];
    const maxSeverity = flags.length > 0
      ? flags.reduce((max, f) => (sevRank[f.severity] || 0) > (sevRank[max] || 0) ? f.severity : max, 'LOW')
      : null;
    const isAnomalous = maxSeverity !== null && sevRank[maxSeverity] > sevRank['LOW'];

    // Enrich with known site info if eNB matches
    let known_site = null;
    if (siteLookupFn && m.cell_enb) {
      const site = siteLookupFn(m.cell_enb);
      if (site) {
        known_site = {
          site_id: site.site_id,
          lat: site.lat, lng: site.lng,
          tech: site.tech,
        };
      }
    }

    return {
      ...m,
      flags,
      is_anomalous: isAnomalous,
      max_severity: isAnomalous ? maxSeverity : null,
      known_site,
    };
  });

  // 9b. Build aggregated anomaly view — one entry per (rule + cell_id + enb)
  //     instead of 47 individual samples for the same issue
  const aggAnomalies = {};
  for (const m of enriched) {
    if (!m.is_anomalous || !m.flags) continue;
    for (const f of m.flags) {
      if (f.severity === 'LOW') continue;
      const cellId = m.cell_pci != null ? String(m.cell_pci) : 'x';
      const enbId = m.cell_enb || 'x';
      const aggKey = `${f.rule}|${cellId}|${enbId}`;
      if (!aggAnomalies[aggKey]) {
        aggAnomalies[aggKey] = {
          ...m, // use first measurement as representative
          _agg_rule: f.rule,
          _agg_severity: f.severity,
          _agg_samples: new Set(),
          _agg_devices: new Set(),
          _agg_timestamps: [],
          _agg_rsrp_values: [],
          flags: [],
        };
      }
      const agg = aggAnomalies[aggKey];
      agg._agg_samples.add(m.sample_id);
      if (m.deviceInfo_deviceId) agg._agg_devices.add(m.deviceInfo_deviceId);
      if (m.timestamp) agg._agg_timestamps.push(new Date(m.timestamp).getTime());
      if (m.signal_rsrp != null) agg._agg_rsrp_values.push(Number(m.signal_rsrp));
      // Keep highest severity
      if ((sevRank[f.severity] || 0) > (sevRank[agg._agg_severity] || 0)) {
        agg._agg_severity = f.severity;
        agg.max_severity = f.severity;
      }
      // Collect unique flag details (avoid duplicating same rule text)
      if (!agg.flags.some(ef => ef.rule === f.rule)) {
        agg.flags.push(f);
      }
    }
  }

  // Finalize aggregated entries
  const aggregatedList = Object.values(aggAnomalies).map(agg => {
    const ts = agg._agg_timestamps.sort((a, b) => a - b);
    const rsrps = agg._agg_rsrp_values;
    // Update the representative flags with aggregated stats
    for (const f of agg.flags) {
      f.sample_count = agg._agg_samples.size;
      f.device_count = agg._agg_devices.size;
      f.first_seen = ts.length > 0 ? new Date(ts[0]).toISOString() : null;
      f.last_seen = ts.length > 0 ? new Date(ts[ts.length - 1]).toISOString() : null;
    }
    return {
      ...agg,
      is_anomalous: true,
      _agg_sample_count: agg._agg_samples.size,
      _agg_device_count: agg._agg_devices.size,
      _agg_first_seen: ts.length > 0 ? new Date(ts[0]).toISOString() : null,
      _agg_last_seen: ts.length > 0 ? new Date(ts[ts.length - 1]).toISOString() : null,
      _agg_avg_rsrp: rsrps.length > 0 ? (rsrps.reduce((a, b) => a + b, 0) / rsrps.length) : null,
      // Clean up internal sets
      _agg_samples: undefined, _agg_devices: undefined,
      _agg_timestamps: undefined, _agg_rsrp_values: undefined,
    };
  });

  const anomalousCount = aggregatedList.length;

  // 10. Record alerts — one per aggregated anomaly (not per sample)
  const newAlerts = [];
  for (const agg of aggregatedList) {
    for (const f of agg.flags) {
      if (f.severity === 'LOW') continue;
      const alert = recordAlert(f, agg);
      if (alert) newAlerts.push(alert);
    }
  }
  if (newAlerts.length > 0) {
    const critAlerts = newAlerts.filter(a => a.severity === 'CRITICAL' || a.severity === 'HIGH');
    logAlertsToConsole(newAlerts);
    pushSSE(newAlerts);
    if (critAlerts.length > 0) {
      fireWebhook(critAlerts); // Only webhook CRITICAL/HIGH
    }
    console.log(`[ALERTS] ${newAlerts.length} new alerts recorded (${alertStats.total} total in memory)`);
  }

  console.log(`[SCAN] Scanned ${measurements.length} measurements: ${allFlags.length} flags → ${anomalousCount} aggregated anomalies`);

  res.json({
    ok: true,
    data: {
      measurements: enriched,           // all measurements (for map)
      aggregated: aggregatedList,        // grouped anomalies (for table)
      total_scanned: measurements.length,
      total_flags: allFlags.length,
      anomalous_count: anomalousCount,
      summary,
      region: regionLabel,
      mode: currentMode,
    },
  });
}));

// ---------------------------------------------------------------------------
// API: RSU devices — latest position per modem device (RSU mode)
// ---------------------------------------------------------------------------
app.get('/api/rsu/devices', safeRoute(async (req, res) => {
  // hours = active window for online/offline status (not a filter — all devices returned)
  const hours = parseInt(req.query.hours, 10) || 24;
  let bbox = null;
  if (req.query.bbox) {
    const parts = req.query.bbox.split(',').map(parseFloat);
    if (parts.length === 4 && parts.every(p => !isNaN(p))) {
      bbox = { latMin: parts[0], lngMin: parts[1], latMax: parts[2], lngMax: parts[3] };
    }
  }
  const devices = await db.getRSUDevices(bbox, hours);
  console.log(`[RSU] ${devices.length} RSU devices in polygon (active window=${hours}h)`);
  res.json({ ok: true, data: devices });
}));

// ---------------------------------------------------------------------------
// API: RSU device detail — measurements history for a single device
// ---------------------------------------------------------------------------
app.get('/api/rsu/device/:deviceId', safeRoute(async (req, res) => {
  const deviceId = req.params.deviceId;
  const limit = parseInt(req.query.limit, 10) || 100;
  const startTs = req.query.start || null;
  const endTs = req.query.end || null;
  const measurements = await db.getRSUDeviceHistory(deviceId, limit, startTs, endTs);
  res.json({ ok: true, data: measurements });
}));

// ---------------------------------------------------------------------------
// API: RSU device timeline — bucketed aggregates for playback
// Supports: ?hours=24&bucket=5  OR  ?start=ISO&end=ISO&bucket=5
// ---------------------------------------------------------------------------
app.get('/api/rsu/device/:deviceId/timeline', safeRoute(async (req, res) => {
  const deviceId = req.params.deviceId;
  const bucket = parseInt(req.query.bucket, 10) || 5;
  const startTs = req.query.start || null;
  const endTs = req.query.end || null;
  const hours = parseInt(req.query.hours, 10) || 24;
  const timeline = await db.getRSUDeviceTimeline(deviceId, bucket, startTs, endTs, hours);
  res.json({ ok: true, data: timeline });
}));

// ---------------------------------------------------------------------------
// API: RSU WiFi — measurements from wifi_measurements table
// ---------------------------------------------------------------------------
app.get('/api/rsu/device/:deviceId/wifi', safeRoute(async (req, res) => {
  const deviceId = req.params.deviceId;
  const limit = parseInt(req.query.limit, 10) || 500;
  const startTs = req.query.start || null;
  const endTs = req.query.end || null;
  const measurements = await db.getRSUWifiHistory(deviceId, limit, startTs, endTs);
  res.json({ ok: true, data: measurements });
}));

app.get('/api/rsu/device/:deviceId/wifi/summary', safeRoute(async (req, res) => {
  const deviceId = req.params.deviceId;
  const startTs = req.query.start || null;
  const endTs = req.query.end || null;
  const summary = await db.getRSUWifiSummary(deviceId, startTs, endTs);
  res.json({ ok: true, data: summary });
}));

// ---------------------------------------------------------------------------
// API: ClickHouse connection settings (runtime configurable)
// ---------------------------------------------------------------------------
app.get('/api/settings/clickhouse', (req, res) => {
  res.json({
    ok: true,
    data: {
      host: config.clickhouse.host,
      port: config.clickhouse.port,
      database: config.clickhouse.database,
      username: config.clickhouse.username,
      // Never send password back — show masked placeholder
      hasPassword: !!config.clickhouse.password,
    },
  });
});

app.post('/api/settings/clickhouse', async (req, res) => {
  const { host, port, database, username, password } = req.body;
  if (!host || !username) {
    return res.status(400).json({ ok: false, error: 'host and username required' });
  }

  // Update config in memory
  config.clickhouse.host = host.trim();
  config.clickhouse.port = parseInt(port, 10) || 8443;
  config.clickhouse.database = (database || 'default').trim();
  config.clickhouse.username = username.trim();
  if (password && password !== '••••••••') {
    config.clickhouse.password = password;
  }

  // Reset DB client to force reconnect with new settings
  db.resetClient();

  // Test the new connection
  try {
    await db.healthCheck();
    lastHealthCheckOk = true;
    console.log(`[SETTINGS] ClickHouse connected: ${config.clickhouse.host}:${config.clickhouse.port} as ${config.clickhouse.username}`);
    res.json({ ok: true, message: 'Connected successfully' });
  } catch (err) {
    lastHealthCheckOk = false;
    console.error(`[SETTINGS] ClickHouse connection failed:`, err.message);
    res.json({ ok: false, error: 'Connection failed: ' + err.message });
  }
});

// Debug: full measurements table schema
app.get('/api/debug/schema', safeRoute(async (req, res) => {
  const rs = await db.describeTable('measurements');
  res.json({ ok: true, data: rs });
}));

// Debug: search for specific RSU device IDs in measurements
app.get('/api/debug/rsu-search', safeRoute(async (req, res) => {
  const deviceIds = [
    '860302050770881', '860302050766871',
    '868759034997975', '868759034998064',
    '860302050782860', '868759035016445', '868759034992539',
  ];
  const results = await db.searchDeviceIds(deviceIds);
  res.json({ ok: true, data: results });
}));

// Debug: check what source values exist in the measurements table
app.get('/api/debug/sources', safeRoute(async (req, res) => {
  const hours = parseInt(req.query.hours, 10) || 24;
  const rows = await db.getSourceDistribution(hours);
  console.log('[DEBUG] Source values:', JSON.stringify(rows));
  res.json({ ok: true, data: rows });
}));

// ---------------------------------------------------------------------------
// API: Sites
// ---------------------------------------------------------------------------
app.get('/api/sites/stats', safeRoute(async (req, res) => {
  const [siteStats, byTech] = await Promise.all([
    db.getSiteStats(),
    db.getSitesByTech(),
  ]);
  res.json({ ok: true, data: { ...siteStats, by_tech: byTech } });
}));

// ---------------------------------------------------------------------------
// API: Site samples
// ---------------------------------------------------------------------------
app.get('/api/site-samples/stats', safeRoute(async (req, res) => {
  const stats = await db.getSiteSampleStats();
  res.json({ ok: true, data: stats });
}));

// ---------------------------------------------------------------------------
// API: Region config + MCC analysis
// ---------------------------------------------------------------------------
app.get('/api/regions', safeRoute(async (req, res) => {
  res.json({ ok: true, data: config.regions, current: config.agent.region });
}));

app.get('/api/mcc/distribution', safeRoute(async (req, res) => {
  const regionCode = req.query.region || config.agent.region;
  const region = config.regions[regionCode] || config.regions.GLOBAL;
  const mccData = await db.getMCCDistribution(region.bbox);

  // Enrich with country info and flag unexpected MCCs
  const enriched = mccData.map((row) => {
    const info = config.getMCCInfo(row.mcc);
    const isExpected = !region.expectedMCC || region.expectedMCC.includes(row.mcc);
    return {
      ...row,
      country: info.country,
      flag: info.flag,
      is_expected: isExpected,
      anomaly: !isExpected,
    };
  });

  res.json({ ok: true, data: enriched, region: region.name });
}));

app.get('/api/measurements/region', safeRoute(async (req, res) => {
  const regionCode = req.query.region || config.agent.region;
  const region = config.regions[regionCode] || config.regions.GLOBAL;
  const limit = parseInt(req.query.limit, 10) || 50;

  const [stats, measurements] = await Promise.all([
    db.getRegionMeasurementStats(region.bbox),
    db.getRecentMeasurementsFiltered(region.bbox, limit),
  ]);

  res.json({ ok: true, data: { stats, measurements, region: region.name } });
}));

// ---------------------------------------------------------------------------
// API: Debug — find MCC 001 and other anomalous measurements
// ---------------------------------------------------------------------------
app.get('/api/debug/mcc001', safeRoute(async (req, res) => {
  const daysBack = parseInt(req.query.days, 10) || 7;
  console.log(`[DEBUG] Searching for MCC 001 / PCI 31+EARFCN 9580 in last ${daysBack} days...`);

  // Search for MCC 001 explicitly + null-MCC measurements near the reported location (32.19, 34.89)
  const [mcc001, nullMCCNearTelAviv, mccValues] = await Promise.all([
    db.debugFindMCC001(daysBack),
    db.debugNullMCCNearLocation(32.19, 34.89, 0.5, daysBack),
    db.debugMCCValues(daysBack),
  ]);

  console.log(`[DEBUG] MCC 001 matches: ${mcc001.length}`);
  console.log(`[DEBUG] Null/empty MCC near 32.19,34.89: ${nullMCCNearTelAviv.length}`);
  console.log(`[DEBUG] Distinct MCC values: ${mccValues.length}`);
  if (mcc001.length > 0) {
    console.log(`[DEBUG] First match:`, JSON.stringify(mcc001[0]).substring(0, 300));
  }
  if (nullMCCNearTelAviv.length > 0) {
    console.log(`[DEBUG] First null-MCC near TLV:`, JSON.stringify(nullMCCNearTelAviv[0]).substring(0, 300));
  }

  // Also show what the MCC anomaly query would return
  const config = require('./config');
  const region = config.getRegion();
  let mccAnomalies = [];
  if (region.expectedMCC) {
    mccAnomalies = await db.getMCCAnomalyMeasurements(region.expectedMCC, region.bbox, 50);
    console.log(`[DEBUG] getMCCAnomalyMeasurements returned: ${mccAnomalies.length} rows`);
  }

  res.json({
    ok: true,
    data: {
      mcc001_matches: mcc001,
      null_mcc_near_tel_aviv: nullMCCNearTelAviv,
      null_mcc_near_tel_aviv_count: nullMCCNearTelAviv.length,
      mcc_values_in_db: mccValues,
      mcc_anomaly_query_results: mccAnomalies.length,
      mcc_anomaly_sample: mccAnomalies.slice(0, 5),
      search_window_days: daysBack,
      region: region.name,
      expected_mcc: region.expectedMCC,
    },
  });
}));

// ---------------------------------------------------------------------------
// API: Debug — inspect bad_measurements raw records
// ---------------------------------------------------------------------------
app.get('/api/debug/bad-raw', safeRoute(async (req, res) => {
  const daysBack = parseInt(req.query.days, 10) || 7;
  const rows = await db.getBadMeasurementsWithRawData(daysBack, 50);
  console.log(`[DEBUG] bad_measurements with raw_record: ${rows.length} rows`);
  if (rows.length > 0) {
    console.log(`[DEBUG] First raw_record type: ${typeof rows[0].raw_record}`);
    console.log(`[DEBUG] First raw_record sample:`, String(rows[0].raw_record).substring(0, 500));
  }
  res.json({ ok: true, data: rows });
}));

// ---------------------------------------------------------------------------
// API: Rule threshold configuration (in-memory, persists for server lifetime)
// ---------------------------------------------------------------------------
let ruleThresholds = {
  MCC_MISMATCH: { enabled: true, expectedMCC: '425', minUniqueSamples: 1 },
  TEST_NETWORK: { enabled: true, minUniqueSamples: 1 },
  TA_ZERO_CLUSTER: { enabled: true, minUniqueSamples: 2 },
  DOWNGRADE_2G: { enabled: true, minUniqueSamples: 1 },
  GPS_SPOOFING: { enabled: true, minDistanceKm: 2.0 },
  CELL_LOCATION_MISMATCH: { enabled: true, minDistanceKm: 5.0 },
  RF_ANOMALY: { enabled: true, minUniqueSamples: 1 },
  BAD_MEASUREMENT: { enabled: true, minUniqueSamples: 1 },
  PCI_COLLISION: { enabled: true },
  JAMMING_INDICATOR: { enabled: true },
  TAC_ANOMALY: { enabled: true },
  EMPTY_NEIGHBORS: { enabled: true },
  RAPID_RESELECTION: { enabled: true },
};

// ---------------------------------------------------------------------------
// API: Deployment mode (SDK / RSU) — switchable at runtime
// ---------------------------------------------------------------------------
let deploymentMode = config.agent.deploymentMode || 'SDK'; // 'SDK' | 'RSU'

app.get('/api/mode', (req, res) => {
  res.json({ ok: true, data: { mode: deploymentMode } });
});

app.post('/api/mode', (req, res) => {
  const newMode = (req.body.mode || '').toUpperCase();
  if (newMode !== 'SDK' && newMode !== 'RSU') {
    return res.status(400).json({ ok: false, error: 'mode must be SDK or RSU' });
  }
  deploymentMode = newMode;
  console.log(`[MODE] Switched to ${deploymentMode}`);
  res.json({ ok: true, data: { mode: deploymentMode } });
});

// Make mode accessible to scan endpoint
app.locals.getDeploymentMode = () => deploymentMode;

app.get('/api/thresholds', (req, res) => {
  res.json({ ok: true, data: ruleThresholds });
});

app.post('/api/thresholds', (req, res) => {
  const newThresholds = req.body;
  if (!newThresholds || typeof newThresholds !== 'object') {
    return res.status(400).json({ ok: false, error: 'Invalid thresholds' });
  }
  // Merge into existing thresholds
  for (const rule of Object.keys(newThresholds)) {
    if (ruleThresholds[rule]) {
      Object.assign(ruleThresholds[rule], newThresholds[rule]);
    } else {
      ruleThresholds[rule] = newThresholds[rule];
    }
  }
  console.log('[THRESHOLDS] Updated:', JSON.stringify(ruleThresholds));
  res.json({ ok: true, data: ruleThresholds });
});

// Make thresholds available to rules engine
app.locals.ruleThresholds = ruleThresholds;

// ---------------------------------------------------------------------------
// API: In-memory alert history (replaces the dead ClickHouse write path)
// ---------------------------------------------------------------------------
app.get('/api/alerts', safeRoute(async (req, res) => {
  const limit = parseInt(req.query.limit, 10) || 200;
  const severity = req.query.severity; // optional filter: CRITICAL, HIGH, etc.
  let alerts = alertHistory;
  if (severity) {
    alerts = alerts.filter(a => a.severity === severity.toUpperCase());
  }
  // Most recent first
  const recent = alerts.slice(-limit).reverse();
  res.json({
    ok: true,
    data: recent,
    stats: alertStats,
    total: alertHistory.length,
  });
}));

app.get('/api/alerts/stats', safeRoute(async (req, res) => {
  res.json({ ok: true, data: alertStats });
}));

// SSE endpoint for real-time alert push to dashboard
app.get('/api/alerts/stream', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
  });
  res.write(`data: ${JSON.stringify({ type: 'connected', stats: alertStats })}\n\n`);
  sseClients.add(res);
  req.on('close', () => sseClients.delete(res));
});

// ---------------------------------------------------------------------------
// API: Health check (updated — no more watermark/threat_events dependency)
// ---------------------------------------------------------------------------
app.get('/health', safeRoute(async (req, res) => {
  await db.healthCheck();
  lastHealthCheckOk = true;
  res.json({
    status: 'ok',
    alerts_in_memory: alertStats.total,
    alert_breakdown: alertStats,
    uptime: process.uptime(),
    sse_clients: sseClients.size,
  });
}));

// ---------------------------------------------------------------------------
// Start server
// ---------------------------------------------------------------------------
function startServer() {
  const port = config.server.port;
  app.listen(port, async () => {
    console.log(`[AGENT] SOC Dashboard at http://localhost:${port}`);
    if (config.isConfigured()) {
      console.log(`[AGENT] ClickHouse: ${config.clickhouse.host}:${config.clickhouse.port} as ${config.clickhouse.username}`);
      try {
        await db.healthCheck();
        lastHealthCheckOk = true;
        console.log(`[AGENT] ClickHouse: CONNECTED`);
      } catch (e) {
        lastHealthCheckOk = false;
        console.log(`[AGENT] ClickHouse: OFFLINE (${e.message})`);
      }
    } else {
      console.log(`[AGENT] ClickHouse: NOT CONFIGURED — waiting for credentials via UI`);
    }
  });
}

module.exports = { app, startServer };

// Auto-start if run directly (node server.js)
if (require.main === module) {
  startServer();
}
