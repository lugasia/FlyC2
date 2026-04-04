/**
 * FlycommC2 Server — RF Threat Detection API
 *
 * ENGINEERING STANDARDS:
 * - Military-grade system. No shortcuts. No "for now" compromises.
 * - BOTH tables (measurements + bad_measurements) queried with the user's polygon + time range.
 * - Rules engine receives everything within scope — it decides what's anomalous, not the pipeline.
 * - Alert timestamps = measurement time (when RF event occurred), not server time.
 * - No special-case query functions for specific anomaly types. The rules are generic.
 *   Feed all data in, rules catch everything.
 * - Targeted queries (MCC, TA=0, downgrades) are a SAFETY NET for the sampling LIMIT,
 *   not a replacement for proper bad_measurements ingestion.
 */
const express = require('express');
const path = require('path');
const jwt = require('jsonwebtoken');
const config = require('./config');
const db = require('./db');
const { runRules } = require('./rules');
const { runStatistics } = require('./stats');
const orgStore = require('./orgStore');
const { generateDemoData } = require('./demoGenerator');

const JWT_SECRET = process.env.JWT_SECRET || 'flycommc2-jwt-secret-change-in-production';
const JWT_EXPIRES = '24h';

const app = express();
app.use(express.json());

// CORS — allow Vercel-hosted dashboard to call this API
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// ---------------------------------------------------------------------------
// JWT Auth — login, me, middleware
// ---------------------------------------------------------------------------
function signToken(user, org) {
  return jwt.sign({
    userId: user.id,
    email: user.email,
    orgId: user.org_id,
    role: user.role,
    license: org ? org.license : 'BOTH',
  }, JWT_SECRET, { expiresIn: JWT_EXPIRES });
}

function authMiddleware(req, res, next) {
  // Accept token from Authorization header or ?token= query param (for SSE/EventSource)
  let token = null;
  const header = req.headers.authorization;
  if (header && header.startsWith('Bearer ')) {
    token = header.slice(7);
  } else if (req.query.token) {
    token = req.query.token;
  }
  if (!token) {
    return res.status(401).json({ ok: false, error: 'Authentication required' });
  }
  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    req.auth = decoded;
    next();
  } catch (err) {
    return res.status(401).json({ ok: false, error: 'Invalid or expired token' });
  }
}

function requireRole(...roles) {
  return (req, res, next) => {
    if (!req.auth || !roles.includes(req.auth.role)) {
      return res.status(403).json({ ok: false, error: 'Insufficient permissions' });
    }
    next();
  };
}

// Public: login
app.post('/api/auth/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    if (!email || !password) {
      return res.status(400).json({ ok: false, error: 'Email and password required' });
    }
    const user = await orgStore.verifyPassword(email, password);
    if (!user) {
      return res.status(401).json({ ok: false, error: 'Invalid credentials' });
    }
    const org = user.org_id ? orgStore.getOrg(user.org_id) : null;
    const token = signToken(user, org);
    res.json({
      ok: true,
      token,
      user: orgStore.sanitizeUser(user),
      org: org ? { id: org.id, name: org.name, license: org.license, logo_url: org.logo_url, demo_mode: org.demo_mode } : null,
    });
  } catch (err) {
    console.error('[AUTH] Login error:', err.message);
    res.status(500).json({ ok: false, error: 'Login failed' });
  }
});

// Protected: current user info
app.get('/api/auth/me', authMiddleware, (req, res) => {
  const user = orgStore.getUser(req.auth.userId);
  if (!user) return res.status(401).json({ ok: false, error: 'User not found' });
  const org = req.auth.orgId ? orgStore.getOrg(req.auth.orgId) : null;
  res.json({
    ok: true,
    user: orgStore.sanitizeUser(user),
    org: org ? { id: org.id, name: org.name, license: org.license, logo_url: org.logo_url, demo_mode: org.demo_mode, cluster: org.cluster, sub_clusters: org.sub_clusters } : null,
  });
});

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
    timestamp: measurement.timestamp || new Date().toISOString(),
    server_time: new Date().toISOString(),
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
// Geo helpers — bbox clipping for org-scoped scans
// ---------------------------------------------------------------------------
function polygonToBbox(polygon) {
  if (!polygon || !polygon.coordinates || !polygon.coordinates[0]) return null;
  const coords = polygon.coordinates[0]; // outer ring
  let latMin = 90, latMax = -90, lngMin = 180, lngMax = -180;
  for (const [lng, lat] of coords) {
    if (lat < latMin) latMin = lat;
    if (lat > latMax) latMax = lat;
    if (lng < lngMin) lngMin = lng;
    if (lng > lngMax) lngMax = lng;
  }
  return { latMin, lngMin, latMax, lngMax };
}

function clipBbox(bbox, clusterBbox) {
  if (!bbox || !clusterBbox) return bbox || clusterBbox;
  return {
    latMin: Math.max(bbox.latMin, clusterBbox.latMin),
    lngMin: Math.max(bbox.lngMin, clusterBbox.lngMin),
    latMax: Math.min(bbox.latMax, clusterBbox.latMax),
    lngMax: Math.min(bbox.lngMax, clusterBbox.lngMax),
  };
}

// ---------------------------------------------------------------------------
// Serve SOC Dashboard (static files — no auth needed)
// ---------------------------------------------------------------------------
app.use(express.static(path.join(__dirname, 'dashboard')));

// ---------------------------------------------------------------------------
// Auth wall — all routes below require a valid JWT
// ---------------------------------------------------------------------------
app.use('/api/scan', authMiddleware);
app.use('/api/threats', authMiddleware);
app.use('/api/stats', authMiddleware);
app.use('/api/measurements', authMiddleware);
app.use('/api/bad-measurements', authMiddleware);
app.use('/api/search', authMiddleware);
app.use('/api/rsu', authMiddleware);
app.use('/api/sites', authMiddleware);
app.use('/api/alerts', authMiddleware);
app.use('/api/regions', authMiddleware);
app.use('/api/mcc', authMiddleware);
app.use('/api/mode', authMiddleware);
app.use('/api/thresholds', authMiddleware);
app.use('/api/settings', authMiddleware);
app.use('/api/debug', authMiddleware);
app.use('/api/admin', authMiddleware, requireRole('super_admin'));

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
let scanInProgress = false; // Concurrency lock — ClickHouse Cloud allows max 1 connection
let scanStartTime = 0;     // Timestamp when scan started — for stuck-lock protection

app.get('/api/scan/live', safeRoute(async (req, res) => {
  // Prevent concurrent scans — ClickHouse Cloud serverless can't handle parallel queries
  // Auto-release stuck lock after 5 minutes (safety valve)
  if (scanInProgress && (Date.now() - scanStartTime) < 300000) {
    return res.json({ ok: false, error: 'Scan already in progress. Please wait.' });
  }
  if (scanInProgress) {
    console.log('[SCAN] Releasing stuck scan lock (>5min)');
  }
  scanInProgress = true;
  scanStartTime = Date.now();
  try {

  // Demo mode intercept — return synthetic data instead of querying ClickHouse
  if (req.auth && req.auth.orgId) {
    const demoOrg = orgStore.getOrg(req.auth.orgId);
    if (demoOrg && demoOrg.demo_mode) {
      const mode = deploymentMode;
      const demo = generateDemoData(demoOrg, { count: 200, mode });
      const ruleFlags = await runRules(demo.measurements, [], demo.expectedMCC, null, ruleThresholds);
      // Build enriched response same as real scan
      const flagsBySample = {};
      const summary = {};
      for (const f of ruleFlags) {
        const key = f.rule || 'UNKNOWN';
        summary[key] = (summary[key] || 0) + 1;
        if (!flagsBySample[f.sample_id]) flagsBySample[f.sample_id] = [];
        flagsBySample[f.sample_id].push({ rule: key, severity: f.severity, score: f.score, details: f.details, sample_count: 1, device_count: 1 });
      }
      const sevRank = { CRITICAL: 4, HIGH: 3, MEDIUM: 2, LOW: 1 };
      const enriched = demo.measurements.map(m => {
        const flags = flagsBySample[m.sample_id] || [];
        const maxSev = flags.length > 0 ? flags.reduce((max, f) => (sevRank[f.severity] || 0) > (sevRank[max] || 0) ? f.severity : max, 'LOW') : null;
        const isAnom = maxSev !== null && sevRank[maxSev] > sevRank['LOW'];
        return { ...m, flags, is_anomalous: isAnom, max_severity: isAnom ? maxSev : null };
      });
      const aggregated = enriched.filter(m => m.is_anomalous);
      scanInProgress = false;
      return res.json({
        ok: true, data: {
          measurements: enriched, aggregated, total_scanned: demo.measurements.length,
          total_flags: ruleFlags.length, anomalous_count: aggregated.length, summary,
          region: demoOrg.name + ' (Demo)', mode, demo: true,
        },
      });
    }
  }

  let regionCode = req.query.region || config.agent.region;
  let region = config.regions[regionCode] || config.regions.GLOBAL;
  const limit = parseInt(req.query.limit, 10) || 1000;
  const startDate = req.query.startDate || null;
  const endDate = req.query.endDate || null;
  let hours;
  // timeFilter: absolute dates for precise DB queries, hours as fallback
  let timeFilter = null;
  if (startDate && endDate) {
    timeFilter = { startDate, endDate };
    const start = new Date(startDate);
    const end = new Date(endDate);
    hours = Math.ceil((end.getTime() - start.getTime()) / 3600000);
    if (hours < 1) hours = 24;
  } else {
    hours = parseInt(req.query.hours, 10) || 24;
  }

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

  // Org-scoped clipping — restrict bbox to the org's cluster polygon
  if (req.auth && req.auth.orgId) {
    const org = orgStore.getOrg(req.auth.orgId);
    if (org && org.cluster && org.cluster.coordinates) {
      const clusterBbox = polygonToBbox(org.cluster);
      if (clusterBbox) {
        bbox = clipBbox(bbox, clusterBbox);
      }
    }
  }

  const regionLabel = isCustomBbox ? `Custom (${regionCode})` : region.name;

  // RSU mode → only scan source='modem' from measurements table
  const sourceFilter = (deploymentMode === 'RSU') ? 'modem' : null;
  const modeLabel = sourceFilter ? `RSU (source=modem)` : 'SDK (all sources)';

  console.log(`[SCAN] Scan: ${regionLabel} [${modeLabel}], limit=${limit}, hours=${hours}, bbox=${bbox ? `${bbox.latMin.toFixed(2)},${bbox.lngMin.toFixed(2)},${bbox.latMax.toFixed(2)},${bbox.lngMax.toFixed(2)}` : 'none'}`);

  // ---------------------------------------------------------------------------
  // 1. Fetch measurements
  //    RSU mode: chunked full-coverage (all unique samples — typically <5K)
  //    SDK mode: fast LIMIT approach (sample of recent data — millions of rows)
  // ---------------------------------------------------------------------------
  let recentMeasurements;
  if (deploymentMode === 'RSU' && bbox) {
    console.log(`[SCAN] Query 1/6: all unique RSU measurements (chunked)...`);
    recentMeasurements = await db.getAllUniqueMeasurements(bbox, hours, timeFilter, sourceFilter, limit);
  } else if (bbox) {
    console.log(`[SCAN] Query 1/6: recent measurements (LIMIT ${limit})...`);
    recentMeasurements = await db.getRecentMeasurementsFiltered(bbox, limit, hours, timeFilter, sourceFilter);
  } else {
    console.log(`[SCAN] Query 1/6: recent measurements (no bbox)...`);
    recentMeasurements = await db.getRecentMeasurements(limit);
  }
  console.log(`[SCAN] Query 1/6: got ${recentMeasurements.length} rows`);

  // ---------------------------------------------------------------------------
  // 2. Targeted anomaly queries — safety net for specific indicators
  // ---------------------------------------------------------------------------
  let taZeros = [], mccAnomalies = [], downgrades = [];

  if (recentMeasurements.length > 0) {
    console.log(`[SCAN] Query 2/2: targeted anomalies (UNION ALL)...`);
    const targeted = await db.getTargetedAnomalies(bbox, region.expectedMCC, hours, timeFilter, sourceFilter);
    taZeros = targeted.taZeros;
    mccAnomalies = targeted.mccAnomalies;
    downgrades = targeted.downgrades;
    console.log(`[SCAN] Query 2/2: TA=0: ${taZeros.length}, MCC: ${mccAnomalies.length}, downgrades: ${downgrades.length}`);
  } else {
    console.log(`[SCAN] No measurements in this area — skipping targeted queries`);
  }

  // ---------------------------------------------------------------------------
  // 3. Merge and deduplicate by sample_id
  // ---------------------------------------------------------------------------
  const seenSampleIds = new Set();
  const measurements = [];

  for (const m of recentMeasurements) {
    if (!seenSampleIds.has(m.sample_id)) {
      seenSampleIds.add(m.sample_id);
      measurements.push(m);
    }
  }

  let injectedCount = 0;
  for (const m of [...mccAnomalies, ...taZeros, ...downgrades]) {
    if (!seenSampleIds.has(m.sample_id)) {
      seenSampleIds.add(m.sample_id);
      measurements.push(m);
      injectedCount++;
    }
  }

  const uniqueBeforeBad = measurements.length;
  console.log(`[SCAN] ${recentMeasurements.length} rows → ${uniqueBeforeBad} unique + ${injectedCount} targeted injected`);

  // ---------------------------------------------------------------------------
  // 4. BAD MEASUREMENTS — SDK mode only
  // ---------------------------------------------------------------------------
  let badParsed = [];
  let badMccParsed = [];

  if (deploymentMode !== 'RSU') {
  try {
    console.log(`[SCAN] Query 5/6: bad_measurements (bbox-filtered, 30s timeout)...`);
    const badRaw = await Promise.race([
      db.getBadMeasurementsInBbox(bbox, hours, 1000, timeFilter),
      new Promise((_, rej) => setTimeout(() => rej(new Error('timeout 30s')), 30000)),
    ]);
    for (const row of badRaw) {
      const parsed = db.parseBadMeasurementRawRecord(row);
      if (!parsed || !parsed.location_lat_rounded || !parsed.location_lng_rounded) continue;
      badParsed.push(parsed);
    }
    console.log(`[SCAN] Query 5/6: ${badRaw.length} raw → ${badParsed.length} in bbox`);
  } catch (err) {
    console.log(`[SCAN] Query 5/6 skipped: ${err.message}`);
  }

  // Targeted: MCC anomalies in bad_measurements (LIKE scan)
  if (timeFilter && region.expectedMCC && region.expectedMCC.length > 0) {
    try {
      console.log(`[SCAN] Query 6/6: MCC anomalies in bad_measurements (LIKE, date-range)...`);
      const badMccRaw = await Promise.race([
        db.getBadMeasurementsRFAnomalies(region.expectedMCC, bbox, hours, 500, timeFilter),
        new Promise((_, rej) => setTimeout(() => rej(new Error('timeout 30s')), 30000)),
      ]);
      for (const row of badMccRaw) {
        const parsed = db.parseBadMeasurementRawRecord(row);
        if (!parsed || !parsed.location_lat_rounded || !parsed.location_lng_rounded) continue;
        badMccParsed.push(parsed);
      }
      console.log(`[SCAN] Query 6/6: ${badMccRaw.length} raw → ${badMccParsed.length} parsed`);
    } catch (err) {
      console.log(`[SCAN] Query 6/6 skipped: ${err.message}`);
    }
  }

    // Merge bad_measurements into main set
    for (const m of [...badParsed, ...badMccParsed]) {
      if (!seenSampleIds.has(m.sample_id)) {
        seenSampleIds.add(m.sample_id);
        measurements.push(m);
      }
    }
    if (badParsed.length + badMccParsed.length > 0) {
      console.log(`[SCAN] +${badParsed.length} bad +${badMccParsed.length} badMCC → ${measurements.length} total unique`);
    }
  } else {
    console.log('[SCAN] RSU mode — skipping bad_measurements queries');
  }

  // Custom date range: filter out measurements outside the requested window
  if (startDate && endDate) {
    const rangeStart = new Date(startDate).getTime();
    const rangeEnd = new Date(endDate).getTime();
    const before = measurements.length;
    const inRange = measurements.filter(m => {
      const ts = m.timestamp ? new Date(m.timestamp).getTime() : 0;
      return ts >= rangeStart && ts <= rangeEnd;
    });
    measurements.length = 0;
    measurements.push(...inRange);
    console.log(`[SCAN] Date range filter: ${before} → ${measurements.length} (${startDate} to ${endDate})`);
  }

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
  } finally {
    scanInProgress = false;
  }
}));

// ---------------------------------------------------------------------------
// API: Progressive Scan — meta (plan) + chunk (execute per time window)
// Frontend calls /meta first to learn how many chunks, then /chunk?i=0,1,2...
// ---------------------------------------------------------------------------
app.get('/api/scan/meta', safeRoute(async (req, res) => {
  const sourceFilter = (deploymentMode === 'RSU') ? 'modem' : null;
  const hours = parseInt(req.query.hours, 10) || 24;
  let timeFilter = null;
  if (req.query.startDate && req.query.endDate) {
    timeFilter = { startDate: req.query.startDate, endDate: req.query.endDate };
  }
  let bbox = null;
  if (req.query.bbox) {
    const parts = req.query.bbox.split(',').map(parseFloat);
    if (parts.length === 4 && parts.every(p => !isNaN(p))) {
      bbox = { latMin: parts[0], lngMin: parts[1], latMax: parts[2], lngMax: parts[3] };
    }
  }

  // Org-scoped bbox clipping
  if (req.auth && req.auth.orgId && req.auth.role !== 'super_admin') {
    const org = orgStore.getOrg(req.auth.orgId);
    if (org && org.cluster && org.cluster.coordinates) {
      const clusterBbox = polygonToBbox(org.cluster);
      if (clusterBbox) bbox = clipBbox(bbox, clusterBbox);
    }
  }

  const meta = await db.getScanMeta(bbox, hours, timeFilter, sourceFilter);
  const totalRows = parseInt(meta.total, 10) || 0;
  const uniqueSamples = parseInt(meta.unique_samples, 10) || 0;
  const activeDevices = parseInt(meta.active_devices, 10) || 0;

  // Calculate chunks: aim for ~1000 rows per chunk
  const ROWS_PER_CHUNK = 1000;
  const totalChunks = Math.max(1, Math.ceil(totalRows / ROWS_PER_CHUNK));

  console.log(`[SCAN META] ${totalRows} rows, ${uniqueSamples} unique samples, ${activeDevices} devices → ${totalChunks} chunks`);

  res.json({
    ok: true, data: {
      totalRows, uniqueSamples, activeDevices, totalChunks,
      minTs: meta.min_ts, maxTs: meta.max_ts,
      mode: deploymentMode,
    },
  });
}));

app.get('/api/scan/chunk', safeRoute(async (req, res) => {
  const sourceFilter = (deploymentMode === 'RSU') ? 'modem' : null;
  const chunkIndex = parseInt(req.query.i, 10) || 0;
  const totalChunks = parseInt(req.query.total, 10) || 1;
  const minTs = req.query.minTs;
  const maxTs = req.query.maxTs;

  if (!minTs || !maxTs) {
    return res.status(400).json({ ok: false, error: 'minTs and maxTs required' });
  }

  let bbox = null;
  if (req.query.bbox) {
    const parts = req.query.bbox.split(',').map(parseFloat);
    if (parts.length === 4 && parts.every(p => !isNaN(p))) {
      bbox = { latMin: parts[0], lngMin: parts[1], latMax: parts[2], lngMax: parts[3] };
    }
  }

  // Org-scoped bbox clipping
  if (req.auth && req.auth.orgId && req.auth.role !== 'super_admin') {
    const org = orgStore.getOrg(req.auth.orgId);
    if (org && org.cluster && org.cluster.coordinates) {
      const clusterBbox = polygonToBbox(org.cluster);
      if (clusterBbox) bbox = clipBbox(bbox, clusterBbox);
    }
  }

  // Calculate this chunk's time window
  const start = new Date(minTs).getTime();
  const end = new Date(maxTs).getTime();
  const windowMs = (end - start) / totalChunks;
  const chunkStart = new Date(start + chunkIndex * windowMs).toISOString();
  const chunkEnd = new Date(start + (chunkIndex + 1) * windowMs + (chunkIndex === totalChunks - 1 ? 1000 : 0)).toISOString(); // +1s on last chunk to include boundary

  console.log(`[SCAN CHUNK ${chunkIndex + 1}/${totalChunks}] ${chunkStart} → ${chunkEnd}`);

  // Fetch raw measurements for this time window
  const rawMeasurements = await db.getMeasurementsInTimeWindow(bbox, chunkStart, chunkEnd, sourceFilter);

  // Deduplicate by sample_id
  const seen = new Set();
  const measurements = [];
  for (const m of rawMeasurements) {
    if (!seen.has(m.sample_id)) {
      seen.add(m.sample_id);
      measurements.push(m);
    }
  }

  // Detect region for rules
  let regionCode = req.query.region || config.agent.region;
  let region = config.regions[regionCode] || config.regions.GLOBAL;

  // Run rules on this chunk
  const knownCells = await db.getKnownCells(bbox);
  const ruleFlags = await runRules(measurements, knownCells, region.expectedMCC || [], bbox, ruleThresholds);

  // Stat detection
  let statFlags = [];
  try {
    const uniqueCellIds = [...new Set(measurements.map(m => String(m.cell_pci)).filter(id => id && id !== 'undefined'))];
    const baselines = await db.getCellBaselines(uniqueCellIds);
    statFlags = await runStatistics(measurements, baselines);
  } catch (_) {}

  const allFlags = [...ruleFlags, ...statFlags];

  // Build flag map
  const flagsBySample = {};
  const summary = {};
  for (const f of allFlags) {
    const key = f.rule || f.stat_check || 'UNKNOWN';
    summary[key] = (summary[key] || 0) + 1;
    if (!flagsBySample[f.sample_id]) flagsBySample[f.sample_id] = [];
    flagsBySample[f.sample_id].push({
      rule: key, severity: f.severity, score: f.score, details: f.details,
      known_site_lat: f.known_site_lat, known_site_lng: f.known_site_lng,
      known_site_id: f.known_site_id, distance_km: f.distance_km,
      sample_count: f.sample_count || 1, device_count: f.device_count || 1,
    });
  }

  // Enrich
  const sevRank = { CRITICAL: 4, HIGH: 3, MEDIUM: 2, LOW: 1 };
  const enriched = measurements.map(m => {
    const flags = flagsBySample[m.sample_id] || [];
    const maxSev = flags.length > 0 ? flags.reduce((max, f) => (sevRank[f.severity] || 0) > (sevRank[max] || 0) ? f.severity : max, 'LOW') : null;
    const isAnom = maxSev !== null && sevRank[maxSev] > sevRank['LOW'];
    return { ...m, flags, is_anomalous: isAnom, max_severity: isAnom ? maxSev : null };
  });

  const aggregated = enriched.filter(m => m.is_anomalous);

  // Count unique devices in this chunk
  const chunkDevices = new Set(measurements.map(m => m.deviceInfo_deviceId).filter(Boolean));

  // Record alerts
  for (const agg of aggregated) {
    for (const f of agg.flags) {
      if (f.severity !== 'LOW') {
        const alert = recordAlert(f, agg);
        if (alert) {
          pushSSE([alert]);
        }
      }
    }
  }

  console.log(`[SCAN CHUNK ${chunkIndex + 1}/${totalChunks}] ${measurements.length} unique samples, ${allFlags.length} flags, ${aggregated.length} anomalies, ${chunkDevices.size} devices`);

  res.json({
    ok: true, data: {
      measurements: enriched,
      aggregated,
      total_scanned: measurements.length,
      total_flags: allFlags.length,
      anomalous_count: aggregated.length,
      summary,
      region: region.name,
      mode: deploymentMode,
      chunk: {
        index: chunkIndex,
        total: totalChunks,
        chunkDevices: chunkDevices.size,
        timeStart: chunkStart,
        timeEnd: chunkEnd,
      },
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

  // ORG SCOPING: clip bbox to org's cluster polygon
  if (req.auth && req.auth.orgId && req.auth.role !== 'super_admin') {
    const org = orgStore.getOrg(req.auth.orgId);
    if (org && org.cluster && org.cluster.coordinates) {
      const clusterBbox = polygonToBbox(org.cluster);
      bbox = bbox ? clipBbox(bbox, clusterBbox) : clusterBbox;
      if (!bbox || bbox.latMin > bbox.latMax || bbox.lngMin > bbox.lngMax) {
        return res.json({ ok: true, data: [] });
      }
    }
  }

  // Query both tables in parallel and merge
  const [measDevices, modemDevices] = await Promise.all([
    db.getRSUDevices(bbox, hours),
    db.getModemMeasurementDevices(bbox, hours).catch(err => {
      console.warn('[RSU] modem_measurements query failed, skipping:', err.message);
      return [];
    }),
  ]);
  // Merge: modem_measurements devices that aren't already in measurements (by device_id)
  const measIds = new Set(measDevices.map(d => d.device_id));
  const uniqueModem = modemDevices.filter(d => !measIds.has(d.device_id));
  const devices = [...measDevices, ...uniqueModem];
  console.log(`[RSU] ${measDevices.length} from measurements + ${uniqueModem.length} from modem_measurements = ${devices.length} total`);
  res.json({ ok: true, data: devices });
}));

// ---------------------------------------------------------------------------
// Middleware: validate device is within org's cluster
// ---------------------------------------------------------------------------
async function validateDeviceAccess(req, res, next) {
  if (!req.auth || req.auth.role === 'super_admin') return next();
  const org = orgStore.getOrg(req.auth.orgId);
  if (!org || !org.cluster || !org.cluster.coordinates) return next(); // No cluster defined, allow
  const deviceId = req.params.deviceId;
  const clusterBbox = polygonToBbox(org.cluster);
  try {
    const devices = await db.getRSUDevices(clusterBbox, 720); // 30 days window
    const found = devices.some(d => d.device_id === deviceId);
    if (!found) return res.status(403).json({ ok: false, error: 'Device not in your cluster' });
    next();
  } catch (err) {
    console.error('[AUTH] Device access check failed:', err.message);
    return res.status(500).json({ ok: false, error: 'Device access check failed' });
  }
}

// ---------------------------------------------------------------------------
// API: RSU device detail — measurements history for a single device
// ---------------------------------------------------------------------------
app.get('/api/rsu/device/:deviceId', validateDeviceAccess, safeRoute(async (req, res) => {
  const deviceId = req.params.deviceId;
  const limit = parseInt(req.query.limit, 10) || 100;
  const startTs = req.query.start || null;
  const endTs = req.query.end || null;
  // Try measurements table first; if empty, fall back to modem_measurements
  let measurements = await db.getRSUDeviceHistory(deviceId, limit, startTs, endTs);
  if (measurements.length === 0) {
    const modemData = await db.getRSUModemMeasurements(deviceId, limit, startTs, endTs);
    if (modemData.length > 0) {
      // Normalize modem_measurements fields to match the expected measurement field names
      measurements = modemData.map(m => ({
        timestamp: m.timestamp, sample_id: m.sample_id, source: m.source || 'modem',
        signal_rsrp: m.signal, signal_rsrq: m.quality, signal_snr: m.sinr, signal_rssi: m.rssi,
        signal_timingAdvance: null, signal_txPower: m.tx_power, signal_cqi: m.cqi,
        cell_pci: m.pcid, cell_eci: m.cellid, cell_enb: m.enodeb_id, cell_tac: m.tac,
        tech: m.rat, band_downlinkFrequency: m.frequency, band_number: m.band,
        band_bandwidth: m.bandwidth, band_name: m.band != null ? 'B' + m.band : null,
        network_PLMN: m.mcc && m.mnc ? m.mcc + '-' + m.mnc : null,
        network_mcc: m.mcc, network_mnc: m.mnc,
        location_lat_rounded: m.lat, location_lng_rounded: m.lng,
        location_altitude: m.altitude, location_speed: m.loc_speed, location_heading: m.heading,
        location_accuracy: m.horizontal_accuracy,
        deviceInfo_deviceId: m.serial_number, deviceInfo_deviceModel: m.device_name,
        deviceInfo_uptime: m.uptime, deviceInfo_modemVersion: m.modem_version,
        deviceInfo_temperature: null,
        connectionStatus: m.connection, isRegistered: null,
        // Extra modem-only fields (passed through for Modem tab)
        ue_state: m.ue_state, rrc_state: m.rrc_state, duplex: m.duplex,
        ri: m.ri, srxlev: m.srxlev, data_modem: m.data_modem,
        fix_type: m.fix_type, fix_quality: m.fix_quality,
        satellites_used: m.satellites_used, hdop: m.hdop, pdop: m.pdop, vdop: m.vdop,
        signal_prx: m.signal_prx, signal_drx: m.signal_drx,
        signal_rx2: m.signal_rx2, signal_rx3: m.signal_rx3,
        ca_index: m.ca_index, is_nsa: m.is_nsa, scs: m.scs,
        average_rtt: m.average_rtt, jitter: m.jitter, packet_loss: m.packet_loss,
        download_mbps: m.download_mbps, upload_mbps: m.upload_mbps,
        _source: 'modem_measurements',
      }));
    }
  }
  res.json({ ok: true, data: measurements });
}));

// ---------------------------------------------------------------------------
// API: RSU device timeline — bucketed aggregates for playback
// Supports: ?hours=24&bucket=5  OR  ?start=ISO&end=ISO&bucket=5
// ---------------------------------------------------------------------------
app.get('/api/rsu/device/:deviceId/timeline', validateDeviceAccess, safeRoute(async (req, res) => {
  const deviceId = req.params.deviceId;
  const bucket = parseInt(req.query.bucket, 10) || 5;
  const startTs = req.query.start || null;
  const endTs = req.query.end || null;
  const hours = parseInt(req.query.hours, 10) || 24;
  let timeline = await db.getRSUDeviceTimeline(deviceId, bucket, startTs, endTs, hours);
  // Fall back to modem_measurements timeline if measurements table has no data
  if (timeline.length === 0) {
    timeline = await db.getRSUModemMeasurementsTimeline(deviceId, bucket, startTs, endTs, hours);
  }
  res.json({ ok: true, data: timeline });
}));

// ---------------------------------------------------------------------------
// API: RSU WiFi — measurements from wifi_measurements table
// ---------------------------------------------------------------------------
app.get('/api/rsu/device/:deviceId/wifi', validateDeviceAccess, safeRoute(async (req, res) => {
  const deviceId = req.params.deviceId;
  const limit = parseInt(req.query.limit, 10) || 500;
  const startTs = req.query.start || null;
  const endTs = req.query.end || null;
  const measurements = await db.getRSUWifiHistory(deviceId, limit, startTs, endTs);
  res.json({ ok: true, data: measurements });
}));

// ---------------------------------------------------------------------------
// API: RSU modem_measurements — additional modem-level data
// ---------------------------------------------------------------------------
app.get('/api/rsu/device/:deviceId/modem', validateDeviceAccess, safeRoute(async (req, res) => {
  const deviceId = req.params.deviceId;
  const limit = parseInt(req.query.limit, 10) || 100;
  const startTs = req.query.start || null;
  const endTs = req.query.end || null;
  const data = await db.getRSUModemMeasurements(deviceId, limit, startTs, endTs);
  res.json({ ok: true, data });
}));

app.get('/api/rsu/device/:deviceId/modem/qoe', validateDeviceAccess, safeRoute(async (req, res) => {
  const deviceId = req.params.deviceId;
  const data = await db.getLatestModemQoE(deviceId);
  res.json({ ok: true, data });
}));

app.get('/api/rsu/device/:deviceId/modem/timeline', validateDeviceAccess, safeRoute(async (req, res) => {
  const deviceId = req.params.deviceId;
  const bucket = parseInt(req.query.bucket, 10) || 5;
  const startTs = req.query.start || null;
  const endTs = req.query.end || null;
  const hours = parseInt(req.query.hours, 10) || 24;
  const data = await db.getRSUModemMeasurementsTimeline(deviceId, bucket, startTs, endTs, hours);
  res.json({ ok: true, data });
}));

app.get('/api/rsu/device/:deviceId/wifi/summary', validateDeviceAccess, safeRoute(async (req, res) => {
  const deviceId = req.params.deviceId;
  const startTs = req.query.start || null;
  const endTs = req.query.end || null;
  const summary = await db.getRSUWifiSummary(deviceId, startTs, endTs);
  res.json({ ok: true, data: summary });
}));

// ---------------------------------------------------------------------------
// API: Known sites in region — for map overlay (RSU mode)
// ---------------------------------------------------------------------------
app.get('/api/sites', safeRoute(async (req, res) => {
  let bbox = null;
  if (req.query.bbox) {
    const parts = req.query.bbox.split(',').map(parseFloat);
    if (parts.length === 4 && parts.every(p => !isNaN(p))) {
      bbox = { latMin: parts[0], lngMin: parts[1], latMax: parts[2], lngMax: parts[3] };
    }
  }
  // Return only cells seen in RSU measurements (last year), enriched with known site DB
  const sites = await db.getScannedSites(bbox);
  res.json({ ok: true, data: sites });
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
  // Auto-detect region from org's cluster center
  let current = 'GLOBAL';
  if (req.auth && req.auth.orgId) {
    const org = orgStore.getOrg(req.auth.orgId);
    if (org && org.cluster && org.cluster.coordinates && org.cluster.coordinates[0]) {
      const coords = org.cluster.coordinates[0];
      let latSum = 0, lngSum = 0;
      for (const [lng, lat] of coords) { latSum += lat; lngSum += lng; }
      const centerLat = latSum / coords.length;
      const centerLng = lngSum / coords.length;
      for (const [code, r] of Object.entries(config.regions)) {
        if (code === 'GLOBAL' || !r.bbox) continue;
        if (centerLat >= r.bbox.latMin && centerLat <= r.bbox.latMax &&
            centerLng >= r.bbox.lngMin && centerLng <= r.bbox.lngMax) {
          current = code;
          break;
        }
      }
    }
  }
  // Fallback to env var if no org cluster
  if (current === 'GLOBAL' && config.agent.region !== 'GLOBAL') {
    current = config.agent.region;
  }
  res.json({ ok: true, data: config.regions, current: current });
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
  TAC_LAC_JUMP: { enabled: true, minTacChanges: 2 },
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

  // License enforcement — org can only use modes their license allows
  if (req.auth && req.auth.orgId) {
    const org = orgStore.getOrg(req.auth.orgId);
    if (org && org.license !== 'BOTH') {
      if (newMode !== org.license) {
        return res.status(403).json({ ok: false, error: `Your license only allows ${org.license} mode` });
      }
    }
  }

  deploymentMode = newMode;
  console.log(`[MODE] ${req.auth.email} switched to ${deploymentMode}`);
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
// API: Admin — super_admin only (org/user CRUD)
// Auth + role check applied via app.use('/api/admin', ...) above
// ---------------------------------------------------------------------------

// --- Impersonation: super admin views SOC as another org ---
app.post('/api/admin/impersonate/:orgId', safeRoute(async (req, res) => {
  const org = orgStore.getOrg(req.params.orgId);
  if (!org) return res.status(404).json({ ok: false, error: 'Org not found' });

  const impersonationToken = jwt.sign({
    userId: req.auth.userId,
    email: req.auth.email,
    orgId: org.id,
    role: 'super_admin',
    license: org.license,
    impersonating: true,
    originalOrgId: req.auth.originalOrgId || req.auth.orgId,
  }, JWT_SECRET, { expiresIn: '4h' });

  console.log(`[ADMIN] ${req.auth.email} impersonating org: ${org.name}`);
  res.json({
    ok: true,
    data: {
      token: impersonationToken,
      org: { id: org.id, name: org.name, logo_url: org.logo_url, license: org.license, cluster: org.cluster, sub_clusters: org.sub_clusters, demo_mode: org.demo_mode },
    },
  });
}));

app.post('/api/admin/stop-impersonation', safeRoute(async (req, res) => {
  const originalOrgId = req.auth.originalOrgId || req.auth.orgId;
  const org = orgStore.getOrg(originalOrgId);
  const user = orgStore.getUser(req.auth.userId);
  if (!user || !org) return res.status(400).json({ ok: false, error: 'Could not restore original context' });

  const token = signToken(user, org);
  console.log(`[ADMIN] ${req.auth.email} stopped impersonation, back to ${org.name}`);
  res.json({
    ok: true,
    data: {
      token,
      org: { id: org.id, name: org.name, license: org.license, logo_url: org.logo_url, cluster: org.cluster, sub_clusters: org.sub_clusters, demo_mode: org.demo_mode },
    },
  });
}));

// --- Orgs ---
app.get('/api/admin/orgs', (req, res) => {
  const orgs = orgStore.getOrgs();
  res.json({ ok: true, data: orgs });
});

app.post('/api/admin/orgs', (req, res) => {
  const org = orgStore.createOrg(req.body);
  res.json({ ok: true, data: org });
});

app.put('/api/admin/orgs/:orgId', (req, res) => {
  const org = orgStore.updateOrg(req.params.orgId, req.body);
  if (!org) return res.status(404).json({ ok: false, error: 'Org not found' });
  res.json({ ok: true, data: org });
});

app.delete('/api/admin/orgs/:orgId', (req, res) => {
  const ok = orgStore.deleteOrg(req.params.orgId);
  if (!ok) return res.status(404).json({ ok: false, error: 'Org not found' });
  res.json({ ok: true });
});

// --- Users ---
app.get('/api/admin/users', (req, res) => {
  const orgId = req.query.org_id || null;
  const users = orgStore.sanitizeUsers(orgStore.getUsers(orgId));
  res.json({ ok: true, data: users });
});

app.post('/api/admin/users', async (req, res) => {
  try {
    const user = await orgStore.createUser(req.body);
    res.json({ ok: true, data: user });
  } catch (err) {
    res.status(400).json({ ok: false, error: err.message });
  }
});

app.put('/api/admin/users/:userId', async (req, res) => {
  const user = await orgStore.updateUser(req.params.userId, req.body);
  if (!user) return res.status(404).json({ ok: false, error: 'User not found' });
  res.json({ ok: true, data: user });
});

app.delete('/api/admin/users/:userId', (req, res) => {
  const ok = orgStore.deleteUser(req.params.userId);
  if (!ok) return res.status(404).json({ ok: false, error: 'User not found' });
  res.json({ ok: true });
});

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

module.exports = app;
module.exports.app = app;
module.exports.startServer = startServer;

// Auto-start if run directly (node server.js)
if (require.main === module) {
  startServer();
}
