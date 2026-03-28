const { createClient } = require('@clickhouse/client');
const config = require('./config');

let client = null;

function getClient() {
  if (!client) {
    client = createClient({
      url: `https://${config.clickhouse.host}:${config.clickhouse.port}`,
      database: config.clickhouse.database,
      username: config.clickhouse.username,
      password: config.clickhouse.password,
      request_timeout: 300000,
      max_open_connections: 1,
      keep_alive: {
        enabled: false,
      },
      clickhouse_settings: {
        connect_timeout: 60,
        receive_timeout: 300,
        send_timeout: 300,
      },
    });

    console.log(`[DB] Connecting to https://${config.clickhouse.host}:${config.clickhouse.port} as ${config.clickhouse.username}`);
  }
  return client;
}

/**
 * Force-reset the client — used when connection settings change at runtime.
 */
function resetClient() {
  if (client) {
    try { client.close(); } catch (_) {}
  }
  client = null;
  console.log('[DB] Client reset — will reconnect with current config on next query');
}

// ---------------------------------------------------------------------------
// Retry wrapper for transient ClickHouse errors (socket hang up, ECONNRESET)
// ---------------------------------------------------------------------------
async function queryWithRetry(queryOpts, retries = 2) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await getClient().query(queryOpts);
    } catch (err) {
      const isTransient = err.code === 'ECONNRESET' || err.code === 'EAI_AGAIN' ||
        err.code === 'ECONNABORTED' || err.code === 'ETIMEDOUT' ||
        (err.message && (err.message.includes('socket hang up') ||
         err.message.includes('ECONNRESET') || err.message.includes('aborted') ||
         err.message.includes('Timeout') || err.message.includes('TLS') ||
         err.message.includes('disconnected')));
      if (isTransient && attempt < retries) {
        console.log(`[DB] Transient error (attempt ${attempt + 1}/${retries + 1}), retrying in ${attempt + 1}s: ${err.message}`);
        // Force new client on retry — old connections may be stale
        client = null;
        await new Promise(r => setTimeout(r, 1000 * (attempt + 1)));
        continue;
      }
      throw err;
    }
  }
}

// ---------------------------------------------------------------------------
// Health check
// ---------------------------------------------------------------------------
async function healthCheck() {
  const rs = await queryWithRetry({ query: 'SELECT 1 AS ok', format: 'JSONEachRow' });
  const rows = await rs.json();
  return rows.length > 0;
}

// ---------------------------------------------------------------------------
// Schema discovery — log sites table columns at startup for diagnostics
// ---------------------------------------------------------------------------
async function discoverSitesSchema() {
  try {
    const rs = await queryWithRetry({ query: 'DESCRIBE TABLE sites', format: 'JSONEachRow' });
    const cols = await rs.json();
    const colNames = cols.map(c => c.name);
    console.log(`[DB] Sites table columns (${colNames.length}): ${colNames.join(', ')}`);
    return colNames;
  } catch (e) {
    console.error('[DB] Failed to discover sites schema:', e.message);
    return [];
  }
}

// ---------------------------------------------------------------------------
// Watermark management
// ---------------------------------------------------------------------------
async function getLastWatermark() {
  const rs = await queryWithRetry({
    query: 'SELECT last_processed_ts FROM agent_state ORDER BY updated_at DESC LIMIT 1',
    format: 'JSONEachRow',
  });
  const rows = await rs.json();
  if (rows.length === 0) return null;
  return rows[0].last_processed_ts;
}

async function updateWatermark(ts) {
  await getClient().insert({
    table: 'agent_state',
    values: [{ last_processed_ts: ts, updated_at: new Date().toISOString() }],
    format: 'JSONEachRow',
  });
}

// ---------------------------------------------------------------------------
// Measurements — CORRECT column names from DESCRIBE TABLE
// ---------------------------------------------------------------------------
// Key column mappings (actual schema):
//   cell identifier:  cell_pci (UInt16), cell_eci, cell_ecgi, cell_enb, cell_nci, cell_cid
//   PLMN:             network_PLMN (String "220-03"), network_mcc (UInt16), network_mnc (UInt8)
//   RAT/tech:         tech (String: "LTE", "WCDMA", "NR", "GSM")
//   SINR:             signal_snr (NOT signal_sinr)
//   roaming:          network_isRoaming (Bool)
//   device ID:        deviceInfo_deviceId (String, always populated)
//   band freq:        band_downlinkFrequency, band_downlinkEarfcn, band_channelNumber
// ---------------------------------------------------------------------------
const MEASUREMENT_COLUMNS = [
  'timestamp', 'cell_pci', 'cell_eci', 'cell_ecgi', 'cell_tac', 'cell_enb',
  'network_PLMN', 'network_mcc', 'network_mnc', 'network_iso',
  'tech', 'signal_rsrp', 'signal_rssi',
  'signal_rsrq', 'signal_snr', 'signal_timingAdvance', 'signal_txPower',
  'band_downlinkFrequency', 'band_downlinkEarfcn', 'band_channelNumber',
  'band_name', 'band_number',
  'location_lat_rounded', 'location_lng_rounded',
  'deviceInfo_deviceId', 'deviceInfo_deviceModel',
  'network_isRoaming', 'network_operator',
  'sample_id', 'connectionStatus', 'isRegistered', 'source', 'neighborNo',
].join(', ');

async function getNewMeasurements(sinceTs, limit) {
  const rs = await queryWithRetry({
    query: `SELECT ${MEASUREMENT_COLUMNS}
            FROM measurements
            WHERE timestamp > {since_ts:String}
            ORDER BY timestamp ASC
            LIMIT {limit:UInt32}`,
    query_params: { since_ts: sinceTs, limit },
    format: 'JSONEachRow',
  });
  return rs.json();
}

// ---------------------------------------------------------------------------
// Cell baselines (24h rolling averages)
// ---------------------------------------------------------------------------
async function getCellBaselines(cellIds) {
  if (!cellIds || cellIds.length === 0) return {};

  // Use cell_pci as the cell identifier (most commonly populated)
  const rs = await queryWithRetry({
    query: `SELECT
              cell_pci,
              avg(signal_rsrp)            AS avg_rsrp,
              stddevPop(signal_rsrp)      AS std_rsrp,
              avg(signal_rssi)            AS avg_rssi,
              stddevPop(signal_rssi)      AS std_rssi,
              avg(signal_snr)             AS avg_snr,
              stddevPop(signal_snr)       AS std_snr,
              avg(signal_timingAdvance)   AS avg_ta,
              stddevPop(signal_timingAdvance) AS std_ta
            FROM measurements
            WHERE toString(cell_pci) IN ({cell_ids:Array(String)})
              AND timestamp > now() - INTERVAL 24 HOUR
            GROUP BY cell_pci`,
    query_params: { cell_ids: cellIds },
    format: 'JSONEachRow',
  });
  const rows = await rs.json();
  const map = {};
  for (const r of rows) {
    map[String(r.cell_pci)] = r;
  }
  return map;
}

// ---------------------------------------------------------------------------
// Known / whitelisted cells
// ---------------------------------------------------------------------------
/**
 * Load known cell sites, optionally filtered by region bbox.
 * IMPORTANT: The sites table has bad data — entries with Israeli PLMNs but
 * GPS coordinates in Singapore, Pacific, etc. These cause massive false positives
 * in the rule engine (7000+km "GPS spoofing" detections). Filtering by bbox
 * eliminates these bogus entries.
 *
 * @param {Object} [bbox] - { latMin, latMax, lngMin, lngMax } — expanded by 3° margin
 */
async function getKnownCells(bbox) {
  let whereClause = '';
  const params = {};

  if (bbox) {
    // Use a modest margin (1°) to include towers near borders
    // but exclude foreign towers in neighboring countries (Cyprus, Lebanon, etc.)
    // which cause eNB ID collisions with local towers.
    const margin = 1.0;
    whereClause = `WHERE lat >= {latMin:Float64}
                     AND lat <= {latMax:Float64}
                     AND lng >= {lngMin:Float64}
                     AND lng <= {lngMax:Float64}
                     AND lat != 0 AND lng != 0`;
    params.latMin = bbox.latMin - margin;
    params.latMax = bbox.latMax + margin;
    params.lngMin = bbox.lngMin - margin;
    params.lngMax = bbox.lngMax + margin;
  }

  const rs = await queryWithRetry({
    query: `SELECT *
            FROM sites
            ${whereClause}`,
    query_params: params,
    format: 'JSONEachRow',
  });
  const rows = await rs.json();
  // Log column names from first row to help identify PLMN column
  if (rows.length > 0) {
    const cols = Object.keys(rows[0]);
    console.log(`[DB] Sites row columns: ${cols.join(', ')}`);
    // Log first row for diagnostics
    const sample = rows[0];
    console.log(`[DB] Sites sample: site_id=${sample.site_id}, lat=${sample.lat}, lng=${sample.lng}, plmn=${sample.plmn || sample.network_plmn || sample.cell_plmn || sample.mcc_mnc || 'N/A'}`);
  }
  return rows;
}

// ---------------------------------------------------------------------------
// Bad measurements integration
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Diagnostic: search for a measurement by lat/lng in both tables
// ---------------------------------------------------------------------------
async function searchMeasurementByLocation(lat, lng, radiusDeg = 0.01, hoursBack = 48) {
  const results = { measurements: [], bad_measurements: [] };

  // Search measurements table
  try {
    const rs = await queryWithRetry({
      query: `SELECT timestamp, sample_id, cell_pci, cell_enb, cell_eci, cell_ecgi,
                     network_PLMN, network_mcc, network_mnc, tech, signal_rsrp, signal_snr,
                     signal_timingAdvance, band_downlinkEarfcn, band_number,
                     location_lat_rounded, location_lng_rounded,
                     network_operator, network_isRoaming, deviceInfo_deviceId,
                     connectionStatus, isRegistered
              FROM measurements
              WHERE timestamp > now() - INTERVAL {hours:UInt32} HOUR
                AND location_lat_rounded >= {latMin:Float64}
                AND location_lat_rounded <= {latMax:Float64}
                AND location_lng_rounded >= {lngMin:Float64}
                AND location_lng_rounded <= {lngMax:Float64}
              ORDER BY timestamp DESC
              LIMIT 50`,
      query_params: {
        hours: hoursBack,
        latMin: lat - radiusDeg,
        latMax: lat + radiusDeg,
        lngMin: lng - radiusDeg,
        lngMax: lng + radiusDeg,
      },
      format: 'JSONEachRow',
    });
    results.measurements = await rs.json();
  } catch (e) {
    console.error('[DB] searchMeasurementByLocation (measurements):', e.message);
  }

  // Search bad_measurements table using JSONExtract for precise coordinate matching
  try {
    const rs = await queryWithRetry({
      query: `SELECT timestamp, sample_id, reason,
                     substring(raw_record, 1, 500) AS raw_snippet,
                     JSONExtractFloat(raw_record, 'location', 'geo', 'coordinates', 2) AS geo_lat,
                     JSONExtractFloat(raw_record, 'location', 'geo', 'coordinates', 1) AS geo_lng
              FROM bad_measurements
              WHERE timestamp > now() - INTERVAL {hours:UInt32} HOUR
                AND raw_record != '' AND length(raw_record) > 10
                AND (
                  (
                    JSONExtractFloat(raw_record, 'location', 'geo', 'coordinates', 2) BETWEEN {latMin:Float64} AND {latMax:Float64}
                    AND JSONExtractFloat(raw_record, 'location', 'geo', 'coordinates', 1) BETWEEN {lngMin:Float64} AND {lngMax:Float64}
                  )
                  OR
                  (
                    JSONHas(raw_record, 'location', 'tileId_1') = 1
                    AND toFloat64OrNull(splitByChar(',', JSONExtractString(raw_record, 'location', 'tileId_1'))[2])
                        BETWEEN {latMin:Float64} AND {latMax:Float64}
                    AND toFloat64OrNull(splitByChar(',', JSONExtractString(raw_record, 'location', 'tileId_1'))[1])
                        BETWEEN {lngMin:Float64} AND {lngMax:Float64}
                  )
                )
              ORDER BY timestamp DESC
              LIMIT 50`,
      query_params: {
        hours: hoursBack,
        latMin: lat - radiusDeg,
        latMax: lat + radiusDeg,
        lngMin: lng - radiusDeg,
        lngMax: lng + radiusDeg,
      },
      format: 'JSONEachRow',
    });
    results.bad_measurements = await rs.json();
  } catch (e) {
    console.error('[DB] searchMeasurementByLocation (bad_measurements):', e.message);
  }

  return results;
}

async function getBadMeasurementPatterns() {
  const rs = await queryWithRetry({
    query: `SELECT reason,
                   COUNT(*)              AS count,
                   groupArray(sample_id) AS sample_ids
            FROM bad_measurements
            GROUP BY reason
            ORDER BY count DESC`,
    format: 'JSONEachRow',
  });
  return rs.json();
}

async function checkAgainstBadMeasurements(sampleIds) {
  if (!sampleIds || sampleIds.length === 0) return [];

  const rs = await queryWithRetry({
    query: `SELECT sample_id, reason, raw_record, timestamp
            FROM bad_measurements
            WHERE sample_id IN ({ids:Array(String)})`,
    query_params: { ids: sampleIds },
    format: 'JSONEachRow',
  });
  return rs.json();
}

async function getRecentBadMeasurements(hours = 24) {
  const rs = await queryWithRetry({
    query: `SELECT id, timestamp, sample_id, createdAt, reason
            FROM bad_measurements
            WHERE timestamp > now() - INTERVAL {hours:UInt32} HOUR
            ORDER BY timestamp DESC`,
    query_params: { hours },
    format: 'JSONEachRow',
  });
  return rs.json();
}

// ---------------------------------------------------------------------------
// Threat events
// ---------------------------------------------------------------------------
async function writeThreatEvents(threats) {
  if (!threats || threats.length === 0) return;

  const rows = threats.map((t) => ({
    cell_id: t.cell_id || '',
    cell_ecgi: t.cell_ecgi || '',
    location_lat: t.location_lat || 0,
    location_lng: t.location_lng || 0,
    threat_type: t.threat_type || 'UNKNOWN',
    severity: t.severity || 'MEDIUM',
    score: t.score || 0,
    confidence: t.confidence || 0,
    reasoning: t.reasoning || '',
    sample_id: t.sample_id || '',
    raw_flags: t.raw_flags || [],
    is_confirmed: t.is_confirmed !== undefined ? t.is_confirmed : 1,
  }));

  await getClient().insert({
    table: 'threat_events',
    values: rows,
    format: 'JSONEachRow',
  });
}

// ---------------------------------------------------------------------------
// Alert log
// ---------------------------------------------------------------------------
async function writeAlertLog(entry) {
  await getClient().insert({
    table: 'alert_log',
    values: [entry],
    format: 'JSONEachRow',
  });
}

// ---------------------------------------------------------------------------
// Dashboard / API queries
// ---------------------------------------------------------------------------
async function getRecentThreats(limit = 100) {
  const rs = await queryWithRetry({
    query: `SELECT *
            FROM threat_events
            ORDER BY detected_at DESC
            LIMIT {limit:UInt32}`,
    query_params: { limit },
    format: 'JSONEachRow',
  });
  return rs.json();
}

async function getThreatStats() {
  const rs = await queryWithRetry({
    query: `SELECT
              severity,
              COUNT(*) AS count
            FROM threat_events
            WHERE detected_at > now() - INTERVAL 24 HOUR
            GROUP BY severity
            ORDER BY count DESC`,
    format: 'JSONEachRow',
  });
  const rows = await rs.json();
  return rows;
}

async function getBadMeasurementStats() {
  const rs = await queryWithRetry({
    query: `SELECT reason, COUNT(*) AS count
            FROM bad_measurements
            GROUP BY reason
            ORDER BY count DESC`,
    format: 'JSONEachRow',
  });
  return rs.json();
}

async function getRecentBadMeasurementsForDashboard(limit = 50) {
  const rs = await queryWithRetry({
    query: `SELECT id, timestamp, sample_id, reason
            FROM bad_measurements
            WHERE timestamp > now() - INTERVAL 24 HOUR
            ORDER BY timestamp DESC
            LIMIT {limit:UInt32}`,
    query_params: { limit },
    format: 'JSONEachRow',
  });
  return rs.json();
}

// ---------------------------------------------------------------------------
// Dashboard: Measurements overview
// ---------------------------------------------------------------------------
async function getMeasurementStats() {
  // No access to system.parts — use count() directly (cached well by ClickHouse)
  const countRs = await queryWithRetry({
    query: `SELECT count() AS total FROM measurements`,
    format: 'JSONEachRow',
  });
  const countRows = await countRs.json();
  const totalEstimate = countRows[0]?.total || 0;

  // Get recent stats from last 7 days only (much faster than full scan)
  const rs = await queryWithRetry({
    query: `SELECT
              countDistinct(cell_pci)              AS unique_cells,
              countDistinct(deviceInfo_deviceId)    AS unique_devices,
              min(timestamp)                        AS earliest,
              max(timestamp)                        AS latest
            FROM measurements
            WHERE timestamp > now() - INTERVAL 7 DAY`,
    format: 'JSONEachRow',
  });
  const rows = await rs.json();
  const row = rows[0] || {};
  row.total_measurements = totalEstimate;
  return row;
}

async function getMeasurementStats24h() {
  const rs = await queryWithRetry({
    query: `SELECT
              count()                                          AS measurements_24h,
              countDistinct(cell_pci)                           AS active_cells_24h,
              countDistinct(deviceInfo_deviceId)                AS active_devices_24h,
              countIf(tech IN ('GSM','EDGE','GPRS'))            AS downgrade_count,
              avg(signal_rsrp)                                  AS avg_rsrp,
              avg(signal_snr)                                   AS avg_snr
            FROM measurements
            WHERE timestamp > now() - INTERVAL 24 HOUR`,
    format: 'JSONEachRow',
  });
  const rows = await rs.json();
  return rows[0] || {};
}

async function getRecentMeasurements(limit = 50) {
  // MUST filter by time window — ORDER BY on 838M+ rows without WHERE causes timeout
  const rs = await queryWithRetry({
    query: `SELECT timestamp, cell_pci, cell_eci, cell_ecgi, cell_enb, cell_tac,
                   network_PLMN, network_mcc, network_mnc, network_iso,
                   tech, signal_rsrp, signal_rssi, signal_snr, signal_timingAdvance, signal_txPower,
                   band_downlinkEarfcn, band_downlinkFrequency, band_name, band_number,
                   location_lat_rounded, location_lng_rounded,
                   network_isRoaming, network_operator,
                   sample_id, deviceInfo_deviceId, deviceInfo_deviceModel,
                   connectionStatus, isRegistered, source
            FROM measurements
            WHERE timestamp > now() - INTERVAL 1 HOUR
            ORDER BY timestamp DESC
            LIMIT {limit:UInt32}`,
    query_params: { limit },
    format: 'JSONEachRow',
  });
  return rs.json();
}

async function getRatDistribution() {
  const rs = await queryWithRetry({
    query: `SELECT tech, count() AS count
            FROM measurements
            WHERE timestamp > now() - INTERVAL 24 HOUR
            GROUP BY tech
            ORDER BY count DESC`,
    format: 'JSONEachRow',
  });
  return rs.json();
}

// ---------------------------------------------------------------------------
// Dashboard: Sites overview
// ---------------------------------------------------------------------------
async function getSiteStats() {
  const rs = await queryWithRetry({
    query: `SELECT
              count()              AS total_sites,
              countDistinct(tech)  AS tech_types
            FROM sites`,
    format: 'JSONEachRow',
  });
  const rows = await rs.json();
  return rows[0] || {};
}

async function getSitesByTech() {
  const rs = await queryWithRetry({
    query: `SELECT tech, count() AS count
            FROM sites
            GROUP BY tech
            ORDER BY count DESC`,
    format: 'JSONEachRow',
  });
  return rs.json();
}

// ---------------------------------------------------------------------------
// Dashboard: Site samples overview
// ---------------------------------------------------------------------------
async function getSiteSampleStats() {
  const rs = await queryWithRetry({
    query: `SELECT count() AS total_site_samples
            FROM site_samples`,
    format: 'JSONEachRow',
  });
  const rows = await rs.json();
  return rows[0] || {};
}

// ---------------------------------------------------------------------------
// Dashboard: MCC distribution (country code analysis)
// Uses network_mcc (numeric) directly — no string parsing needed
// ---------------------------------------------------------------------------
async function getMCCDistribution(bbox) {
  let whereClause = 'WHERE timestamp > now() - INTERVAL 24 HOUR';
  const params = {};
  if (bbox) {
    whereClause += ` AND location_lat_rounded >= {latMin:Float64}
                     AND location_lat_rounded <= {latMax:Float64}
                     AND location_lng_rounded >= {lngMin:Float64}
                     AND location_lng_rounded <= {lngMax:Float64}`;
    params.latMin = bbox.latMin;
    params.latMax = bbox.latMax;
    params.lngMin = bbox.lngMin;
    params.lngMax = bbox.lngMax;
  }

  const rs = await queryWithRetry({
    query: `SELECT
              lpad(toString(network_mcc), 3, '0') AS mcc,
              network_PLMN,
              network_iso,
              network_operator,
              count() AS count
            FROM measurements
            ${whereClause}
              AND network_mcc > 0
            GROUP BY mcc, network_PLMN, network_iso, network_operator
            ORDER BY count DESC
            LIMIT 50`,
    query_params: params,
    format: 'JSONEachRow',
  });
  return rs.json();
}

async function getRegionMeasurementStats(bbox) {
  if (!bbox) return null;
  const rs = await queryWithRetry({
    query: `SELECT
              count()                                          AS total,
              countDistinct(cell_pci)                           AS unique_cells,
              countDistinct(deviceInfo_deviceId)                AS unique_devices,
              countIf(tech IN ('GSM','EDGE','GPRS'))            AS downgrade_count
            FROM measurements
            WHERE timestamp > now() - INTERVAL 24 HOUR
              AND location_lat_rounded >= {latMin:Float64}
              AND location_lat_rounded <= {latMax:Float64}
              AND location_lng_rounded >= {lngMin:Float64}
              AND location_lng_rounded <= {lngMax:Float64}`,
    query_params: {
      latMin: bbox.latMin, latMax: bbox.latMax,
      lngMin: bbox.lngMin, lngMax: bbox.lngMax,
    },
    format: 'JSONEachRow',
  });
  const rows = await rs.json();
  return rows[0] || {};
}

async function getRecentMeasurementsFiltered(bbox, limit = 50, hoursBack = 24) {
  // Always filter by time to avoid full-table scan on 838M+ rows
  let whereClause = `WHERE timestamp > now() - INTERVAL ${Math.round(hoursBack)} HOUR`;
  const params = { limit };
  if (bbox) {
    whereClause += ` AND location_lat_rounded >= {latMin:Float64}
                     AND location_lat_rounded <= {latMax:Float64}
                     AND location_lng_rounded >= {lngMin:Float64}
                     AND location_lng_rounded <= {lngMax:Float64}`;
    params.latMin = bbox.latMin;
    params.latMax = bbox.latMax;
    params.lngMin = bbox.lngMin;
    params.lngMax = bbox.lngMax;
  }

  const rs = await queryWithRetry({
    query: `SELECT timestamp, cell_pci, cell_eci, cell_ecgi, cell_enb, cell_tac,
                   network_PLMN, network_mcc, network_mnc, network_iso,
                   tech, signal_rsrp, signal_rssi, signal_snr, signal_timingAdvance, signal_txPower,
                   band_downlinkEarfcn, band_downlinkFrequency, band_name, band_number,
                   location_lat_rounded, location_lng_rounded,
                   network_isRoaming, network_operator,
                   sample_id, deviceInfo_deviceId, deviceInfo_deviceModel,
                   connectionStatus, isRegistered, source
            FROM measurements
            ${whereClause}
            ORDER BY timestamp DESC
            LIMIT {limit:UInt32}`,
    query_params: params,
    format: 'JSONEachRow',
  });
  return rs.json();
}

// ---------------------------------------------------------------------------
// Anomaly-focused queries — scan wider windows for specific indicators
// ---------------------------------------------------------------------------

/**
 * Get measurements with unexpected MCC inside the region bbox.
 * This finds foreign network codes appearing within your monitored area
 * (e.g. MCC 525 Singapore showing up in Israel = suspicious).
 * Requires bbox to avoid flagging legitimate measurements from other countries.
 */
async function getMCCAnomalyMeasurements(expectedMCCs, bbox, limit = 100, hoursBack = 24) {
  if (!expectedMCCs || expectedMCCs.length === 0) return [];
  if (!bbox) return []; // Must have bbox — without it we'd flag every non-local measurement worldwide

  const expectedNumeric = expectedMCCs.map(m => parseInt(m, 10));

  const rs = await queryWithRetry({
    query: `SELECT timestamp, cell_pci, cell_eci, cell_ecgi, cell_enb, cell_tac,
                   network_PLMN, network_mcc, network_mnc, network_iso,
                   tech, signal_rsrp, signal_rssi, signal_snr, signal_timingAdvance, signal_txPower,
                   band_downlinkEarfcn, band_downlinkFrequency, band_name, band_number,
                   location_lat_rounded, location_lng_rounded,
                   network_isRoaming, network_operator,
                   sample_id, deviceInfo_deviceId, deviceInfo_deviceModel,
                   connectionStatus, isRegistered, source
            FROM measurements
            WHERE timestamp > now() - INTERVAL {hours:UInt32} HOUR
              AND network_mcc NOT IN ({expected_numeric:Array(UInt16)})
              AND network_mcc > 0
              AND location_lat_rounded >= {latMin:Float64}
              AND location_lat_rounded <= {latMax:Float64}
              AND location_lng_rounded >= {lngMin:Float64}
              AND location_lng_rounded <= {lngMax:Float64}
            ORDER BY timestamp DESC
            LIMIT {limit:UInt32}`,
    query_params: {
      expected_numeric: expectedNumeric, limit,
      hours: Math.round(hoursBack),
      latMin: bbox.latMin, latMax: bbox.latMax,
      lngMin: bbox.lngMin, lngMax: bbox.lngMax,
    },
    format: 'JSONEachRow',
  });
  return rs.json();
}

/**
 * Dedicated search for MCC 001 (Test Network) — ALWAYS critical.
 * Test network PLMN should never appear on real devices in production.
 * Searches globally (no bbox) with 7-day window because:
 *   - Test networks are extremely rare and always suspicious
 *   - Location may be imprecise or spoofed
 *   - We don't want to miss any occurrence
 * Also searches by PCI+EARFCN combo the user reported (PCI 31, EARFCN 9580).
 */
async function getTestNetworkMeasurements(limit = 200, hoursBack = 168) {
  const rs = await queryWithRetry({
    query: `SELECT timestamp, cell_pci, cell_eci, cell_ecgi, cell_enb, cell_tac,
                   network_PLMN, network_mcc, network_mnc, network_iso,
                   tech, signal_rsrp, signal_rssi, signal_snr, signal_timingAdvance, signal_txPower,
                   band_downlinkEarfcn, band_downlinkFrequency, band_name, band_number,
                   location_lat_rounded, location_lng_rounded,
                   network_isRoaming, network_operator,
                   sample_id, deviceInfo_deviceId, deviceInfo_deviceModel,
                   connectionStatus, isRegistered, source
            FROM measurements
            WHERE timestamp > now() - INTERVAL {hours:UInt32} HOUR
              AND (
                network_mcc = 1
                OR network_PLMN LIKE '001%'
                OR network_PLMN LIKE '001-%'
              )
            ORDER BY timestamp DESC
            LIMIT {limit:UInt32}`,
    query_params: { limit, hours: Math.round(hoursBack) },
    format: 'JSONEachRow',
  });
  return rs.json();
}

/** Get measurements with TA=0 or TA=1 (potential IMSI catcher proximity indicator) */
async function getTAZeroMeasurements(bbox, limit = 500, hoursBack = 24) {
  let whereClause = `WHERE timestamp > now() - INTERVAL ${Math.round(hoursBack)} HOUR
    AND signal_timingAdvance <= 1
    AND signal_timingAdvance >= 0`;
  const params = { limit };

  if (bbox) {
    whereClause += ` AND location_lat_rounded >= {latMin:Float64}
                     AND location_lat_rounded <= {latMax:Float64}
                     AND location_lng_rounded >= {lngMin:Float64}
                     AND location_lng_rounded <= {lngMax:Float64}`;
    params.latMin = bbox.latMin;
    params.latMax = bbox.latMax;
    params.lngMin = bbox.lngMin;
    params.lngMax = bbox.lngMax;
  }

  const rs = await queryWithRetry({
    query: `SELECT timestamp, cell_pci, cell_eci, cell_ecgi, cell_enb, cell_tac,
                   network_PLMN, network_mcc, network_mnc, network_iso,
                   tech, signal_rsrp, signal_rssi, signal_snr, signal_timingAdvance, signal_txPower,
                   band_downlinkEarfcn, band_downlinkFrequency, band_name, band_number,
                   location_lat_rounded, location_lng_rounded,
                   network_isRoaming, network_operator,
                   sample_id, deviceInfo_deviceId, deviceInfo_deviceModel,
                   connectionStatus, isRegistered, source
            FROM measurements
            ${whereClause}
            ORDER BY timestamp DESC
            LIMIT {limit:UInt32}`,
    query_params: params,
    format: 'JSONEachRow',
  });
  return rs.json();
}

/** Get 2G downgrade measurements (GSM/EDGE/GPRS — no mutual auth) */
async function getDowngradeMeasurements(bbox, limit = 100, hoursBack = 24) {
  let whereClause = `WHERE timestamp > now() - INTERVAL ${Math.round(hoursBack)} HOUR
    AND tech IN ('GSM', 'EDGE', 'GPRS')`;
  const params = { limit };

  if (bbox) {
    whereClause += ` AND location_lat_rounded >= {latMin:Float64}
                     AND location_lat_rounded <= {latMax:Float64}
                     AND location_lng_rounded >= {lngMin:Float64}
                     AND location_lng_rounded <= {lngMax:Float64}`;
    params.latMin = bbox.latMin;
    params.latMax = bbox.latMax;
    params.lngMin = bbox.lngMin;
    params.lngMax = bbox.lngMax;
  }

  const rs = await queryWithRetry({
    query: `SELECT timestamp, cell_pci, cell_eci, cell_ecgi, cell_enb, cell_tac,
                   network_PLMN, network_mcc, network_mnc, network_iso,
                   tech, signal_rsrp, signal_rssi, signal_snr, signal_timingAdvance, signal_txPower,
                   band_downlinkEarfcn, band_downlinkFrequency, band_name, band_number,
                   location_lat_rounded, location_lng_rounded,
                   network_isRoaming, network_operator,
                   sample_id, deviceInfo_deviceId, deviceInfo_deviceModel,
                   connectionStatus, isRegistered, source
            FROM measurements
            ${whereClause}
            ORDER BY timestamp DESC
            LIMIT {limit:UInt32}`,
    query_params: params,
    format: 'JSONEachRow',
  });
  return rs.json();
}

/**
 * Get measurements with GPS location anomalies — THREE approaches:
 *
 *  1) GPS outside region bbox — clear signal (MCC 425 but GPS outside Israel) — 96h
 *  2) GPS spoofing via ClickHouse JOIN — measurements where GPS is >5km from the
 *     known cell tower (matched by eNB ID via the sites table). Uses greatCircleDistance().
 *     Goes back 7 days. This is the KEY detector — catches "GPS at sea but technically
 *     inside bbox" because it compares GPS to actual tower location, not bbox edges.
 *  3) Fallback outside-bbox catches the rest
 */
async function getLocationAnomalyMeasurements(expectedMCCs, bbox, limit = 200, hoursBack = 96) {
  if (!expectedMCCs || expectedMCCs.length === 0 || !bbox) return [];
  const expectedNumeric = expectedMCCs.map(m => parseInt(m, 10));

  // Prong 1: GPS coordinates fall OUTSIDE the region bbox (96h)
  const outsideBboxPromise = queryWithRetry({
    query: `SELECT timestamp, cell_pci, cell_eci, cell_ecgi, cell_enb, cell_tac,
                   network_PLMN, network_mcc, network_mnc, network_iso,
                   tech, signal_rsrp, signal_rssi, signal_snr, signal_timingAdvance, signal_txPower,
                   band_downlinkEarfcn, band_downlinkFrequency, band_name, band_number,
                   location_lat_rounded, location_lng_rounded,
                   network_isRoaming, network_operator,
                   sample_id, deviceInfo_deviceId, deviceInfo_deviceModel,
                   connectionStatus, isRegistered, source
            FROM measurements
            WHERE timestamp > now() - INTERVAL {hours:UInt32} HOUR
              AND network_mcc IN ({expected_mccs:Array(UInt16)})
              AND location_lat_rounded != 0
              AND location_lng_rounded != 0
              AND (
                location_lat_rounded < {latMin:Float64}
                OR location_lat_rounded > {latMax:Float64}
                OR location_lng_rounded < {lngMin:Float64}
                OR location_lng_rounded > {lngMax:Float64}
              )
            ORDER BY timestamp DESC
            LIMIT {limit:UInt32}`,
    query_params: {
      expected_mccs: expectedNumeric,
      hours: Math.round(hoursBack),
      latMin: bbox.latMin,
      latMax: bbox.latMax,
      lngMin: bbox.lngMin,
      lngMax: bbox.lngMax,
      limit,
    },
    format: 'JSONEachRow',
  });

  // Prong 2: GPS SPOOFING — JOIN measurements with sites on eNB ID,
  // compute distance using ClickHouse's greatCircleDistance(),
  // return only measurements where GPS is >5km from the known cell tower.
  // Goes back 7 DAYS. Ordered by distance DESC (worst spoofing first).
  // This catches the "at sea" case — GPS inside bbox but far from tower.
  // IMPORTANT: Also filter sites to be within an expanded region bbox (+2°).
  // The sites table has some bad data (e.g. eNB entries with Israeli PLMN but
  // GPS in Singapore/Pacific). Without the site bbox filter, these bogus entries
  // create false positives with 8000+km distances.
  const siteMargin = 2.0; // degrees — allow sites slightly outside the strict bbox
  const gpsSpoofPromise = queryWithRetry({
    query: `
      SELECT m.timestamp, m.cell_pci, m.cell_eci, m.cell_ecgi, m.cell_enb, m.cell_tac,
             m.network_PLMN, m.network_mcc, m.network_mnc, m.network_iso,
             m.tech, m.signal_rsrp, m.signal_rssi, m.signal_snr, m.signal_timingAdvance, m.signal_txPower,
             m.band_downlinkEarfcn, m.band_downlinkFrequency, m.band_name, m.band_number,
             m.location_lat_rounded, m.location_lng_rounded,
             m.network_isRoaming, m.network_operator,
             m.sample_id, m.deviceInfo_deviceId, m.deviceInfo_deviceModel,
             m.connectionStatus, m.isRegistered
      FROM measurements m
      INNER JOIN sites s ON toUInt32(m.cell_enb) = s.site_id
      WHERE m.timestamp > now() - INTERVAL {hours:UInt32} HOUR
        AND m.network_mcc IN ({expected_mccs:Array(UInt16)})
        AND m.location_lat_rounded != 0
        AND m.location_lng_rounded != 0
        AND m.cell_enb != 0
        AND s.lat != 0
        AND s.lng != 0
        AND s.lat >= {siteLatMin:Float64}
        AND s.lat <= {siteLatMax:Float64}
        AND s.lng >= {siteLngMin:Float64}
        AND s.lng <= {siteLngMax:Float64}
        AND greatCircleDistance(m.location_lat_rounded, m.location_lng_rounded, s.lat, s.lng) > 5000
      ORDER BY greatCircleDistance(m.location_lat_rounded, m.location_lng_rounded, s.lat, s.lng) DESC
      LIMIT {gps_limit:UInt32}`,
    query_params: {
      expected_mccs: expectedNumeric,
      hours: Math.round(hoursBack),
      siteLatMin: bbox.latMin - siteMargin,
      siteLatMax: bbox.latMax + siteMargin,
      siteLngMin: bbox.lngMin - siteMargin,
      siteLngMax: bbox.lngMax + siteMargin,
      gps_limit: 500,
    },
    format: 'JSONEachRow',
  });

  const [outsideBboxResult, gpsSpoofResult] = await Promise.all([outsideBboxPromise, gpsSpoofPromise]);
  const outsideBbox = await outsideBboxResult.json();
  const gpsSpoof = await gpsSpoofResult.json();

  console.log(`[DB] Location anomalies: ${outsideBbox.length} outside-bbox(96h) + ${gpsSpoof.length} GPS-spoof-join(7d, >5km from tower)`);
  if (gpsSpoof.length > 0) {
    const top = gpsSpoof[0];
    const lat = top.location_lat_rounded;
    const lng = top.location_lng_rounded;
    console.log(`[DB] Top GPS spoof: eNB=${top.cell_enb} at GPS (${lat}, ${lng}) — sample ${(top.sample_id || '').substring(0, 24)}...`);
  }

  return [...outsideBbox, ...gpsSpoof];
}

// ---------------------------------------------------------------------------
// Debug: find specific measurements (MCC 001, specific PCI/EARFCN combos)
// ---------------------------------------------------------------------------
async function debugFindMCC001(daysBack = 7) {
  // Search multiple ways: by MCC value, by PCI+EARFCN, by PLMN string
  const rs = await queryWithRetry({
    query: `SELECT timestamp, cell_pci, cell_eci, cell_ecgi, cell_enb, cell_tac,
                   network_PLMN, network_mcc, network_mnc, network_iso,
                   tech, signal_rsrp, signal_rssi, signal_snr, signal_timingAdvance,
                   band_downlinkEarfcn, band_downlinkFrequency, band_name, band_number,
                   location_lat_rounded, location_lng_rounded,
                   network_isRoaming, network_operator,
                   sample_id, deviceInfo_deviceId, deviceInfo_deviceModel
            FROM measurements
            WHERE timestamp > now() - INTERVAL {days:UInt32} DAY
              AND (
                network_mcc IN (0, 1)
                OR network_PLMN LIKE '%001%'
                OR (cell_pci = 31 AND band_downlinkEarfcn = 9580)
              )
            ORDER BY timestamp DESC
            LIMIT 200`,
    query_params: { days: daysBack },
    format: 'JSONEachRow',
  });
  return rs.json();
}

/** Search bad_measurements for test network / MCC anomalies
 *  The Flycomm pipeline moves suspicious measurements to bad_measurements
 *  instead of measurements — MCC 001 ends up here! */
async function getBadMeasurementsWithRawData(daysBack = 7, limit = 500) {
  const rs = await queryWithRetry({
    query: `SELECT id, timestamp, sample_id, createdAt, reason, raw_record
            FROM bad_measurements
            WHERE timestamp > now() - INTERVAL {days:UInt32} DAY
            ORDER BY timestamp DESC
            LIMIT {limit:UInt32}`,
    query_params: { days: daysBack, limit },
    format: 'JSONEachRow',
  });
  return rs.json();
}

/**
 * Search bad_measurements raw_record JSON for RF anomaly indicators.
 * The Flycomm pipeline puts suspicious measurements here instead of the
 * measurements table. We need to mine these for:
 *  - MCC 001 (test network)
 *  - Any MCC not matching expected region
 *  - Specific PCI+EARFCN combos
 * Searches the raw_record string directly using ClickHouse JSON functions.
 */
async function getBadMeasurementsRFAnomalies(expectedMCCs, bbox, daysBack = 7, limit = 200) {
  // Strategy: use cheap string LIKE pre-filter to narrow rows before expensive JSONExtract.
  // MCC 001 (test network) appears as "mcc":1, or "mcc": 1 in JSON.
  // We look for test networks (MCC 0 or 1) and unexpected MCCs.
  //
  // Phase 1: fast string scan for MCC 001 / MCC 0 (always interesting)
  // Phase 2: if we have expected MCCs, also find unexpected ones (more targeted)

  const params = { days: daysBack, limit };

  // Build a LIKE-based pre-filter for test networks: "mcc":1, or "mcc": 1, or "mcc":0
  // This is very fast because ClickHouse can scan raw strings without JSON parsing
  let likeConditions = `(
    raw_record LIKE '%"mcc":1,%' OR raw_record LIKE '%"mcc": 1,%'
    OR raw_record LIKE '%"mcc":1}%' OR raw_record LIKE '%"mcc": 1}%'
    OR raw_record LIKE '%"mcc":0,%' OR raw_record LIKE '%"mcc": 0,%'
    OR raw_record LIKE '%"mcc":0}%' OR raw_record LIKE '%"mcc": 0}%'
  )`;

  // If we have expected MCCs, we also want records that do NOT contain any expected MCC.
  // For Israel (425): we want anything that is NOT "mcc":425
  // But doing NOT LIKE for every expected MCC is fragile, so we use a two-pass approach:
  // First pass: grab all rows using cheap LIKE for known-interesting MCCs
  // The rule engine will then flag the unexpected ones properly.
  const expectedNumeric = (expectedMCCs || []).map(m => parseInt(m, 10));

  // For non-expected MCCs, we can't easily do NOT LIKE for all of them,
  // so we target the most interesting ones: test networks (0,1) + known IMSI-catcher MCCs
  // The rule engine handles the final MCC validation anyway.

  const rs = await queryWithRetry({
    query: `SELECT id, timestamp, sample_id, createdAt, reason, raw_record
            FROM bad_measurements
            WHERE timestamp > now() - INTERVAL {days:UInt32} DAY
              AND raw_record != ''
              AND length(raw_record) > 10
              AND ${likeConditions}
            ORDER BY timestamp DESC
            LIMIT {limit:UInt32}`,
    query_params: params,
    format: 'JSONEachRow',
  });
  return rs.json();
}

/**
 * Fetch ALL recent bad_measurements with raw_record (not just test MCCs).
 * The getBadMeasurementsRFAnomalies only catches MCC 0/1 test networks.
 * This query pulls everything so the rule engine can flag with BAD_MEASUREMENT.
 */
async function getAllRecentBadMeasurements(hoursBack = 24, limit = 500) {
  const rs = await queryWithRetry({
    query: `SELECT id, timestamp, sample_id, createdAt, reason, raw_record
            FROM bad_measurements
            WHERE timestamp > now() - INTERVAL {hours:UInt32} HOUR
              AND raw_record != ''
              AND length(raw_record) > 10
            ORDER BY timestamp DESC
            LIMIT {limit:UInt32}`,
    query_params: { hours: hoursBack, limit },
    format: 'JSONEachRow',
  });
  return rs.json();
}

/**
 * Fetch bad_measurements within a geographic bounding box using JSONExtract
 * to filter by coordinates server-side. This is the primary function for the
 * scan pipeline — ensures we don't miss region-specific measurements due to
 * global LIMIT exhaustion.
 *
 * Uses GeoJSON path (location.geo.coordinates = [lng, lat]) as primary,
 * with tileId_1 fallback ("lng,lat" string).
 */
async function getBadMeasurementsInBbox(bbox, hoursBack = 168, limit = 1000) {
  if (!bbox) return getAllRecentBadMeasurements(hoursBack, limit);

  const margin = 1.0; // 1° margin around bbox
  const latMin = bbox.latMin - margin;
  const latMax = bbox.latMax + margin;
  const lngMin = bbox.lngMin - margin;
  const lngMax = bbox.lngMax + margin;

  const rs = await queryWithRetry({
    query: `SELECT id, timestamp, sample_id, createdAt, reason, raw_record
            FROM bad_measurements
            WHERE timestamp > now() - INTERVAL {hours:UInt32} HOUR
              AND raw_record != ''
              AND length(raw_record) > 10
              AND (
                -- Primary: GeoJSON coordinates path [lng, lat]
                (
                  JSONExtractFloat(raw_record, 'location', 'geo', 'coordinates', 2) BETWEEN {latMin:Float64} AND {latMax:Float64}
                  AND JSONExtractFloat(raw_record, 'location', 'geo', 'coordinates', 1) BETWEEN {lngMin:Float64} AND {lngMax:Float64}
                )
                OR
                -- Fallback: tileId_1 string "lng,lat" — parse with splitByChar
                (
                  JSONHas(raw_record, 'location', 'tileId_1') = 1
                  AND toFloat64OrNull(splitByChar(',', JSONExtractString(raw_record, 'location', 'tileId_1'))[2])
                      BETWEEN {latMin:Float64} AND {latMax:Float64}
                  AND toFloat64OrNull(splitByChar(',', JSONExtractString(raw_record, 'location', 'tileId_1'))[1])
                      BETWEEN {lngMin:Float64} AND {lngMax:Float64}
                )
              )
            ORDER BY timestamp DESC
            LIMIT {limit:UInt32}`,
    query_params: { hours: hoursBack, limit, latMin, latMax, lngMin, lngMax },
    format: 'JSONEachRow',
  });
  return rs.json();
}

// ---------------------------------------------------------------------------
// Parse bad_measurements raw_record JSON into flat measurement format
// ---------------------------------------------------------------------------

/** Extract lat/lng from a raw_record location object.
 *  Handles multiple Flycomm SDK formats:
 *    1. GeoJSON:     location.geo.coordinates = [lng, lat]
 *    2. Direct:      location.lat / location.lng (or .lon / .longitude)
 *    3. Nested:      location.position.lat / .lng
 *    4. tileId_1:    "lat,lng" string in location.tileId_1 (e.g. "31.74,34.31")
 *    5. Top-level:   raw.lat / raw.lng
 *    6. Flat:        raw.location_lat_rounded / raw.location_lng_rounded
 */
function extractLocation(raw) {
  const loc = raw?.location;
  let lat = null, lng = null;

  // 1. GeoJSON
  const coords = loc?.geo?.coordinates;
  if (Array.isArray(coords) && coords.length >= 2) {
    lat = Number(coords[1]); lng = Number(coords[0]);
    if (!isNaN(lat) && !isNaN(lng) && lat !== 0) return { lat, lng };
  }

  // 2. Direct on location object
  lat = Number(loc?.lat ?? loc?.latitude);
  lng = Number(loc?.lng ?? loc?.lon ?? loc?.longitude);
  if (!isNaN(lat) && !isNaN(lng) && lat !== 0) return { lat, lng };

  // 3. Nested position
  lat = Number(loc?.position?.lat ?? loc?.position?.latitude);
  lng = Number(loc?.position?.lng ?? loc?.position?.lon ?? loc?.position?.longitude);
  if (!isNaN(lat) && !isNaN(lng) && lat !== 0) return { lat, lng };

  // 4. tileId_1 string (format: "lat,lng" — seen in Flycomm SDK)
  // e.g. tileId_1: "31.74,34.31"  (or "-118.3,1.2" — check if lat is in valid range)
  const tileStr = loc?.tileId_1;
  if (typeof tileStr === 'string' && tileStr.includes(',')) {
    const parts = tileStr.split(',');
    if (parts.length >= 2) {
      const t0 = Number(parts[0]); const t1 = Number(parts[1]);
      // Figure out which is lat (should be -90..90) and which is lng
      if (!isNaN(t0) && !isNaN(t1)) {
        if (Math.abs(t0) <= 90) { lat = t0; lng = t1; }
        else if (Math.abs(t1) <= 90) { lat = t1; lng = t0; }
        if (lat !== null && lat !== 0) return { lat, lng };
      }
    }
  }

  // 5. Top-level on raw object
  lat = Number(raw?.lat ?? raw?.latitude);
  lng = Number(raw?.lng ?? raw?.lon ?? raw?.longitude);
  if (!isNaN(lat) && !isNaN(lng) && lat !== 0) return { lat, lng };

  // 6. Flat format (same as measurements table columns)
  lat = Number(raw?.location_lat_rounded);
  lng = Number(raw?.location_lng_rounded);
  if (!isNaN(lat) && !isNaN(lng) && lat !== 0) return { lat, lng };

  return { lat: null, lng: null };
}

/**
 * Converts a bad_measurements row (with raw_record JSON string) into the flat
 * column format the rule engine expects (matching the measurements table schema).
 * Returns null if raw_record is missing or unparseable.
 */
function parseBadMeasurementRawRecord(row) {
  if (!row || !row.raw_record) return null;

  let raw;
  try {
    raw = typeof row.raw_record === 'string' ? JSON.parse(row.raw_record) : row.raw_record;
  } catch (e) {
    console.error(`[DB] Failed to parse raw_record for sample ${row.sample_id}:`, e.message);
    return null;
  }

  const { lat, lng } = extractLocation(raw);

  // Try to extract cell/network/signal from multiple possible structures:
  // Flycomm SDK raw_record can have data at top-level or nested under providers/primaryProviders
  const cell = raw.cell || {};
  const network = raw.network || {};
  const signal = raw.signal || {};
  const band = raw.band || {};

  // Some raw_records have providers array instead of flat cell/network/signal
  let provider = null;
  const providers = raw.primaryProviders || raw.providers || raw.cells || [];
  if (Array.isArray(providers) && providers.length > 0) {
    provider = providers[0]; // Use first (primary) provider
  }

  const getCell = (key) => cell[key] ?? provider?.[key] ?? provider?.cell?.[key] ?? null;
  const getNet = (key) => network[key] ?? provider?.network?.[key] ?? provider?.[key] ?? null;
  const getSig = (key) => signal[key] ?? provider?.signal?.[key] ?? provider?.[key] ?? null;
  const getBand = (key) => band[key] ?? provider?.band?.[key] ?? null;

  // Build PLMN from mcc+mnc if not directly available
  const mcc = getNet('mcc') ?? raw.mcc;
  const mnc = getNet('mnc') ?? raw.mnc;
  let plmn = getNet('PLMN') || getNet('plmn') || raw.network_PLMN;
  if (!plmn && mcc != null && mnc != null) {
    plmn = `${String(mcc).padStart(3, '0')}-${String(mnc).padStart(2, '0')}`;
  }

  return {
    // Identity
    sample_id: raw.sample_id || row.sample_id,
    timestamp: raw.timestamp || raw.createdAt || row.timestamp,

    // Cell info
    cell_pci: getCell('pci') ?? raw.pci ?? null,
    cell_eci: getCell('eci') ?? raw.eci ?? null,
    cell_enb: getCell('enb') ?? raw.enb ?? null,
    cell_tac: getCell('tac') ?? raw.tac ?? null,
    cell_ecgi: getCell('ecgi') ?? raw.ecgi ?? null,

    // Network
    network_mcc: mcc,
    network_mnc: mnc,
    network_PLMN: plmn,
    network_isRoaming: getNet('isRoaming') ?? null,
    network_operator: getNet('operator') ?? raw.network_operator ?? null,
    network_iso: getNet('iso') ?? null,

    // Signal
    signal_rsrp: getSig('rsrp') ?? raw.rsrp ?? null,
    signal_rsrq: getSig('rsrq') ?? raw.rsrq ?? null,
    signal_rssi: getSig('rssi') ?? raw.rssi ?? null,
    signal_snr: getSig('snr') ?? raw.snr ?? null,
    signal_timingAdvance: getSig('timingAdvance') ?? getSig('ta') ?? raw.ta ?? null,

    // Band
    band_downlinkEarfcn: getBand('downlinkEarfcn') ?? getBand('channelNumber') ?? raw.earfcn ?? null,
    band_name: getBand('name') ?? null,
    band_number: getBand('number') ?? null,

    // Location
    location_lat_rounded: lat,
    location_lng_rounded: lng,

    // Device & tech
    tech: raw.tech || provider?.tech || null,
    connectionStatus: raw.connectionStatus || provider?.connectionStatus || null,
    isRegistered: raw.isRegistered ?? provider?.isRegistered ?? null,
    deviceInfo_deviceId: raw.deviceInfo?.deviceId || raw.deviceId || null,
    deviceInfo_deviceModel: raw.deviceInfo?.deviceModel || raw.deviceModel || null,

    // Mark as sourced from bad_measurements for traceability
    _source: 'bad_measurements',
    _bad_reason: row.reason || null,
  };
}

/** Debug: search null/empty MCC measurements near a specific location */
async function debugNullMCCNearLocation(lat, lng, radiusDeg = 0.5, daysBack = 30) {
  const rs = await queryWithRetry({
    query: `SELECT timestamp, cell_pci, cell_eci, cell_ecgi, cell_enb, cell_tac,
                   network_PLMN, network_mcc, network_mnc, network_iso,
                   tech, signal_rsrp, signal_rssi, signal_snr, signal_timingAdvance,
                   band_downlinkEarfcn, band_downlinkFrequency, band_name, band_number,
                   location_lat_rounded, location_lng_rounded,
                   network_isRoaming, network_operator,
                   sample_id, deviceInfo_deviceId, deviceInfo_deviceModel
            FROM measurements
            WHERE timestamp > now() - INTERVAL {days:UInt32} DAY
              AND (network_mcc IS NULL OR network_mcc = 0 OR network_PLMN = '')
              AND location_lat_rounded >= {latMin:Float64}
              AND location_lat_rounded <= {latMax:Float64}
              AND location_lng_rounded >= {lngMin:Float64}
              AND location_lng_rounded <= {lngMax:Float64}
            ORDER BY timestamp DESC
            LIMIT 100`,
    query_params: {
      days: daysBack,
      latMin: lat - radiusDeg, latMax: lat + radiusDeg,
      lngMin: lng - radiusDeg, lngMax: lng + radiusDeg,
    },
    format: 'JSONEachRow',
  });
  return rs.json();
}

/** Debug: show all distinct MCC values in the last N days */
async function debugMCCValues(daysBack = 7) {
  const rs = await queryWithRetry({
    query: `SELECT
              network_mcc,
              lpad(toString(network_mcc), 3, '0') AS mcc_padded,
              network_PLMN,
              count() AS cnt
            FROM measurements
            WHERE timestamp > now() - INTERVAL {days:UInt32} DAY
            GROUP BY network_mcc, mcc_padded, network_PLMN
            ORDER BY cnt DESC
            LIMIT 100`,
    query_params: { days: daysBack },
    format: 'JSONEachRow',
  });
  return rs.json();
}

// ---------------------------------------------------------------------------
// Cleanup
// ---------------------------------------------------------------------------
async function close() {
  if (client) {
    await client.close();
    client = null;
  }
}

// ---------------------------------------------------------------------------
// WiFi measurements — from wifi_measurements table (separate from cellular)
// ---------------------------------------------------------------------------
async function getRSUWifiHistory(deviceId, limit = 500, startTs = null, endTs = null) {
  let whereClause = `WHERE deviceInfo_deviceId = {did:String}`;
  const params = { did: deviceId, limit };
  if (startTs) {
    whereClause += ` AND timestamp >= toDateTime64({startTs:String}, 3, 'UTC')`;
    params.startTs = startTs.replace('Z', '').replace('T', ' ');
  }
  if (endTs) {
    whereClause += ` AND timestamp <= toDateTime64({endTs:String}, 3, 'UTC')`;
    params.endTs = endTs.replace('Z', '').replace('T', ' ');
  }
  const rs = await queryWithRetry({
    query: `SELECT timestamp, sample_id, ssid, bssid, interface, signal, frequency, channel,
                   channelWidth, channelName,
                   deviceInfo_deviceId, deviceInfo_deviceModel
            FROM wifi_measurements
            ${whereClause}
            ORDER BY timestamp DESC
            LIMIT {limit:UInt32}`,
    query_params: params,
    format: 'JSONEachRow',
  });
  return rs.json();
}

async function getRSUWifiSummary(deviceId, startTs = null, endTs = null) {
  let whereClause = `WHERE deviceInfo_deviceId = {did:String}`;
  const params = { did: deviceId };
  if (startTs) {
    whereClause += ` AND timestamp >= toDateTime64({startTs:String}, 3, 'UTC')`;
    params.startTs = startTs.replace('Z', '').replace('T', ' ');
  }
  if (endTs) {
    whereClause += ` AND timestamp <= toDateTime64({endTs:String}, 3, 'UTC')`;
    params.endTs = endTs.replace('Z', '').replace('T', ' ');
  }
  const rs = await queryWithRetry({
    query: `SELECT ssid, bssid, channelName,
                   count() AS cnt,
                   avg(signal) AS avg_signal,
                   min(signal) AS min_signal,
                   max(signal) AS max_signal,
                   avg(frequency) AS avg_freq,
                   min(timestamp) AS first_seen,
                   max(timestamp) AS last_seen
            FROM wifi_measurements
            ${whereClause}
            GROUP BY ssid, bssid, channelName
            ORDER BY cnt DESC
            LIMIT 50`,
    query_params: params,
    format: 'JSONEachRow',
  });
  return rs.json();
}

module.exports = {
  healthCheck,
  discoverSitesSchema,
  getLastWatermark,
  updateWatermark,
  getNewMeasurements,
  getCellBaselines,
  getKnownCells,
  getBadMeasurementPatterns,
  checkAgainstBadMeasurements,
  getRecentBadMeasurements,
  writeThreatEvents,
  writeAlertLog,
  getRecentThreats,
  getThreatStats,
  getBadMeasurementStats,
  getRecentBadMeasurementsForDashboard,
  getMeasurementStats,
  getMeasurementStats24h,
  getRecentMeasurements,
  getRecentMeasurementsFiltered,
  getRatDistribution,
  getSiteStats,
  getSitesByTech,
  getSiteSampleStats,
  getMCCDistribution,
  getRegionMeasurementStats,
  getMCCAnomalyMeasurements,
  getTestNetworkMeasurements,
  getTAZeroMeasurements,
  getDowngradeMeasurements,
  getLocationAnomalyMeasurements,
  getBadMeasurementsWithRawData,
  getBadMeasurementsRFAnomalies,
  getAllRecentBadMeasurements,
  getBadMeasurementsInBbox,
  parseBadMeasurementRawRecord,
  debugFindMCC001,
  debugNullMCCNearLocation,
  debugMCCValues,
  searchMeasurementByLocation,
  getRSUDevices,
  getSourceDistribution,
  searchDeviceIds,
  resetClient,
  getRSUDeviceHistory,
  getRSUDeviceTimeline,
  getRSUWifiHistory,
  getRSUWifiSummary,
  describeTable,
  close,
};

// ---------------------------------------------------------------------------
// Source distribution — what source values exist in measurements
// ---------------------------------------------------------------------------
async function getSourceDistribution(hoursBack = 24) {
  const rs = await queryWithRetry({
    query: `SELECT source, count() AS cnt
            FROM measurements
            WHERE timestamp > now() - INTERVAL ${Math.round(hoursBack)} HOUR
            GROUP BY source
            ORDER BY cnt DESC
            LIMIT 20`,
    format: 'JSONEachRow',
  });
  return rs.json();
}

// ---------------------------------------------------------------------------
// Describe any table schema
// ---------------------------------------------------------------------------
async function describeTable(tableName) {
  const safe = tableName.replace(/[^a-zA-Z0-9_]/g, '');
  const rs = await queryWithRetry({ query: `DESCRIBE TABLE ${safe}`, format: 'JSONEachRow' });
  return rs.json();
}

// ---------------------------------------------------------------------------
// Search for specific device IDs across all sources
// ---------------------------------------------------------------------------
async function searchDeviceIds(deviceIds) {
  const rs = await queryWithRetry({
    query: `SELECT deviceInfo_deviceId AS device_id, source,
                   count() AS cnt,
                   min(timestamp) AS first_seen, max(timestamp) AS last_seen,
                   argMax(location_lat_rounded, timestamp) AS lat,
                   argMax(location_lng_rounded, timestamp) AS lng
            FROM measurements
            WHERE deviceInfo_deviceId IN ({ids:Array(String)})
            GROUP BY deviceInfo_deviceId, source
            ORDER BY device_id, source`,
    query_params: { ids: deviceIds },
    format: 'JSONEachRow',
  });
  return rs.json();
}

// ---------------------------------------------------------------------------
// RSU device detail — recent measurements for a single device
// ---------------------------------------------------------------------------
const RSU_DETAIL_COLUMNS = `
  timestamp, sample_id, source,
  signal_rsrp, signal_rsrq, signal_rssi, signal_snr, signal_timingAdvance, signal_txPower,
  signal_csiRsrp, signal_csiRsrq, signal_csiSinr, signal_ssSinr, signal_cqi,
  cell_pci, cell_eci, cell_ecgi, cell_enb, cell_tac,
  tech, band_downlinkEarfcn, band_downlinkFrequency, band_name, band_number, band_bandwidth,
  network_PLMN, network_mcc, network_mnc, network_operator, network_isRoaming,
  location_lat_rounded, location_lng_rounded, location_accuracy, location_altitude, location_speed, location_heading,
  satellites_gnss_satellitesNo, satellites_gps_satellitesNo, satellites_glonass_satellitesNo,
  satellites_galileo_satellitesNo, satellites_beidou_satellitesNo,
  internet_latency, internet_jitter, internet_latencyLoss,
  internet_downloadMbps, internet_uploadMbps,
  internet_downloadDuration, internet_uploadDuration,
  deviceInfo_deviceId, deviceInfo_deviceModel, deviceInfo_imei, deviceInfo_uptime, deviceInfo_temperature,
  deviceInfo_modemVersion, deviceInfo_connectivtyStatus,
  connectionStatus, isRegistered
`;

async function getRSUDeviceHistory(deviceId, limit = 100, startTs = null, endTs = null) {
  let whereClause = `WHERE deviceInfo_deviceId = {did:String} AND source = 'modem'`;
  const params = { did: deviceId, limit };
  if (startTs) {
    whereClause += ` AND timestamp >= toDateTime64({startTs:String}, 3, 'UTC')`;
    params.startTs = startTs.replace('Z', '').replace('T', ' ');
  }
  if (endTs) {
    whereClause += ` AND timestamp <= toDateTime64({endTs:String}, 3, 'UTC')`;
    params.endTs = endTs.replace('Z', '').replace('T', ' ');
  }
  const rs = await queryWithRetry({
    query: `SELECT ${RSU_DETAIL_COLUMNS}
            FROM measurements
            ${whereClause}
            ORDER BY timestamp DESC
            LIMIT {limit:UInt32}`,
    query_params: params,
    format: 'JSONEachRow',
  });
  return rs.json();
}

// ---------------------------------------------------------------------------
// RSU device timeline — bucketed signal aggregates for playback
// ---------------------------------------------------------------------------
async function getRSUDeviceTimeline(deviceId, bucketMinutes = 5, startTs = null, endTs = null, hoursBack = 24) {
  let whereClause = `WHERE deviceInfo_deviceId = {did:String} AND source = 'modem'`;
  const params = { did: deviceId, bucket: Math.round(bucketMinutes) };
  if (startTs && endTs) {
    whereClause += ` AND timestamp >= toDateTime64({startTs:String}, 3, 'UTC') AND timestamp <= toDateTime64({endTs:String}, 3, 'UTC')`;
    params.startTs = startTs.replace('Z', '').replace('T', ' ');
    params.endTs = endTs.replace('Z', '').replace('T', ' ');
  } else {
    whereClause += ` AND timestamp > now() - INTERVAL ${Math.round(hoursBack)} HOUR`;
  }
  const rs = await queryWithRetry({
    query: `SELECT
              toStartOfInterval(timestamp, INTERVAL {bucket:UInt32} MINUTE) AS ts,
              count() AS samples,
              -- Cellular
              avg(signal_rsrp) AS avg_rsrp, min(signal_rsrp) AS min_rsrp, max(signal_rsrp) AS max_rsrp,
              avg(signal_rsrq) AS avg_rsrq, avg(signal_snr) AS avg_snr,
              avg(signal_timingAdvance) AS avg_ta,
              argMax(cell_pci, timestamp) AS pci,
              argMax(tech, timestamp) AS tech,
              argMax(network_PLMN, timestamp) AS plmn,
              argMax(network_operator, timestamp) AS operator,
              argMax(band_downlinkEarfcn, timestamp) AS earfcn,
              argMax(band_name, timestamp) AS band_name,
              -- QoE
              avg(internet_latency) AS avg_latency,
              avg(internet_jitter) AS avg_jitter,
              avg(internet_latencyLoss) AS avg_packet_loss,
              avg(internet_downloadMbps) AS avg_download,
              avg(internet_uploadMbps) AS avg_upload,
              -- GNSS
              argMax(satellites_gnss_satellitesNo, timestamp) AS gnss_sats,
              argMax(satellites_gps_satellitesNo, timestamp) AS gps_sats,
              argMax(satellites_glonass_satellitesNo, timestamp) AS glonass_sats,
              argMax(satellites_galileo_satellitesNo, timestamp) AS galileo_sats,
              argMax(satellites_beidou_satellitesNo, timestamp) AS beidou_sats,
              argMax(location_accuracy, timestamp) AS loc_accuracy,
              argMax(location_altitude, timestamp) AS loc_altitude,
              argMax(location_speed, timestamp) AS loc_speed,
              -- Location
              argMax(location_lat_rounded, timestamp) AS lat,
              argMax(location_lng_rounded, timestamp) AS lng,
              -- Device
              argMax(deviceInfo_temperature, timestamp) AS temperature,
              argMax(deviceInfo_uptime, timestamp) AS uptime
            FROM measurements
            ${whereClause}
            GROUP BY ts
            ORDER BY ts ASC`,
    query_params: params,
    format: 'JSONEachRow',
  });
  return rs.json();
}

// ---------------------------------------------------------------------------
// RSU devices — latest position per modem device within a bbox + time window
// ---------------------------------------------------------------------------
async function getRSUDevices(bbox, activeWindowHours = 24) {
  // RSU devices: all unique devices from source='modem', ALL TIME, within bbox.
  // Also compute whether each device has data within the active window (online/offline).
  let bboxClause = '';
  const params = { activeWindowHours: Math.round(activeWindowHours) };
  if (bbox) {
    bboxClause = ` AND location_lat_rounded >= {latMin:Float64}
                    AND location_lat_rounded <= {latMax:Float64}
                    AND location_lng_rounded >= {lngMin:Float64}
                    AND location_lng_rounded <= {lngMax:Float64}`;
    params.latMin = bbox.latMin;
    params.latMax = bbox.latMax;
    params.lngMin = bbox.lngMin;
    params.lngMax = bbox.lngMax;
  }

  const rs = await queryWithRetry({
    query: `SELECT
              deviceInfo_deviceId AS device_id,
              argMax(location_lat_rounded, timestamp) AS lat,
              argMax(location_lng_rounded, timestamp) AS lng,
              argMax(deviceInfo_deviceModel, timestamp) AS device_model,
              max(timestamp) AS last_seen,
              min(timestamp) AS first_seen,
              count() AS total_samples,
              countIf(timestamp > now() - INTERVAL {activeWindowHours:UInt32} HOUR) AS active_samples,
              maxIf(signal_rsrp, timestamp > now() - INTERVAL {activeWindowHours:UInt32} HOUR) AS latest_rsrp,
              argMaxIf(tech, timestamp, timestamp > now() - INTERVAL {activeWindowHours:UInt32} HOUR) AS latest_tech,
              argMaxIf(network_PLMN, timestamp, timestamp > now() - INTERVAL {activeWindowHours:UInt32} HOUR) AS latest_plmn,
              argMaxIf(cell_pci, timestamp, timestamp > now() - INTERVAL {activeWindowHours:UInt32} HOUR) AS latest_pci
            FROM measurements
            WHERE source = 'modem'${bboxClause}
            GROUP BY deviceInfo_deviceId
            HAVING device_id != ''
              AND lat != 0 AND lng != 0
            ORDER BY last_seen DESC
            LIMIT 500`,
    query_params: params,
    format: 'JSONEachRow',
  });
  return rs.json();
}
