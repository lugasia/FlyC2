/**
 * rsuMonitor.js — RSU Real-Time State Manager + Poll Loop
 *
 * Maintains in-memory state per RSU device and per cluster.
 * Polls ClickHouse every 5 seconds for new measurements (delta query),
 * runs fast detection rules, and pushes results via SSE to connected clients.
 *
 * Lifecycle:
 *   1. start() — called on server startup, discovers RSU orgs, bootstraps state
 *   2. poll loop — runs every RSU_POLL_INTERVAL_MS, processes deltas
 *   3. stop() — called on shutdown, clears intervals
 */
'use strict';

const config = require('./config');
const db = require('./db');
const orgStore = require('./orgStore');
const { runRSURules, updateDeviceState, extractDeviceId } = require('./rsuRules');

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Derive expected MCCs from a bbox by finding which region preset contains
 * the bbox center. Falls back to global config region if no match.
 */
function deriveExpectedMCCs(bbox) {
  if (!bbox) return [];
  const centerLat = (bbox.latMin + bbox.latMax) / 2;
  const centerLng = (bbox.lngMin + bbox.lngMax) / 2;

  for (const [, region] of Object.entries(config.regions)) {
    if (!region.bbox || !region.expectedMCC) continue;
    const rb = region.bbox;
    if (centerLat >= rb.latMin && centerLat <= rb.latMax &&
        centerLng >= rb.lngMin && centerLng <= rb.lngMax) {
      return region.expectedMCC;
    }
  }
  // Fallback to global config region
  const globalRegion = config.getRegion();
  return globalRegion.expectedMCC || [];
}

// ---------------------------------------------------------------------------
// Per-org monitor state
// ---------------------------------------------------------------------------

/** @type {Map<string, OrgMonitor>} orgId → monitor instance */
const monitors = new Map();

/** SSE clients subscribed to RSU stream: Set<{ res, orgId }> */
const rsuSSEClients = new Set();

/** Callback for pushing alerts to main alert system (set by server.js) */
let onAlertCallback = null;

/**
 * Create a fresh device state object.
 */
function createDeviceState() {
  return {
    lastMeasurement: null,
    previousMeasurement: null,
    lastServingMeasurement: null,  // last measurement with enodeb_id (serving cell)
    knownCells: new Set(),
    knownTACs: new Set(),
    baselineRSRP: null,
    baselineRSRQ: null,            // rolling RSRQ average for quality tracking
    baselineSatCount: null,
    baselineAccuracy: null,        // rolling GPS accuracy baseline
    anchorPosition: null,
    lastAlertTime: {},
    lastSeenAt: 0,
    // State-machine flags (prevent repeated alerts for same condition)
    jammingActive: false,
    signalDegradationActive: false,
    rsrqDegradationActive: false,
    gpsJammingActive: false,
    gpsAccuracyDegraded: false,
    gpsSpoofingActive: false,
    foreignMCCActive: false,
    lastServingCell: null,          // "PCI_eNB" of current serving cell
    knownUnknownENBs: new Set(),    // unknown eNBs already alerted on
  };
}

/**
 * Create a fresh cluster state object.
 */
function createClusterState() {
  return {
    knownCells: new Map(), // cellKey → { firstSeen, lastSeen, devices, mcc, tac }
    sitesIndex: new Set(), // Set of known eNB IDs from sites database
  };
}

/**
 * OrgMonitor — manages real-time RSU monitoring for one organization.
 */
class OrgMonitor {
  constructor(org) {
    this.orgId = org.id;
    this.orgName = org.name;
    this.bbox = null;
    this.expectedMCCs = [];
    this.intervalHandle = null;
    this.watermark = null; // ISO timestamp — only fetch measurements after this
    this.devices = new Map(); // deviceId → deviceState
    this.clusterState = createClusterState();

    // Stats
    this.pollCount = 0;
    this.lastPollTime = null;
    this.lastPollDurationMs = 0;
    this.lastMeasurementTime = null;
    this.totalMeasurementsProcessed = 0;
    // Data source toggles (controllable via /api/rsu/sources)
    this.dataSources = { useModemMeasurements: true, useMeasurements: false };
    this.totalAlertsGenerated = 0;

    // Derive bbox from org cluster polygon
    if (org.cluster && org.cluster.coordinates && org.cluster.coordinates[0]) {
      const coords = org.cluster.coordinates[0];
      let latMin = 90, latMax = -90, lngMin = 180, lngMax = -180;
      for (const [lng, lat] of coords) {
        if (lat < latMin) latMin = lat;
        if (lat > latMax) latMax = lat;
        if (lng < lngMin) lngMin = lng;
        if (lng > lngMax) lngMax = lng;
      }
      this.bbox = { latMin, lngMin, latMax, lngMax };
    }

    // Derive expected MCCs from org's cluster location
    // Find which region preset contains the cluster center
    this.expectedMCCs = deriveExpectedMCCs(this.bbox);
  }

  /**
   * Bootstrap — build initial state from existing data.
   * Strategy:
   *   1. Try last N minutes (fast delta query)
   *   2. If empty, discover devices in bbox and load their recent history
   * This ensures we pick up RSUs that haven't reported in the last few minutes.
   */
  async bootstrap() {
    if (!this.bbox) {
      console.log(`[RSU-MON] ${this.orgName}: no cluster polygon — skipping bootstrap`);
      return;
    }

    const bootstrapMinutes = config.agent.rsuBootstrapMinutes || 5;
    const since = new Date(Date.now() - bootstrapMinutes * 60000).toISOString();
    console.log(`[RSU-MON] ${this.orgName}: bootstrapping...`);

    try {
      // Step 1: Try fast delta from last N minutes
      const [measRows, modemRows] = await Promise.all([
        db.getNewRSUMeasurements(this.bbox, since),
        db.getNewModemMeasurements(this.bbox, since).catch(err => {
          console.warn(`[RSU-MON] ${this.orgName}: modem_measurements delta failed:`, err.message);
          return [];
        }),
      ]);

      let allRows = [...measRows, ...modemRows.map(normalizeModemRow)];

      // Step 2: If no recent data, discover devices via full device list
      // and load their last 100 measurements each (builds baselines + known cells)
      if (allRows.length === 0) {
        console.log(`[RSU-MON] ${this.orgName}: no data in last ${bootstrapMinutes}min — discovering devices...`);
        const [measDevices, modemDevices] = await Promise.all([
          db.getRSUDevices(this.bbox, 720).catch(() => []),   // 30 days
          db.getModemMeasurementDevices(this.bbox, 720).catch(() => []),
        ]);

        // Merge unique device IDs
        const deviceIds = new Set();
        for (const d of [...measDevices, ...modemDevices]) {
          if (d.device_id) deviceIds.add(d.device_id);
        }

        console.log(`[RSU-MON] ${this.orgName}: found ${deviceIds.size} device(s) — loading recent history...`);

        // Load last 100 measurements per device to build baselines
        for (const deviceId of deviceIds) {
          try {
            let rows = await db.getRSUDeviceHistory(deviceId, 100);
            if (rows.length === 0) {
              const modemRows2 = await db.getRSUModemMeasurements(deviceId, 100);
              rows = modemRows2.map(normalizeModemRow);
            }
            allRows.push(...rows);
          } catch (err) {
            console.warn(`[RSU-MON] ${this.orgName}: failed to load history for ${deviceId}:`, err.message);
          }
        }
      }

      // Sort by timestamp ascending (oldest first)
      allRows.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

      // Process each measurement to build state (no alerting during bootstrap)
      for (const m of allRows) {
        const deviceId = extractDeviceId(m);
        if (!deviceId) continue;

        if (!this.devices.has(deviceId)) {
          this.devices.set(deviceId, createDeviceState());
        }
        updateDeviceState(this.devices.get(deviceId), m);

        // Register cells in cluster state (quietly, no alerts)
        const pci = m.cell_pci || m.pcid || null;
        const enb = m.cell_enb || m.enodeb_id || null;
        const earfcn = m.band_downlinkEarfcn || m.frequency || null;
        if (pci != null) {
          const key = `${pci}_${enb || '?'}_${earfcn || '?'}`;
          if (!this.clusterState.knownCells.has(key)) {
            this.clusterState.knownCells.set(key, {
              firstSeen: m.timestamp,
              lastSeen: m.timestamp,
              devices: new Set([deviceId]),
              mcc: m.network_mcc || m.mcc || null,
              tac: m.cell_tac || m.tac || null,
            });
          } else {
            const cell = this.clusterState.knownCells.get(key);
            cell.lastSeen = m.timestamp;
            cell.devices.add(deviceId);
          }
        }
      }

      // Set watermark to latest timestamp or now
      if (allRows.length > 0) {
        this.watermark = allRows[allRows.length - 1].timestamp;
        this.lastMeasurementTime = this.watermark;
      } else {
        this.watermark = since;
      }

      // Load known sites DB for unknown-site detection
      try {
        const sites = await db.getKnownCells(this.bbox);
        for (const site of sites) {
          if (site.site_id != null) {
            this.clusterState.sitesIndex.add(String(site.site_id));
          }
        }
        console.log(`[RSU-MON] ${this.orgName}: loaded ${this.clusterState.sitesIndex.size} known site eNBs`);
      } catch (err2) {
        console.warn(`[RSU-MON] ${this.orgName}: failed to load sites DB:`, err2.message);
      }

      console.log(`[RSU-MON] ${this.orgName}: bootstrap done — ${allRows.length} measurements, ${this.devices.size} devices, ${this.clusterState.knownCells.size} known cells`);
    } catch (err) {
      console.error(`[RSU-MON] ${this.orgName}: bootstrap error:`, err.message);
      this.watermark = since;
    }
  }

  /**
   * Single poll cycle — fetch delta, run rules, push SSE.
   */
  async poll() {
    if (!this.bbox || !this.watermark) return;
    // Prevent overlapping polls — if previous poll is still running, skip
    if (this._pollInFlight) return;
    this._pollInFlight = true;

    const t0 = Date.now();
    this.pollCount++;

    try {
      // Query data sources based on toggle settings
      const queries = [];
      if (this.dataSources.useModemMeasurements) {
        queries.push(db.getNewModemMeasurements(this.bbox, this.watermark).then(rows => rows.map(normalizeModemRow)));
      }
      if (this.dataSources.useMeasurements) {
        queries.push(db.getNewRSUMeasurements(this.bbox, this.watermark));
      }
      const results = await Promise.all(queries);
      const allRows = results.flat();

      // Deduplicate by sample_id
      const seen = new Set();
      const unique = [];
      for (const m of allRows) {
        const key = m.sample_id || `${m.timestamp}_${extractDeviceId(m)}`;
        if (!seen.has(key)) {
          seen.add(key);
          unique.push(m);
        }
      }

      // Sort by timestamp ascending
      unique.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

      const allAlerts = [];
      const newMeasurements = [];

      for (const m of unique) {
        const deviceId = extractDeviceId(m);
        if (!deviceId) continue;

        if (!this.devices.has(deviceId)) {
          this.devices.set(deviceId, createDeviceState());
        }
        const deviceState = this.devices.get(deviceId);

        // Run rules BEFORE updating state (so rules see previous state)
        const orgConfig = { expectedMCCs: this.expectedMCCs };
        const alerts = runRSURules(m, deviceState, this.clusterState, orgConfig);
        if (alerts.length > 0) {
          allAlerts.push(...alerts);
          this.totalAlertsGenerated += alerts.length;
        }

        // Update state with this measurement
        updateDeviceState(deviceState, m);
        newMeasurements.push(m);
      }

      // Check for offline devices (only when SSE clients are listening)
      const hasClients = [...rsuSSEClients].some(c => c.orgId === this.orgId);
      if (hasClients) {
        const offlineAlerts = this._checkOfflineDevices();
        allAlerts.push(...offlineAlerts);
      }

      // Advance watermark — add 1ms to avoid re-fetching the same row
      if (unique.length > 0) {
        const lastTs = unique[unique.length - 1].timestamp;
        // Parse and add 1ms to ensure strict "after" semantics
        const dt = new Date(String(lastTs).replace(' ', 'T') + (String(lastTs).includes('Z') ? '' : 'Z'));
        dt.setMilliseconds(dt.getMilliseconds() + 1);
        this.watermark = dt.toISOString().replace('T', ' ').replace('Z', '');
        this.lastMeasurementTime = lastTs;
      }

      this.totalMeasurementsProcessed += unique.length;
      this.lastPollTime = new Date().toISOString();
      this.lastPollDurationMs = Date.now() - t0;

      // Push to SSE clients
      if (hasClients && (newMeasurements.length > 0 || allAlerts.length > 0)) {
        this._pushSSE({
          type: 'rsu:update',
          orgId: this.orgId,
          measurements: newMeasurements,
          alerts: allAlerts,
          stats: {
            pollCount: this.pollCount,
            queryTimeMs: this.lastPollDurationMs,
            trackedDevices: this.devices.size,
            trackedCells: this.clusterState.knownCells.size,
            totalAlerts: this.totalAlertsGenerated,
          },
        });
      }

      // Push alerts to main alert system (recordAlert + pushSSE in server.js)
      if (allAlerts.length > 0 && onAlertCallback) {
        for (const alert of allAlerts) {
          onAlertCallback(alert);
        }
      }

      // Only log when there's actual data (suppress idle noise)
      if (unique.length > 0) {
        console.log(`[RSU-MON] ${this.orgName}: poll #${this.pollCount} — ${unique.length} new measurements, ${allAlerts.length} alerts (${this.lastPollDurationMs}ms)`);
      }
    } catch (err) {
      this.lastPollTime = new Date().toISOString();
      this.lastPollDurationMs = Date.now() - t0;
      console.error(`[RSU-MON] ${this.orgName}: poll error:`, err.message);
    } finally {
      this._pollInFlight = false;
    }
  }

  /**
   * Check for devices that haven't sent data recently.
   */
  _checkOfflineDevices() {
    const alerts = [];
    const threshold = config.agent.rsuOfflineThresholdMs || 30000;
    const now = Date.now();
    const cooldownMs = (config.agent.rsuAlertCooldowns.RSU_DEVICE_OFFLINE || 60) * 1000;

    for (const [deviceId, state] of this.devices) {
      if (state.lastSeenAt && (now - state.lastSeenAt) > threshold) {
        // Check cooldown
        const lastAlert = state.lastAlertTime.RSU_DEVICE_OFFLINE;
        if (lastAlert && (now - lastAlert) < cooldownMs) continue;

        const offlineSecs = Math.round((now - state.lastSeenAt) / 1000);
        alerts.push({
          rule: 'RSU_DEVICE_OFFLINE',
          severity: 'MEDIUM',
          score: 50,
          device_id: deviceId,
          timestamp: new Date().toISOString(),
          details: `RSU device offline for ${offlineSecs}s — no data received`,
        });
        state.lastAlertTime.RSU_DEVICE_OFFLINE = now;
        this.totalAlertsGenerated++;
      }
    }
    return alerts;
  }

  /**
   * Push data to all SSE clients for this org.
   */
  _pushSSE(data) {
    const payload = `data: ${JSON.stringify(data)}\n\n`;
    for (const client of rsuSSEClients) {
      if (client.orgId === this.orgId) {
        try {
          client.res.write(payload);
        } catch {
          rsuSSEClients.delete(client);
        }
      }
    }
  }

  /**
   * Start the poll loop.
   */
  startPolling() {
    const intervalMs = config.agent.rsuPollIntervalMs || 5000;
    console.log(`[RSU-MON] ${this.orgName}: starting poll loop (${intervalMs}ms interval)`);
    this.intervalHandle = setInterval(() => this.poll(), intervalMs);
  }

  /**
   * Stop the poll loop.
   */
  stop() {
    if (this.intervalHandle) {
      clearInterval(this.intervalHandle);
      this.intervalHandle = null;
    }
  }

  /**
   * Get status snapshot for debug endpoint.
   */
  getStatus() {
    const deviceStatuses = [];
    for (const [deviceId, state] of this.devices) {
      const offlineMs = state.lastSeenAt ? Date.now() - state.lastSeenAt : null;
      deviceStatuses.push({
        device_id: deviceId,
        online: offlineMs != null && offlineMs < (config.agent.rsuOfflineThresholdMs || 30000),
        lastSeenAgo: offlineMs != null ? `${Math.round(offlineMs / 1000)}s` : 'never',
        knownCells: state.knownCells.size,
        knownTACs: state.knownTACs.size,
        baselineRSRP: state.baselineRSRP ? state.baselineRSRP.mean.toFixed(1) + 'dBm' : null,
        baselineSats: state.baselineSatCount ? state.baselineSatCount.mean.toFixed(0) : null,
        anchorPosition: state.anchorPosition || null,
        activeCooldowns: Object.entries(state.lastAlertTime)
          .filter(([, t]) => (Date.now() - t) < 300000)
          .map(([rule, t]) => ({ rule, agoSecs: Math.round((Date.now() - t) / 1000) })),
      });
    }

    return {
      orgId: this.orgId,
      orgName: this.orgName,
      bbox: this.bbox,
      expectedMCCs: this.expectedMCCs,
      watermark: this.watermark,
      pollCount: this.pollCount,
      lastPollTime: this.lastPollTime,
      lastPollDurationMs: this.lastPollDurationMs,
      lastMeasurementTime: this.lastMeasurementTime,
      totalMeasurementsProcessed: this.totalMeasurementsProcessed,
      totalAlertsGenerated: this.totalAlertsGenerated,
      trackedDevices: this.devices.size,
      trackedCells: this.clusterState.knownCells.size,
      knownSites: this.clusterState.sitesIndex.size,
      knownCells: [...this.clusterState.knownCells.entries()].map(([key, val]) => ({
        key,
        firstSeen: val.firstSeen,
        lastSeen: val.lastSeen,
        deviceCount: val.devices.size,
        mcc: val.mcc,
        tac: val.tac,
      })),
      devices: deviceStatuses,
      sseClients: [...rsuSSEClients].filter(c => c.orgId === this.orgId).length,
    };
  }
}

// ---------------------------------------------------------------------------
// Normalize modem_measurements rows to match measurements field names
// ---------------------------------------------------------------------------

function normalizeModemRow(m) {
  return {
    ...m,
    // Map modem_measurements fields → measurements field names
    signal_rsrp: m.signal,
    signal_rsrq: m.quality,
    signal_snr: m.sinr,
    signal_rssi: m.rssi,
    signal_timingAdvance: null, // modem_measurements doesn't have TA
    signal_txPower: m.tx_power,
    signal_cqi: m.cqi,
    cell_pci: m.pcid,
    cell_enb: m.enodeb_id,
    cell_tac: m.tac,
    cell_eci: m.cellid,
    tech: m.rat,
    band_downlinkEarfcn: m.frequency,
    band_name: m.band,
    band_bandwidth: m.bandwidth,
    network_PLMN: (m.mcc && m.mnc) ? `${m.mcc}-${m.mnc}` : null,
    network_mcc: m.mcc,
    network_mnc: m.mnc,
    location_lat_rounded: m.lat,
    location_lng_rounded: m.lng,
    location_accuracy: m.horizontal_accuracy || m.position_accuracy,
    location_altitude: m.altitude,
    location_speed: m.loc_speed,
    location_heading: m.heading,
    satellites_gnss_satellitesNo: m.satellites_used,
    internet_latency: m.average_rtt,
    internet_jitter: m.jitter,
    internet_latencyLoss: m.packet_loss,
    internet_downloadMbps: m.download_mbps,
    internet_uploadMbps: m.upload_mbps,
    deviceInfo_deviceId: m.serial_number,
    deviceInfo_uptime: m.uptime,
    _source: 'modem_measurements',
  };
}

// ---------------------------------------------------------------------------
// Module API
// ---------------------------------------------------------------------------

/**
 * Start RSU monitoring for all eligible organizations.
 * Called once on server startup.
 */
async function start() {
  const orgs = orgStore.getOrgs();
  const rsuOrgs = orgs.filter(o =>
    (o.license === 'RSU' || o.license === 'BOTH') && o.cluster
  );

  if (rsuOrgs.length === 0) {
    console.log('[RSU-MON] No RSU orgs with cluster polygons found — monitor inactive');
    return;
  }

  console.log(`[RSU-MON] Starting monitors for ${rsuOrgs.length} org(s)`);

  // Start each org in parallel — don't let one org's slow bootstrap block another
  for (const org of rsuOrgs) {
    const monitor = new OrgMonitor(org);
    monitors.set(org.id, monitor);
    // Fire-and-forget: bootstrap + start polling independently
    monitor.bootstrap().then(() => {
      monitor.startPolling();
    }).catch(err => {
      console.error(`[RSU-MON] Failed to start monitor for ${org.name}:`, err.message);
    });
  }
}

/**
 * Stop all monitors.
 */
function stop() {
  for (const [, monitor] of monitors) {
    monitor.stop();
  }
  monitors.clear();
  console.log('[RSU-MON] All monitors stopped');
}

/**
 * Get status for all monitors (debug endpoint).
 */
function getStatus() {
  const statuses = [];
  for (const [, monitor] of monitors) {
    statuses.push(monitor.getStatus());
  }
  return {
    active: monitors.size > 0,
    monitors: statuses,
    totalSSEClients: rsuSSEClients.size,
    pollIntervalMs: config.agent.rsuPollIntervalMs,
  };
}

/**
 * Get status for a specific org.
 */
function getOrgStatus(orgId) {
  const monitor = monitors.get(orgId);
  return monitor ? monitor.getStatus() : null;
}

/**
 * Register an SSE client for RSU streaming.
 */
function addSSEClient(res, orgId) {
  const client = { res, orgId };
  rsuSSEClients.add(client);
  return client;
}

/**
 * Remove an SSE client.
 */
function removeSSEClient(client) {
  rsuSSEClients.delete(client);
}

/**
 * Set callback for pushing alerts to main alert system.
 * Called by server.js on startup: rsuMonitor.setOnAlert(fn)
 */
function setOnAlert(fn) {
  onAlertCallback = fn;
}

/**
 * Toggle which data sources the monitor polls.
 * @param {string} orgId - org to configure (null = all orgs)
 * @param {{ useModemMeasurements: boolean, useMeasurements: boolean }} sources
 */
function setDataSources(orgId, sources) {
  for (const [id, monitor] of monitors) {
    if (orgId && id !== orgId) continue;
    monitor.dataSources = {
      useModemMeasurements: sources.useModemMeasurements !== false,
      useMeasurements: sources.useMeasurements === true,
    };
    console.log(`[RSU-MON] ${monitor.orgName}: data sources → modem_measurements=${monitor.dataSources.useModemMeasurements}, measurements=${monitor.dataSources.useMeasurements}`);
  }
}

module.exports = {
  start,
  stop,
  getStatus,
  getOrgStatus,
  addSSEClient,
  removeSSEClient,
  setOnAlert,
  setDataSources,
};
