/**
 * demoGenerator.js — Synthetic measurement generator for demo orgs
 *
 * Creates realistic-looking cellular measurements with injected anomalies,
 * then runs them through the real rules engine so demo orgs see actual
 * detection logic in action.
 *
 * Anomaly types injected:
 *   - IMSI catcher: TA=0 cluster with 3+ devices on same PCI
 *   - Rogue MCC: MCC 001 (test network) in production area
 *   - Jamming: strong RSRP + poor RSRQ signature
 *   - GPS spoofing: TA implies distance but device reports nearby location
 */
const crypto = require('crypto');

// Israeli operator PLMNs for realistic base data
const LEGIT_PLMNS = ['425-01', '425-02', '425-03'];
const LEGIT_MCCS = ['425'];
const BANDS = [
  { band: 3, earfcn: 1300, freq: 1800, name: 'B3' },
  { band: 7, earfcn: 2850, freq: 2600, name: 'B7' },
  { band: 20, earfcn: 6200, freq: 800, name: 'B20' },
  { band: 1, earfcn: 100, freq: 2100, name: 'B1' },
];

function rand(min, max) {
  return Math.random() * (max - min) + min;
}

function randInt(min, max) {
  return Math.floor(rand(min, max + 1));
}

function pick(arr) {
  return arr[randInt(0, arr.length - 1)];
}

function generateSampleId() {
  return 'demo-' + crypto.randomBytes(8).toString('hex');
}

function generateDeviceId() {
  return 'DEMO-' + crypto.randomBytes(4).toString('hex').toUpperCase();
}

/**
 * Generate synthetic measurements for a demo org.
 *
 * @param {Object} org — the org object with cluster polygon and demo_rsus
 * @param {Object} options — { count, anomalyRatio, mode }
 * @returns {{ measurements: Array, expectedMCC: string[] }}
 */
function generateDemoData(org, options = {}) {
  const count = options.count || 200;
  const anomalyRatio = options.anomalyRatio || 0.15;
  const mode = options.mode || 'SDK';

  // Derive bbox from cluster polygon
  const bbox = clusterToBbox(org.cluster);
  if (!bbox) return { measurements: [], expectedMCC: LEGIT_MCCS };

  const measurements = [];
  const anomalyCount = Math.floor(count * anomalyRatio);
  const normalCount = count - anomalyCount;

  // Generate device pool (5-15 devices)
  const devicePool = [];
  const numDevices = randInt(5, 15);
  for (let i = 0; i < numDevices; i++) {
    devicePool.push(generateDeviceId());
  }

  // RSU device IDs if in RSU mode
  const rsuDevices = (org.demo_rsus || []).map(r => r.device_id);
  if (mode === 'RSU' && rsuDevices.length === 0) {
    rsuDevices.push('DEMO-RSU-001', 'DEMO-RSU-002');
  }

  const now = new Date();

  // --- Normal measurements ---
  for (let i = 0; i < normalCount; i++) {
    measurements.push(generateNormalMeasurement(bbox, devicePool, rsuDevices, mode, now, i));
  }

  // --- Anomaly injections ---
  const anomalyTypes = ['imsi_catcher', 'rogue_mcc', 'jamming', 'gps_spoof'];
  const anomaliesPerType = Math.ceil(anomalyCount / anomalyTypes.length);

  // IMSI catcher: TA=0 cluster — 3+ devices, same PCI, same EARFCN
  const imsiPci = randInt(10, 400);
  const imsiBand = pick(BANDS);
  const imsiLat = rand(bbox.latMin, bbox.latMax);
  const imsiLng = rand(bbox.lngMin, bbox.lngMax);
  for (let i = 0; i < anomaliesPerType; i++) {
    const m = generateBaseMeasurement(bbox, devicePool, rsuDevices, mode, now, normalCount + i);
    m.cell_pci = imsiPci;
    m.band_downlinkFrequency = imsiBand.earfcn;
    m.band_number = imsiBand.band;
    m.band_name = imsiBand.name;
    m.signal_timingAdvance = 0; // TA=0 — IMSI catcher signature
    m.signal_rsrp = rand(-65, -45); // Very strong signal (close range)
    m.location_lat_rounded = imsiLat + rand(-0.002, 0.002);
    m.location_lng_rounded = imsiLng + rand(-0.002, 0.002);
    m.deviceInfo_deviceId = pick(devicePool); // different devices
    measurements.push(m);
  }

  // Rogue MCC: MCC 001 (test network) — shouldn't exist in production
  for (let i = 0; i < anomaliesPerType; i++) {
    const m = generateNormalMeasurement(bbox, devicePool, rsuDevices, mode, now, normalCount + anomaliesPerType + i);
    m.network_mcc = '001';
    m.network_PLMN = '001-01';
    m.network_operator = 'TEST NETWORK';
    measurements.push(m);
  }

  // Jamming: strong RSRP but terrible RSRQ — RF interference signature
  const jamLat = rand(bbox.latMin, bbox.latMax);
  const jamLng = rand(bbox.lngMin, bbox.lngMax);
  for (let i = 0; i < anomaliesPerType; i++) {
    const m = generateNormalMeasurement(bbox, devicePool, rsuDevices, mode, now, normalCount + anomaliesPerType * 2 + i);
    m.signal_rsrp = rand(-70, -50); // Strong signal
    m.signal_rsrq = rand(-25, -18); // Terrible quality — jamming
    m.signal_snr = rand(-5, 2);     // Poor SNR
    m.location_lat_rounded = jamLat + rand(-0.005, 0.005);
    m.location_lng_rounded = jamLng + rand(-0.005, 0.005);
    measurements.push(m);
  }

  // GPS spoofing: TA implies far distance but GPS says nearby
  for (let i = 0; i < anomaliesPerType; i++) {
    const m = generateNormalMeasurement(bbox, devicePool, rsuDevices, mode, now, normalCount + anomaliesPerType * 3 + i);
    m.signal_timingAdvance = randInt(50, 200); // TA=50+ → 4+ km away
    m.signal_rsrp = rand(-60, -45);           // But signal is very strong (should be far)
    measurements.push(m);
  }

  return { measurements, expectedMCC: LEGIT_MCCS };
}

function generateBaseMeasurement(bbox, devicePool, rsuDevices, mode, now, index) {
  const band = pick(BANDS);
  const plmn = pick(LEGIT_PLMNS);
  const [mcc, mnc] = plmn.split('-');
  const device = mode === 'RSU' && rsuDevices.length > 0 ? pick(rsuDevices) : pick(devicePool);
  const ts = new Date(now.getTime() - randInt(0, 3600000)); // within last hour

  return {
    sample_id: generateSampleId(),
    timestamp: ts.toISOString(),
    source: mode === 'RSU' ? 'modem' : 'sdk',
    cell_pci: randInt(1, 503),
    cell_eci: randInt(100000, 9999999),
    cell_enb: randInt(1000, 99999),
    cell_tac: randInt(1, 65535),
    tech: 'LTE',
    band_downlinkFrequency: band.earfcn,
    band_number: band.band,
    band_name: band.name,
    band_bandwidth: pick([5, 10, 15, 20]),
    network_PLMN: plmn,
    network_mcc: mcc,
    network_mnc: mnc,
    network_operator: mcc === '425' ? pick(['Partner', 'Cellcom', 'Pelephone']) : 'Unknown',
    signal_rsrp: rand(-110, -65),
    signal_rsrq: rand(-15, -5),
    signal_snr: rand(5, 25),
    signal_rssi: rand(-95, -55),
    signal_timingAdvance: randInt(0, 30),
    location_lat_rounded: rand(bbox.latMin, bbox.latMax),
    location_lng_rounded: rand(bbox.lngMin, bbox.lngMax),
    location_altitude: rand(0, 500),
    location_speed: rand(0, 120),
    location_accuracy: rand(5, 50),
    deviceInfo_deviceId: device,
    deviceInfo_deviceModel: mode === 'RSU' ? 'Quectel RM520N' : 'SDK Device',
    neighbors: randInt(0, 6) > 0 ? `PCI:${randInt(1,503)},PCI:${randInt(1,503)}` : '',
    _demo: true,
  };
}

function generateNormalMeasurement(bbox, devicePool, rsuDevices, mode, now, index) {
  return generateBaseMeasurement(bbox, devicePool, rsuDevices, mode, now, index);
}

function clusterToBbox(cluster) {
  if (!cluster || !cluster.coordinates || !cluster.coordinates[0]) return null;
  const coords = cluster.coordinates[0];
  let latMin = 90, latMax = -90, lngMin = 180, lngMax = -180;
  for (const [lng, lat] of coords) {
    if (lat < latMin) latMin = lat;
    if (lat > latMax) latMax = lat;
    if (lng < lngMin) lngMin = lng;
    if (lng > lngMax) lngMax = lng;
  }
  return { latMin, lngMin, latMax, lngMax };
}

module.exports = { generateDemoData };
