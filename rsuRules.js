/**
 * rsuRules.js — Change-based detection rules for RSU real-time monitoring.
 *
 * RSUs are STATIC sensors. Alerts fire on STATE TRANSITIONS (something changed
 * from baseline), not on raw thresholds every measurement. An alert fires ONCE
 * when the anomaly begins. Repeated alerts for the same ongoing condition are
 * suppressed by state flags (jammingActive, foreignMCCActive, etc.).
 *
 * CRITICAL: Only evaluate SERVING CELL measurements (has enodeb_id).
 * The modem_measurements table contains both serving and neighbor cell data.
 * Neighbor cells have no enodeb_id and use inter-frequency RSRP/RSRQ values
 * that would cause false positives.
 */
'use strict';

const config = require('./config');

const CRITICAL = 'CRITICAL';
const HIGH = 'HIGH';
const MEDIUM = 'MEDIUM';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function extractMCC(m) {
  if (m.network_mcc) return String(m.network_mcc).replace(/\D/g, '').substring(0, 3);
  if (m.mcc) return String(m.mcc).replace(/\D/g, '').substring(0, 3);
  return null;
}

function extractPCI(m) { return m.cell_pci || m.pcid || null; }

function extractENB(m) {
  if (m.cell_enb != null && m.cell_enb !== '' && m.cell_enb !== '0' && m.cell_enb !== 0) return String(m.cell_enb);
  if (m.enodeb_id != null && m.enodeb_id !== '' && m.enodeb_id !== '0' && m.enodeb_id !== 0) return String(m.enodeb_id);
  return null;
}

function extractEARFCN(m) { return m.band_downlinkEarfcn || m.frequency || null; }
function extractTAC(m) { return m.cell_tac || m.tac || null; }

function extractRSRP(m) {
  const v = m.signal_rsrp != null ? Number(m.signal_rsrp) : (m.signal != null ? Number(m.signal) : null);
  return (v != null && !isNaN(v)) ? v : null;
}

function extractRSRQ(m) {
  const v = m.signal_rsrq != null ? Number(m.signal_rsrq) : (m.quality != null ? Number(m.quality) : null);
  return (v != null && !isNaN(v)) ? v : null;
}

function extractTech(m) { return m.tech || m.rat || null; }

function extractLat(m) {
  const v = m.location_lat_rounded != null ? Number(m.location_lat_rounded) : (m.lat != null ? Number(m.lat) : null);
  return (v != null && !isNaN(v) && v !== 0) ? v : null;
}

function extractLng(m) {
  const v = m.location_lng_rounded != null ? Number(m.location_lng_rounded) : (m.lng != null ? Number(m.lng) : null);
  return (v != null && !isNaN(v) && v !== 0) ? v : null;
}

function extractSatCount(m) {
  if (m.satellites_gnss_satellitesNo != null) return Number(m.satellites_gnss_satellitesNo);
  if (m.satellites_used != null) return Number(m.satellites_used);
  return null;
}

function extractAccuracy(m) {
  const v = m.location_accuracy != null ? Number(m.location_accuracy)
    : (m.horizontal_accuracy != null ? Number(m.horizontal_accuracy) : null);
  return (v != null && !isNaN(v)) ? v : null;
}

function extractDeviceId(m) { return m.deviceInfo_deviceId || m.serial_number || null; }

/**
 * Is this a SERVING cell measurement (not a neighbor scan)?
 * Serving cells have enodeb_id populated. Neighbor cells don't.
 */
function isServingCell(m) {
  const enb = extractENB(m);
  return enb != null;
}

function distanceMeters(lat1, lng1, lat2, lng2) {
  const R = 6371000;
  const toRad = x => x * Math.PI / 180;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function cellKey(pci, enb, earfcn) { return `${pci || '?'}_${enb || '?'}_${earfcn || '?'}`; }

// ---------------------------------------------------------------------------
// Rule functions — all change-based, serving cell only
// ---------------------------------------------------------------------------

/**
 * RSU_MCC_MISMATCH — Serving cell MCC changed to a foreign value.
 * Fires ONCE on transition. Clears when MCC returns to expected.
 */
function checkMCCMismatch(m, deviceState, _clusterState, orgConfig) {
  if (!isServingCell(m)) return null;
  const mcc = extractMCC(m);
  if (!mcc || mcc === '0' || mcc === '001') return null;

  const expected = orgConfig.expectedMCCs;
  if (!expected || expected.length === 0) return null;

  const isForeign = !expected.includes(mcc);

  if (isForeign && !deviceState.foreignMCCActive) {
    // ONSET: transition to foreign MCC
    deviceState.foreignMCCActive = true;
    const info = config.getMCCInfo(mcc);
    return {
      rule: 'RSU_MCC_MISMATCH',
      severity: CRITICAL,
      score: 95,
      details: `Foreign MCC ${mcc} (${info.country} ${info.flag}) detected — expected ${expected.join('/')}`,
    };
  } else if (!isForeign && deviceState.foreignMCCActive) {
    // RECOVERY: back to expected MCC
    deviceState.foreignMCCActive = false;
  }
  return null;
}

/**
 * RSU_NEW_CELL — PCI+eNB+EARFCN combo never seen before in this cluster.
 * Only serving cells (with eNB). Fires once per new cell.
 */
function checkNewCell(m, _deviceState, clusterState, _orgConfig) {
  if (!isServingCell(m)) return null;
  const pci = extractPCI(m);
  const enb = extractENB(m);
  const earfcn = extractEARFCN(m);
  if (pci == null) return null;

  const key = cellKey(pci, enb, earfcn);
  if (clusterState.knownCells.has(key)) return null;

  clusterState.knownCells.set(key, {
    firstSeen: m.timestamp, lastSeen: m.timestamp,
    devices: new Set([extractDeviceId(m)]),
    mcc: extractMCC(m), tac: extractTAC(m),
  });

  return {
    rule: 'RSU_NEW_CELL',
    severity: HIGH,
    score: 80,
    details: `New cell detected: PCI=${pci} eNB=${enb} EARFCN=${earfcn || '?'} MCC=${extractMCC(m) || '?'}`,
  };
}

/**
 * RSU_CELL_CHANGE — Serving cell changed (handover).
 * On a fixed sensor, serving cell changes are unusual.
 */
function checkCellChange(m, deviceState, _clusterState, _orgConfig) {
  if (!isServingCell(m)) return null;
  const pci = extractPCI(m);
  const enb = extractENB(m);
  if (pci == null || enb == null) return null;

  const currentCell = `${pci}_${enb}`;
  const prevCell = deviceState.lastServingCell;

  if (prevCell && prevCell !== currentCell) {
    deviceState.lastServingCell = currentCell;
    return {
      rule: 'RSU_CELL_CHANGE',
      severity: MEDIUM,
      score: 50,
      details: `Serving cell changed: ${prevCell} → ${currentCell}`,
    };
  }

  deviceState.lastServingCell = currentCell;
  return null;
}

/**
 * RSU_TAC_JUMP — TAC changed on same PCI+eNB between consecutive serving measurements.
 */
function checkTACJump(m, deviceState, _clusterState, _orgConfig) {
  if (!isServingCell(m)) return null;
  const tac = extractTAC(m);
  const pci = extractPCI(m);
  const enb = extractENB(m);
  if (tac == null || pci == null) return null;

  const prev = deviceState.lastServingMeasurement;
  if (!prev) return null;

  const prevTac = extractTAC(prev);
  const prevPci = extractPCI(prev);
  const prevEnb = extractENB(prev);

  if (prevPci == pci && prevEnb == enb && prevTac != null && prevTac != tac) {
    return {
      rule: 'RSU_TAC_JUMP',
      severity: HIGH,
      score: 75,
      details: `TAC changed ${prevTac} → ${tac} on cell PCI=${pci} eNB=${enb}`,
    };
  }
  return null;
}

/**
 * RSU_SIGNAL_DEGRADATION — RSRP drops >15dB from rolling baseline.
 * Already change-based. Serving cell only.
 */
function checkSignalDegradation(m, deviceState, _clusterState, _orgConfig) {
  if (!isServingCell(m)) return null;
  const rsrp = extractRSRP(m);
  if (rsrp == null) return null;

  const baseline = deviceState.baselineRSRP;
  if (!baseline || baseline.count < 10) return null;

  const dropDB = baseline.mean - rsrp;
  if (dropDB > 15 && !deviceState.signalDegradationActive) {
    deviceState.signalDegradationActive = true;
    return {
      rule: 'RSU_SIGNAL_DEGRADATION',
      severity: MEDIUM,
      score: 60,
      details: `RSRP dropped ${dropDB.toFixed(1)}dB (current: ${rsrp}dBm, baseline: ${baseline.mean.toFixed(1)}dBm)`,
    };
  } else if (dropDB <= 8) {
    // Recovery: within 8dB of baseline
    deviceState.signalDegradationActive = false;
  }
  return null;
}

/**
 * RSU_CELLULAR_JAMMING — RSRQ degrades significantly from baseline while RSRP stays strong.
 * State-machine: fires ONCE on onset, clears on recovery.
 * Serving cell only.
 */
function checkCellularJamming(m, deviceState, _clusterState, _orgConfig) {
  if (!isServingCell(m)) return null;
  const rsrp = extractRSRP(m);
  const rsrq = extractRSRQ(m);
  if (rsrp == null || rsrq == null) return null;

  const rsrqBaseline = deviceState.baselineRSRQ;
  if (!rsrqBaseline || rsrqBaseline.count < 10) return null;

  // Jamming signature: RSRQ drops >10dB from baseline AND RSRP is strong (> -90 dBm)
  const rsrqDrop = rsrqBaseline.mean - rsrq;
  const isJamming = rsrqDrop > 10 && rsrp > -90;

  if (isJamming && !deviceState.jammingActive) {
    // ONSET of jamming
    deviceState.jammingActive = true;
    return {
      rule: 'RSU_CELLULAR_JAMMING',
      severity: CRITICAL,
      score: 90,
      details: `Jamming onset: RSRQ dropped ${rsrqDrop.toFixed(1)}dB from baseline (${rsrqBaseline.mean.toFixed(1)} → ${rsrq}dB) while RSRP=${rsrp}dBm`,
    };
  } else if (!isJamming && deviceState.jammingActive && rsrqDrop < 5) {
    // RECOVERY: RSRQ back within 5dB of baseline
    deviceState.jammingActive = false;
  }
  return null;
}

/**
 * RSU_RSRQ_DEGRADATION — Quality degradation (RSRQ drop) without the jamming RSRP signature.
 * Fires once on onset.
 */
function checkRSRQDegradation(m, deviceState, _clusterState, _orgConfig) {
  if (!isServingCell(m)) return null;
  const rsrq = extractRSRQ(m);
  if (rsrq == null) return null;

  const rsrqBaseline = deviceState.baselineRSRQ;
  if (!rsrqBaseline || rsrqBaseline.count < 10) return null;

  const rsrqDrop = rsrqBaseline.mean - rsrq;

  if (rsrqDrop > 8 && !deviceState.rsrqDegradationActive && !deviceState.jammingActive) {
    deviceState.rsrqDegradationActive = true;
    return {
      rule: 'RSU_RSRQ_DEGRADATION',
      severity: MEDIUM,
      score: 55,
      details: `RSRQ degraded ${rsrqDrop.toFixed(1)}dB from baseline (${rsrqBaseline.mean.toFixed(1)} → ${rsrq}dB)`,
    };
  } else if (rsrqDrop <= 4) {
    deviceState.rsrqDegradationActive = false;
  }
  return null;
}

/**
 * RSU_GPS_JAMMING — Satellite count drops significantly from baseline.
 * State-machine: fires ONCE on onset, clears on recovery.
 */
function checkGPSJamming(m, deviceState, _clusterState, _orgConfig) {
  const sats = extractSatCount(m);
  const accuracy = extractAccuracy(m);
  const baseline = deviceState.baselineSatCount;

  // Satellite count transition
  if (sats != null && baseline && baseline.mean > 8) {
    if (sats < 4 && !deviceState.gpsJammingActive) {
      deviceState.gpsJammingActive = true;
      return {
        rule: 'RSU_GPS_JAMMING',
        severity: HIGH,
        score: 85,
        details: `GPS jamming onset: satellites dropped to ${sats} (baseline: ${baseline.mean.toFixed(0)})`,
      };
    } else if (sats > 6 && deviceState.gpsJammingActive) {
      deviceState.gpsJammingActive = false;
    }
  }

  // Accuracy transition
  if (accuracy != null && !deviceState.gpsJammingActive) {
    const baselineAcc = deviceState.baselineAccuracy;
    if (baselineAcc && baselineAcc.mean < 10 && accuracy > 50 && !deviceState.gpsAccuracyDegraded) {
      deviceState.gpsAccuracyDegraded = true;
      return {
        rule: 'RSU_GPS_JAMMING',
        severity: HIGH,
        score: 80,
        details: `GPS accuracy degraded: ${accuracy.toFixed(0)}m (baseline: ${baselineAcc.mean.toFixed(0)}m)`,
      };
    } else if (accuracy < 15 && deviceState.gpsAccuracyDegraded) {
      deviceState.gpsAccuracyDegraded = false;
    }
  }

  return null;
}

/**
 * RSU_GPS_SPOOFING — RSU location jumps >100m from anchor.
 * Already change-based (compares against fixed anchor).
 */
function checkGPSSpoofing(m, deviceState, _clusterState, _orgConfig) {
  const lat = extractLat(m);
  const lng = extractLng(m);
  if (lat == null || lng == null) return null;

  const anchor = deviceState.anchorPosition;
  if (!anchor) return null;

  const dist = distanceMeters(anchor.lat, anchor.lng, lat, lng);
  if (dist > 100 && !deviceState.gpsSpoofingActive) {
    deviceState.gpsSpoofingActive = true;
    return {
      rule: 'RSU_GPS_SPOOFING',
      severity: CRITICAL,
      score: 95,
      details: `RSU position jumped ${dist.toFixed(0)}m from anchor — GPS spoofing suspected`,
    };
  } else if (dist < 30 && deviceState.gpsSpoofingActive) {
    deviceState.gpsSpoofingActive = false;
  }
  return null;
}

/**
 * RSU_NETWORK_DOWNGRADE — Tech dropped from LTE/NR to 2G.
 * Serving cell only. Already change-based (compares consecutive).
 */
function checkNetworkDowngrade(m, deviceState, _clusterState, _orgConfig) {
  if (!isServingCell(m)) return null;
  const tech = extractTech(m);
  if (!tech) return null;

  const prev = deviceState.lastServingMeasurement;
  if (!prev) return null;
  const prevTech = extractTech(prev);
  if (!prevTech) return null;

  const advancedTechs = ['LTE', 'LTE-A', 'NR', 'NR-NSA', 'NR-SA', '5G', 'LTE_CA'];
  const legacyTechs = ['GSM', 'GPRS', 'EDGE', '2G'];

  const wasAdvanced = advancedTechs.some(t => prevTech.toUpperCase().includes(t));
  const isLegacy = legacyTechs.some(t => tech.toUpperCase().includes(t));

  if (wasAdvanced && isLegacy) {
    return {
      rule: 'RSU_NETWORK_DOWNGRADE',
      severity: HIGH,
      score: 75,
      details: `Network downgraded: ${prevTech} → ${tech}`,
    };
  }
  return null;
}

/**
 * RSU_UNKNOWN_SITE — Connected to unknown eNB (not in sites DB).
 * Fires ONCE per unknown eNB (tracked in knownUnknownENBs set).
 */
function checkUnknownSite(m, deviceState, clusterState, _orgConfig) {
  if (!isServingCell(m)) return null;
  const enb = extractENB(m);
  const pci = extractPCI(m);
  if (enb == null) return null;

  const sitesIndex = clusterState.sitesIndex;
  if (!sitesIndex || sitesIndex.size === 0) return null;

  if (sitesIndex.has(enb)) return null; // known site

  // Already alerted on this unknown eNB?
  if (deviceState.knownUnknownENBs.has(enb)) return null;

  deviceState.knownUnknownENBs.add(enb);
  return {
    rule: 'RSU_UNKNOWN_SITE',
    severity: HIGH,
    score: 70,
    details: `Connected to unknown cell tower: eNB=${enb} PCI=${pci} — not in sites database`,
  };
}

// ---------------------------------------------------------------------------
// Rule registry
// ---------------------------------------------------------------------------

const ALL_RULES = [
  { name: 'RSU_MCC_MISMATCH', fn: checkMCCMismatch },
  { name: 'RSU_NEW_CELL', fn: checkNewCell },
  { name: 'RSU_CELL_CHANGE', fn: checkCellChange },
  { name: 'RSU_TAC_JUMP', fn: checkTACJump },
  { name: 'RSU_SIGNAL_DEGRADATION', fn: checkSignalDegradation },
  { name: 'RSU_CELLULAR_JAMMING', fn: checkCellularJamming },
  { name: 'RSU_RSRQ_DEGRADATION', fn: checkRSRQDegradation },
  { name: 'RSU_GPS_JAMMING', fn: checkGPSJamming },
  { name: 'RSU_GPS_SPOOFING', fn: checkGPSSpoofing },
  { name: 'RSU_NETWORK_DOWNGRADE', fn: checkNetworkDowngrade },
  { name: 'RSU_UNKNOWN_SITE', fn: checkUnknownSite },
];

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

function runRSURules(measurement, deviceState, clusterState, orgConfig) {
  const alerts = [];
  const cooldowns = config.agent.rsuAlertCooldowns;
  const now = Date.now();

  for (const rule of ALL_RULES) {
    const lastAlert = deviceState.lastAlertTime[rule.name];
    const cooldownMs = (cooldowns[rule.name] || 0) * 1000;
    if (cooldownMs > 0 && lastAlert && (now - lastAlert) < cooldownMs) continue;

    try {
      const alert = rule.fn(measurement, deviceState, clusterState, orgConfig);
      if (alert) {
        alert.device_id = extractDeviceId(measurement);
        alert.sample_id = measurement.sample_id || null;
        alert.timestamp = measurement.timestamp || new Date().toISOString();
        alert.cell_id = extractPCI(measurement);
        alert.cell_enb = extractENB(measurement);
        alert.location_lat = extractLat(measurement);
        alert.location_lng = extractLng(measurement);
        alert.network_mcc = extractMCC(measurement);
        alerts.push(alert);
        deviceState.lastAlertTime[rule.name] = now;
      }
    } catch (err) {
      console.error(`[RSU-RULES] Error in ${rule.name}:`, err.message);
    }
  }

  return alerts;
}

/**
 * Update device state baselines with a new measurement.
 * Called AFTER running rules so rules see pre-update state.
 */
function updateDeviceState(deviceState, measurement) {
  const isServing = isServingCell(measurement);
  const rsrp = extractRSRP(measurement);
  const rsrq = extractRSRQ(measurement);
  const sats = extractSatCount(measurement);
  const accuracy = extractAccuracy(measurement);
  const lat = extractLat(measurement);
  const lng = extractLng(measurement);

  // Only update signal baselines from serving cell measurements
  if (isServing) {
    // Rolling RSRP baseline
    if (rsrp != null) {
      if (!deviceState.baselineRSRP) {
        deviceState.baselineRSRP = { mean: rsrp, count: 1 };
      } else {
        const alpha = Math.min(1 / deviceState.baselineRSRP.count, 0.05);
        deviceState.baselineRSRP.mean += alpha * (rsrp - deviceState.baselineRSRP.mean);
        deviceState.baselineRSRP.count++;
      }
    }

    // Rolling RSRQ baseline
    if (rsrq != null) {
      if (!deviceState.baselineRSRQ) {
        deviceState.baselineRSRQ = { mean: rsrq, count: 1 };
      } else {
        const alpha = Math.min(1 / deviceState.baselineRSRQ.count, 0.05);
        deviceState.baselineRSRQ.mean += alpha * (rsrq - deviceState.baselineRSRQ.mean);
        deviceState.baselineRSRQ.count++;
      }
    }

    // Track last serving measurement separately
    deviceState.lastServingMeasurement = measurement;
  }

  // GNSS baselines (not cell-specific)
  if (sats != null && sats > 0) {
    if (!deviceState.baselineSatCount) {
      deviceState.baselineSatCount = { mean: sats, min: sats, count: 1 };
    } else {
      const alpha = Math.min(1 / deviceState.baselineSatCount.count, 0.05);
      deviceState.baselineSatCount.mean += alpha * (sats - deviceState.baselineSatCount.mean);
      if (sats < deviceState.baselineSatCount.min) deviceState.baselineSatCount.min = sats;
      deviceState.baselineSatCount.count++;
    }
  }

  if (accuracy != null && accuracy > 0) {
    if (!deviceState.baselineAccuracy) {
      deviceState.baselineAccuracy = { mean: accuracy, count: 1 };
    } else {
      const alpha = Math.min(1 / deviceState.baselineAccuracy.count, 0.05);
      deviceState.baselineAccuracy.mean += alpha * (accuracy - deviceState.baselineAccuracy.mean);
      deviceState.baselineAccuracy.count++;
    }
  }

  // Anchor position — set once from first valid position
  if (!deviceState.anchorPosition && lat != null && lng != null) {
    deviceState.anchorPosition = { lat, lng };
  }

  // Track known TACs and cells
  const tac = extractTAC(measurement);
  if (tac != null) deviceState.knownTACs.add(String(tac));

  const pci = extractPCI(measurement);
  const enb = extractENB(measurement);
  const earfcn = extractEARFCN(measurement);
  if (pci != null && enb != null) {
    deviceState.knownCells.add(cellKey(pci, enb, earfcn));
  }

  // Shift measurement history
  deviceState.previousMeasurement = deviceState.lastMeasurement;
  deviceState.lastMeasurement = measurement;
  deviceState.lastSeenAt = Date.now();
}

module.exports = {
  runRSURules,
  updateDeviceState,
  extractDeviceId,
  extractMCC,
  extractPCI,
  extractENB,
  extractEARFCN,
  extractTAC,
  extractRSRP,
  extractLat,
  extractLng,
  cellKey,
  isServingCell,
};
