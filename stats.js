const config = require('./config');
const db = require('./db');
const { CRITICAL, HIGH, MEDIUM } = config.severity;

/**
 * Z-score computation. Returns 0 if stddev is zero.
 */
function zScore(value, mean, std) {
  if (!std || std === 0) return 0;
  return (value - mean) / std;
}

function scoreFromZ(z) {
  return Math.min(0.9, 0.5 + Math.abs(z) * 0.1);
}

/**
 * Statistical anomaly scoring — z-score analysis against 24h baselines.
 *
 * Uses cell_pci as baseline key (most consistently populated field
 * from Android CellIdentityLte.getPci()).
 *
 * Baselines are per-cell rolling 24h averages from db.getCellBaselines().
 */
async function runStatistics(measurements, baselines) {
  const flags = [];
  const locationCounts = {};

  for (const m of measurements) {
    const cellKey = String(m.cell_pci || '');
    const bl = baselines[cellKey];
    if (!bl) continue;

    const locationKey = `${m.location_lat_rounded},${m.location_lng_rounded}`;
    if (!locationCounts[locationKey]) {
      locationCounts[locationKey] = { total: 0, flagged: 0, samples: [] };
    }
    locationCounts[locationKey].total++;

    let flaggedThisSample = false;
    const baseFlag = {
      cell_id: cellKey,
      cell_ecgi: m.cell_ecgi || '',
      location_lat: m.location_lat_rounded,
      location_lng: m.location_lng_rounded,
    };

    // --- SNR z-score ---
    const snr = m.signal_snr !== null && m.signal_snr !== undefined ? Number(m.signal_snr) : null;
    if (snr !== null) {
      const z = zScore(snr, Number(bl.avg_snr), Number(bl.std_snr));
      if (Math.abs(z) > 3) {
        flaggedThisSample = true;
        flags.push({ sample_id: m.sample_id, stat_check: 'SNR_ZSCORE', z_score: z,
          severity: HIGH, score: scoreFromZ(z),
          details: `SNR ${snr}dB vs baseline avg=${Number(bl.avg_snr).toFixed(1)}, z=${z.toFixed(2)} — possible jamming`,
          ...baseFlag });
      }
    }

    // --- Timing Advance z-score ---
    const ta = m.signal_timingAdvance !== null && m.signal_timingAdvance !== undefined ? Number(m.signal_timingAdvance) : null;
    if (ta !== null) {
      const z = zScore(ta, Number(bl.avg_ta), Number(bl.std_ta));
      if (Math.abs(z) > 3) {
        flaggedThisSample = true;
        flags.push({ sample_id: m.sample_id, stat_check: 'TA_ZSCORE', z_score: z,
          severity: MEDIUM, score: scoreFromZ(z),
          details: `TA=${ta} vs baseline avg=${Number(bl.avg_ta).toFixed(1)}, z=${z.toFixed(2)}`,
          ...baseFlag });
      }
    }

    if (flaggedThisSample) {
      locationCounts[locationKey].flagged++;
      locationCounts[locationKey].samples.push(m.sample_id);
    }
  }

  // --- CLUSTER_ANOMALY: >80% of devices at same location flagged ---
  for (const [loc, counts] of Object.entries(locationCounts)) {
    if (counts.total >= 3 && counts.flagged / counts.total > 0.8) {
      const [lat, lng] = loc.split(',');
      for (const sid of counts.samples) {
        flags.push({ sample_id: sid, stat_check: 'CLUSTER_ANOMALY', z_score: 0,
          severity: CRITICAL, score: 0.95,
          details: `${counts.flagged}/${counts.total} devices anomalous at (${lat},${lng}) — coordinated event`,
          cell_id: '', cell_ecgi: '',
          location_lat: parseFloat(lat), location_lng: parseFloat(lng) });
      }
    }
  }

  return flags;
}

module.exports = { runStatistics };
