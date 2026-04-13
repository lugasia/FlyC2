const config = require('./config');
const db = require('./db');
const { CRITICAL, HIGH, MEDIUM, LOW } = config.severity;

/**
 * FlycommC2 Rule Engine v3 — Based on 3GPP TS 36.213/36.321 and Android CellInfo API
 *
 * Android API → ClickHouse column mapping:
 *   CellIdentityLte.getCi()            → cell_eci      (28-bit E-UTRAN Cell ID)
 *   CellIdentityLte.getPci()           → cell_pci      (0-503 LTE, 0-1008 NR)
 *   Top 20 bits of ECI = eNB ID        → cell_enb      (0-1,048,575)
 *   Full ECGI = MCC+MNC+ECI            → cell_ecgi
 *   CellIdentityLte.getTac()           → cell_tac      (16-bit Tracking Area Code)
 *   CellIdentityLte.getEarfcn()        → band_downlinkEarfcn
 *   CellSignalStrengthLte.getTimingAdvance() → signal_timingAdvance (0-1282, step≈78.12m)
 *   CellSignalStrengthLte.getRsrp()    → signal_rsrp   (-140 to -43 dBm)
 *   CellInfo.getCellConnectionStatus() → connectionStatus (REGISTERED/NONE)
 *   Network registration               → network_PLMN, network_mcc, network_mnc
 *   CellInfo subclass type             → tech (LTE/NR/WCDMA/GSM)
 *
 * sites table = known cell database:
 *   site_id = eNB ID, lat/lng = tower location
 *   sectors[].pci = Physical Cell IDs for each sector
 *   sectors[].dl_frequency_code = expected EARFCN/ARFCN per sector
 *   sectors[].tx_frequency = expected DL frequency in MHz
 */

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Haversine distance in km between two lat/lng points */
function haversineKm(lat1, lng1, lat2, lng2) {
  const R = 6371;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLng = (lng2 - lng1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
    Math.sin(dLng / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

/** Convert TA value to estimated distance in meters (LTE: each step ≈ 78.12m) */
function taToMeters(ta) {
  return ta * 78.12;
}

/** Estimate distance from RSRP using simplified Okumura-Hata (urban, ~1800MHz) */
function rsrpToEstimatedKm(rsrp, txPowerDbm = 46) {
  // Path loss = txPower - rsrp
  const pl = txPowerDbm - rsrp;
  // Simplified Okumura-Hata: PL = 137.4 + 35.2*log10(d_km) for urban 1800MHz
  // Rearranging: d_km = 10^((PL - 137.4) / 35.2)
  if (pl < 80) return 0.01; // very close
  const dKm = Math.pow(10, (pl - 137.4) / 35.2);
  return Math.max(0.01, Math.min(dKm, 200)); // clamp to reasonable range
}

// ---------------------------------------------------------------------------
// Normalize PLMN to "MCC-MNC" format (e.g. "42501" → "425-01", "425-01" stays)
// ---------------------------------------------------------------------------
function normalizePlmn(val) {
  if (!val || val === '0') return null;
  const s = String(val).trim();
  if (s === '' || s === '0') return null;
  if (s.includes('-')) return s;
  if (s.length >= 5) return s.slice(0, 3) + '-' + s.slice(3);
  return s;
}

// ---------------------------------------------------------------------------
// Build reference structures from sites table
// ---------------------------------------------------------------------------
function buildSiteIndex(knownCells, regionBbox) {
  // Primary index: "siteId:plmn" → [entries] (same site_id+PLMN can have multiple locations)
  // Fallback index: siteId → [entries] (for measurements missing PLMN or legacy paths)
  const siteByPlmn = new Map();  // "3030:425-01" → [entry1, entry2, ...]
  const siteById   = new Map();  // 3030 → [entry1, entry2, ...]
  // PCI index: pciNum → [{ site_id, plmn, lat, lng }]
  const pciIndex = new Map();

  let parsedSectors = 0;
  let totalSectors = 0;

  // Auto-detect the PLMN column name (sites table may use different names)
  const plmnCols = ['plmn', 'network_plmn', 'cell_plmn', 'mcc_mnc', 'PLMN'];
  let plmnColName = null;
  if (knownCells.length > 0) {
    const firstRow = knownCells[0];
    for (const col of plmnCols) {
      if (firstRow[col] !== undefined && firstRow[col] !== null && firstRow[col] !== '') {
        plmnColName = col;
        break;
      }
    }
    // Also try deriving PLMN from MCC+MNC columns
    if (!plmnColName && firstRow.mcc !== undefined && firstRow.mnc !== undefined) {
      plmnColName = '_derived_mcc_mnc';
    }
    console.log(`[RULES] Sites PLMN column: ${plmnColName || 'NOT FOUND — site lookups will be less precise'}`);
  }

  // Helper: extract PLMN string from a site row (normalized to "MCC-MNC")
  function getSitePlmn(site) {
    if (plmnColName === '_derived_mcc_mnc') {
      const mcc = String(site.mcc || '').padStart(3, '0');
      const mnc = String(site.mnc || '').padStart(2, '0');
      return mcc !== '000' ? `${mcc}-${mnc}` : null;
    }
    return normalizePlmn(plmnColName ? site[plmnColName] : null);
  }

  // Helper: is a coordinate inside the region bbox (tight 0.5° margin)?
  const MARGIN = 0.5;
  function isInRegion(lat, lng) {
    if (!regionBbox) return true;
    return lat >= (regionBbox.latMin - MARGIN) &&
           lat <= (regionBbox.latMax + MARGIN) &&
           lng >= (regionBbox.lngMin - MARGIN) &&
           lng <= (regionBbox.lngMax + MARGIN);
  }

  for (const site of knownCells) {
    const siteId = site.site_id;
    const lat = Number(site.lat);
    const lng = Number(site.lng);

    if (!siteId || isNaN(lat) || isNaN(lng)) continue;

    const plmn = getSitePlmn(site);
    const inRegion = isInRegion(lat, lng);

    const entry = {
      site_id: siteId,
      plmn,
      lat,
      lng,
      inRegion,
      tech: site.tech,
      height: Number(site.height) || 0,
      max_distance: Number(site.max_distance_propagation) || 0,
      pcis: new Set(),
      earfcns: new Set(),
    };

    // Sectors can come from ClickHouse in several formats:
    //   1. Array of objects: [{ pci: 1, dl_frequency_code: 100 }, ...]
    //   2. JSON string (if stored as String column)
    //   3. Nested type flattened: site["sectors.pci"] = [1,2,3], site["sectors.dl_frequency_code"] = [100,200,300]
    //   4. Array of tuples: [[1, "A", 100], [2, "B", 200]]
    let sectors = site.sectors;

    // Case 2: JSON string → parse
    if (typeof sectors === 'string') {
      try { sectors = JSON.parse(sectors); } catch (_) { sectors = null; }
    }

    // Case 3: ClickHouse Nested flattened columns
    if (!sectors || (Array.isArray(sectors) && sectors.length === 0)) {
      const nestedPcis = site['sectors.pci'];
      const nestedEarfcns = site['sectors.dl_frequency_code'];
      if (Array.isArray(nestedPcis) && nestedPcis.length > 0) {
        sectors = nestedPcis.map((pci, i) => ({
          pci,
          dl_frequency_code: nestedEarfcns ? nestedEarfcns[i] : undefined,
        }));
      }
    }

    if (sectors && Array.isArray(sectors)) {
      for (const sector of sectors) {
        totalSectors++;
        const sectorPci = Array.isArray(sector) ? sector[0] : sector.pci;
        const sectorEarfcn = Array.isArray(sector) ? sector[3] : sector.dl_frequency_code;

        if (sectorPci !== undefined && sectorPci !== null) {
          const pciNum = Number(sectorPci);
          if (!isNaN(pciNum)) {
            entry.pcis.add(pciNum);
            parsedSectors++;
            if (!pciIndex.has(pciNum)) pciIndex.set(pciNum, []);
            pciIndex.get(pciNum).push({ site_id: siteId, plmn, lat, lng, inRegion });
          }
        }
        if (sectorEarfcn) {
          entry.earfcns.add(Number(sectorEarfcn));
        }
      }
    }

    // Primary index: composite key "siteId:plmn" for exact operator match (array — same ID can have multiple locations)
    if (plmn) {
      const key1 = `${siteId}:${plmn}`;
      if (!siteByPlmn.has(key1)) siteByPlmn.set(key1, []);
      siteByPlmn.get(key1).push(entry);
      const numId = Number(siteId);
      if (!isNaN(numId)) {
        const key2 = `${numId}:${plmn}`;
        if (!siteByPlmn.has(key2)) siteByPlmn.set(key2, []);
        siteByPlmn.get(key2).push(entry);
      }
    }

    // Fallback index: siteId → array of all entries (for lookups without PLMN)
    const numId = Number(siteId);
    if (!siteById.has(siteId)) siteById.set(siteId, []);
    siteById.get(siteId).push(entry);
    if (!isNaN(numId) && String(numId) !== String(siteId)) {
      if (!siteById.has(numId)) siteById.set(numId, []);
      siteById.get(numId).push(entry);
    }
  }

  const allEntries = [...new Set([...siteByPlmn.values()].flat())];
  const inRegionCount = allEntries.filter(e => e.inRegion).length;
  console.log(`[RULES] Site index: ${siteByPlmn.size} PLMN-keyed, ${siteById.size} ID-keyed, ` +
    `PCI index: ${pciIndex.size} unique PCIs, ${parsedSectors}/${totalSectors} sectors with PCI, ` +
    `${inRegionCount} in-region`);

  // Lookup function: find the correct site for a measurement.
  // Strategy: exact PLMN match first → fallback to in-region match by ID → null.
  // When multiple entries exist for the same key, pick the closest to the measurement.
  let lookupLogCount = 0;

  // Helper: from an array of entries, pick the closest to the given lat/lng.
  // Prefers in-region entries. Falls back to first in-region if no lat/lng provided.
  function pickClosest(entries, measLat, measLng) {
    if (!entries || entries.length === 0) return null;
    if (entries.length === 1) return entries[0];

    // If we have measurement coords, sort by distance
    if (measLat != null && measLng != null) {
      let best = null;
      let bestDist = Infinity;
      for (const e of entries) {
        if (!e.inRegion) continue; // skip out-of-region
        const d = haversineKm(measLat, measLng, e.lat, e.lng);
        if (d < bestDist) { bestDist = d; best = e; }
      }
      // If nothing in-region, try all
      if (!best) {
        for (const e of entries) {
          const d = haversineKm(measLat, measLng, e.lat, e.lng);
          if (d < bestDist) { bestDist = d; best = e; }
        }
      }
      return best;
    }

    // No measurement coords — prefer in-region
    return entries.find(e => e.inRegion) || entries[0];
  }

  function lookupSite(enbId, plmn, measLat, measLng) {
    // 1. Exact match: siteId + PLMN (most precise — no false matches)
    if (plmn) {
      const key = `${enbId}:${plmn}`;
      const candidates = siteByPlmn.get(key);
      if (candidates && candidates.length > 0) {
        const match = pickClosest(candidates, measLat, measLng);
        if (lookupLogCount < 5) {
          lookupLogCount++;
          console.log(`[RULES] lookupSite(${enbId}, ${plmn}, ${measLat?.toFixed(3)}, ${measLng?.toFixed(3)}) → EXACT match (${candidates.length} entries), picked (${match.lat.toFixed(4)}, ${match.lng.toFixed(4)}) inRegion=${match.inRegion}`);
        }
        return match;
      }
    }

    // 2. Fallback: find by siteId, prefer closest in-region entry
    const fallback = siteById.get(enbId) || siteById.get(String(enbId));
    if (!fallback || fallback.length === 0) {
      if (lookupLogCount < 5) {
        lookupLogCount++;
        console.log(`[RULES] lookupSite(${enbId}, ${plmn}) → NOT FOUND (no site with this ID)`);
      }
      return null;
    }

    const match = pickClosest(fallback, measLat, measLng);
    if (lookupLogCount < 5) {
      lookupLogCount++;
      console.log(`[RULES] lookupSite(${enbId}, ${plmn}) → FALLBACK (no PLMN match), ${fallback.length} candidates, using ${match.plmn || 'no-plmn'} at (${match.lat.toFixed(4)}, ${match.lng.toFixed(4)}) inRegion=${match.inRegion}`);
    }
    return match;
  }

  // Lookup by PCI + PLMN + location — used to enrich measurements missing eNB/ECI
  // (e.g. bad_measurements where the pipeline strips cell identity).
  // Returns the site entry if a unique PCI+PLMN+proximity match is found.
  function lookupSiteByPci(pci, plmn, measLat, measLng) {
    if (pci == null) return null;
    const pciNum = Number(pci);
    const entries = pciIndex.get(pciNum);
    if (!entries || entries.length === 0) return null;

    // Filter by PLMN if available
    let candidates = plmn
      ? entries.filter(e => e.plmn === plmn)
      : entries;
    if (candidates.length === 0) candidates = entries; // fallback to all

    // Filter to in-region only
    const inRegion = candidates.filter(e => e.inRegion);
    if (inRegion.length > 0) candidates = inRegion;

    // Pick closest to measurement location
    if (candidates.length === 0) return null;
    if (measLat != null && measLng != null) {
      let best = null, bestDist = Infinity;
      for (const e of candidates) {
        const d = haversineKm(measLat, measLng, e.lat, e.lng);
        if (d < bestDist) { bestDist = d; best = e; }
      }
      // Only accept if within reasonable distance (< 20km)
      if (best && bestDist < 20) return best;
      return null;
    }
    return candidates.length === 1 ? candidates[0] : null;
  }

  return { siteByPlmn, siteById, pciIndex, lookupSite, lookupSiteByPci };
}

// ---------------------------------------------------------------------------
// Main rule engine
// ---------------------------------------------------------------------------
async function runRules(measurements, knownCells, expectedMCCs = [], regionBbox = null, thresholds = {}) {
  const flags = [];
  const { pciIndex, lookupSite, lookupSiteByPci } = buildSiteIndex(knownCells, regionBbox);

  // Rule threshold helpers
  function isRuleEnabled(ruleName) {
    if (!thresholds[ruleName]) return true; // enabled by default
    return thresholds[ruleName].enabled !== false;
  }
  function getThreshold(ruleName, field, defaultVal) {
    if (thresholds[ruleName] && thresholds[ruleName][field] !== undefined) return thresholds[ruleName][field];
    return defaultVal;
  }

  // Expected MCC set — still used for test network guard on site-based rules
  const expectedMccSet = new Set(expectedMCCs.map(String));

  // -----------------------------------------------------------------------
  // Enrichment pass: fill in missing eNB/ECI from PCI+PLMN+location lookup.
  // bad_measurements often have cell: { pci: 235 } but no eci/enb because
  // the Flycomm pipeline strips cell identity. We can recover it from the
  // sites table PCI index if there's a nearby match.
  // -----------------------------------------------------------------------
  let enrichedCount = 0;
  for (const m of measurements) {
    const hasEnb = m.cell_enb && Number(m.cell_enb) !== 0;
    const hasEci = m.cell_eci && Number(m.cell_eci) !== 0;
    const hasPci = m.cell_pci != null && m.cell_pci !== '';
    if (!hasEnb && !hasEci && hasPci) {
      // Skip enrichment for measurements with no valid PLMN (MCC 000 or empty).
      // These are bad/garbage measurements — pinning them to a real site via PCI
      // alone is misleading and gives false confidence.
      const mccRaw = m.network_mcc != null ? String(m.network_mcc) : '';
      if (mccRaw === '0' || mccRaw === '000' || mccRaw === '') continue;

      const mPlmn = normalizePlmn(m.network_PLMN) ||
        (m.network_mcc && m.network_mnc
          ? `${String(m.network_mcc).padStart(3, '0')}-${String(m.network_mnc).padStart(2, '0')}`
          : null);
      const mLat = m.location_lat_rounded ? Number(m.location_lat_rounded) : null;
      const mLng = m.location_lng_rounded ? Number(m.location_lng_rounded) : null;
      const site = lookupSiteByPci(m.cell_pci, mPlmn, mLat, mLng);
      if (site) {
        m.cell_enb = site.site_id;
        m._enriched_enb = true;
        enrichedCount++;
      }
    }
  }
  if (enrichedCount > 0) {
    console.log(`[RULES] Enriched ${enrichedCount} measurements with eNB from PCI+PLMN+location lookup`);
  }

  // -----------------------------------------------------------------------
  // PCI COLLISION — RSU MODE ONLY
  // LTE has 504 PCIs — operators reuse them across the network by design.
  // SDK data (different phones, locations, times) CANNOT detect real collisions.
  // RSU mode: same PCI + same EARFCN + geographic proximity (<2km) + same
  // time window (<5min) from fixed sensors = genuine collision/rogue cell.
  // -----------------------------------------------------------------------
  const pciCollisionSamples = new Set();
  const pciCollisionInfo = {};

  // Only run in RSU mode (source='modem' — fixed location sensors)
  const isRSUMode = measurements.length > 0 && measurements.some(m => m.source === 'modem');

  if (isRSUMode && isRuleEnabled('PCI_COLLISION')) {
    // Group by PCI+EARFCN → list of {enb, lat, lng, ts, sample_id, device}
    const pciEarfcnMap = {}; // "pci_earfcn" → [{enb, lat, lng, ts, sample_id, device}]

    for (const m of measurements) {
      const pciVal = m.cell_pci != null ? Number(m.cell_pci) : null;
      const earfcnVal = m.band_downlinkEarfcn ? Number(m.band_downlinkEarfcn) : null;
      if (pciVal === null || !earfcnVal) continue;
      const eciVal = m.cell_eci ? Number(m.cell_eci) : null;
      const enbVal = m.cell_enb ? Number(m.cell_enb) : (eciVal ? (eciVal >> 8) : null);
      if (!enbVal) continue;

      const key = `${pciVal}_${earfcnVal}`;
      if (!pciEarfcnMap[key]) pciEarfcnMap[key] = [];
      pciEarfcnMap[key].push({
        enb: enbVal, pci: pciVal, earfcn: earfcnVal,
        lat: m.location_lat_rounded ? Number(m.location_lat_rounded) : null,
        lng: m.location_lng_rounded ? Number(m.location_lng_rounded) : null,
        ts: m.timestamp ? new Date(m.timestamp).getTime() : 0,
        sample_id: m.sample_id,
        device: m.deviceInfo_deviceId || m.sample_id,
      });
    }

    // Check each PCI+EARFCN group for true collisions:
    // different eNBs + within 2km + within 5min
    for (const [key, entries] of Object.entries(pciEarfcnMap)) {
      const enbGroups = {};
      for (const e of entries) {
        if (!enbGroups[e.enb]) enbGroups[e.enb] = [];
        enbGroups[e.enb].push(e);
      }
      const enbIds = Object.keys(enbGroups);
      if (enbIds.length < 2) continue; // same eNB — no collision

      // Check proximity + time overlap between eNB groups
      for (let i = 0; i < enbIds.length; i++) {
        for (let j = i + 1; j < enbIds.length; j++) {
          const groupA = enbGroups[enbIds[i]];
          const groupB = enbGroups[enbIds[j]];
          // Check if any pair is within 2km and 5min
          let collision = false;
          for (const a of groupA) {
            if (collision) break;
            for (const b of groupB) {
              if (!a.lat || !b.lat) continue;
              const dist = haversineKm(a.lat, a.lng, b.lat, b.lng);
              const timeDiff = Math.abs(a.ts - b.ts);
              if (dist < 2 && timeDiff < 300000) { // <2km, <5min
                collision = true;
                const desc = `PCI ${a.pci} EARFCN ${a.earfcn}: eNB ${a.enb} vs eNB ${b.enb} — ${dist.toFixed(1)}km apart, ${(timeDiff/1000).toFixed(0)}s window`;
                pciCollisionInfo[a.pci] = desc;
                // Mark all samples from both groups
                for (const s of groupA) pciCollisionSamples.add(s.sample_id);
                for (const s of groupB) pciCollisionSamples.add(s.sample_id);
                break;
              }
            }
          }
        }
      }
    }

    if (Object.keys(pciCollisionInfo).length > 0) {
      console.log(`[RULES] PCI collisions (RSU): ${Object.keys(pciCollisionInfo).length}`);
    }
  }

  // (PHANTOM_CELL removed — operator registry is incomplete. Missing PLMN ≠ rogue cell.
  //  A cell in Congo with PLMN 630-01 is legitimate even though we only have Israeli operators
  //  in OPERATOR_LICENSED_BANDS. Cannot detect phantoms without a global operator registry.)
  const phantomCellSamples = new Set();
  const phantomCellInfo = {};

  // -----------------------------------------------------------------------
  // TA=0 cluster analysis: count (cell_pci, cell_eci) → unique device_ids
  // Per 3GPP, TA=0 means UE is within 0-78m. Seeing 2+ unique UEs with TA=0
  // on same cell is a strong IMSI catcher indicator.
  // -----------------------------------------------------------------------
  const taZeroClusters = {}; // key: "pci_eci" → { devices: Set, samples: [] }

  // First pass: collect TA=0 clusters
  for (const m of measurements) {
    const ta = m.signal_timingAdvance;
    if (ta !== null && ta !== undefined && Number(ta) === 0) {
      const key = `${m.cell_pci}_${m.cell_eci || 'x'}`;
      if (!taZeroClusters[key]) {
        taZeroClusters[key] = { devices: new Set(), samples: [], pci: m.cell_pci, eci: m.cell_eci };
      }
      taZeroClusters[key].devices.add(m.deviceInfo_deviceId || m.sample_id);
      taZeroClusters[key].samples.push(m.sample_id);
    }
  }

  // -----------------------------------------------------------------------
  // 2G DOWNGRADE cluster: count unique devices on 2G per cell
  // Only flag as NETWORK_DOWNGRADE if ≥2 unique devices downgraded on same cell
  // -----------------------------------------------------------------------
  const downgradeClusters = {}; // key: "pci" → { devices: Set }
  const DOWNGRADE_RATS = new Set(['GSM', 'EDGE', 'GPRS']);
  for (const m of measurements) {
    if (!m.tech || !DOWNGRADE_RATS.has(String(m.tech).toUpperCase())) continue;
    const key = String(m.cell_pci || 'x');
    if (!downgradeClusters[key]) downgradeClusters[key] = { devices: new Set() };
    downgradeClusters[key].devices.add(m.deviceInfo_deviceId || m.sample_id);
  }

  // Second pass: per-measurement rules
  for (const m of measurements) {
    const cellId = String(m.cell_pci || '');
    const rsrp = m.signal_rsrp !== null && m.signal_rsrp !== undefined ? Number(m.signal_rsrp) : null;
    const ta = m.signal_timingAdvance !== null && m.signal_timingAdvance !== undefined ? Number(m.signal_timingAdvance) : null;
    const pci = m.cell_pci !== null && m.cell_pci !== undefined ? Number(m.cell_pci) : null;
    const eci = m.cell_eci ? Number(m.cell_eci) : null;
    const enbId = m.cell_enb ? Number(m.cell_enb) : (eci ? (eci >> 8) : null);
    const earfcn = m.band_downlinkEarfcn ? Number(m.band_downlinkEarfcn) : null;
    const mLat = m.location_lat_rounded ? Number(m.location_lat_rounded) : null;
    const mLng = m.location_lng_rounded ? Number(m.location_lng_rounded) : null;

    // Extract measurement PLMN early — used for PLMN-aware site lookups
    const mPlmn = normalizePlmn(m.network_PLMN) ||
      (m.network_mcc && m.network_mnc
        ? `${String(m.network_mcc).padStart(3, '0')}-${String(m.network_mnc).padStart(2, '0')}`
        : null);
    const mccStr = String(m.network_mcc || '').padStart(3, '0');
    const mccMatchesRegion = expectedMccSet.size === 0 || expectedMccSet.has(mccStr);

    const baseFlag = {
      cell_id: cellId,
      cell_ecgi: m.cell_ecgi || '',
      location_lat: mLat,
      location_lng: mLng,
    };

    // =====================================================================
    // RULE 0: BAD_MEASUREMENT SOURCE CONTEXT FLAG
    // Measurements in the bad_measurements table met anomaly criteria in the
    // Flycomm pipeline. This is an ENRICHMENT flag — it adds diagnostic context
    // but is NOT an alert by itself. Other rules (MCC_MISMATCH, GPS_SPOOFING,
    // TA_ZERO_CLUSTER, etc.) will generate the actual alerts if warranted.
    // =====================================================================
    if (m._source === 'bad_measurements' && isRuleEnabled('BAD_MEASUREMENT')) {
      const reason = m._bad_reason || 'unknown';

      // Build diagnostic details from available data
      const diag = [];
      const hasEnb = m.cell_enb && Number(m.cell_enb) !== 0;
      const hasEci = m.cell_eci && Number(m.cell_eci) !== 0;
      const hasPci = m.cell_pci != null && m.cell_pci !== '';

      // Cell identity analysis
      if (!hasEnb && !hasEci && hasPci) {
        diag.push(`PCI ${m.cell_pci} with no eNB/ECI — cell identity stripped by pipeline`);
      }
      if (m._enriched_enb) {
        diag.push(`eNB ${m.cell_enb} enriched from PCI+PLMN lookup`);
      }

      // Signal quality
      if (m.signal_rsrp != null) {
        const rsrpVal = Number(m.signal_rsrp);
        if (rsrpVal > -70) diag.push(`Unusually strong signal (RSRP ${rsrpVal} dBm)`);
        else if (rsrpVal < -120) diag.push(`Very weak signal (RSRP ${rsrpVal} dBm)`);
      }
      if (m.signal_timingAdvance != null) {
        const taVal = Number(m.signal_timingAdvance);
        if (taVal <= 1) diag.push(`TA=${taVal} — device extremely close to cell`);
        const distKm = (taVal * 78.12 / 1000).toFixed(1);
        if (taVal > 0) diag.push(`Distance to cell: ~${distKm}km (TA=${taVal})`);
      }

      // Network identity
      if (m.network_mcc) {
        const mcc = Number(m.network_mcc);
        if (mcc === 0 || mcc === 1) diag.push(`Test network MCC ${mcc}`);
        else if (expectedMCCs && expectedMCCs.length > 0 && !expectedMCCs.includes(mcc) && !expectedMCCs.includes(String(mcc))) {
          diag.push(`Unexpected MCC ${mcc} (expected ${expectedMCCs.join('/')})`);
        }
      }

      // Tech analysis
      if (m.tech && ['GSM', 'EDGE', 'GPRS'].includes(m.tech)) {
        diag.push(`2G (${m.tech})`);
      }

      const details = diag.length > 0
        ? `Pipeline anomaly (${reason}): ${diag.join('; ')}`
        : `Pipeline anomaly (reason: ${reason}) — context only, see other rules for alerts`;

      flags.push({
        sample_id: m.sample_id, rule: 'BAD_MEASUREMENT', severity: LOW, score: 0.15,
        details,
        ...baseFlag,
      });
    }

    // =====================================================================
    // RULE 1: eNB/ECI GEOGRAPHIC VALIDATION (BAND-AWARE)
    // Distance thresholds adjusted by frequency band:
    //   Low band  (700-900 MHz, B8/B20/B28): 15-20km rural
    //   Mid band  (1800-2100 MHz, B1/B3):     8-12km
    //   High band (2600 MHz+, B7/B38-41):     3-5km urban
    // =====================================================================
    if (isRuleEnabled('CELL_LOCATION_MISMATCH') && mccMatchesRegion && enbId && mLat && mLng) {
      // Band-aware max range (km) — based on frequency propagation physics
      // User-configured threshold takes precedence; band defaults only when using default (5.0)
      const userDistThreshold = getThreshold('CELL_LOCATION_MISMATCH', 'minDistanceKm', 5.0);
      const userCustomized = thresholds.CELL_LOCATION_MISMATCH && thresholds.CELL_LOCATION_MISMATCH.minDistanceKm !== undefined && thresholds.CELL_LOCATION_MISMATCH.minDistanceKm !== 5.0;
      let bandMaxRange = userDistThreshold;
      if (!userCustomized && earfcn) {
        const bandInfo = config.earfcnToBandExported ? config.earfcnToBandExported(earfcn) : null;
        const bandNum = bandInfo || null;
        if (bandNum) {
          // Low bands: longer range
          if ([5, 8, 12, 13, 14, 17, 18, 19, 20, 26, 28, 29].includes(bandNum)) {
            bandMaxRange = 18; // 700-900 MHz — up to 18km
          } else if ([1, 3, 4, 10, 25].includes(bandNum)) {
            bandMaxRange = 10; // 1800-2100 MHz — up to 10km
          } else if ([7, 38, 40, 41, 42, 43, 48].includes(bandNum)) {
            bandMaxRange = 5;  // 2600 MHz+ — up to 5km
          }
        }
      }

      const knownSite = lookupSite(enbId, mPlmn, mLat, mLng);
      if (knownSite && knownSite.lat && knownSite.lng && knownSite.inRegion) {
        const distKm = haversineKm(mLat, mLng, knownSite.lat, knownSite.lng);
        const bandLabel = earfcn ? ` (max ${bandMaxRange}km for this band)` : '';
        if (distKm > bandMaxRange * 2) {
          flags.push({ sample_id: m.sample_id, rule: 'CELL_LOCATION_MISMATCH', severity: CRITICAL, score: 0.95,
            details: `eNB ${enbId} (${eci ? 'ECI ' + eci : 'PCI ' + pci}) registered at (${knownSite.lat.toFixed(3)},${knownSite.lng.toFixed(3)}) but measured ${distKm.toFixed(1)}km away${bandLabel}`,
            known_site_lat: knownSite.lat, known_site_lng: knownSite.lng, known_site_id: String(enbId), distance_km: distKm,
            ...baseFlag });
        } else if (distKm > bandMaxRange) {
          flags.push({ sample_id: m.sample_id, rule: 'CELL_LOCATION_MISMATCH', severity: HIGH, score: 0.80,
            details: `eNB ${enbId} (${eci ? 'ECI ' + eci : 'PCI ' + pci}) registered ${distKm.toFixed(1)}km from measurement${bandLabel} — edge of plausibility`,
            known_site_lat: knownSite.lat, known_site_lng: knownSite.lng, known_site_id: String(enbId), distance_km: distKm,
            ...baseFlag });
        }
      }
      // NOTE: missing eNB NOT flagged — sites DB is incomplete, missing ≠ rogue
    }

    // (Rule 2 PCI_LOCATION_MISMATCH / UNKNOWN_PCI removed — PCI is reused
    //  across the network (0-503 in LTE), so PCI-based geographic lookups
    //  are unreliable and produce too many false positives. Use eNB/ECI instead.)

    // =====================================================================
    // RULE 2b: GPS SPOOFING DETECTION (TA + RSRP Physics Validation)
    //
    // A device reports GPS at location X but is connected to a cell whose
    // known tower (eNB ID) is at location Y. GPS spoofing is ONLY flagged
    // when the RF physics (TA + RSRP) CONTRADICT the GPS distance.
    //
    // Logic:
    //   gpsDistKm = haversine(GPS, tower)
    //   taDistKm  = TA * 78.12m (LTE timing advance resolution)
    //   rsrpDistKm = Okumura-Hata path loss estimate
    //
    // Case 1: DEFINITE GPS SPOOFING — GPS says far, but TA+RSRP say close
    //   GPS >10km from tower, but TA suggests <2km AND RSRP suggests <3km
    //   → device is physically near the tower, GPS coordinates are fake.
    //
    // Case 2: DEFINITE GPS SPOOFING — GPS says close, but TA+RSRP say far
    //   GPS <2km from tower, but TA >10km AND RSRP is extremely weak
    //   → device is physically far from tower, GPS is spoofed to tower loc.
    //
    // Case 3: LARGE DISTANCE with physics mismatch
    //   GPS >15km from tower — flag at HIGH if ANY RF indicator contradicts.
    //
    // NOT flagged: GPS 3km from tower, TA=0, RSRP=-114. This is normal:
    //   TA=0 has 78m resolution (could be 0-78m OR a TA reporting quirk),
    //   RSRP=-114 is weak (consistent with 3-5km urban cell edge).
    //   The TA and RSRP don't clearly contradict 3km.
    //
    // GUARD: Only when MCC matches region & we have eNB.
    // =====================================================================
    if (isRuleEnabled('GPS_SPOOFING') && mccMatchesRegion && enbId && mLat && mLng) {
      const gpsMinDist = getThreshold('GPS_SPOOFING', 'minDistanceKm', 5.0);
      const knownSiteForGps = lookupSite(enbId, mPlmn, mLat, mLng);
      if (knownSiteForGps && knownSiteForGps.lat && knownSiteForGps.lng && knownSiteForGps.inRegion) {
        const gpsDistKm = haversineKm(mLat, mLng, knownSiteForGps.lat, knownSiteForGps.lng);

        if (gpsDistKm > gpsMinDist) {
          // Compute RF-based distance estimates
          const taDistKm = ta !== null ? taToMeters(ta) / 1000 : null;
          const rsrpDistKm = rsrp !== null ? rsrpToEstimatedKm(rsrp) : null;

          // Build physics evidence
          let physicsContradict = false;
          const evidence = [];

          // Check TA contradiction: if TA says device is much closer than GPS suggests
          if (taDistKm !== null) {
            const taGpsDelta = Math.abs(gpsDistKm - taDistKm);
            if (taDistKm < 2 && gpsDistKm > 10) {
              // TA says within ~2km, GPS says >10km — strong contradiction
              physicsContradict = true;
              evidence.push(`TA=${ta} (~${taDistKm.toFixed(1)}km) contradicts GPS distance ${gpsDistKm.toFixed(1)}km`);
            } else if (ta > 0 && taGpsDelta > gpsDistKm * 0.7 && gpsDistKm > 8) {
              // Non-zero TA disagrees with GPS by >70% — moderate contradiction
              physicsContradict = true;
              evidence.push(`TA=${ta} (~${taDistKm.toFixed(1)}km) vs GPS ${gpsDistKm.toFixed(1)}km (${(taGpsDelta / gpsDistKm * 100).toFixed(0)}% mismatch)`);
            }
            // NOTE: TA=0 with GPS 3-5km is NOT a contradiction:
            // TA=0 means 0-78m resolution, common at cell edge with timing quirks
          }

          // Check RSRP contradiction: strong signal but GPS says far away
          if (rsrp !== null) {
            if (rsrp > -85 && gpsDistKm > 5) {
              // Very strong RSRP (>-85 dBm) but GPS >5km — device must be close
              physicsContradict = true;
              evidence.push(`RSRP=${rsrp} dBm (strong, ~${rsrpDistKm.toFixed(1)}km est.) contradicts GPS ${gpsDistKm.toFixed(1)}km`);
            } else if (rsrp > -100 && gpsDistKm > 15) {
              // Decent RSRP but GPS >15km — unlikely
              physicsContradict = true;
              evidence.push(`RSRP=${rsrp} dBm (~${rsrpDistKm.toFixed(1)}km est.) unlikely at GPS distance ${gpsDistKm.toFixed(1)}km`);
            }
          }

          // For very large distances (>15km), flag even without perfect TA/RSRP data
          // as the GPS is almost certainly wrong for a connected cell
          if (gpsDistKm > 15 && !physicsContradict) {
            physicsContradict = true;
            evidence.push(`GPS ${gpsDistKm.toFixed(1)}km from connected eNB — exceeds max LTE range for urban deployment`);
          }

          if (physicsContradict) {
            const sev = gpsDistKm > 15 ? CRITICAL : HIGH;
            flags.push({
              sample_id: m.sample_id,
              rule: 'GPS_SPOOFING',
              severity: sev,
              score: sev === CRITICAL ? 0.95 : 0.85,
              details: `Device GPS at (${mLat.toFixed(4)},${mLng.toFixed(4)}) connected to eNB ${enbId}` +
                ` (${eci ? 'ECI ' + eci + ', ' : ''}PCI ${pci})` +
                ` — tower at (${knownSiteForGps.lat.toFixed(4)},${knownSiteForGps.lng.toFixed(4)})` +
                `, ${gpsDistKm.toFixed(1)}km. ${evidence.join('; ')}`,
              known_site_lat: knownSiteForGps.lat,
              known_site_lng: knownSiteForGps.lng,
              known_site_id: String(enbId),
              distance_km: gpsDistKm,
              ...baseFlag,
            });
          }
        }
      }
    }

    // =====================================================================
    // RULE 3: TA=0 CLUSTER — IMSI CATCHER INDICATOR
    // Per 3GPP TS 36.321: TA=0 means UE is within 0-78m of eNB.
    // A SINGLE TA=0 can be legitimate (UE near tower).
    // MULTIPLE unique UEs (≥2) reporting TA=0 on the SAME cell in the same
    // time window → strong indicator of portable BTS / IMSI catcher.
    // =====================================================================
    if (isRuleEnabled('TA_ZERO_CLUSTER') && ta !== null && ta === 0) {
      const taMinSamples = getThreshold('TA_ZERO_CLUSTER', 'minUniqueSamples', 2);
      const clusterKey = `${m.cell_pci}_${m.cell_eci || 'x'}`;
      const cluster = taZeroClusters[clusterKey];
      if (cluster && cluster.devices.size >= Math.max(5, taMinSamples)) {
        flags.push({ sample_id: m.sample_id, rule: 'TA_ZERO_CLUSTER_CRITICAL', severity: CRITICAL, score: 0.95,
          details: `TA=${ta} — ${cluster.devices.size} unique devices report TA=0 on PCI ${cluster.pci} (≥5 UEs, IMSI catcher pattern)`,
          ...baseFlag });
      } else if (cluster && cluster.devices.size >= taMinSamples) {
        flags.push({ sample_id: m.sample_id, rule: 'TA_ZERO_CLUSTER', severity: HIGH, score: 0.80,
          details: `TA=${ta} — ${cluster.devices.size} unique devices report TA=0 on PCI ${cluster.pci} (multi-UE zero-distance pattern)`,
          ...baseFlag });
      }
    }

    // =====================================================================
    // RULE 3b: PCI COLLISION — RSU MODE ONLY
    // Same PCI + same EARFCN + <2km + <5min from fixed RSU sensors.
    // Disabled in SDK mode (different phones/locations = normal PCI reuse).
    // =====================================================================
    if (isRuleEnabled('PCI_COLLISION') && pci !== null && pciCollisionSamples.has(m.sample_id)) {
      const collisionDesc = pciCollisionInfo[pci] || `PCI ${pci} collision`;
      // Higher severity if this eNB is the unknown one (not in sites database)
      const knownSite = enbId ? lookupSite(enbId, mPlmn, mLat, mLng) : null;
      const isUnknownEnb = !knownSite;
      const sev = isUnknownEnb ? CRITICAL : HIGH;
      flags.push({
        sample_id: m.sample_id, rule: 'PCI_COLLISION', severity: sev,
        score: isUnknownEnb ? 0.95 : 0.80,
        details: collisionDesc + (isUnknownEnb ? ' — this eNB is UNKNOWN (likely rogue)' : ' — this eNB is in sites DB'),
        ...baseFlag,
      });
    }

    // =====================================================================
    // RULE 3c: PHANTOM CELL — unknown PLMN not in operator registry
    // ≥2 unique devices required. CRITICAL if also TA=0.
    // =====================================================================
    if (isRuleEnabled('PHANTOM_CELL') && pci !== null && phantomCellSamples.has(m.sample_id)) {
      // Find matching phantom profile
      const phantomKey = `${pci}_${mPlmn || 'x'}`;
      const phantomInfo = phantomCellInfo[phantomKey];
      if (phantomInfo) {
        const sev = phantomInfo.hasTA0 ? CRITICAL : HIGH;
        flags.push({
          sample_id: m.sample_id, rule: 'PHANTOM_CELL', severity: sev,
          score: phantomInfo.hasTA0 ? 0.95 : 0.80,
          details: phantomInfo.desc + (phantomInfo.hasTA0 ? ' — TA=0 (portable BTS proximity)' : ''),
          ...baseFlag,
        });
      }
    }

    // =====================================================================
    // RULE 4: TA-RSRP DISTANCE MISMATCH
    // TA gives distance estimate. RSRP gives path-loss-based distance.
    // If they disagree by >2km → signal doesn't match physics.
    // Example: TA says 10km but RSRP says 500m = amplified/spoofed signal.
    // =====================================================================
    if (ta !== null && ta > 0 && rsrp !== null && mLat && mLng) {
      const taDistKm = taToMeters(ta) / 1000;
      const rsrpDistKm = rsrpToEstimatedKm(rsrp);
      const mismatchKm = Math.abs(taDistKm - rsrpDistKm);

      if (mismatchKm > 5) {
        flags.push({ sample_id: m.sample_id, rule: 'TA_RSRP_MISMATCH', severity: HIGH, score: 0.75,
          details: `TA says ${taDistKm.toFixed(1)}km (TA=${ta}) but RSRP ${rsrp}dBm suggests ${rsrpDistKm.toFixed(1)}km — ${mismatchKm.toFixed(1)}km mismatch (signal amplification?)`,
          ...baseFlag });
      } else if (mismatchKm > 2) {
        flags.push({ sample_id: m.sample_id, rule: 'TA_RSRP_MISMATCH', severity: MEDIUM, score: 0.55,
          details: `TA/RSRP distance mismatch: ${mismatchKm.toFixed(1)}km (TA→${taDistKm.toFixed(1)}km, RSRP→${rsrpDistKm.toFixed(1)}km)`,
          ...baseFlag });
      }
    }

    // =====================================================================
    // =====================================================================
    // RULE 4b: RSRP/RSRQ RATIO ANOMALY — JAMMING INDICATOR
    // Strong RSRP (signal power) + very poor RSRQ (signal quality) indicates
    // heavy co-channel interference — signature of broadband jamming or
    // a nearby rogue transmitter. Legitimate strong signals have decent RSRQ.
    //   Normal:  RSRP > -85 dBm → RSRQ should be > -12 dB
    //   Jamming: RSRP > -85 dBm but RSRQ < -15 dB
    // =====================================================================
    if (isRuleEnabled('JAMMING_INDICATOR') && rsrp !== null) {
      const rsrq = m.signal_rsrq !== null && m.signal_rsrq !== undefined ? Number(m.signal_rsrq) : null;
      if (rsrq !== null) {
        if (rsrp > -85 && rsrq < -15) {
          flags.push({
            sample_id: m.sample_id, rule: 'JAMMING_INDICATOR', severity: CRITICAL, score: 0.90,
            details: `Strong signal (RSRP=${rsrp} dBm) but very poor quality (RSRQ=${rsrq} dB) — heavy interference/jamming signature`,
            ...baseFlag,
          });
        } else if (rsrp > -90 && rsrq < -17) {
          flags.push({
            sample_id: m.sample_id, rule: 'JAMMING_INDICATOR', severity: HIGH, score: 0.75,
            details: `Decent signal (RSRP=${rsrp} dBm) with degraded quality (RSRQ=${rsrq} dB) — possible interference`,
            ...baseFlag,
          });
        }
      }
    }

    // =====================================================================
    // RULE: TAC/LAC JUMP — Part A: Reserved/Invalid TAC Values
    // Per 3GPP: TAC 0, 1, 65534, 65535 are reserved in LTE.
    // TAC 16777215 is reserved in 5G NR SA.
    // These are never used in live commercial networks.
    // =====================================================================
    if (isRuleEnabled('TAC_LAC_JUMP') && m.cell_tac != null) {
      const tac = Number(m.cell_tac);
      const INVALID_TACS = [0, 1, 65534, 65535, 16777215];

      if (INVALID_TACS.includes(tac)) {
        const isUnknownEnb = !lookupSite(enbId, mPlmn, mLat, mLng);
        const sev = isUnknownEnb ? CRITICAL : HIGH;

        flags.push({
          sample_id: m.sample_id,
          rule: 'TAC_LAC_JUMP',
          severity: sev,
          score: sev === CRITICAL ? 0.92 : 0.82,
          details: `Reserved TAC=${tac} on eNB ${enbId || '?'} (PCI ${pci})` +
            (isUnknownEnb ? ' — unknown cell + reserved TAC = likely rogue' : ' — reserved TAC value, possible spoofed station'),
          ...baseFlag,
        });
      }
    }

    // (EARFCN_MISMATCH removed — sites DB sector list is incomplete,
    //  missing EARFCN ≠ anomaly. Use UNLICENSED_BAND instead — deterministic from 3GPP.)

    // =====================================================================
    // RULE 5: OPERATOR BAND/EARFCN/NR-ARFCN LICENSE CHECK
    // Each operator (identified by PLMN/MCC-MNC) has specific licensed frequency
    // bands in each country — for BOTH LTE and 5G NR.
    // If a cell claims to be e.g. Partner (425-01) but broadcasts on a band
    // Partner doesn't hold a license for in Israel — that's a strong IMSI catcher
    // indicator. A real tower can ONLY use licensed spectrum.
    //
    // LTE: EARFCN 0-56739 → mapped via earfcnToBand()
    // 5G NR: NR-ARFCN >100000 → mapped via nrArfcnToBand()
    //
    // Example LTE: 425-01 on Band 5 (850MHz) → Partner has no Band 5 → CRITICAL
    // Example NR:  425-01 on NR-ARFCN 700000 → n79 (4.4GHz) → not licensed → CRITICAL
    // =====================================================================
    // mPlmn already extracted at top of loop
    if (earfcn && mPlmn) {
      const earfcnCheck = config.isEarfcnValidForOperator(earfcn, mPlmn);
      // Only flag if: (1) not valid, (2) operator is known, (3) we can identify the band
      // If earfcnToBand returns null (unknown EARFCN), skip — can't validate what we can't identify
      if (earfcnCheck && !earfcnCheck.valid && !earfcnCheck.unknown && earfcnCheck.detectedBand) {
        const detectedBand = earfcnCheck.detectedBand;
        const isNr = earfcnCheck.isNr;
        const bandPrefix = isNr ? 'NR n' : 'Band ';
        const arfcnLabel = isNr ? 'NR-ARFCN' : 'EARFCN';
        const allowedBands = earfcnCheck.allowedBands || [];
        const allowedStr = allowedBands.map(b => isNr ? `n${b}` : `B${b}`).join(', ');
        flags.push({
          sample_id: m.sample_id, rule: 'UNLICENSED_BAND', severity: CRITICAL, score: 0.95,
          details: `${earfcnCheck.operatorName} (${mPlmn}) on ${arfcnLabel} ${earfcn} → ${bandPrefix}${detectedBand} — NOT licensed. Allowed: [${allowedStr}]`,
          ...baseFlag,
        });
      }
    }

    // Also check by band_number if available (more direct than EARFCN mapping)
    // SKIP for GSM/EDGE/GPRS: GSM uses frequency-based numbering (900, 1800, 1900)
    // which is incompatible with 3GPP LTE/NR band numbers (1-48, n1-n261).
    // GSM "Band 1800" != LTE "Band 1800"; comparing them causes false positives.
    const techUpper = String(m.tech || '').toUpperCase();
    const isGsm = ['GSM', 'EDGE', 'GPRS'].includes(techUpper);
    const bandNum = m.band_number ? Number(m.band_number) : null;
    if (bandNum && bandNum > 0 && mPlmn && !isGsm) {
      const bandCheck = config.isBandValidForOperator(bandNum, mPlmn);
      if (bandCheck && !bandCheck.valid && !bandCheck.unknown) {
        const allowedBands = bandCheck.allowedBands || [];
        const allowedStr = allowedBands.map(b => bandCheck.isNr ? `n${b}` : `B${b}`).join(', ');
        // Only add if we didn't already flag via EARFCN check above
        const alreadyFlagged = flags.some(f => f.sample_id === m.sample_id && f.rule === 'UNLICENSED_BAND');
        if (!alreadyFlagged) {
          const isNr = String(m.tech).toUpperCase() === 'NR' || String(m.tech).toUpperCase() === '5G';
          const bandLabel = isNr ? `NR n${bandNum}` : `B${bandNum}`;
          flags.push({
            sample_id: m.sample_id, rule: 'UNLICENSED_BAND', severity: CRITICAL, score: 0.95,
            details: `${bandCheck.operatorName} (${mPlmn}) on ${bandLabel} — NOT licensed. Allowed: [${allowedStr}]`,
            ...baseFlag,
          });
        }
      }
    }

    // =====================================================================
    // RULE 6: NETWORK DOWNGRADE ATTACK
    // Forced downgrade to 2G (GSM/EDGE/GPRS) disables LTE encryption.
    // Requires ≥2 unique devices on the same cell to avoid single-device flukes.
    // =====================================================================
    if (isRuleEnabled('DOWNGRADE_2G') && DOWNGRADE_RATS.has(String(m.tech).toUpperCase())) {
      const dgMinSamples = getThreshold('DOWNGRADE_2G', 'minUniqueSamples', 2);
      const dgKey = String(m.cell_pci || 'x');
      const dgCluster = downgradeClusters[dgKey];
      if (dgCluster && dgCluster.devices.size >= dgMinSamples) {
        flags.push({ sample_id: m.sample_id, rule: 'NETWORK_DOWNGRADE', severity: HIGH, score: 0.75,
          details: `RAT downgraded to ${m.tech} on PCI ${m.cell_pci || '?'} — ${dgCluster.devices.size} devices on 2G (A5/1 broken)`,
          ...baseFlag });
      }
    }

    // =====================================================================
    // RULE 7: MCC ANOMALY — unexpected country code for region
    // (reuse mccStr computed earlier for site-based guard)
    // =====================================================================
    if (isRuleEnabled('MCC_MISMATCH') && mccStr) {
      // Use custom expected MCC from threshold config, or fall back to region config
      const customExpectedMCC = getThreshold('MCC_MISMATCH', 'expectedMCC', '');
      const mccExpected = customExpectedMCC
        ? customExpectedMCC.split(',').map(s => s.trim())
        : (config.getRegion().expectedMCC || []).map(String);
      const isMccUnexpected = mccExpected.length > 0 && !mccExpected.includes(mccStr);
      if (isMccUnexpected) {
        const info = config.getMCCInfo(mccStr);
        const region = config.getRegion();
        const isCritical = mccStr === '001'; // Test network
        flags.push({ sample_id: m.sample_id, rule: 'MCC_ANOMALY', severity: isCritical ? CRITICAL : HIGH,
          score: isCritical ? 0.95 : 0.90,
          details: `MCC ${mccStr} (${info.flag} ${info.country}) unexpected in ${region.name}. Expected: ${mccExpected.join(',')}`,
          ...baseFlag });
      }
    }

    // =====================================================================
    // RULE 8: ROAMING in home coverage area
    // =====================================================================
    if (m.network_isRoaming === true || m.network_isRoaming === 1) {
      flags.push({ sample_id: m.sample_id, rule: 'ROAMING', severity: LOW, score: 0.40,
        details: `Device roaming on ${m.network_operator || '?'} (${m.network_PLMN})`,
        ...baseFlag });
    }

    // =====================================================================
    // RULE 9: EXTREME RSRP (> -50 dBm)
    // Per 3GPP: RSRP range is -140 to -43 dBm. Values > -50 are rare
    // and suggest transmitter is extremely close or amplified.
    // =====================================================================
    if (rsrp !== null && rsrp > -50) {
      flags.push({ sample_id: m.sample_id, rule: 'EXTREME_RSRP', severity: MEDIUM, score: 0.65,
        details: `RSRP ${rsrp} dBm > -50 — transmitter very close or amplified`,
        ...baseFlag });
    }

    // =====================================================================
    // RULE 10: NEIGHBOR CELL LIST ANOMALY
    // Legitimate macro cells always advertise neighbors. A cell with zero
    // reported neighbors is suspicious — rogue cells typically don't
    // configure neighbor lists. Only flag on registered/connected cells.
    // =====================================================================
    if (isRuleEnabled('EMPTY_NEIGHBORS') && m.neighborNo !== null && m.neighborNo !== undefined) {
      const neighborCount = Number(m.neighborNo);
      const isConnected = m.connectionStatus === 'REGISTERED' || m.isRegistered;
      if (isConnected && neighborCount === 0 && pci !== null) {
        // Unknown eNB with no neighbors = higher severity
        const knownSiteNeighbor = enbId ? lookupSite(enbId, mPlmn, mLat, mLng) : null;
        const sev = !knownSiteNeighbor ? HIGH : MEDIUM;
        flags.push({
          sample_id: m.sample_id, rule: 'EMPTY_NEIGHBORS', severity: sev,
          score: sev === HIGH ? 0.75 : 0.55,
          details: `PCI ${pci}${enbId ? ' eNB ' + enbId : ''} reports 0 neighbor cells — ` +
            (knownSiteNeighbor ? 'known site but empty neighbor list' : 'unknown cell with no neighbors (rogue indicator)'),
          ...baseFlag,
        });
      }
    }
  }

  // -----------------------------------------------------------------------
  // POST-LOOP: RAPID CELL RESELECTION — per-device analysis
  // If a device bounces between cells rapidly (especially 4G↔2G), it may
  // indicate an active downgrade attack in progress.
  // -----------------------------------------------------------------------
  if (isRuleEnabled('RAPID_RESELECTION')) {
    // Group measurements by device, sorted by time
    const deviceTimelines = {}; // deviceId → [{timestamp, tech, pci, enb, sample_id, lat, lng}]
    for (const m of measurements) {
      const did = m.deviceInfo_deviceId;
      if (!did) continue;
      const ts = m.timestamp ? new Date(m.timestamp).getTime() : 0;
      if (!ts) continue;
      if (!deviceTimelines[did]) deviceTimelines[did] = [];
      deviceTimelines[did].push({
        ts, tech: String(m.tech || '').toUpperCase(),
        pci: m.cell_pci, enb: m.cell_enb,
        sample_id: m.sample_id,
        lat: m.location_lat_rounded ? Number(m.location_lat_rounded) : null,
        lng: m.location_lng_rounded ? Number(m.location_lng_rounded) : null,
      });
    }

    const RESELECTION_WINDOW_MS = 5 * 60 * 1000; // 5-minute sliding window
    const MIN_RESELECTIONS = 4; // 4+ cell changes in 5 min = suspicious

    for (const [did, timeline] of Object.entries(deviceTimelines)) {
      if (timeline.length < MIN_RESELECTIONS) continue;
      timeline.sort((a, b) => a.ts - b.ts);

      // Sliding window: count cell changes
      for (let i = 0; i < timeline.length; i++) {
        // Find all measurements within window starting at i
        let cellChanges = 0;
        let has2GDrop = false;
        let prevPci = timeline[i].pci;
        let windowEnd = i;

        for (let j = i + 1; j < timeline.length; j++) {
          if (timeline[j].ts - timeline[i].ts > RESELECTION_WINDOW_MS) break;
          windowEnd = j;
          if (timeline[j].pci !== prevPci) {
            cellChanges++;
            prevPci = timeline[j].pci;
          }
          const tech = timeline[j].tech;
          if (['GSM', 'EDGE', 'GPRS'].includes(tech)) has2GDrop = true;
        }

        if (cellChanges >= MIN_RESELECTIONS) {
          const sev = has2GDrop ? CRITICAL : HIGH;
          const windowSec = ((timeline[windowEnd].ts - timeline[i].ts) / 1000).toFixed(0);
          // Flag the first sample in the window
          flags.push({
            sample_id: timeline[i].sample_id, rule: 'RAPID_RESELECTION',
            severity: sev, score: has2GDrop ? 0.90 : 0.75,
            details: `Device ${did.substring(0, 12)}... changed cells ${cellChanges} times in ${windowSec}s` +
              (has2GDrop ? ' — includes 2G downgrade (active attack pattern)' : ' — rapid handover sequence'),
            cell_id: String(timeline[i].pci || ''),
            location_lat: timeline[i].lat,
            location_lng: timeline[i].lng,
          });
          break; // one flag per device per scan
        }
      }
    }
  }

  // =====================================================================
  // RULE: TAC/LAC JUMP — Part B: Per-eNB TAC Consistency
  //
  // A legitimate eNB broadcasts a FIXED TAC. If the same eNB (identified
  // by cell_enb) is observed with multiple different TAC values across
  // ANY measurements (from any device), it indicates:
  //   - IMSI catcher spoofing the eNB identity with wrong TAC
  //   - Cell-in-the-middle rewriting TAC to force tracking area updates
  //   - Operator misconfiguration (rare, lower severity)
  //
  // Groups all measurements by eNB, counts unique TAC values per eNB.
  // Flags if unique TAC count >= minTacChanges threshold (default: 2).
  //
  // Works across 2G (LAC), 3G (LAC), 4G (TAC), 5G (TAC) — the field
  // cell_tac holds LAC for 2G/3G and TAC for 4G/5G.
  // =====================================================================
  if (isRuleEnabled('TAC_LAC_JUMP')) {
    const MIN_TAC_CHANGES = getThreshold('TAC_LAC_JUMP', 'minTacChanges', 2);

    // Build per-eNB TAC map: eNB → { tacs: Map<tac, [measurements]> }
    const enbTacMap = {};

    for (const m of measurements) {
      const enb = m.cell_enb;
      if (!enb) continue;
      const tac = m.cell_tac != null ? Number(m.cell_tac) : null;
      if (tac == null) continue;

      const key = String(enb);
      if (!enbTacMap[key]) enbTacMap[key] = { tacs: new Map() };

      if (!enbTacMap[key].tacs.has(tac)) enbTacMap[key].tacs.set(tac, []);
      enbTacMap[key].tacs.get(tac).push(m);
    }

    // Check each eNB for TAC inconsistency
    for (const [enbKey, data] of Object.entries(enbTacMap)) {
      const uniqueTacCount = data.tacs.size;
      if (uniqueTacCount < MIN_TAC_CHANGES) continue;

      // This eNB has been seen with multiple TACs — suspicious
      const tacEntries = Array.from(data.tacs.entries());
      tacEntries.sort((a, b) => b[1].length - a[1].length); // Sort by count descending

      // The most common TAC is likely the legitimate one
      const dominantTac = tacEntries[0][0];
      const dominantCount = tacEntries[0][1].length;

      // Flag every measurement with a non-dominant TAC
      for (let t = 1; t < tacEntries.length; t++) {
        const anomalousTac = tacEntries[t][0];
        const anomalousMeasurements = tacEntries[t][1];

        const INVALID_TACS = new Set([0, 1, 65534, 65535, 16777215]);
        const hasInvalidTac = INVALID_TACS.has(anomalousTac);

        for (const m of anomalousMeasurements) {
          // Skip if already flagged by Part A (reserved TAC detection)
          const alreadyFlagged = flags.some(f =>
            f.sample_id === m.sample_id && f.rule === 'TAC_LAC_JUMP'
          );
          if (alreadyFlagged) continue;

          const rsrp = m.signal_rsrp != null ? Number(m.signal_rsrp) : null;
          const strongSignal = rsrp != null && rsrp > -70;

          const mLat = m.location_lat_rounded ? Number(m.location_lat_rounded) : null;
          const mLng = m.location_lng_rounded ? Number(m.location_lng_rounded) : null;
          const mPlmn = m.network_PLMN || '';
          const knownSite = lookupSite(Number(enbKey), mPlmn, mLat, mLng);
          const isUnknownSite = !knownSite;

          // Severity escalation
          let sev = HIGH;
          const escalations = [];

          if (hasInvalidTac) {
            sev = CRITICAL;
            escalations.push('reserved TAC=' + anomalousTac);
          }
          if (strongSignal) {
            sev = CRITICAL;
            escalations.push('strong signal ' + rsrp + ' dBm');
          }
          if (isUnknownSite) {
            sev = CRITICAL;
            escalations.push('unknown cell');
          }

          const pci = m.cell_pci != null ? m.cell_pci : '?';
          const tech = String(m.tech || 'LTE').toUpperCase();
          const idType = ['GSM', 'EDGE', 'GPRS', 'WCDMA', 'HSPA', 'UMTS'].includes(tech) ? 'LAC' : 'TAC';
          const escStr = escalations.length > 0 ? ' — ' + escalations.join(', ') : '';

          flags.push({
            sample_id: m.sample_id,
            rule: 'TAC_LAC_JUMP',
            severity: sev,
            score: sev === CRITICAL ? 0.92 : 0.78,
            details: `eNB ${enbKey} (PCI ${pci}) seen with ${uniqueTacCount} different ${idType}s: ` +
              `expected ${idType}=${dominantTac} (${dominantCount} samples) but got ${idType}=${anomalousTac}${escStr}`,
            cell_id: String(pci),
            location_lat: mLat,
            location_lng: mLng,
          });
        }
      }
    }
  }

  // -----------------------------------------------------------------------
  // POST-LOOP: MULTI-DEVICE CORRELATION
  // If 3+ devices flag the same suspicious cell (same eNB or PCI+EARFCN),
  // boost confidence. A single device could be a fluke; 3+ is corroboration.
  // -----------------------------------------------------------------------
  const cellFlagCorrelation = {}; // "enb|pci" → { devices: Set, rules: Set, sampleIds: [] }
  for (const f of flags) {
    if (f.severity === 'LOW') continue; // skip context flags
    const cellKey = f.cell_id || 'x';
    if (!cellFlagCorrelation[cellKey]) {
      cellFlagCorrelation[cellKey] = { devices: new Set(), rules: new Set(), sampleIds: [] };
    }
    // Find the source measurement to get device ID
    const srcMeas = measurements.find(m => m.sample_id === f.sample_id);
    if (srcMeas && srcMeas.deviceInfo_deviceId) {
      cellFlagCorrelation[cellKey].devices.add(srcMeas.deviceInfo_deviceId);
    }
    cellFlagCorrelation[cellKey].rules.add(f.rule);
    cellFlagCorrelation[cellKey].sampleIds.push(f.sample_id);
  }

  // Boost: add MULTI_DEVICE_CORROBORATION flag for cells flagged by 3+ devices
  for (const [cellKey, corr] of Object.entries(cellFlagCorrelation)) {
    if (corr.devices.size >= 3) {
      // Add one corroboration flag per cell (attach to first sample)
      flags.push({
        sample_id: corr.sampleIds[0],
        rule: 'MULTI_DEVICE_CORROBORATION',
        severity: CRITICAL,
        score: 0.95,
        details: `Cell ${cellKey} flagged by ${corr.devices.size} independent devices — ` +
          `rules: ${[...corr.rules].join(', ')} — high-confidence threat`,
        cell_id: cellKey,
        location_lat: null, location_lng: null,
      });
    }
  }

  // (NEW_CELL_IN_AREA / UNKNOWN_ENB removed — sites DB is incomplete, missing eNB ≠ rogue)

  return flags;
}

module.exports = { runRules };
