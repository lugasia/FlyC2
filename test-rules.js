/**
 * Quick test: runs live scan against real ClickHouse data and prints results.
 * Usage: node test-rules.js
 */
require('dotenv').config();
const config = require('./config');
const db = require('./db');
const { runRules } = require('./rules');
const { runStatistics } = require('./stats');

async function main() {
  console.log('=== FlycommC2 Rule Engine v3 — Live Test ===\n');
  console.log(`Region: ${config.agent.region} (${config.getRegion().name})`);

  // 1. Health check
  await db.healthCheck();
  console.log('ClickHouse: OK\n');

  // 2. Load known cells (sites table)
  const knownCells = await db.getKnownCells();
  console.log(`Sites loaded: ${knownCells.length}`);

  // Debug: check sectors format — crucial for understanding how ClickHouse returns Nested data
  if (knownCells.length > 0) {
    const sample = knownCells[0];
    console.log(`  Sample site_id: ${sample.site_id}, lat: ${sample.lat}, lng: ${sample.lng}, tech: ${sample.tech}`);
    console.log(`  height: ${sample.height}, max_distance_propagation: ${sample.max_distance_propagation}`);
    console.log(`  sectors type: ${typeof sample.sectors}`);
    console.log(`  sectors isArray: ${Array.isArray(sample.sectors)}`);

    // Check for flattened Nested columns
    const allKeys = Object.keys(sample);
    const nestedKeys = allKeys.filter(k => k.startsWith('sectors.'));
    if (nestedKeys.length > 0) {
      console.log(`  !! Nested flattened keys found: ${nestedKeys.join(', ')}`);
      for (const k of nestedKeys) {
        const val = sample[k];
        console.log(`    ${k}: type=${typeof val}, isArray=${Array.isArray(val)}, value=${JSON.stringify(val).substring(0, 150)}`);
      }
    }

    if (typeof sample.sectors === 'string') {
      console.log('  !! sectors is a STRING');
      console.log('  Raw value (first 300 chars):', String(sample.sectors).substring(0, 300));
      try {
        const parsed = JSON.parse(sample.sectors);
        console.log(`  Parsed sectors: ${parsed.length} entries`);
        if (parsed.length > 0) console.log(`  First sector:`, JSON.stringify(parsed[0]).substring(0, 200));
      } catch (e) {
        console.log('  !! Failed to parse sectors JSON:', e.message);
      }
    } else if (Array.isArray(sample.sectors)) {
      console.log(`  sectors count: ${sample.sectors.length}`);
      if (sample.sectors.length > 0) {
        const first = sample.sectors[0];
        console.log(`  First sector type: ${typeof first}, isArray: ${Array.isArray(first)}`);
        console.log(`  First sector:`, JSON.stringify(first).substring(0, 300));
      }
    } else if (sample.sectors && typeof sample.sectors === 'object') {
      console.log(`  sectors is an object (not array):`, JSON.stringify(sample.sectors).substring(0, 300));
    }

    // Print ALL keys of the sample site to see what ClickHouse actually returns
    console.log(`  All site keys: ${allKeys.join(', ')}`);
  }

  // 3. Get recent measurements (use region-filtered if set)
  const region = config.getRegion();
  const limit = 200;
  let measurements;
  if (region.bbox) {
    measurements = await db.getRecentMeasurementsFiltered(region.bbox, limit);
  } else {
    measurements = await db.getRecentMeasurements(limit);
  }
  console.log(`\nMeasurements loaded: ${measurements.length}`);

  if (measurements.length === 0) {
    console.log('No measurements in time window. Try increasing time range or changing region.');
    await db.close();
    return;
  }

  // Debug: show a sample measurement
  const m0 = measurements[0];
  console.log(`\nSample measurement:`);
  console.log(`  timestamp: ${m0.timestamp}`);
  console.log(`  cell_pci: ${m0.cell_pci}, cell_eci: ${m0.cell_eci}, cell_enb: ${m0.cell_enb}`);
  console.log(`  cell_ecgi: ${m0.cell_ecgi}, cell_tac: ${m0.cell_tac}`);
  console.log(`  network_PLMN: ${m0.network_PLMN}, network_mcc: ${m0.network_mcc}`);
  console.log(`  tech: ${m0.tech}, signal_rsrp: ${m0.signal_rsrp}, signal_snr: ${m0.signal_snr}`);
  console.log(`  signal_timingAdvance: ${m0.signal_timingAdvance}`);
  console.log(`  band_downlinkEarfcn: ${m0.band_downlinkEarfcn}`);
  console.log(`  location: (${m0.location_lat_rounded}, ${m0.location_lng_rounded})`);
  console.log(`  deviceInfo_deviceId: ${m0.deviceInfo_deviceId}`);
  console.log(`  network_isRoaming: ${m0.network_isRoaming}`);

  // Show TA distribution
  const taValues = measurements.map(m => m.signal_timingAdvance).filter(v => v !== null && v !== undefined);
  const taZeroCount = taValues.filter(v => Number(v) <= 1).length;
  console.log(`\nTA distribution: ${taValues.length} non-null values`);
  console.log(`  TA=0 or TA=1: ${taZeroCount} (${(taZeroCount/Math.max(1,taValues.length)*100).toFixed(1)}%)`);
  if (taValues.length > 0) {
    const sorted = taValues.map(Number).sort((a,b) => a-b);
    console.log(`  min=${sorted[0]}, max=${sorted[sorted.length-1]}, median=${sorted[Math.floor(sorted.length/2)]}`);
  }

  // Show unique eNB IDs
  const enbIds = [...new Set(measurements.map(m => m.cell_enb).filter(Boolean))];
  console.log(`\nUnique eNB IDs in measurements: ${enbIds.length}`);
  console.log(`  Values: ${enbIds.slice(0, 20).join(', ')}${enbIds.length > 20 ? '...' : ''}`);

  // 4. Run rules
  console.log('\n--- Running Rule Engine v3 ---');
  const ruleFlags = await runRules(measurements, knownCells);
  console.log(`Rule flags: ${ruleFlags.length}`);

  // Summarize by rule
  const ruleSummary = {};
  for (const f of ruleFlags) {
    const key = f.rule || 'UNKNOWN';
    if (!ruleSummary[key]) ruleSummary[key] = { count: 0, severities: {} };
    ruleSummary[key].count++;
    ruleSummary[key].severities[f.severity] = (ruleSummary[key].severities[f.severity] || 0) + 1;
  }
  for (const [rule, info] of Object.entries(ruleSummary)) {
    console.log(`  ${rule}: ${info.count} (${JSON.stringify(info.severities)})`);
  }

  // Show first 5 flags with details
  if (ruleFlags.length > 0) {
    console.log('\nFirst 5 rule flags:');
    for (const f of ruleFlags.slice(0, 5)) {
      console.log(`  [${f.severity}] ${f.rule}: ${f.details}`);
    }
  }

  // 5. Run statistical analysis
  console.log('\n--- Running Statistical Engine ---');
  const uniqueCellIds = [...new Set(
    measurements.map(m => String(m.cell_pci)).filter(id => id && id !== 'undefined' && id !== 'null')
  )];
  const baselines = await db.getCellBaselines(uniqueCellIds);
  console.log(`Baselines loaded for ${Object.keys(baselines).length} cells`);

  const statFlags = await runStatistics(measurements, baselines);
  console.log(`Stat flags: ${statFlags.length}`);

  const statSummary = {};
  for (const f of statFlags) {
    const key = f.stat_check || 'UNKNOWN';
    if (!statSummary[key]) statSummary[key] = 0;
    statSummary[key]++;
  }
  for (const [check, count] of Object.entries(statSummary)) {
    console.log(`  ${check}: ${count}`);
  }

  // 6. Combined summary
  const totalFlags = ruleFlags.length + statFlags.length;
  const flaggedSamples = new Set([
    ...ruleFlags.map(f => f.sample_id),
    ...statFlags.map(f => f.sample_id),
  ]);
  console.log(`\n=== SUMMARY ===`);
  console.log(`Total flags: ${totalFlags}`);
  console.log(`Unique samples flagged: ${flaggedSamples.size} / ${measurements.length}`);
  console.log(`Anomaly rate: ${(flaggedSamples.size / measurements.length * 100).toFixed(1)}%`);

  await db.close();
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
