require('dotenv').config();
const { createClient } = require('@clickhouse/client');

const credentials = Buffer.from(
  `${process.env.CLICKHOUSE_USER}:${process.env.CLICKHOUSE_PASSWORD}`
).toString('base64');

const client = createClient({
  url: `https://${process.env.CLICKHOUSE_HOST}:${process.env.CLICKHOUSE_PORT}`,
  database: process.env.CLICKHOUSE_DB || 'default',
  username: process.env.CLICKHOUSE_USER,
  password: process.env.CLICKHOUSE_PASSWORD,
  request_timeout: 30000,
  http_headers: { Authorization: `Basic ${credentials}` },
});

async function testQuery(label, query) {
  try {
    console.log(`\n--- ${label} ---`);
    const rs = await client.query({ query, format: 'JSONEachRow' });
    const rows = await rs.json();
    console.log('OK:', JSON.stringify(rows.slice(0, 5), null, 2));
    return rows;
  } catch (err) {
    console.error('FAIL:', err.message.substring(0, 300));
    return null;
  }
}

async function run() {
  console.log('=== FlycommC2 Diagnostics v2 ===\n');

  // 1. COLUMN NAMES — just get the names and types (no LIMIT on DESCRIBE)
  await testQuery('MEASUREMENTS COLUMNS', 'DESCRIBE TABLE measurements');

  // 2. ONE ROW — just the keys, printed compactly
  console.log('\n--- ONE ROW (keys + values) ---');
  try {
    const rs = await client.query({
      query: 'SELECT * FROM measurements ORDER BY timestamp DESC LIMIT 1',
      format: 'JSONEachRow',
    });
    const rows = await rs.json();
    if (rows.length > 0) {
      const row = rows[0];
      const keys = Object.keys(row);
      console.log(`Column count: ${keys.length}`);
      console.log('Columns:', keys.join(', '));
      console.log('\nValues:');
      for (const k of keys) {
        console.log(`  ${k}: ${JSON.stringify(row[k])}`);
      }
    }
  } catch (err) {
    console.error('FAIL:', err.message.substring(0, 300));
  }

  // 3. Quick counts
  await testQuery('MEASUREMENTS COUNT', 'SELECT count() AS total FROM measurements');
  await testQuery('24H COUNT', 'SELECT count() AS total FROM measurements WHERE timestamp > now() - INTERVAL 24 HOUR');

  // 4. Check signal columns we use in the dashboard
  await testQuery('SIGNAL SAMPLE', `SELECT signal_rsrp, signal_sinr, signal_timingAdvance, network_rat
    FROM measurements LIMIT 3`);

  // 5. Check PLMN columns
  await testQuery('PLMN SAMPLE', `SELECT cell_plmn, network_PLMN, count() AS c FROM measurements
    WHERE cell_plmn != '' AND cell_plmn IS NOT NULL
    GROUP BY cell_plmn, network_PLMN ORDER BY c DESC LIMIT 5`);

  // 6. Check location columns
  await testQuery('LOCATION SAMPLE', `SELECT location_lat_rounded, location_lng_rounded
    FROM measurements WHERE location_lat_rounded != 0 LIMIT 3`);

  // 7. Check system.parts access
  await testQuery('SYSTEM.PARTS', `SELECT sum(rows) AS total FROM system.parts WHERE table = 'measurements' AND active = 1`);

  // 8. Sites table schema
  await testQuery('SITES COLUMNS', 'DESCRIBE TABLE sites');

  // 9. Sites sample
  await testQuery('SITES SAMPLE', 'SELECT * FROM sites LIMIT 1');

  // 10. site_samples schema
  await testQuery('SITE_SAMPLES COLUMNS', 'DESCRIBE TABLE site_samples');

  await client.close();
  console.log('\n=== Done ===');
}

run().catch(console.error);
