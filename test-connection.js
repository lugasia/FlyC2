require('dotenv').config();
const { createClient } = require('@clickhouse/client');

const host = process.env.CLICKHOUSE_HOST;
const port = process.env.CLICKHOUSE_PORT;
const user = process.env.CLICKHOUSE_USER;
const pass = process.env.CLICKHOUSE_PASSWORD;

console.log('Testing ClickHouse connection...');
console.log('URL:', `https://${host}:${port}`);
console.log('User:', user);
console.log('Password:', `${pass.slice(0, 4)}...${pass.slice(-2)} (${pass.length} chars)`);

const credentials = Buffer.from(`${user}:${pass}`).toString('base64');
console.log('Auth header:', `Basic ${credentials.slice(0, 10)}...`);

async function test() {
  const client = createClient({
    url: `https://${host}:${port}`,
    database: process.env.CLICKHOUSE_DB || 'default',
    username: user,
    password: pass,
    request_timeout: 15000,
    http_headers: {
      Authorization: `Basic ${credentials}`,
    },
  });

  try {
    const rs = await client.query({ query: 'SELECT 1 AS ok', format: 'JSONEachRow' });
    const rows = await rs.json();
    console.log('\nConnection SUCCESS:', rows);

    const tables = await client.query({ query: 'SHOW TABLES', format: 'JSONEachRow' });
    const tableList = await tables.json();
    console.log('Tables:', tableList.map(t => t.name));
  } catch (err) {
    console.error('\nConnection FAILED:', err.message);
  } finally {
    await client.close();
  }
}

test();
