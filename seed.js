#!/usr/bin/env node
/**
 * seed.js — Bootstrap orgs.json from orgs.example.json with real bcrypt hashes
 *
 * Usage:
 *   node seed.js                          # creates orgs.json with default passwords
 *   node seed.js --admin-pass=MySecret    # set super_admin password
 *   node seed.js --demo-pass=DemoPass     # set demo user password
 *
 * Safe to re-run — will NOT overwrite existing orgs.json unless --force is passed.
 */
const fs = require('fs');
const path = require('path');
const bcrypt = require('bcryptjs');

const DATA_FILE = path.join(__dirname, 'orgs.json');
const EXAMPLE_FILE = path.join(__dirname, 'orgs.example.json');

async function main() {
  const args = process.argv.slice(2);
  const force = args.includes('--force');

  if (fs.existsSync(DATA_FILE) && !force) {
    console.log('orgs.json already exists. Use --force to overwrite.');
    process.exit(0);
  }

  if (!fs.existsSync(EXAMPLE_FILE)) {
    console.error('orgs.example.json not found — cannot seed.');
    process.exit(1);
  }

  // Parse CLI args
  let adminPass = 'admin123';
  let demoPass = 'demo123';
  for (const arg of args) {
    if (arg.startsWith('--admin-pass=')) adminPass = arg.split('=')[1];
    if (arg.startsWith('--demo-pass=')) demoPass = arg.split('=')[1];
  }

  const data = JSON.parse(fs.readFileSync(EXAMPLE_FILE, 'utf8'));

  // Hash passwords
  for (const user of data.users) {
    if (user.role === 'super_admin') {
      user.password_hash = await bcrypt.hash(adminPass, 10);
      console.log(`  Super admin: ${user.email} / ${adminPass}`);
    } else {
      user.password_hash = await bcrypt.hash(demoPass, 10);
      console.log(`  User: ${user.email} / ${demoPass}`);
    }
    // Generate real UUIDs
    user.id = require('crypto').randomUUID();
  }

  // Generate real org IDs and link users
  for (const org of data.orgs) {
    const oldId = org.id;
    org.id = require('crypto').randomUUID();
    // Update user references
    for (const user of data.users) {
      if (user.org_id === oldId) user.org_id = org.id;
    }
  }

  fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2), 'utf8');
  console.log('\norgs.json created successfully.');
  console.log('Start the server with: node start.js');
}

main().catch(err => { console.error(err); process.exit(1); });
