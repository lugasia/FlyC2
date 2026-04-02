/**
 * orgStore.js — Organization + User data layer (flat-file JSON)
 *
 * Reads/writes orgs.json. Each org has users, cluster polygon, license, branding.
 * Used by authMiddleware and admin API.
 */
const fs = require('fs');
const path = require('path');
const bcrypt = require('bcryptjs');
const crypto = require('crypto');

const DATA_FILE = path.join(__dirname, 'orgs.json');

// In-memory cache — reloaded from disk on every write
let data = null;

function load() {
  if (data) return data;
  try {
    const raw = fs.readFileSync(DATA_FILE, 'utf8');
    data = JSON.parse(raw);
  } catch (err) {
    // First boot — empty state
    data = { orgs: [], users: [] };
  }
  return data;
}

function save() {
  fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2), 'utf8');
}

function reload() {
  data = null;
  return load();
}

// ---------------------------------------------------------------------------
// Organizations
// ---------------------------------------------------------------------------
function getOrgs() {
  return load().orgs;
}

function getOrg(orgId) {
  return load().orgs.find(o => o.id === orgId) || null;
}

function createOrg({ name, license, cluster, sub_clusters, logo_url, demo_mode, demo_rsus }) {
  const d = load();
  const org = {
    id: crypto.randomUUID(),
    name,
    license: license || 'BOTH',           // SDK | RSU | BOTH
    cluster: cluster || null,              // GeoJSON Polygon
    sub_clusters: sub_clusters || [],      // [{ name, polygon }]
    logo_url: logo_url || null,
    demo_mode: !!demo_mode,
    demo_rsus: demo_rsus || [],            // [{ device_id, lat, lng, label }]
    created_at: new Date().toISOString(),
  };
  d.orgs.push(org);
  save();
  return org;
}

function updateOrg(orgId, updates) {
  const d = load();
  const idx = d.orgs.findIndex(o => o.id === orgId);
  if (idx === -1) return null;
  // Only allow safe fields
  const allowed = ['name', 'license', 'cluster', 'sub_clusters', 'logo_url', 'demo_mode', 'demo_rsus'];
  for (const key of allowed) {
    if (updates[key] !== undefined) d.orgs[idx][key] = updates[key];
  }
  save();
  return d.orgs[idx];
}

function deleteOrg(orgId) {
  const d = load();
  const idx = d.orgs.findIndex(o => o.id === orgId);
  if (idx === -1) return false;
  d.orgs.splice(idx, 1);
  // Also remove all users in this org
  d.users = d.users.filter(u => u.org_id !== orgId);
  save();
  return true;
}

// ---------------------------------------------------------------------------
// Users
// ---------------------------------------------------------------------------
function getUsers(orgId) {
  const d = load();
  if (orgId) return d.users.filter(u => u.org_id === orgId);
  return d.users;
}

function getUser(userId) {
  return load().users.find(u => u.id === userId) || null;
}

function getUserByEmail(email) {
  return load().users.find(u => u.email === email.toLowerCase()) || null;
}

async function createUser({ email, password, name, org_id, role }) {
  const d = load();
  if (d.users.find(u => u.email === email.toLowerCase())) {
    throw new Error('Email already exists');
  }
  const hash = await bcrypt.hash(password, 10);
  const user = {
    id: crypto.randomUUID(),
    email: email.toLowerCase(),
    password_hash: hash,
    name: name || email.split('@')[0],
    org_id: org_id || null,               // null for super_admin
    role: role || 'operator',             // super_admin | admin | operator
    created_at: new Date().toISOString(),
  };
  d.users.push(user);
  save();
  return sanitizeUser(user);
}

async function updateUser(userId, updates) {
  const d = load();
  const idx = d.users.findIndex(u => u.id === userId);
  if (idx === -1) return null;
  const allowed = ['email', 'name', 'role', 'org_id'];
  for (const key of allowed) {
    if (updates[key] !== undefined) {
      d.users[idx][key] = key === 'email' ? updates[key].toLowerCase() : updates[key];
    }
  }
  if (updates.password) {
    d.users[idx].password_hash = await bcrypt.hash(updates.password, 10);
  }
  save();
  return sanitizeUser(d.users[idx]);
}

function deleteUser(userId) {
  const d = load();
  const idx = d.users.findIndex(u => u.id === userId);
  if (idx === -1) return false;
  d.users.splice(idx, 1);
  save();
  return true;
}

async function verifyPassword(email, password) {
  const user = getUserByEmail(email);
  if (!user) return null;
  const match = await bcrypt.compare(password, user.password_hash);
  return match ? user : null;
}

// Strip password_hash from user objects before sending to client
function sanitizeUser(user) {
  if (!user) return null;
  const { password_hash, ...safe } = user;
  return safe;
}

function sanitizeUsers(users) {
  return users.map(sanitizeUser);
}

module.exports = {
  load, reload, save,
  getOrgs, getOrg, createOrg, updateOrg, deleteOrg,
  getUsers, getUser, getUserByEmail, createUser, updateUser, deleteUser,
  verifyPassword, sanitizeUser, sanitizeUsers,
};
