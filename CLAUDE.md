# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FlycommC2 — RF threat detection platform for cellular network security. Analyzes ClickHouse measurement data through deterministic rules to detect IMSI catchers, rogue base stations, jamming, and GPS spoofing. Serves a real-time SOC dashboard.

## Repository

GitHub: https://github.com/lugasia/FlyC2 (branch: `main`)

## UI/UX Skill

**For ALL UI/UX work, use the ui-ux-pro-max skill** installed at `agent/.claude/skills/ui-ux-pro-max/`.

Read `agent/.claude/skills/ui-ux-pro-max/SKILL.md` before making visual changes. Use the search tool for design guidance:
```bash
python3 agent/.claude/skills/ui-ux-pro-max/src/scripts/search.py "<query>" --domain <domain>
```
Domains: `product`, `style`, `typography`, `color`, `landing`, `chart`, `ux`. Stacks: `html-tailwind` (our stack).

Priority order: Accessibility → Touch/Interaction → Performance → Style → Layout → Typography/Color → Animation → Forms → Navigation → Charts.

## Commands

```bash
cd agent && npm install
node start.js            # Dashboard at http://localhost:3000
node test-rules.js       # Test rules against live data
node diagnose.js         # Check ClickHouse schema
```

## Architecture

Plain Node.js (>=18), CommonJS, no build step. ~8,500 LOC across 18 core files. Monolithic dashboard (`dashboard/index.html`, ~8,500 lines inline HTML/CSS/JS).

### Data Flow

```
Browser Dashboard (index.html)
  │ HTTPS + JWT
  ▼
server.js (Express)
  ├─ authMiddleware → JWT verify, attach org context
  ├─ Demo intercept → demoGenerator.js (synthetic data)
  └─ Scan orchestration:
     1. orgStore → cluster polygon for bbox clipping
     2. db → getRecentMeasurementsFiltered()
     3. db → getTargetedAnomalies() [TA=0, MCC, downgrades]
     4. db → getBadMeasurementsInBbox()
     5. db → getKnownCells(bbox) [cached 1hr]
     6. rules → runRules() → deterministic flags
     7. stats → runStatistics() → z-score flags
     8. Merge + deduplicate + aggregate
     9. recordAlert() → in-memory history + SSE + webhook
     10. Response → enriched measurements + anomalies
  │
  ▼
ClickHouse (READ-ONLY, HTTPS port 8443)
  ├─ measurements (billions of rows)
  ├─ bad_measurements (RF anomalies, raw JSON)
  ├─ sites (known base stations, ~5,730 rows)
  ├─ modem_measurements (RSU serving/neighbor cells)
  ├─ agent_state (watermark for agent loop)
  └─ threat_events (confirmed threats)
```

### File-by-File Architecture

#### Entry Point

**`start.js`** (22 lines) — Application bootstrap. Displays startup banner, calls `startServer()` from server.js.

#### Backend Core

**`server.js`** (~1,850 lines) — Express HTTP API, scan orchestration, alert management, multi-tenant routing.
- **40+ API endpoints**: auth, scan, threats, measurements, RSU, sites, settings, admin, debug
- **Primary scan**: `GET /api/scan/live` — orchestrates the full detection pipeline (10-step flow above)
- **Chunked scan**: `GET /api/scan/chunk` — progressive loading for large datasets
- **Alert system**: In-memory history (5,000 max), SSE push to browsers, webhook dispatch (CRITICAL/HIGH only)
- **Key helpers**: `safeRoute()` error wrapper, `signToken()` JWT creation, `polygonToBbox()`/`clipBbox()` geo utils
- **Global state**: `scanInProgress` lock (prevents concurrent scans), `deploymentMode` (SDK/RSU), `ruleThresholds` per-rule config
- **Key routes**:
  - Auth: `POST /api/auth/login`, `GET /api/auth/me`, `GET /api/status`
  - Scan: `GET /api/scan/live`, `/scan/meta`, `/scan/chunk`
  - RSU: `GET /api/rsu/devices`, `/rsu/device/:id`, `/rsu/device/:id/timeline`
  - Admin: `/api/admin/*` (super_admin only) — org/user CRUD, logo, demo RSU config
  - Settings: `GET/POST /api/settings/clickhouse`, `/api/mode`, `/api/thresholds`

**`db.js`** (~2,186 lines) — ClickHouse client, 50+ query functions, connection pooling, retry logic.
- **Client lifecycle**: `getClient()`, `resetClient()`, `queryWithRetry()` (2 retries on ECONNRESET/ETIMEDOUT)
- **Scan queries**: `getRecentMeasurementsFiltered()`, `getTargetedAnomalies()` (UNION ALL), `getAllUniqueMeasurements()` (RSU chunked)
- **Bad measurements**: `getBadMeasurementsInBbox()`, `getBadMeasurementsRFAnomalies()`, `parseBadMeasurementRawRecord()`
- **Sites**: `getKnownCells(bbox)` — cached 1hr TTL, bbox-filtered, 0.5° margin expansion
- **RSU**: `getNewRSUMeasurements()`, `getNewModemMeasurements()`, `getRSUDevices()`, `getRSUDeviceHistory()`
- **Baselines**: `getCellBaselines(cellIds)` — 24h rolling stats for z-score
- **Connection**: HTTPS port 8443, Basic auth, 300s timeout, max 1 connection, sequential queries

**`config.js`** (~658 lines) — Configuration singleton, MCC registry, region presets, operator band licenses.
- **ClickHouse config**: host, port, username, password, database (from `.env` or runtime POST)
- **MCC registry**: 200+ entries (country + flag emoji). 425=Israel, 310=USA, etc.
- **Region presets**: GLOBAL, ISRAEL, USA, EU — each with bbox + expectedMCC array
- **`OPERATOR_LICENSED_BANDS`**: Per-PLMN EARFCN ranges (e.g., "425-01" Partner Israel → B3, B7, B20)
- **Band definitions**: 3GPP TS 36.101 EARFCN→band (LTE B1-B43), NR ARFCN→band (n1-n78)
- **Exports**: `earfcnToBandExported()`, `nrArfcnToBandExported()`, `getMCCInfo()`, `getRegion()`, `isConfigured()`, severity levels

#### Detection Engine

**`rules.js`** (~1,245 lines) — Deterministic anomaly detection based on 3GPP standards and RF physics.
- **Main export**: `runRules(measurements, knownCells, expectedMCCs, regionBbox, thresholds)` → array of `{ sample_id, rule, severity, score, details }`
- **Site indexing**: `buildSiteIndex()` → fast lookup by eNB/PLMN/PCI with fallback chain (PLMN-exact → ID → PCI proximity)
- **Multi-pass architecture**:
  1. Enrichment pass — fill missing eNB/ECI from PCI+PLMN+location
  2. Pre-analysis — TA=0 clusters, PCI collisions, 2G downgrade clusters
  3. Per-measurement loop — all physics/identity rules
  4. Post-measurement — behavioral rules
- **Physics helpers**: `haversineKm()`, `taToMeters()` (78.12m/step), `rsrpToEstimatedKm()` (Okumura-Hata path loss)
- **Design rules**: Sites DB is incomplete (missing eNB ≠ anomaly), operator registry is incomplete (missing PLMN ≠ phantom), only flag positively known issues

**Physics/identity rules** (single sample sufficient):
| Rule | What it detects |
|------|----------------|
| `MCC_ANOMALY` | network_mcc not in expected MCCs for region |
| `UNLICENSED_BAND` | EARFCN not in operator's licensed spectrum (per 3GPP) |
| `CELL_LOCATION_MISMATCH` | TA/RSRP physics conflict with known site (band-aware: low 18km, mid 10km, high 5km) |
| `GPS_SPOOFING` | TA→distance contradicts GPS location |
| `JAMMING_INDICATOR` | Strong RSRP (>-65dBm) + poor RSRQ (<-15dB) |
| `TAC_ANOMALY` | TA value >1282 (violates 3GPP) |
| `EMPTY_NEIGHBORS` | Serving cell ≠ "0" but neighbor list empty |
| `TA_RSRP_MISMATCH` | TA implies far distance but RSRP very strong |
| `EXTREME_RSRP` | RSRP <-140dBm or >-43dBm (outside LTE bounds) |
| `FREQUENCY_BAND_MISMATCH` | EARFCN inconsistent with declared band |

**Behavioral rules** (≥2 unique devices):
| Rule | What it detects |
|------|----------------|
| `TA_ZERO_CLUSTER` | 3+ devices same PCI+EARFCN with TA≤1 (IMSI catcher signature) |
| `NETWORK_DOWNGRADE` | 2+ devices on legacy 2G/3G on expected PLMN |
| `RAPID_RESELECTION` | 5+ cells in <30sec (device being probed) |

**RSU-only**: `PCI_COLLISION` — same PCI + EARFCN + <2km + <5min
**Post-scan**: `MULTI_DEVICE_CORROBORATION` — 3+ devices flagged on same cell

**`stats.js`** (97 lines) — Z-score anomaly detection against 24h rolling baselines.
- **Export**: `runStatistics(measurements, baselines)` → array of `{ sample_id, stat_check, z_score, severity }`
- **Baselines**: Per cell_pci from ClickHouse (avg/std for SNR, TA, RSRP)
- **Checks**: `SNR_ZSCORE` (jamming), `TA_ZSCORE` (distance anomaly), `CLUSTER_ANOMALY` (>80% anomalous = coordinated event → CRITICAL)
- **Scoring**: `scoreFromZ(z) = min(0.9, 0.5 + |z| * 0.1)`, threshold |z| > 3

#### RSU Real-Time Monitoring

**`rsuMonitor.js`** (~665 lines) — Delta-based polling with per-device state machine for RSU hardware.
- **Key class**: `OrgMonitor` — one per RSU organization, polls every 5s
- **Lifecycle**: `start()` → discover RSU orgs → `bootstrap()` per org → `poll()` loop → `stop()`
- **State machine per device**: Tracks `jammingActive`, `signalDegradationActive`, `gpsJammingActive`, `gpsSpoofingActive`, `foreignMCCActive`, baseline RSRP/RSRQ/satCount, known cells/TACs, anchor position
- **Key concept**: RSUs are STATIC sensors — alerts fire on **state transitions** only, not raw thresholds. Repeated alerts for same ongoing condition are suppressed.
- **Poll sources** (configurable via `/api/rsu/sources`): `useModemMeasurements` (modem_measurements table) and/or `useMeasurements` (measurements table, source='modem')
- **CRITICAL**: Only evaluate **serving cell** measurements (has enodeb_id). Neighbor cells have no eNB and inter-frequency RSRP values that cause false positives.

**`rsuRules.js`** (~588 lines) — Change-based detection rules for RSU monitoring.
- **Export**: `runRSURules(measurement, deviceState, clusterState, orgConfig)` → alert or null
- **Rules**: `RSU_MCC_MISMATCH` (foreign MCC transition), `RSU_NEW_CELL` (unseen PCI+eNB+EARFCN), `RSU_JAMMING` (RSRP >-60 + RSRQ <-20), `RSU_SIGNAL_DEGRADATION` (RSRP drops >15dB or RSRQ <-20), `RSU_GPS_JAMMING` (accuracy jump), `RSU_GPS_SPOOFING` (TA vs GPS mismatch), `RSU_UNKNOWN_ENB` (eNB not in sites DB)
- **Helpers**: `extractMCC()`, `extractPCI()`, `extractENB()`, `extractEARFCN()`, `extractRSRP()`, `extractRSRQ()`, `isServingCell()`, `updateDeviceState()`

#### Multi-Tenant Architecture

**`orgStore.js`** (~177 lines) — Flat-file JSON organization + user CRUD with bcrypt passwords.
- **Data file**: `orgs.json` (gitignored), template: `orgs.example.json`
- **Org fields**: id, name, license (`SDK`|`RSU`|`BOTH`), cluster (GeoJSON Polygon), sub_clusters, logo_url, demo_mode, demo_rsus
- **User fields**: id, email, password_hash (bcrypt), name, org_id, role (`super_admin`|`admin`|`operator`)
- **Exports**: `getOrgs()`, `getOrg()`, `createOrg()`, `updateOrg()`, `deleteOrg()`, `getUsers()`, `getUserByEmail()`, `createUser()`, `verifyPassword()`, `sanitizeUser()`

**`authMiddleware.js`** — JWT validation middleware.
- Validates `Authorization: Bearer <token>` on every API call
- Attaches `req.auth = { userId, email, orgId, role, license }` to request
- `requireRole(...roles)` middleware for role-based access control

**`demoGenerator.js`** (~200 lines) — Synthetic measurement generator for demo mode orgs.
- **Export**: `generateDemoData(org, {count, anomalyRatio, mode})` → `{ measurements, expectedMCC }`
- Generates 85% normal + 15% anomaly measurements within org cluster bbox
- **Injected anomalies** (~4% each): IMSI catcher (TA=0 cluster), rogue MCC (001 test network), jamming (strong RSRP + bad RSRQ), GPS spoofing (TA vs signal mismatch)
- Uses real Israeli operator PLMNs (425-01/02/03), realistic signal ranges

**`seed.js`** (71 lines) — Bootstrap CLI to create `orgs.json` from template.
- Usage: `node seed.js [--admin-pass=X] [--demo-pass=X] [--force]`
- Generates UUIDs, hashes passwords with bcrypt, links users to orgs

#### AI Escalation & Alerting

**`ai.js`** (~133 lines) — Claude API integration for high-suspicion event confirmation.
- **Export**: `escalateToAI(flaggedEvents)` → confirmed threats with confidence scores
- Filters events above `suspicionThreshold`, batches to Claude API
- Threat types: IMSI_CATCHER, ROGUE_BASE_STATION, JAMMING, DOWNGRADE_ATTACK, SIGNAL_SPOOFING, DISTANCE_SPOOFING

**`alerts.js`** (95 lines) — Format and dispatch confirmed threats.
- **Export**: `dispatch(threats)` → console (color-coded) + JSON log + webhook + ClickHouse alert_log
- Severity coloring: CRITICAL=red, HIGH=orange, MEDIUM=yellow, LOW=gray

#### Agent Loop (Standalone Mode)

**`index.js`** (~184 lines) — Standalone polling loop (alternative to dashboard server).
- Lifecycle: load watermark → fetch delta → load sites → run rules → run stats → escalate to AI → write threat_events → dispatch alerts → update watermark → sleep → repeat
- Single-threaded, skips if previous loop still running

**`state.js`** (37 lines) — Watermark persistence for agent loop resumption.
- `loadWatermark()` / `saveWatermark(ts)` — reads/writes ClickHouse `agent_state` table

#### Diagnostics

**`diagnose.js`** (88 lines) — Schema discovery CLI. Runs `DESCRIBE TABLE`, sample queries, counts, signal/PLMN/location samples.

**`test-rules.js`** (176 lines) — Load recent measurements, run full detection, print results.

**`test-connection.js`** (44 lines) — Quick ClickHouse `SELECT 1` connectivity check.

### Frontend Dashboard (dashboard/index.html)

Monolithic ~8,500-line inline HTML/CSS/JS file. No build step, no framework.

**Tech**: Leaflet.js (map), leaflet-draw (polygon drawing), leaflet-heat (heatmap), Canvas (charts).

**Layout**:
- **Auth gate**: Full-screen login/setup if ClickHouse not configured
- **Sidebar** (270px): SDK/RSU mode toggle, region selector, time range, scan area (draw/search/preset), threshold config, connection settings
- **Map**: CartoDB dark tiles, anomaly markers (color=severity), heatmap layer (toggled per threat type), RSU device icons
- **Threat Breakdown**: CSS donut chart + clickable bar chart per rule type. Click = filter table. Shift+click = heatmap.
- **Threat Timeline**: Canvas stacked area chart (MEDIUM/HIGH/CRITICAL over time)
- **Scan Table**: Sortable, filterable anomalies. Row click → Cell Threat Profile slide-out + fly-to-map.
- **RSU Panel**: Right slide-out with Cellular/QoE/GNSS/Alerts tabs. Timeline playback with date range + bucket size.
- **Cell Profile**: Right slide-out with cell identity, signal bars, detection flags, same-cell history.
- **Executive Summary**: Left slide-out (480px) with 6 sections: Scan Overview, Severity Distribution, Anomaly Type Breakdown, MNO Breakdown, Top Anomalous Cells, Time Pattern. Computed client-side from `accumulatedAnomalies`/`accumulatedClean`/`accumulatedSummary`. Interactive drill-down: click rule/PLMN/cell → filter table or open Cell Profile.

**Color scheme**: Dark theme (`#080c14` bg, `#e8edf5` text). Severity: CRITICAL `#ff3b3b`, HIGH `#ff8c00`, MEDIUM `#ffc107`, LOW `#5c6b80`.

**API integration**: JWT in localStorage, `Authorization` header on all calls, `?token=` for SSE EventSource. `API_BASE` via `?api=` query param or localStorage.

### Scan Flow (server.js → rules.js)

`GET /api/scan/live` orchestrates the scan:
1. Auth & org scope — verify JWT, get cluster polygon, clip bbox
2. Demo mode check — if `org.demo_mode=true`, return synthetic data from `demoGenerator.js`
3. Fetch recent measurements from ClickHouse (bbox + time filtered, LIMIT 1000)
4. Short-circuit if 0 results (no data in area)
5. Fetch targeted anomalies via single UNION ALL: TA=0 (500) + MCC anomalies (500) + downgrades (100)
6. Fetch bad_measurements (bbox-filtered in Node.js, 30s timeout)
7. Merge + deduplicate by `sample_id`
8. Load known cells from `sites` table (cached in memory, 1hr TTL)
9. Run `rules.js` deterministic detection
10. Run `stats.js` statistical z-score detection (optional — skip on error)
11. Enrich measurements with flags, compute `is_anomalous` (requires severity > LOW)
12. Aggregate by (rule + cell_id + eNB) for table view
13. Record alerts to in-memory history + SSE push + webhook (CRITICAL/HIGH only)
14. Return enriched measurements + aggregated anomalies + summary to dashboard

Supports custom date ranges via `startDate`/`endDate` query params (uses `BETWEEN parseDateTimeBestEffort()` in SQL).

### Multi-Tenant Architecture (v2)

FlycommC2 is a licensed SaaS platform. Each client is an **Organization** with a license, cluster polygon, and branding.

**Auth**: JWT-based (email + password). Token in `Authorization: Bearer <token>` header on every API call.
- `orgStore.js` — reads/writes `orgs.json` (flat-file JSON, gitignored)
- `authMiddleware` — validates JWT, attaches `req.auth = { userId, email, orgId, role, license }`
- Roles: `super_admin` (all orgs), `admin` (own org), `operator` (read-only)

**Organization scoping**:
- `license`: `"SDK"` | `"RSU"` | `"BOTH"` — controls available deployment modes
- `cluster`: GeoJSON Polygon — ALL scans are bbox-clipped to this polygon
- `sub_clusters`: array of named sub-polygons within the cluster
- `logo_url`: white-label branding (replaces FlycommC2 logo)
- `demo_mode`: when true, scan pipeline returns synthetic anomalies from `demoGenerator.js`

**Admin API** (`/api/admin/*`): super_admin only — org CRUD, user CRUD, logo, demo RSU config.

### Deployment

- **Frontend**: `frontend/index.html` hosted on Railway (static serve). `dashboard/index.html` kept in sync.
- **Backend**: `agent/` with `node start.js` locally (VPN to ClickHouse)
- **`API_BASE`**: set via `?api=` query param or localStorage
- **Deployment modes**:
  - **SDK Mode** (default): Scans measurements from mobile SDK, includes bad_measurements, full rule detection
  - **RSU Mode**: Scans from Quectel modem hardware (roadside units), change-based RSU rules, real-time state machine monitoring
  - **Demo Mode**: When `org.demo_mode=true`, no ClickHouse queries, synthetic data from `demoGenerator.js`

### Design Rules for Detection Engine
- **Sites DB is incomplete** — never treat missing eNB as anomaly
- **Operator registry is incomplete** — missing PLMN ≠ phantom cell
- **Only flag what we positively know is wrong** (e.g., UNLICENSED_BAND = operator broadcasting on spectrum they don't own per 3GPP)
- **Physics thresholds are hardcoded**: TA=78.12m/step, EARFCN→band per 3GPP TS 36.101
- **Band-aware distances**: low band (700-900MHz) = 18km, mid (1800-2100MHz) = 10km, high (2600MHz+) = 5km

### ClickHouse Connection
- **READ-ONLY** access via HTTPS port 8443
- Credentials configurable at runtime via `POST /api/settings/clickhouse` or `.env` file
- `config.isConfigured()` returns true when host + username are set
- Cold starts: `request_timeout: 300000` (5min), `max_open_connections: 1`, sequential queries
- `queryWithRetry()` handles ECONNRESET/Timeout with client reset + retry
- **Tables**: measurements, bad_measurements, sites, modem_measurements, agent_state, alert_log, threat_events

### Key Files Summary
| File | Lines | Purpose |
|------|-------|---------|
| `start.js` | 22 | Entry point, calls `startServer()` |
| `server.js` | ~1,850 | Express API, scan orchestration, alerts, SSE, admin |
| `db.js` | ~2,186 | ClickHouse client, 50+ queries, retry, sites cache |
| `config.js` | ~658 | MCC registry, regions, operator bands, severity levels |
| `rules.js` | ~1,245 | Deterministic detection (14 rules), multi-pass architecture |
| `stats.js` | 97 | Z-score anomaly detection (3 statistical checks) |
| `rsuMonitor.js` | ~665 | RSU polling loop, per-device state machine |
| `rsuRules.js` | ~588 | RSU change-based detection (7 rules) |
| `orgStore.js` | ~177 | Multi-tenant org/user CRUD (flat-file JSON + bcrypt) |
| `demoGenerator.js` | ~200 | Synthetic anomaly generator for demo mode |
| `seed.js` | 71 | Bootstrap `orgs.json` with bcrypt hashes |
| `ai.js` | ~133 | Claude API escalation for high-suspicion events |
| `alerts.js` | 95 | Alert dispatch (console, JSON, webhook, ClickHouse) |
| `index.js` | ~184 | Standalone agent polling loop |
| `state.js` | 37 | Watermark persistence for agent loop |
| `diagnose.js` | 88 | Schema discovery CLI |
| `test-rules.js` | 176 | Detection test runner |
| `dashboard/index.html` | ~8,500 | Full SOC dashboard (inline HTML/CSS/JS) |

### Domain Concepts
- **MCC** — Mobile Country Code (first 3 digits of PLMN). 425=Israel, 310=USA, 630=Congo.
- **PLMN** — Public Land Mobile Network (MCC-MNC). e.g., 425-01 = Partner Israel.
- **PCI** — Physical Cell ID (0-503 LTE). Reused across network by design — NOT unique.
- **eNB** — eNodeB ID (base station). Derived from ECI: `enbId = eci >> 8`.
- **EARFCN** — frequency channel number. Maps to band via 3GPP formula: `F_DL = F_DL_low + 0.1 * (EARFCN - N_offs_DL)`.
- **TA** — Timing Advance. Each step ≈ 78.12m distance. TA=0 with multiple UEs = IMSI catcher indicator.
- **RSRP/RSRQ/SINR** — signal power/quality/noise metrics. Strong RSRP + poor RSRQ = jamming signature.
