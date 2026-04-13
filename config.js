require('dotenv').config();

// ---------------------------------------------------------------------------
// MCC (Mobile Country Code) registry — maps MCC to country name
// Used for geographic filtering and MCC anomaly detection
// ---------------------------------------------------------------------------
const MCC_REGISTRY = {
  '001': { country: 'Test Network', flag: '🧪' },
  '202': { country: 'Greece', flag: '🇬🇷' },
  '204': { country: 'Netherlands', flag: '🇳🇱' },
  '206': { country: 'Belgium', flag: '🇧🇪' },
  '208': { country: 'France', flag: '🇫🇷' },
  '214': { country: 'Spain', flag: '🇪🇸' },
  '216': { country: 'Hungary', flag: '🇭🇺' },
  '218': { country: 'Bosnia and Herzegovina', flag: '🇧🇦' },
  '219': { country: 'Croatia', flag: '🇭🇷' },
  '220': { country: 'Serbia', flag: '🇷🇸' },
  '221': { country: 'Kosovo', flag: '🇽🇰' },
  '222': { country: 'Italy', flag: '🇮🇹' },
  '226': { country: 'Romania', flag: '🇷🇴' },
  '228': { country: 'Switzerland', flag: '🇨🇭' },
  '230': { country: 'Czech Republic', flag: '🇨🇿' },
  '234': { country: 'United Kingdom', flag: '🇬🇧' },
  '240': { country: 'Sweden', flag: '🇸🇪' },
  '242': { country: 'Norway', flag: '🇳🇴' },
  '244': { country: 'Finland', flag: '🇫🇮' },
  '248': { country: 'Estonia', flag: '🇪🇪' },
  '250': { country: 'Russia', flag: '🇷🇺' },
  '255': { country: 'Ukraine', flag: '🇺🇦' },
  '260': { country: 'Poland', flag: '🇵🇱' },
  '262': { country: 'Germany', flag: '🇩🇪' },
  '270': { country: 'Luxembourg', flag: '🇱🇺' },
  '272': { country: 'Ireland', flag: '🇮🇪' },
  '274': { country: 'Iceland', flag: '🇮🇸' },
  '276': { country: 'Albania', flag: '🇦🇱' },
  '278': { country: 'Malta', flag: '🇲🇹' },
  '280': { country: 'Cyprus', flag: '🇨🇾' },
  '282': { country: 'Georgia', flag: '🇬🇪' },
  '283': { country: 'Armenia', flag: '🇦🇲' },
  '284': { country: 'Bulgaria', flag: '🇧🇬' },
  '286': { country: 'Turkey', flag: '🇹🇷' },
  '288': { country: 'Faroe Islands', flag: '🇫🇴' },
  '289': { country: 'Abkhazia', flag: '🏴' },
  '290': { country: 'Greenland', flag: '🇬🇱' },
  '292': { country: 'San Marino', flag: '🇸🇲' },
  '293': { country: 'Slovenia', flag: '🇸🇮' },
  '294': { country: 'North Macedonia', flag: '🇲🇰' },
  '295': { country: 'Liechtenstein', flag: '🇱🇮' },
  '297': { country: 'Montenegro', flag: '🇲🇪' },
  '302': { country: 'Canada', flag: '🇨🇦' },
  '308': { country: 'Saint Pierre', flag: '🇵🇲' },
  '310': { country: 'United States', flag: '🇺🇸' },
  '311': { country: 'United States', flag: '🇺🇸' },
  '312': { country: 'United States', flag: '🇺🇸' },
  '313': { country: 'United States', flag: '🇺🇸' },
  '314': { country: 'United States', flag: '🇺🇸' },
  '315': { country: 'United States', flag: '🇺🇸' },
  '316': { country: 'United States', flag: '🇺🇸' },
  '330': { country: 'Puerto Rico', flag: '🇵🇷' },
  '334': { country: 'Mexico', flag: '🇲🇽' },
  '338': { country: 'Jamaica', flag: '🇯🇲' },
  '340': { country: 'Martinique', flag: '🇲🇶' },
  '346': { country: 'Cayman Islands', flag: '🇰🇾' },
  '350': { country: 'Bermuda', flag: '🇧🇲' },
  '352': { country: 'Barbados', flag: '🇧🇧' },
  '356': { country: 'Saint Kitts', flag: '🇰🇳' },
  '360': { country: 'Saint Vincent', flag: '🇻🇨' },
  '362': { country: 'Curacao', flag: '🇨🇼' },
  '363': { country: 'Aruba', flag: '🇦🇼' },
  '364': { country: 'Bahamas', flag: '🇧🇸' },
  '366': { country: 'Dominica', flag: '🇩🇲' },
  '370': { country: 'Dominican Republic', flag: '🇩🇴' },
  '372': { country: 'Haiti', flag: '🇭🇹' },
  '374': { country: 'Trinidad', flag: '🇹🇹' },
  '376': { country: 'Turks and Caicos', flag: '🇹🇨' },
  '400': { country: 'Azerbaijan', flag: '🇦🇿' },
  '401': { country: 'Kazakhstan', flag: '🇰🇿' },
  '402': { country: 'Bhutan', flag: '🇧🇹' },
  '404': { country: 'India', flag: '🇮🇳' },
  '405': { country: 'India', flag: '🇮🇳' },
  '410': { country: 'Pakistan', flag: '🇵🇰' },
  '412': { country: 'Afghanistan', flag: '🇦🇫' },
  '413': { country: 'Sri Lanka', flag: '🇱🇰' },
  '414': { country: 'Myanmar', flag: '🇲🇲' },
  '415': { country: 'Lebanon', flag: '🇱🇧' },
  '416': { country: 'Jordan', flag: '🇯🇴' },
  '417': { country: 'Syria', flag: '🇸🇾' },
  '418': { country: 'Iraq', flag: '🇮🇶' },
  '419': { country: 'Kuwait', flag: '🇰🇼' },
  '420': { country: 'Saudi Arabia', flag: '🇸🇦' },
  '421': { country: 'Yemen', flag: '🇾🇪' },
  '422': { country: 'Oman', flag: '🇴🇲' },
  '424': { country: 'UAE', flag: '🇦🇪' },
  '425': { country: 'Israel', flag: '🇮🇱' },
  '426': { country: 'Bahrain', flag: '🇧🇭' },
  '427': { country: 'Qatar', flag: '🇶🇦' },
  '428': { country: 'Mongolia', flag: '🇲🇳' },
  '429': { country: 'Nepal', flag: '🇳🇵' },
  '432': { country: 'Iran', flag: '🇮🇷' },
  '434': { country: 'Uzbekistan', flag: '🇺🇿' },
  '436': { country: 'Tajikistan', flag: '🇹🇯' },
  '437': { country: 'Kyrgyzstan', flag: '🇰🇬' },
  '438': { country: 'Turkmenistan', flag: '🇹🇲' },
  '440': { country: 'Japan', flag: '🇯🇵' },
  '441': { country: 'Japan', flag: '🇯🇵' },
  '450': { country: 'South Korea', flag: '🇰🇷' },
  '452': { country: 'Vietnam', flag: '🇻🇳' },
  '454': { country: 'Hong Kong', flag: '🇭🇰' },
  '455': { country: 'Macau', flag: '🇲🇴' },
  '456': { country: 'Cambodia', flag: '🇰🇭' },
  '457': { country: 'Laos', flag: '🇱🇦' },
  '460': { country: 'China', flag: '🇨🇳' },
  '466': { country: 'Taiwan', flag: '🇹🇼' },
  '467': { country: 'North Korea', flag: '🇰🇵' },
  '470': { country: 'Bangladesh', flag: '🇧🇩' },
  '472': { country: 'Maldives', flag: '🇲🇻' },
  '502': { country: 'Malaysia', flag: '🇲🇾' },
  '505': { country: 'Australia', flag: '🇦🇺' },
  '510': { country: 'Indonesia', flag: '🇮🇩' },
  '514': { country: 'East Timor', flag: '🇹🇱' },
  '515': { country: 'Philippines', flag: '🇵🇭' },
  '520': { country: 'Thailand', flag: '🇹🇭' },
  '525': { country: 'Singapore', flag: '🇸🇬' },
  '528': { country: 'Brunei', flag: '🇧🇳' },
  '530': { country: 'New Zealand', flag: '🇳🇿' },
  '536': { country: 'Nauru', flag: '🇳🇷' },
  '537': { country: 'Papua New Guinea', flag: '🇵🇬' },
  '539': { country: 'Tonga', flag: '🇹🇴' },
  '540': { country: 'Solomon Islands', flag: '🇸🇧' },
  '541': { country: 'Vanuatu', flag: '🇻🇺' },
  '542': { country: 'Fiji', flag: '🇫🇯' },
  '602': { country: 'Egypt', flag: '🇪🇬' },
  '603': { country: 'Algeria', flag: '🇩🇿' },
  '604': { country: 'Morocco', flag: '🇲🇦' },
  '605': { country: 'Tunisia', flag: '🇹🇳' },
  '606': { country: 'Libya', flag: '🇱🇾' },
  '607': { country: 'Gambia', flag: '🇬🇲' },
  '608': { country: 'Senegal', flag: '🇸🇳' },
  '612': { country: 'Ivory Coast', flag: '🇨🇮' },
  '615': { country: 'Togo', flag: '🇹🇬' },
  '616': { country: 'Benin', flag: '🇧🇯' },
  '620': { country: 'Ghana', flag: '🇬🇭' },
  '621': { country: 'Nigeria', flag: '🇳🇬' },
  '624': { country: 'Cameroon', flag: '🇨🇲' },
  '625': { country: 'Cape Verde', flag: '🇨🇻' },
  '630': { country: 'Congo', flag: '🇨🇬' },
  '631': { country: 'DRC', flag: '🇨🇩' },
  '633': { country: 'Seychelles', flag: '🇸🇨' },
  '634': { country: 'Sudan', flag: '🇸🇩' },
  '635': { country: 'Rwanda', flag: '🇷🇼' },
  '636': { country: 'Ethiopia', flag: '🇪🇹' },
  '637': { country: 'Somalia', flag: '🇸🇴' },
  '639': { country: 'Kenya', flag: '🇰🇪' },
  '640': { country: 'Tanzania', flag: '🇹🇿' },
  '641': { country: 'Uganda', flag: '🇺🇬' },
  '642': { country: 'Burundi', flag: '🇧🇮' },
  '643': { country: 'Mozambique', flag: '🇲🇿' },
  '645': { country: 'Zambia', flag: '🇿🇲' },
  '646': { country: 'Madagascar', flag: '🇲🇬' },
  '648': { country: 'Zimbabwe', flag: '🇿🇼' },
  '649': { country: 'Namibia', flag: '🇳🇦' },
  '650': { country: 'Malawi', flag: '🇲🇼' },
  '651': { country: 'Lesotho', flag: '🇱🇸' },
  '652': { country: 'Botswana', flag: '🇧🇼' },
  '653': { country: 'Eswatini', flag: '🇸🇿' },
  '654': { country: 'Comoros', flag: '🇰🇲' },
  '655': { country: 'South Africa', flag: '🇿🇦' },
  '657': { country: 'Eritrea', flag: '🇪🇷' },
  '659': { country: 'South Sudan', flag: '🇸🇸' },
  '702': { country: 'Belize', flag: '🇧🇿' },
  '704': { country: 'Guatemala', flag: '🇬🇹' },
  '706': { country: 'El Salvador', flag: '🇸🇻' },
  '708': { country: 'Honduras', flag: '🇭🇳' },
  '710': { country: 'Nicaragua', flag: '🇳🇮' },
  '712': { country: 'Costa Rica', flag: '🇨🇷' },
  '714': { country: 'Panama', flag: '🇵🇦' },
  '716': { country: 'Peru', flag: '🇵🇪' },
  '722': { country: 'Argentina', flag: '🇦🇷' },
  '724': { country: 'Brazil', flag: '🇧🇷' },
  '730': { country: 'Chile', flag: '🇨🇱' },
  '732': { country: 'Colombia', flag: '🇨🇴' },
  '734': { country: 'Venezuela', flag: '🇻🇪' },
  '736': { country: 'Bolivia', flag: '🇧🇴' },
  '738': { country: 'Guyana', flag: '🇬🇾' },
  '740': { country: 'Ecuador', flag: '🇪🇨' },
  '744': { country: 'Paraguay', flag: '🇵🇾' },
  '746': { country: 'Suriname', flag: '🇸🇷' },
  '748': { country: 'Uruguay', flag: '🇺🇾' },
};

// ---------------------------------------------------------------------------
// Region presets — expected MCCs per geographic cluster
// Clients select a region and the agent flags any MCC outside the expected set
// ---------------------------------------------------------------------------
const REGION_PRESETS = {
  GR: {
    name: 'Greece',
    flag: '🇬🇷',
    expectedMCC: ['202'],
    bbox: { latMin: 34.8, latMax: 41.8, lngMin: 19.4, lngMax: 29.6 },
  },
  IL: {
    name: 'Israel',
    flag: '🇮🇱',
    expectedMCC: ['425'],
    // Bounding box (rough) for location-based filtering
    bbox: { latMin: 29.4, latMax: 33.4, lngMin: 34.2, lngMax: 35.9 },
  },
  US: {
    name: 'United States',
    flag: '🇺🇸',
    expectedMCC: ['310', '311', '312', '313', '314', '315', '316'],
    bbox: { latMin: 24.5, latMax: 49.4, lngMin: -125.0, lngMax: -66.9 },
  },
  UK: {
    name: 'United Kingdom',
    flag: '🇬🇧',
    expectedMCC: ['234', '235'],
    bbox: { latMin: 49.9, latMax: 60.9, lngMin: -8.2, lngMax: 1.8 },
  },
  DE: {
    name: 'Germany',
    flag: '🇩🇪',
    expectedMCC: ['262'],
    bbox: { latMin: 47.3, latMax: 55.1, lngMin: 5.9, lngMax: 15.0 },
  },
  FR: {
    name: 'France',
    flag: '🇫🇷',
    expectedMCC: ['208'],
    bbox: { latMin: 41.3, latMax: 51.1, lngMin: -5.1, lngMax: 9.6 },
  },
  SA: {
    name: 'Saudi Arabia',
    flag: '🇸🇦',
    expectedMCC: ['420'],
    bbox: { latMin: 16.4, latMax: 32.2, lngMin: 34.6, lngMax: 55.7 },
  },
  AE: {
    name: 'UAE',
    flag: '🇦🇪',
    expectedMCC: ['424'],
    bbox: { latMin: 22.6, latMax: 26.1, lngMin: 51.6, lngMax: 56.4 },
  },
  GLOBAL: {
    name: 'Global (all regions)',
    flag: '🌍',
    expectedMCC: null, // No filtering
    bbox: null,
  },
};

// ---------------------------------------------------------------------------
// Licensed bands per operator per country (MCC-MNC → allowed LTE/NR bands)
// If a device reports PLMN 425-01 but is on a band NOT in this list → anomaly
//
// Sources: Israeli Ministry of Communications spectrum allocations,
//          3GPP TS 36.101 (E-UTRA band definitions), 3GPP TS 38.101 (NR bands)
//
// LTE Band → EARFCN range (downlink):
//   Band 1  (2100 MHz): EARFCN 0–599
//   Band 3  (1800 MHz): EARFCN 1200–1949
//   Band 7  (2600 MHz): EARFCN 2750–3449
//   Band 8  (900 MHz):  EARFCN 3450–3799
//   Band 20 (800 MHz):  EARFCN 6150–6449
//   Band 28 (700 MHz):  EARFCN 9210–9659
//   Band 40 (2300 MHz TDD): EARFCN 38650–39649
//   Band 41 (2500 MHz TDD): EARFCN 39650–41589
// NR Band → NR-ARFCN ranges (not used in EARFCN field, detected via tech=NR):
//   n78 (3500 MHz): NR-ARFCN 620000–653333
// ---------------------------------------------------------------------------
const OPERATOR_LICENSED_BANDS = {
  // Israel — PLMN key format: "MCC-MNC"
  '425-01': { // Partner (Orange)
    name: 'Partner',
    country: 'IL',
    bands: [1, 3, 7, 8, 20, 28, 40, 41],    // LTE bands
    nrBands: [1, 3, 7, 8, 28, 41, 78],       // 5G NR bands (n78 primary, DSS on n1/n3/n7/n8/n28, TDD n41)
    // Valid EARFCN ranges for this operator (union of all licensed LTE bands)
    earfcnRanges: [[0, 599], [1200, 1949], [2750, 3449], [3450, 3799], [6150, 6449], [8040, 9659], [38650, 41589]],
    // Valid NR-ARFCN ranges (5G NR downlink)
    nrArfcnRanges: [
      [422000, 434000],  // n1:  2110-2170 MHz (DSS)
      [361000, 376000],  // n3:  1805-1880 MHz (DSS)
      [524000, 538000],  // n7:  2620-2690 MHz (DSS)
      [185000, 192000],  // n8:  925-960 MHz (DSS)
      [151600, 160600],  // n28: 758-803 MHz (DSS)
      [499200, 537999],  // n41: 2496-2690 MHz TDD
      [620000, 653333],  // n78: 3300-3800 MHz TDD (primary 5G)
    ],
  },
  '425-02': { // Cellcom
    name: 'Cellcom',
    country: 'IL',
    bands: [1, 3, 7, 8, 20, 28, 40, 41],
    nrBands: [1, 3, 7, 8, 28, 41, 78],
    earfcnRanges: [[0, 599], [1200, 1949], [2750, 3449], [3450, 3799], [6150, 6449], [8040, 9659], [38650, 41589]],
    nrArfcnRanges: [
      [422000, 434000], [361000, 376000], [524000, 538000],
      [185000, 192000], [151600, 160600], [499200, 537999], [620000, 653333],
    ],
  },
  '425-03': { // Pelephone
    name: 'Pelephone',
    country: 'IL',
    bands: [1, 3, 7, 8, 20, 28, 40, 41],
    nrBands: [1, 3, 7, 8, 28, 41, 78],
    earfcnRanges: [[0, 599], [1200, 1949], [2750, 3449], [3450, 3799], [6150, 6449], [8040, 9659], [38650, 41589]],
    nrArfcnRanges: [
      [422000, 434000], [361000, 376000], [524000, 538000],
      [185000, 192000], [151600, 160600], [499200, 537999], [620000, 653333],
    ],
  },
  '425-07': { // Hot Mobile (network sharing with Partner)
    name: 'Hot Mobile',
    country: 'IL',
    bands: [1, 3, 7, 28, 40, 41],             // B1(2100), B3(1800), B7(2600), B28(700), B40/B41(TDD)
    nrBands: [1, 3, 7, 28, 41, 78],           // n28(700 DSS), n78(3500 primary)
    earfcnRanges: [[0, 599], [1200, 1949], [2750, 3449], [8040, 9659], [38650, 41589]],
    nrArfcnRanges: [
      [422000, 434000],                        // n1: 2100 MHz (DSS)
      [361000, 376000], [524000, 538000],      // n3, n7 (DSS)
      [151600, 160600],                        // n28: 700 MHz (DSS)
      [499200, 537999], [620000, 653333],      // n41, n78
    ],
  },
  '425-08': { // Golan Telecom (MVNO on Cellcom)
    name: 'Golan Telecom',
    country: 'IL',
    bands: [1, 3, 7, 8, 20, 28, 40, 41],     // Same as Cellcom (host network)
    nrBands: [1, 3, 7, 8, 28, 41, 78],
    earfcnRanges: [[0, 599], [1200, 1949], [2750, 3449], [3450, 3799], [6150, 6449], [8040, 9659], [38650, 41589]],
    nrArfcnRanges: [
      [422000, 434000], [361000, 376000], [524000, 538000],
      [185000, 192000], [151600, 160600], [499200, 537999], [620000, 653333],
    ],
  },
  '425-12': { // Partner (additional MNC)
    name: 'Partner',
    country: 'IL',
    bands: [1, 3, 7, 8, 20, 28, 40, 41],
    nrBands: [1, 3, 7, 8, 28, 41, 78],
    earfcnRanges: [[0, 599], [1200, 1949], [2750, 3449], [3450, 3799], [6150, 6449], [8040, 9659], [38650, 41589]],
    nrArfcnRanges: [
      [422000, 434000], [361000, 376000], [524000, 538000],
      [185000, 192000], [151600, 160600], [499200, 537999], [620000, 653333],
    ],
  },
};

// GSM (2G) licensed bands per country — used for NETWORK_DOWNGRADE validation
const COUNTRY_GSM_BANDS = {
  IL: [900, 1800],  // GSM-900 and DCS-1800
};

const config = {
  clickhouse: {
    host: process.env.CLICKHOUSE_HOST || '',
    port: parseInt(process.env.CLICKHOUSE_PORT, 10) || 8443,
    database: process.env.CLICKHOUSE_DB || 'default',
    username: process.env.CLICKHOUSE_USER || '',
    password: process.env.CLICKHOUSE_PASSWORD || '',
  },

  agent: {
    pollIntervalMs: parseInt(process.env.POLL_INTERVAL_MS, 10) || 60000,
    batchSize: parseInt(process.env.BATCH_SIZE, 10) || 50000,
    suspicionThreshold: parseFloat(process.env.SUSPICION_THRESHOLD) || 0.6,
    region: process.env.AGENT_REGION || 'GR',
    // Deployment mode: 'SDK' (mobile SDK data) or 'RSU' (Remote Sensing Units / modems)
    // RSU mode shows device icons on map and treats all measurements as modem data
    deploymentMode: (process.env.DEPLOYMENT_MODE || 'SDK').toUpperCase(),

    // RSU real-time monitoring
    rsuPollIntervalMs: parseInt(process.env.RSU_POLL_INTERVAL_MS, 10) || 5000,
    rsuBootstrapMinutes: 5,   // On startup, load this many minutes of history to build state
    rsuAlertCooldowns: {      // Per-device cooldown per rule (seconds). 0 = state machine handles it.
      RSU_MCC_MISMATCH: 0,       // state flag: foreignMCCActive
      RSU_NEW_CELL: 300,          // 5min — cell can appear/disappear
      RSU_CELL_CHANGE: 60,        // 1min — handover ping-pong
      RSU_TAC_JUMP: 120,          // 2min — rapid oscillation
      RSU_SIGNAL_DEGRADATION: 0,  // state flag: signalDegradationActive
      RSU_CELLULAR_JAMMING: 0,    // state flag: jammingActive
      RSU_RSRQ_DEGRADATION: 0,    // state flag: rsrqDegradationActive
      RSU_GPS_JAMMING: 0,         // state flag: gpsJammingActive
      RSU_GPS_SPOOFING: 0,        // state flag: gpsSpoofingActive
      RSU_NETWORK_DOWNGRADE: 120, // 2min
      RSU_UNKNOWN_SITE: 0,        // tracked per-eNB in knownUnknownENBs
      RSU_DEVICE_OFFLINE: 60,
    },
    rsuOfflineThresholdMs: 30000, // Device considered offline after 30s of no data
  },

  severity: {
    CRITICAL: 'CRITICAL',
    HIGH: 'HIGH',
    MEDIUM: 'MEDIUM',
    LOW: 'LOW',
  },

  anthropic: {
    apiKey: process.env.ANTHROPIC_API_KEY || '',
    model: 'claude-sonnet-4-20250514',
    maxEventsPerBatch: 20,
    confirmationThreshold: 0.7,
  },

  server: {
    port: parseInt(process.env.PORT, 10) || parseInt(process.env.SERVER_PORT, 10) || 3000,
  },

  alert: {
    webhookUrl: process.env.ALERT_WEBHOOK_URL || '',
  },

  mcc: MCC_REGISTRY,
  regions: REGION_PRESETS,
  operatorBands: OPERATOR_LICENSED_BANDS,
  countryGsmBands: COUNTRY_GSM_BANDS,
};

/**
 * Extract MCC (first 3 digits) from a PLMN string.
 */
config.extractMCC = function (plmn) {
  if (!plmn) return null;
  const s = String(plmn).replace(/\D/g, '');
  return s.length >= 3 ? s.substring(0, 3) : null;
};

/**
 * Get the current region config.
 */
config.getRegion = function () {
  return REGION_PRESETS[config.agent.region] || REGION_PRESETS.GLOBAL;
};

/**
 * Check if an MCC is expected for the current region.
 */
config.isMCCExpected = function (mcc) {
  const region = config.getRegion();
  if (!region.expectedMCC) return true; // GLOBAL mode — everything allowed
  return region.expectedMCC.includes(mcc);
};

/**
 * Look up country info for an MCC.
 */
config.getMCCInfo = function (mcc) {
  return MCC_REGISTRY[mcc] || { country: `Unknown (${mcc})`, flag: '❓' };
};

/**
 * Get licensed band info for an operator by PLMN (e.g. "425-01" or "42501").
 * Returns null if operator is not in the database.
 */
config.getOperatorBands = function (plmn) {
  if (!plmn) return null;
  // Normalize: "42501" → "425-01", "425-01" → "425-01"
  let key = String(plmn).replace(/\D/g, '');
  if (key.length >= 5) {
    key = key.substring(0, 3) + '-' + key.substring(3).replace(/^0+/, '') // "42501" → "425-1"
      .padStart(2, '0'); // "425-1" → "425-01"
  }
  return OPERATOR_LICENSED_BANDS[key] || null;
};

/**
 * Check if an EARFCN is valid for an operator.
 * Returns { valid: boolean, band: number|null, operatorName: string }
 */
config.isEarfcnValidForOperator = function (earfcn, plmn) {
  const opInfo = config.getOperatorBands(plmn);
  if (!opInfo) return { valid: true, unknown: true }; // Unknown operator — can't check

  const e = Number(earfcn);
  if (isNaN(e)) return { valid: true, unknown: true };

  // Detect if this is an NR-ARFCN (5G) vs LTE EARFCN
  const isNr = e > 100000;

  if (isNr) {
    // Check against NR-ARFCN ranges
    const nrRanges = opInfo.nrArfcnRanges || [];
    for (const [min, max] of nrRanges) {
      if (e >= min && e <= max) {
        return { valid: true, operatorName: opInfo.name, band: nrArfcnToBand(e), isNr: true };
      }
    }
    return {
      valid: false,
      operatorName: opInfo.name,
      allowedBands: opInfo.nrBands || [],
      earfcn: e,
      detectedBand: nrArfcnToBand(e),
      isNr: true,
    };
  }

  // LTE EARFCN check
  for (const [min, max] of opInfo.earfcnRanges) {
    if (e >= min && e <= max) {
      return { valid: true, operatorName: opInfo.name, band: earfcnToBand(e), isNr: false };
    }
  }

  return {
    valid: false,
    operatorName: opInfo.name,
    allowedBands: opInfo.bands,
    earfcn: e,
    detectedBand: earfcnToBand(e),
    isNr: false,
  };
};

/**
 * Check if an LTE band number is licensed for an operator.
 */
config.isBandValidForOperator = function (bandNum, plmn) {
  const opInfo = config.getOperatorBands(plmn);
  if (!opInfo) return { valid: true, unknown: true };
  const b = Number(bandNum);
  if (isNaN(b) || b === 0) return { valid: true, unknown: true };

  // Check both LTE bands and NR bands (deduplicated — DSS bands appear in both)
  const allBands = [...new Set((opInfo.bands || []).concat(opInfo.nrBands || []))];
  return {
    valid: allBands.includes(b),
    operatorName: opInfo.name,
    allowedBands: allBands,
    band: b,
    isNr: (opInfo.nrBands || []).includes(b),
  };
};

/**
 * Map EARFCN to LTE band number.
 */
function earfcnToBand(earfcn) {
  const e = Number(earfcn);
  if (e >= 0 && e <= 599) return 1;
  if (e >= 600 && e <= 1199) return 2;
  if (e >= 1200 && e <= 1949) return 3;
  if (e >= 1950 && e <= 2399) return 4;
  if (e >= 2400 && e <= 2649) return 5;
  if (e >= 2750 && e <= 3449) return 7;
  if (e >= 3450 && e <= 3799) return 8;
  if (e >= 4150 && e <= 4749) return 10;
  if (e >= 5010 && e <= 5179) return 12;
  if (e >= 5180 && e <= 5279) return 13;
  if (e >= 5280 && e <= 5379) return 14;
  if (e >= 5730 && e <= 5849) return 17;
  if (e >= 5850 && e <= 5999) return 18;
  if (e >= 6000 && e <= 6149) return 19;
  if (e >= 6150 && e <= 6449) return 20;
  if (e >= 6450 && e <= 6599) return 21;
  if (e >= 6600 && e <= 7399) return 22;
  if (e >= 7500 && e <= 7699) return 25;
  if (e >= 7700 && e <= 8039) return 26;
  if (e >= 8040 && e <= 8689) return 28;
  if (e >= 8690 && e <= 9039) return 29;
  if (e >= 9040 && e <= 9209) return 30;
  if (e >= 9210 && e <= 9659) return 28; // Band 28 extended
  if (e >= 9770 && e <= 10109) return 31;
  if (e >= 36000 && e <= 36199) return 33;
  if (e >= 36200 && e <= 36349) return 34;
  if (e >= 36350 && e <= 36949) return 35;
  if (e >= 36950 && e <= 37549) return 36;
  if (e >= 37550 && e <= 37749) return 37;
  if (e >= 37750 && e <= 38249) return 38;
  if (e >= 38250 && e <= 38649) return 39;
  if (e >= 38650 && e <= 39649) return 40;
  if (e >= 39650 && e <= 41589) return 41;
  if (e >= 41590 && e <= 43589) return 42;
  if (e >= 43590 && e <= 45589) return 43;
  if (e >= 45590 && e <= 46589) return 44;
  if (e >= 46590 && e <= 46789) return 45;
  if (e >= 46790 && e <= 54539) return 46;
  if (e >= 54540 && e <= 55239) return 47;
  if (e >= 55240 && e <= 56739) return 48;
  return null; // Unknown
}
config.earfcnToBand = earfcnToBand;

/**
 * Map NR-ARFCN to 5G NR band number.
 * Per 3GPP TS 38.101-1 (FR1: sub-6GHz) and TS 38.101-2 (FR2: mmWave).
 *
 * NR-ARFCNs are in a different numeric space than LTE EARFCNs.
 * The Android CellIdentityNr.getNrarfcn() returns values typically >100000.
 * Heuristic: if value > 100000, treat as NR-ARFCN; otherwise treat as LTE EARFCN.
 */
function nrArfcnToBand(nrarfcn) {
  const n = Number(nrarfcn);
  // FR1 bands (sub-6GHz) — downlink NR-ARFCN ranges
  if (n >= 422000 && n <= 434000) return 1;   // n1:  2110-2170 MHz FDD
  if (n >= 386000 && n <= 398000) return 2;   // n2:  1930-1990 MHz FDD (check before n25)
  if (n >= 361000 && n <= 376000) return 3;   // n3:  1805-1880 MHz FDD
  if (n >= 173800 && n <= 178800) return 5;   // n5:  869-894 MHz FDD
  if (n >= 524000 && n <= 538000) return 7;   // n7:  2620-2690 MHz FDD
  if (n >= 185000 && n <= 192000) return 8;   // n8:  925-960 MHz FDD
  if (n >= 145800 && n <= 149200) return 12;  // n12: 729-746 MHz FDD
  if (n >= 149200 && n <= 151200) return 14;  // n14: 758-768 MHz FDD
  if (n >= 172000 && n <= 175000) return 18;  // n18: 860-875 MHz FDD
  if (n >= 158200 && n <= 164200) return 20;  // n20: 791-821 MHz FDD
  if (n >= 398001 && n <= 399000) return 25;  // n25: 1990-1995 MHz FDD (n25 range NOT covered by n2)
  if (n >= 171800 && n <= 178800) return 26;  // n26: 859-894 MHz FDD
  if (n >= 151600 && n <= 160600) return 28;  // n28: 758-803 MHz FDD
  if (n >= 143400 && n <= 145600) return 29;  // n29: 717-728 MHz SDL
  if (n >= 285400 && n <= 286400) return 30;  // n30: 2350-2360 MHz FDD
  if (n >= 514000 && n <= 524000) return 38;  // n38: 2570-2620 MHz TDD
  if (n >= 376000 && n <= 384000) return 39;  // n39: 1880-1920 MHz TDD
  if (n >= 460000 && n <= 480000) return 40;  // n40: 2300-2400 MHz TDD
  if (n >= 499200 && n <= 537999) return 41;  // n41: 2496-2690 MHz TDD
  if (n >= 743334 && n <= 795000) return 46;  // n46: 5150-5925 MHz TDD (unlicensed)
  if (n >= 790334 && n <= 795000) return 47;  // n47: 5855-5925 MHz TDD
  if (n >= 636667 && n <= 646666) return 48;  // n48: 3550-3700 MHz TDD (CBRS)
  if (n >= 286400 && n <= 303400) return 50;  // n50: 1432-1517 MHz SDL
  if (n >= 285400 && n <= 286400) return 51;  // n51: 1427-1432 MHz SDL
  if (n >= 422000 && n <= 440000) return 66;  // n66: 2110-2200 MHz FDD
  if (n >= 620000 && n <= 653333) return 78;  // n78: 3300-3800 MHz TDD (check before n77 — n78 is subset)
  if (n >= 653334 && n <= 680000) return 77;  // n77: 3800-4200 MHz TDD (n77 range NOT covered by n78)
  if (n >= 693334 && n <= 733333) return 79;  // n79: 4400-5000 MHz TDD
  if (n >= 123400 && n <= 130400) return 71;  // n71: 617-652 MHz FDD
  // n90 is same frequency as n41 — Android reports as n41, so n90 mapping not needed

  // FR2 bands (mmWave)
  if (n >= 2054166 && n <= 2104165) return 257; // n257: 26.5-29.5 GHz
  if (n >= 2016667 && n <= 2070832) return 258; // n258: 24.25-27.5 GHz
  if (n >= 2229166 && n <= 2279165) return 260; // n260: 37-40 GHz
  if (n >= 2070833 && n <= 2084999) return 261; // n261: 27.5-28.35 GHz

  return null; // Unknown
}
config.nrArfcnToBand = nrArfcnToBand;

/**
 * Detect if an ARFCN value is an NR-ARFCN (5G) vs LTE EARFCN.
 * NR-ARFCNs from Android's CellIdentityNr.getNrarfcn() are typically >100000.
 * LTE EARFCNs max out at ~56739 (Band 48).
 */
config.isNrArfcn = function (arfcn) {
  return Number(arfcn) > 100000;
};

/**
 * Returns true only when ClickHouse host + username are configured.
 */
config.isConfigured = function () {
  return !!(config.clickhouse.host && config.clickhouse.username);
};

// Export earfcnToBand for use in rules.js band-aware distance thresholds
config.earfcnToBandExported = earfcnToBand;

module.exports = config;
