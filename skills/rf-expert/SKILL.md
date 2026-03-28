---
name: rf-expert
description: |
  RF threat detection and cellular network analysis expert. 3GPP standards (LTE/NR/GSM), EARFCN-to-band conversion, signal analysis (RSRP/RSRQ/SINR/TA), propagation physics, air-interface cyber threats (IMSI catchers, GPS spoofing, rogue BTS, downgrade attacks).
  MANDATORY TRIGGERS: cellular RF analysis, telecom security, cell tower anomaly detection, IMSI catcher detection, GPS spoofing analysis, TA/RSRP physics validation, rogue BTS identification, 3GPP RF work, detection rules for cellular anomalies, Android CellInfo API data, ClickHouse RF measurements, SOC dashboards. If user mentions PCI, eNB, EARFCN, PLMN, TA, RSRP, MCC/MNC, cell towers, signal strength, or downgrades — use this skill.
---

# RF Expert — Cellular Network Security & Signal Analysis

You are an RF and cellular network security expert with deep knowledge of 3GPP standards,
radio propagation physics, and air-interface cyber threats. Your analysis must be grounded
in physics and protocol specifications — never guess when you can calculate.

## Core Principle: Physics First

Every RF detection rule must be validated against physical reality. Before flagging an anomaly:

1. **Calculate, don't assume.** Convert EARFCN → band → frequency → expected propagation.
2. **Cross-validate with multiple indicators.** A single anomalous reading (e.g., TA=0) is not
   sufficient — correlate with RSRP, RSRQ, distance, and RAT context.
3. **Understand measurement limitations.** TA has 78.12m resolution. RSRP varies ±6dB in urban
   fading. GPS has ~5m accuracy outdoors but can be 50m+ indoors.
4. **Consider the deployment context.** Band 20 (800 MHz) covers 15km+ rural; Band 7 (2600 MHz)
   covers 3-5km urban. Same TA value means different things on different bands.

## 1. Frequency & Band Reference

### EARFCN → Band → Frequency (LTE)

Per 3GPP TS 36.101 Table 5.7.3-1, the downlink frequency is:

```
F_DL = F_DL_low + 0.1 * (EARFCN - N_offs_DL)   [MHz]
```

Read `references/lte_bands.md` for the complete EARFCN range table with all FDD and TDD bands,
including F_DL_low and N_offs_DL values.

**Quick reference for common Israeli/EMEA bands:**

| Band | DL Freq (MHz)  | EARFCN Range   | Typical Range | Common Use           |
|------|---------------|----------------|---------------|----------------------|
| B1   | 2110-2170     | 0-599          | 5-8 km        | Primary LTE (urban)  |
| B3   | 1805-1880     | 1200-1949      | 5-10 km       | Primary LTE (urban)  |
| B7   | 2620-2690     | 2750-3449      | 2-5 km        | Capacity (dense urban)|
| B8   | 925-960       | 3450-3799      | 10-15 km      | Coverage (rural/indoor)|
| B20  | 791-862       | 6150-6449      | 10-20 km      | Coverage (rural)     |
| B28  | 758-823       | 8040-8689      | 10-15 km      | Coverage/FirstNet    |
| B38  | 2570-2620     | 37750-38249    | 2-4 km        | TDD capacity         |
| B40  | 2300-2400     | 38650-39649    | 3-5 km        | TDD capacity         |
| B41  | 2496-2690     | 39650-41589    | 2-5 km        | TDD capacity         |

### NR-ARFCN → Band (5G NR)

Per 3GPP TS 38.101-1/2. NR-ARFCNs are in a separate numeric space (typically >100000 from
Android's `CellIdentityNr.getNrarfcn()`).

Read `references/nr_bands.md` for the complete NR-ARFCN table.

**Heuristic:** If ARFCN value > 100,000 → treat as NR-ARFCN, not LTE EARFCN.

### GSM ARFCN → Band

Per 3GPP TS 45.005. GSM ARFCNs are in range 0-1023.

| Band     | ARFCN Range | DL Freq (MHz) |
|----------|-------------|---------------|
| GSM 900  | 1-124       | 935-960       |
| GSM 1800 | 512-885     | 1805-1880     |
| GSM 1900 | 512-810     | 1930-1990     |
| GSM 850  | 128-251     | 869-894       |

## 2. Signal Propagation Physics

### Timing Advance (TA) — 3GPP TS 36.321

TA is a discrete value commanding the UE to advance its uplink transmission timing to
compensate for propagation delay.

```
Distance = TA × 78.12 meters    (LTE, each TA step = 16 × Ts = 16/(15000×2048) seconds)
```

**Critical nuances:**
- **TA=0 does NOT always mean 0 meters.** It means 0-78m. The UE could be anywhere in that
  first quantization bin. In practice, TA=0 is reported for devices up to ~100m from the tower.
- **TA=0 is common and legitimate** for devices near a cell site. A single TA=0 reading is
  not anomalous — even at 2-3km GPS distance, due to indoor/multipath effects.
- **TA=0 cluster is suspicious** when MULTIPLE unique devices report TA=0 on the SAME cell
  in a short time window — this suggests a portable BTS (IMSI catcher) where all UEs are
  physically adjacent to the rogue transmitter.
- **Maximum TA value:** 1282 (LTE) → ~100 km theoretical max range.

**NR Timing Advance:** Variable resolution depending on subcarrier spacing (SCS):
- SCS 15 kHz: ~4.89m per TA step
- SCS 30 kHz: ~2.44m per TA step
- SCS 120 kHz: ~0.61m per TA step

### RSRP — Reference Signal Received Power (3GPP TS 36.214)

RSRP measures the power of LTE Reference Signals across the channel bandwidth.

| RSRP (dBm) | Signal Quality | Typical Distance (Urban B3) |
|-------------|---------------|----------------------------|
| > -80       | Excellent     | < 0.5 km (near tower)      |
| -80 to -90  | Good          | 0.5-2 km                   |
| -90 to -100 | Fair          | 2-5 km                     |
| -100 to -110| Poor          | 5-10 km                    |
| -110 to -120| Very poor     | Cell edge, 8-15 km         |
| < -120      | Near unusable | Beyond normal coverage     |

**These are rough guides** — actual distance depends on frequency, environment (urban/rural/indoor),
antenna height, TX power, and terrain. Use the Okumura-Hata or COST-231 model for estimates:

```
Path Loss (dB) = TX_power_dBm - RSRP
Distance (km) = 10^((PathLoss - 137.4) / 35.2)    [simplified urban 1800MHz]
```

For frequency-aware estimation, read `references/propagation_models.md`.

### RSRQ — Reference Signal Received Quality (3GPP TS 36.214)

```
RSRQ = N × RSRP / RSSI    (in linear scale)
```

Where N = number of resource blocks. RSRQ captures interference + noise.

| RSRQ (dB)  | Quality    | Interpretation                    |
|------------|-----------|-----------------------------------|
| > -10      | Good      | Low interference                  |
| -10 to -15 | Normal    | Typical urban                     |
| -15 to -20 | Poor      | High interference or cell edge    |
| < -20      | Very poor | Severe interference — investigate |

### SINR / SNR

Signal-to-Interference-plus-Noise Ratio. Not standardized by 3GPP but reported by most chipsets.

| SINR (dB) | Quality     |
|-----------|------------|
| > 20      | Excellent  |
| 13-20     | Good       |
| 0-13      | Fair       |
| < 0       | Poor       |

**Anomaly indicator:** SINR < -3 dB with RSRP > -90 dBm suggests strong co-channel interference,
possibly from a rogue transmitter on the same frequency.

## 3. Cell Identity Architecture

### LTE Cell Identifiers (3GPP TS 36.331)

```
ECGI = MCC + MNC + ECI                    (globally unique cell ID)
ECI  = eNB_ID (20 bits) + Cell_ID (8 bits)  (28-bit E-UTRAN Cell Identifier)
eNB_ID = ECI >> 8                          (uniquely identifies a base station)
Cell_ID = ECI & 0xFF                       (sector within the eNB, 0-255)
PCI  = Physical Cell ID (0-503)            (NOT globally unique — reused every ~504 cells)
TAC  = Tracking Area Code (16-bit)         (groups cells into tracking areas)
```

**Key insight for threat detection:**
- **eNB ID is the tower identifier.** When you have eNB, you know WHICH tower. ECI adds the
  sector (cell within the tower). Not having ECI is normal — many Android implementations
  report eNB but not the full ECI.
- **PCI is NOT a unique identifier.** There are only 504 LTE PCIs. They are reused across
  the network. Never use PCI alone for geographic lookups — it will produce false positives.
  PCI is useful for: (a) cell handover decisions, (b) combined with PLMN+location for
  disambiguation, (c) detecting PCI conflicts (two cells with same PCI visible to one UE).
- **eNB + PLMN uniquely identifies a tower** for a given operator. This is the reliable
  lookup key for site databases.

### Android CellInfo API Mapping

| Android API                              | ClickHouse Column          | Notes                    |
|------------------------------------------|---------------------------|--------------------------|
| CellIdentityLte.getCi()                  | cell_eci                  | 28-bit ECI               |
| CellIdentityLte.getPci()                 | cell_pci                  | 0-503                    |
| Top 20 bits of ECI                       | cell_enb                  | eNB/site ID              |
| CellIdentityLte.getTac()                 | cell_tac                  | 16-bit TAC               |
| CellIdentityLte.getEarfcn()              | band_downlinkEarfcn       | DL EARFCN                |
| CellSignalStrengthLte.getTimingAdvance() | signal_timingAdvance      | 0-1282                   |
| CellSignalStrengthLte.getRsrp()          | signal_rsrp               | -140 to -43 dBm          |
| CellSignalStrengthLte.getRsrq()          | signal_rsrq               | -34 to 3 dB              |
| CellSignalStrengthLte.getRssnr()         | signal_snr                | -20 to 30 dB             |
| CellInfo.getCellConnectionStatus()       | connectionStatus          | REGISTERED/NONE          |
| Network registration type                | tech                      | LTE/NR/WCDMA/GSM         |
| Registered PLMN                          | network_PLMN              | e.g., "42501"            |

## 4. Air-Interface Cyber Threats

### Threat 1: IMSI Catcher / Rogue Base Station (Stingray)

**What it is:** A portable device that impersonates a legitimate cell tower to intercept
communications, track location, or downgrade encryption.

**Detection indicators (must correlate 2+ indicators):**
- TA=0 cluster: ≥2 unique devices report TA=0 on same cell in same time window
- Unknown eNB: eNB ID not in operator's site database
- PCI conflict: Same PCI as a known cell but at a different location
- Forced 2G downgrade: Devices suddenly switch from LTE to GSM/EDGE
- Abnormally strong signal: RSRP > -70 dBm at a location where other cells show -90 to -110
- MCC 001: Test network MCC used by IMSI catchers to avoid operator detection
- Missing features: No VoLTE, limited bearer setup, short-lived cell

**False positive avoidance:**
- A single TA=0 is not suspicious (device could be near the real tower)
- A new eNB could be a legitimate new site installation
- 2G coverage exists legitimately in rural/indoor areas
- Temporary cells (COWs) appear during events — check for known temporary deployments

### Threat 2: GPS Spoofing

**What it is:** Device reports fake GPS coordinates while physically at a different location.
Detected by comparing GPS position against RF-based position estimates.

**Detection logic — physics validation required:**

```
GPS says device is at location A
Cell tower (identified by eNB) is at known location B
GPS_distance = haversine(A, B)
TA_distance = TA × 78.12m
RSRP_distance = path_loss_model(RSRP, frequency, environment)
```

**Flag GPS spoofing ONLY when RF physics contradict GPS:**

| Condition                                          | Verdict       |
|---------------------------------------------------|---------------|
| GPS 3km, TA=0, RSRP=-114 (weak)                  | NORMAL        |
| GPS 20km, TA=0, RSRP=-80 (strong)                | GPS SPOOFED   |
| GPS 15km, TA=2 (~156m), RSRP=-85                 | GPS SPOOFED   |
| GPS 1km, TA=50 (~3.9km), RSRP=-100               | GPS SPOOFED   |
| GPS >15km from connected cell                     | GPS SPOOFED*  |

*At >15km, even rural LTE is at absolute cell edge. A connected cell 15+km from GPS
is almost certainly a GPS spoof regardless of TA/RSRP.

**Why TA=0 + weak RSRP + moderate distance is NOT spoofing:**
TA has 78m resolution. TA=0 means the device is in the first quantization bin (0-78m).
But in practice, some chipsets report TA=0 as a default/fallback when timing measurement
is unreliable (indoor, low SNR). Meanwhile RSRP=-114 is consistent with being 3-5km away
in urban Band 7 (2600 MHz). The two indicators don't contradict the GPS — they're just noisy.

### Threat 3: Network Downgrade Attack

**What it is:** An attacker forces devices to fall back from 4G/5G to 2G (GSM), where
encryption (A5/1) is broken and there's no mutual authentication.

**Detection indicators:**
- Device on GSM/EDGE/GPRS in area with known LTE/NR coverage
- Sudden RAT change from LTE→GSM without coverage gap
- Multiple devices downgrading simultaneously in same area

**Legitimate downgrades:**
- Rural areas with 2G-only coverage
- Indoor environments with no LTE penetration (rare with modern networks)
- VoLTE fallback to CSFB (Circuit Switched Fallback) for voice calls — this is a controlled
  RAT change to 3G, not 2G, and is normal

### Threat 4: Cell Location Mismatch

**What it is:** A measurement from a cell (identified by eNB) at a location that's physically
far from the cell's registered tower position.

**When the cell is at the wrong place:**
- eNB registered at (32.19, 35.25) but measurement at (31.70, 34.30) = 80km → rogue BTS
  spoofing the eNB identity
- eNB at (32.19, 35.25), measurement at (32.22, 35.28) = 3km → normal cell coverage

**Thresholds should be band-aware:**
- Band 7 (2600 MHz): Flag at >5km — max practical range ~5km urban
- Band 3 (1800 MHz): Flag at >8km — typical range ~5-10km
- Band 20 (800 MHz): Flag at >15km — can cover 10-20km rural
- Band 1 (2100 MHz): Flag at >8km — similar to Band 3

### Threat 5: EARFCN/Band Anomaly

**What it is:** A cell broadcasts on a frequency not licensed to the PLMN operator.

**Detection:** Cross-reference the cell's EARFCN with the operator's licensed spectrum.

**Example (Israel):**
- PLMN 425-01 (Partner/Hot Mobile) on Band 7 → legitimate (Partner has B7 license)
- PLMN 425-02 (Cellcom) on Band 1 → suspicious (verify license allocation)
- PLMN 001-01 on any band → test network, always suspicious

Read `references/operator_spectrum.md` for per-country operator frequency allocations.

### Threat 6: RF Fingerprint Anomaly

**What it is:** Signal characteristics that don't match the expected profile for a cell.

**Indicators:**
- Unusually strong RSRP (> -65 dBm): suggests very close transmitter (possible portable BTS)
- RSRP/RSRQ mismatch: Strong RSRP but very poor RSRQ (< -15) suggests jamming/interference
- TA-RSRP distance mismatch: TA says 10km, RSRP says 500m → amplified/relayed signal
- Impossible combinations: LTE with TA > 100km or RSRP > -43 dBm

## 5. PLMN & Operator Reference

### MCC Structure (ITU-T E.212)

```
PLMN = MCC (3 digits) + MNC (2-3 digits)
```

- MCC 001: Test networks (IMSI catchers commonly use this)
- MCC 425: Israel
- MCC 310-316: United States
- MCC 234: United Kingdom
- MCC 262: Germany

### Israel Operators (MCC 425)

| PLMN   | Operator       | Licensed Bands       | Notes                        |
|--------|---------------|---------------------|------------------------------|
| 425-01 | Partner/Orange | B1, B3, B7, B20     | Also Hot Mobile MVNO         |
| 425-02 | Cellcom        | B1, B3, B7, B28     |                              |
| 425-03 | Pelephone      | B1, B3, B7, B40     |                              |
| 425-09 | We4G           | B7, B40             | Wholesale LTE network        |
| 425-12 | Golan Telecom  | Uses Partner infra   | MVNO                         |

For other countries, read `references/operator_spectrum.md`.

## 6. Detection Rule Design Principles

When writing detection rules for RF threats:

1. **Never rely on a single indicator.** Always require correlation of 2+ independent signals.
   - Bad: "TA=0 → IMSI catcher"
   - Good: "TA=0 + 3 unique devices + unknown eNB → IMSI catcher"

2. **Make thresholds band-aware.** A 5km distance that's anomalous on Band 7 is normal on Band 20.

3. **Validate GPS spoofing with RF physics.** Distance from tower alone is NOT sufficient.
   You must show that TA and/or RSRP contradict the GPS distance.

4. **Use eNB, not PCI, for geographic correlation.** PCI (0-503) is reused across the network.
   eNB is the unique tower identifier. If only PCI is available, combine with PLMN + approximate
   location for disambiguation.

5. **Account for measurement noise.** RSRP varies ±6dB due to fading. TA can report 0 as
   default. GPS can be 50m+ off indoors. Build margins into your thresholds.

6. **Calculate the frequency from EARFCN.** Don't just show "EARFCN 2850" — calculate and
   display "B7, 2630.0 MHz (EARFCN 2850)". This makes the analysis immediately interpretable
   by RF engineers.

7. **Log the physics.** When flagging an anomaly, include the TA distance estimate, RSRP
   distance estimate, GPS distance, and the specific contradiction in the alert details.

## 7. Useful Formulas

```
# LTE TA to distance
distance_m = TA * 78.12

# LTE EARFCN to DL frequency
F_DL = F_DL_low + 0.1 * (EARFCN - N_offs_DL)

# Free space path loss
FSPL_dB = 20*log10(d_km) + 20*log10(f_MHz) + 32.44

# Okumura-Hata (urban, 150-1500 MHz, 1-20km)
PL = 69.55 + 26.16*log10(f) - 13.82*log10(h_b) - a(h_m) + (44.9 - 6.55*log10(h_b))*log10(d)
# where a(h_m) = (1.1*log10(f)-0.7)*h_m - (1.56*log10(f)-0.8) for medium city

# COST-231 Hata (1500-2000 MHz extension)
PL = 46.3 + 33.9*log10(f) - 13.82*log10(h_b) - a(h_m) + (44.9 - 6.55*log10(h_b))*log10(d) + C_m
# C_m = 0 dB (medium city), 3 dB (metropolitan)

# eNB from ECI
eNB_ID = ECI >> 8        # top 20 bits
sector = ECI & 0xFF      # bottom 8 bits

# RSRP to estimated distance (simplified urban 1800 MHz, TX=46 dBm)
PL = 46 - RSRP
d_km = 10^((PL - 137.4) / 35.2)
```
