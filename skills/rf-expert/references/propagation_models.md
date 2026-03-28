# RF Propagation Models for Distance Estimation

## 1. Free Space Path Loss (FSPL)

The theoretical minimum path loss — no obstacles, direct line of sight.

```
FSPL (dB) = 20×log10(d_km) + 20×log10(f_MHz) + 32.44
```

**Inverse (distance from RSRP):**
```
PL = TX_power_dBm - RSRP
d_km = 10^((PL - 20×log10(f_MHz) - 32.44) / 20)
```

Only accurate for satellite links, rooftop-to-rooftop LOS, or very short distances.
Real-world urban environments have 20-40 dB additional loss.

## 2. Okumura-Hata Model

The workhorse model for urban/suburban macro-cell planning.

**Valid range:** 150-1500 MHz, 1-20 km, BS height 30-200m, MS height 1-10m.

```
PL (dB) = 69.55 + 26.16×log10(f) - 13.82×log10(h_b) - a(h_m) + [44.9 - 6.55×log10(h_b)]×log10(d)
```

Where:
- f = frequency in MHz
- h_b = base station antenna height in meters (typical: 25-50m)
- h_m = mobile antenna height in meters (typical: 1.5m)
- d = distance in km
- a(h_m) = mobile antenna correction factor

**Correction factors:**

Medium/small city:
```
a(h_m) = (1.1×log10(f) - 0.7)×h_m - (1.56×log10(f) - 0.8)
```

Large city (f ≤ 300 MHz):
```
a(h_m) = 8.29×(log10(1.54×h_m))² - 1.1
```

Large city (f > 300 MHz):
```
a(h_m) = 3.2×(log10(11.75×h_m))² - 4.97
```

**Environment corrections:**
- Suburban: PL_suburban = PL_urban - 2×(log10(f/28))² - 5.4
- Open/rural: PL_rural = PL_urban - 4.78×(log10(f))² + 18.33×log10(f) - 40.94

## 3. COST-231 Hata Model

Extends Okumura-Hata to 1500-2000 MHz. Used for Band 1, Band 3, Band 4.

```
PL (dB) = 46.3 + 33.9×log10(f) - 13.82×log10(h_b) - a(h_m) + [44.9 - 6.55×log10(h_b)]×log10(d) + C_m
```

Where C_m = 0 dB (medium city/suburban) or 3 dB (metropolitan/dense urban).

## 4. Simplified Distance Estimator

For quick back-of-the-envelope calculations in detection rules:

```javascript
function estimateDistanceKm(rsrp, freq_mhz, tx_power_dbm, environment) {
  const pathLoss = tx_power_dbm - rsrp;

  // Select model coefficients based on frequency and environment
  let A, B; // PL = A + B*log10(d)

  if (freq_mhz < 1000) {
    // Sub-1GHz (Band 8, 20, 28): wider coverage
    A = environment === 'urban' ? 120.9 : 108.5;
    B = environment === 'urban' ? 37.6 : 33.8;
  } else if (freq_mhz < 2200) {
    // 1-2.2 GHz (Band 1, 3): standard macro
    A = environment === 'urban' ? 137.4 : 124.0;
    B = environment === 'urban' ? 35.2 : 31.5;
  } else {
    // 2.2+ GHz (Band 7, 38, 40, 41): capacity layer
    A = environment === 'urban' ? 142.1 : 128.5;
    B = environment === 'urban' ? 36.7 : 32.8;
  }

  const d_km = Math.pow(10, (pathLoss - A) / B);
  return Math.max(0.01, Math.min(d_km, 200)); // clamp to reasonable range
}
```

## 5. Typical Cell Ranges by Band (Urban Deployment)

These are practical planning ranges, not maximum theoretical coverage.

| Band | Freq (MHz) | TX Power | Typical Urban Range | Max Suburban Range |
|------|-----------|----------|--------------------|--------------------|
| B1   | 2100      | 40-46 dBm| 3-5 km            | 8-12 km            |
| B3   | 1800      | 40-46 dBm| 3-8 km            | 10-15 km           |
| B7   | 2600      | 40-46 dBm| 2-4 km            | 5-8 km             |
| B8   | 900       | 37-43 dBm| 5-10 km           | 15-25 km           |
| B20  | 800       | 40-46 dBm| 5-10 km           | 15-25 km           |
| B28  | 700       | 40-46 dBm| 5-10 km           | 15-30 km           |
| B38  | 2600      | 37-43 dBm| 1-3 km            | 3-5 km             |
| B40  | 2300      | 37-43 dBm| 2-4 km            | 5-8 km             |
| B41  | 2500      | 37-43 dBm| 1-3 km            | 3-6 km             |
| n77  | 3500      | 37-43 dBm| 1-2 km            | 3-5 km             |
| n78  | 3500      | 37-43 dBm| 1-2 km            | 3-5 km             |

## 6. TA Distance vs RSRP Distance Cross-Validation

When both TA and RSRP are available, compare the two distance estimates:

```
ta_distance = TA * 78.12 / 1000  (km, for LTE)
rsrp_distance = estimateDistanceKm(RSRP, freq, TX_power, 'urban')

mismatch = |ta_distance - rsrp_distance|

if mismatch > max(ta_distance, rsrp_distance) * 0.7:
    # >70% disagreement — investigate
    # Possible: signal relay, amplifier, or measurement error

if ta_distance < 0.5 and rsrp_distance > 5:
    # Device claims to be very close (TA-wise) but signal is weak
    # This is actually NORMAL — TA=0 is a catch-all bin and RSRP
    # fluctuates. NOT an anomaly by itself.

if ta_distance > 5 and rsrp_distance < 0.5:
    # Device claims far by TA but signal is very strong
    # Suspicious: possible signal amplifier/repeater
```
