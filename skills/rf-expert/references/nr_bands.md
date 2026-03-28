# 5G NR Band Table — NR-ARFCN Reference

Per 3GPP TS 38.101-1 (FR1: sub-6 GHz) and TS 38.101-2 (FR2: mmWave).

## NR-ARFCN to Frequency

```
F_REF = F_REF_offs + ΔF_Global × (NR-ARFCN - N_REF_offs)
```

| Frequency Range | ΔF_Global (kHz) | F_REF_offs (MHz) | N_REF_offs | NR-ARFCN Range  |
|-----------------|-----------------|-------------------|------------|-----------------|
| 0-3000 MHz      | 5               | 0                 | 0          | 0-599999        |
| 3000-24250 MHz  | 15              | 3000              | 600000     | 600000-2016666  |
| 24250-100000 MHz| 60              | 24250.08          | 2016667    | 2016667-3279165 |

## FR1 Bands (Sub-6 GHz)

| Band | Freq (MHz)    | Duplex | NR-ARFCN DL Range     | Common Use                |
|------|--------------|--------|----------------------|---------------------------|
| n1   | 2110-2170    | FDD    | 422000-434000        | Global (refarmed from B1) |
| n2   | 1930-1990    | FDD    | 386000-398000        | Americas                  |
| n3   | 1805-1880    | FDD    | 361000-376000        | Global (primary NR FDD)   |
| n5   | 869-894      | FDD    | 173800-178800        | Americas (700/850)        |
| n7   | 2620-2690    | FDD    | 524000-538000        | Global (refarmed from B7) |
| n8   | 925-960      | FDD    | 185000-192000        | Global (900 MHz NR)       |
| n20  | 791-862      | FDD    | 158200-172400        | Europe (800 MHz)          |
| n25  | 1930-1995    | FDD    | 386000-399000        | Americas                  |
| n28  | 758-823      | FDD    | 151600-164600        | APAC/LATAM (700 MHz)      |
| n38  | 2570-2620    | TDD    | 514000-524000        | EU (TDD 2.6 GHz)         |
| n40  | 2300-2400    | TDD    | 460000-480000        | Global (TDD 2.3 GHz)     |
| n41  | 2496-2690    | TDD    | 499200-537999        | Global (primary NR TDD)   |
| n66  | 2110-2200    | FDD    | 422000-440000        | Americas (AWS ext)        |
| n71  | 617-652      | FDD    | 123400-130400        | Americas (600 MHz)        |
| n77  | 3300-4200    | TDD    | 620000-680000        | Global (C-band)           |
| n78  | 3300-3800    | TDD    | 620000-653333        | Global (primary C-band)   |
| n79  | 4400-5000    | TDD    | 693334-733333        | Japan, China              |

## FR2 Bands (mmWave)

| Band  | Freq (GHz)   | Duplex | NR-ARFCN Range        | Common Use           |
|-------|-------------|--------|----------------------|----------------------|
| n257  | 26.5-29.5   | TDD    | 2054166-2104165      | Global mmWave        |
| n258  | 24.25-27.5  | TDD    | 2016667-2070832      | Global mmWave        |
| n259  | 39.5-43.5   | TDD    | -                    | Future mmWave        |
| n260  | 37-40       | TDD    | 2229166-2279165      | Americas mmWave      |
| n261  | 27.5-28.35  | TDD    | 2070833-2084999      | Americas mmWave      |

## Android API Detection

```javascript
// Heuristic to distinguish LTE EARFCN from NR-ARFCN:
if (arfcn > 100000) {
  // Almost certainly NR-ARFCN (LTE EARFCNs max out ~56739 for Band 48)
  // Use NR band table
} else {
  // LTE EARFCN — use LTE band table
}
```

Note: Some Android versions report NR-ARFCN via `CellIdentityNr.getNrarfcn()` as a separate
field from `CellIdentityLte.getEarfcn()`. The Flycomm SDK stores both in `band_downlinkEarfcn`.
