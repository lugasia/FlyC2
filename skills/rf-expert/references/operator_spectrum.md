# Operator Spectrum Allocations

## Israel (MCC 425)

### Partner Communications (425-01)
- **Brand names:** Partner, Orange (former), Hot Mobile (MVNO using Partner infra)
- **LTE Bands:** B1 (2100), B3 (1800), B7 (2600), B20 (800)
- **EARFCN ranges:** B1: 0-599, B3: 1200-1949, B7: 2750-3449, B20: 6150-6449
- **5G NR:** n78 (3500 MHz) — mid-band 5G
- **Coverage notes:** Nationwide LTE, B20 for rural/indoor, B7 for urban capacity

### Cellcom (425-02)
- **LTE Bands:** B1 (2100), B3 (1800), B7 (2600), B28 (700)
- **EARFCN ranges:** B1: 0-599, B3: 1200-1949, B7: 2750-3449, B28: 8040-8689
- **5G NR:** n78 (3500 MHz)
- **Coverage notes:** Nationwide, B28 for wide-area coverage

### Pelephone (425-03)
- **Brand names:** Pelephone, Bezeq subsidiary
- **LTE Bands:** B1 (2100), B3 (1800), B7 (2600), B40 (2300 TDD)
- **EARFCN ranges:** B1: 0-599, B3: 1200-1949, B7: 2750-3449, B40: 38650-39649
- **5G NR:** n78 (3500 MHz)
- **Coverage notes:** Nationwide, B40 TDD for capacity in urban

### We4G (425-09)
- **Type:** Wholesale LTE network (Golan, others use as MVNO backend)
- **LTE Bands:** B7 (2600), B40 (2300 TDD)
- **EARFCN ranges:** B7: 2750-3449, B40: 38650-39649
- **Coverage notes:** Urban-focused, capacity-oriented

### Golan Telecom (425-12)
- **Type:** MVNO using Partner (425-01) and We4G (425-09) infrastructure
- **Will appear as:** 425-01 or 425-09 in radio measurements

### PHI (425-15)
- **Type:** IoT/M2M network
- **Coverage notes:** Limited, specific IoT deployments

### Rami Levy (425-16)
- **Type:** MVNO — uses various networks
- **Coverage notes:** Rebranded service, no own infrastructure

## Anomaly Detection for Israeli Operators

### Expected PLMNs in Israel
Any measurement in Israel (bbox: 29.4-33.4°N, 34.2-35.9°E) should have:
- MCC 425 (Israel)
- Known MNCs: 01, 02, 03, 09, 12, 15, 16

### Suspicious PLMNs in Israel
- **MCC 001**: Test network — almost always IMSI catcher or lab equipment
- **MCC 310-316**: US operators — roaming is possible but rare for connected cells
- **MCC 425 with unknown MNC**: New MVNO or rogue network
- **Any non-425 MCC on a registered (serving) cell**: Unusual — device should register on local MCC

### Band Validation Rules
If you see an Israeli PLMN (425-xx) on a band not allocated to that operator:
- 425-01 on B40 → suspicious (Partner doesn't have B40, Pelephone does)
- 425-02 on B20 → suspicious (Cellcom has B28 not B20, Partner has B20)
- 425-03 on B20 → suspicious (Pelephone doesn't have B20)
- 425-09 on B1 or B3 → suspicious (We4G only has B7 and B40)

## United States (MCC 310-316) — Brief Reference

| PLMN    | Operator   | Key Bands                    |
|---------|-----------|------------------------------|
| 310-260 | T-Mobile  | B2, B4, B12, B66, B71, n41   |
| 310-120 | Sprint*   | B25, B26, B41                |
| 311-480 | Verizon   | B2, B4, B5, B13, B66, n77   |
| 310-410 | AT&T      | B2, B4, B5, B12, B14, B30   |

*Sprint merged into T-Mobile.

## United Kingdom (MCC 234)

| PLMN    | Operator       | Key Bands              |
|---------|---------------|------------------------|
| 234-10  | O2            | B1, B3, B8, B20, B40   |
| 234-15  | Vodafone      | B1, B3, B7, B8, B20    |
| 234-20  | Three UK      | B1, B3, B20            |
| 234-30  | EE (BT)       | B1, B3, B7, B20, B38   |
| 234-33  | EE (BT)       | Same as 234-30         |

## Germany (MCC 262)

| PLMN    | Operator       | Key Bands              |
|---------|---------------|------------------------|
| 262-01  | Telekom       | B1, B3, B7, B8, B20    |
| 262-02  | Vodafone DE   | B1, B3, B7, B8, B20    |
| 262-03  | O2/Telefonica | B1, B3, B7, B8, B20    |
| 262-06  | 1&1 Drillisch | B1, B3, B7             |
