# LTE Band Table — Complete EARFCN Reference

Per 3GPP TS 36.101 Table 5.7.3-1.

## Formula

```
F_DL (MHz) = F_DL_low + 0.1 × (EARFCN - N_offs_DL)
F_UL (MHz) = F_UL_low + 0.1 × (EARFCN_UL - N_offs_UL)
```

## FDD Bands

| Band | F_DL_low (MHz) | N_offs_DL | EARFCN Range  | F_UL_low (MHz) | Duplex Spacing | Common Regions       |
|------|---------------|-----------|---------------|----------------|----------------|----------------------|
| 1    | 2110          | 0         | 0-599         | 1920           | 190 MHz        | Global (EMEA, APAC)  |
| 2    | 1930          | 600       | 600-1199      | 1850           | 80 MHz         | Americas             |
| 3    | 1805          | 1200      | 1200-1949     | 1710           | 95 MHz         | Global (primary)     |
| 4    | 2110          | 1950      | 1950-2399     | 1710           | 400 MHz        | Americas             |
| 5    | 869           | 2400      | 2400-2649     | 824            | 45 MHz         | Americas, APAC       |
| 7    | 2620          | 2750      | 2750-3449     | 2500           | 120 MHz        | Global (capacity)    |
| 8    | 925           | 3450      | 3450-3799     | 880            | 45 MHz         | Global (coverage)    |
| 10   | 2110          | 4150      | 4150-4749     | 1710           | 400 MHz        | Americas             |
| 11   | 1475.9        | 4750      | 4750-4949     | 1427.9         | 48 MHz         | Japan                |
| 12   | 729           | 5010      | 5010-5179     | 699            | 30 MHz         | Americas (700 MHz)   |
| 13   | 746           | 5180      | 5180-5279     | 777            | -31 MHz*       | Americas (FirstNet)  |
| 14   | 758           | 5280      | 5280-5379     | 788            | -30 MHz*       | Americas (FirstNet)  |
| 17   | 734           | 5730      | 5730-5849     | 704            | 30 MHz         | Americas (700 MHz)   |
| 18   | 860           | 5850      | 5850-5999     | 815            | 45 MHz         | Japan                |
| 19   | 875           | 6000      | 6000-6149     | 830            | 45 MHz         | Japan                |
| 20   | 791           | 6150      | 6150-6449     | 832            | -41 MHz*       | Europe (DD, 800MHz)  |
| 21   | 1495.9        | 6450      | 6450-6599     | 1447.9         | 48 MHz         | Japan                |
| 22   | 3510          | 6600      | 6600-7399     | 3410           | 100 MHz        | EU (3.5 GHz)         |
| 25   | 1930          | 7500      | 7500-7699     | 1850           | 80 MHz         | Americas (ext B2)    |
| 26   | 859           | 7700      | 7700-8039     | 814            | 45 MHz         | Americas (ext B5)    |
| 28   | 758           | 8040      | 8040-8689     | 703            | 55 MHz         | APAC, LATAM (APT)    |
| 29   | 717           | 8690      | 8690-9039     | --             | SDL only       | Americas (700 DL)    |
| 30   | 2350          | 9040      | 9040-9209     | 2305           | 45 MHz         | Americas             |
| 31   | 462.5         | 9210      | 9770-10109    | 452.5          | 10 MHz         | Latin America        |
| 32   | 1452          | 9920      | 9920-10359    | --             | SDL only       | Europe               |

*Bands 13, 14, 20: reverse duplex (UL above DL).

## TDD Bands

| Band | Freq (MHz)    | N_offs     | EARFCN Range    | Common Regions            |
|------|--------------|------------|-----------------|---------------------------|
| 33   | 1900-1920    | 36000      | 36000-36199     | EU (historical)           |
| 34   | 2010-2025    | 36200      | 36200-36349     | EU (historical)           |
| 35   | 1850-1910    | 36350      | 36350-36949     | Americas                  |
| 36   | 1930-1990    | 36950      | 36950-37549     | Americas                  |
| 37   | 1910-1930    | 37550      | 37550-37749     | Americas                  |
| 38   | 2570-2620    | 37750      | 37750-38249     | Global (TD-LTE)           |
| 39   | 1880-1920    | 38250      | 38250-38649     | China                     |
| 40   | 2300-2400    | 38650      | 38650-39649     | Global (TD-LTE)           |
| 41   | 2496-2690    | 39650      | 39650-41589     | Global (CBRS/TD-LTE)      |
| 42   | 3400-3600    | 41590      | 41590-43589     | EU/APAC (3.5 GHz)         |
| 43   | 3600-3800    | 43590      | 43590-45589     | EU (3.5 GHz ext)          |
| 44   | 703-803      | 45590      | 45590-46589     | APAC (APT 700)            |
| 45   | 1447-1467    | 46590      | 46590-46789     | Japan                     |
| 46   | 5150-5925    | 46790      | 46790-54539     | LAA (unlicensed)          |
| 47   | 5855-5925    | 54540      | 54540-55239     | V2X                       |
| 48   | 3550-3700    | 55240      | 55240-56739     | CBRS (USA)                |

## Quick Lookup Examples

```
EARFCN 100   → Band 1,  F_DL = 2110 + 0.1×(100-0)     = 2120.0 MHz
EARFCN 1300  → Band 3,  F_DL = 1805 + 0.1×(1300-1200)  = 1815.0 MHz
EARFCN 2850  → Band 7,  F_DL = 2620 + 0.1×(2850-2750)  = 2630.0 MHz
EARFCN 3500  → Band 8,  F_DL = 925  + 0.1×(3500-3450)   = 930.0  MHz
EARFCN 6300  → Band 20, F_DL = 791  + 0.1×(6300-6150)   = 806.0  MHz
EARFCN 38000 → Band 38, F_DL = 2570 + 0.1×(38000-37750) = 2595.0 MHz
```
