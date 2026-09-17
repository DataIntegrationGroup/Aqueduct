# BernCo HydroVu — Location Survey (for completing `location_ids`)

Survey date: 2026-09-15. Live check of all 53 locations on the BernCo
HydroVu account.

## Parameters

Each location can report several measurements at once. `parameterId="4"`
("Level: Depth to Water") is DTW — the only one Aqueduct ingests into the
canonical model. The other common ones: `1` Temperature, `2` Pressure, `3`
Depth (the sensor's depth below the water surface — opposite direction from
DTW, not the same thing), `5` Elevation, `9`–`14` water-quality readings
(conductivity, salinity, TDS, density), `16` Baro, `26`/`33` battery. Those
are all just gateway/sensor telemetry or measurements Aqueduct doesn't use.

## Locations WITH DTW (36) — 21 active, 2 dormant, 13 worth checking

"Active" means it reported DTW within the last 60 days. "Dormant" means a
long gap (stopped in 2024 or 2025).
**"Worth checking" is its own category, not "dormant":** these 13 locations
have gaps of only a few weeks to a few months, all last reporting sometime in
2026 (several as recently as mid-July) — that could be a temporary outage,
maintenance, or connectivity issue rather than a dead sensor.

**Decision: ingest all 34 (Active + Worth checking locations)**, excluding only
the 2 confirmed Dormant ones. This can change later — if any "Worth checking"
location turns out to be genuinely dormant rather than a temporary gap, drop
it from the allowlist at that point.

| Location | ID | Earliest reading | Latest reading | Status |
|---|---|---|---|---|
| Z - SandovalCounty 2 | 6679271820361728 | 2018-09-21 | 2024-02-01 | Dormant |
| E-9673-969652 | 6452475357167616 | 2023-02-25 | 2025-01-15 | Dormant |
| WhisperingPines-1002958 (already in allowlist) | 5617246532927488 | 1970-01-01 (bad clock, see note) | 2026-01-12 | Worth checking |
| E-2034-969620 | 4739653592547328 | 2023-02-24 | 2026-02-08 | Worth checking |
| ArroyoVista-1002964 | 4910076672737280 | 2024-02-16 | 2026-02-23 | Worth checking |
| Carlito Springs Well | 4728230513278976 | 2024-02-28 | 2026-05-11 | Worth checking |
| Masa Del Sol | 6714629525405696 | 2026-03-31 | 2026-06-02 | Worth checking |
| E-9673-969652TD | 5852074096852992 | 2025-02-09 | 2026-07-06 | Worth checking |
| default-969659 | 4657879867523072 | 2025-02-19 | 2026-07-14 | Worth checking |
| Smith-1-965697TD | 4535701328494592 | 2025-02-19 | 2026-07-15 | Worth checking |
| obsoE-9477-969662 | 4591421252042752 | 2023-02-24 | 2026-07-16 | Worth checking |
| OsitaRanch-969626TD | 4881147431354368 | 2023-02-08 | 2026-07-16 | Worth checking |
| default-969622 | 6344228741185536 | 2025-02-19 | 2026-07-16 | Worth checking |
| Magnum Steel | 6670441099886592 | 2025-04-24 | 2026-07-16 | Worth checking |
| E-55-POD 15-1173850TD | 6725276880732160 | 1970-01-01 (bad clock, see note) | 2026-07-16 | Worth checking |
| LVCC | 4551148003983360 | 2026-06-29 | 2026-08-11 | Active |
| Buckboard2-1042846 | 5114911035621376 | 2024-02-15 | 2026-08-11 | Active |
| CedarHill-966586 | 4743285781692416 | 2024-04-08 | 2026-09-01 | Active |
| LiveOak-1092511 | 4985256085422080 | 2024-02-16 | 2026-09-01 | Active |
| RichyPlace-971379 | 5077740153077760 | 2024-02-16 | 2026-09-02 | Active |
| BCFD41-1091568 | 5176076726042624 | 2024-02-15 | 2026-09-02 | Active |
| SabinoSouth-1091617 | 4640585604464640 | 2024-05-31 | 2026-09-03 | Active |
| VistaGrandeCC-586873 | 5164658647760896 | 2018-06-13 | 2026-09-03 | Active |
| GrannyCanyon01-1091585 | 5415858396856320 | 2024-02-15 | 2026-09-03 | Active |
| Los Suenos 4-636473 | 5243580757966848 | 2013-07-24 | 2026-09-10 | Active |
| PinonRidge-1092457 | 5828859770830848 | 2014-01-22 | 2026-09-10 | Active |
| ColumbineThompson05-375835 | 5950167649222656 | 2014-10-09 | 2026-09-10 | Active |
| SierraVista-966932 (already in allowlist) | 6255051791532032 | 2014-01-22 | 2026-09-10 | Active |
| EMCNW50-1093362 | 6623684603543552 | 2024-02-15 | 2026-09-10 | Active |
| McGraneRd-968485 | 4569871521546240 | 2024-07-03 | 2026-09-11 | Active |
| BCFDWildlandSub-1091579 | 4620410022133760 | 2009-05-18 | 2026-09-11 | Active |
| Ojito-1002959 | 5534228269105152 | 2011-04-29 | 2026-09-11 | Active |
| SP5VuLink-457931 | 4583081054175232 | 2015-11-30 | 2026-09-15 | Active |
| SP6VuLink-457971 | 4595206250168320 | 2016-07-19 | 2026-09-15 | Active |
| SP3VuLink-457113 | 5002593259880448 | 2014-09-08 | 2026-09-15 | Active |
| SP4VuLink-636814 | 5657721615810560 | 2019-11-05 | 2026-09-15 | Active |

**Note on `1970-01-01` earliest dates:** bad device clocks, not real readings —
`bernco_hydrovu.md` already documents this. Real data starts later
(`WhisperingPines-1002958` in 2010, `E-55-POD 15-1173850TD` in 2025).

**`WhisperingPines-1002958` is already in the allowlist but hasn't reported
since 2026-01-12** (about 8 months) — worth checking with BernCo whether it's
expected to come back online.

## Locations WITHOUT DTW (17), and what they report instead

| Location | ID | Reports |
|---|---|---|
| SerenityMesa | 4562953333243904 | no data at all |
| SPwellnest-1099946VL | 4856788170440704 | Temperature, Baro, Battery Level |
| E-94077-1193582VL (Anaya-1) | 4890597735137280 | Temperature, Baro, Battery Level |
| default-1191022 | 4905339374338048 | Temperature, Baro, Battery Level |
| E-2034-1192571VL | 4951324613279744 | Temperature, Baro, Battery Level |
| E-9673-1194722VL | 5038590631215104 | Temperature, Baro, Battery Level |
| default-1193641 | 5281914902609920 | Temperature, Baro, Battery Level |
| E-1639POD1-1191525VL | 5331836498477056 | Temperature, Baro, Battery Level |
| E-55-POD 15-1193641VL | 5991114414620672 | Temperature, Baro, Battery Level |
| Smith-1-1194043VL | 6034748916760576 | Temperature, Baro, Battery Level |
| Magnum Steel Transducer | 6205683930300416 | Temperature, Baro, Battery Level |
| OsitaRanch-1194053VL | 6373441737195520 | Temperature, Baro, Battery Level |
| E-8428-1192586VL | 6395458674884608 | Temperature, Baro, Battery Level |
| Carlito Springs Baro | 6066881290960896 | Temperature, Baro |
| Carlito Springs Flume | 6523865594200064 | Temperature, Baro, Pressure, Elevation |
| Carlito Springs Lower Pool | 5543564384534528 | Temperature, Pressure, Depth (sensor), Elevation |
| default-817181 | 6179866399342592 | Full water-quality sonde suite (Temperature, Pressure, Depth, Conductivity, Resistivity, Salinity, TDS, Density) — never reported DTW despite being a real sonde |

Most of the "no DTW" locations are VuLink gateways reporting only their own
telemetry (temperature/baro/battery) — no water-level sensor attached at all.
`default-817181` is the one exception worth a second look: it's a full sonde
that has simply never reported parameter 4.

## How this was checked

One request per location at `startTime=0` for the earliest reading (HydroVu's
pagination goes straight to the real first reading regardless of year), and
one request covering the last 60 days for the latest. For the 15 non-active
locations, a further backward search (doubling the window each time until
data is found) plus a forward walk to pagination's real end gave the actual
last reading date.
