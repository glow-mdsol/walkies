# Analysis Catalogue

This file lists analysis sets the app can do based on available data.

Legend:
- [x] Implemented now
- [ ] Planned / not implemented yet

## FIT / GPX only (activity-first)

### Session summary
- [x] Distance, duration, average HR
- [x] Activity timeline (distance-aligned HR + altitude)

### Route and map
- [x] GPS route map with start/finish markers
- [x] Route coloring by HR zone
- [x] Hour markers on map

### Weather context (from route/time)
- [x] Temperature and apparent temperature along route
- [x] Wind speed along route
- [x] Headwind profile (distance-aligned)
- [x] Wind direction exposure (wind rose)
- [x] Weather stress score + band
- [x] Headwind exposure percentage

### Stress / HR decoupling (FIT/GPX + optional weather)
- [x] Heuristic model of expected HR from exertion proxy (speed + uphill grade + heat load)
- [x] Distance-aligned chart: observed HR vs expected HR + HR residual
- [x] Possible non-exertional HR elevation score + elevated-minutes estimate
- [x] Caveat in report labeling this as non-diagnostic (review signal, not anxiety diagnosis)
- [x] In-report interpretation note (what this signal can and cannot mean)
- [x] User sensitivity controls (residual smoothing + elevated threshold)
- [x] Multi-session trend chart (decoupling score + elevated minutes)

### Potential additions
- [ ] Pace/speed breakdown by segment
- [ ] Elevation gain/loss and grade analysis
- [ ] Compare two walks (normalized distance/time)

## FIT + CareLink (BGM / insulin)

### Core diabetes metrics
- [x] BG delta during walk
- [x] BG slope during walk
- [x] Time in range (4.0-8.0 mmol/L)
- [x] Hypo count during walk
- [x] Bolus total during walk

### Combined timeline
- [x] Distance-aligned multi-axis timeline with:
  - [x] HR
  - [x] Altitude
  - [x] BG
  - [x] Basal rate
  - [x] Bolus events
- [x] Walk-window shading and phase markers

### Training insight panels
- [x] Phase glucose response (pre / during / post)
- [x] HR-zone vs glucose stability

### Potential additions
- [ ] BG drop rate per km
- [ ] Hypo-risk heuristic score during walk
- [ ] Fueling/insulin recommendation hints (non-clinical)
- [ ] Session-to-session trend charts

## FIT + CareLink + Weather

### Cross-factor analysis
- [x] Weather metrics visible alongside BG/HR outcomes
- [ ] BG slope vs apparent temperature scatter (multi-session)
- [ ] HR drift vs headwind component scatter (multi-session)
- [ ] Condition-normalized effort score over time

## UI and audience modes

- [x] Activity (FIT/GPX) tab for non-diabetes use
- [x] Full Analysis tab for full data stack
- [x] Auto-hide diabetes-only reports if BG data is missing
- [x] BG data badge (detected / not present)
