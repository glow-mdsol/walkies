# Walkies

**Walkies** is an open-source web application designed for Type 1 diabetics (specifically tuned for MiniMed 780G pump + Guardian 4 CGM users and Garmin GPS sport watches) who hike and walk.

The platform aligns and visualizes activity performance metrics (heart rate, pace, elevation profile, distance) with physiological and glycemic signals (continuous blood glucose, basal deliveries, bolus timing, SmartGuard automation, and active insulin-on-board).

---

## Key Features

- **Multi-Track Distance Alignment**: Synchronizes Garmin GPS and biometric tracks with CareLink continuous glucose monitor (CGM) and insulin pump data along route distance.
- **Dynamic Map Visualization**: Interactive Leaflet maps with route polyline color shading based on heart rate zones, glucose levels, or elevation, complete with hourly progress markers.
- **Pharmacokinetic Active Insulin (IOB)**: Dynamic active bolus decay curve calculation driven by customizable insulin profiles (Fiasp, NovoRapid, Humalog, Apidra, Actrapid, Humulin S, or custom kinetics).
- **Metabolic & Exertion Analytics**: Time In Range (TIR), glucose rate of change, hypo/hyper event detection, HR drift / decoupling analysis, and weather strain evaluation.
- **Secure Sharing Links**: Generate time-scoped, revocable public links to share walk analyses with healthcare providers or caregivers without exposing private account data.
- **Direct Cloud Sync**: Automated synchronization with Garmin Connect and Medtronic CareLink Cloud.

---

## Screenshots

### Walk Library & Dashboard

![Walkies walk list](img/walks.png)

*The landing dashboard provides secure access, walk library browsing, route comparisons, activity rating, and file management.*

---

### Comprehensive Analysis Overview

![Walkies analysis overview](img/analysis.png)

*The main analysis view correlates activity metrics, altitude, heart rate, and glycemic response against route distance.*

---

### Glucose, Basal Delivery & Active Insulin

![Walkies glucose and insulin chart](img/alt_insulin.png)

*Combines CGM sensor glucose, automated/manual basal deliveries, bolus events, and active bolus decay (IOB) across the duration of the walk.*

---

### Stress & Aerobic Decoupling

![Walkies stress chart](img/stress.png)

*Compares observed heart rate against expected heart rate heuristics to detect physiological strain and cardiac drift.*

---

### Interactive Map View

![Walkies map view](img/map.png)

*Displays the GPS route colored by heart rate intensity, with milestone and hourly progress markers.*

---

## Insulin Profiles & Kinetics

Walkies includes a configurable insulin profile system to accurately model active insulin on board (IOB) decay rather than relying on generic fixed durations.

### Profile Configuration & Clinical Templates

![Walkies insulin profile setup](img/insulin_prof.png)

Predefined clinical kinetic templates are included for common insulin analogs:
- **Fiasp** (Ultra-Rapid)
- **NovoRapid** (Rapid)
- **Humalog** (Rapid)
- **Apidra** (Rapid)
- **Actrapid** (Short-Acting / Regular)
- **Humulin S** (Short-Acting / Regular)
- **Custom Kinetics** (User-defined onset, peak window, and total DIA)

### Active Profile in Analysis

![Walkies insulin profile in analysis](img/insulin_profsel.png)

*The active profile drives the real-time pharmacokinetic IOB curve displayed alongside basal and bolus events.*

---

## Supported Data Inputs

| Source | File Type | Data Extracted |
|---|---|---|
| **Garmin Sport Watches** | `.fit` | GPS coordinates, elevation profile, heart rate, cadence, distance, timestamps |
| **Medtronic CareLink** | `.csv` | CGM sensor glucose readings, basal rates, bolus volumes, carb entries, SmartGuard states |
| **GPS / Mapping Tools** | `.gpx` | Route tracks and waypoints (optional fallback) |

---

## Clinical Parameters

- **Target Glucose Range**: 4.0 – 8.0 mmol/L (72 – 144 mg/dL)
- **Hypoglycemia Threshold**: ≤ 3.9 mmol/L (70 mg/dL)
- **Hyperglycemia Threshold**: ≥ 12.9 mmol/L (232 mg/dL)
- **Default Carb Ratio (ICR)**: 5 g/U
- **Default Insulin Sensitivity (ISF)**: 2.0 mmol/L per U
- **Standard Duration of Insulin Action (DIA)**: ~4 hours (peak action ~75 min for rapid-acting analogs)

---

## Documentation

- **[Developer Guide](DEVELOPMENT.md)**: Local development setup, environment configuration, database migrations, Docker Compose, testing, and API reference.
- **[Deployment Guide](DEPLOYMENT.md)**: Production deployment on Google Cloud Platform (Cloud Run, Firebase Hosting, Neon PostgreSQL), self-hosting on VPS, and health data governance (UK GDPR / HIPAA).
- **[Analysis Catalogue](Analysis.md)**: Detailed breakdown of implemented and planned physiological, glycemic, and environmental metrics.

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.