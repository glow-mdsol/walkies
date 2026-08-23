# Deployment Guide

The GCP + Firebase + Neon walkthrough lives in `README.md`. This document covers the decisions behind those choices, the health-data considerations that affect which options are safe to use, and what to watch for on free tiers.

---

## What data this app stores

| Data type | Where | Sensitivity |
|---|---|---|
| CGM glucose readings (mmol/L, timestamped) | DB + raw Carelink CSV | High — health data |
| Insulin doses and bolus events | DB + raw Carelink CSV | High — health data |
| GPS tracks and activity points | DB + raw FIT file | Medium — location |
| Heart rate, cadence, distance | DB + raw FIT file | Medium |
| Analytics cache (TiR, drop rate, risk profile) | DB | High — derived health data |
| Share tokens (for HCP / caregiver access) | DB | Medium |
| Google OAuth sub identifier | DB | Low — no PII beyond identifier |

The raw files (.csv, .fit) in Cloud Storage and the analytics rows in PostgreSQL together constitute a **health dataset** under UK GDPR (and would be PHI under US HIPAA if this were a covered entity, which it is not — see below).

---

## Regulatory position

### UK GDPR

Geoffrey is a UK resident; this app processes his health data. Under UK GDPR, health data is **Special Category data** (Article 9). As the data subject *and* the person operating the system, Geoffrey is both the **data controller** and the **data processor** for his own data.

Key points:
- There is no requirement to file with the ICO for processing your own personal data for personal use.
- The moment a third party can access the data (e.g., a caregiver or HCP clicking a share link), the controller/processor relationship extends to them as recipients.
- Any cloud provider used to store the data is a **data processor** under UK GDPR and should ideally sign a **Data Processing Agreement (DPA)**.

### HIPAA (US)

HIPAA applies to **covered entities** (healthcare providers, insurers, clearinghouses) and their **business associates**. An individual managing their own health data for personal use is **not a covered entity**. HIPAA does not apply here. This does not diminish the sensitivity of the data — it only means there is no statutory obligation to obtain a BAA.

### Practical implication

The DPA point matters if you ever invite an endocrinologist or caregiver to view share links:

> Once health data is accessible to a third party, UK GDPR Article 9 processing requirements apply to that disclosure, and a lawful basis must exist (Article 9(2)(a) — explicit consent of the data subject is the most natural basis here, since Geoffrey consents to sharing his own health data).

---

## Concerns with free database providers

| Provider | Free tier | Will sign a DPA? | Data region | Verdict |
|---|---|---|---|---|
| **Neon** (current) | 0.5 GB, US-East-1 or EU-West | ✅ DPA available (Pro plan required for BAA/DPA execution) | US or EU selectable | ⚠️ Free tier: no executed DPA; Pro ($19/mo) unlocks it. EU region available. |
| **Supabase** | 500 MB, 2 projects | ❌ No DPA on free tier | US (AWS us-east-1) | ❌ Not suitable for health data without upgrade |
| **PlanetScale** | Discontinued free tier | N/A | N/A | N/A |
| **Turso** (SQLite-edge) | 9 GB, 500 DBs | ❌ No DPA on free tier | Various | ❌ Not suitable for health data without upgrade |
| **CockroachDB** | 10 GB | ✅ DPA available on free tier | EU selectable | ✅ Acceptable — DPA available, EU region option |
| **Self-hosted SQLite on VPS** | Cost of VPS (~£3–5/mo Hetzner) | N/A — you control the server | EU (Hetzner has Helsinki, Nuremberg) | ✅ Best option for data sovereignty |

### The core problem with free tiers

Free tiers are designed for hobby projects, not health data. Common issues:

1. **No DPA / data processing agreement.** The provider can inspect, migrate, or delete your data under their own terms. Most explicitly exclude health/biometric data from their acceptable use policy on free tiers.
2. **Data residency defaults to the US.** Free Neon and Supabase instances land on `us-east-1`. UK GDPR requires that data transferred outside the UK/EEA has adequate protection (US adequacy decision is in flux post-Schrems II; UK has its own International Data Transfer Agreement framework).
3. **No audit log / access controls.** Who at Neon can query your glucose readings? Free tiers have no SLA or access transparency.
4. **Ephemeral storage risk.** Free tiers can suspend inactive instances and delete data after 30–90 days of inactivity.

---

## Recommended deployment paths

### Option A — Current stack (GCP + Neon free tier)

**Acceptable for personal solo use where no third party accesses the data.** The moment you share a walk link with an HCP or caregiver, you should move to Option B or C.

Mitigations if staying on free Neon:
- Set Neon region to `eu-west` (Frankfurt) at project creation, not `us-east`.
- Encrypt the raw Carelink CSVs before uploading to GCS (`CSEK` customer-supplied encryption keys).
- Rotate the Cloud Run service account key regularly.
- Do not store the `walkies-deployer-key.json` anywhere after adding it to GitHub Secrets.

### Option B — GCP + Neon Pro (EU) — recommended if sharing with HCPs

Switch Neon to a paid plan (starts at $19/mo) to gain:
- Executed DPA from Neon.
- EU region (Frankfurt) for data residency.
- Point-in-time recovery for the analytics DB.

No change to the deployment pipeline — just update `ANALYTICS_DB_URL`.

### Option C — Self-hosted VPS (best for data sovereignty)

Run the full stack on a Hetzner CX22 (Helsinki, EU, ~£4/mo):

```
Docker Compose
├── backend (FastAPI, Cloud Run → Docker on VPS)
├── frontend (served by Caddy with auto-HTTPS)
└── postgres (or keep SQLite — no external dependency)
```

Advantages:
- You control the disk; no provider can access your data.
- SQLite works fine for one user — no Postgres required.
- Caddy handles HTTPS automatically via Let's Encrypt.
- No free-tier inactivity timeouts.
- Total cost: ~£4–5/mo.

The walk file storage (currently GCS) can be replaced with a local volume mount — GCS is only needed for durability and CDN when running serverless (Cloud Run).

### Option D — Keep it local (development only)

`task dev` against `backend/data/` and a local SQLite file is already private by design. This is the zero-risk option if the app is only ever run on your own machine.

---

## File storage (Cloud Storage)

The raw Carelink CSVs and FIT files in the GCS bucket are the highest-sensitivity assets — they are the complete, unprocessed health record. Recommendations:

- Enable **Object Versioning** on the bucket so files are recoverable.
- Set **Uniform bucket-level access** (no ACLs) and ensure the bucket is not public.
- Consider **customer-managed encryption keys (CMEK)** via Cloud KMS if you move to Option B/C — this means Google cannot decrypt the files at rest without your key.
- Do **not** set a lifecycle delete policy below 90 days.

---

## Summary

| Concern | Risk level | Mitigation |
|---|---|---|
| No DPA on Neon free tier | Medium (personal use) → High (if sharing) | Move to Neon Pro or self-host before sharing |
| Data in US region | Low (personal) → Medium (GDPR sharing) | Set Neon region to EU at creation; or self-host |
| Raw files unencrypted at rest in GCS | Low (access-controlled by IAM) | CMEK for higher assurance |
| Free tier inactivity suspension | Medium | Upgrade or self-host; schedule a weekly keep-alive ping |
| No audit trail on DB access | Low (personal) | Acceptable for personal use; upgrade if sharing with HCPs |
