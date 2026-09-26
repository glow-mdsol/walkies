# Deployment Guide

This guide covers production deployment options for Walkies, step-by-step instructions for deploying to Google Cloud Platform (GCP) with Firebase Hosting and Neon PostgreSQL, health-data privacy considerations, and self-hosting alternatives.

---

## 1. Production Deployment (GCP + Firebase + Neon)

Walkies can be deployed to Google Cloud Platform (GCP) entirely within their free tier boundaries using Google Cloud Run, Firebase Hosting, Google Cloud Storage, and a serverless PostgreSQL database (e.g., Neon.tech).

```
┌─────────────────────────────────────────────────────────┐
│                   Production Architecture                │
├───────────────────────┬─────────────────────────────────┤
│ Frontend              │ Firebase Hosting (Global CDN)   │
│ Backend API           │ Google Cloud Run (Container)    │
│ Analytics / Metadata  │ Neon Serverless PostgreSQL      │
│ File Storage          │ Google Cloud Storage (Bucket)   │
│ CI/CD Pipeline        │ GitHub Actions Workflow         │
└───────────────────────┴─────────────────────────────────┘
```

### Prerequisites

- [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) (`gcloud` CLI) installed and authenticated (`gcloud auth login`).
- A Google Cloud Project created (note the **Project ID**).
- A free PostgreSQL database account at [Neon](https://neon.tech/) to get a connection string.
- Google OAuth 2.0 Client ID created in Google Cloud Console.

### Step 1: Provision GCP Resources

A helper script (`deploy.sh`) is provided in the repository root to configure required GCP services and IAM permissions.

Run it from your terminal:

```bash
chmod +x deploy.sh
./deploy.sh <YOUR_PROJECT_ID> <YOUR_REGION>
```

*Note: If no region is provided, it defaults to `us-central1`.*

The script will:
1. Enable necessary GCP APIs (Cloud Run, Artifact Registry, Cloud Storage, IAM).
2. Create an Artifact Registry repository (`walkies-repo`) for Docker container images.
3. Create a Cloud Storage bucket (`gs://walkies-data-<project-id>`) for durable `.fit`, `.csv`, and `.gpx` file storage.
4. Create a deployment Service Account (`walkies-deployer`) with required IAM roles.
5. Generate a credentials key file (`walkies-deployer-key.json`) for GitHub Actions.

### Step 2: Initialize Firebase Hosting

1. Go to the [Firebase Console](https://console.firebase.google.com/).
2. Select your existing GCP project.
3. Navigate to **Hosting** and click **Get Started** to initialize hosting.

### Step 3: Configure GitHub Secrets

In your GitHub repository, go to **Settings > Secrets and variables > Actions** and add the following repository secrets:

| Secret Name | Description |
|---|---|
| `GCP_PROJECT_ID` | Your Google Cloud project ID |
| `GCP_SA_KEY` | Entire JSON content of `walkies-deployer-key.json` |
| `GCP_REGION` | GCP region (e.g. `us-central1` or `europe-west1`) |
| `GOOGLE_CLIENT_ID` | Google OAuth 2.0 Client ID |
| `ANALYTICS_DB_URL` | Neon PostgreSQL connection string (with SSL mode) |

> [!CAUTION]
> After saving `GCP_SA_KEY` in GitHub Secrets, securely delete `walkies-deployer-key.json` from your local machine. Never commit service account keys to source control.

### Step 4: Deploy via GitHub Actions

Push changes to the `main` branch of your repository. The GitHub Actions workflow in `.github/workflows/deploy.yml` will automatically build container images, deploy Cloud Run services, and publish frontend assets to Firebase Hosting.

### Step 5: Post-Deployment Configuration

1. **Authorized OAuth Origins**:
   - Open Google Cloud Console > **APIs & Services** > **Credentials**.
   - Under your OAuth 2.0 Client ID, add your Firebase Hosting URL (e.g., `https://<project-id>.web.app` and `https://<project-id>.firebaseapp.com`) to **Authorized JavaScript Origins** and **Authorized Redirect URIs**.
2. **Database Migrations**:
   - Run the migration task against the production database:
   ```bash
   export ANALYTICS_DB_URL="postgresql://user:password@host/dbname?sslmode=require"
   task analytics:migrate
   ```

---

## 2. Health Data & Regulatory Considerations

### What Data This App Stores

| Data Type | Storage Location | Sensitivity |
|---|---|---|
| CGM glucose readings (mmol/L, timestamped) | DB + raw Carelink CSV | **High** — health data |
| Insulin doses and bolus events | DB + raw Carelink CSV | **High** — health data |
| GPS tracks and activity points | DB + raw FIT file | **Medium** — location |
| Heart rate, cadence, distance | DB + raw FIT file | **Medium** |
| Analytics cache (TiR, drop rate, risk profile) | DB | **High** — derived health data |
| Share tokens (for HCP / caregiver access) | DB | **Medium** |
| Google OAuth sub identifier | DB | **Low** — identifier only |

The raw files (`.csv`, `.fit`) in Cloud Storage and analytics rows in PostgreSQL together constitute a **health dataset** under UK GDPR.

### Regulatory Position

#### UK GDPR
- Health data is **Special Category data** (Article 9).
- For personal use, an individual is both the **data controller** and **data processor** for their own data (no ICO registration needed for pure personal processing).
- The moment a third party accesses the data (e.g., sharing a route or glycemic report with an endocrinologist or caregiver), lawful basis requirements apply (Article 9(2)(a) — explicit consent).
- Cloud storage/database providers act as **data processors** under UK GDPR and should ideally execute a **Data Processing Agreement (DPA)**.

#### HIPAA (US)
HIPAA applies to **covered entities** (hospitals, insurers, clearinghouses) and business associates. Individuals managing their personal health data are not covered entities. While statutory HIPAA obligations do not apply directly, data sensitivity remains critical.

---

## 3. Database Provider Evaluation & Free Tier Caveats

| Provider | Free Tier | DPA Availability | Region Selection | Recommendation |
|---|---|---|---|---|
| **Neon** (Current) | 0.5 GB | ✅ DPA on Pro plan ($19/mo) | US or EU selectable | ⚠️ Free tier for solo personal use; Pro plan for DPA / EU residency. |
| **CockroachDB** | 10 GB | ✅ DPA available on free tier | EU selectable | ✅ Acceptable — DPA available, EU region option. |
| **Supabase** | 500 MB | ❌ No DPA on free tier | US (AWS us-east-1) | ❌ Not recommended without paid upgrade. |
| **Self-Hosted VPS** | Disk size | N/A — you control the server | UK / EU (e.g. Hetzner) | ✅ **Best option for data sovereignty.** |

### Core Risks of Free Database Tiers
1. **No Executed DPA**: Free tiers rarely include signed data processing agreements and may exclude health data in Acceptable Use Policies.
2. **Default US Data Residency**: Free tiers often allocate resources to US regions by default.
3. **Inactivity Suspension**: Free instances may be suspended or purged after periods of inactivity.

---

## 4. Deployment Architecture Options

### Option A — GCP + Neon Free Tier (Personal Solo Use)
Suitable for solo personal tracking where no third parties access the data.

*Recommended mitigations*:
- Select `eu-west` (Frankfurt) when creating the Neon database.
- Rotate Cloud Run service account keys regularly.
- Ensure Cloud Storage bucket permissions are strictly private.

### Option B — GCP + Neon Pro EU (Recommended for HCP Sharing)
Upgrading Neon to a paid plan unlocks:
- Executed DPA from Neon.
- Guaranteed EU data residency.
- Point-in-time recovery for the analytics database.

### Option C — Self-Hosted VPS (Best for Complete Data Sovereignty)
Run the entire stack on a dedicated VPS (e.g., Hetzner CX22, ~£4/mo):

```text
Docker Compose Stack
├── backend (FastAPI)
├── frontend (Served via Caddy / Nginx with auto-HTTPS)
└── postgres (or local SQLite file mount)
```

**Advantages**:
- Full data sovereignty with complete control over storage disks.
- SQLite or PostgreSQL run locally without third-party database dependencies.
- Automated HTTPS certificates via Let's Encrypt / Caddy.
- Predictable performance with no serverless cold starts or inactivity timeouts.

### Option D — Local Only (Zero Cloud Exposure)
Run `task dev` locally against `backend/data/` with local SQLite storage. All health records remain on your local machine with zero external network transmission.

---

## 5. File Storage Security (Cloud Storage)

Raw CareLink CSVs and Garmin FIT files contain unredacted medical and location records. For cloud deployments:
- **Enable Object Versioning** to prevent accidental deletion or corruption.
- **Enforce Uniform Bucket-Level Access** to prevent unintended public object ACLs.
- **Customer-Managed Encryption Keys (CMEK)**: For high-security environments, encrypt storage buckets with Cloud KMS keys.
- Avoid aggressive lifecycle deletion policies (retain data >= 90 days).
