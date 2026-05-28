#!/usr/bin/env bash
# deploy.sh - Script to provision GCP resources for Walkies deployment.
# Usage: ./deploy.sh [PROJECT_ID] [REGION]

set -euo pipefail

# Configuration with defaults
PROJECT_ID="${1:-$(gcloud config get-value project 2>/dev/null || echo "")}"
REGION="${2:-us-central1}"
REPO_NAME="walkies-repo"
BUCKET_NAME="walkies-data-${PROJECT_ID}"
SA_NAME="walkies-deployer"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

if [ -z "$PROJECT_ID" ]; then
  echo "Error: PROJECT_ID is required."
  echo "Please pass it as the first argument: ./deploy.sh [PROJECT_ID] [REGION]"
  echo "Or configure a default project: gcloud config set project [PROJECT_ID]"
  exit 1
fi

echo "========================================="
echo "Initializing GCP resources for Walkies"
echo "Project: $PROJECT_ID"
echo "Region:  $REGION"
echo "========================================="

# 1. Enable APIs
echo "Enabling GCP APIs..."
gcloud services enable \
  artifactregistry.googleapis.com \
  run.googleapis.com \
  storage.googleapis.com \
  iam.googleapis.com \
  --project="$PROJECT_ID"

# 2. Create Artifact Registry
echo "Checking Artifact Registry repository..."
if gcloud artifacts repositories describe "$REPO_NAME" --location="$REGION" --project="$PROJECT_ID" &>/dev/null; then
  echo "Artifact repository '$REPO_NAME' already exists."
else
  echo "Creating Artifact Registry repository '$REPO_NAME'..."
  gcloud artifacts repositories create "$REPO_NAME" \
    --repository-format=docker \
    --location="$REGION" \
    --description="Walkies docker images" \
    --project="$PROJECT_ID"
fi

# 3. Create Google Cloud Storage Bucket for walk files
echo "Checking GCS bucket..."
if gsutil ls -p "$PROJECT_ID" "gs://${BUCKET_NAME}" &>/dev/null; then
  echo "Bucket gs://${BUCKET_NAME} already exists."
else
  echo "Creating GCS bucket gs://${BUCKET_NAME}..."
  gsutil mb -p "$PROJECT_ID" -l "$REGION" -c standard "gs://${BUCKET_NAME}"
  # Enable uniform bucket-level access for security
  gsutil uniformbucketlevelaccess set on "gs://${BUCKET_NAME}"
fi

# 4. Create Service Account for GitHub Actions Deployments
echo "Checking deployer Service Account..."
if gcloud iam service-accounts describe "$SA_EMAIL" --project="$PROJECT_ID" &>/dev/null; then
  echo "Service account $SA_EMAIL already exists."
else
  echo "Creating Service Account '$SA_NAME'..."
  gcloud iam service-accounts create "$SA_NAME" \
    --display-name="Walkies GitHub Deployer" \
    --project="$PROJECT_ID"
fi

# 5. Grant IAM Permissions to Service Account
echo "Granting roles to Service Account..."
ROLES=(
  "roles/artifactregistry.writer"
  "roles/run.developer"
  "roles/storage.objectAdmin"
  "roles/iam.serviceAccountUser"
)

for ROLE in "${ROLES[@]}"; do
  echo "Granting $ROLE..."
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="$ROLE" \
    --condition=None \
    --quiet
done

# 6. Generate Key file for GitHub Actions secret
KEY_FILE="walkies-deployer-key.json"
echo "Generating Service Account key..."
if [ -f "$KEY_FILE" ]; then
  echo "A local key file '$KEY_FILE' already exists. Skipping generation."
else
  gcloud iam service-accounts keys create "$KEY_FILE" \
    --iam-account="$SA_EMAIL" \
    --project="$PROJECT_ID"
  echo "Created key file: $KEY_FILE"
fi

echo "========================================="
echo "SUCCESS!"
echo "========================================="
echo "1. Set up your free Neon database at https://neon.tech/ and get the Connection String."
echo "2. Add the following secrets to your GitHub repository (Settings > Secrets and variables > Actions):"
echo "   - GCP_PROJECT_ID: $PROJECT_ID"
echo "   - GCP_SA_KEY: (Copy the contents of the generated '$KEY_FILE')"
echo "   - GCP_REGION: $REGION"
echo "   - ANALYTICS_DB_URL: (Your Neon Postgres connection string starting with postgresql://)"
echo "   - GOOGLE_CLIENT_ID: (Your Google OAuth 2 Client ID)"
echo "3. Safely delete the local '$KEY_FILE' key file after adding it to GitHub. Do not commit it!"
echo "========================================="
