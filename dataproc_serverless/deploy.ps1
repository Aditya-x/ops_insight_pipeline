# =============================================================================
# Dataproc Serverless Daily Ingestion - Deployment Script (PowerShell)
# =============================================================================

# Configuration
$PROJECT_ID = "biz-ops-031099"
$REGION = "asia-south1"
$GCS_BUCKET = "ops-tickets-files"
$FUNCTION_NAME = "trigger-daily-ingestion"
$SCHEDULER_NAME = "daily-ticket-ingestion-scheduler"
$SERVICE_ACCOUNT_NAME = "dataproc-sa"

Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "  Dataproc Serverless Pipeline Deployment" -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host ""

# -----------------------------------------------------------------------------
# Step 0: Enable required APIs
# -----------------------------------------------------------------------------
Write-Host "[Step 0] Enabling required APIs..." -ForegroundColor Yellow

gcloud services enable dataproc.googleapis.com cloudfunctions.googleapis.com cloudscheduler.googleapis.com cloudbuild.googleapis.com bigquery.googleapis.com storage.googleapis.com --project=$PROJECT_ID

Write-Host "[OK] APIs enabled" -ForegroundColor Green
Write-Host ""

# -----------------------------------------------------------------------------
# Step 1: Create Service Account for Dataproc Serverless
# -----------------------------------------------------------------------------
Write-Host "[Step 1] Setting up Service Account..." -ForegroundColor Yellow

$SA_EMAIL = "$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com"

# Create service account (ignore error if exists)
gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME --display-name="Dataproc Serverless Service Account" --project=$PROJECT_ID 2>$null

# Grant necessary roles
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA_EMAIL" --role="roles/dataproc.worker" --quiet 2>$null
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA_EMAIL" --role="roles/bigquery.dataEditor" --quiet 2>$null
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA_EMAIL" --role="roles/bigquery.jobUser" --quiet 2>$null
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA_EMAIL" --role="roles/storage.objectAdmin" --quiet 2>$null

Write-Host "[OK] Service account configured: $SA_EMAIL" -ForegroundColor Green
Write-Host ""

# -----------------------------------------------------------------------------
# Step 2: Upload PySpark job to GCS
# -----------------------------------------------------------------------------
Write-Host "[Step 2] Uploading PySpark job to GCS..." -ForegroundColor Yellow

gsutil cp .\daily_incremental_job.py gs://$GCS_BUCKET/spark_jobs/daily_incremental_job.py

Write-Host "[OK] PySpark job uploaded to: gs://$GCS_BUCKET/spark_jobs/daily_incremental_job.py" -ForegroundColor Green
Write-Host ""

# -----------------------------------------------------------------------------
# Step 3: Deploy Cloud Function (trigger)
# -----------------------------------------------------------------------------
Write-Host "[Step 3] Deploying Cloud Function..." -ForegroundColor Yellow

gcloud functions deploy $FUNCTION_NAME --gen2 --runtime=python311 --region=$REGION --source="./trigger_function" --entry-point=trigger_dataproc_batch --trigger-http --allow-unauthenticated --memory=256MB --timeout=60s --project=$PROJECT_ID

# Get function URL
$FUNCTION_URL = gcloud functions describe $FUNCTION_NAME --region=$REGION --project=$PROJECT_ID --format="value(serviceConfig.uri)"

if ([string]::IsNullOrEmpty($FUNCTION_URL)) {
    Write-Host "[ERROR] Failed to get function URL. Check if function deployed correctly." -ForegroundColor Red
    Write-Host "Run: gcloud functions list --region=$REGION" -ForegroundColor Yellow
    exit 1
}

Write-Host "[OK] Function deployed at: $FUNCTION_URL" -ForegroundColor Green
Write-Host ""

# -----------------------------------------------------------------------------
# Step 4: Create Cloud Scheduler Job
# -----------------------------------------------------------------------------
Write-Host "[Step 4] Creating Cloud Scheduler job..." -ForegroundColor Yellow

# Delete existing job (ignore error)
gcloud scheduler jobs delete $SCHEDULER_NAME --location=$REGION --project=$PROJECT_ID --quiet 2>$null

# Create new scheduler job - runs daily at 8:00 AM IST
gcloud scheduler jobs create http $SCHEDULER_NAME --location=$REGION --schedule="0 8 * * *" --time-zone="Asia/Kolkata" --uri="$FUNCTION_URL" --http-method=GET --attempt-deadline=600s --project=$PROJECT_ID

Write-Host "[OK] Scheduler created! Runs daily at 8:00 AM IST" -ForegroundColor Green
Write-Host ""

# -----------------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------------
Write-Host "=============================================" -ForegroundColor Green
Write-Host "  DEPLOYMENT COMPLETE!" -ForegroundColor Green
Write-Host "=============================================" -ForegroundColor Green
Write-Host ""
Write-Host "Resources Created:" -ForegroundColor Cyan
Write-Host "   - Service Account: $SA_EMAIL"
Write-Host "   - PySpark Job: gs://$GCS_BUCKET/spark_jobs/daily_incremental_job.py"
Write-Host "   - Cloud Function: $FUNCTION_NAME"
Write-Host "   - Cloud Scheduler: $SCHEDULER_NAME (daily at 8 AM IST)"
Write-Host ""
Write-Host "Test Commands:" -ForegroundColor Yellow
Write-Host "   # Trigger manually:"
Write-Host "   curl $FUNCTION_URL"
Write-Host ""
Write-Host "   # Trigger with specific date:"
Write-Host "   Invoke-RestMethod -Uri '$FUNCTION_URL' -Method POST -ContentType 'application/json' -Body '{\"date\": \"2023-11-01\"}'"
Write-Host ""
Write-Host "   # Force run scheduler:"
Write-Host "   gcloud scheduler jobs run $SCHEDULER_NAME --location=$REGION"
Write-Host ""
Write-Host "   # View batch jobs:"
Write-Host "   gcloud dataproc batches list --region=$REGION"
Write-Host ""
