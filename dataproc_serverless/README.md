# 🚀 Dataproc Serverless - Daily Ingestion Pipeline

This pipeline uses **Dataproc Serverless** to process daily ticket data from GCS, leveraging your existing PySpark expertise.

---

## 📊 Cost Estimation (with $200 Free Credits)

| Component | Per Run | 61 Days (Nov-Dec) | Notes |
|-----------|---------|-------------------|-------|
| **Dataproc Serverless** | ~$0.10-0.20 | ~$6-12 | 2-4 DCUs × 5 min × $0.06/DCU-hr |
| **Cloud Function** | ~$0 | ~$0 | Free tier (2M/month) |
| **Cloud Scheduler** | ~$0 | ~$0 | Free tier (3 jobs) |
| **BigQuery** | ~$0.01 | ~$0.60 | Query processing |
| **GCS** | ~$0 | ~$0 | Already in use |
| **Total** | ~$0.15 | **~$10-15** | **Well within $200 budget!** |

---

## 🏗️ Architecture

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  Cloud Scheduler │────▶│  Cloud Function  │────▶│    Dataproc      │
│  (Daily @ 8AM)   │     │  (Trigger)       │     │    Serverless    │
│                  │     │                  │     │                  │
│  asia-south1     │     │  256MB / 60s     │     │  PySpark Job     │
└──────────────────┘     │  Submit batch    │     │  2-10 executors  │
                         └──────────────────┘     └────────┬─────────┘
                                                           │
                         ┌─────────────────────────────────┼─────────────────┐
                         │                                 ▼                 │
                         │           ┌──────────────────────────────┐        │
                         │           │         BigQuery             │        │
                         │           │                              │        │
                    ┌────┴────┐      │  • tickets_raw (append)     │        │
                    │   GCS   │      │  • fact_tickets             │        │
                    │         │◀─────│  • features_daily           │        │
                    │ CSV per │      │  • backlog_daily            │        │
                    │   day   │      │  • daily_agent_load         │        │
                    └─────────┘      │  • ticket_sla_risk          │        │
                                     │  • ingestion_state          │        │
                                     └──────────────────────────────┘        │
                                                                             │
                         ┌───────────────────────────────────────────────────┘
                         │
                         ▼
                    Dashboard
                    (Looker Studio)
```

---

## 📁 Files

```
dataproc_serverless/
├── daily_incremental_job.py      # Main PySpark job for Dataproc Serverless
├── trigger_function/
│   ├── main.py                   # Cloud Function to trigger batch
│   └── requirements.txt          # Function dependencies
├── deploy.ps1                    # One-click deployment (PowerShell)
└── README.md                     # This file
```

---

## 🚀 Deployment

### Prerequisites

1. **Google Cloud SDK** installed and authenticated
2. **$200 free credits** (or billing enabled)

### One-Command Deployment

```powershell
cd d:\Data_Analysis\tickets-de-proj\dataproc_serverless
.\deploy.ps1
```

This script will:
1. ✅ Enable required APIs
2. ✅ Create service account with proper permissions
3. ✅ Upload PySpark job to GCS
4. ✅ Deploy Cloud Function
5. ✅ Create Cloud Scheduler (8:00 AM IST daily)

---

## 🧪 Testing

### Test the full pipeline manually:

```powershell
# Option 1: Trigger via Cloud Scheduler
gcloud scheduler jobs run daily-ticket-ingestion-scheduler --location=asia-south1

# Option 2: Trigger Cloud Function directly
$url = gcloud functions describe trigger-daily-ingestion --region=asia-south1 --format='value(serviceConfig.uri)'
Invoke-RestMethod -Uri $url -Method GET
```

### Process a specific date:

```powershell
# Trigger with specific date
Invoke-RestMethod -Uri $url -Method POST -ContentType "application/json" -Body '{"date": "2023-11-01"}'
```

### Submit PySpark job directly (for testing):

```powershell
gcloud dataproc batches submit pyspark `
    gs://ops-tickets-files/spark_jobs/daily_incremental_job.py `
    --region=asia-south1 `
    --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.32.2.jar `
    --service-account=dataproc-sa@biz-ops-031099.iam.gserviceaccount.com `
    -- --date 2023-11-01
```

---

## 📈 Monitoring

### View running/completed batch jobs:

```powershell
# List all batches
gcloud dataproc batches list --region=asia-south1

# View specific batch
gcloud dataproc batches describe BATCH_ID --region=asia-south1

# View batch logs
gcloud dataproc batches wait BATCH_ID --region=asia-south1
```

### Check ingestion progress in BigQuery:

```sql
-- View latest ingestions
SELECT * 
FROM `biz-ops-031099.ops_analytics.ingestion_state`
ORDER BY simulation_date DESC
LIMIT 10;

-- Check days remaining
SELECT 
    MAX(simulation_date) as last_processed,
    DATE_DIFF(DATE('2023-12-31'), MAX(simulation_date), DAY) as days_remaining
FROM `biz-ops-031099.ops_analytics.ingestion_state`;
```

### View Cloud Function logs:

```powershell
gcloud functions logs read trigger-daily-ingestion --region=asia-south1 --limit=50
```

---

## ⚙️ Configuration

### Change processing schedule:

Edit `deploy.ps1`:
```powershell
--schedule="0 9 * * *"   # 9:00 AM instead of 8:00 AM
--schedule="0 */6 * * *" # Every 6 hours (faster simulation)
```

### Adjust Dataproc resources:

Edit `trigger_function/main.py`:
```python
"spark.dynamicAllocation.minExecutors": "4",   # More min executors
"spark.dynamicAllocation.maxExecutors": "20",  # More max executors
```

### Reset and start from scratch:

```sql
-- In BigQuery Console
TRUNCATE TABLE `biz-ops-031099.ops_analytics.ingestion_state`;
```

---

## 🆚 Why Dataproc Serverless?

| Feature | Cloud Functions | Dataproc Serverless |
|---------|-----------------|---------------------|
| **Your existing code** | Need to rewrite | ✅ Reuse PySpark skills |
| **Spark support** | ❌ No | ✅ Yes |
| **Max runtime** | 60 min | ✅ 24 hours |
| **Memory** | 32 GB max | ✅ Unlimited (scales) |
| **Debugging** | Limited | ✅ Full Spark UI |
| **Cost** | $0 | ~$0.10-0.20/run |

---

## 🛑 Stopping the Pipeline

```powershell
# Pause scheduler (keeps job, stops execution)
gcloud scheduler jobs pause daily-ticket-ingestion-scheduler --location=asia-south1

# Resume later
gcloud scheduler jobs resume daily-ticket-ingestion-scheduler --location=asia-south1

# Delete everything
gcloud scheduler jobs delete daily-ticket-ingestion-scheduler --location=asia-south1
gcloud functions delete trigger-daily-ingestion --region=asia-south1
```

---

## 🔄 Fast-Forward Testing

Want to process all 61 days quickly? Run this in a loop:

```powershell
# Process multiple days rapidly
$url = gcloud functions describe trigger-daily-ingestion --region=asia-south1 --format='value(serviceConfig.uri)'

# Loop through dates
$startDate = [datetime]"2023-11-01"
$endDate = [datetime]"2023-12-31"

for ($d = $startDate; $d -le $endDate; $d = $d.AddDays(1)) {
    $dateStr = $d.ToString("yyyy-MM-dd")
    Write-Host "Processing $dateStr..."
    Invoke-RestMethod -Uri $url -Method POST -ContentType "application/json" -Body "{`"date`": `"$dateStr`"}"
    Start-Sleep -Seconds 30  # Wait for job to complete
}
```

---

## 💡 Tips

1. **Monitor costs** in Cloud Console → Billing → Reports
2. **Set budget alerts** to avoid surprises
3. **Use Spark UI** (in Dataproc Console) to debug performance issues
4. **Check batch status** before running next day to avoid duplicates

---

## 📅 Timeline

With daily runs at 8 AM IST:
- **Nov 1 - Nov 30**: Day 1-30
- **Dec 1 - Dec 31**: Day 31-61
- **Completion**: ~2 months of real time

For faster simulation, increase scheduler frequency or use the fast-forward script above!
