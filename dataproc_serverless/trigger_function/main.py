"""
Cloud Function to trigger Dataproc Serverless Batch Job
========================================================
This lightweight function is triggered by Cloud Scheduler and submits
a Dataproc Serverless batch job for daily data ingestion.

Cost: ~$0.00 (within free tier)
"""

import functions_framework
from google.cloud import dataproc_v1
import json

# Configuration
PROJECT_ID = "biz-ops-031099"
REGION = "asia-south1"  # Mumbai region
GCS_BUCKET = "ops-tickets-files"
PYSPARK_FILE = f"gs://{GCS_BUCKET}/spark_jobs/daily_incremental_job.py"

# Dataproc Serverless configuration
SPARK_BATCH_CONFIG = {
    "pyspark_batch": {
        "main_python_file_uri": PYSPARK_FILE,
        "jar_file_uris": [
            # BigQuery connector for Spark 3.3+
            "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.32.2.jar"
        ]
    },
    "runtime_config": {
        "version": "2.1",  # Spark 3.3, Java 11
        "properties": {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.minExecutors": "2",
            "spark.dynamicAllocation.maxExecutors": "10",
        }
    },
    "environment_config": {
        "execution_config": {
            "service_account": f"dataproc-sa@{PROJECT_ID}.iam.gserviceaccount.com",
            "subnetwork_uri": f"projects/{PROJECT_ID}/regions/{REGION}/subnetworks/default"
        }
    }
}


@functions_framework.http
def trigger_dataproc_batch(request):
    """
    Trigger a Dataproc Serverless batch job.
    Called by Cloud Scheduler daily.
    
    Optional query parameters:
        - date: Specific date to process (YYYY-MM-DD)
    """
    try:
        # Parse optional date parameter
        request_json = request.get_json(silent=True)
        target_date = None
        
        if request_json and "date" in request_json:
            target_date = request_json["date"]
        elif request.args.get("date"):
            target_date = request.args.get("date")
        
        print(f"🚀 Triggering Dataproc Serverless batch job")
        if target_date:
            print(f"   Target date: {target_date}")
        
        # Create Dataproc client
        client = dataproc_v1.BatchControllerClient(
            client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"}
        )
        
        # Prepare batch config
        batch_config = SPARK_BATCH_CONFIG.copy()
        
        # Add date argument if specified
        if target_date:
            batch_config["pyspark_batch"]["args"] = ["--date", target_date]
        
        # Generate unique batch ID
        import datetime
        batch_id = f"daily-ingestion-{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        # Submit batch job
        request = dataproc_v1.CreateBatchRequest(
            parent=f"projects/{PROJECT_ID}/locations/{REGION}",
            batch=batch_config,
            batch_id=batch_id
        )
        
        operation = client.create_batch(request=request)
        
        # Note: We don't wait for completion - let it run async
        response = {
            "status": "submitted",
            "batch_id": batch_id,
            "operation_name": operation.operation.name,
            "message": f"Batch job '{batch_id}' submitted successfully"
        }
        
        print(f"✅ Job submitted: {batch_id}")
        return json.dumps(response), 200, {"Content-Type": "application/json"}
        
    except Exception as e:
        error_response = {
            "status": "error",
            "message": str(e)
        }
        print(f"❌ Error: {e}")
        return json.dumps(error_response), 500, {"Content-Type": "application/json"}
