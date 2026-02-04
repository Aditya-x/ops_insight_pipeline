import os
import json
import calendar
import logging
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql import SparkSession



# # Load the ADC credentials and extract key info - to run locally
adc_path = r"C:\Users\91876\AppData\Roaming\gcloud\application_default_credentials.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = adc_path

# # Read the credentials file to extract service account details
with open(adc_path) as f:
    creds = json.load(f)

spark = (
    SparkSession.builder
    .appName("local-spark-gcp")
    .master("local[*]")
    .config("spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS", adc_path)
    .config(
        "spark.jars",
        "file:///D:/Data_Analysis/tickets-de-proj/spark_jars/gcs-connector.jar,"
        "file:///D:/Data_Analysis/tickets-de-proj/spark_jars/spark-bigquery.jar"
    )
    .config(
        "spark.hadoop.fs.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
    )
    .config(
        "spark.hadoop.fs.AbstractFileSystem.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
    )
    # 🔑 Use service account credentials directly
    .config(
        "spark.hadoop.google.cloud.auth.type",
        "SERVICE_ACCOUNT_JSON_KEYFILE"
    )
    .config(
        "spark.hadoop.google.cloud.auth.service.account.email",
        creds.get("client_email", "")
    )
    .config(
        "spark.hadoop.google.cloud.auth.service.account.private.key.id",
        creds.get("private_key_id", "")
    )
    .config(
        "spark.hadoop.google.cloud.auth.service.account.private.key",
        creds.get("private_key", "").replace("\\n", "\n")
    )
    .config(
        "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
        adc_path
    )
    # 🔑 BLOCK METADATA SERVER
    .config(
        "spark.hadoop.google.cloud.auth.service.account.enable",
        "true"
    )
    .config("spark.sql.adaptive.enabled", "true")  # Auto-optimize queries
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")  # Reduce partitions
    .config("spark.sql.files.maxPartitionBytes", "134217728").config("spark.driver.memory", "4g")
    .getOrCreate()
)




# ------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------
YEAR = 2023
GCS_BASE_PATH = "gs://ops-tickets-files/raw"
PROJECT_ID = "biz-ops-031099"
DATASET_ID = "ops_analytics"
TABLE_ID = "tickets_raw"
TEMP_GCS_BUCKET = "temp-gcs-bucket-spark01"

# ------------------------------------------------------------------
# LOGGING
# ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# MAIN BACKFILL LOOP
# ------------------------------------------------------------------


def load_month_by_month(year, start_month=1, end_month=10):
    """
    SAFER: Load data month-by-month to reduce memory pressure
    Prevents OOM by processing smaller chunks
    """
    total_rows = 0
    
    for month in range(start_month, end_month + 1):
        month_str = f"{month:02d}"
        month_path = f"{GCS_BASE_PATH}/{year}/{month_str}/*.csv"
        
        logger.info(f"📅 Loading month {month_str}/{year}: {month_path}")
        
        try:
            df = (
                spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(month_path)
            )
            
            # Add metadata columns
            df_with_metadata = (
                df
                .withColumn("_ingestion_ts", current_timestamp())
                .withColumn("_source_file", input_file_name())
            )
            
            # Write to BigQuery
            (
                df_with_metadata.write
                .format("bigquery")
                .option("table", f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
                .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
                .mode("append")
                .save()
            )
            
            row_count = df_with_metadata.count()
            total_rows += row_count
            logger.info(f"✅ Month {month_str} loaded: {row_count:,} rows")
            
            # Clear cached data to free memory
            spark.catalog.clearCache()
            
        except Exception as e:
            logger.error(f"❌ Failed to load month {month_str}: {str(e)}")
            continue  # Continue with next month instead of failing entirely
    
    return total_rows


# ------------------------------------------------------------------
# EXECUTION
# ------------------------------------------------------------------

if __name__ == "__main__":
    
    logger.info("🎯 Starting MONTH-BY-MONTH load (safer for large datasets)")
    try:
        total_rows = load_month_by_month(YEAR, start_month=1, end_month=10)
        logger.info(f"🎉 Backfill completed! Total rows: {total_rows:,}")
    except Exception as e:
        logger.error(f"❌ Backfill failed: {str(e)}")
    finally:
        spark.stop()