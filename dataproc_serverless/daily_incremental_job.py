"""
Daily Incremental Ingestion Job for Dataproc Serverless
========================================================
Processes one day of ticket data from GCS, loads to BigQuery,
and updates all feature tables.

Usage:
    Triggered by Cloud Scheduler -> Cloud Function -> Dataproc Serverless Batch

Arguments (optional):
    --date: Specific date to process (YYYY-MM-DD format)
            If not provided, automatically determines next date from state table
"""

import sys
import argparse
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, when, avg, date_format, hour, count, sum as spark_sum,
    countDistinct, lit, coalesce, to_timestamp, current_timestamp, input_file_name
)

# =============================================================================
# CONFIGURATION
# =============================================================================
PROJECT_ID = "biz-ops-031099"
DATASET_ID = "ops_analytics"
GCS_BUCKET = "ops-tickets-files"
TEMP_GCS_BUCKET = "temp-gcs-bucket-spark01"
SIMULATION_YEAR = 2023
SIMULATION_START_MONTH = 11
SIMULATION_START_DAY = 1


def create_spark_session():
    """Create Spark session configured for Dataproc Serverless"""
    return (
        SparkSession.builder
        .appName("daily-ticket-ingestion")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


def get_next_simulation_date(spark):
    """
    Determine which date to process next by checking the state table.
    Returns the next unprocessed date.
    """
    try:
        state_df = (
            spark.read
            .format("bigquery")
            .option("table", f"{PROJECT_ID}.{DATASET_ID}.ingestion_state")
            .load()
        )
        
        max_date_row = state_df.agg({"simulation_date": "max"}).collect()[0]
        last_date = max_date_row[0]
        
        if last_date:
            next_date = last_date + timedelta(days=1)
            return next_date
    except Exception as e:
        print(f"⚠️ State table not found or empty: {e}")
    
    # Default: Start from Nov 1, 2023
    return datetime(SIMULATION_YEAR, SIMULATION_START_MONTH, SIMULATION_START_DAY).date()


def check_if_date_exists(spark, simulation_date):
    """
    Check if data for this date already exists in tickets_raw.
    Returns True if data exists, False otherwise.
    """
    date_str = simulation_date.strftime("%Y-%m-%d")
    
    try:
        # Check tickets_raw using BigQuery - more efficient than reading table
        query = f"SELECT COUNT(*) as count FROM `{PROJECT_ID}.{DATASET_ID}.tickets_raw` WHERE DATE(created_at) = '{date_str}'"
        
        count_df = (
            spark.read
            .format("bigquery")
            .option("query", query)
            .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
            .load()
        )
        
        row_count = count_df.collect()[0]["count"]
        
        if row_count > 0:
            print(f"⚠️ Data for {date_str} already exists in tickets_raw ({row_count} rows).")
            return True
            
        print(f"✅ Check passed: No existing data for {date_str}")
        return False
        
    except Exception as e:
        # If table doesn't exist yet, it's safe to proceed
        print(f"ℹ️ Could not check exists (first run?): {e}")
        return False


def ingest_day_from_gcs(spark, simulation_date):
    """
    Load one day's CSV from GCS into BigQuery tickets_raw table.
    
    Args:
        spark: SparkSession
        simulation_date: The date to ingest (e.g., 2023-11-01)
    
    Returns:
        DataFrame with the day's data, row count
    """
    date_str = simulation_date.strftime("%Y-%m-%d")
    month_str = f"{simulation_date.month:02d}"
    
    gcs_path = f"gs://{GCS_BUCKET}/raw/{SIMULATION_YEAR}/{month_str}/{date_str}.csv"
    
    print(f"📥 Loading data from: {gcs_path}")
    
    # Read CSV from GCS
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(gcs_path)
    )
    
    # Add metadata columns
    df_with_metadata = (
        df
        .withColumn("_ingestion_ts", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )
    
    row_count = df_with_metadata.count()
    print(f"📊 Loaded {row_count:,} rows for {date_str}")
    
    # Append to tickets_raw table
    (
        df_with_metadata.write
        .format("bigquery")
        .option("table", f"{PROJECT_ID}.{DATASET_ID}.tickets_raw")
        .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
        .mode("append")
        .save()
    )
    
    print(f"✅ Appended {row_count:,} rows to tickets_raw")
    return df_with_metadata, row_count


def transform_new_data(df_with_metadata):
    """
    Transform the newly ingested data to create fact records.
    Operates on the in-memory dataframe, avoiding a BigQuery read.
    """
    print("🔄 Transforming new data...")
    
    fact_df = (
        df_with_metadata
        .withColumn("created_date", to_date("created_at"))
        .withColumn(
            "resolved_date", 
            when(col("resolved_at").isNotNull(), 
                 to_date(to_timestamp(col("resolved_at"), "M/d/yy H:mm")))
        )
        .withColumn("is_resolved", col("resolved_at").isNotNull())
        .withColumn(
            "resolution_hours_clean", 
            when(col("resolved_at").isNotNull(), col("resolution_hours"))
        )
        .select(
            "ticket_id", "created_at", "resolved_at", "created_date", "resolved_date",
            "is_resolved", "resolution_hours_clean", "sla_hours", "sla_breached",
            "priority", "category", "channel", "customer_tier", "status", "agent_id"
        )
    )
    
    return fact_df


def append_to_fact_tickets(fact_df):
    """Append new transformed records to fact_tickets"""
    print(f"📊 Appending {fact_df.count():,} rows to fact_tickets...")
    
    (
        fact_df.write
        .format("bigquery")
        .option("table", f"{PROJECT_ID}.{DATASET_ID}.fact_tickets")
        .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
        .mode("append")
        .save()
    )
    
    print("✅ fact_tickets updated (Append)")


def update_features_daily(fact_df):
    """Calculate daily metrics for the new data and update BOTH features_daily and daily_ops_metrics"""
    print("📊 Updating features_daily & daily_ops_metrics (Incremental)...")
    
    features_df = (
        fact_df
        .groupBy(col("created_date").alias("ops_date"))
        .agg(
            count("*").alias("total_tickets"),
            spark_sum(col("is_resolved").cast("int")).alias("resolved_tickets"),
            spark_sum((~col("is_resolved")).cast("int")).alias("open_tickets"),
            avg("resolution_hours_clean").alias("avg_tat"),
            (
                spark_sum(when(col("sla_breached") == True, 1).otherwise(0)) /
                spark_sum(col("is_resolved").cast("int"))
            ).alias("sla_breach_rate")
        )
    )
    
    # Update features_daily
    (
        features_df.write
        .format("bigquery")
        .option("table", f"{PROJECT_ID}.{DATASET_ID}.features_daily")
        .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
        .mode("append")
        .save()
    )
    
    # Update daily_ops_metrics (Same data)
    (
        features_df.write
        .format("bigquery")
        .option("table", f"{PROJECT_ID}.{DATASET_ID}.daily_ops_metrics")
        .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
        .mode("append")
        .save()
    )
    
    print("✅ features_daily & daily_ops_metrics appended")


def update_backlog_daily(spark):
    """
    Update backlog_daily table.
    NOTE: Backlog is cumulative, so we DO need to scan the full fact table 
    to get accurate counts of currently open tickets across all history.
    """
    print("📊 Updating backlog_daily (Full Scan for Accuracy)...")
    
    # We must read from BQ to get full history of what is open
    # But we can optimize by ensuring we only scan necessary columns
    
    full_fact_df = (
        spark.read
        .format("bigquery")
        .option("table", f"{PROJECT_ID}.{DATASET_ID}.fact_tickets")
        .load()
        .select("created_date", "resolved_at")  # Scan only needed columns
    )

    backlog_df = (
        full_fact_df
        .groupBy(col("created_date").alias("ops_date"))
        .agg(
            spark_sum(when(col("resolved_at").isNull(), 1).otherwise(0)).alias("backlog_size")
        )
    )
    
    (
        backlog_df.write
        .format("bigquery")
        .option("table", f"{PROJECT_ID}.{DATASET_ID}.backlog_daily")
        .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
        .mode("overwrite")
        .save()
    )
    
    print("✅ backlog_daily updated (Full Refresh)")


def update_daily_agent_load(fact_df):
    """Update daily_agent_load with NEW agent performance metrics (Append)"""
    print("📊 Updating daily_agent_load (Incremental)...")
    
    # Tickets assigned per agent per day (from new data)
    tickets_created = (
        fact_df
        .groupBy(col("created_date").alias("ops_date"), "agent_id")
        .agg(
            count("ticket_id").alias("tickets_assigned"),
            spark_sum((~col("is_resolved")).cast("int")).alias("open_tickets_from_day")
        )
    )
    
    # Tickets resolved per agent per day (from new data)
    tickets_resolved = (
        fact_df
        .filter(col("is_resolved"))
        .groupBy(col("resolved_date").alias("ops_date"), "agent_id")
        .agg(
            count("ticket_id").alias("tickets_resolved"),
            avg("resolution_hours_clean").alias("avg_resolution_hours"),
            spark_sum(col("sla_breached").cast("int")).alias("sla_breaches")
        )
    )
    
    # Full outer join for the current day(s)
    agent_load_df = (
        tickets_created
        .join(tickets_resolved, on=["ops_date", "agent_id"], how="full_outer")
        .select(
            "ops_date", "agent_id",
            coalesce(col("tickets_assigned"), lit(0)).alias("tickets_assigned"),
            coalesce(col("tickets_resolved"), lit(0)).alias("tickets_resolved"),
            col("avg_resolution_hours"),
            coalesce(col("sla_breaches"), lit(0)).alias("sla_breaches"),
            coalesce(col("open_tickets_from_day"), lit(0)).alias("open_tickets_from_day")
        )
    )
    
    (
        agent_load_df.write
        .format("bigquery")
        .option("table", f"{PROJECT_ID}.{DATASET_ID}.daily_agent_load")
        .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
        .mode("append")
        .save()
    )
    
    print("✅ daily_agent_load appended")


def update_ticket_sla_risk(fact_df):
    """Update ticket_sla_risk analysis table (Append)"""
    print("📊 Updating ticket_sla_risk (Incremental)...")
    
    sla_risk_df = (
        fact_df
        .select(
            "ticket_id",
            col("created_at").alias("created_at_ts"),
            "created_date",
            "priority", "category", "agent_id",
            "resolution_hours_clean", "sla_hours", "sla_breached"
        )
        .withColumn("day_of_week", date_format(col("created_at_ts"), "E"))
        .withColumn("hour_of_day", hour(col("created_at_ts")))
        .withColumn(
            "is_high_priority",
            when(col("priority").isin("Critical", "High"), True).otherwise(False)
        )
        .withColumn(
            "is_long_resolution",
            when(col("resolution_hours_clean") > col("sla_hours"), True).otherwise(False)
        )
    )
    
    (
        sla_risk_df.write
        .format("bigquery")
        .option("table", f"{PROJECT_ID}.{DATASET_ID}.ticket_sla_risk")
        .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
        .mode("append")
        .save()
    )
    
    print("✅ ticket_sla_risk appended")


def update_ingestion_state(spark, simulation_date, rows_ingested):
    """Record ingestion progress in state table"""
    print("📝 Updating ingestion state...")
    
    from pyspark.sql.types import StructType, StructField, DateType, TimestampType, LongType
    
    schema = StructType([
        StructField("simulation_date", DateType(), False),
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("rows_ingested", LongType(), False)
    ])
    
    state_data = [(simulation_date, datetime.now(), rows_ingested)]
    state_df = spark.createDataFrame(state_data, schema)
    
    (
        state_df.write
        .format("bigquery")
        .option("table", f"{PROJECT_ID}.{DATASET_ID}.ingestion_state")
        .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
        .mode("append")
        .save()
    )
    
    print(f"✅ State updated: {simulation_date} - {rows_ingested} rows")


def main():
    """Main entry point for the daily ingestion job"""
    
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, help="Specific date to process (YYYY-MM-DD)")
    args, _ = parser.parse_known_args()
    
    print("🚀 Starting Daily Incremental Ingestion Job")
    print("=" * 60)
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Determine which date to process
        if args.date:
            simulation_date = datetime.strptime(args.date, "%Y-%m-%d").date()
            print(f"📅 Processing specified date: {simulation_date}")
        else:
            simulation_date = get_next_simulation_date(spark)
            print(f"📅 Auto-detected next date: {simulation_date}")
        
        # Check if we've processed all data
        end_date = datetime(SIMULATION_YEAR, 12, 31).date()
        if simulation_date > end_date:
            print("🎉 All simulation data (Nov-Dec 2023) has been processed!")
            print(f"   Last date: {end_date}")
            return
        
        # Check for existing data first!
        if check_if_date_exists(spark, simulation_date):
            print(f"🛑 Skipping ingestion for {simulation_date} to prevent duplicates.")
            return

        # Step 1: Ingest the day's data
        print("\n" + "=" * 60)
        print("STEP 1: Ingesting raw data from GCS")
        print("=" * 60)
        # We capture the loaded dataframe to reuse it!
        raw_df, rows_ingested = ingest_day_from_gcs(spark, simulation_date)
        
        # Step 2: Update all feature tables
        print("\n" + "=" * 60)
        print("STEP 2: Updating feature tables (Incremental)")
        print("=" * 60)
        
        # Transform the IN-MEMORY dataframe - no re-read from BQ needed!
        fact_df = transform_new_data(raw_df)
        fact_df.cache()  # Cache for reuse
        
        # Append to fact table
        append_to_fact_tickets(fact_df)
        
        # Update derived tables
        update_features_daily(fact_df)
        update_daily_agent_load(fact_df)
        update_ticket_sla_risk(fact_df)
        
        # Backlog still needs full refresh to be accurate
        update_backlog_daily(spark)
        
        fact_df.unpersist()
        
        # Step 3: Update state
        print("\n" + "=" * 60)
        print("STEP 3: Recording ingestion state")
        print("=" * 60)
        update_ingestion_state(spark, simulation_date, rows_ingested)
        
        print("\n" + "=" * 60)
        print(f"🎉 SUCCESS! Processed {simulation_date}")
        print(f"   Rows ingested: {rows_ingested:,}")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        raise e
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
