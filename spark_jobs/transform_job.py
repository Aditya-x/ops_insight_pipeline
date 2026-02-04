import os
import json
from pyspark.sql.functions import (
    col, to_date, when, avg, dayofweek, date_format, hour, count, sum as spark_sum,
    countDistinct, lit, expr, coalesce, to_timestamp
)
from pyspark.sql import SparkSession
from google.cloud import bigquery
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load ADC credentials
adc_path = r"C:\Users\91876\AppData\Roaming\gcloud\application_default_credentials.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = adc_path

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
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .config("spark.hadoop.google.cloud.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE")
    .config("spark.hadoop.google.cloud.auth.service.account.email", creds.get("client_email", ""))
    .config("spark.hadoop.google.cloud.auth.service.account.private.key.id", creds.get("private_key_id", ""))
    .config("spark.hadoop.google.cloud.auth.service.account.private.key", creds.get("private_key", "").replace("\\n", "\n"))
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", adc_path)
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    # Performance optimizations
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.shuffle.partitions", "200")  # Adjust based on data size
    .getOrCreate()
)

#------------------------------------------------------------------
# CONFIG
#------------------------------------------------------------------
PROJECT_ID = "biz-ops-031099"
DATASET_ID = "ops_analytics"
TABLE_ID = "tickets_raw"
TEMP_GCS_BUCKET = "temp-gcs-bucket-spark01"

#-------------------------------------------
# Read and Transform in One Pass
#-------------------------------------------

raw_df = (
    spark.read
    .format("bigquery")
    .option("table", f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    .load()
)

# **OPTIMIZATION 1: Cache the base transformed dataframe**
fact_tickets_df = (
    raw_df
    .withColumn("created_date", to_date("created_at"))
    .withColumn("resolved_date", when(col("resolved_at").isNotNull(), to_date(to_timestamp(col("resolved_at"), "M/d/yy H:mm"))))
    .withColumn("is_resolved", col("resolved_at").isNotNull())
    .withColumn("resolution_hours_clean", when(col("resolved_at").isNotNull(), col("resolution_hours")))
    .select(
        "ticket_id",
        "created_at",
        "resolved_at",
        "created_date",
        "resolved_date",
        "is_resolved",
        "resolution_hours_clean",
        "sla_hours",
        "sla_breached",
        "priority",
        "category",
        "channel",
        "customer_tier",
        "status",
        "agent_id",
    )
    .cache()  # Cache since we reuse this 4 times
)

# **OPTIMIZATION 2: Single aggregation to create all daily metrics**
# Instead of 3 separate groupBy operations, do it once
daily_aggregates = (
    fact_tickets_df
    .groupBy(col("created_date").alias("ops_date"))
    .agg(
        # Features daily metrics
        count("*").alias("total_tickets"),
        spark_sum(col("is_resolved").cast("int")).alias("resolved_tickets"),
        spark_sum((~col("is_resolved")).cast("int")).alias("open_tickets"),
        avg("resolution_hours_clean").alias("avg_tat"),
        (
            spark_sum(when(col("sla_breached") == True, 1).otherwise(0)) /
            spark_sum(col("is_resolved").cast("int"))
        ).alias("sla_breach_rate"),
        
        # Backlog metrics (from backlog_table_df)
        spark_sum(when(col("resolved_at").isNull(), 1).otherwise(0)).alias("backlog_size"),
        
        # Agent count for tickets_per_agent calculation
        countDistinct("agent_id").alias("distinct_agents")
    )
    .cache()  # Cache since used in multiple writes
)

# Extract separate dataframes from the single aggregation
features_daily_df = daily_aggregates.select(
    "ops_date", "total_tickets", "resolved_tickets", "open_tickets", 
    "avg_tat", "sla_breach_rate"
)

backlog_table_df = daily_aggregates.select("ops_date", "backlog_size")

# Agent daily load - comprehensive design combining created and resolved perspectives
# Tickets ASSIGNED (created) per agent per day
tickets_created_df = (
    fact_tickets_df
    .groupBy(col("created_date").alias("ops_date"), "agent_id")
    .agg(
        count("ticket_id").alias("tickets_assigned"),
        spark_sum((~col("is_resolved")).cast("int")).alias("open_tickets_from_day")
    )
)

# Tickets RESOLVED per agent per day
tickets_resolved_df = (
    fact_tickets_df
    .filter(col("is_resolved"))
    .groupBy(col("resolved_date").alias("ops_date"), "agent_id")
    .agg(
        count("ticket_id").alias("tickets_resolved"),
        avg("resolution_hours_clean").alias("avg_resolution_hours"),
        spark_sum(col("sla_breached").cast("int")).alias("sla_breaches")
    )
)

# FULL OUTER JOIN to get complete picture for each agent per day
daily_agent_load_df = (
    tickets_created_df
    .join(tickets_resolved_df, on=["ops_date", "agent_id"], how="full_outer")
    .select(
        "ops_date",
        "agent_id",
        coalesce(col("tickets_assigned"), lit(0)).alias("tickets_assigned"),
        coalesce(col("tickets_resolved"), lit(0)).alias("tickets_resolved"),
        col("avg_resolution_hours"),
        coalesce(col("sla_breaches"), lit(0)).alias("sla_breaches"),
        coalesce(col("open_tickets_from_day"), lit(0)).alias("open_tickets_from_day")
    )
)


ticket_sla_risk_df = (
    fact_tickets_df
    .select(
        "ticket_id",
        col("created_at").alias("created_at_ts"),
        col("created_date"),
        "priority",
        "category",
        "agent_id",
        "resolution_hours_clean",
        "sla_hours",
        "sla_breached"
    )
    .withColumn(
        "day_of_week",
        date_format(col("created_at_ts"), "E")  # Mon, Tue, ...
    )
    .withColumn(
        "hour_of_day",
        hour(col("created_at_ts"))
    )
    .withColumn(
        "is_high_priority",
        when(col("priority").isin("Critical", "High"), True).otherwise(False)
    )
    .withColumn(
        "is_long_resolution",
        when(col("resolution_hours_clean") > col("sla_hours"), True).otherwise(False)
    )
)

#-------------------------------------------
# **OPTIMIZATION 3: Parallel writes using threads**
#-------------------------------------------

def write_to_bq(df, table_name):
    """Helper function to write dataframe to BigQuery"""
    full_table = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    df.write \
        .format("bigquery") \
        .option("table", full_table) \
        .option("temporaryGcsBucket", TEMP_GCS_BUCKET) \
        .mode("overwrite") \
        .save()
    return f"✅ Written: {table_name}"

# Write all tables in parallel
tables_to_write = [
    # (fact_tickets_df, "fact_tickets"),
    # (features_daily_df, "features_daily"),
    # (backlog_table_df, "backlog_daily"),
    # (daily_agent_load_df, "daily_agent_load"),
    (ticket_sla_risk_df, "ticket_sla_risk")
]

with ThreadPoolExecutor(max_workers=4) as executor:
    futures = [executor.submit(write_to_bq, df, name) for df, name in tables_to_write]
    
    for future in as_completed(futures):
        print(future.result())

# Unpersist cached dataframes
fact_tickets_df.unpersist()
daily_aggregates.unpersist()

#-------------------------------------------
# **OPTIMIZATION 4: Combined SQL execution in BigQuery**
#-------------------------------------------

client = bigquery.Client()
