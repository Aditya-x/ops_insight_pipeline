from google.cloud import storage
import pandas as pd
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime


# ---------------------------------
#  python env - python -m venv .venv; . .\.venv\Scripts\Activate.ps1
# ---------------------------------

storage_client = storage.Client()
bucket_name = "ops-tickets-files"
bucket = storage_client.bucket(bucket_name)

def upload_single_partition(bucket, day_date, day_data, target_year, target_month, bucket_name):
    """Upload a single day's partition to GCS"""
    blob_path = f"raw/{target_year}/{target_month:02d}/{day_date}.csv"
    csv_buffer = io.StringIO()
    
    day_data.to_csv(csv_buffer, index=False)
    
    blob = bucket.blob(blob_path)
    blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
    
    return f"gs://{bucket_name}/{blob_path}"

def filter_and_upload_daily_partition_optimized(df, timestamp_col, target_month, target_year, bucket_name, max_workers=10):
    """
    Optimized version with parallel uploads and better memory handling
    """
    # Filter once for the target month
    month_mask = (df[timestamp_col].dt.year == target_year) & (df[timestamp_col].dt.month == target_month)
    filtered_df = df[month_mask].copy()
    
    if filtered_df.empty:
        print(f"No data for {target_year}-{target_month:02d}")
        return
    
    # Create date column for grouping
    filtered_df['temp_date'] = filtered_df[timestamp_col].dt.date
    
    # Parallel upload using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:   
        futures = []
        
        for day_date, day_data in filtered_df.groupby('temp_date'):
            # Drop temp_date before uploading
            day_data_clean = day_data.drop(columns=['temp_date'])
            
            future = executor.submit(
                upload_single_partition,
                bucket, day_date, day_data_clean, target_year, target_month, bucket_name
            )
            futures.append(future)
        
        # Process completed uploads
        for future in as_completed(futures):
            try:
                result = future.result()
                print(f"Successfully uploaded: {result}")
            except Exception as e:
                print(f"Upload failed: {e}")

# Main execution
my_df = pd.read_csv("data/ops_tickets.csv")

# **KEY OPTIMIZATION: Parse datetime ONCE**
my_df["created_at"] = pd.to_datetime(my_df["created_at"])

# Process all months
for month in range(1, 13):
    print(f"\nProcessing month: {month}")
    filter_and_upload_daily_partition_optimized(
        df=my_df,
        timestamp_col="created_at",
        target_month=month,
        target_year=2023,
        bucket_name=bucket_name,
        max_workers=10  # Adjust based on your network/quota
    )

# Upload agents data
agents_data = "data/agents_master.csv"
blob_path = "agents_data/agents_master.csv"
blob = bucket.blob(blob_path)
blob.upload_from_filename(agents_data)
print(f"File uploaded at: {blob_path}")