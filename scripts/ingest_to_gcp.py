from google.cloud import storage
import pandas as pd
import io

#Initializing the client
storage_client = storage.Client()


# Bucket Reference
bucket_name = "ops-analytics-raw"
bucket = storage_client.bucket(bucket_name)

def filter_and_upload_daily_partition(df, timestamp_col, target_month, target_year, bucket_name):
    """
            Filters for a specific month and uploads data partitioned daily to:
            gs://bucket_name/raw/year/month/YYYY-MM-DD.csv
    """
        # 2. Ensure timestamp is datetime and filter by month/year
    df[timestamp_col] = pd.to_datetime(df[timestamp_col])
    month_mask = (df[timestamp_col].dt.year == target_year) & (df[timestamp_col].dt.month == target_month)
    filtered_df = df[month_mask].copy()


        # 3. Create a 'date' only column for grouping
    filtered_df['temp_date'] = filtered_df[timestamp_col].dt.date

    # groupby specific date and upload

    for day_date, day_data in filtered_df.groupby('temp_date'):
        # Example: raw/2026/01/2026-01-03.csv
        blob_path = f"raw/{target_year}/{target_month:02d}/{day_date}.csv"
        csv_buffer = io.StringIO() #convert daily dataframe csv string in memory( no local save needed)

        #drop the temp_date column before upload
        day_data.drop(columns=['temp_date']).to_csv(csv_buffer, index=False)

        # upload the string contect to GCS
        blob = bucket.blob(blob_path)
        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')

        print(f"Successfully uploaded: gs://{bucket_name}/{blob_path}")


my_df = pd.read_csv("../data/ops_tickets.csv")

for month in range(1,13):

    filter_and_upload_daily_partition(
        df=my_df,
        timestamp_col="created_at",
        target_month=month,
        target_year=2023,
        bucket_name=bucket_name
    )


agents_data = "../data/agents_master.csv"

blob_path = "agents_data/agents_master.csv"
blob = bucket.blob(blob_path)
blob.upload_from_filename(agents_data)
print(f"File uploaded at: {blob_path}")