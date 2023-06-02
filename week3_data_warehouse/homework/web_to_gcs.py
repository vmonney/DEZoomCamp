import os
import requests
from io import BytesIO
import gzip
from google.cloud import storage
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor


"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""
# tag / fhv
# services = ['fhv', 'green', 'yellow']
init_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
# get the env variable and set it to the bucket name if not exists
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc_data_lake_stoked-utility-387710")

table_schema_fhv = pa.schema(
    [
        ("dispatching_base_num", pa.string()),
        ("pickup_datetime", pa.timestamp("s")),
        ("dropOff_datetime", pa.timestamp("s")),
        ("PUlocationID", pa.int64()),
        ("DOlocationID", pa.int64()),
        ("SR_Flag", pa.int64()),
        ("Affiliated_base_number", pa.string()),
    ]
)

table_schema_green = pa.schema(
    [
        ("VendorID", pa.string()),
        ("lpep_pickup_datetime", pa.timestamp("s")),
        ("lpep_dropoff_datetime", pa.timestamp("s")),
        ("store_and_fwd_flag", pa.string()),
        ("RatecodeID", pa.int64()),
        ("PULocationID", pa.int64()),
        ("DOLocationID", pa.int64()),
        ("passenger_count", pa.int64()),
        ("trip_distance", pa.float64()),
        ("fare_amount", pa.float64()),
        ("extra", pa.float64()),
        ("mta_tax", pa.float64()),
        ("tip_amount", pa.float64()),
        ("tolls_amount", pa.float64()),
        ("ehail_fee", pa.float64()),
        ("improvement_surcharge", pa.float64()),
        ("total_amount", pa.float64()),
        ("payment_type", pa.int64()),
        ("trip_type", pa.int64()),
        ("congestion_surcharge", pa.float64()),
    ]
)

table_schema_yellow = pa.schema(
    [
        ("VendorID", pa.string()),
        ("tpep_pickup_datetime", pa.timestamp("s")),
        ("tpep_dropoff_datetime", pa.timestamp("s")),
        ("passenger_count", pa.int64()),
        ("trip_distance", pa.float64()),
        ("RatecodeID", pa.string()),
        ("store_and_fwd_flag", pa.string()),
        ("PULocationID", pa.int64()),
        ("DOLocationID", pa.int64()),
        ("payment_type", pa.int64()),
        ("fare_amount", pa.float64()),
        ("extra", pa.float64()),
        ("mta_tax", pa.float64()),
        ("tip_amount", pa.float64()),
        ("tolls_amount", pa.float64()),
        ("improvement_surcharge", pa.float64()),
        ("total_amount", pa.float64()),
        ("congestion_surcharge", pa.float64()),
    ]
)


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_file(local_file)


def web_to_gcs(year, service):
    futures = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        for i in range(12):
            futures.append(executor.submit(process_month, year, service, i))


def process_month(year, service, i):
    month = str(i + 1).zfill(2)
    file_name = f"{service}_tripdata_{year}-{month}.csv.gz"
    request_url = f"{init_url}{service}/{file_name}"
    try:
        r = requests.get(request_url)
        r.raise_for_status()  # Will raise HTTPError for bad status codes
    except requests.exceptions.RequestException as e:
        # handle error, possibly retry or log the failed URL for later review
        print(f"Failed to download {request_url}: {e}")
        return None

    # Decompress the gzip content
    decompressed_content = gzip.decompress(r.content)
    csv_buffer = BytesIO(decompressed_content)
    parquet_buffer = format_to_parquet(csv_buffer, service)
    parquet_buffer.seek(0)
    try:
        upload_to_gcs(
            BUCKET,
            f"NYC_taxi_trips/{service}/{file_name.replace('.csv.gz', '.parquet')}",
            parquet_buffer,
        )
    except Exception as e:
        print(f"Failed to upload {file_name} to GCS: {e}")
        return None
    print(f"filename : {file_name}")


def format_to_parquet(src_data, service):
    table = pv.read_csv(src_data)
    if service == "yellow":
        table = table.cast(table_schema_yellow)
    elif service == "green":
        table = table.cast(table_schema_green)
    elif service == "fhv":
        table = table.cast(table_schema_fhv)
    else:
        raise ValueError(f"Specify the schema for the {service} service")
    buf = BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    return buf


web_to_gcs("2019", "yellow")
web_to_gcs("2020", "yellow")
web_to_gcs("2019", "green")
web_to_gcs("2020", "green")
web_to_gcs("2019", "fhv")
