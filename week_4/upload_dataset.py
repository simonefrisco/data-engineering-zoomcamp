import io
import os
from pathlib import Path
import requests
import pandas as pd
from google.cloud import storage

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "mage-zoomcamp-sf879")

yellow_green_dtypes = {
    'VendorID': pd.Int64Dtype(),
    'lpep_pickup_datetime': str,
    'lpep_dropoff_datetime': str,
    'store_and_fwd_flag': str,
    'RatecodeID': pd.Int64Dtype(),
    'PULocationID': pd.Int64Dtype(),
    'DOLocationID': pd.Int64Dtype(),
    'passenger_count': pd.Int64Dtype(),
    'trip_distance': float,
    'fare_amount': float,
    'extra': float,
    'mta_tax': float,
    'tip_amount': float,
    'tolls_amount': float,
    'ehail_fee': float,
    'improvement_surcharge': float,
    'total_amount': float,
    'payment_type': pd.Int64Dtype(),
    'trip_type': pd.Int64Dtype(),
    'congestion_surcharge': float
}


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file,
                            #   timeout=600
                              )


def web_to_gcs(year, service):
    data_dtypes = yellow_green_dtypes
    if service == 'yellow':
        parse_dates = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
    elif service == 'green':
        parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    else:
        parse_dates = ['pickup_datetime', 'dropOff_datetime']
        data_dtypes = {
                'dispatching_base_num': str,
                'PUlocationID': float,
                'DOlocationID': float,
                'SR_Flag': float,
                'Affiliated_base_number': str
            }
        
    
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name
        file_name = str( Path().cwd() / f"data\{service}_tripdata_{year}-{month}.csv.gz")
        url_file =  f"{service}_tripdata_{year}-{month}.csv.gz"
        print(url_file)
        # read it back into a parquet file
        try:
            print('try reading file...')
            df = pd.read_csv(file_name, compression='gzip', low_memory=False, dtype=data_dtypes, parse_dates=parse_dates)
        except Exception as e :
            print('exception : ', e)
            print('downloading file...')
            # download it using requests via a pandas df
            request_url = f"{init_url}{service}/{url_file}"
            r = requests.get(request_url)
            open(file_name, 'wb').write(r.content)
            df = pd.read_csv(file_name, compression='gzip', low_memory=False, dtype=data_dtypes, parse_dates=parse_dates)
        
        df['lpep_pickup_date'] = df[parse_dates[0]].dt.date

        print('shape : ', df.shape)
        print(f"Local: {file_name}")
        if (len(df)>6_000_000) : 
            print('Process in batches of 4_000_000')
            for iter_ in range(0, len(df), 4_000_000):
                end = min(iter_+4_000_000, len(df))
                sub = df.iloc[iter_ : end]
                
                print('process from ', iter_, ' to ', end)
                
                sub_file_name = file_name.replace('.csv.gz', f'-{iter_}-{end}.parquet')
                sub_url = url_file.replace('.csv.gz', f'-{iter_}-{end}.parquet')
                
                sub.to_parquet(sub_file_name, engine='pyarrow')
                upload_to_gcs(
                    BUCKET, f"taxi_data_per_month/{service}/{sub_url}", sub_file_name
                )                
                print(f"GCS : taxi_data_per_month/{service}/{sub_url}")
        else:
            file_name = file_name.replace('.csv.gz', '.parquet')
            url_file = url_file.replace('.csv.gz', '.parquet')
            df.to_parquet(file_name, engine='pyarrow')
            print(f"Parquet: {file_name}")

            # upload it to gcs 
            upload_to_gcs(BUCKET, f"taxi_data_per_month/{service}/{url_file}", file_name)
            # remove file :
            # os.remove(file_name.replace('.parquet', '.csv.gz'))
            # os.remove(file_name)
            print(f"GCS: taxi_data_per_month/{service}/{url_file}")

        del df


# web_to_gcs('2019', 'green')
# web_to_gcs('2020', 'green')
# web_to_gcs('2019', 'yellow')
# web_to_gcs('2020', 'yellow')
web_to_gcs('2019', 'fhv')


# %%
# generate a dict from this data:
#  VendorID               630918 non-null  int64  
#  1   lpep_pickup_datetime   630918 non-null  object 
#  2   lpep_dropoff_datetime  630918 non-null  object 
#  3   store_and_fwd_flag     630918 non-null  object 
#  4   RatecodeID             630918 non-null  int64  
#  5   PULocationID           630918 non-null  int64  
#  6   DOLocationID           630918 non-null  int64  
#  7   passenger_count        630918 non-null  int64  
#  8   trip_distance          630918 non-null  float64
#  9   fare_amount            630918 non-null  float64
#  10  extra                  630918 non-null  float64
#  11  mta_tax                630918 non-null  float64
#  12  tip_amount             630918 non-null  float64
#  13  tolls_amount           630918 non-null  float64
#  14  ehail_fee              0 non-null       float64
#  15  improvement_surcharge  630918 non-null  float64
#  16  total_amount           630918 non-null  float64
#  17  payment_type           630918 non-null  int64  
#  18  trip_type              630918 non-null  int64  
#  19  congestion_surcharge   84538 non-null   float64

# data = {
#     'VendorID': pd.Int64Dtype(),
#     'lpep_pickup_datetime': str,
#     'lpep_dropoff_datetime': str,
#     'store_and_fwd_flag': str,
#     'RatecodeID': pd.Int64Dtype(),
#     'PULocationID': pd.Int64Dtype(),
#     'DOLocationID': pd.Int64Dtype(),
#     'passenger_count': pd.Int64Dtype(),
#     'trip_distance': float,
#     'fare_amount': float,
#     'extra': float,
#     'mta_tax': float,
#     'tip_amount': float,
#     'tolls_amount': float,
#     'ehail_fee': float,
#     'improvement_surcharge': float,
#     'total_amount': float,
#     'payment_type': pd.Int64Dtype(),
#     'trip_type': pd.Int64Dtype(),
#     'congestion_surcharge': float
# }
# # %%
# # generate a dict from this data:
# # 0   dispatching_base_num    1475564 non-null  object 
# #  1   pickup_datetime         1475564 non-null  object 
# #  2   dropOff_datetime        1475564 non-null  object 
# #  3   PUlocationID            1474460 non-null  float64
# #  4   DOlocationID            1474460 non-null  float64
# #  5   SR_Flag                 0 non-null        float64
# #  6   Affiliated_base_number  1468253 non-null  object 

# data = {
#     'dispatching_base_num': str,
#     'PUlocationID': float,
#     'DOlocationID': float,
#     'SR_Flag': float,
#     'Affiliated_base_number': str
# }