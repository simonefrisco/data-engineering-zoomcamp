

# Week 4

During this week we explore dbt.

# Requirements

3 dataset :
- yellow taxi 2019
- yellow taxi 2020
- fhd

## 1) Upload dataset

- Fetch data from API and upload parquet files in GCS

Make sure to install the dependencies:

```
```
pip install pandas pyarrow google-cloud-storage
set GOOGLE_APPLICATION_CREDENTIALS=cedar-unison-413811-47e15ebf5e92.json
set GCP_GCS_BUCKET=mage-zoomcamp-sf879
```

I Used the script:
https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/03-data-warehouse/extras/web_to_gcs.py

I did some changes in order to :
- upload huge parquet file
- deal with dtype errors

```
python upload_dataset.py
```

## 2) Setup Bigquery tables

- Like week 3 we are going to create 3 tables in Bigquery (native tables):
```

1. Yellow Tripdata
2. Green Tripdata
3. FHV Tripdata
```
python upload_dataset.py
```


## Init dbt

setup dbt project -> https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/dbt_cloud_setup.md

```
dbt build 
or
dbt build --select <model_name>
```

- Generate Documentation

- make sure to install the dependencies in dbt cloud 


packages.yaml
```
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
  - package: dbt-labs/codegen
    version: 0.12.1
```

```
dbt deps
```
```
dbt docs generate
```
