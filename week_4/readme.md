

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

pip install pandas pyarrow google-cloud-storage
set GOOGLE_APPLICATION_CREDENTIALS=cedar-unison-413811-47e15ebf5e92.json
set GCP_GCS_BUCKET=mage-zoomcamp-sf879

python upload_dataset.py

Run the upload_dataset.py script

## Init dbt



adding this file : 



# Setup DBT

