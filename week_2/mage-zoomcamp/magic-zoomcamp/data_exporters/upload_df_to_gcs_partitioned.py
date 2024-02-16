from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path
import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

def get_config_by_env(env : str = 'default'):
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    return ConfigFileLoader(config_path, env)
conf = get_config_by_env('dev')

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = conf.get('GOOGLE_SERVICE_ACC_KEY_FILEPATH')


bucket_name = 'mage-zoomcamp-sf879'
table_name = 'nyc_taxi_data'
root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data_to_google_cloud_storage(data: DataFrame, **kwargs) -> None:


    data['tpep_pickup_date'] = data['tpep_pickup_datetime'].dt.date

    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table = table, 
        root_path = root_path ,
        partition_cols = ['tpep_pickup_date'],
        filesystem = gcs
    )
