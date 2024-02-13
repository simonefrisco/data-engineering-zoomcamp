from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from os import path
import os

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

def get_config_by_env(env : str = 'default'):
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    return ConfigFileLoader(config_path, env)

import pyarrow as pa
import pyarrow.parquet as pq

conf = get_config_by_env('dev')

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = conf.get('GOOGLE_SERVICE_ACC_KEY_FILEPATH')

bucket_name = 'mage-zoomcamp-sf879'
table_name = 'nyc_taxi_data'
root_path = f'{bucket_name}/{table_name}'

@data_loader
def load_from_google_cloud_storage(*args, **kwargs):
    
    gcs = pa.fs.GcsFileSystem()

    data = pq.read_table(
        root_path ,
        filesystem = gcs
    )

    return data.to_pandas()


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
