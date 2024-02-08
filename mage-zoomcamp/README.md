## Data Loader
import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    urls = [
        'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-10.csv.gz',
        'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-11.csv.gz',
        'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-12.csv.gz'
        ]        

    taxi_dtypes = {
                    'VendorID': pd.Int64Dtype(),
                    'passenger_count': pd.Int64Dtype(),
                    'trip_distance': float,
                    'RatecodeID': pd.Int64Dtype(),
                    'store_and_fwd_flag': str,
                    'PULocationID': pd.Int64Dtype(),
                    'DOLocationID': pd.Int64Dtype(),
                    'payment_type': pd.Int64Dtype(),
                    'fare_amount': float,
                    'extra': float,
                    'mta_tax': float,
                    'tip_amount': float,
                    'tolls_amount': float,
                    'improvement_surcharge': float,
                    'total_amount': float,
                    'congestion_surcharge': float
                }

    parse_dates=['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    
    # empty df
    data = pd.DataFrame()

    #loop the urls
    for url in urls:
        df = pd.read_csv(url, sep=",", compression="gzip", dtype=taxi_dtypes, parse_dates=parse_dates)
        data = pd.concat([data, df], ignore_index=True)

    return data
    
    #the original
    #return pd.read_csv(url1, sep=",", compression="gzip", dtype=taxi_dtypes, parse_dates=parse_dates)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'


## Transformer
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    # remove zero passengers
    print("Rows with zero passengers: ", data['passenger_count'].isin([0]).sum())
    data=data[data['passenger_count']>0]

    # remove zero distance
    print("Rows with zero distance: ", data['trip_distance'].isin([0]).sum())
    data=data[data['trip_distance']>0]

    # add date column
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    # rename headers
    columns_original = data.columns
    data.columns = (data.columns
                .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
                .str.lower()
                )
    columns_new = data.columns

    num_dif = 0
    for i in range(len(columns_original)):
        if columns_original[i] != columns_new[i]:
            num_dif += 1

    print("number of changes is ", num_dif)



    print("unique vendor ID are ", list(data['vendor_id'].unique()))
    
    return data


@test
def test_output(output, *args):
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passengers'
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with zero distance'
    assert 'vendor_id' in output.columns, "The column 'vendor_id' is missing"


## Data Exporter
import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/static-mediator-411916-870297e261d6.json"

bucket_name = 'mage-zoomcamp-belaz'
project_id = 'static-mediator-411916'

table_name = "green_taxi"

root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data(data, *args, **kwargs):
    
    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )
