import polars as pl
import json
import pyarrow.dataset as ds
from abc import ABC
import os
import datetime
from pydantic_settings import BaseSettings

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.conf")

class ETLConfig(BaseSettings):
    file_drop: str
    raw: str
    staging: str
    database_url: str
    ingest_from_date: str = None
    ingest_to_date: str = None

    class Config:
        env_file = CONFIG_PATH


def load_config(config_path=CONFIG_PATH) -> ETLConfig:
    return ETLConfig(_env_file=config_path)

def clean_col(col: str) -> str:
    return col.strip().replace(" ", "_").lower()

def get_date_range(start: str, end: str):
    start_obj = datetime.datetime.strptime(start, "%Y-%m-%d")
    end_obj = datetime.datetime.strptime(end, "%Y-%m-%d")
    date_list = [start_obj + datetime.timedelta(days=x) for x in range((end_obj - start_obj).days)]

    return date_list

def _read_partitioned_ds(path: str) -> pl.DataFrame:
    df = pl.scan_pyarrow_dataset(
        ds.dataset(path)
    ).collect()
    return df

def _write_partitioned_ds(df: pl.DataFrame, path: str, format: str, partition_cols: list = None) -> None:
    ds.write_dataset(
        df.to_arrow(),
        path,
        format=format,
        partitioning=partition_cols
    )
    # df.write_parquet(path, use_pyarrow=True,
    # pyarrow_options={"partition_cols": ["year", "month", "date"]})

class FileSystemHandler(ABC):
    @staticmethod
    def read_json(path: str) -> None:
        ...        
    @staticmethod
    def write_json(path: str, data: dict) -> None:
        ...
    @staticmethod
    def write_partitioned_ds(df: pl.DataFrame, path: str, format: str, partition_cols: list = None) -> None:
        ...  
    @staticmethod
    def read_partitioned_ds(path: str) -> pl.DataFrame:
        ...    
    @staticmethod
    def list_dir(path: str) -> list:
        ...

class AWSFileSystemHandler:
    ...

class LocalFileSystemHandler:
    @staticmethod
    def read_json(path: str) -> dict:
        with open(path, 'r') as f:
            data = json.loads(f.read())    
        return data
    @staticmethod
    def write_json(path: str, data: dict) -> None:
        with open(path, 'w') as f:
            f.write(json.dumps(data, indent=4))
    @staticmethod
    def write_partitioned_ds(df: pl.DataFrame, path: str, format: str, partition_cols: list = None) -> None:
        return _write_partitioned_ds(df, path, format, partition_cols)
    @staticmethod
    def read_partitioned_ds(path: str) -> pl.DataFrame:
        return _read_partitioned_ds(path)    
    @staticmethod
    def list_dir(path: str) -> list:
        return os.listdir(path)
    
