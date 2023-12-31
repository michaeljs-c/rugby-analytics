from ingest import ingest
from extract import extract
from transform import transform
from load import load
from helpers import get_date_range, LocalFileSystemHandler, AWSFileSystemHandler, ETLConfig, FileSystemHandler
from model import get_db
import datetime

# from abc import abstractmethod
# class ETL:
#     def __init__(self, config: ETLConfig):
#         self.config = config
#     @abstractmethod
#     def run(config: ETLConfig, logger, filehandler):
#         ...
#     def ingest(config: ETLConfig):
#         ...
#     def extract(config: ETLConfig):
#         ...
#     def transform(config: ETLConfig):
#         ...
#     def load(config: ETLConfig):
#         ...

def etl(config: ETLConfig, stages: str, filehandler: FileSystemHandler):
    if filehandler == 'local':
        filehandler = LocalFileSystemHandler()
    elif filehandler == 'aws': 
        filehandler = AWSFileSystemHandler()
    else:
        raise ValueError("Select a valid filehandler")
    
    if 'ingest' in stages:
        if config.ingest_from_date and config.ingest_to_date:
            date_list = get_date_range(config.ingest_from_date, config.ingest_to_date)
        else:
            # default to ingest ESPN data from yesterday
            date_list = [datetime.datetime.today() - datetime.timedelta(days=1)] 
        ingest(date_list, config.file_drop, filehandler)
    if 'extract' in stages:
        extract(config.file_drop, config.raw, filehandler)
    if 'transform' in stages:
        transform(config.raw, config.staging, filehandler)
    if 'load' in stages:
        db_session = get_db(config.database_url)
        load(config.staging, db_session, filehandler)   

