from ingest import ingest
from extract import extract
from transform import transform
from load import load
from helpers import get_date_range, LocalFileSystemHandler, load_config
from model import get_db
import datetime

from abc import abstractmethod

# class ETL:
#     @abstractmethod
#     def run(config: ETLConfig, logger, filehandler):
#         ...

# def ingest(config: ETLConfig):
#     ingest(date_list, config.file_drop, filehandler)

# def extract(config: ETLConfig):
#     ...
# def transform(config: ETLConfig):
#     ...
# def load(config: ETLConfig):
#     ...
def cmd():
    import argparse

    parser = argparse.ArgumentParser(description="Rugby ETL")
    parser.add_argument("--stages", '-s', help="Comma separated ETL stages to run (e.g. ingest,extract,transform,ingest)")
    parser.add_argument("--filehandler", '-f', help="Which file handler to use (local, aws)", default='local')

    args = parser.parse_args()
    stages = args.stages

    config = load_config()

    if args.filehandler == 'local':
        filehandler = LocalFileSystemHandler()
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

if __name__ == '__main__':
    cmd()

    # date_list = get_date_range("2011-01-01", "2011-01-05")
    # config = load_config()
    # db_session = get_db()
    # filehandler = LocalFileSystemHandler()
    
    # ingest(date_list, config.file_drop, filehandler)
    # extract(config.file_drop, config.raw, filehandler)
    # transform(config.raw, config.staging, filehandler)
    # load(config.staging, db_session, filehandler)    

    
