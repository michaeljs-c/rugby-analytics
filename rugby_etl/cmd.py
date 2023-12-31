import argparse
from helpers import load_config
from etl import etl


def cmd():
    parser = argparse.ArgumentParser(description="Rugby ETL")
    parser.add_argument("--stages", '-s', help="Comma separated ETL stages to run (e.g. ingest,extract,transform,ingest)")
    parser.add_argument("--filehandler", '-f', help="Which file handler to use (local, aws)", default='local', choices=['local', 'aws'])

    args = parser.parse_args()
    stages = args.stages

    config = load_config()

    etl(config, stages, args.filehandler)

if __name__ == '__main__':
    cmd()
