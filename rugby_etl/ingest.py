import requests
import datetime
import time
import json
import os
import logging
import random
from typing import List
from helpers import FileSystemHandler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
file_handler = logging.FileHandler('log.log')
logger.addHandler(file_handler)

# user agent required in the request body
HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

def get_parsed_block(text: str) -> dict:
    for line in text.split("\n"):
        if "window.__INITIAL_STATE__ = " in line:
            day_data = line[line.index("=")+1: ]
            break
    return json.loads(day_data.replace(";", ""))

def ingest(date_list: List[datetime.datetime], data_root: str, filehandler: FileSystemHandler):
    files = filehandler.list_dir(data_root)
    counter = 0

    for date in date_list:
        counter += 1
        if counter % 100 == 0:
            logger.info("Sleeping for 1 minute")
            time.sleep(60)

        date_string = date.strftime('%Y%m%d')
        if f'{date_string}_schedule.json' in files:
            logger.info(f"Skipping {date_string}_schedule.json. File already exists")
            with open(os.path.join(data_root, f'{date_string}_schedule.json'), 'r') as f:
                parsed_day_data = json.loads(f.read())
        else:
            url = f"https://www.espn.com/rugby/fixtures/_/date/{date_string}"
            r = requests.get(url, headers=HEADERS)
            time.sleep(random.randint(1,5) + random.random())

            parsed_day_data = get_parsed_block(r.text)
            filehandler.write_json(f'{data_root}/{date_string}_schedule.json', parsed_day_data)

        group = parsed_day_data['schedule']['groups']
        if len(group) == 0:
            logger.info(f'No games on {date_string}')
            continue

        for game in group[0]['complete']:
            gameid = game['gameId']

            if f'{date_string}_{gameid}_game_raw.json' in files:
                logger.info(f"Skipping {date_string}_{gameid}_game_raw.json. File already exists")
                continue
        
            href = game['soccerRecord']['href']
            if "/rugby/" not in href:
                logger.info(f"Skipping {date_string}_{gameid}_game_raw.json. No data found")
                continue
            url = f"https://www.espn.com{href}"
            
            r = requests.get(url, headers=HEADERS)
            time.sleep(random.randint(1,5) + random.random())

            parsed_data = get_parsed_block(r.text)
            filehandler.write_json(f'{data_root}/{date_string}_{gameid}_game_raw.json', parsed_data)
            
