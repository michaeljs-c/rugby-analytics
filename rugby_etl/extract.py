# def get_field(field, text):
#     right_start = text.find(">", text.find(">", text.find(field)) + 1) + 1
#     right_end = text.find("<", right_start)

#     left_end = text.rfind("<", 0, text.rfind("<", 0, text.find(field)))
#     left_start = text.rfind(">", 0, left_end) + 1

#     return text[left_start: left_end], text[right_start: right_end]

# with open("out.txt") as f:  
#     print(get_field("Red Cards", f.read()))

import os
import polars as pl
from helpers import FileSystemHandler, clean_col


def get_players(input_data: dict) -> list:
    parsed_blocks = (
        input_data['gamePackage']['matchLineUp']['home']['team'] +
        input_data['gamePackage']['matchLineUp']['home']['reserves'] +
        input_data['gamePackage']['matchLineUp']['away']['team'] +
        input_data['gamePackage']['matchLineUp']['away']['reserves']
    )
    data = []
    home_id = input_data['gamePackage']['gameStrip']['teams']['home']['id']
    away_id = input_data['gamePackage']['gameStrip']['teams']['away']['id']

    for stat in parsed_blocks:
        player = {}
        for key, val in stat.items():
            if key not in ['uid', 'guid', 'url', 'subToolTip', 'eventTimes']:
                if isinstance(val, dict):
                    player[clean_col(val['name'])] = val['value']
                else:
                    player[clean_col(key)] = val
        player['gameid'] = input_data['gamePackage']['meta']['gameId']
        if stat['homeAway'] == 'home':
            player['teamid'] = home_id
        else:
            player['teamid'] = away_id
        data.append(player)
    return data

def get_match_details(input_data: dict) -> list:
    matches = []
    group = input_data['schedule']['groups']

    if len(group) != 0:
        for game in group[0]['complete']:
            data = {}
            data['date'] = game['date']
            data['gameid'] = game['gameId']
            data['home_id'] = game['homeTeam']['id']
            data['away_id'] = game['awayTeam']['id']
            data['home_team'] = game['homeTeam']['name']
            data['away_team'] = game['awayTeam']['name']
            data['home_score'] = game['homeTeam']['score']
            data['away_score'] = game['awayTeam']['score']
            data['leagueid'] = game['leagueId']
            data['league'] = game['league']
            data['attendance'] = game['attendance']
            data['location'] = game['location']
            
            matches.append(data)
    return matches
    # data['title'] = input_data['gamePackage']['analytics']['chartbeat']['title']
    # data['country'] = input_data['gamePackage']['country']

    # md = input_data['gamePackage']['matchDetails']
    # for key, val in md.items():
    #     if key != 'text':
    #         data[clean_col(key)] = val

    # return data

def get_game_stats(input_data: dict) -> list:
    teams = []

    for team in ['home', 'away']:
        team_data = {}
        team_data['teamid'] = input_data['gamePackage']['gameStrip']['teams'][team]['id']
        team_data['gameid'] = input_data['gamePackage']['meta']['gameId']
        team_data['team'] = input_data['gamePackage']['matchStats'][team]['name']    

        for stat in input_data['gamePackage']['matchStats']['table']:
            team_data[clean_col(stat['text'])] = stat[f'{team}Value']
        for stat in input_data['gamePackage']['matchStats']['dataVis']:
            team_data[clean_col(stat['text'])] = stat[f'{team}Value']
        for stat in input_data['gamePackage']['matchEvents']['col'][0][0]['data']:
            team_data[clean_col(stat['text'])] = stat[f'{team}Value']
        for stat in input_data['gamePackage']['matchEvents']['col'][1][1]['data']:
            team_data[clean_col(stat['text'])] = stat[f'{team}Value']
        for stat in input_data['gamePackage']['matchAttacking']['col'][1][0]['data']:
            team_data[clean_col(stat['text'])] = stat[f'{team}Value']
        for stat in input_data['gamePackage']['matchDefending']['col'][0]:
            name = clean_col(stat['data']['text'])
            team_data[f'total_{name}'] = stat['data'][f'{team}Total']
            team_data[f'won_{name}'] = stat['data'][f'{team}Won']

        stat = input_data['gamePackage']['matchDiscipline']['col'][0][0]['data']
        name = clean_col(stat['text'])
        team_data[f'total_{name}'] = stat[f'{team}Total']

        for stat in input_data['gamePackage']['matchDiscipline']['col'][1][0]['data']:
            team_data[clean_col(stat['text'])] = stat[f'{team}Value']
            
        teams.append(team_data)

    return teams

def extract(in_path: str, out_path: str, filehandler: FileSystemHandler) -> None:
    matches = []
    game_stats = []
    players = []
    
    for file in filehandler.list_dir(in_path):
        data = filehandler.read_json(os.path.join(in_path, file))
        if "schedule" in file:
            matches.extend(get_match_details(data))
        elif "game" in file:
            game_stats.extend(get_game_stats(data))
            players.extend(get_players(data))

    filehandler.write_partitioned_ds(
        pl.DataFrame(game_stats), 
        os.path.join(out_path, 'game_stats.parquet'),
        'parquet'
    )
    filehandler.write_partitioned_ds(
        pl.DataFrame(matches), 
        os.path.join(out_path, 'matches.parquet'),
        'parquet'
    )
    filehandler.write_partitioned_ds(
        pl.DataFrame(players), 
        os.path.join(out_path, 'player_stats.parquet'),
        'parquet'
    )
    # df = pl.DataFrame(matches)
    # df = df.with_columns(pl.col("date").str.to_datetime("%Y-%m-%dT%H:%M%Z")).with_columns(
    #         pl.col('date').dt.year().alias('year'),
    #         pl.col('date').dt.month().alias('month'),
    #         pl.col('date').dt.date().alias('date')
    #     )
    # df.write_parquet(out_path + "text.parquet", use_pyarrow=True,
    # pyarrow_options={"partition_cols": ["year", "month", "date"]})
    # filehandler.write_partitioned_ds(df, out_path + "test.parquet", ["year", "month", "date"])

