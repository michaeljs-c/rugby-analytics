from helpers import FileSystemHandler
import os
import polars as pl
from model import GameStats, PlayerStats, Matches

def insert_data_into_table(data: pl.DataFrame, session, table_class):
    for _, row in data.iterrows():
        record = table_class(**row.to_dict())
        session.add(record)
    session.commit()

def load(in_path, session, filehandler: FileSystemHandler):
    game_stats_pl = filehandler.read_partitioned_ds(os.path.join(in_path, 'game_stats.parquet'))
    matches_pl = filehandler.read_partitioned_ds(os.path.join(in_path, 'matches.parquet'))
    player_stats_pl = filehandler.read_partitioned_ds(os.path.join(in_path, 'player_stats.parquet'))
    
    # Insert data into respective tables
    insert_data_into_table(game_stats_pl, session, GameStats)
    insert_data_into_table(matches_pl, session, Matches)
    insert_data_into_table(player_stats_pl, session, PlayerStats)
