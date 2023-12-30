from helpers import FileSystemHandler
import os
import polars as pl

def preprocess_game_stats(data: pl.DataFrame):
    return data
def preprocess_matches(df: pl.DataFrame):
    df = df.with_columns(
    pl.col('date').str.to_datetime(format='%Y-%m-%dT%H:%MZ')
)
    return df
def preprocess_player_stats(data: pl.DataFrame):
    return data

# from dataclasses import dataclass
# from typing import Callable

# @dataclass
# class DataSource:
#     name: str
#     preprocess: Callable
#     file_type: str = 'parquet'

# datasources = [
#     DataSource('game_stats', preprocess_game_stats),
#     DataSource('matches', preprocess_matches),
#     DataSource('player_stats', preprocess_player_stats)
# ]

def transform(in_path, out_path, filehandler: FileSystemHandler):
    game_stats = filehandler.read_partitioned_ds(os.path.join(in_path, 'game_stats.parquet')).pipe(preprocess_game_stats)
    matches = filehandler.read_partitioned_ds(os.path.join(in_path, 'matches.parquet')).pipe(preprocess_matches)
    player_stats = filehandler.read_partitioned_ds(os.path.join(in_path, 'player_stats.parquet')).pipe(preprocess_player_stats)

    filehandler.write_partitioned_ds(
        game_stats, 
        os.path.join(out_path, 'game_stats.parquet'),
        'parquet'
    )
    filehandler.write_partitioned_ds(
        matches, 
        os.path.join(out_path, 'matches.parquet'),
        'parquet'
    )
    filehandler.write_partitioned_ds(
        player_stats, 
        os.path.join(out_path, 'player_stats.parquet'),
        'parquet'
    )

    
