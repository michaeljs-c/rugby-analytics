from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta

Base = declarative_base()

class GameStats(Base):
    __tablename__ = 'game_stats'

    teamid = Column(Integer, primary_key=True)
    gameid = Column(Integer, primary_key=True)
    team = Column(String)
    runs = Column(Integer)
    meters_run = Column(Integer)
    clean_breaks = Column(Integer)
    tackles = Column(Integer)
    missed_tackles = Column(Integer)
    turnover_knock_on = Column(Integer)
    possession = Column(Float)
    territory = Column(Float)
    tries = Column(Integer)
    conversion_goals = Column(Integer)
    penalty_goals = Column(Integer)
    kick_percent_success = Column(String)
    kicks_from_hand = Column(Integer)
    passes = Column(Integer)
    possession_1h_2h = Column(String)
    territory_1h_2h = Column(String)
    defenders_beaten = Column(Integer)
    offload = Column(Integer)
    rucks_won = Column(Integer)
    mauls_won = Column(Integer)
    turnovers_conceded = Column(Integer)
    total_scrums_won = Column(Integer)
    won_scrums_won = Column(Integer)
    total_lineouts_won = Column(Integer)
    won_lineouts_won = Column(Integer)
    total_penalties_conceded = Column(Integer)
    red_cards = Column(Integer)
    yellow_cards = Column(Integer)
    total_free_kicks_conceded = Column(Integer)

    __table_args__ = (
        PrimaryKeyConstraint('teamid', 'gameid'),
    )

class Matches(Base):
    __tablename__ = 'matches'

    gameid = Column(Integer, primary_key=True)
    date = Column(DateTime)
    home_id = Column(Integer)
    away_id = Column(Integer)
    home_team = Column(String)
    away_team = Column(String)
    home_score = Column(Integer)
    away_score = Column(Integer)
    leagueid = Column(Integer)
    league = Column(String)
    attendance = Column(Integer)
    location = Column(String)

class PlayerStats(Base):
    __tablename__ = 'player_stats'

    id = Column(Integer, primary_key=True)
    gameid = Column(Integer, primary_key=True)
    teamid = Column(Integer)
    name = Column(String)
    number = Column(Integer)
    position = Column(String)
    captain = Column(Boolean)
    subbed = Column(Boolean)
    homeaway = Column(String)
    onpitch = Column(Boolean)
    wasactive = Column(Boolean)
    tries = Column(Integer)
    try_assists = Column(Integer)
    points = Column(Integer)
    kicks = Column(Integer)
    passes = Column(Integer)
    runs = Column(Integer)
    meters_run = Column(Integer)
    clean_breaks = Column(Integer)
    defenders_beaten = Column(Integer)
    offload = Column(Integer)
    lineoutwonsteal = Column(Integer)
    turnovers_conceded = Column(Integer)
    tackles = Column(Integer)
    missed_tackles = Column(Integer)
    lineouts_won = Column(Integer)
    penalties_conceded = Column(Integer)
    yellow_cards = Column(Integer)
    red_cards = Column(Integer)
    penalties = Column(Integer)
    penalty_goals = Column(Integer)
    conversion_goals = Column(Integer)
    drop_goals_converted = Column(Integer)

    __table_args__ = (
        PrimaryKeyConstraint('id', 'gameid'),
    )

def create_database_engine(database_url):
    engine = create_engine(database_url)
    return engine

def create_tables(engine):
    Base.metadata.create_all(engine)

def create_session(engine):
    Session = sessionmaker(bind=engine)
    return Session()

def get_db(database_url):
    # Create a SQLAlchemy engine with the parameterized database URL
    engine = create_database_engine(database_url)
    # Create tables if they don't exist
    create_tables(engine)
    session = create_session(engine)
    return session
