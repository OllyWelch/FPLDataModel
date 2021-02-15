import requests
import pandas as pd
import json
import sqlalchemy as db
import os

def get_player_info(db_uri=os.environ.get('DB_URI')):
    # Retrieve info from Bootstrap page
    print('Getting player info...')
    bootstrap_request = requests.get("https://fantasy.premierleague.com/api/bootstrap-static/")
    request_json = bootstrap_request.json()

    # Extract player and team info from bootstrap request ready for consolidation
    players = pd.read_json(json.dumps(request_json["elements"]))[['id', 'first_name', 
    'second_name', 'team', 'now_cost', 'element_type', 'status']]
    teams = pd.read_json(json.dumps(request_json["teams"]))[['id', 'name', 'short_name']]

    # Rename the columns of teams so it doesn't clash with any players columns
    teams = teams.rename(columns={'id': 'team', 'name': 'team_name', 'short_name': 'team_short_name'})

    # Set index to team in teams df ready for join
    teams = teams.set_index('team')

    # Join teams df to players and drop superfluous team id column
    players = players.join(teams, on='team', how='inner').drop(columns='team')

    # Rescale current price by factor of 10
    players['now_cost'] = players['now_cost'] / 10

    # Map element type to player position
    type_mapping = {
        1: "GKP",
        2: "DEF",
        3: "MID",
        4: "FWD"
    }
    players = players.replace({'element_type': type_mapping})

    # Rename columns to friendlier names
    players = players.rename(columns={'now_cost': 'current_price', 'element_type': 'position'})

    # Set player id to the index ready for writing to DB
    players.set_index('id', inplace=True)

    # Connect to DB with UTF8 encoding as a parameter
    db_uri += '?charset=utf8'
    engine = db.create_engine(db_uri)

    # Dump player info to DB
    players.to_sql('player_info', con=engine, if_exists='replace')
    print('Player info successfully added to database.')