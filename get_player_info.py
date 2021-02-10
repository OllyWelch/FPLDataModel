import requests
import pandas as pd
import json
import sqlalchemy as db
import os

def get_player_info(db_uri=os.environ.get('DB_URI')):
    print('Getting player info...')
    bootstrap_request = requests.get("https://fantasy.premierleague.com/api/bootstrap-static/")
    request_json = bootstrap_request.json()

    players = pd.read_json(json.dumps(request_json["elements"]))[['id', 'first_name', 
    'second_name', 'team', 'now_cost', 'element_type']]
    teams = pd.read_json(json.dumps(request_json["teams"]))[['id', 'name', 'short_name']]
    teams = teams.rename(columns={'id': 'team', 'name': 'team_name', 'short_name': 'team_short_name'})
    teams = teams.set_index('team')
    players = players.join(teams, on='team', how='inner').drop(columns='team')
    players['now_cost'] = players['now_cost'] / 10
    type_mapping = {
        1: "GKP",
        2: "DEF",
        3: "MID",
        4: "FWD"
    }
    players = players.replace({'element_type': type_mapping})
    players = players.rename(columns={'now_cost': 'current_price', 'element_type': 'position'})
    players.set_index('id', inplace=True)

    db_uri += '?charset=utf8'
    engine = db.create_engine(db_uri)

    players.to_sql('player_info', con=engine, if_exists='replace')
    print('Player info successfully added to database.')