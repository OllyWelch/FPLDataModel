import requests
import pandas as pd
import json
import sqlalchemy as db
import os

def get_player_info():
    print('Getting player info...')
    bootstrap_request = requests.get("https://fantasy.premierleague.com/api/bootstrap-static/")
    request_json = bootstrap_request.json()

    players = pd.read_json(json.dumps(request_json["elements"]))[['id', 'first_name', 
    'second_name', 'team', 'now_cost']]
    teams = pd.read_json(json.dumps(request_json["teams"]))[['id', 'name', 'short_name']]
    teams = teams.rename(columns={'id': 'team', 'name': 'team_name', 'short_name': 'team_short_name'})
    teams = teams.set_index('team')
    players = players.join(teams, on='team', how='inner')
    players['now_cost'] = players['now_cost'] / 10
    players.set_index('id', inplace=True)

    db_uri = os.environ.get('DB_URI') + '?charset=utf8'
    engine = db.create_engine(db_uri)

    players.to_sql('player_info', con=engine, if_exists='replace')
    print('Player info successfully added to database.')