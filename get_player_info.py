import requests
import pandas as pd
import json
import sqlalchemy as db
import os

def get_player_info(db_uri=os.environ.get('DB_URI')):
    print('Getting player info...')
    bootstrap_request = requests.get("https://fantasy.premierleague.com/api/bootstrap-static/")
    request_json = bootstrap_request.json()

    players = pd.read_json(json.dumps(request_json["elements"]))[['id', 'first_name', 'second_name']]
    players.set_index('id', inplace=True)

    db_uri += '?charset=utf8'
    engine = db.create_engine(db_uri)

    players.to_sql('player_info', con=engine, if_exists='replace')
    print('Player info successfully added to database.')