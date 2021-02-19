import requests
import json
import numpy as np
import pandas as pd
from datetime import datetime
from player import Player
import sqlalchemy as db
import os

"""
We must collect the data we need. We use a function which takes the list of players from the bootstrap
API and gathers information needed, also making use of the Player class from player.py
"""

def get_feature_data(player_list):

    """
    Creates a dataframe player_data with player data including form, next fixture opposition strength and 
    each players points per game
    """
    print("Gathering up to date player data...")

    # columns to be extracted from bootstrap
    bootstrap_columns = ["id","team", "ict_index", "chance_of_playing_this_round", "form", "points_per_game"]
    # columns to be extracted from element history
    player_columns = ["total_points"]
    # columns to be extracted from element fixtures
    fixture_columns = ['is_home', 'difficulty']

    # create empty dataframe as an empty list 
    player_data = []

    # For each player:
    for player in player_list:
        # create a class instance for player specific data
        player_cls = Player(player['id'])
        # make an empty row
        row = []
        # loop over all the necessary columns, appending the values to the empty row
        try: 
            for column in bootstrap_columns:
                row.append(player[column])
            for column in player_columns:
                row.append(player_cls.history[column])
            for column in fixture_columns:
                row.append(player_cls.fixtures[column])
        except:
            break
        player_data.append(row)

    # Add the matrix of data in player_data to a pandas dataframe with all the relevant columns, index is id
    player_data = pd.DataFrame(player_data, columns=bootstrap_columns+player_columns+fixture_columns).set_index('id')
    # map is_home to integer
    player_data.is_home = player_data.is_home.astype('int')
    print("Player data gathered!")

    return player_data


def get_response_data(entry_player_mapping, player_data):

    """Function takes the output of get_feature_data as input and extracts the response from each player. 
    We use the entry ids from the previous gameweek along with their mapping to a player id to output a 
    dataframe with columns of entry_id and points."""

    player_data = player_data.rename(columns={'total_points': 'points_scored'})
    return entry_player_mapping.join(player_data, on='id')['points_scored']


def data_collection(db_uri=os.environ.get('POSTGRES')):
    """
    First step is to request all the data from the bootstrap page of the API in order to collect general player and team data
    """

    # request data from API
    bootstrap_request = requests.get("https://fantasy.premierleague.com/api/bootstrap-static/")

    # convert request to json
    request_json = bootstrap_request.json()

    # extract lists of events, players, and teams
    events = pd.read_json(json.dumps(request_json["events"]))
    players = request_json["elements"]

    """
    Finally, we check if an update needs to be performed. If the current gameweek is not finished, no update is performed.
    Otherwise, up to date information is gathered. If the data for the next gameweek already exists in the database, we delete it, reset
    the auto increment and dump the new data to the database.
    """

    # connect to database
    engine = db.create_engine(db_uri)

    # is update needed?

    print('Checking if current gameweek is finished...')

    most_recent_update = list(engine.execute('SELECT MAX(TIMESTAMP) FROM features;'))[0][0]
    next_unfinished_gameweek_start_time = list(events[events.finished == False].loc[:, 'deadline_time'])[0].replace(tzinfo=None)
    is_update_required = next_unfinished_gameweek_start_time > datetime.now()

    # if update needed
    if is_update_required:

        print('Current gameweek is finished, proceeding to get updated data')

        # get updated data
        new_data = get_feature_data(players)

        try:
            # check if data for next gameweek exists in the database
            last_finished_gameweek_start_time = list(pd.to_datetime(events[events.finished].loc[:, 'deadline_time']))[-1].replace(tzinfo=None)
            does_next_gameweek_data_exist = most_recent_update > last_finished_gameweek_start_time
        except: does_next_gameweek_data_exist = False

        # if it exists
        if does_next_gameweek_data_exist:

            print("Removing out of date data...")

            # delete most recent data
            engine.execute('DELETE FROM features WHERE timestamp=(SELECT MAX(timestamp) FROM features);')

            print("Out of date data removed.")

        else: 
            # if we haven't already collected the new data, we can get the response data
            query = 'SELECT entry_id, player_id FROM features WHERE timestamp=(SELECT MAX(timestamp) FROM features);'
            mapping = pd.read_sql(query, con=engine, index_col='entry_id')
            mapping = mapping.rename(columns={'player_id': 'id'})
            # function which extracts response data from gathered player data
            response = get_response_data(mapping, new_data)
            # dump response data to db
            response.to_sql('response', con=engine, if_exists='append')

        print("Preparing to insert feature data to database...")
        # rename the columns to more friendly names
        column_dict = {
            'team': 'team_id',
            'chance_of_playing_this_round': 'chance_of_playing',
            'total_points': 'previous_points',
            'difficulty': 'next_fixture_difficulty'
        }
        new_data = new_data.rename(columns=column_dict)
        new_data = new_data.rename_axis('player_id')
        # get max entry id
        try: 
            auto_increment = list(engine.execute('SELECT MAX(entry_id) FROM features'))[0][0] + 1
        except:
            auto_increment = 1
        # reset index and set to auto_increment, then rename index as entry id
        new_data = new_data.reset_index()
        new_data.index = np.arange(auto_increment, auto_increment + len(new_data))
        new_data = new_data.rename_axis('entry_id')

        # add timestamp to new_data
        new_data['timestamp'] = datetime.now().replace(microsecond=0)

        print("Feature data prepared, adding to database...")
        # dump data to features table within the database
        new_data.to_sql('features', con=engine, if_exists='append')

        print('Data successfully added to database.')

    else: 
        print('Gameweek in progress, no update performed.')
