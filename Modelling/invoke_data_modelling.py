import os
import json
import boto3
import pandas as pd
import numpy as np
import sqlalchemy as db

client = boto3.client('lambda')
db_uri = os.environ.get('POSTGRES')

def handler(event, context):

    # Connect to the database
    engine = db.create_engine(db_uri)

    # Retrieve the features, response, and player_info tables from the database
    features = pd.read_sql('SELECT * FROM features', engine, index_col='entry_id')
    response = pd.read_sql('SELECT * FROM response', engine, index_col='entry_id')
    player_info = pd.read_sql('SELECT * FROM player_info', engine, index_col='id')
    
    # Retrieve hyperparameters from the database
    hyperparameters = pd.read_sql('SELECT * FROM hyperparameters', con=engine)
    hyperparameters_dict = {column: int(hyperparameters.loc[0, column]) for column in hyperparameters.columns}

    # Fill any blanks in the features table - for the column chance of playing fill with 100 and the rest with 0
    statuses = player_info.loc[:, 'status']
    features = features.join(statuses, on='player_id')
    
    # get index values of unavailable, doubtful and injured players for this gameweek
    unavailable_index = features[(features.timestamp == max(features.timestamp)) & 
    ((features.status=='u') | (features.status=='s'))].index
    injured_index = features[(features.timestamp == max(features.timestamp)) & (features.status == 'i')].index
    doubtful_index = features[(features.timestamp == max(features.timestamp)) & (features.status == 'd')].index
    
    # Fill the na values accordingly
    features.loc[unavailable_index, 'chance_of_playing'] = 0
    features.loc[injured_index, 'chance_of_playing'] = 0
    features.loc[doubtful_index, 'chance_of_playing'] = features.loc[doubtful_index, 'chance_of_playing'].fillna(50)
    features['chance_of_playing'].fillna(100, inplace=True)
    features.fillna(0, inplace=True)

    # Join the response to the features table and drop unnecessary columns timestamp and player_id
    df = features.join(response, how='inner')
    df.drop(columns=['timestamp', 'player_id', 'status'], inplace=True)

    # Set up X and y matrices
    X, y = df.iloc[:, :-1].to_json(orient="split"), df.iloc[:, -1].to_json(orient="split")
    
    # Now retrieve the data to predict
    new_df = features[features.timestamp == max(features.timestamp)].copy()

    # Pull out the player ids for use later
    player_ids = list(new_df['player_id'].values)

    # Drop columns not used in the model
    new_df.drop(columns=['timestamp', 'player_id', 'status'], inplace=True)
    
    # Convert new_df to a json format
    new_df = new_df.to_json(orient="split")
    
    # Package data to sent to DataModelling Lambda function
    inputParams = {
        'X': json.loads(X),
        'y': json.loads(y),
        'hyperparameters': hyperparameters_dict,
        'X_new': json.loads(new_df),
        'grid': False
    }
    
    # Invoke DataModelling Lambda function
    response = client.invoke(
        FunctionName = 'arn:aws:lambda:eu-west-2:388851918592:function:FPLDataModelling',
        InvocationType = 'RequestResponse',
        Payload = json.dumps(inputParams)
    )

    # Retrieve predictions from the response
    predictions = json.load(response['Payload'])['predictions']
    
    # Put predictions in a dataframe with player_ids
    prediction_df = pd.DataFrame({'player_id': player_ids, 'prediction': predictions}).sort_values(
        by='prediction', ascending=False).set_index('player_id')
    
    # Dump predictions to the database
    prediction_df.to_sql('predictions', con=engine, if_exists='replace')

    return {
        'status': 200,
        'body': 'Data modelled successfully and predictions added to database' 
    }