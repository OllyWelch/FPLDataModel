import sqlalchemy as db
import pandas as pd
import numpy as np
import os
from CustomRegressor import CustomRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import mean_squared_error
from sklearn.pipeline import Pipeline

def modelling(db_uri=os.environ.get('DB_URI')):
    # Connect to the database
    engine = db.create_engine(db_uri)

    # Retrieve the features, response, and player_info tables from the database
    features = pd.read_sql('SELECT * FROM features', engine, index_col='entry_id')
    response = pd.read_sql('SELECT * FROM response', engine, index_col='entry_id')
    player_info = pd.read_sql('SELECT * FROM player_info', engine, index_col='id')

    # Fill any blanks in the features table - for the column chance of playing fill with 100 and the rest with 0
    features['chance_of_playing'].fillna(100, inplace=True)
    features.fillna(0, inplace=True)

    # Join the response to the features table and drop unnecessary columns timestamp and player_id
    df = features.join(response, how='inner')
    df.drop(columns=['timestamp', 'player_id'], inplace=True)

    # Set up X and y matrices
    X, y = df.iloc[:, :-1], df.iloc[:, -1]

    # Split into train and test sets of features and response
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, random_state=0)

    # DATA PIPELINE:
    pipe = Pipeline([
        ('scaler', StandardScaler()),
        ('regressor', CustomRegressor())
    ])

    # Set parameters to gridsearch over
    max_depth = np.arange(3, 7)
    min_samples_leaf = np.arange(2, 11)
    min_samples_split = np.arange(2, 8)

    reg_params = [{'max_depth': md, 'min_samples_leaf': msl, 'min_samples_split': mss}
    for md in max_depth for msl in min_samples_leaf for mss in min_samples_split]

    param_grid = [{
            'regressor__reg_params': reg_params,
        }]

    # Fit the gridsearched model to the training data with 4-fold CV
    grid = GridSearchCV(pipe, n_jobs=-1, param_grid=param_grid, cv=4, verbose=True, scoring='r2')
    grid.fit(X_train, y_train)

    # Give best CV score, as well as training and test set performance with the gridsearched parameters
    print('Best score obtained with params {}'.format(grid.best_params_))
    print('Best CV score {}'.format(grid.best_score_))
    print('Performance on training set {}'.format(grid.score(X_train, y_train)))
    print('Performance on test set: {}'.format(grid.score(X_test, y_test)))

    # Dump the chosen hyperparameters to the database for future use
    params = grid.best_params_
    param_df = pd.DataFrame(columns=['max_depth', 'min_samples_leaf', 'min_samples_split'])
    param_df.loc[0] = params['regressor__reg_params'].values()
    param_df.to_sql('hyperparameters', con=engine, if_exists='replace')

       
    # Now retrieve the data to predict
    new_df = features[features.timestamp == max(features.timestamp)].copy()

    # Pull out the player ids for use later
    player_ids = new_df['player_id'].values

    # Drop columns not used in the model
    new_df.drop(columns=['timestamp', 'player_id'], inplace=True)

    # Make new predictions and add to a DF with the player_ids
    new_predictions = grid.predict(new_df)
    prediction_df = pd.DataFrame({'player_id': player_ids, 'prediction': new_predictions}).sort_values(by='prediction', ascending=False).set_index('player_id')

    print(prediction_df.head(10).join(player_info, on='player_id'))


if __name__ == "__main__":
    modelling()
