import sqlalchemy as db
import pandas as pd
import numpy as np
import os
from pre_scaler import PreScaler
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.pipeline import Pipeline


# Connect to the database
db_uri = os.environ.get('DB_URI')
engine = db.create_engine(db_uri)

# Retrieve the features, response, and player_info tables from the database
features = pd.read_sql('SELECT * FROM features', engine, index_col='entry_id')
response = pd.read_sql('SELECT * FROM response', engine, index_col='entry_id')
player_info = pd.read_sql('SELECT * FROM player_info', engine, index_col='id')

print(features)
print(response)

# # Fill any blanks in the features table - for the column chance of playing fill with 100 and the rest with 0
# features['chance_of_playing'].fillna(100, inplace=True)
# features.fillna(0, inplace=True)

# # Join the response to the features table and drop unnecessary columns timestamp and player_id
# df = features.join(response, how='inner')
# df.drop(columns=['timestamp', 'player_id'], inplace=True)

# # Set up X and y matrices
# X, y = df.iloc[:, :-1], df.iloc[:, -1]

# # Split into train and test sets of features and response
# X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, random_state=0)

# # DATA PIPELINE:
# pipe = Pipeline([
#     ('pre_scaler', PreScaler()),
#     ('scaler', StandardScaler()),
#     ('regressor', RandomForestRegressor(n_estimators=100, random_state=0))
# ])

# # Set parameters to gridsearch over
# max_depth = [5, 10, 15]
# min_samples_leaf = [5, 6, 7, 8]
# min_samples_split = [5, 10]


# param_grid = [{
#         'regressor__max_depth': max_depth,
#         'regressor__min_samples_leaf': min_samples_leaf,
#         'regressor__min_samples_split': min_samples_split,
#     }]

# # Fit the gridsearched model to the training data with 4-fold CV
# grid = GridSearchCV(pipe, n_jobs=-1, param_grid=param_grid, cv=4, verbose=True)
# grid.fit(X_train, y_train)

# # Give best CV score, as well as training and test set performance with the gridsearched parameters
# print('Best score obtained with params {}'.format(grid.best_params_))
# print('Best CV score {}'.format(grid.best_score_))
# print('Performance on training set {}'.format(grid.score(X_train, y_train)))
# print('Performance on test set: {}'.format(grid.score(X_test, y_test)))

# # Now retrieve the data to predict
# new_df = features[features.timestamp == max(features.timestamp)].copy()

# # Pull out the player ids for use later
# player_ids = new_df['player_id'].values

# # Drop columns not used in the model
# new_df.drop(columns=['timestamp', 'player_id'], inplace=True)

# # Make new predictions and add to a DF with the player_ids
# new_predictions = grid.predict(new_df)
# prediction_df = pd.DataFrame({'player_id': player_ids, 'prediction': new_predictions}).sort_values(by='prediction', ascending=False).set_index('player_id')

# # Dump the predictions to the database
# prediction_df.to_sql('predictions', con=engine, if_exists='replace')

# # Print out the top predictions for this week
# print(prediction_df.head(20))