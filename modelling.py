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


db_uri = os.environ.get('DB_URI')
engine = db.create_engine(db_uri)

features = pd.read_sql('SELECT * FROM features', engine, index_col='entry_id')
response = pd.read_sql('SELECT * FROM response', engine, index_col='entry_id')
player_info = pd.read_sql('SELECT * FROM player_info', engine, index_col='id')
features['chance_of_playing'].fillna(100, inplace=True)
features.fillna(0, inplace=True)

df = features.join(response, how='inner')
df.drop(columns=['timestamp', 'player_id'], inplace=True)


X, y = df.iloc[:, :-1], df.iloc[:, -1]

# Split into train and test sets of features and response
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, random_state=0)

# DATA PIPELINE:
pipe = Pipeline([
    ('pre_scaler', PreScaler()),
    ('scaler', StandardScaler()),
    ('regressor', RandomForestRegressor(n_estimators=100, random_state=0))
])


max_depth = [6]
min_samples_leaf = [6]
min_samples_split = [10]


param_grid = [{
        'regressor__max_depth': max_depth,
        'regressor__min_samples_leaf': min_samples_leaf,
        'regressor__min_samples_split': min_samples_split,
    }]

grid = GridSearchCV(pipe, n_jobs=-1, param_grid=param_grid, cv=4, verbose=True)
grid.fit(X_train, y_train)

print('Best CV score {}'.format(grid.best_score_))
print('Best score obtained with params {}'.format(grid.best_params_))
print('Performance on training set {}'.format(grid.score(X_train, y_train)))
print('Performance on test set: {}'.format(grid.score(X_test, y_test)))

new_df = features[features.timestamp == max(features.timestamp)].copy()
player_ids = new_df['player_id'].values
new_df.drop(columns=['timestamp', 'player_id'], inplace=True)
new_predictions = grid.predict(new_df)
prediction_df = pd.DataFrame({'player_id': player_ids, 'prediction': new_predictions}).sort_values(by='prediction', ascending=False).set_index('player_id')
prediction_df.to_sql('predictions', con=engine, if_exists='replace')
