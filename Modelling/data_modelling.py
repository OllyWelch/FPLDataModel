import json
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from CustomRegressor import CustomRegressor


def handler(event, context):
    # Read data sent to handler
    X = np.array(event["X"]["data"])
    y = np.array(event["y"]["data"])
    hyperparameters = event["hyperparameters"]
    X_new = np.array(event["X_new"]["data"])
    
    max_depth = hyperparameters["max_depth"]
    min_samples_leaf = hyperparameters["min_samples_leaf"]
    min_samples_split = hyperparameters["min_samples_split"]

    # DATA PIPELINE:
    pipe = Pipeline([
        ('scaler', StandardScaler()),
        ('regressor', CustomRegressor(
            reg_params={'max_depth':max_depth, 'min_samples_leaf':min_samples_leaf,
             'min_samples_split':min_samples_split}))
    ])

    # Fit the pipeline to the whole dataset
    pipe.fit(X, y)

    # Predict on the new data
    predictions = list(pipe.predict(X_new))

    return {
        'statusCode': 200,
        'predictions': predictions
    }
