# FPLDataModel

This code consists of two main parts:

## Data Collection

This part is intended as one AWS lambda function that connects to and updates a cloud hosted database which consists of tables *features*, *response*, *player_info*, and *predictions*. Each line in features corresponds to one player on one specific FPL gameweek, including form, points per game and ICT index. The response table contains the points scored on this gameweek for each entry in the features table. The table player_info contains general information about each player including their name and availability indexed on their player id. Finally, predictions includes the expected points output by the model for each player for the coming gameweek.

The **handler()** function in main.py gathers updated information from the FPL API for the features and response table for the coming gameweek, as well as updating the general player information.

## Data Modelling

The Modelling directory consists of all the components needed to connect to the data in the database, and generate predictions for the upcoming gameweek.

The first component is a lambda function in data_modelling.py which takes in as input parameters the features, response and hyperparameter data, trains a model using the custom Scikit-learn regression in custom_regressor.py, then outputs predictions for the upcoming gameweek based on this model.

The other component is a lambda function in invoke_data_modelling.py that invokes this modelling - sending the relevant data from the database and formatting the predictions so they can be input to the database. This function acts as a bridge between the database and the modelling function.
