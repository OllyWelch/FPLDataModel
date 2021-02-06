# FPLDataModel

This code consists of two main parts:

**Data Collection**

This part connects to a database hosted on AWS which consists of tables 'features', 'response', 'player_info', and 'predictions'. Each line in features corresponds to one player on one specific FPL gameweek, including form, points per game and ICT index. The response table contains the points scored on this gameweek for each entry in the features table. The table player_info contains general information about each player including their name indexed on their player id. Finally, predictions includes the expected points output by the model for each player for the coming gameweek.

The data_collection() function gathers updated information for the features and response table for the coming gameweek.

**Data Modelling**

The modelling() function takes all the data collected in the database and creates a random forest regression model from all the past data in order to predict the coming weeks scores. It then outputs these predictions to the predictions table in the database.
