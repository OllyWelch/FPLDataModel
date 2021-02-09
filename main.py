from data_collection import data_collection
from get_player_info import get_player_info
from modelling import modelling

def handler(event, context):
    # Get database Uri from Lambda event
    db_uri = event['DB_URI']

    # Do the data collection step
    data_collection(db_uri=db_uri)

    # Do player info collection
    get_player_info(db_uri=db_uri)

    # Do modelling
    modelling(db_uri=db_uri)

    return {
        'status_code': 200,
        'body': 'Function run successfully'
    }
