from data_collection import data_collection
from get_player_info import get_player_info
import pymysql

pymysql.install_as_MySQLdb()

def handler(event, context):

    # Do the data collection step
    data_collection()

    # Do player info collection
    get_player_info()

    return {
        'status_code': 200,
        'body': 'Function run successfully'
    }
