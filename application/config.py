import os
import json
import pathlib
import configparser

__file_path = os.path.abspath(__file__)
__dir_path = os.path.dirname(__file_path)

PROJ_LOC=pathlib.Path(__dir_path).parent
AVRO_SCHEMA_LOC=os.path.join(PROJ_LOC, "avro_modules")

# Database url format
# dialect+driver://username:password@host:port/database
# postgresql+pg8000://dbuser:kx%25jj5%2Fg@pghost10/appdb

if int(os.environ.get("INGESTION_PROD", '0')) != 1:
    print("in the dev environment")
    print("environment variables: {}".format(list(os.environ.keys())))
    SQLITE_DATABASE_LOCATION=os.path.join(PROJ_LOC, "database")
    SQLITE_DATABASE_NAME="user_access_tokens.db"

    __app_config = configparser.ConfigParser()
    __app_config.read(os.path.join(PROJ_LOC,'config.ini'))

    HOST_CONFIG = __app_config['HOST']
    FITBIT_CONFIG = __app_config['FITBIT']
    IOS_APP_CONFIG = __app_config['IOS_APP']
    GOOGLE_FIT_CONFIG = __app_config['GOOGLE_FIT']

    KAFKA_CONFIG = __app_config['KAFKA']

    EVENTHUB_CONFIG = __app_config['EVENTHUB']

    DB_CONFIG = __app_config['CREDENTIALS_DATABASE']

    TASK_QUEUE_CONFIG = __app_config['TASK_QUEUE']

    os.environ['CREDENTIALS_DB_USER'] = DB_CONFIG['USERNAME']
    os.environ['CREDENTIALS_DB_PASSWORD'] = DB_CONFIG['PASSWORD']
    os.environ['CREDENTIALS_DB_HOST'] = DB_CONFIG['HOST']
    os.environ['CREDENTIALS_DB_NAME'] = DB_CONFIG['NAME']
else:
    HOST_CONFIG = {
        'HOST_ADDRESS': os.environ['INGESTION_HOST_ADDRESS']
    }

    FITBIT_CONFIG = {
        'CLIENT_ID': os.environ['FITBIT_CLIENT_ID'],
        'CLIENT_SECRET': os.environ['FITBIT_CLIENT_SECRET'],
        'REDIRECT_URL': os.environ['FITBIT_REDIRECT_URL'],
        'AUTH_URL': os.environ['FITBIT_AUTH_URL'],
        'REQUEST_URL': os.environ['FITBIT_REQUEST_URL'],
        'API_ENDPOINT': os.environ['FITBIT_API_ENDPOINT']
    }

    os.makedirs("config_json", exist_ok=True)
    with open("config_json/google_secret.json", "w") as secrets_file:
        secrets_file.write(json.dumps(json.loads(os.environ['GOOGLE_FIT_SECRET_JSON'])))

    GOOGLE_FIT_CONFIG = {
        'CLIENT_ID': os.environ['GOOGLE_FIT_CLIENT_ID'],
        'CLIENT_SECRET': os.environ['GOOGLE_FIT_CLIENT_SECRET'],
        'REDIRECT_URL': os.environ['GOOGLE_FIT_REDIRECT_URL'],
        'AUTH_URL': os.environ['GOOGLE_FIT_AUTH_URL'],
        'API_ENDPOINT': os.environ['GOOGLE_FIT_API_ENDPOINT'],
        'TOKEN_URL': os.environ['GOOGLE_FIT_TOKEN_URL'],
        'SECRET_JSON': "config_json/google_secret.json"
    }

    IOS_APP_CONFIG = {
        'KEY': os.environ['IOS_APP_KEY']
    }

    EVENTHUB_CONFIG = {
        'CONNECTION_STRING': os.environ['EVENTHUB_CONNECTION_STRING'],
        'EVENTHUB_NAME': os.environ['EVENTHUB_NAME'],
        'SCHEMA_REGISTRY_FQNS': os.environ['EVENTHUB_SCHEMA_REGISTRY_FQNS'],
        'SCHEMA_REGISTRY_GROUP': os.environ['EVENTHUB_SCHEMA_REGISTRY_GROUP'],
        'DATASTREAM_EVENTHUB_CONNECTION_STRING': os.environ['DATASTREAM_EVENTHUB_CONNECTION_STRING'],
        'DATASTREAM_EVENTHUB_NAME': os.environ['DATASTREAM_EVENTHUB_NAME']
    }

    TASK_QUEUE_CONFIG = {
        'CONNECTION_STRING': os.environ['TASK_QUEUE_CONNECTION_STRING'],
        "QUEUE_NAME": os.environ['TASK_QUEUE_NAME']
    }
