import json
# from producer.send_record import send_record
from producer.send_records_azure import send_records_to_eventhub

from .utils.fitbit_parsers import *
# from application.config import KAFKA_CONFIG
import logging

# set Kafka listener port from config file
LOG = logging.getLogger(__name__)
allowed_streams = ['heartrate', 'activity']

RECORD_PROCESSING = {
    'heartrate': format_heartrate,
    'activity': fitbit_activity_parser ,
    'sleep': fitbit_sleep_parser
}

SCHEMA_LOC = './avro_modules'
SCHEMA_MAPPING = {
    'heartrate': 'fitbit_stream_schema.avsc',
    'activity': 'event_schema.avsc',
    'sleep': 'event_schema.avsc'
}

TOPIC_MAPPING = {
    'heartrate': 'fitbit_stream_heartrate',
    'activity': 'testhub-new',
    'sleep': 'testhub-new'
}



class Bunch(object):
    def __init__(self, adict):
        self.__dict__.update(adict)


def send_records_to_producer(personicle_user_id, records, stream_name, limit = None):
    count = 0
    record_formatter = RECORD_PROCESSING[stream_name]
    schema = SCHEMA_MAPPING[stream_name]
    topic = TOPIC_MAPPING[stream_name]
    formatted_records = []
    for record in records:
        formatted_record = record_formatter(record, personicle_user_id)
        if type(formatted_record) is dict:
            formatted_records.append(formatted_record)
        elif type(formatted_record) is list:
            formatted_records.extend(formatted_record)
        else:
            LOG.error("Record not processed correctly for stream {}, record data: {} \n formatted record: {}".format(stream_name, json.dumps(record, indent=2), json.dumps(formatted_record, indent=2)))
            
        count += 1

        if limit is not None and count <= limit:
            break
    # loop = asyncio.new_event_loop()
    # asyncio.set_event_loop(loop)
    try:
        send_records_to_eventhub(schema, formatted_records, topic)
        num_records_sent = len(formatted_records)
        status = True
    except Exception as e:
        LOG.error("Error while sending records to eventhub for stream {}: \n {}".format(stream_name, str(e)))
        num_records_sent = 0
        status = False
    return status, num_records_sent


# TESTING METHOD FOR BULK UPLOAD OF FITBIT DATA
# DEPRECATED - CONFLUENT KAFKA LIBRARY REMOVED FROM REQUIREMENTS
def send_records_from_file_to_producer(filename, stream_name, limit = None):
    count = 0
    record_formatter = RECORD_PROCESSING[stream_name]
    schema = SCHEMA_MAPPING[stream_name]
    topic = TOPIC_MAPPING[stream_name]

    with open(filename, "r") as record_file:
        records = json.load(record_file)

    for record in records:
        formatted_record = record_formatter(record)
        print(formatted_record)
        count += 1

        # send the record and schema to producer
        args = Bunch({
                    'topic': topic,
                    'schema_file': schema,
                    'record_value': json.dumps(formatted_record),
                    "bootstrap_servers": "localhost:9092",
                    "schema_registry": "http://localhost:8081",
                    "record_key": None
                })
        print(args.schema_file)
        send_record(args)

        if limit is not None and limit <= count:
            break

if __name__ == "__main__":
    send_records_from_file_to_producer("./pmdata/p01/fitbit/heart_rate.json", "heartrate")
    
    # Test Command
    # python send_record.py --topic test_event --schema-file quick-test-schema.avsc --record-value '{"id": 999, "product": "foo", "quantity": 100, "price": 50}'