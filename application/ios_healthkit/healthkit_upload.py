import json
# from producer.send_record import send_record
from producer.send_records_azure import send_records_to_eventhub

from .utils.healthkit_parsers import *
import os
import logging

LOG = logging.getLogger(__name__)


# set Kafka listener port from config file

allowed_streams = ['sleep']

RECORD_PROCESSING = {
    'sleep': format_healthkit_sleep_event
}

SCHEMA_LOC = './avro'
SCHEMA_MAPPING = {
    'sleep': 'event_schema.avsc'
}

TOPIC_MAPPING = {
    'sleep': 'healthkit_events_sleep'
}



class Bunch(object):
    def __init__(self, adict):
        self.__dict__.update(adict)


def send_records_to_producer(personicle_user_id, records, stream_name, limit = None):
    count = 0
    record_formatter = RECORD_PROCESSING[stream_name]
    schema = SCHEMA_MAPPING[stream_name]
    topic = TOPIC_MAPPING[stream_name]


    LOG.info("Sending records to producer")
    LOG.info("Formatting records...")

    formatted_record_list = []

    for record in records:
        formatted_record = record_formatter(record, personicle_user_id)
        formatted_record_list.append(formatted_record)
        count += 1

        if count == 1:
            LOG.info("Calling send_records_to_eventhub...")
            LOG.info(str(formatted_record))


        if limit is not None and count >= limit:
            break

    LOG.info(f'Calling send_records_to_eventhub on {len(formatted_record_list)} records')
    send_records_to_eventhub(schema, formatted_record_list, 'testhub-new')


if __name__ == "__main__":
    send_records_to_producer("./heartrate.json", "heartrate")

    # Test Command
    # python send_record.py --topic test_event --schema-file quick-test-schema.avsc --record-value '{"id": 999, "product": "foo", "quantity": 100, "price": 50}'