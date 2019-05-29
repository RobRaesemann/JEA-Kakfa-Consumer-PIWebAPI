# Our Kafka Service consumes the messages from the Kafka topic and processes
# them before sending to PI.


import asyncio
from kafka import KafkaConsumer
import time
import json
import datetime
from pytz import timezone

from src.services.config_service import get_kafka_interval,get_kafka_topic, \
    get_kafka_server,get_kafka_consumer_group,get_aggregator_prefix,get_kafka_timezone
from src.services.pi_service import post_data_to_pi
from src.services.foundation_db_service import get_tags_going_to_pi
from src.services.pi_webid_cache_service import get_tag_webid_cache

KAFKA_TOPIC = get_kafka_topic()
CONSUMER_GROUP = get_kafka_consumer_group()
KAFKA_SERVER = get_kafka_server()
LOCAL_TZ = get_kafka_timezone()
TAG_PREFIX = get_aggregator_prefix()

tag_webid_cache = None

# Out Kafka consumer should consume the earilest message that it has not 
# yet read. This means if the consumer is stopped for some time, it will 
# pick up where it left off so that all of the data is backfilled into PI.
# Using Consumer Groups allow us to track our progress automatically. We 
# should also be able to run more than one consumer to spread the load, if 
# needed, although this has not been tested.
consumer = KafkaConsumer(KAFKA_TOPIC, auto_offset_reset='earliest',
                             bootstrap_servers=[KAFKA_SERVER],
                             enable_auto_commit=True,
                             group_id=CONSUMER_GROUP,
                             api_version=(0, 10),
                             consumer_timeout_ms=1000)

def decode_tags_from_message(message):
    """
    Decode the Kafka message and strip the prefix added by the HTTP south plugin 
    on the aggregator node and create a reading more suitable for PI.
    """
    pi_values = []
    message = json.loads(message)
    for item in message:
        for reading in item['readings']:
            pi_value = {}
            pi_value['tagname'] = f"{item['asset']}.{reading}".replace(TAG_PREFIX,'')
            pi_value['value'] = item['readings'][reading]
            pi_value['timestamp'] = item['timestamp']
            pi_values.append(pi_value)
    return pi_values

def load_pi_webid_cache():
    """
    We cache a dictionary of PI WebIds so that we can translate the tagname that
    we get from Kafka to the WebId required for the POST to the PI WebAPI.
    """
    taglist = get_tags_going_to_pi()
    return taglist

def build_pi_webapi_payload(pi_values):
    """
    Takes the Kafka payload and massages it into a payload for the PI WebAPI 
    POST.
    
    TODO: 
    (1) We assume that everything coming in from Kafka is going to PI at this point. Later we will
    have some readings from asset that go to PI and some that go elsewhere. We will need to ignore any readings
    that we don't want to send to PI.
    (2) Cleaned up and optimize. Add error handling and logging
    """
    readings = []
    for value in pi_values:
        tagname = value['tagname']
        if tagname in tag_webid_cache:
            reading = dict()
            item = dict()
            items = []
            reading['WebId'] = tag_webid_cache[tagname]
            localtz = timezone(LOCAL_TZ)
            ts_temp = datetime.datetime.strptime(value['timestamp'], '%m/%d/%Y %H:%M:%S') #.isoformat()
            ts = localtz.localize(ts_temp).isoformat()
            item['Timestamp'] = ts
            item['Value'] = value['value']
            items.append(item)
            reading['Items'] = items
            readings.append(reading)
    return readings

async def consume_kafka_forever():
    """
    Process Kafka Messages posted to the topic. This will start with the oldest
    message which has not been processed so that Kafka acts as a buffer. 
    
    TODO: Add some error handling and restart the loop if it fails for some reason
    """
    global tag_webid_cache
    tag_webid_cache = get_tag_webid_cache()

    while True:
        for message in consumer:
            pi_values = decode_tags_from_message(message.value)
            pi_values_by_webid = build_pi_webapi_payload(pi_values)
            post_data_to_pi(pi_values_by_webid)
        time.sleep(1)

def subscribe():
    """
    Subscribe to the Kafka Topic
    """
    consumer.subscribe(topics=[KAFKA_TOPIC])

def consume():
    """
    Our main routine. Subscribes to the Kafka topic and then starts a task
    to consume the Kafka messages. Used anyncio I/O although we aren't really
    doing anything ansynchronously yet.
    """
    # Subscribe to the Kafka topic 
    subscribe()
    # Endless loo to consume the topic items
    asyncio.run(consume_kafka_forever())