import asyncio
from kafka import KafkaConsumer
import time
import json
import datetime
from pytz import timezone

from src.services.config_service import get_kafka_interval,get_kafka_topic,get_kafka_server,get_kafka_consumer_group,get_aggregator_prefix
from src.services.pi_service import post_data_to_pi
from src.services.foundation_db_service import get_tags_going_to_pi
from src.services.pi_webid_cache_service import get_tag_webid_cache

KAFKA_TOPIC = get_kafka_topic()
CONSUMER_GROUP = get_kafka_consumer_group()
KAFKA_SERVER = get_kafka_server()

tag_webid_cache = None

consumer = KafkaConsumer(KAFKA_TOPIC, auto_offset_reset='earliest',
                             bootstrap_servers=[KAFKA_SERVER],
                             enable_auto_commit=True,
                             group_id=CONSUMER_GROUP,
                             api_version=(0, 10),
                             consumer_timeout_ms=1000)

def decode_tags_from_message(message):
    """


    """

    pi_values = []
    message = json.loads(message)
    for item in message:
        for reading in item['readings']:
            pi_value = {}

            pi_value['tagname'] = f"{item['asset']}.{reading}".replace('http1.','')
            pi_value['value'] = item['readings'][reading]
            pi_value['timestamp'] = item['timestamp']
            pi_values.append(pi_value)

    return pi_values

def load_pi_webid_cache():
    taglist = get_tags_going_to_pi()
    return taglist

def build_pi_webapi_payload(pi_values):
    readings = []
    for value in pi_values:
        tagname = value['tagname']
        if tagname in tag_webid_cache:
            reading = dict()
            item = dict()
            items = []
            reading['WebId'] = tag_webid_cache[tagname]
            timestring = value['timestamp']
            localtz = timezone('America/New_York')
            ts1 = datetime.datetime.strptime(timestring, '%m/%d/%Y %H:%M:%S') #.isoformat()
            ts = localtz.localize(ts1).isoformat()
            item['Timestamp'] = ts
            item['Value'] = value['value']
            items.append(item)
            reading['Items'] = items
            readings.append(reading)
            print(reading)
    return readings

async def consume_kafka_forever():
    """


    """
    global tag_webid_cache
    tag_webid_cache = get_tag_webid_cache()
    
    
    
    
    while True:
        for message in consumer:
            pi_values = decode_tags_from_message(message.value)
            pi_values_by_webid = build_pi_webapi_payload(pi_values)
            await post_data_to_pi(pi_values_by_webid)
        time.sleep(1)

def subscribe():
    consumer.subscribe(topics=[KAFKA_TOPIC])

def consume():
    """
    
    """
    # Subscribe to the Kafka topic 
    subscribe()
    # Endless loo to consume the topic items
    asyncio.run(consume_kafka_forever())