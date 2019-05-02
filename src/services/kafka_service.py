import asyncio
from kafka import KafkaConsumer
import time
import json

from src.services.config_service import get_kafka_interval,get_kafka_topic,get_kafka_server
from src.services.pi_service import post_data_to_pi

KAFKA_TOPIC = get_kafka_topic
CONSUMER_GROUP = get_kafka_consumer_group
KAFKA_SERVER = get_kafka_server


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
            pi_value['tagname'] = f"{item['asset']}.{reading}"
            pi_value['value'] = item['readings'][reading]
            pi_value['timestamp'] = item['timestamp']
            pi_values.append(pi_value)

    return pi_values

def consume_kafka_forever():
    """


    """
    while True:
        for message in consumer:
            pi_values = decode_tags_from_message(message.value)
            await post_data_to_pi(pi_values)
        time.sleep(1)

def subscribe():
    consumer.subscribe(topics=[KAFKA_TOPIC])

async def consume():
    """

    TODO: Explore how to call this async
    """
    subscribe()
    consume_kafka_forever()