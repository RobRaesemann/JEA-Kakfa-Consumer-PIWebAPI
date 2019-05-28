from kafka import KafkaProducer
import numpy as np
import random
import asyncio
import json
import uuid
from datetime import datetime
import time


assets = ["http1.robinwood.transformer1","http1.robinwood.transformer2","http1.robinwood.transformer3"]

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                            api_version=(0, 11),
                            value_serializer=lambda x: json.dumps(x).encode('utf-8')
                            )


def _create_readings():
    payload_block = list()
    for asset in assets:
        read = dict()
        readings = dict()
        np.random.seed(int(time.time()))
        rand_readings = np.random.random(size=2)
        readings['top_oil_temp'] = rand_readings[0]
        readings['ltc_tank_temp'] = rand_readings[1] 

        read['asset'] = asset
        read['readings'] = readings
        read["timestamp"] = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        read["key"] = str(uuid.uuid4())
        payload_block.append(read)

    return payload_block


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                            api_version=(0, 10),
                            value_serializer=lambda x: json.dumps(x).encode('utf-8')
                            )

x=0
while True:
    x=x+1
    print(f'{x}')
    payload = _create_readings()
    producer.send('iot-readings',value=payload)
    producer.flush()
    time.sleep(1)

