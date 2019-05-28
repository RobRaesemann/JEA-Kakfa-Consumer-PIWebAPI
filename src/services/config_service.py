import os
import json

PROJECT_PATH = os.getcwd()
CONFIG_PATH = os.path.join(PROJECT_PATH, 'config')
CONFIG_FILE_PATH = os.path.join(CONFIG_PATH, 'config.json')

def _get_config(key):
    with open(CONFIG_FILE_PATH) as jason_data_file:
        data = json.load(jason_data_file)
    
    return data[key]

def get_pi_web_api_url():
    return _get_config("pi_web_api")["url"]    

def get_pi_web_api_crt():
    return os.path.join(CONFIG_PATH, _get_config("pi_web_api")["crt"])

def get_kafka_topic():
    return _get_config("kafka")["topic"]

def get_kafka_interval():
    return _get_config("kafka")["interval_secs"]

def get_kafka_server():
    return _get_config("kafka")["server"]

def get_kafka_consumer_group():
    return _get_config("kafka")["consumer_group"]

def get_cache_filename():
    return _get_config("webid_cache")["cache_filename"]

def get_aggregator_prefix():
    return _get_config("kafka")["aggregator_prefix"]