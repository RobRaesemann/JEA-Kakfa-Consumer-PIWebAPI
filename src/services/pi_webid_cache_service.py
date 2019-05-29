"""
Our Pi WebId Cache Service provides a way for us to keep a dictionary of 
asset names to PI WebId translations. This should make processing Kafka 
messages faster. 



"""



import pickle
import os
import json
import requests
import requests_futures

from src.services.config_service import get_cache_filename
from src.services.foundation_db_service import get_tags_going_to_pi
from src.services.pi_service import get_webid
from src.services.logger_service import LOG

CACHE_FILENAME = get_cache_filename()

# def add_web_id_to_pi_values(pi_values):
#     """

#     """
#     for pi_value in pi_values:
#         print(pi_value)

def save_tag_dict(tag_dict):

    try:
        outfile = open(CACHE_FILENAME,'wb')
        pickle.dump(tag_dict,outfile)
        outfile.close()
    except Exception as ex:
        LOG.exception(f'Exception saving pi webid cache file: {ex}')


def load_cache():
    try:
        CACHE_FILENAME = get_cache_filename()
        # if the cache file exists, return the dictionary of tagnames and webids
        if os.path.isfile(CACHE_FILENAME):
            pickle_in = open(CACHE_FILENAME,'rb')
            webid_dict = pickle.load(pickle_in)
            return webid_dict
        # If the cache file does not yet exist, return a blank dictionary
        else:
            newdict = dict()
            return newdict
    except Exception as ex:
        LOG.Exception(f'Exception loading pi webid cache file: {ex}')

def get_tag_webid_cache():
    """
    Load the list of tags going to PI from the Foundation. Get the 
    WebId if it has not already been cached.
    """

    tag_cache = load_cache()
    tag_list = get_tags_going_to_pi()

    for tag in tag_list:
        if tag not in tag_cache:
            webid = get_webid(tag)
            tag_cache[tag] = webid

    save_tag_dict(tag_cache)

    return tag_cache

