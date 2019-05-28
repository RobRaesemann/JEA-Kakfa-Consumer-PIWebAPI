import pickle
import os
import json
import requests
import requests_futures

from src.services.config_service import get_cache_filename
from src.services.foundation_db_service import get_tags_going_to_pi
from src.services.pi_service import get_webid
from src.services.logger_service import LOG



def add_web_id_to_pi_values(pi_values):
    """

    """
    for pi_value in pi_values:
        print(pi_value)

def load_pickled_tag_dict(tag_dict):
    filename = 'tag_web_id.pkl'
    outfile = open(filename,'wb')
    pickle.dump(tag_dict,outfile)
    outfile.close()

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


    """

    tag_cache = load_cache()
    tag_list = get_tags_going_to_pi()

    for tag in tag_list:
        if tag not in tag_cache:
            webid = get_webid(tag)
            tag_cache[tag] = webid

    
    return tag_cache

