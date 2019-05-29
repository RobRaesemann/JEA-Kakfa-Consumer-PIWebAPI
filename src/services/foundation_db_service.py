"""
This module provides a simple mock for the Foundation Database. At this point it
simply gives us a list of tags that should go to PI.
"""
import sqlite3

SQLITE_FILE = './data/FoundationDbMock.db'


def create_connection():
    """ 
    create a database connection to the SQLite database
    specified by the db_file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(SQLITE_FILE)
        return conn
    except Exception as ex:
        print(ex)
 
    return None


def get_tags_going_to_pi():
    """
    We don't entirely know what the Foundation Database will look like yet.
    This is a simple mock containing just a list of tags and a field called 
    PI, which we make 'Y', if we want to the tag to go to PI.

    We collect this list of tags and then use the pi_webid_cache_service to 
    make a dictionary which acts as a lookup cache to translate the asset names
    received from the Kafka messages into PI WebIds used to POST to PI.
    """

    conn = create_connection() 
    
    cur = conn.cursor()
    cur.execute("SELECT tag FROM tags where PI = 'Y'")
 
    rows = cur.fetchall()
    if rows:
        tag_list = list()
        for row in rows:
            tag_list.append(row[0])
    return tag_list


get_tags_going_to_pi()