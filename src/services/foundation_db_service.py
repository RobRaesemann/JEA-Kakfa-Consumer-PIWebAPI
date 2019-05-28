import sqlite3

SQLITE_FILE = './data/FoundationDbMock.db'


def create_connection():
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
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