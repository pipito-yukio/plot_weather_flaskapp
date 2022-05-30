import sqlite3
from sqlite3 import Error

"""
Database connection operate functions with SQLite3
"""


def get_connection(db_path, auto_commit=False, read_only=False, logger=None):
    """
    Open database connection.
    :param db_path: database file absolute path
    :param auto_commit: True or False
    :param read_only: True or False
    :param logger: application logger or None
    :return: conn
    """
    try:
        if read_only:
            db_uri = "file://{}?mode=ro".format(db_path)
            conn = sqlite3.connect(db_uri, uri=True)
        else:
            conn = sqlite3.connect(db_path)
            if auto_commit:
                conn.isolation_level = None # auto commit
    except Error as e:
        if logger is not None:
            logger.error(e)
        raise e
    return conn


def close_connection(conn, logger=None):
    """
    Close database connection
    :param conn: Database connection
    :param logger: application logger or None
    """
    try:
        if conn is not None:
            conn.close()
    except Error as e:
        if logger is not None:
            logger.warning(e)


def create_table(conn, create_table_sql, logger=None):
    """
    Create Table.
    :param conn: Database connection
    :param create_table_sql:
    :param logger: application logger or None
    :except: Database error
    """
    try:
        cur = conn.cursor()
        cur.execute(create_table_sql)
    except Error as e:
        if logger is not None:
            logger.fatal(e)
        raise e
