import sys
from os import path
import pymysql
import db_conn

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from db_conn import db_conn

CONFIG_FILE = "../config.live.ini"
APP_PATH = path.abspath(path.dirname(__file__))


class DatabaseInteraction:
    def __init__(self):
        # config file for db,  ie config.live.ini
        self.filename = path.join(APP_PATH, CONFIG_FILE)

        self.conn_obj = db_conn(self.filename)
        self.db_name = "koa"
        self.db = None

    def connect_db(self, db_name, second_try=False):
        conn = self.conn_obj.connect(db_name)
        if not conn:
            print("ought oh!")
        try:
            curse = conn.cursor(pymysql.cursors.DictCursor)
        except:
            if second_try:
                sys.exit(f"could not connect to the database: {db_name}")
            self.connect_db(db_name, second_try=True)

        return curse

    def close_db_connection(self, db_name):
        self.conn_obj.close(db_name)

    def get_db_connection(self):
        return self.db

    def make_query(self, query, params, db_name=None):
        """
        Query the DB.  Opening/Closing connection avoids issues with the cursor
        crashing.

        :param query:
        :param params: (tuple) the escaped parameters for the query string
        :return:
        """
        if db_name:
            self.db = self.connect_db(db_name)
        else:
            self.db = self.connect_db(self.db_name)

        if params:
            self.db.execute(query, params)
        else:
            self.db.execute(query)

        if 'UPDATE' in query.upper():
            result = self.db.rowcount
        else:
            result = self.db.fetchall()

        self.close_db_connection(self.db_name)

        return result
