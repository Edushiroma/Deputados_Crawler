import traceback
from pyhive import hive
import configparser
import pandas as pd
import os

class Connector():
    def __init__(self, config_path='db.ini'):
        self.config = configparser.ConfigParser()
        self.config["hive"] = {
            "HOST" : os.getenv("HIVE_HOST"),
            "PORT" : os.getenv("HIVE_PORT"),
            "USER" : os.getenv("HIVE_USER"),
            "PWD" : os.getenv("HIVE_PWD"),
            "AUTH" : os.getenv("HIVE_AUTH"),
            "DATASET" : os.getenv("HIVE_DATASET"),
            "STAGE" : os.getenv("HIVE_STAGE")
        }


    def connect(self):
        return hive.Connection(host=self.config.get('hive', 'HOST'),
                        port=int(self.config.get('hive', 'PORT')),
                        username=self.config.get('hive', 'USER'),
                        password=self.config.get('hive', 'PWD'),
                        auth=self.config.get('hive', 'AUTH')
                       )
    
    def test_conn(self):
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        print(cursor.fetchall())

    def execute_query(self, query):
        conn = self.connect()
        result = None

        try:
            conn = Connector().connect()
            cursor = conn.cursor()
            cursor.execute(query)

            result = cursor.fetchone()
            conn.commit()
            conn.close()

        except:
            print(traceback.print_exc())
            conn.close()
            exit(1)

        return result

if __name__=='__main__':
    Connector().test_conn()
