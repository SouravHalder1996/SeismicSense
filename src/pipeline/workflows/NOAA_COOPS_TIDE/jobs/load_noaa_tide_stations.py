import os, sys
from utils.helper import get_db_connection, get_base_dir
from common.logger import logging
from common.exception import CustomException
import sqlparse

BASE_DIR = get_base_dir()

def execute_query(conn, cur, file_path):
    with open(file_path, 'r') as f:
        sql = f.read()

    statements = sqlparse.split(sql)
    for i, statement in enumerate(statements):
        statement = statement.strip()
        if not statement or statement.startswith('--'):
            continue   
        cur.execute(statement)

    conn.commit()
    return cur

def load_data():
    conn = get_db_connection()
    cur = conn.cursor()
    TRANSFORMATION_PATH = os.path.join(BASE_DIR, "src", "pipeline", "workflows", "NOAA_COOPS_TIDE", "transformation")
    sql_files = [
        "transform_noaa_tide_stations.sql",
        "load_noaa_tide_stations.sql"
    ]
    try:
        for file in sql_files:
            SQL_FILE_PATH = os.path.normpath(os.path.join(BASE_DIR, TRANSFORMATION_PATH, file))
            cur = execute_query(conn, cur, SQL_FILE_PATH)
            logging.info(f"{file} executed successfully.")
            logging.info(f'{cur.statusmessage}')
            # logging.info(f'{cur.rowcount}')
            print(f"{file} executed successfully.")
            print(f'{cur.statusmessage}')
            # print(f'{cur.rowcount}')

    except Exception as e:
        raise CustomException(e, sys)
    
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    load_data()