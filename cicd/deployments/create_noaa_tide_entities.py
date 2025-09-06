import os
from utils.helper import get_db_connection, get_base_dir
from src.logger import logging

BASE_DIR = get_base_dir()

def execute_query(conn, file_path):
    with open(file_path, 'r') as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    return cur

def create_entities():
    conn = get_db_connection()
    TABLE_PATH = os.path.join(BASE_DIR, "src", "pipeline", "workflows", "NOAA_COOPS_TIDE", "ddl", "tables")
    table_files = [
        "stg_noaa_tide_stations.sql",
        "dm_t_noaa_tide_stations_dim.sql"
    ]
    for file in table_files:
        FILE_PATH = os.path.normpath(os.path.join(BASE_DIR, TABLE_PATH, file))
        cur = execute_query(conn, FILE_PATH)
        logging.info(f"{cur.statusmessage} {file}")
        print(f"{cur.statusmessage} {file}")

    # VIEW_PATH = os.path.join(BASE_DIR, "src", "pipeline", "workflows", "NOAA_COOPS_TIDE", "ddl", "views")
    # view_files = [
    #     "dm_usgs_earthquake_fact.sql",
    #     "dm_usgs_earthquake_recent.sql",
    #     "dm_usgs_earthquake_summary.sql"
    # ]
    # for file in view_files:
    #     FILE_PATH = os.path.normpath(os.path.join(BASE_DIR, VIEW_PATH, file))
    #     cur = execute_query(conn, FILE_PATH)
    #     logging.info(f"{cur.statusmessage} {file}")
    #     print(f"{cur.statusmessage} {file}")

    conn.close()

if __name__ == '__main__':
    create_entities()