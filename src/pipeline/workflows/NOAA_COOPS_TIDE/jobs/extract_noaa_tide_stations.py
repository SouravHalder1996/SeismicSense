import requests, os, sys
from datetime import datetime
import pandas as pd
from psycopg2.extras import execute_values
from utils import helper
from common.logger import logging
from common.exception import CustomException

BASE_DIR = helper.get_base_dir()
config = helper.get_config('noaa_tide')
ingestion_delta_path = os.path.normpath(os.path.join(BASE_DIR, config["paths"]["ingestion_delta_folder"]))
ingestion_archive_path = os.path.normpath(os.path.join(BASE_DIR, config["paths"]["ingestion_archive_folder"]))

STATIONS_API_URL = config.get('STATIONS_API_URL')

def extract_all_stations():

    logging.info("Extracting all NOAA tide stations from API")
    print("Extracting all NOAA tide stations from API")
    params = {
    'type': 'waterlevels',
    'format':'json'
    }   
    response = requests.get(STATIONS_API_URL, params=params)
    if response.status_code == 200:
        stations = response.json()
        return stations
    else:
        raise CustomException(f"Failed to fetch stations. Status code: {response.status_code}", sys)

def write_to_dataframe(stations):
    logging.info("Writing stations data to DataFrame")
    print("Writing stations data to DataFrame")
    records = []
    for station in stations.get('stations', []):
        records.append({
            'station_id': station.get('id'),
            'station_name': station.get('name'),
            'state' : station.get('state'),
            'latitude': station.get('lat'),
            'longitude': station.get('lng'),
            'timezone': station.get('timezone'),
            'timezone_offset': station.get('timezonecorr'),
            'tidal' : station.get('tidal'),
            'greatlakes': station.get('greatlakes'),
            'shefcode': station.get('shefcode'),
            'affiliations': station.get('affiliations'),
            'tide_type': station.get('tideType'),
            'source_filename' : csv_filename
        })

    df = pd.DataFrame(records)
    return df

def insert_into_staging_table(conn, df):
    
    logging.info("Inserting data into staging table")
    print("Inserting data into staging table")

    try:
        cur = conn.cursor()

        trunc_query = "TRUNCATE TABLE stg_noaa_tide_stations;"
        cur.execute(trunc_query)
        logging.info(f'{trunc_query} -- {cur.statusmessage}')
        print(f'{trunc_query} -- {cur.statusmessage}')

        columns = [
            "station_id", 
            "station_name", 
            "state", 
            "latitude", 
            "longitude", 
            "timezone",
            "timezone_offset", 
            "tidal", 
            "greatlakes", 
            "shefcode", 
            "affiliations",
            "tide_type", 
            "source_filename"
        ]
        insert_query = f"""
            INSERT INTO stg_noaa_tide_stations ({', '.join(columns)})
            VALUES %s
        """

        data = [tuple(row[col] for col in columns) for _, row in df.iterrows()]
        execute_values(cur, insert_query, data)
        logging.info(f'{insert_query} -- {cur.statusmessage} -- {cur.rowcount}')
        print(f'{insert_query} -- {cur.statusmessage} -- {cur.rowcount}')

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise CustomException(f"Error inserting data into staging table: {e}", sys)
    
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":

    logging.info("---------------=============== Extraction Started ===============---------------")
    print("---------------=============== Extraction Started ===============---------------")

    stations = extract_all_stations()
    logging.info(f"Total stations fetched: {stations.get('count', 0)}")
    print(f"Total stations fetched: {stations.get('count', 0)}")

    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M")
    csv_filename = f"noaa_tide_stations_{timestamp_str}.csv"

    df = write_to_dataframe(stations)
    if df is None or df.empty:
        logging.info("No stations data available to write to DataFrame.")
        print("No stations data available to write to DataFrame.")
    else:
        logging.info(f"DataFrame created with {len(df)} records.")
        print(f"DataFrame created with {len(df)} records.")
        delta_path = helper.save_csv(ingestion_delta_path, csv_filename, df)
        logging.info(f"Data file saved at {delta_path}")
        print(f"Data file saved at {delta_path}")

        conn = helper.get_db_connection()
        insert_into_staging_table(conn, df)
        logging.info("Data successfully loaded into staging table.")
        print("Data successfully loaded into staging table.")
        archive_path = helper.move_file(delta_path, ingestion_archive_path, csv_filename)
        logging.info(f"File moved to archive at {archive_path}")
        print(f"File moved to archive at {archive_path}")

    logging.info("---------------=============== Extraction Completed ===============---------------")
    print("---------------=============== Extraction Completed ===============---------------")


