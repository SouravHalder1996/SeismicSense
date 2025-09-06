import requests, os, sys
from datetime import datetime, timedelta, timezone
import pandas as pd
from common.exception import CustomException
from common.logger import logging
from psycopg2.extras import execute_values
from utils import helper
import time


BASE_DIR = helper.get_base_dir()
config = helper.get_config("usgs")

QUERY_API_URL = config["QUERY_API_URL"]
COUNT_API_URL = config["COUNT_API_URL"]
format = config["format"]
limit = config["limit"]
delta_days = config["delta_days"]
full_load = config["full_load"]
ingestion_delta_path = os.path.normpath(os.path.join(BASE_DIR, config["paths"]["ingestion_delta_folder"]))
ingestion_archive_path = os.path.normpath(os.path.join(BASE_DIR, config["paths"]["ingestion_archive_folder"]))

def get_params(load_type):
    if load_type == "full":
        end_time = datetime.now(timezone.utc)
        start_time = datetime.strptime("2020-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        params = {
        "starttime" : start_time.isoformat(),
        "endtime" : end_time.isoformat(),
        "format" : format,
        "orderby" : "time"
        }

    if load_type == "delta":
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=delta_days)
        params = {
        "starttime": start_time.isoformat(),
        "endtime": end_time.isoformat(),
        "format": format,
        "orderby": "time"
        }
    
    return params, end_time

def get_total_count(params):
    logging.info("Checking total records count from USGS API.")
    print("Checking total records count from USGS API.")
    response = requests.get(COUNT_API_URL, params=params)
    if response.status_code == 200:
        return response.json()['count']
    else:
        raise CustomException(f"Failed to fetch total count from {COUNT_API_URL}. Status Code: {response.status_code}", sys)
    
def extract_data(params):
    total_count = get_total_count(params)
    chunks = (total_count // limit) + 1
    logging.info(f"Total records found to process: {total_count}, in {chunks} chunks.")
    print(f"Total records found to process: {total_count}, in {chunks} chunks.")

    session = requests.Session()
    features = []
    failed_chunks = []
    logging.info("Data extraction started from USGS API.")
    print("Data extraction started from USGS API.")
    for chunk in range(chunks):
        params['limit'] = limit
        params['offset'] = chunk * limit + 1
        response = session.get(QUERY_API_URL, params=params)
        logging.info(f'{datetime.now().strftime("%Y-%m-%d %H:%M")} --> Chunk: {chunk} --> Response Status Code: {response.status_code}')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M")} --> Chunk: {chunk} --> Response Status Code: {response.status_code}')
        if response.status_code == 200:
            features.extend(response.json().get("features", []))
            time.sleep(10)
        else:
            failed_chunks.append(chunk)

    if failed_chunks:
        logging.info(f"Failed chunks: {failed_chunks}")
        print(f"Failed chunks: {failed_chunks}")
        logging.info("Retrying failed chunks...")
        print("Retrying failed chunks...")
        for chunk in failed_chunks:
            params['limit'] = limit
            params['offset'] = chunk * limit + 1
            response = session.get(QUERY_API_URL, params=params)
            logging.info(f'{datetime.now().strftime("%Y-%m-%d %H:%M")} --> Chunk: {chunk} --> Response Status Code: {response.status_code}')
            print(f'{datetime.now().strftime("%Y-%m-%d %H:%M")} --> Chunk: {chunk} --> Response Status Code: {response.status_code}')
            if response.status_code == 200:
                features.extend(response.json().get("features", []))
                time.sleep(10)
            else:
                retry_count = 3
                while retry_count > 0:
                    time.sleep(15)
                    logging.info(f'Retrying... {retry_count}')
                    print(f'Retrying... {retry_count}')
                    response = session.get(QUERY_API_URL, params=params)
                    logging.info(f'{datetime.now().strftime("%Y-%m-%d %H:%M")} --> Chunk: {chunk} --> Response Status Code: {response.status_code}')
                    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M")} --> Chunk: {chunk} --> Response Status Code: {response.status_code}')
                    if response.status_code == 200:
                        features.extend(response.json().get("features", []))
                        break
                    retry_count -= 1
    logging.info("Data extracted successfully from USGS API.")
    print("Data extracted successfully from USGS API.")
    return features

def write_to_dataframe(features):
    if not features:
        logging.info("No features to process.")
        print("No features to process.")
        return
    records = []
    for feature in features:
        properties = feature.get("properties", {})
        geometry = feature.get("geometry", {})
        coords = geometry.get("coordinates", [None, None, None])
        
        records.append({
            "quake_id": feature.get("id"),
            "latitude": coords[1],
            "longitude": coords[0],
            "depth_km": coords[2],
            "magnitude": properties.get("mag"),
            "magnitude_type": properties.get("magType"),
            "place_name": properties.get("place"),
            "event_time": properties.get("time"),
            "updated_time": properties.get("updated"),
            "url": properties.get("url"),
            "detail_url": properties.get("detail"),
            "felt_reports": properties.get("felt"),
            "community_intensity": properties.get("cdi"),
            "modified_mercalli_intensity": properties.get("mmi"),
            "alert_level": properties.get("alert"),
            "status": properties.get("status"),
            "tsunami": properties.get("tsunami"),
            "significance": properties.get("sig"),
            "network": properties.get("net"),
            "network_event_code": properties.get("code"),
            "number_of_stations": properties.get("nst"),
            "distance_to_nearest_station": properties.get("dmin"),
            "rms": properties.get("rms"),
            "azimuthal_gap": properties.get("gap"),
            "event_type": properties.get("type"),
            "source_file_name": csv_filename
        })

    return pd.DataFrame(records)

def insert_into_staging_table(conn, df):
    if df.empty:
        logging.info("DataFrame is empty. No data to insert.")
        print("DataFrame is empty. No data to insert.")
        return
    
    try:
        cur = conn.cursor()
        columns = [
            "quake_id", 
            "latitude", 
            "longitude", 
            "depth_km", 
            "magnitude", 
            "magnitude_type", 
            "place_name",
            "event_time", 
            "updated_time", 
            "url", 
            "detail_url", 
            "felt_reports",
            "community_intensity", 
            "modified_mercalli_intensity", 
            "alert_level", 
            "status", 
            "tsunami",
            "significance", 
            "network", 
            "network_event_code",
            "number_of_stations", 
            "distance_to_nearest_station", 
            "rms", 
            "azimuthal_gap",
            "event_type", 
            "source_file_name"
        ]

        trunc_query = f"""
            TRUNCATE TABLE stg_usgs_earthquake;
        """
        cur.execute(trunc_query)
        logging.info(f'{trunc_query} -- {cur.statusmessage}')
        print(f'{trunc_query} -- {cur.statusmessage}')

        insert_query = f"""
            INSERT INTO stg_usgs_earthquake ({', '.join(columns)})
            VALUES %s
        """
        data = [tuple(row[col] for col in columns) for _, row in df.iterrows()]
        execute_values(cur, insert_query, data)
        logging.info(f'{insert_query} -- {cur.statusmessage} -- {cur.rowcount}')
        print(f'{insert_query} -- {cur.statusmessage} -- {cur.rowcount}')

    except Exception as e:
        raise CustomException(e, sys)

    finally:
        conn.commit()
        cur.close()
        conn.close()


if __name__ == "__main__":

    logging.info("---------------=============== Extraction Started ===============---------------")
    print("---------------=============== Extraction Started ===============---------------")
    load_type = "delta" if not full_load else "full"
    logging.info(f'Load type: {load_type}')
    print(f'Load type: {load_type}')
    params, end_time = get_params(load_type)
    logging.info(f"Params: {params}")
    print(f"Params: {params}")
    timestamp_str = end_time.strftime("%Y%m%d_%H%M")

    features = extract_data(params)
    logging.info(f"Extracted {len(features)} features.")
    print(f"Extracted {len(features)} features.")

    csv_filename = f"usgs_earthquake_{timestamp_str}.csv"
    
    df = write_to_dataframe(features)
    if df is not None:
        logging.info(f"DataFrame created with {len(df)} records.")
        print(f"DataFrame created with {len(df)} records.")
        path = helper.save_csv(ingestion_delta_path, csv_filename, df)
        logging.info(f"Data written at {path}")
        print(f"Data written at {path}")
    else:
        logging.info("No data to save.")
        print("No data to save.")

    conn = helper.get_db_connection()
    insert_into_staging_table(conn, df)
    logging.info("Data successfully loaded into staging table.")
    print("Data successfully loaded into staging table.")
    archive_path = helper.move_file(path, ingestion_archive_path, csv_filename)
    logging.info(f"File moved to archive at {archive_path}")
    print(f"File moved to archive at {archive_path}")

