import yaml
import os
import psycopg2


def get_config(key):
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
    CONFIG_PATH = os.path.join(BASE_DIR, "config", "config.yaml")

    with open(CONFIG_PATH, 'r') as file:
        config = yaml.safe_load(file)[key]
    
    return config

def get_base_dir():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

def get_db_connection():
    db = get_config('db')
    conn = psycopg2.connect(
        dbname = db['dbname'],
        user = db['user'],
        password = db['password'],
        host = db['host'],
        port = db['port']
    )
    return conn

def save_csv(file_path, file_name, file):
    if not os.path.exists(file_path):
        os.makedirs(file_path)
    
    full_path = os.path.join(file_path, file_name)
    file.to_csv(full_path, index=False)

    return full_path

def move_file(src, dest, file_name):
    if not os.path.exists(dest):
        os.makedirs(dest)
    
    dest_full_path = os.path.join(dest, file_name)
    os.replace(src, dest_full_path)

    return dest_full_path