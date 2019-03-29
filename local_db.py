from dotenv import load_dotenv
import os
import psycopg2
import pandas as pd
import time

conn = None
df = None
df_keywords = None
df_nct = None

def create_env_vars():
    global hostname, database, username, password
    hostname = os.getenv('hostname')
    port = os.getenv('port')
    database = os.getenv('database')
    username = os.getenv('username')
    password = os.getenv('password')

def compute_results():
    print('computing results')
    conn = psycopg2.connect(host=hostname, database=database, user=username, password=password)
    # df = pd.read_sql('select name from ctgov.studies RIGHT JOIN ctgov.sponsors ON ctgov.studies.nct_id = ctgov.sponsors.nct_id', con=conn)
    df = pd.read_sql('select * from ctgov.studies', con=conn)
    print('done')
    return df

if __name__ == '__main__':
    load_dotenv()
    create_env_vars()
    conn = psycopg2.connect(host=hostname, database=database, user=username, password=password)
    start_time = time.time()
    results = compute_results()
    end_time = time.time()
    print(results)
    print(end_time - start_time)
