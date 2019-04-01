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


def get_studies():
    return pd.read_sql('select * from ctgov.studies', con=conn)


def get_keywords():
    return pd.read_sql('select * from ctgov.keywords', con=conn)


def get_sponsors():
    return pd.read_sql('select * from ctgov.sponsors', con=conn)


def join_tables(first_table_name, second_table_name):
    return pd.read_sql('select * from ' +
          first_table_name +
          ' RIGHT JOIN ' +
          second_table_name + ' ON ' + first_table_name + '.nct_id = ' + second_table_name + '.nct_id', con=conn)


if __name__ == '__main__':
    load_dotenv()
    create_env_vars()
    conn = psycopg2.connect(host=hostname, database=database, user=username, password=password)
    start_time = time.time()
    results = join_tables('ctgov.studies', 'ctgov.keywords')
    end_time = time.time()
    # print(results)
    print(end_time - start_time)

    start_time = time.time()
    pd.read_sql('select * from ctgov.studies RIGHT JOIN ctgov.keywords ON ctgov.studies.nct_id = ctgov.keywords.nct_id', con=conn)
    end_time = time.time()
