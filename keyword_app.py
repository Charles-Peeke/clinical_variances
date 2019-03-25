from dotenv import load_dotenv
import os
import psycopg2
import pandas as pd
import time
import matplotlib.pyplot as plt

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


def get_keywords():
    print('computing keywords')
    start_time = time.time()

    global df_keywords
    df_keywords = pd.read_sql('SELECT studies.nct_id, downcase_name FROM '
                              'studies RIGHT JOIN keywords '
                              'ON studies.nct_id = keywords.nct_id ', con=conn)
    print('selected keywords: %.2f sec' %(time.time()-start_time))


if __name__ == '__main__':
    load_dotenv()
    create_env_vars()
    conn = psycopg2.connect(host=hostname, database=database, user=username, password=password)
    get_keywords()
    key_dict = {}
    start_time = time.time()
    print('Collecting Keyword variances')
    for nct_id, keyw in zip(df_keywords['nct_id'], df_keywords['downcase_name']):
        if nct_id in key_dict:
            key_dict[nct_id] += 1
        else:
            key_dict[nct_id] = 1
    print('Keyword variances Calculated: %.2f sec' %(time.time()-start_time))
    ids = key_dict.keys()
    counts = key_dict.values()
    plt.bar(ids, counts)
    plt.show()
