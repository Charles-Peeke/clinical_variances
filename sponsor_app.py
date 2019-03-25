from dotenv import load_dotenv
import os
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt


conn = None
df = None


def create_variables():
    global hostname, port, database, username, password
    hostname = os.getenv('hostname')
    port = os.getenv('port')
    database = os.getenv('database')
    username = os.getenv('username')
    password = os.getenv('password')


def compute_results():
    print('computing results')
    conn = psycopg2.connect(host=hostname, database=database, user=username, password=password)
    df = pd.read_sql('select name from studies RIGHT JOIN sponsors ON studies.nct_id = sponsors.nct_id', con=conn)
    # df = pd.read_sql('select * from studies', con=conn)
    print('done')
    return df


def get_unique_sponsor_names(df):
    names = []
    for name in df['name'].tolist():
        if name not in names:
            names.append(name)
    return names


def write_list_to_file(list):
    with open('unique_sponsor_names.txt', 'w') as f:
        for item in list:
            f.write("%s\n" % item)
    print("New list created")


if __name__ == '__main__':
    load_dotenv()
    create_variables()
    df = compute_results()
    merck_names = {}
    for name in df['name'].tolist():
        if "Merck" in name or "MSD" in name:
            if name not in merck_names:
                merck_names[name] = 1
            else:
                merck_names[name] += 1

    # plt.hist(merck_names.keys(), bins=len(merck_names.keys()))
    # plt.bar(merck_names.keys(), merck_names.values())
    # plt.xlabel('name')
    # plt.ylabel('count')
    # plt.xticks(rotation='vertical')
    # plt.ylim(0,400)
    # plt.show()

    # merck_names['Merck Sharp & Dohme Corp.'] = 100
    # merck_names['Merck KGaA, Darmstadt, Germany'] = 100
    plt.pie(merck_names.values(), labels=merck_names.keys())
    plt.show()
