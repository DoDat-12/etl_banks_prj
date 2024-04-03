# Hands-on Lab:
# Acquiring and Processing Information on the World's Largest Banks

import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3
import numpy as np

# Exchange rate CSV path
# wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv

url = 'https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_csv_path = 'exchange_rate.csv'
output_csv_path = 'Largest_banks_data.csv'
table_extract_attribs = ['Name', 'MC_USD_Billion']
table_final_attribs = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'

# Task 1: Logging function


def log_process(message):
    timestamp_format = '%Y-%m-%d %H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open('code_log.txt', 'a') as f:
        f.write(timestamp + ': ' + message + '\n')


# Task 2: Extraction of data


def extract(url, table_attribs):
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')

    df = pd.DataFrame(columns=table_attribs)

    tbody_attribs = soup.find_all('tbody')
    tr_attribs = tbody_attribs[0].find_all('tr')
    for tr in tr_attribs:
        td_attribs = tr.find_all('td')
        if len(td_attribs) != 0 and td_attribs[1].find('a') is not None:
            df = pd.concat([df, pd.DataFrame([{table_attribs[0]: td_attribs[1].find_all('a')[1].contents[0],
                                               table_attribs[1]: float(td_attribs[2].text.rstrip('\n'))}])],
                           ignore_index=True)
    return df


def transform(df, exchange_csv_path):
    # Create exchange dictionary
    exchange_df = pd.read_csv(exchange_csv_path)
    exchange_dict = exchange_df.set_index('Currency').to_dict()['Rate']

    df['MC_GBP_Billion'] = [np.round(x * exchange_dict['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_dict['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_dict['INR'], 2) for x in df['MC_USD_Billion']]
    return df


def load_to_csv(df, output_csv_path):
    df.to_csv(output_csv_path)


def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    print(query_statement)
    df = pd.read_sql(query_statement, sql_connection)
    print(df)


# ETL
df = extract(url, table_extract_attribs)
log_process('Extracting data...')

df = transform(df, exchange_csv_path)
log_process('Transforming data...')

load_to_csv(df, output_csv_path)
log_process('Loading data to csv...')

sql_connection = sqlite3.connect(db_name)
load_to_db(df, sql_connection, table_name)
log_process('Loading data to sqlite database...')

run_query('SELECT * FROM Largest_banks', sql_connection)
run_query('SELECT AVG(MC_GBP_Billion) FROM Largest_banks', sql_connection)
run_query('SELECT Name from Largest_banks LIMIT 5', sql_connection)
