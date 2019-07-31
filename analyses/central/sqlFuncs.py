"""
This script contains functions for easily getting into SQLlite databases
"""

import sqlite3
from sqlite3 import Error
import pandas as pd


def create_connection(db_file):
    """
    Connect to sqlite database, return an error if it cannot
    :param db_file: database path location
    """
    try:
        conn = sqlite3.connect(db_file)
        print('SQL Connection Created')
        return conn
    except Error as e:
        print(e)

    return None


def get_data(conn, startdate, enddate, cpd):
    """
    gather data between start and end dates
    :param conn: SQLlite3 connection
    :param startdate: starting date string , format ex. '2019-07-23 00:00:00'
    :param enddate: end date string, format ex. 2019-07-24 23:59:59
    :param cpd: the SQLlite database header of the data column
    :return:
    """

    cur = conn.cursor()
    # get data between start and end date
    cur.execute(f"SELECT * FROM {cpd} WHERE date BETWEEN '{startdate}' AND '{enddate}'")
    rows = cur.fetchall()

    # convert to clean dataframe
    data = pd.DataFrame(rows)
    data.columns = ['id', 'date', '1', 'status', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14',
                    'ch4', '15', '16', '17']

    # create proper datetimes
    data['datetime'] = pd.to_datetime(data['date'])
    data.dropna(axis=0, inplace=True)
    data.reset_index(drop=True, inplace=True)

    # create fake date points
    data.insert(data.shape[1], 'rows', data.index.value_counts().sort_index().cumsum())

    # drop poor columns
    badcols = list(range(1, 18))
    data.drop(badcols, axis=1, inplace=True)

    print('Data Gathered')

    return data