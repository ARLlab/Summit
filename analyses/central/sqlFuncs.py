"""
This script contains functions for easily getting into SQLlite databases
"""

import sqlite3
from sqlite3 import Error
import pandas as pd
import math





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
    if len(rows) > 100000:
        iterations = math.ceil(len(rows) / 100000)
        n = int(len(rows) / (iterations * 2))

        chunked_rows = [rows[i * n:(i + 1) * n] for i in range((len(rows) + n - 1) // n)]

        data = pd.DataFrame()
        for i in range(len(chunked_rows)):
            frame = pd.DataFrame(chunked_rows[i])
            data = data.append(frame, ignore_index=True)
            del frame
            print(f'Data Chunk {i} Appended')

    data.columns = ['id', 'date', '1', 'status', '2', '3', '4', '5', '6', 'pos', '7', '8', '9', '10', '11', 'ch4',
                    '12', '13', '14']
    badcols = list(pd.Series(range(1, 15)).astype(str))
    data.drop(badcols, axis=1, inplace=True)

    # create proper datetimes
    data['datetime'] = pd.to_datetime(data['date'])
    data.dropna(axis=0, inplace=True)
    data.reset_index(drop=True, inplace=True)

    # create fake date points
    data.insert(data.shape[1], 'rows', data.index.value_counts().sort_index().cumsum())

    print('Data Gathered')

    return data

