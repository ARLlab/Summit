"""
This script connects to and gets data from a database. (insert the path of the intended databas)
"""

import sqlite3
from sqlite3 import Error
import csv
import pandas as pd


def sql_connection():
    try:
        con = sqlite3.connect(dbpath)                       #establish connection to database
        print ("Connection successfully established!")
        return con
    except Error as e:
        print(e)
        return None       #prints error if cannot establish connection

def createcsv(x):
    cursor = x.cursor()
    df = pd.read_sql_query("select pa_line_id, median, date from runs", con)
    df = df.dropna()
    print(df)


if __name__ == '__main__':

    dbname = r'summit_methane.sqlite'
    dbpath = r"C:\Users\ARL\Desktop\Summit_Databases_Sept2019\{}".format(dbname)

    con = sql_connection()
    createcsv(con)

