"""
This script connects to and gets data from a database. (insert the path of the intended databas)
"""

import sqlite3
from sqlite3 import Error

dbname = str(input('enter database path:'))  #ask user for name of database
dbpath = r"C:\Users\ARL\Desktop\Summit_Databases_Sept2019\{}".format(dbname)

def sql_connection():
    try:
        con = sqlite3.connect(dbpath)                       #establish connection to database
        print ("Connection successfully established!")
        return con
    except Error as e:
        print(e)
        return None       #prints error if cannot establish connection

con = sql_connection()

def sql_fetch(x):
    cursor = x.cursor()
    cursor.execute('SELECT * FROM  peaks ')
    rows = cursor.fetchall()
    for row in rows:
        print(row)

sql_fetch(con)


#example path: C:\Users\ARL\Desktop\Summit_Databases_Sept2019\summit_methane.sqlite
