"""
This script connects to and gets data from a database. (insert the path of the intended databas)
"""

import sqlite3
from sqlite3 import Error
import pandas as pd
from datetime import datetime, date, timedelta
from windrose import WindroseAxes
import matplotlib.pyplot as plt
import matplotlib.cm as cm


def wind_data():
    wind_data_read = pd.read_csv(r'met_sum_insitu_1_obop_hour_2019.txt', header=None)
    date_index = []
    ws = []
    wd = []
    for index, row in wind_data_read.iterrows():
        string = str(row.values)
        ws.append(float(string[26:30]))
        wd.append(float(string[21:24]))
        date = string[6:19]
        date = date.replace(' ', '-')
        date = datetime.strptime(date, '%Y-%m-%d-%H').strftime('%Y-%m-%d::%H')
        date_index.append(date)


    wind_data = pd.DataFrame(list(zip(wd, ws)), columns=['wd', 'ws'], index = date_index)
    wind_data = wind_data[wind_data.wd != 999.0]

    return wind_data


def sql_connection():
    try:
        con = sqlite3.connect(dbpath)                       #establish connection to database
        print ("Connection successfully established!")
        return con
    except Error as e:
        print(e)
        return None       #prints error if cannot establish connection

def peaks_df(x):
    cursor = x.cursor()

    dfpeaks = pd.read_sql_query("select pa_line_id, name, pa, mr from peaks", x, index_col='pa_line_id')
    dfpeaks.dropna(inplace=True)
    return dfpeaks

def lines_df(x):
    cursor = x.cursor()
    dflines = pd.read_sql_query("select id, date from palines", x, index_col='id')

    return dflines


def get_compounds():
    '''name: name of the compound as a string
       type: enter 'pa' to get peak area data
            or 'mr' to get mixing ratio data'''
    indv = peaks_df(con)[peaks_df(con)['name'] != 'CH4_0']
    indv.drop(['name'], axis=1, inplace = True)

    indv = indv.join(lines_df(con))
    indv.dropna(subset=['date'], inplace=True)

    indv['date'] = pd.to_datetime(indv['date'], format='%Y-%m-%d %H:%M:%S')

    '''start_date = datetime(2019, 7, 1)
    end_date = start_date + timedelta(days=90)

    indv = indv[indv['date'] > start_date]
    indv = indv[indv['date'] < end_date]'''

    #indv['date'] = indv['date'].apply(lambda x: x.strftime('%Y-%m-%d::%H'))
    indv.set_index(keys='date', inplace=True)

    return indv

def windrose_mr():

    dframe = get_compounds().join(wind_data())
    #dframe = dframe[dframe['wd'] > 70][dframe['wd'] < 350]
    ax = WindroseAxes.from_ax()
    ax.bar(dframe['wd'].tolist(), dframe['mr'].tolist(), normed=True, opening=0.85, bins=10, nsector=32,
           cmap=cm.cool)
    ax.set_legend()
    plt.title('2019 July-September Wind Data for methane')
    locs, labels = plt.yticks()
    new_labels = [str(s) + '%' for s in labels]
    for i in range(len(new_labels)):
        new_labels[i] = new_labels[i][12:15] + new_labels[i][17]
    plt.yticks(locs, labels=new_labels)

    plt.show()

def create_csv():
    dframe = get_compounds()
    dframe.to_csv(r'C:\Users\ARL\Desktop\Summit\analyses\Connor\test_data\methane_2019_data.csv', sep=" ", header=True)

if __name__ == '__main__':

    dbname = r'summit_methane.sqlite'
    dbpath = r"C:\Users\ARL\Desktop\Summit_Databases_Sept2019\{}".format(dbname)

    con = sql_connection()
    windrose_mr()
    create_csv()

