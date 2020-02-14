import pandas

import sqlite3
from sqlite3 import Error
import pandas as pd
from datetime import datetime
from windrose import WindroseAxes
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np
import plotly.express as px


def wind_data():
    wind_data_read = pd.read_csv(r'met_sum_insitu_1_obop_hour_2018.txt', header=None)
    date_index = []
    ws = []
    wd = []
    for index, row in wind_data_read.iterrows():
        string = str(row.values)
        ws.append(float(string[26:30]))
        wd.append(float(string[21:24]))
        date = string[6:19]
        date = date.replace(' ', '-')
        date = datetime.strptime(date, '%Y-%m-%d-%H').strftime('%m-%d::%H')
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

    dfpeaks = pd.read_sql_query("select line_id, name, pa, mr from peaks", x, index_col='line_id')
    dfpeaks.fillna(999.999999, inplace=True)

    return dfpeaks

def lines_df(x):
    cursor = x.cursor()
    dflines = pd.read_sql_query("select id, date from nmhclines", x, index_col='id')

    return dflines


def indv_compound(name):
    '''name: name of the compound as a string
       type: enter 'pa' to get peak area data
            or 'mr' to get mixing ratio data'''
    indv = peaks_df(con)[peaks_df(con)['name'] == name]
    indv.rename(columns={'pa': f'{name}_pa', 'mr': f'{name}_mr'}, inplace=True)
    indv.drop(['name'], axis=1, inplace = True)
    indv = indv.join(lines_df(con))
    indv['date'] = pd.to_datetime(indv['date'], format='%Y-%m-%d %H:%M:%S')
    indv['date'] = indv['date'].apply(lambda x: x.strftime('%m-%d::%H'))
    indv = indv[indv[f'{name}_pa'] != 999.999999]

    indv.set_index(keys='date', inplace=True)

    return indv

def windrose_pa(name):
    dframe = indv_compound(name).join(wind_data())
    #dframe = dframe[dframe['wd'] > 70][dframe['wd'] < 350]
    ax = WindroseAxes.from_ax()
    ax.bar(dframe['wd'].tolist(), dframe[f'{name}_pa'].tolist(), normed=True, opening=0.85, bins=10, nsector=32,
           cmap=cm.cool)
    ax.set_legend()
    plt.title('2018 Summit Wind Data')
    locs, labels = plt.yticks()
    new_labels = [str(s) + '%' for s in labels]
    for i in range(len(new_labels)):
        new_labels[i] = new_labels[i][12:15] + new_labels[i][17]
    plt.yticks(locs, labels=new_labels)

    plt.show()

if __name__ == '__main__':

    dbname = r'summit_voc.sqlite'
    dbpath = r"C:\Users\ARL\Desktop\Summit_Databases_Sept2019\{}".format(dbname)

    con = sql_connection()


    windrose_pa('toluene')





    '''dframe_2019 = pd.read_excel(r'C:\Users\ARL\Desktop\Summit_GC_2019\NMHC_results\Ambient_2019.xlsx', header=None)
    dframe_2019.set_index(0, inplace=True)
    dframe_transposed = dframe_2019.T
    dframe_2019 = dframe_transposed.loc[:, [compound]]
    dframe_2019 = dframe_2019.iloc[:, [j for j, c in enumerate(dframe_2019.columns) if j not in [0, 2, 3]]]

    dframe_2019['file'] = dframe_transposed.iloc[:, 0]
    dframe_2019['decimal_date'] = dframe_transposed.iloc[:, 39]
    dframe_2019.dropna(inplace=True, subset=['file'])
    dframe_2019['decmial_date_year'] = [(2019 + float(row[0])/365) for row in dframe_2019[['decimal_date']].values]


    dframe_2019['Year'] = dframe_2019['file'].apply(lambda x: int(str(x)[0:4]))
    dframe_2019['Yearly_Day'] = dframe_2019['file'].apply(lambda x: int(str(x)[4:7]))
    dframe_2019['Hour'] = dframe_2019['file'].apply(lambda x: int(str(x)[7:9]))
    dframe_2019['Minute'] = dframe_2019['file'].apply(lambda x: int(str(x)[9:11]))
    dframe_2019['Second'] = dframe_2019['file'].apply(lambda x: int(str(x)[11:13]))

    base_date = datetime(year=2019, month=1, day=1)
    dframe_2019['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                            seconds=int(row[3])
                                            ) for row in
                      dframe_2019[[
                          'Yearly_Day',
                          'Hour',
                          'Minute',
                          'Second']].values]

    dframe_2019.drop(columns=['file', 'Hour', 'Minute','Yearly_Day','Second','Year'], inplace=True)


    dframe_2019.dropna(inplace=True)
    dframe_2019.set_index('date', inplace=True)
    dframe_2019.rename(columns={compound: f'{compound}_mr'}, inplace=True)
    #dframe_2019 = dframe_2019[dframe_2019[f'{compound}_mr'] != 0]


    dframe_2018 = pd.read_excel(r'Z:\Data\Summit_GC\Summit_GC_2018\NMHC_results\Ambient_2018_V2.xlsx',
                                               header=None)
    dframe_2018.set_index(0, inplace=True)
    dframe_transposed = dframe_2018.T
    dframe_2018 = dframe_transposed.loc[:, [compound]]
    dframe_2018 = dframe_2018.iloc[:, [j for j, c in enumerate(dframe_2018.columns) if j not in [0, 2, 3]]]

    dframe_2018['file'] = dframe_transposed.iloc[:, 0]
    dframe_2018['decimal_date'] = dframe_transposed.iloc[:, 39]
    dframe_2018.dropna(inplace=True, subset=['file'])
    dframe_2018['decmial_date_year'] = [(2018 + float(row[0]) / 365) for row in dframe_2018[['decimal_date']].values]

    dframe_2018['Year'] = dframe_2018['file'].apply(lambda x: int(str(x)[0:4]))
    dframe_2018['Yearly_Day'] = dframe_2018['file'].apply(lambda x: int(str(x)[4:7]))
    dframe_2018['Hour'] = dframe_2018['file'].apply(lambda x: int(str(x)[7:9]))
    dframe_2018['Minute'] = dframe_2018['file'].apply(lambda x: int(str(x)[9:11]))
    dframe_2018['Second'] = dframe_2018['file'].apply(lambda x: int(str(x)[11:13]))

    base_date = datetime(year=2018, month=1, day=1)
    dframe_2018['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                                 seconds=int(row[3])
                                                 ) for row in
                           dframe_2018[[
                               'Yearly_Day',
                               'Hour',
                               'Minute',
                               'Second']].values]

    dframe_2018.drop(columns=['file', 'Hour', 'Minute', 'Yearly_Day', 'Second', 'Year'], inplace=True)

    dframe_2018.dropna(inplace=True)
    dframe_2018.set_index('date', inplace=True)
    dframe_2018.rename(columns={compound: f'{compound}_mr'}, inplace=True)


    dframe_2017 = pd.read_excel(r'Z:\Data\Summit_GC\Summit_GC_2017\NMHC_results\Ambient_2017.xlsx',
                                              header=None)
    dframe_2017.set_index(0, inplace=True)
    dframe_transposed = dframe_2017.T
    dframe_2017 = dframe_transposed.loc[:, [compound]]
    dframe_2017 = dframe_2017.iloc[:, [j for j, c in enumerate(dframe_2017.columns) if j not in [0, 2, 3]]]

    dframe_2017['file'] = dframe_transposed.iloc[:, 0]
    dframe_2017['decimal_date'] = dframe_transposed.iloc[:, 39]
    dframe_2017.dropna(inplace=True, subset=['file'])
    dframe_2017['decmial_date_year'] = [(2017 + float(row[0]) / 365) for row in dframe_2017[['decimal_date']].values]

    dframe_2017['Year'] = dframe_2017['file'].apply(lambda x: int(str(x)[0:4]))
    dframe_2017['Yearly_Day'] = dframe_2017['file'].apply(lambda x: int(str(x)[4:7]))
    dframe_2017['Hour'] = dframe_2017['file'].apply(lambda x: int(str(x)[7:9]))
    dframe_2017['Minute'] = dframe_2017['file'].apply(lambda x: int(str(x)[9:11]))
    dframe_2017['Second'] = dframe_2017['file'].apply(lambda x: int(str(x)[11:13]))

    base_date = datetime(year=2017, month=1, day=1)
    dframe_2017['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                                 seconds=int(row[3])
                                                 ) for row in
                           dframe_2017[[
                               'Yearly_Day',
                               'Hour',
                               'Minute',
                               'Second']].values]

    dframe_2017.drop(columns=['file', 'Hour', 'Minute', 'Yearly_Day', 'Second', 'Year'], inplace=True)

    dframe_2017.dropna(inplace=True)
    dframe_2017.set_index('date', inplace=True)
    dframe_2017.rename(columns={compound: f'{compound}_mr'}, inplace=True)
    '''
