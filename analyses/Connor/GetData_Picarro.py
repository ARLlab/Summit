import sqlite3
from sqlite3 import Error
import pandas as pd
from datetime import datetime, date, timedelta
import ast
from windrose import WindroseAxes
import matplotlib.cm as cm
import matplotlib.pyplot as plt
import numpy as np
from GetData_voc import wind_data

def sql_connection(dbpath):
    try:
        con = sqlite3.connect(dbpath)                       #establish connection to database
        print ("Connection successfully established!")
        return con
    except Error as e:
        print(e)
        return None       #prints error if cannot establish connection

def picarro_df(con, start, end):
    start_date = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')

    dframe = pd.read_sql('select date, co, co2, ch4 from data', con, index_col='date')
    dframe.index = pd.to_datetime(dframe.index)
    dframe = dframe.resample('60min').median()  #includes start date but not end date
    dframe['decimal_date'] = dframe.index.year + (dframe.index.dayofyear - 1 + (dframe.index.hour + (
        dframe.index.minute + (dframe.index.second)/60)/60)/24)/365

    dframe = dframe.loc[dframe.index < end_date]
    dframe = dframe.loc[dframe.index > start_date]
    return dframe

def plot_picarro(compounds, start, end):
    start_string = datetime.strptime(start, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
    end_string = datetime.strptime(end, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

    for compound in compounds:
        dframe = picarro_df(con, start, end)

        plt.scatter(dframe.decimal_date, dframe[f'{compound}'], s=12, c='blue', alpha=0.5)
        plt.ylabel('Mixing Ratio')
        plt.xlabel('Decimal Year')
        plt.suptitle(f'{compound} mixing ratio ({start_string} to {end_string})')
        plt.grid(True, linestyle='--')

        plt.savefig(fr'C:\Users\ARL\Desktop\Summit\analyses\Connor\plots\Methane_Picarro\{compound}_picarro_mr')
        plt.clf()

def windrose_picarro(compounds, start, end):
    for compound in compounds:
        compound_df = picarro_df(con, start, end)
        compound_df.index = compound_df.index.strftime('%Y-%m-%d-%H')
        start_string = datetime.strptime(start, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
        end_string = datetime.strptime(end, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

        dframe = compound_df.join(wind_data())
        dframe.dropna(inplace=True)
        # dframe = dframe[dframe['wd'] > 72][dframe['wd'] < 342]
        ax = WindroseAxes.from_ax()
        ax.bar(dframe['wd'].tolist(), dframe[f'{compound}'].tolist(), normed=True, opening=0.85, bins=10, nsector=32,
               cmap=cm.cool)
        ax.set_legend()
        plt.title(f'{start_string} to {end_string} Wind Data for methane')
        locs, labels = plt.yticks()
        new_labels = [str(s) + '%' for s in labels]
        for i in range(len(new_labels)):
            new_labels[i] = new_labels[i][12:15] + new_labels[i][17]
        plt.yticks(locs, labels=new_labels)

        plt.show()


if __name__ == '__main__':
    dbname = r'summit_picarro.sqlite'
    dbpath = r"C:\Users\ARL\Desktop\Summit_Databases_Sept2019\{}".format(dbname)
    con = sql_connection(dbpath)

    picarro_compounds = ['co', 'co2', 'ch4']

    # windrose_picarro(['ch4'], '2019-5-1 0:0:0', '2020-1-1 0:0:0')
    #print(picarro_df(con, '2019-1-1 0:0:0', '2019-9-30 0:0:0'))
    plot_picarro(picarro_compounds, '2019-5-1 0:0:0', '2020-1-1 0:0:0')



