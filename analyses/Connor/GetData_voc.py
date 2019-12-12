import sqlite3
from sqlite3 import Error
import pandas as pd
from datetime import datetime, date, timedelta
from windrose import WindroseAxes
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np
import plotly.express as px
import ast

from read_voc_excel import excel_voc, excel_blanks, excel_trap_blanks, excel_rf_BH, excel_rf_Brad6


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
        date = datetime.strptime(date, '%Y-%m-%d-%H') #.strftime('%Y-%m-%d::%H')
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

    dfpeaks = pd.read_sql_query("select line_id, name, mr from peaks", x, index_col='line_id')
    dfpeaks.fillna(999.999999, inplace=True)

    return dfpeaks

def lines_df(x):
    cursor = x.cursor()
    dflines = pd.read_sql_query("select id, date from nmhclines", x, index_col='id')

    return dflines


def indv_compound(name):
    '''creates a dataframe with columns ethane_pa, ethane_mr, and
    the date formated yyyy-mm-dd HH-MM-SS. Enter the name of the
    compound as a string'''
    indv = peaks_df(con)[peaks_df(con)['name'] == name]
    indv.rename(columns={'mr': f'{name}_mr'}, inplace=True)
    indv.drop(['name'], axis=1, inplace=True)

    indv = indv.join(lines_df(con))
    indv.dropna(subset=['date'], inplace=True)

    indv['date'] = pd.to_datetime(indv['date'], format='%Y-%m-%d %H:%M:%S')
    indv = indv[indv[f'{name}_mr'] != 999.999999]

    '''start_date = datetime(2019, 7, 1)
    end_date = start_date + timedelta(days=90)

    indv = indv[indv['date'] > start_date]
    indv = indv[indv['date'] < end_date]'''

    return indv

def windrose_mr(name):
    compound = indv_compound(name)
    compound['date'] = compound['date'].apply(lambda x: x.strftime('%Y-%m-%d-%H'))
    compound.set_index(keys='date', inplace=True)

    dframe = compound.join(wind_data())
    #dframe = dframe[dframe['wd'] > 70][dframe['wd'] < 350]
    ax = WindroseAxes.from_ax()
    ax.bar(dframe['wd'].tolist(), dframe[f'{name}_mr'].tolist(), normed=True, opening=0.85, bins=10, nsector=32,
           cmap=cm.cool)
    ax.set_legend()
    plt.title(f'2019 July-September Wind Data for {name}')
    locs, labels = plt.yticks()
    new_labels = [str(s) + '%' for s in labels]
    for i in range(len(new_labels)):
        new_labels[i] = new_labels[i][12:15] + new_labels[i][17]
    plt.yticks(locs, labels=new_labels)

    plt.show()

def plot_mr(compounds, start_date, end_date):

    for name in compounds:
        plt.figure(dpi=600)
        dframe = excel_voc(name, start_date, end_date)

        plt.scatter(dframe.decimal_date, dframe[f'{name}_mr'], s=12, c='blue', alpha=0.5)
        plt.ylabel('Mixing Ratio')
        plt.xlabel('Decimal Year')
        plt.suptitle(f'{name} mixing ratio')
        plt.grid(True, linestyle='--')

        plt.savefig(fr'C:\Users\ARL\Desktop\Summit\analyses\Connor\plots\mr_vs_day\{name}_mr', dpi=600)

def plot_blanks_mr(compounds, start_date, end_date):

    for name in compounds:

        fig1 = plt.figure(1)
        fig2 = plt.figure(2)
        ax1 = fig1.add_subplot(111)
        ax2 = fig2.add_subplot(211)
        ax3 = fig2.add_subplot(212)

        dframe_blanks = excel_blanks(name, start_date, end_date)
        dframe_trap_blanks = excel_trap_blanks(name, start_date, end_date)

        ax1.scatter(dframe_trap_blanks.decimal_date, dframe_trap_blanks[f'{name}_mr'], s=12, c='red', alpha=0.5,
                    marker='o',
                    label='trap_blanks')
        ax1.scatter(dframe_blanks.decimal_date, dframe_blanks[f'{name}_mr'], s=12, c='blue', alpha=0.5,
                    marker='s', label='blanks')

        fig1.legend(loc='upper left')
        ax1.set_ylabel('Mixing Ratio')
        ax1.set_xlabel('Decimal Year')
        ax1.title.set_text(f'{name} blank')
        ax1.grid(True, linestyle='--')

        ax2 = fig2.add_subplot(211)
        ax3 = fig2.add_subplot(212)
        ax2.grid(True, linestyle='--')
        ax3.grid(True, linestyle='--')

        ax2.scatter(dframe_blanks.decimal_date, dframe_blanks[f'{name}_mr'], s=12, c='blue', alpha=0.5,
                    marker='s')
        ax2.title.set_text(f'{name} blanks')
        ax2.set_ylabel('Mixing Ratio')
        ax2.set_xlabel('Decimal Year')
        ax3.scatter(dframe_trap_blanks.decimal_date, dframe_trap_blanks[f'{name}_mr'], s=12, c='red', alpha=0.5,
                    marker='o')
        ax3.title.set_text(f'{name} trap_blanks')
        ax3.set_ylabel('Mixing Ratio')
        ax3.set_xlabel('Decimal Year')
        plt.subplots_adjust(bottom=0.1)
        fig2.tight_layout()

        fig1.savefig(fr'C:\Users\ARL\Desktop\Summit\analyses\Connor\plots\blanks_vs_day\{name}_blanks', dpi=600)
        fig2.savefig(fr'C:\Users\ARL\Desktop\Summit\analyses\Connor\plots\blanks_vs_day\{name}_blanks_separate',
                   dpi=600)
        fig1.clf()
        fig2.clf()

def plot_rf(compounds, start_date, end_date):
    for name in compounds:

        fig1 = plt.figure(1, dpi=600)
        fig2 = plt.figure(2, dpi=600)
        ax1 = fig1.add_subplot(111)
        ax2 = fig2.add_subplot(211)
        ax3 = fig2.add_subplot(212)

        dframe_rf_BH = excel_rf_BH(name, start_date, end_date)
        dframe_rf_Brad6 = excel_rf_Brad6(name, start_date, end_date)

        ax1.scatter(dframe_rf_BH.decimal_date, dframe_rf_BH[f'{name}_rf'], s=12, c='red', alpha=0.5,
                    marker='o',
                    label='BH')
        ax1.scatter(dframe_rf_Brad6.decimal_date, dframe_rf_Brad6[f'{name}_rf'], s=12, c='blue', alpha=0.5,
                    marker='s', label='Brad6')

        fig1.legend(loc='upper left')
        ax1.set_ylabel('Mixing Ratio')
        ax1.set_xlabel('Decimal Year')
        ax1.title.set_text(f'{name} responce factors')
        ax1.grid(True, linestyle='--')

        ax2 = fig2.add_subplot(211)
        ax3 = fig2.add_subplot(212)
        ax2.grid(True, linestyle='--')
        ax3.grid(True, linestyle='--')

        ax2.scatter(dframe_rf_Brad6.decimal_date, dframe_rf_Brad6[f'{name}_rf'], s=12, c='blue', alpha=0.5,
                    marker='s')
        ax2.title.set_text(f'{name} brad6 responce factors')
        ax2.set_ylabel('Mixing Ratio')
        ax2.set_xlabel('Decimal Year')
        ax3.scatter(dframe_rf_BH.decimal_date, dframe_rf_BH[f'{name}_rf'], s=12, c='red', alpha=0.5,
                    marker='o')
        ax3.title.set_text(f'{name} brad hall responce factors')
        ax3.set_ylabel('Mixing Ratio')
        ax3.set_xlabel('Decimal Year')
        fig2.tight_layout()

        fig1.savefig(fr'C:\Users\ARL\Desktop\Summit\analyses\Connor\plots\rf_vs_day\{name}_rf', dpi=600)
        fig2.savefig(fr'C:\Users\ARL\Desktop\Summit\analyses\Connor\plots\rf_vs_day\{name}_rf_separate',
                     dpi=600)
        fig1.clf()
        fig2.clf()

def create_csv(name):
    dframe = indv_compound(name)
    dframe['date'] = dframe['date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    dframe.set_index(keys='date', inplace=True)
    dframe.to_csv(fr'C:\Users\ARL\Desktop\Summit\analyses\Connor\test_data\{name}_data.csv', sep=" ", header=True)

if __name__ == '__main__':

    dbname = r'summit_voc.sqlite'
    dbpath = r"C:\Users\ARL\Desktop\Summit_Databases_Sept2019\{}".format(dbname)

    '''con = sql_connection()'''
    '''windrose_mr('ethane')'''

    compounds = ['ethane', 'ethene', 'propane', 'propene', 'i-butane', 'n-butane', 'i-pentane', 'n-pentane',
                 'hexane', 'Benzene', 'Toluene']

    plot_rf(compounds, '2019-1-1 0:0:0', '2019-12-30 0:0:0')


    '''post_feb = indv_compound('hexane').set_index('date')
    pre_feb = excel_voc('2019-1-1 0:0:0', '2019-02-01 14:44:32')
    print(pd.concat([pre_feb, post_feb]))'''





