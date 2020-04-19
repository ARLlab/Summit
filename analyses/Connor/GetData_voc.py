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
from matplotlib.font_manager import FontProperties

from read_voc_excel import excel_voc, excel_blanks, excel_trap_blanks, excel_rf_BH, excel_rf_Brad6, excel_rf_BA


def wind_data():
    wind_data_read_2017 = pd.read_csv(r'met_sum_insitu_1_obop_hour_2017.txt', header=None)
    wind_data_read_2018 = pd.read_csv(r'met_sum_insitu_1_obop_hour_2018.txt', header=None)
    wind_data_read_2019 = pd.read_csv(r'met_sum_insitu_1_obop_hour_2019.txt', header=None)
    wind_data_read = pd.concat([wind_data_read_2017, wind_data_read_2018, wind_data_read_2019])
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

def windrose_mr(compounds, start, end):
    for compound in compounds:
        compound_df = excel_voc(compound, start, end)
        compound_df.index = compound_df.index.strftime('%Y-%m-%d-%H')
        start_string = datetime.strptime(start, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
        end_string = datetime.strptime(end, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

        # dframe = compound_df.join(wind_data())
        # dframe.dropna(inplace=True)
        # # dframe = dframe[dframe['wd'] > 72][dframe['wd'] < 342][dframe['ws'] > 1.0]
        # ax = WindroseAxes.from_ax()
        # ax.bar(dframe['wd'].tolist(), dframe[f'{compound}_mr'].tolist(), normed=True, opening=0.85, bins=10, nsector=32,
        #        cmap=cm.cool)
        # ax.set_legend()
        # plt.title(f'Wind Data for {compound} ({start_string} to {end_string})')
        # locs, labels = plt.yticks()
        # new_labels = [str(s) + '%' for s in labels]
        # for i in range(len(new_labels)):
        #     new_labels[i] = new_labels[i][12:15] + new_labels[i][17]
        # plt.yticks(locs, labels=new_labels)

        dframe = compound_df.join(wind_data())
        dframe.dropna(inplace=True)
        dframe = dframe[dframe['wd'] > 72][dframe['wd'] < 342][dframe['ws'] > 1.0]
        ax = WindroseAxes.from_ax()
        ax.bar(dframe['wd'].tolist(), dframe['ws'].tolist(), normed=True, opening=0.85, bins=10, nsector=32,
               cmap=cm.cool)
        ax.set_legend()
        plt.title(f'Cleaned Wind Data for all compounds')
        locs, labels = plt.yticks()
        new_labels = [str(s) + '%' for s in labels]
        for i in range(len(new_labels)):
            new_labels[i] = new_labels[i][12:15] + new_labels[i][17]
        plt.yticks(locs, labels=new_labels)

        plt.savefig(fr'C:\Users\ARL\Desktop\Summit\analyses\Connor\plots\windroses\all_compounds', dpi=600)

def plot_mr(compounds, start_date, end_date):
    counter = 0
    colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k', [(0.6, 0, 0.7)], [(0.5, 1, 1)]]
    fontP = FontProperties()
    fontP.set_size(5)
    start_string = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
    end_string = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

    for name in compounds:
        dframe = excel_voc(name, start_date, end_date)
        fig1 = plt.figure(1, dpi=600)
        ax1 = fig1.add_subplot(111)

        ax1.scatter(dframe.decimal_date, dframe[f'{name}_mr'], s=12, c='blue', alpha=0.5)
        ax1.set_ylabel('Mixing Ratio')
        ax1.set_xlabel('Decimal Year')
        ax1.title.set_text(f'{name} mixing ratio ({start_string} to {end_string})')
        ax1.grid(True, linestyle='--')
        fig1.savefig(fr'C:\Users\ARL\Desktop\Summit\analyses\Connor\plots\mr_vs_day\{name}_mr', dpi=600)

        fig1.clf()

    #     fontP = FontProperties()
    #     fontP.set_size(5)
    #     fig2 = plt.figure(2, dpi=600)
    #     ax2 = fig2.add_subplot(111)
    #     ax2.grid(True, linestyle='--')
    #     ax2.title.set_text(f'{start_string} to {end_string} mixing ratios ({compounds[0]}:{compounds[-1]})')
    #     ax2.set_ylabel('Mixing Ratio')
    #     ax2.set_xlabel('Decimal Year')
    #     ax2.scatter(dframe.decmial_date_year, dframe[f'{name}_mr'], s=12, c=colors[counter], alpha=0.5)
    #     fig2.legend(loc='upper left', prop=fontP)
    #     fig2.tight_layout()
    #
    #     counter += 1
    # fig2.savefig(rf'C:\Users\ARL\Desktop\Summit\analyses\Connor\plots\mr_vs_day\totals_{compounds[0]}:{compounds[-1]}', dpi=600)


def plot_blanks_mr(compounds, start_date, end_date):
    start_string = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
    end_string = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

    for name in compounds:

        fig1 = plt.figure(1)
        fig2 = plt.figure(2)
        ax1 = fig1.add_subplot(111)
        # ax2 = fig2.add_subplot(211)
        # ax3 = fig2.add_subplot(212)

        dframe_blanks = excel_blanks(name, start_date, end_date)
        dframe_blanks = dframe_blanks[dframe_blanks[f'{name}_mr'] != 99999]
        dframe_trap_blanks = excel_trap_blanks(name, start_date, end_date)
        dframe_trap_blanks = dframe_trap_blanks[dframe_trap_blanks[f'{name}_mr'] != 99999]
        dframe_blanks = dframe_blanks[dframe_blanks[f'{name}_mr'] < 0.10]
        dframe_trap_blanks = dframe_trap_blanks[dframe_trap_blanks[f'{name}_mr'] < 0.10]

        ax1.scatter(dframe_trap_blanks.decmial_date_year, dframe_trap_blanks[f'{name}_mr'], s=12, c='red', alpha=0.5,
                    marker='o',
                    label='trap_blanks')
        ax1.scatter(dframe_blanks.decmial_date_year, dframe_blanks[f'{name}_mr'], s=12, c='blue', alpha=0.5,
                    marker='s', label='blanks')

        fig1.legend(loc='upper left')
        ax1.set_ylabel('Mixing Ratio')
        ax1.set_xlabel('Decimal Year')
        ax1.title.set_text(f'{name} blank ({start_string} to {end_string}')
        ax1.grid(True, linestyle='--')

        # ax2 = fig2.add_subplot(211)
        # ax3 = fig2.add_subplot(212)
        # ax2.grid(True, linestyle='--')
        # ax3.grid(True, linestyle='--')
        #
        # ax2.scatter(dframe_blanks.decimal_date, dframe_blanks[f'{name}_mr'], s=12, c='blue', alpha=0.5,
        #             marker='s')
        # ax2.title.set_text(f'{name} blanks')
        # ax2.set_ylabel('Mixing Ratio')
        # ax2.set_xlabel('Decimal Year')
        # ax3.scatter(dframe_trap_blanks.decimal_date, dframe_trap_blanks[f'{name}_mr'], s=12, c='red', alpha=0.5,
        #             marker='o')
        # ax3.title.set_text(f'{name} trap_blanks')
        # ax3.set_ylabel('Mixing Ratio')
        # ax3.set_xlabel('Decimal Year')
        # plt.subplots_adjust(bottom=0.1)
        # fig2.tight_layout()
        #
        fig1.savefig(fr'C:\Users\ARL\Desktop\Summit\analyses\Connor\plots\blanks_vs_day\{name}_blanks_adjusted',
                     dpi=600)
        # fig2.savefig(fr'C:\Users\ARL\Desktop\Summit\analyses\Connor\plots\blanks_vs_day\{name}_blanks_separate',
        #            dpi=600)
        fig1.clf()
        # fig2.clf()

def plot_rf(compounds, start_date, end_date):
    start_string = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
    end_string = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
    counter = 0
    colors = ['b', 'g', 'r', 'c','m','y','k', [(0.6,0,0.7)], [(0.5,1,1)], [(0.3,1,0.7)]]
    for name in compounds:

        fig1 = plt.figure(1, dpi=600)
        fig2 = plt.figure(2, dpi=600)
        ax1 = fig1.add_subplot(111)
        ax2 = fig2.add_subplot(211)
        ax3 = fig2.add_subplot(212)

        dframe_rf_BH = excel_rf_BH(name, start_date, end_date)
        dframe_rf_BH = dframe_rf_BH[dframe_rf_BH[f'{name}_rf']!=99999]
        dframe_rf_Brad6 = excel_rf_Brad6(name, start_date, end_date)
        dframe_rf_Brad6 = dframe_rf_Brad6[dframe_rf_Brad6[f'{name}_rf'] != 99999]
        dframe_rf_BA = excel_rf_BA(name, start_date, end_date)
        dframe_rf_BA = dframe_rf_BA[dframe_rf_BA[f'{name}_rf'] != 99999]
        dframe_rf_BA = dframe_rf_BA[dframe_rf_BA[f'{name}_rf'] < 100]

        # ax1.scatter(dframe_rf_BH.decmial_date_year, dframe_rf_BH[f'{name}_rf'], s=12, c='red', alpha=0.5,
        #             marker='o',
        #             label='BH')
        # ax1.scatter(dframe_rf_Brad6.decmial_date_year, dframe_rf_Brad6[f'{name}_rf'], s=12, c='blue', alpha=0.5,
        #             marker='s', label='Brad6')
        # ax1.scatter(dframe_rf_BA.decmial_date_year, dframe_rf_BA[f'{name}_rf'], s=12, c='yellow', alpha=0.5,
        #             marker='s', label='BA')
        #
        # fig1.legend(loc='upper left')
        # ax1.set_ylabel('Mixing Ratio')
        # ax1.set_xlabel('Decimal Year')
        # ax1.title.set_text(f'{name} response factors (adjusted)')
        # ax1.grid(True, linestyle='--')

        # ax2 = fig2.add_subplot(211)
        # ax3 = fig2.add_subplot(212)
        # ax2.grid(True, linestyle='--')
        # ax3.grid(True, linestyle='--')
        #
        # ax2.scatter(dframe_rf_Brad6.decimal_date_year, dframe_rf_Brad6[f'{name}_rf'], s=12, c='blue', alpha=0.5,
        #             marker='s')
        # ax2.title.set_text(f'{name} brad6 response factors')
        # ax2.set_ylabel('Mixing Ratio')
        # ax2.set_xlabel('Decimal Year')
        # ax3.scatter(dframe_rf_BH.decimal_date_year, dframe_rf_BH[f'{name}_rf'], s=12, c='red', alpha=0.5,
        #             marker='o')
        # ax3.title.set_text(f'{name} brad hall response factors')
        # ax3.set_ylabel('Mixing Ratio')
        # ax3.set_xlabel('Decimal Year')
        # fig2.tight_layout()

        # fig1.savefig(fr'C:\Users\ARL\Desktop\Summit\analyses\Connor\plots\rf_vs_day\{name}_rf', dpi=600)
        # fig2.savefig(fr'C:\Users\ARL\Desktop\Summit\analyses\Connor\plots\rf_vs_day\{name}_rf_separate',
        #           dpi=600)

        fig1.clf()
        fig2.clf()

        fontP = FontProperties()
        fontP.set_size(5)

        fig3 = plt.figure(3, dpi=600)
        ax4 = fig3.add_subplot(111)
        ax4.grid(True, linestyle='--')
        ax4.scatter(dframe_rf_Brad6.decmial_date_year, dframe_rf_Brad6[f'{name}_rf'], s=12, c=colors[counter],
                    alpha=0.5,
                    marker = 's', label=f'{name}')
        fig3.legend(loc='upper left', prop=fontP)
        ax4.title.set_text(f'brad6 response factors ({start_string} to {end_string})')
        ax4.set_ylabel('Mixing Ratio')
        ax4.set_xlabel('Decimal Year')

        fig4 = plt.figure(4, dpi=600)
        ax5 = fig4.add_subplot(111)
        ax5.grid(True, linestyle='--')
        ax5.scatter(dframe_rf_BH.decmial_date_year, dframe_rf_BH[f'{name}_rf'], s=12, c=colors[counter], alpha=0.5,
                    marker='o', label=f'{name}')
        fig4.legend(loc='upper left', prop=fontP)
        ax5.title.set_text(f'BH response factors ({start_string} to {end_string})')
        ax5.set_ylabel('Mixing Ratio')
        ax5.set_xlabel('Decimal Year')

        fig5 = plt.figure(5, dpi=600)
        ax6 = fig5.add_subplot(111)
        ax6.grid(True, linestyle='--')
        ax6.scatter(dframe_rf_BA.decmial_date_year, dframe_rf_BA[f'{name}_rf'], s=12, c=colors[counter], alpha=0.5,
                    marker='o', label=f'{name}')
        fig5.legend(loc='upper left', prop=fontP)
        ax6.title.set_text(f'BA response factors ({start_string} to {end_string})')
        ax6.set_ylabel('Mixing Ratio')
        ax6.set_xlabel('Decimal Year')
        fig3.tight_layout
        fig4.tight_layout
        fig5.tight_layout


        counter += 1
    fig3.savefig(fr'C:\Users\ARL\Desktop\Summit\analyses\Connor\plots\rf_vs_day\total_rf_Brad6', dpi=600)
    fig4.savefig(fr'C:\Users\ARL\Desktop\Summit\analyses\Connor\plots\rf_vs_day\total_rf_BH', dpi=600)
    fig5.savefig(fr'C:\Users\ARL\Desktop\Summit\analyses\Connor\plots\rf_vs_day\total_rf_BA', dpi=600)

def create_csv(compounds):

    dframe = pd.DataFrame(index=excel_voc('ethane', '2017-10-21 0:0:0', '2019-12-30 0:0:0').index)

    for compound in compounds:
        comp_dframe = excel_voc(f'{compound}', '2017-10-21 0:0:0', '2019-12-30 0:0:0')
        dframe[f'{compound}'] = comp_dframe[f'{compound}_mr']
        dframe[f'{compound}_numflag'] = np.nan
    dframe.to_csv(path_or_buf=r'C:\Users\ARL\Desktop\Summit\analyses\Connor\voc_data_test_2019.csv')


def plot_wind_difference(compounds, start_date, end_date):
    start_string = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
    end_string = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
    fontP = FontProperties()
    fontP.set_size(5)

    for name in compounds:
        dframe_vocs = excel_voc(name, start_date, end_date)
        dframe_vocs.index = dframe_vocs.index.map(lambda x: x.strftime('%Y-%m-%d %H'))
        dframe = dframe_vocs.join(wind_data())

        dframe_outsector_wind = dframe[dframe['wd'] > 72][dframe['wd'] < 342][dframe['ws'] > 1.0]
        dframe_insector_wind = dframe[~dframe.index.isin(dframe_outsector_wind.index)]

        dframe_outsector = dframe_outsector_wind.drop(['wd','ws'], axis=1)
        dframe_insector = dframe_insector_wind.drop(['wd', 'ws'], axis=1)
        data = [dframe_outsector[f'{name}_mr'].tolist(), dframe_insector[f'{name}_mr'].tolist()]

        fig1 = plt.figure(1, dpi=600)
        ax1 = fig1.add_subplot(111)

        red_diamond = dict(marker='D', markerfacecolor='r', markersize=4, alpha=0.7)
        ax1.boxplot(data, vert=False, flierprops=red_diamond, widths=0.7)
        plt.yticks(range(3), ['', 'clean', 'polluted'])
        ax1.title.set_text(f'{name} distribution ({start_string} to {end_string})')
        ax1.set_xlabel('mixing ratio')

        fig2 = plt.figure(2, dpi=600)
        ax2 = fig2.add_subplot(111)
        ax2.scatter(dframe_insector.decmial_date_year, dframe_insector[f'{name}_mr'], s=12, c='red', alpha=0.5,
                    marker='o', label='polluted')
        ax2.scatter(dframe_outsector.decmial_date_year, dframe_outsector[f'{name}_mr'], s=12, c='blue', alpha=0.2,
                    marker='s', label='not polluted')
        fig2.legend(loc='upper left', prop=fontP)
        ax2.title.set_text(f'{name} scatter')
        ax2.set_xlabel('decimal year')
        ax2.set_ylabel('mixing ratio')
        ax2.grid(True, linestyle='--')

        fig2.tight_layout()

        # fig1.savefig(rf'C:\Users\ARL\Desktop\Summit\analyses\Connor\plots\wind_pollution\{name}_boxplot', dpi=600)
        # fig2.savefig(rf'C:\Users\ARL\Desktop\Summit\analyses\Connor\plots\wind_pollution\{name}_scatter', dpi=600)

        plt.show()

        fig1.clf()
        fig2.clf()




if __name__ == '__main__':

    dbname = r'summit_voc.sqlite'
    dbpath = r"C:\Users\ARL\Desktop\Summit_Databases_Sept2019\{}".format(dbname)

    '''con = sql_connection()'''

    compounds = ['ethane', 'ethene', 'propane', 'propene', 'i-butane', 'n-butane', 'i-pentane', 'n-pentane',
                 'hexane', 'Benzene', 'Toluene']
    compounds_standard = ['ethane', 'propane', 'i-butane', 'n-butane', 'acetylene', 'i-pentane', 'n-pentane',
                 'hexane', 'Benzene', 'Toluene']

    plot_wind_difference(['ethane'], '2017-8-20 0:0:0', '2019-11-30 0:0:0')
    # plot_rf(compounds_standard, '2017-1-1 0:0:0', '2019-12-30 0:0:0')
    # windrose_mr(['propane'], '2017-1-1 0:0:0', '2019-12-31 0:0:0')
    #create_csv(compounds)
    # plot_mr(compounds_standard, '2019-1-1 0:0:0', '2019-12-31 23:59:59')
    # plot_blanks_mr(['Toluene', 'n-pentane'], '2017-8-20 0:0:0', '2019-12-31 23:59:59')


    '''post_feb = indv_compound('hexane').set_index('date')
    pre_feb = excel_voc('2019-1-1 0:0:0', '2019-02-01 14:44:32')
    print(pd.concat([pre_feb, post_feb]))'''





