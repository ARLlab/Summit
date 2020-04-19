import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from datetime import datetime, timedelta
from windrose import WindroseAxes
import matplotlib.cm as cm
import matplotlib.pyplot as plt
from GetData_Picarro import sql_connection, picarro_df, plot_picarro
from GetData_voc import wind_data

def excel_methane(start, end):
    start_date = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    start_year = int(start_date.year)
    end_year = int(end_date.year)

    dframe_2017 = pd.DataFrame()
    dframe_2018 = pd.DataFrame()
    dframe_2019 = pd.DataFrame()

    if start_year == 2017:
        if end_year >= 2017:
            dframe_2017 = pd.read_excel(r'Z:\Data\Summit_GC\Summit_GC_2017\CH4_results\SUM_CH4_insitu_2017.xlsx')
            dframe_2017.dropna(subset=['Run median'], inplace=True)
            dframe_2017 = dframe_2017.loc[:, ['SampleDay', 'SampleHour', 'Run median', 'Decimal Year']]

            base_date = datetime(year=2017, month=1, day=1)
            dframe_2017['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1])
                                                         ) for row in
                                   dframe_2017[[
                                       'SampleDay',
                                       'SampleHour']].values]

            dframe_2017.rename(inplace=True, columns={'Run median': 'run_median', 'Decimal Year': 'decimal_date'})
            dframe_2017 = dframe_2017.loc[:, ['run_median', 'date', 'decimal_date']]
            dframe_2017.set_index('date', inplace=True)
            if end_year >= 2018:
                dframe_2018 = pd.read_excel(r'Z:\Data\Summit_GC\Summit_GC_2018\CH4_results\SUM_CH4_insitu_2018.xlsx')
                dframe_2018.dropna(subset=['Run median'], inplace=True)
                dframe_2018 = dframe_2018.loc[:, ['SampleDay', 'SampleHour', 'Run median', 'Decimal Year']]

                base_date = datetime(year=2018, month=1, day=1)
                dframe_2018['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1])
                                                             ) for row in
                                       dframe_2018[[
                                           'SampleDay',
                                           'SampleHour']].values]

                dframe_2018.rename(inplace=True, columns={'Run median': 'run_median', 'Decimal Year': 'decimal_date'})
                dframe_2018 = dframe_2018.loc[:, ['run_median', 'date', 'decimal_date']]
                dframe_2018.set_index('date', inplace=True)
                if end_year >= 2019:
                    dframe_2019 = pd.read_excel(
                        r'C:\Users\ARL\Desktop\CH4_results_2019_copy\Methane_Automated_2019_Number_Format.xlsx')
                    dframe_2019.dropna(subset=['std_median'], inplace=True)
                    dframe_2019 = dframe_2019.loc[:, ['filename', 'std_median', 'run_median']]

                    dframe_2019['Year'] = dframe_2019['filename'].apply(lambda x: int(str(x)[0:4]))
                    dframe_2019['Yearly_Day'] = dframe_2019['filename'].apply(lambda x: int(str(x)[4:7]))
                    dframe_2019['Hour'] = dframe_2019['filename'].apply(lambda x: int(str(x)[7:9]))
                    dframe_2019['Minute'] = dframe_2019['filename'].apply(lambda x: int(str(x)[9:11]))
                    dframe_2019['Second'] = dframe_2019['filename'].apply(lambda x: int(str(x)[11:13]))

                    base_date = datetime(year=2019, month=1, day=1)
                    dframe_2019['date'] = [
                        base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                              seconds=int(row[3])
                                              ) for row in
                        dframe_2019[[
                            'Yearly_Day',
                            'Hour',
                            'Minute',
                            'Second']].values]

                    dframe_2019['decimal_date'] = [
                        row[0] + (row[1] - 1) / 365 + row[2] / (365 * 24) + row[3] / (365 * 24 * 60) + row[4] / (
                                365 * 24 * 60 * 60)
                        for row in
                        dframe_2019[['Year',
                                     'Yearly_Day',
                                     'Hour',
                                     'Minute',
                                     'Second']].values]
                    dframe_2019 = dframe_2019.loc[:, ['run_median', 'date', 'decimal_date']]
                    dframe_2019.set_index('date', inplace=True)
    elif start_year == 2018:
        if end_year >= 2018:
            dframe_2018 = pd.read_excel(r'Z:\Data\Summit_GC\Summit_GC_2018\CH4_results\SUM_CH4_insitu_2018.xlsx')
            dframe_2018.dropna(subset=['Run median'], inplace=True)
            dframe_2018 = dframe_2018.loc[:, ['SampleDay', 'SampleHour', 'Run median', 'Decimal Year']]

            base_date = datetime(year=2018, month=1, day=1)
            dframe_2018['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1])
                                                         ) for row in
                                   dframe_2018[[
                                       'SampleDay',
                                       'SampleHour']].values]

            dframe_2018.rename(inplace=True, columns={'Run median': 'run_median', 'Decimal Year': 'decimal_date'})
            dframe_2018 = dframe_2018.loc[:, ['run_median', 'date', 'decimal_date']]
            dframe_2018.set_index('date', inplace=True)
            if end_year >= 2019:
                dframe_2019 = pd.read_excel(r'C:\Users\ARL\Desktop\CH4_results_2019_copy\Methane_Automated_2019_Number_Format.xlsx')
                dframe_2019.dropna(subset=['std_median'], inplace=True)
                dframe_2019 = dframe_2019.loc[:, ['filename', 'std_median', 'run_median']]

                dframe_2019['Year'] = dframe_2019['filename'].apply(lambda x: int(str(x)[0:4]))
                dframe_2019['Yearly_Day'] = dframe_2019['filename'].apply(lambda x: int(str(x)[4:7]))
                dframe_2019['Hour'] = dframe_2019['filename'].apply(lambda x: int(str(x)[7:9]))
                dframe_2019['Minute'] = dframe_2019['filename'].apply(lambda x: int(str(x)[9:11]))
                dframe_2019['Second'] = dframe_2019['filename'].apply(lambda x: int(str(x)[11:13]))

                base_date = datetime(year=2019, month=1, day=1)
                dframe_2019['date'] = [
                    base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                          seconds=int(row[3])
                                          ) for row in
                    dframe_2019[[
                        'Yearly_Day',
                        'Hour',
                        'Minute',
                        'Second']].values]

                dframe_2019['decimal_date'] = [
                    row[0] + (row[1] - 1) / 365 + row[2] / (365 * 24) + row[3] / (365 * 24 * 60) + row[4] / (
                            365 * 24 * 60 * 60)
                    for row in
                    dframe_2019[['Year',
                                 'Yearly_Day',
                                 'Hour',
                                 'Minute',
                                 'Second']].values]
                dframe_2019 = dframe_2019.loc[:, ['run_median', 'date', 'decimal_date']]
                dframe_2019.set_index('date', inplace=True)
    elif start_year == 2019:
        dframe_2019 = pd.read_excel(r'C:\Users\ARL\Desktop\CH4_results_2019_copy\Methane_Automated_2019_Number_Format.xlsx')
        dframe_2019.dropna(subset=['std_median'], inplace=True)
        dframe_2019 = dframe_2019.loc[:, ['filename', 'std_median', 'run_median']]

        dframe_2019['Year'] = dframe_2019['filename'].apply(lambda x: int(str(x)[0:4]))
        dframe_2019['Yearly_Day'] = dframe_2019['filename'].apply(lambda x: int(str(x)[4:7]))
        dframe_2019['Hour'] = dframe_2019['filename'].apply(lambda x: int(str(x)[7:9]))
        dframe_2019['Minute'] = dframe_2019['filename'].apply(lambda x: int(str(x)[9:11]))
        dframe_2019['Second'] = dframe_2019['filename'].apply(lambda x: int(str(x)[11:13]))

        base_date = datetime(year=2019, month=1, day=1)
        dframe_2019['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                                     seconds=int(row[3])
                                                     ) for row in
                               dframe_2019[[
                                   'Yearly_Day',
                                   'Hour',
                                   'Minute',
                                   'Second']].values]

        dframe_2019['decimal_date'] = [
            row[0] + (row[1] - 1) / 365 + row[2] / (365 * 24) + row[3] / (365 * 24 * 60) + row[4] / (
                    365 * 24 * 60 * 60)
            for row in
            dframe_2019[['Year',
                         'Yearly_Day',
                         'Hour',
                         'Minute',
                         'Second']].values]
        dframe_2019 = dframe_2019.loc[:, ['run_median', 'date', 'decimal_date']]
        dframe_2019.set_index('date', inplace=True)

    dframe = pd.concat([dframe_2017, dframe_2018, dframe_2019])
    dframe = dframe.loc[dframe.index < end_date]
    dframe = dframe.loc[dframe.index > start_date]

    return(dframe)

def plot_methane(start, end):
    start_string = datetime.strptime(start, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
    end_string = datetime.strptime(end, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
    dframe = excel_methane(start, end)
    dframe = dframe[dframe['run_median'] < 2000][dframe['run_median'] > 1400]
    # dframe = dframe[dframe['run_median'] > 1400]

    dbname = r'summit_picarro.sqlite'
    dbpath = r"C:\Users\ARL\Desktop\Summit_Databases_Sept2019\{}".format(dbname)
    con = sql_connection(dbpath)

    dframe_pic = picarro_df(con, '2019-5-1 0:0:0', '2019-9-30 0:0:0')

    plt.scatter(dframe.decimal_date, dframe['run_median'], s=12, c='red', alpha=0.4)
    plt.scatter(dframe_pic.decimal_date, dframe_pic['ch4'], s=12, c='blue', alpha=0.3)
    plt.ylabel('Mixing Ratio')
    plt.xlabel('Decimal Year')
    plt.ticklabel_format(useOffset=False)
    plt.suptitle(f'GC methane and Picarro ({start_string} to {end_string})')
    plt.grid(True, linestyle='--')

    plt.savefig(fr'C:\Users\ARL\Desktop\Summit\analyses\Connor\plots\Methane_Picarro\methane_picarro_mr')

def plot_methane_difference(start, end):
    dframe = excel_methane(start, end)
    dframe = dframe[dframe['run_median'] < 2000]

    dbname = r'summit_picarro.sqlite'
    dbpath = r"C:\Users\ARL\Desktop\Summit_Databases_Sept2019\{}".format(dbname)
    con = sql_connection(dbpath)
    dframe_pic = picarro_df(con, '2019-5-1 0:0:0', '2019-12-30 0:0:0')
    dframe_re = dframe.reindex(index=dframe_pic.index, fill_value=0, method='nearest')
    dframe_re['difference'] = dframe_re['run_median'] - dframe_pic['ch4']
    dframe.dropna()
    dframe_pic.dropna()

    fig1 = plt.figure(1)
    ax1 = fig1.add_subplot(111)

    ax1.scatter(dframe_re.decimal_date, dframe_re['difference'], s=12, c='red', alpha=0.5)
    ax1.set_ylabel('GC Methane - Picarro Methane')
    ax1.set_xlabel('Decimal Year')
    ax1.title.set_text('difference between GC and Picarro Methane')
    ax1.grid(True, linestyle='--')

    plt.savefig(fr'C:\Users\ARL\Desktop\Summit\analyses\Connor\plots\Methane_Picarro\methane_picarro_difference')

def windrose_methane(start, end):
    compound = excel_methane(start, end)
    compound.index = compound.index.strftime('%Y-%m-%d-%H')
    start_string = datetime.strptime(start, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
    end_string = datetime.strptime(end, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

    dframe = compound.join(wind_data())
    dframe = dframe.loc[dframe['run_median'] < 3000, :]
    dframe = dframe[dframe['wd'] > 72][dframe['wd'] < 342][dframe['ws'] > 1.0]
    ax = WindroseAxes.from_ax()
    ax.bar(dframe['wd'].tolist(), dframe['run_median'].tolist(), normed=True, opening=0.85, bins=10, nsector=32,
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
    # windrose_methane('2017-5-1 0:0:0', '2020-1-1 0:0:0')
    plot_methane('2019-5-1 0:0:0', '2019-12-31 23:59:59')

