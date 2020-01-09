import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from datetime import datetime, timedelta

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
                        r'Z:\Data\Summit_GC\Summit_GC_2019\CH4_results\Methane_Automated_2019.xlsx')
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
                dframe_2019 = pd.read_excel(r'Z:\Data\Summit_GC\Summit_GC_2019\CH4_results\Methane_Automated_2019.xlsx')
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
        dframe_2019 = pd.read_excel(r'Z:\Data\Summit_GC\Summit_GC_2019\CH4_results\Methane_Automated_2019.xlsx')
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


    # dframe_2019 = pd.read_excel(r'Z:\Data\Summit_GC\Summit_GC_2019\CH4_results\Methane_Automated_2019.xlsx')
    # dframe_2019.dropna(subset=['std_median'], inplace=True)
    # dframe_2019 = dframe_2019.loc[:, ['filename', 'std_median', 'run_median']]
    #
    # dframe_2019['Year'] = dframe_2019['filename'].apply(lambda x: int(str(x)[0:4]))
    # dframe_2019['Yearly_Day'] = dframe_2019['filename'].apply(lambda x: int(str(x)[4:7]))
    # dframe_2019['Hour'] = dframe_2019['filename'].apply(lambda x: int(str(x)[7:9]))
    # dframe_2019['Minute'] = dframe_2019['filename'].apply(lambda x: int(str(x)[9:11]))
    # dframe_2019['Second'] = dframe_2019['filename'].apply(lambda x: int(str(x)[11:13]))
    #
    # base_date = datetime(year=2019, month=1, day=1)
    # dframe_2019['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
    #                                         seconds=int(row[3])
    #                                         ) for row in
    #                   dframe_2019[[
    #                       'Yearly_Day',
    #                       'Hour',
    #                       'Minute',
    #                       'Second']].values]
    #
    # dframe_2019['decimal_date'] = [row[0] + (row[1]-1)/365 + row[2]/(365*24) + row[3]/(365*24*60) + row[4]/(
    #         365*24*60*60)
    #                                for row in
    #                                dframe_2019[['Year',
    #                                                                                                'Yearly_Day',
    #                                                                                         'Hour',
    #                                                                               'Minute',
    #                                                                    'Second']].values]
    # dframe_2019 = dframe_2019.loc[:, ['std_median', 'run_median', 'date', 'decimal_date']]
    # dframe_2019.set_index('date', inplace=True)

    dframe = pd.concat([dframe_2017, dframe_2018, dframe_2019])
    dframe = dframe.loc[dframe.index < end_date]
    dframe = dframe.loc[dframe.index > start_date]

    return(dframe)

if __name__ == '__main__':
    print(excel_methane('2017-6-1 0:0:0', '2019-6-30 0:0:0'))

